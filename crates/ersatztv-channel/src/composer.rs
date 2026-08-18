//! Per-cohort playlist composition.
//!
//! A cohort's playlist is the shared session's playlist with a templated
//! item's segments replaced by the cohort's variant segments, keyed by the
//! playout item ids recorded in each session's sidecar. Substitution is
//! per position: the decision (variant or shared) is made per item just
//! before the item enters the served window, and a position that has already
//! been served is never served again from the other source. Everything here
//! operates on recorded metadata; nothing is inferred from media timestamps.
//!
//! The composed playlist shares the shared playlist's sequence space: a
//! shared segment's media sequence is the index in its file name, and a
//! substituted segment takes the sequence of the shared segment it replaced.
//! A client moved between the two playlists therefore lands on the same
//! numbering, instead of two counters that started at different times.

use std::collections::{HashMap, HashSet, VecDeque};

use ersatztv_core::sidecar::{PlaylistSidecar, SidecarPipeline};
use time::OffsetDateTime;
use time::macros::format_description;

/// Mirror of the worker's segment length; used only for envelope grid
/// arithmetic, exactly as the worker anchors its own playlist.
pub const SEGMENT_SECONDS: u64 = 4;

// This mirror is maintained by hand. If it disagrees with the length the
// pipeline actually encodes with, every grid computation here silently
// drifts off the real segments.
const _: () = assert!(
    SEGMENT_SECONDS == ffpipeline::pipeline::SEGMENT_SECONDS as u64,
    "composer::SEGMENT_SECONDS must mirror ffpipeline::pipeline::SEGMENT_SECONDS"
);

/// How many entries a rendered window carries, and so how far behind the
/// newest composed entry the serve head sits: the head stays a full window
/// back, keeping the three-target-durations of media rfc8216bis 6.2.2
/// requires. Worth its cost in delay: a head at the composed edge serves
/// one segment, and a player cannot buffer one segment.
pub const SERVED_SEGMENTS: usize = 10;

/// How often the composer repeats a warning about a state it cannot leave,
/// so a stuck head is visible in the log without filling it at tick rate.
const STALL_WARN_SECONDS: i64 = 30;

/// How long an entry stays in session history after leaving the serve window.
const HISTORY_SECONDS: u64 = 120;

/// How long an item's decision stays open, measured from the program date
/// time of the item's first shared segment. Past it, the item is pinned to
/// shared; earlier, composition holds back instead.
///
/// A variant's source is live, so it cannot be worked ahead: it connects at
/// the item's air time and closes its first segment about ten seconds later,
/// whatever the shared session's work-ahead happened to be when the variant
/// spawned. Measured over 22 clean windows on channels 11 and 13 on
/// 2026-08-12, with the shared session's work-ahead ranging from 23s to 55s,
/// that startup was 8.6s to 11.2s after the item's own stamp and showed no
/// dependence on the work-ahead at all.
///
/// So the budget has to clear an 11s startup, and clear it with room for the
/// two things that eat into it. The composer only re-evaluates about every
/// two seconds, so a decision lands on a poll boundary rather than the
/// instant the variant is ready. And the stamp this deadline is measured
/// from runs early of wall clock over a long run, because the playlist
/// manager's clock only ever advances by emitted segment durations and is
/// never re-anchored to the schedule, which shortens the budget by exactly
/// that error. At the previous 12s the margin was 0.8s to 3.4s and roughly
/// one window in ten lost the race, serving slate over the top of a variant
/// that then arrived two seconds late.
const DECISION_BUDGET_SECONDS: u64 = 20;

/// How far behind the live edge the composed timeline stops.
///
/// A cohort's viewer plays at the newest segment its playlist offers, so
/// whatever the composer emits sets how far behind wall clock that viewer
/// sits. Left to follow production, that distance is an accident: file
/// content transcodes far faster than realtime, so the composed edge runs
/// AHEAD of wall and drags viewers up to it. A variant cannot follow them
/// there. Its source is live, so it connects at air time and its first
/// segment closes about ten seconds later, measured on channel 13. A viewer
/// at the live edge therefore reaches a templated window before any variant
/// output for it can exist, and waits.
///
/// Holding composition this far behind the edge makes the distance a
/// decision instead. Viewers settle roughly a client buffer further back,
/// past the variant's startup, so the substitution is ready before they ask
/// for it and slate stays what it is meant to be: what plays when the
/// variant genuinely does not arrive.
const COMPOSE_TRAIL_SECONDS: u64 = 8;

/// How far the serve head may fall behind the shared playlist's own head
/// before excess lag starts being trimmed. A trim inside this bound never
/// happens; past it, the head jumps only onto an item boundary, so what a
/// viewer loses is the tail of whatever item they were behind in, and they
/// resume exactly at the start of the next one, the same cut a broadcast
/// makes. With no boundary in reach the head keeps walking and retries.
///
/// Measured against the shared head, never against this timeline's own
/// edge. Composition holds whenever a position's variant twin is missing,
/// and a hold stops the composed edge and the head together: a bound read
/// off the composed edge would report no lag at all in exactly the state
/// where the viewer is falling behind the channel fastest.
const MAX_LAG_SEGMENTS: u64 = 10;

/// Past this bound the head skips forward wherever it lands, boundary or
/// not. A gap this size means the cohort's timeline is broken, not late,
/// and a bounded delay matters more than a clean cut.
///
/// Must stay strictly inside the playlist manager's retention window
/// (`HISTORY_DURATION` over `SEGMENT_SECONDS`): a serve head is allowed to
/// trail the shared head by this much, and everything it can point at must
/// still exist on disk. The compile-time assertion lives next to
/// `HISTORY_DURATION` in `playlist_manager.rs`.
pub const HARD_LAG_SEGMENTS: u64 = 20;

// Holding a decision open costs the serve head exactly the time it holds.
// Composition stops at an undecided item once that item's first segment is
// COMPOSE_TRAIL_SECONDS old, and the head cannot walk past a stopped
// composed edge, so the whole remainder of the budget is lag the head
// accumulates against the shared playlist. The budget only spends that much
// when a variant genuinely never arrives, but it has to stay affordable
// even then: past MAX_LAG_SEGMENTS the head starts being trimmed forward,
// which is a decision hold turning into skipped program content.
const _: () = assert!(
    (DECISION_BUDGET_SECONDS - COMPOSE_TRAIL_SECONDS) / SEGMENT_SECONDS < MAX_LAG_SEGMENTS,
    "a full decision hold must not by itself push the serve head past MAX_LAG_SEGMENTS"
);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemDecision {
    /// Substitute variant segments into the item. `anchor_ms` is where the
    /// variant's first segment sits in the item, from its recorded pts
    /// offset; `join_ms` is where substitution actually starts, which is
    /// never before a position this session already served from the shared
    /// feed. Variant segments for positions between the two are consumed
    /// unserved so the substitution stays content-aligned. Final once made.
    Variant { join_ms: u64, anchor_ms: u64 },
    /// Serve the shared feed. Soft: upgrades to a late `Variant` join if the
    /// cohort's variant starts producing anchored output mid-item.
    Shared,
}

#[derive(Debug, Clone)]
pub struct ComposedEntry {
    pub path: String,
    pub duration: f64,
    pub program_date_time: OffsetDateTime,
    pub discontinuity: bool,
    /// The shared sequence number of this position: the index in the shared
    /// segment's file name, whether this entry serves that segment or the
    /// variant segment that replaced it.
    pub sequence: u64,
    /// Whether this entry serves variant content. The serve head may jump
    /// over shared content to catch up with the shared playlist, but the
    /// soft lag trim never jumps over variant content: that content is why
    /// the cohort exists, so a lagging viewer plays all of it a little late
    /// instead of losing it.
    ///
    /// The one exception is the hard trim past [`HARD_LAG_SEGMENTS`], which
    /// skips wherever it lands. That bound exists to keep the head inside
    /// the shared session's retention window, and a window pointing at
    /// deleted segments serves 404s to everyone, so there it really is
    /// better to lose substituted content than to keep referencing it.
    pub variant: bool,
}

/// Per-cohort-session playlist state. Entries are append-only; the head trims
/// forward as segments age out, advancing the media sequence exactly as
/// rfc8216bis 6.2.2 requires of a live playlist.
#[derive(Debug, Default)]
pub struct SessionPlaylist {
    entries: VecDeque<ComposedEntry>,
    /// Sequence number of `entries[0]`; meaningful only while entries exist.
    head_sequence: u64,
    /// Discontinuities that have trimmed off the head.
    head_discontinuity_sequence: u64,
    /// The sequence at the front of the served window, and when it last
    /// advanced. The head mirrors the shared playlist's head whenever the
    /// composed timeline has reached it, and otherwise advances at playback
    /// rate, so a lagging cohort plays through its content instead of having
    /// the window jump over it.
    serve_head: Option<u64>,
    head_advanced_at: Option<OffsetDateTime>,
    /// When the composer last reported a head that is past the hard lag
    /// bound and cannot be moved. Throttles that warning: the state persists
    /// for as long as composition is held, and the composer renders every
    /// couple of seconds.
    lag_stalled_warned_at: Option<OffsetDateTime>,
    decisions: HashMap<String, ItemDecision>,
    /// The sequence number of the first segment ever observed for each
    /// templated item, recorded once per numbering space and never
    /// recomputed within it. Positions inside an item derive from this: the
    /// sidecar trims its history, so anything measured from "the first
    /// segment still listed" shifts as the item ages, and every
    /// position-based decision would shift with it. The numbering-backwards
    /// reset drops every base along with `decisions`, because a sequence
    /// number means nothing outside the space it was observed in.
    item_bases: HashMap<String, u64>,
    /// Names this session in decision logs. Empty (the default) stays silent:
    /// the media and subtitle playlists run the same decisions over the same
    /// sidecars, so only one of the pair carries a label, and each item's
    /// decision is reported once.
    label: String,
    /// Why the last compose walk stopped, quoted by the stall warning so a
    /// held playlist names its cause.
    last_halt: Option<ComposeHalt>,
    /// The newest listed shared sequence at the last walk, quoted alongside.
    last_newest_shared: Option<u64>,
    /// Ticks in a row that reset for backwards numbering. One reset is a
    /// legitimate shared restart; a run of them means the sidecar is
    /// alternating between numbering regimes, i.e. two writers.
    consecutive_resets: u32,
}

pub(crate) fn parse_pdt(input: &str) -> Option<OffsetDateTime> {
    let format = format_description!(
        "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3][offset_hour sign:mandatory][offset_minute]"
    );
    OffsetDateTime::parse(input, format).ok()
}

pub(crate) fn format_pdt(pdt: OffsetDateTime) -> String {
    let format = format_description!(
        "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3][offset_hour sign:mandatory][offset_minute]"
    );
    pdt.format(format).unwrap_or_default()
}

/// Why a templated item has no anchored variant, for the decision log. The
/// anchored check can fail several distinct ways and which one decides what
/// to investigate: a variant that never produced, a variant on the wrong
/// item, or a variant whose pts envelope starts before the shared one.
fn unanchored_reason(pipeline: &SidecarPipeline, variant: Option<&PlaylistSidecar>) -> String {
    let Some(v) = variant else {
        return String::from("no variant sidecar published");
    };
    if v.segments.is_empty() {
        return String::from("variant has produced no segments");
    }
    match v.pipelines.first() {
        None => String::from("variant sidecar lists no pipeline"),
        Some(vp) if vp.item_id != pipeline.item_id => {
            format!("variant is anchored to item {} instead", vp.item_id)
        }
        Some(vp) if vp.pts_offset_ms < pipeline.pts_offset_ms => format!(
            "variant pts_offset {}ms precedes the shared envelope's {}ms",
            vp.pts_offset_ms, pipeline.pts_offset_ms
        ),
        Some(_) => String::from("anchored variant appeared between checks"),
    }
}

/// The two positions the serve head is paced against.
struct ServeBounds {
    /// The newest position this timeline can serve a full window from, and
    /// the furthest a lag trim may force the head to. Composition stops
    /// [`COMPOSE_TRAIL_SECONDS`] behind the live edge on purpose, so the
    /// shared head normally sits past everything composed here; a target
    /// beyond this one is a position this session has declined to reach,
    /// and chasing it only starves the window.
    own_window: u64,
    /// Where the head walks to: the shared playlist's own serve position,
    /// capped at `own_window`. Mirroring the shared head stays the goal
    /// whenever it lands inside what this timeline can serve, so a cohort
    /// never runs ahead of shared into worked-ahead content.
    desired: u64,
}

/// Derives both bounds from the composed window, so they cannot drift apart.
///
/// Their coupling is load-bearing rather than incidental: the lag trim
/// declines to move the head exactly when `own_window <= head`, and because
/// `desired <= own_window` the playback-rate walk is then also out of room.
/// If a change let the trim stall while the walk still had somewhere to go,
/// the trim's clock reset would starve the walk of elapsed time and freeze
/// the head outright, which is the 2026-08-10 livelock that skipped roughly
/// a minute of program content. `a_stalled_trim_implies_a_stalled_walk`
/// pins it.
fn serve_bounds(front: u64, tail: u64, shared_head: Option<u64>) -> ServeBounds {
    let own_window = tail.saturating_sub(SERVED_SEGMENTS as u64 - 1).max(front);
    ServeBounds {
        own_window,
        desired: shared_head.map_or(own_window, |head| head.min(own_window)),
    }
}

/// The sequence number a segment file name carries: the digits before the
/// extension. The playlist manager numbers segments from zero and trims them
/// in order, so this index is also the segment's media sequence in the
/// shared playlist.
fn sequence_of(path: &str) -> Option<u64> {
    let name = path.rsplit('/').next()?;
    let stem = name.split('.').next()?;
    let digits: String = stem.chars().filter(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

impl SessionPlaylist {
    /// How this session names itself in decision logs. Empty when it is the
    /// silent half of a media/subtitle pair.
    pub fn label(&self) -> &str {
        &self.label
    }

    /// A playlist that names itself in decision logs.
    pub fn with_label(label: String) -> SessionPlaylist {
        SessionPlaylist {
            label,
            ..SessionPlaylist::default()
        }
    }

    /// Advances this session's timeline from the two sidecars and renders the
    /// playlist to serve. `variant_prefix` is the path prefix (relative to the
    /// shared session folder) under which the variant's segments are served.
    /// `shared_head` is the media sequence the shared playlist currently
    /// serves from, which the composed window mirrors when it can.
    /// `map_path` converts a segment path for the rendered playlist (identity
    /// for media, `.ts` to `.vtt` for subtitles).
    #[allow(clippy::too_many_arguments)]
    pub fn advance_and_render(
        &mut self,
        shared: &PlaylistSidecar,
        variant: Option<&PlaylistSidecar>,
        variant_prefix: &str,
        shared_head: Option<u64>,
        now: OffsetDateTime,
        target_duration: u32,
        map_path: fn(&str) -> String,
    ) -> String {
        self.decide_items(shared, variant, now);

        // a sidecar whose newest segment precedes everything held means the
        // shared session restarted and renumbered from zero; composed state
        // has to follow it rather than hold a stale history forever. judged
        // from the sidecar itself, never from the composed timeline: the
        // timeline truncates at a position whose variant twin is missing,
        // and with retention reaching behind the held front, a truncation
        // at a trimmed twin is indistinguishable from renumbering when read
        // off the timeline alone
        let newest_shared = shared
            .segments
            .iter()
            .rev()
            .find_map(|s| sequence_of(&s.path));
        if let (Some(newest), Some(front)) = (newest_shared, self.entries.front())
            && newest < front.sequence
        {
            self.consecutive_resets += 1;
            if !self.label.is_empty() {
                log::warn!(
                    "[{}] shared playlist numbering moved backwards (newest listed {} vs \
                     composed history {}..{}); resetting composed session{}",
                    self.label,
                    newest,
                    front.sequence,
                    self.entries.back().map_or(front.sequence, |e| e.sequence),
                    if self.consecutive_resets > 2 {
                        format!(
                            " ({} ticks in a row: the sidecar is alternating between \
                             numbering regimes, which means two writers)",
                            self.consecutive_resets
                        )
                    } else {
                        String::new()
                    }
                );
            }
            self.entries.clear();
            self.serve_head = None;
            self.head_advanced_at = None;
            self.head_discontinuity_sequence = 0;
            // per-item state dies with the numbering it was measured in. a
            // base is a sequence number from the old space: against the new
            // one, saturating_sub collapses every re-aired position to zero
            // and aliases the whole item onto a single twin. a Variant
            // decision's join and anchor are differences of pts offsets from
            // pipelines the restart discarded. the re-anchor in `reconcile`
            // keeps both maps on purpose: there the numbering space
            // continues and only history was trimmed, so the bases (recorded
            // once, exactly to survive trimming) are still true
            self.decisions.clear();
            self.item_bases.clear();
        } else {
            self.consecutive_resets = 0;
        }

        // held positions are append-only history: composition resumes at the
        // first position not yet held, so trimmed-away twins behind it are
        // never revisited
        let resume = ComposeResume {
            from_sequence: self.entries.back().map(|e| e.sequence + 1),
            substituting: self.entries.back().is_some_and(|e| e.variant),
        };
        let (timeline, halt) = compose_timeline_explained(
            shared,
            variant,
            variant_prefix,
            &self.label,
            &self.decisions,
            &self.item_bases,
            resume,
            now - time::Duration::seconds(COMPOSE_TRAIL_SECONDS as i64),
        );
        self.last_halt = Some(halt);
        self.last_newest_shared = newest_shared;
        self.reconcile(timeline);
        self.trim(now);
        self.render(shared_head, now, target_duration, map_path)
    }

    /// The entries the last render published: the served window starting at
    /// the serve head. Empty before the first render. This is exactly the
    /// set of segment references a viewer of this cohort can currently
    /// request, which makes it what the variant manager's served-window
    /// audit checks against disk.
    pub fn served_window(&self) -> impl Iterator<Item = &ComposedEntry> + '_ {
        let skip = self.serve_head.map_or(self.entries.len(), |head| {
            head.saturating_sub(self.head_sequence) as usize
        });
        self.entries.iter().skip(skip).take(SERVED_SEGMENTS)
    }

    /// Decides each templated item's fate. A variant whose recorded anchor
    /// lands inside the item wins from that offset onward: at the start when
    /// it was spawned with lead time, or as a late join when it wasn't. A
    /// `Shared` decision is only a soft pin: it upgrades to a late join the
    /// moment anchored variant output appears, because envelope equality
    /// makes the mid-item switch timestamp-continuous. The join is clamped
    /// past every position this session already served, so an upgrade never
    /// re-serves a position from the other source.
    fn decide_items(
        &mut self,
        shared: &PlaylistSidecar,
        variant: Option<&PlaylistSidecar>,
        now: OffsetDateTime,
    ) {
        let emitted: HashSet<u64> = self.entries.iter().map(|e| e.sequence).collect();

        for pipeline in shared.pipelines.iter().filter(|p| p.templated) {
            if !self.item_bases.contains_key(&pipeline.item_id)
                && let Some(first) = shared
                    .segments
                    .iter()
                    .find(|s| s.item_id == pipeline.item_id)
                    .and_then(|s| sequence_of(&s.path))
            {
                self.item_bases.insert(pipeline.item_id.clone(), first);
            }

            if let Some(ItemDecision::Variant { .. }) = self.decisions.get(&pipeline.item_id) {
                continue;
            }

            let anchored = variant.and_then(|v| {
                if v.segments.is_empty() {
                    return None;
                }
                v.pipelines
                    .first()
                    .filter(|vp| {
                        vp.item_id == pipeline.item_id && vp.pts_offset_ms >= pipeline.pts_offset_ms
                    })
                    .map(|vp| vp.pts_offset_ms - pipeline.pts_offset_ms)
            });

            if let Some(anchor_ms) = anchored {
                // the first position of the item this session has not served
                // yet; substitution must not start before it
                let base = self.item_bases.get(&pipeline.item_id).copied().unwrap_or(0);
                let mut first_unserved_ms = 0u64;
                // walked counts the positions of this item this session has
                // actually emitted. `first_unserved_ms` is a SPAN off `base`
                // rather than a count, so the two agree only while `base` is
                // the item's true first position. Where they disagree is the
                // whole question behind a large join, so both are reported
                let mut walked = 0u64;
                let mut last_emitted = None;
                for segment in shared
                    .segments
                    .iter()
                    .filter(|s| s.item_id == pipeline.item_id)
                {
                    let Some(seq) = sequence_of(&segment.path) else {
                        continue;
                    };
                    if !emitted.contains(&seq) {
                        break;
                    }
                    walked += 1;
                    last_emitted = Some(seq);
                    first_unserved_ms =
                        seq.saturating_sub(base).saturating_add(1) * 1000 * SEGMENT_SECONDS;
                }

                let join_ms = anchor_ms.max(first_unserved_ms);
                if !self.label.is_empty() {
                    let upgraded = matches!(
                        self.decisions.get(&pipeline.item_id),
                        Some(ItemDecision::Shared)
                    );
                    // "late join" means the viewer LOST content, so it has to
                    // depend on the join position and not merely on there
                    // having been a soft pin. A pin that upgrades at join 0
                    // cost nothing: the substitution still starts at the
                    // item's first frame. Reporting those as late joins
                    // inflated every count taken off this line, which is how
                    // ten of the nineteen "late joins" on 2026-08-11 came to
                    // be events where nothing was actually served from shared.
                    let how = match (upgraded, join_ms > 0) {
                        (true, true) => " as a late join after a shared pin",
                        (true, false) => " after a shared pin",
                        (false, _) => "",
                    };
                    log::info!(
                        "[{}] item {}: serving variant{how} (anchor {}ms, join {}ms)",
                        self.label,
                        pipeline.item_id,
                        anchor_ms,
                        join_ms,
                    );

                    // A non-zero join costs the viewer that much of the item,
                    // and two different defects produce one. Reporting the
                    // arithmetic separates them on the first occurrence
                    // instead of after another day of guessing:
                    //
                    //   walked == join_ms / SEGMENT  the session really did
                    //     emit that many positions of this item, so the span
                    //     is honest and the cause is upstream, in whatever let
                    //     composition run that far past the trailing edge
                    //   walked <  join_ms / SEGMENT  the span is inflated
                    //     because `base` is not this airing's first position,
                    //     so the join is fictional and the cause is the base
                    //
                    // `held` bounds both: a join beyond the item's own length
                    // cannot be a position within it at all.
                    if join_ms > 0 {
                        let held = shared
                            .segments
                            .iter()
                            .filter(|s| s.item_id == pipeline.item_id)
                            .count();
                        log::info!(
                            "[{}] item {}: join arithmetic: base {}, last emitted {}, \
                             walked {} position(s), {} held in the sidecar, \
                             span {}ms vs walked {}ms",
                            self.label,
                            pipeline.item_id,
                            base,
                            last_emitted.map_or_else(|| String::from("none"), |s| s.to_string()),
                            walked,
                            held,
                            first_unserved_ms,
                            walked * 1000 * SEGMENT_SECONDS,
                        );
                    }
                }
                self.decisions.insert(
                    pipeline.item_id.clone(),
                    ItemDecision::Variant { join_ms, anchor_ms },
                );
                continue;
            }

            if self.decisions.contains_key(&pipeline.item_id) {
                // already soft-pinned shared; keep waiting for a late join
                continue;
            }

            // the item's first segment (the first shared segment recorded for
            // it) tells us when the item reaches viewers
            let first_shared = shared
                .segments
                .iter()
                .find(|s| s.item_id == pipeline.item_id);

            let Some(first_segment) = first_shared else {
                // no shared output for the item yet; nothing to serve either
                // way, so wait
                continue;
            };

            let Some(item_start) = parse_pdt(&first_segment.program_date_time) else {
                // a segment exists but its program date time cannot be read,
                // so the deadline cannot arm and the item holds with no
                // timer at all: the one state that must never pass silently
                if !self.label.is_empty() {
                    log::warn!(
                        "[{}] item {}: shared segment {} carries unparseable \
                         program_date_time {:?}; the decision deadline cannot arm \
                         and composition holds at this item until it parses",
                        self.label,
                        pipeline.item_id,
                        first_segment.path,
                        first_segment.program_date_time,
                    );
                }
                continue;
            };

            // hold the decision open while there is still time for the
            // variant to produce output. viewers play a full serve window
            // behind the live edge, so the decision can stay open until
            // well AFTER the item's first segment pdt and still be pinned
            // before any viewer's playlist reaches the boundary
            let deadline = item_start + time::Duration::seconds(DECISION_BUDGET_SECONDS as i64);
            if now >= deadline {
                if !self.label.is_empty() {
                    log::warn!(
                        "[{}] item {}: no anchored variant by the decision deadline, \
                         serving shared until one appears: {}{}",
                        self.label,
                        pipeline.item_id,
                        unanchored_reason(pipeline, variant),
                        if pipeline.fallback {
                            " (shared is slate)"
                        } else {
                            ""
                        },
                    );
                }
                self.decisions
                    .insert(pipeline.item_id.clone(), ItemDecision::Shared);
            }
        }
    }

    /// Appends timeline entries this session has not emitted yet. Entries are
    /// identified by sequence; history is never reordered or rewritten, so
    /// every client sees an append-only playlist.
    fn reconcile(&mut self, timeline: Vec<ComposedEntry>) {
        // a sequence this session still needs but the timeline can no longer
        // provide will never arrive; re-anchoring to current content is one
        // clean break for the viewer, where holding would freeze the
        // playlist for good
        if let (Some(oldest), Some(last)) = (timeline.first(), self.entries.back())
            && oldest.sequence > last.sequence + 1
        {
            if !self.label.is_empty() {
                log::warn!(
                    "[{}] sequence {} is no longer available (timeline now starts at {}); \
                     re-anchoring composed session",
                    self.label,
                    last.sequence + 1,
                    oldest.sequence
                );
            }
            self.entries.clear();
            self.serve_head = None;
            self.head_advanced_at = None;
            self.head_discontinuity_sequence = 0;
        }

        let mut regressed: Option<(u64, u64, u64)> = None;
        for entry in timeline {
            match self.entries.back() {
                None => {
                    self.head_sequence = entry.sequence;
                    self.entries.push_back(entry);
                }
                Some(last) if entry.sequence == last.sequence + 1 => {
                    self.entries.push_back(entry);
                }
                Some(last) if entry.sequence > last.sequence + 1 => {
                    // composition emits positions in order, so a gap means an
                    // upstream skip; appending across it would misnumber every
                    // later position, so hold here until it is filled
                    if !self.label.is_empty() {
                        log::warn!(
                            "[{}] composed timeline skipped sequence {} to {}; holding",
                            self.label,
                            last.sequence + 1,
                            entry.sequence
                        );
                    }
                    break;
                }
                // an already-emitted position (or a conflicting twin of one)
                // never re-enters history. positions below the FRONT are a
                // different animal: nothing this session ever held, numbered
                // before its history begins, which means the shared numbering
                // space regressed underneath it (a second writer). counted
                // and reported, because this discard used to be the silent
                // sink that made the dual-writer incident invisible
                _ => {
                    if entry.sequence < self.head_sequence {
                        regressed = Some(match regressed {
                            None => (1, entry.sequence, entry.sequence),
                            Some((n, lo, hi)) => {
                                (n + 1, lo.min(entry.sequence), hi.max(entry.sequence))
                            }
                        });
                    }
                }
            }
        }

        if let Some((count, lo, hi)) = regressed
            && !self.label.is_empty()
        {
            log::warn!(
                "[{}] discarded {count} timeline entries numbered {lo}..{hi}, below \
                 composed history starting at {}: the shared numbering space regressed \
                 (a second writer, or a shared session restart mid-composition)",
                self.label,
                self.head_sequence
            );
        }
    }

    fn trim(&mut self, now: OffsetDateTime) {
        let cutoff = now - time::Duration::seconds(HISTORY_SECONDS as i64);
        while self
            .entries
            .front()
            .is_some_and(|e| e.program_date_time < cutoff)
        {
            // a lagging serve head still needs its window; history behind the
            // head is expendable, the head itself is not
            if self.serve_head.is_some_and(|h| self.head_sequence >= h) {
                break;
            }

            if let Some(removed) = self.entries.pop_front() {
                self.head_sequence += 1;
                if removed.discontinuity {
                    self.head_discontinuity_sequence += 1;
                }
            }
        }
    }

    fn render(
        &mut self,
        shared_head: Option<u64>,
        now: OffsetDateTime,
        target_duration: u32,
        map_path: fn(&str) -> String,
    ) -> String {
        let mut playlist = String::new();
        playlist.push_str("#EXTM3U\n");
        // the composed playlist follows the shared playlist's declarations:
        // version 6 for the rounded EXT-X-TARGETDURATION semantics, and the
        // discontinuity sequence present even at zero
        playlist.push_str("#EXT-X-VERSION:6\n");
        playlist.push_str(&format!("#EXT-X-TARGETDURATION:{target_duration}\n"));

        if self.entries.is_empty() {
            let held = self.serve_head.unwrap_or(0);
            playlist.push_str(&format!("#EXT-X-MEDIA-SEQUENCE:{held}\n"));
            playlist.push_str(&format!(
                "#EXT-X-DISCONTINUITY-SEQUENCE:{}\n",
                self.head_discontinuity_sequence
            ));
            playlist.push_str("#EXT-X-INDEPENDENT-SEGMENTS\n");
            return playlist;
        }

        let front = self.head_sequence;
        let tail = front + self.entries.len() as u64 - 1;

        let ServeBounds {
            own_window,
            desired,
        } = serve_bounds(front, tail, shared_head);

        let mut head = match self.serve_head {
            Some(head) => head.clamp(front, tail),
            None => {
                self.head_advanced_at = Some(now);
                desired.clamp(front, tail)
            }
        };

        // walk forward at playback rate, one segment per segment duration,
        // never past the shared head or this timeline's own last full
        // window: a lagging cohort plays through its backlog instead of
        // having the window jump over it, and a cohort never runs ahead of
        // the shared playlist into worked-ahead content
        let upper = desired.max(head).min(tail);
        if let Some(mut advanced_at) = self.head_advanced_at {
            while head < upper {
                let index = (head - front) as usize;
                let step = time::Duration::seconds_f64(self.entries[index].duration.max(0.1));
                if now - advanced_at < step {
                    break;
                }
                head += 1;
                advanced_at += step;
            }

            // a head that has reached its target banks no credit for the
            // wait. composition holds at a missing variant twin for as long
            // as the twin is missing, and unspent credit would be paid out
            // as one silent jump across everything the hold withheld, which
            // is content, not lag
            let parked = time::Duration::seconds(SEGMENT_SECONDS as i64);
            if head >= upper && now - advanced_at > parked {
                advanced_at = now - parked;
            }
            self.head_advanced_at = Some(advanced_at);
        }

        // a head behind the shared one stays behind: every unplayed entry is
        // content, and a jump anywhere else would eat the middle of it. past
        // the soft bound, excess lag is trimmed only onto an item boundary;
        // past the hard bound, wherever the head lands
        let gap = shared_head.unwrap_or(head).saturating_sub(head);
        if gap > HARD_LAG_SEGMENTS {
            // the furthest position this timeline can serve a window from.
            // when composition is held that is the head itself, and no
            // amount of lag can move it: say so instead of reporting a skip
            // that did not happen
            let target = own_window.max(head);
            if target > head {
                if !self.label.is_empty() {
                    log::warn!(
                        "[{}] composed serve head {head} fell {gap} segments behind \
                         shared head {}; skipping to {target}",
                        self.label,
                        head + gap
                    );
                }
                head = target;
                self.head_advanced_at = Some(now);
                self.lag_stalled_warned_at = None;
            } else if self
                .lag_stalled_warned_at
                .is_none_or(|at| now - at >= time::Duration::seconds(STALL_WARN_SECONDS))
            {
                if !self.label.is_empty() {
                    log::warn!(
                        "[{}] composed serve head {head} is {gap} segments behind shared \
                         head {} and cannot advance: composition is held at {tail} \
                         because {} (history {front}..{tail}, {} entries, newest listed \
                         shared {}). the shared session deletes segments this window \
                         still lists once the lag reaches its retention window",
                        self.label,
                        head + gap,
                        self.last_halt
                            .as_ref()
                            .map_or_else(|| String::from("of an unknown cause"), |h| h.to_string()),
                        self.entries.len(),
                        self.last_newest_shared
                            .map_or_else(|| String::from("none"), |n| n.to_string()),
                    );
                }
                self.lag_stalled_warned_at = Some(now);
            }
        } else if gap > MAX_LAG_SEGMENTS {
            let limit = own_window;
            let mut boundary = None;
            // the NEAREST boundary past the head, so the viewer loses the
            // tail of the item they were behind in and nothing more. Every
            // position between the head and the target goes unserved, so a
            // variant position in that range is substituted content the
            // cohort would lose outright: stop rather than cross it, and let
            // the playback-rate walk carry the head through instead
            for entry in self
                .entries
                .iter()
                .skip((head - front) as usize)
                .take_while(|e| e.sequence <= limit)
            {
                if entry.sequence > head && entry.discontinuity {
                    boundary = Some(entry.sequence);
                    break;
                }
                if entry.variant {
                    break;
                }
            }

            if let Some(target) = boundary {
                if !self.label.is_empty() {
                    log::info!(
                        "[{}] composed serve head {head} trimmed {} segments of lag to \
                         item boundary {target}",
                        self.label,
                        target - head
                    );
                }
                head = target;
                self.head_advanced_at = Some(now);
            }
            self.lag_stalled_warned_at = None;
        } else {
            self.lag_stalled_warned_at = None;
        }

        self.serve_head = Some(head);

        let skip = (head - front) as usize;
        let effective_ds = self.head_discontinuity_sequence
            + self
                .entries
                .iter()
                .take(skip)
                .filter(|e| e.discontinuity)
                .count() as u64;

        playlist.push_str(&format!("#EXT-X-MEDIA-SEQUENCE:{head}\n"));
        playlist.push_str(&format!("#EXT-X-DISCONTINUITY-SEQUENCE:{effective_ds}\n"));
        playlist.push_str("#EXT-X-INDEPENDENT-SEGMENTS\n");

        for entry in self.entries.iter().skip(skip).take(SERVED_SEGMENTS) {
            if entry.discontinuity {
                playlist.push_str("#EXT-X-DISCONTINUITY\n");
            }
            playlist.push_str(&format!("#EXTINF:{:.6},\n", entry.duration));
            playlist.push_str(&format!(
                "#EXT-X-PROGRAM-DATE-TIME:{}\n",
                format_pdt(entry.program_date_time)
            ));
            playlist.push_str(&format!("{}\n", map_path(&entry.path)));
        }

        playlist
    }
}

/// After this much shared coverage exists beyond the variant's produced edge,
/// the missing positions fall back to shared segments instead of holding, so
/// a stalled variant degrades to the shared feed rather than stalling the
/// cohort's playlist. Envelope equality keeps the per-position fallback
/// timestamp-continuous.
const VARIANT_STALL_SECONDS: f64 = 16.0;

/// Builds the cohort's timeline from the two sidecars: shared segments pass
/// through, and each item decided `Variant { .. }` has its shared segments
/// from the join onward replaced position-for-position by the variant's.
/// Both transcodes occupy the same PTS envelope and segment grid, so the
/// substituted entries reuse the shared segment's program date time and
/// sequence number.
///
/// The variant's segments are indexed from the anchor, not the join: a
/// position between the two consumes its variant segment unserved, so a
/// join raised past already-served positions still substitutes the content
/// that belongs at each remaining position. A position the variant skips
/// while stalled is likewise consumed, never shifted later.
///
/// Segments of an undecided templated item are held back; a position the
/// variant has not produced yet holds the timeline at that edge (up to the
/// stall threshold) so the timeline never emits around a hole.
/// Where composition resumes for a session that has already emitted part of
/// the timeline. `from_sequence` is the first position the session does not
/// hold yet: everything earlier is append-only history, and its variant
/// twins may already be trimmed from the variant's own sidecar, so walking
/// it again would read that absence as a hole and truncate. `substituting`
/// seeds the source state at the resume point (whether the last held entry
/// served variant content), so the discontinuity tag at the join comes out
/// exactly as a continuous walk would have tagged it.
#[derive(Debug, Clone, Copy, Default)]
pub struct ComposeResume {
    pub from_sequence: Option<u64>,
    pub substituting: bool,
}

/// Why the compose walk stopped where it did. The serve-side stall warning
/// quotes this, because "composition is held at {tail}" without the cause
/// cost a morning of log archaeology during the 2026-08-11 incident: four
/// distinct stop conditions rendered identically.
#[derive(Debug, Clone, PartialEq)]
pub enum ComposeHalt {
    /// The walk consumed every listed shared segment inside the horizon.
    /// Normal when composition is caught up; with a large serve gap it means
    /// the sidecar's listed numbering no longer reaches past composed
    /// history.
    Exhausted,
    /// The next position's program date time is past the horizon: the
    /// normal trailing edge.
    Horizon,
    /// A templated position with no decision, or one decided `Variant`
    /// whose first segment number was never observed. Held with no timer.
    Undecided {
        item_id: String,
        decided: bool,
        has_base: bool,
    },
    /// A decided `Variant` position whose twin the variant has not produced,
    /// still under the stall threshold.
    MissingTwin {
        item_id: String,
        twin: u64,
        sequence: u64,
        behind_ms: u64,
        shared_edge: u64,
    },
}

impl std::fmt::Display for ComposeHalt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComposeHalt::Exhausted => {
                write!(
                    f,
                    "every listed shared segment inside the horizon is consumed"
                )
            }
            ComposeHalt::Horizon => write!(f, "the trailing-edge horizon (normal)"),
            ComposeHalt::Undecided {
                item_id,
                decided: false,
                ..
            } => write!(f, "item {item_id} is undecided"),
            ComposeHalt::Undecided { item_id, .. } => write!(
                f,
                "item {item_id} is decided variant but its first segment number \
                 was never observed"
            ),
            ComposeHalt::MissingTwin {
                item_id,
                twin,
                sequence,
                behind_ms,
                shared_edge,
            } => write!(
                f,
                "item {item_id} is missing variant twin {twin} at position {sequence} \
                 ({behind_ms}ms of shared coverage past it, stall threshold \
                 {}ms, shared edge {shared_edge})",
                (VARIANT_STALL_SECONDS * 1000.0) as u64
            ),
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn compose_timeline(
    shared: &PlaylistSidecar,
    variant: Option<&PlaylistSidecar>,
    variant_prefix: &str,
    label: &str,
    decisions: &HashMap<String, ItemDecision>,
    item_bases: &HashMap<String, u64>,
    resume: ComposeResume,
    horizon: OffsetDateTime,
) -> Vec<ComposedEntry> {
    compose_timeline_explained(
        shared,
        variant,
        variant_prefix,
        label,
        decisions,
        item_bases,
        resume,
        horizon,
    )
    .0
}

#[allow(clippy::too_many_arguments)]
pub fn compose_timeline_explained(
    shared: &PlaylistSidecar,
    variant: Option<&PlaylistSidecar>,
    variant_prefix: &str,
    label: &str,
    decisions: &HashMap<String, ItemDecision>,
    item_bases: &HashMap<String, u64>,
    resume: ComposeResume,
    horizon: OffsetDateTime,
) -> (Vec<ComposedEntry>, ComposeHalt) {
    let templated: HashMap<&str, bool> = shared
        .pipelines
        .iter()
        .map(|p| (p.item_id.as_str(), p.templated))
        .collect();

    let grid_ms = 1000 * SEGMENT_SECONDS;
    let mut result: Vec<ComposedEntry> = Vec::new();
    let mut substituting = resume.substituting;
    let mut halt = ComposeHalt::Exhausted;

    // the newest shared position inside the horizon. a position held for a
    // missing variant twin measures how much shared coverage has piled up
    // past it against this, which is the only quantity in the walk that
    // keeps growing while a variant is stalled
    let shared_edge = shared
        .segments
        .iter()
        .filter(|s| parse_pdt(&s.program_date_time).is_some_and(|pdt| pdt <= horizon))
        .filter_map(|s| sequence_of(&s.path))
        .max()
        .unwrap_or(0);

    for segment in &shared.segments {
        let Some(pdt) = parse_pdt(&segment.program_date_time) else {
            continue;
        };

        // stop at the trailing edge rather than following production. the
        // timeline is append-only and ordered, so this defers these
        // positions to a later tick instead of dropping them
        if pdt > horizon {
            halt = ComposeHalt::Horizon;
            break;
        }
        let Some(sequence) = sequence_of(&segment.path) else {
            continue;
        };

        // history the session already holds; skipped without touching the
        // substitution state, which `resume` seeded for this very position
        if resume.from_sequence.is_some_and(|from| sequence < from) {
            continue;
        }

        let is_templated = templated.get(segment.item_id.as_str()).copied() == Some(true);

        if is_templated {
            // the position comes from the segment's own number against the
            // item's recorded first number, so it does not shift as the
            // sidecar trims the item's early segments out of its history
            let position_ms = item_bases
                .get(&segment.item_id)
                .map(|base| sequence.saturating_sub(*base) * grid_ms);

            match (decisions.get(&segment.item_id), position_ms) {
                (Some(ItemDecision::Variant { join_ms, anchor_ms }), Some(position_ms)) => {
                    // this position's variant twin, numbered on the grid from
                    // the anchor whether or not the twin is served
                    let has_twin = position_ms + 750 >= *anchor_ms;

                    if has_twin && position_ms + 750 >= *join_ms {
                        let twin = (position_ms + 750 - *anchor_ms) / grid_ms;
                        let vseg = variant.and_then(|v| {
                            v.segments
                                .iter()
                                .find(|s| sequence_of(&s.path) == Some(twin))
                        });

                        if let Some(vseg) = vseg {
                            result.push(ComposedEntry {
                                path: format!("{variant_prefix}{}", vseg.path),
                                duration: vseg.duration,
                                program_date_time: pdt,
                                discontinuity: !substituting
                                    || vseg.discontinuity
                                    || segment.discontinuity,
                                sequence,
                                variant: true,
                            });
                            substituting = true;
                            continue;
                        }

                        // the variant has not produced this position yet:
                        // hold the timeline here unless it has fallen so far
                        // behind that holding would stall viewers.
                        //
                        // measured as shared coverage past the hole, which is
                        // what the threshold is written against and what
                        // grows while a variant is stalled.
                        //
                        // NOT from `twin`: `twin` is this position's own grid
                        // slot, so `anchor_ms + twin * grid_ms` differs from
                        // `position_ms + grid_ms` by less than two segments
                        // for every anchor and every position. The threshold
                        // was never reachable and a stalled variant held the
                        // cohort's timeline for good
                        let behind_ms = shared_edge.saturating_sub(sequence) * grid_ms;
                        if (behind_ms as f64) < VARIANT_STALL_SECONDS * 1000.0 {
                            return (
                                result,
                                ComposeHalt::MissingTwin {
                                    item_id: segment.item_id.clone(),
                                    twin,
                                    sequence,
                                    behind_ms,
                                    shared_edge,
                                },
                            );
                        }

                        if !label.is_empty() {
                            log::info!(
                                "[{}] variant for item {} has no segment {} and shared \
                                 coverage runs {}ms past it; serving shared position {}",
                                label,
                                segment.item_id,
                                twin,
                                behind_ms,
                                position_ms
                            );
                        }
                        // fall through to emit the shared segment, marking the
                        // source switch for players that honor discontinuities.
                        // the variant's segment for this position, if it ever
                        // arrives, stays unserved; later positions keep their
                        // own twins
                        result.push(ComposedEntry {
                            path: segment.path.clone(),
                            duration: segment.duration,
                            program_date_time: pdt,
                            discontinuity: substituting || segment.discontinuity,
                            sequence,
                            variant: false,
                        });
                        substituting = false;
                        continue;
                    }
                    // before the join: shared passes through below
                }
                (Some(ItemDecision::Shared), _) => {
                    // fall through: the shared segment passes into the
                    // timeline like any other
                }
                _ => {
                    // undecided, or an item whose base was never observed:
                    // hold everything from here on back
                    return (
                        result,
                        ComposeHalt::Undecided {
                            item_id: segment.item_id.clone(),
                            decided: decisions.contains_key(&segment.item_id),
                            has_base: item_bases.contains_key(&segment.item_id),
                        },
                    );
                }
            }
        } else {
            substituting = false;
        }

        result.push(ComposedEntry {
            path: segment.path.clone(),
            duration: segment.duration,
            program_date_time: pdt,
            discontinuity: segment.discontinuity,
            sequence,
            variant: false,
        });
    }

    (result, halt)
}

#[cfg(test)]
mod tests {
    use ersatztv_core::sidecar::{SidecarPipeline, SidecarSegment};

    use super::*;

    /// A horizon far past every fixture, for the tests that exercise what
    /// composition decides rather than how far it runs.
    const NO_HORIZON: OffsetDateTime =
        OffsetDateTime::UNIX_EPOCH.saturating_add(time::Duration::days(1));

    fn seg(path: &str, item: &str, offset_secs: i64, discontinuity: bool) -> SidecarSegment {
        SidecarSegment {
            path: path.to_owned(),
            duration: 4.0,
            program_date_time: format_pdt(
                OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(offset_secs),
            ),
            item_id: item.to_owned(),
            discontinuity,
        }
    }

    fn pipeline(item: &str, offset_ms: u64, templated: bool) -> SidecarPipeline {
        SidecarPipeline {
            item_id: item.to_owned(),
            pts_offset_ms: offset_ms,
            duration_ms: 0,
            templated,
            fallback: false,
        }
    }

    fn variant_decision(join_ms: u64, anchor_ms: u64) -> ItemDecision {
        ItemDecision::Variant { join_ms, anchor_ms }
    }

    fn shared_with_templated_item() -> PlaylistSidecar {
        PlaylistSidecar {
            segments: vec![
                seg("live000000.ts", "before", 0, false),
                seg("live000001.ts", "before", 4, false),
                seg("live000002.ts", "game", 8, true),
                seg("live000003.ts", "game", 12, false),
                seg("live000004.ts", "after", 16, true),
            ],
            pipelines: vec![
                pipeline("before", 0, false),
                pipeline("game", 8_000, true),
                pipeline("after", 16_000, false),
            ],
        }
    }

    fn variant_for_game() -> PlaylistSidecar {
        PlaylistSidecar {
            segments: vec![
                seg("live000000.ts", "game", 0, true),
                seg("live000001.ts", "game", 4, false),
            ],
            pipelines: vec![pipeline("game", 8_000, true)],
        }
    }

    /// A channel producing continuously, with a templated item in the middle
    /// whose variant never appears. Long enough that the served window always
    /// has content ahead of it, as a working channel does.
    /// A shared timeline long enough for the serve head to sit a full
    /// window behind the composed edge: anything shorter than
    /// `SERVED_SEGMENTS` pins the head to the front of the timeline, where
    /// no walk and no lag bound can be observed.
    fn long_shared_with_templated_item(count: i64) -> PlaylistSidecar {
        let segments = (0..count)
            .map(|i| {
                let at = i * 4;
                let item = match at {
                    at if at < 24 => "before",
                    at if at < 44 => "game",
                    at if at < 80 => "after",
                    _ => "encore",
                };
                seg(
                    &format!("live{i:06}.ts"),
                    item,
                    at,
                    at == 0 || at == 24 || at == 44 || at == 80,
                )
            })
            .collect();

        PlaylistSidecar {
            segments,
            pipelines: vec![
                pipeline("before", 0, false),
                pipeline("game", 24_000, true),
                pipeline("after", 44_000, false),
                pipeline("encore", 80_000, false),
            ],
        }
    }

    /// A variant anchored on `long_shared_with_templated_item`'s game item.
    fn long_variant_for_game(count: i64) -> PlaylistSidecar {
        PlaylistSidecar {
            segments: (0..count)
                .map(|i| seg(&format!("live{i:06}.ts"), "game", 24 + i * 4, i == 0))
                .collect(),
            pipelines: vec![pipeline("game", 24_000, true)],
        }
    }

    /// A ch15-shaped window: a 140000ms templated item, which is the envelope
    /// logged for item 12216957 on 2026-08-12, inside a channel producing
    /// either side of it. The item occupies sequences 6 through 40.
    fn shared_with_long_templated_item() -> PlaylistSidecar {
        let segments = (0..60i64)
            .map(|i| {
                let at = i * 4;
                let item = match at {
                    at if at < 24 => "before",
                    at if at < 164 => "game",
                    _ => "after",
                };
                seg(
                    &format!("live{i:06}.ts"),
                    item,
                    at,
                    at == 0 || at == 24 || at == 164,
                )
            })
            .collect();

        PlaylistSidecar {
            segments,
            pipelines: vec![
                pipeline("before", 0, false),
                pipeline("game", 24_000, true),
                pipeline("after", 164_000, false),
            ],
        }
    }

    /// A variant for that window that has produced `count` segments, spawned
    /// with `progress_ms`. The pts offset it records is the shared item's
    /// offset plus that progress, which is what the worker stamps, so this is
    /// also what sets `anchor_ms` when the composer reads it back.
    fn long_variant(count: i64, progress_ms: u64) -> PlaylistSidecar {
        let start = 24 + (progress_ms / 1000) as i64;
        PlaylistSidecar {
            segments: (0..count)
                .map(|i| seg(&format!("live{i:06}.ts"), "game", start + i * 4, i == 0))
                .collect(),
            pipelines: vec![pipeline("game", 24_000 + progress_ms, true)],
        }
    }

    fn continuous_shared_with_templated_item() -> PlaylistSidecar {
        let segments = (0..16i64)
            .map(|i| {
                let at = i * 4;
                let item = match at {
                    at if at < 24 => "before",
                    at if at < 44 => "game",
                    _ => "after",
                };
                seg(
                    &format!("live{i:06}.ts"),
                    item,
                    at,
                    at == 0 || at == 24 || at == 44,
                )
            })
            .collect();

        PlaylistSidecar {
            segments,
            pipelines: vec![
                pipeline("before", 0, false),
                pipeline("game", 24_000, true),
                pipeline("after", 44_000, false),
            ],
        }
    }

    /// Composition advances on a timer rather than per request, so it is
    /// sampled at whatever cadence the loop runs. Two samplers walking the
    /// same span must leave a client at the same place, including across the
    /// hold-back while a templated item waits for its decision.
    #[test]
    fn render_cadence_does_not_change_what_is_served() {
        let shared = continuous_shared_with_templated_item();
        let base = OffsetDateTime::UNIX_EPOCH;

        let mut fine = SessionPlaylist::default();
        let mut coarse = SessionPlaylist::default();

        for step in 0..=20i64 {
            let elapsed = 20 + step * 2;
            let now = base + time::Duration::seconds(elapsed);

            let from_fine =
                fine.advance_and_render(&shared, None, "variants/x/", None, now, 4, |s| {
                    s.to_owned()
                });

            // the coarse sampler skips three quarters of those instants, as a
            // client polling once per segment would
            if step % 4 == 0 {
                let from_coarse =
                    coarse.advance_and_render(&shared, None, "variants/x/", None, now, 4, |s| {
                        s.to_owned()
                    });

                assert_eq!(
                    from_fine, from_coarse,
                    "cadence changed the served playlist at {elapsed}s"
                );
            }
        }
    }

    fn decided(item: &str, decision: ItemDecision) -> HashMap<String, ItemDecision> {
        let mut map = HashMap::new();
        map.insert(item.to_owned(), decision);
        map
    }

    /// The item bases a session would have recorded from this sidecar.
    fn bases_of(shared: &PlaylistSidecar) -> HashMap<String, u64> {
        let mut map = HashMap::new();
        for pipeline in shared.pipelines.iter().filter(|p| p.templated) {
            if let Some(first) = shared
                .segments
                .iter()
                .find(|s| s.item_id == pipeline.item_id)
                .and_then(|s| sequence_of(&s.path))
            {
                map.insert(pipeline.item_id.clone(), first);
            }
        }
        map
    }

    /// Four stop conditions used to render identically as "composition is
    /// held at {tail}". The halt names them apart; these pin each name to
    /// its state so the stall warning stays trustworthy.
    #[test]
    fn an_undecided_item_names_itself_in_the_halt() {
        let shared = shared_with_templated_item();

        let (timeline, halt) = compose_timeline_explained(
            &shared,
            None,
            "variants/abc/",
            "",
            &HashMap::new(),
            &HashMap::new(),
            ComposeResume::default(),
            NO_HORIZON,
        );

        assert_eq!(timeline.len(), 2, "composition holds at the undecided item");
        assert_eq!(
            halt,
            ComposeHalt::Undecided {
                item_id: String::from("game"),
                decided: false,
                has_base: false,
            }
        );
    }

    #[test]
    fn a_variant_decision_with_no_observed_base_says_so_in_the_halt() {
        let shared = shared_with_templated_item();

        let (_, halt) = compose_timeline_explained(
            &shared,
            Some(&variant_for_game()),
            "variants/abc/",
            "",
            &decided("game", variant_decision(0, 0)),
            &HashMap::new(),
            ComposeResume::default(),
            NO_HORIZON,
        );

        assert_eq!(
            halt,
            ComposeHalt::Undecided {
                item_id: String::from("game"),
                decided: true,
                has_base: false,
            }
        );
    }

    #[test]
    fn a_missing_twin_under_the_stall_threshold_reports_its_arithmetic() {
        let shared = shared_with_templated_item();
        let empty_variant = PlaylistSidecar {
            segments: vec![],
            pipelines: vec![pipeline("game", 8_000, true)],
        };

        let (timeline, halt) = compose_timeline_explained(
            &shared,
            Some(&empty_variant),
            "variants/abc/",
            "",
            &decided("game", variant_decision(0, 0)),
            &bases_of(&shared),
            ComposeResume::default(),
            NO_HORIZON,
        );

        assert_eq!(timeline.len(), 2, "composition holds at the missing twin");
        assert_eq!(
            halt,
            ComposeHalt::MissingTwin {
                item_id: String::from("game"),
                twin: 0,
                sequence: 2,
                // the newest listed position is 4, the hole is at 2
                behind_ms: 2 * 1000 * SEGMENT_SECONDS,
                shared_edge: 4,
            }
        );
    }

    #[test]
    fn a_caught_up_walk_reports_the_horizon() {
        let shared = shared_with_templated_item();

        let (timeline, halt) = compose_timeline_explained(
            &shared,
            None,
            "variants/abc/",
            "",
            &HashMap::new(),
            &HashMap::new(),
            ComposeResume::default(),
            // between the first and second segments' program date times
            OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(2),
        );

        assert_eq!(timeline.len(), 1);
        assert_eq!(halt, ComposeHalt::Horizon);
    }

    #[test]
    fn a_consumed_sidecar_reports_exhaustion() {
        let shared = PlaylistSidecar {
            segments: vec![
                seg("live000000.ts", "before", 0, false),
                seg("live000001.ts", "before", 4, false),
            ],
            pipelines: vec![pipeline("before", 0, false)],
        };

        let (timeline, halt) = compose_timeline_explained(
            &shared,
            None,
            "variants/abc/",
            "",
            &HashMap::new(),
            &HashMap::new(),
            ComposeResume::default(),
            NO_HORIZON,
        );

        assert_eq!(timeline.len(), 2);
        assert_eq!(halt, ComposeHalt::Exhausted);
    }

    #[test]
    fn substitutes_variant_segments_for_the_templated_item() {
        let shared = shared_with_templated_item();
        let variant = variant_for_game();

        let timeline = compose_timeline(
            &shared,
            Some(&variant),
            "variants/abc/",
            "",
            &decided("game", variant_decision(0, 0)),
            &bases_of(&shared),
            ComposeResume::default(),
            NO_HORIZON,
        );

        let paths: Vec<&str> = timeline.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(
            paths,
            vec![
                "live000000.ts",
                "live000001.ts",
                "variants/abc/live000000.ts",
                "variants/abc/live000001.ts",
                "live000004.ts",
            ]
        );
    }

    #[test]
    fn substituted_entries_keep_the_shared_sequence_numbers() {
        let shared = shared_with_templated_item();
        let variant = variant_for_game();

        let timeline = compose_timeline(
            &shared,
            Some(&variant),
            "variants/abc/",
            "",
            &decided("game", variant_decision(0, 0)),
            &bases_of(&shared),
            ComposeResume::default(),
            NO_HORIZON,
        );

        let sequences: Vec<u64> = timeline.iter().map(|e| e.sequence).collect();
        assert_eq!(sequences, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn variant_segments_take_pdt_from_the_item_start() {
        let shared = shared_with_templated_item();
        let variant = variant_for_game();

        let timeline = compose_timeline(
            &shared,
            Some(&variant),
            "variants/abc/",
            "",
            &decided("game", variant_decision(0, 0)),
            &bases_of(&shared),
            ComposeResume::default(),
            NO_HORIZON,
        );

        let expected_first = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(8);
        let expected_second = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(12);
        assert_eq!(timeline[2].program_date_time, expected_first);
        assert_eq!(timeline[3].program_date_time, expected_second);
    }

    #[test]
    fn splice_points_carry_discontinuities() {
        let shared = shared_with_templated_item();
        let variant = variant_for_game();

        let timeline = compose_timeline(
            &shared,
            Some(&variant),
            "variants/abc/",
            "",
            &decided("game", variant_decision(0, 0)),
            &bases_of(&shared),
            ComposeResume::default(),
            NO_HORIZON,
        );

        // splice in: first variant segment; splice out: first segment of the
        // next item (already a discontinuity in the shared playlist)
        assert!(timeline[2].discontinuity);
        assert!(!timeline[3].discontinuity);
        assert!(timeline[4].discontinuity);
    }

    #[test]
    fn shared_decision_passes_shared_segments_through() {
        let shared = shared_with_templated_item();

        let timeline = compose_timeline(
            &shared,
            None,
            "variants/abc/",
            "",
            &decided("game", ItemDecision::Shared),
            &bases_of(&shared),
            ComposeResume::default(),
            NO_HORIZON,
        );

        let paths: Vec<&str> = timeline.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(
            paths,
            vec![
                "live000000.ts",
                "live000001.ts",
                "live000002.ts",
                "live000003.ts",
                "live000004.ts",
            ]
        );
    }

    #[test]
    fn undecided_item_holds_the_timeline() {
        let shared = shared_with_templated_item();

        let timeline = compose_timeline(
            &shared,
            None,
            "variants/abc/",
            "",
            &HashMap::new(),
            &bases_of(&shared),
            ComposeResume::default(),
            NO_HORIZON,
        );

        let paths: Vec<&str> = timeline.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(paths, vec!["live000000.ts", "live000001.ts"]);
    }

    #[test]
    fn holds_after_substitution_until_variant_catches_up() {
        let shared = shared_with_templated_item();
        // variant produced only one of two segments so far
        let variant = PlaylistSidecar {
            segments: vec![seg("live000000.ts", "game", 0, true)],
            pipelines: vec![pipeline("game", 8_000, true)],
        };

        let timeline = compose_timeline(
            &shared,
            Some(&variant),
            "variants/abc/",
            "",
            &decided("game", variant_decision(0, 0)),
            &bases_of(&shared),
            ComposeResume::default(),
            NO_HORIZON,
        );

        let paths: Vec<&str> = timeline.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(
            paths,
            vec![
                "live000000.ts",
                "live000001.ts",
                "variants/abc/live000000.ts",
            ]
        );
    }

    #[test]
    fn decision_pins_variant_when_output_covers_item_start() {
        let mut session = SessionPlaylist::default();
        let shared = shared_with_templated_item();
        let variant = variant_for_game();
        let now = OffsetDateTime::UNIX_EPOCH;

        session.decide_items(&shared, Some(&variant), now);

        assert_eq!(session.decisions.get("game"), Some(&variant_decision(0, 0)));
    }

    /// LATENT PROPERTY GUARD, not a live defect. `item_bases` is written once,
    /// guarded by `!contains_key`, and cleared only by a full session reset.
    /// `first_unserved_ms` is a SPAN off that base rather than a count of
    /// positions served, so a base that does not belong to the airing being
    /// composed makes the join measure the distance back to that base instead
    /// of a position inside the item. The join is then used as a twin index in
    /// `render`, the variant has no segment that far in, and the cohort serves
    /// shared for the whole window.
    ///
    /// Only a base BELOW the airing's first position does this. The span
    /// subtracts the base, so a base that is too high can only shrink a join,
    /// never inflate one.
    ///
    /// NO KNOWN PATH REACHES IT, and the earlier claim that it explained the
    /// 2026-08-11 outliers is withdrawn. That reading rested on paired
    /// "shared session plays slate for this templated window" lines being one
    /// session entering an item twice. They are two concurrent worker
    /// processes each entering it once: the dual-worker incident, fixed by
    /// legacy 39c43c6d on 2026-08-11 12:05. Counted afterwards, multi-entry
    /// items fall from 59 of 193 on 08-11 to 1 of 196, 2 of 202 and 0 of 96 on
    /// the days following, with none since the 08-13 19:30 boot. Item ids are
    /// also unique across the playout folder (0 repeats in 2295 items), so the
    /// schedule does not list an item twice either.
    ///
    /// The test is kept because the write-once property is real and nothing
    /// enforces that a base belongs to the current airing. It pins the
    /// consequence so a future change that reintroduces the precondition fails
    /// here rather than on air.
    #[test]
    fn a_stale_item_base_inflates_the_join_past_the_item() {
        let mut session = SessionPlaylist::default();

        // a base recorded from a position well before the airing composed
        // below. Constructed, not observed: see the note above
        session.item_bases.insert(String::from("game"), 6);

        // the airing this session is actually composing, a long way further
        // along the same numbering space
        let second_airing: Vec<u64> = (106..=110).collect();
        for sequence in &second_airing {
            session.entries.push_back(ComposedEntry {
                path: format!("live{sequence:06}.ts"),
                duration: 4.0,
                program_date_time: OffsetDateTime::UNIX_EPOCH
                    + time::Duration::seconds(*sequence as i64 * 4),
                discontinuity: *sequence == 106,
                sequence: *sequence,
                variant: false,
            });
        }
        session.head_sequence = 106;

        let shared = PlaylistSidecar {
            segments: second_airing
                .iter()
                .map(|s| seg(&format!("live{s:06}.ts"), "game", *s as i64 * 4, *s == 106))
                .collect(),
            pipelines: vec![pipeline("game", 424_000, true)],
        };
        // a variant anchored exactly at the item start, so anchor_ms is 0 and
        // the join can only come from the span
        let variant = PlaylistSidecar {
            segments: vec![seg("live000000.ts", "game", 424, true)],
            pipelines: vec![pipeline("game", 424_000, true)],
        };

        session.decide_items(&shared, Some(&variant), OffsetDateTime::UNIX_EPOCH);

        let ItemDecision::Variant { join_ms, anchor_ms } = session
            .decisions
            .get("game")
            .copied()
            .expect("the item is anchored, so it must be decided")
        else {
            panic!("expected a Variant decision, got {:?}", session.decisions);
        };

        assert_eq!(anchor_ms, 0, "the variant is anchored at the item start");

        // the airing is five positions long: 20000ms. The join instead spans
        // from the stale base at 6 all the way to 110
        assert_eq!(
            join_ms,
            (110 - 6 + 1) * 4000,
            "the join spans back to the stale base rather than the item"
        );
        assert!(
            join_ms > 20_000,
            "the join is {join_ms}ms into an item that is only 20000ms long, \
             so it indexes a twin that cannot exist. Making the base follow \
             the airing being composed should make this assertion fail"
        );
    }

    #[test]
    fn decision_defers_then_forces_shared_at_the_window_edge() {
        let mut session = SessionPlaylist::default();
        let shared = shared_with_templated_item();

        // long before the item reaches viewers: undecided
        let early = OffsetDateTime::UNIX_EPOCH - time::Duration::seconds(60);
        session.decide_items(&shared, None, early);
        assert!(session.decisions.is_empty());

        // shortly after the item start pdt (+8s) the decision is still open,
        // because viewers play a full serve window behind the live edge
        let at_start = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(8);
        session.decide_items(&shared, None, at_start);
        assert!(session.decisions.is_empty());

        // by the time any viewer's playlist could reach the boundary, the
        // decision must be forced
        let late = OffsetDateTime::UNIX_EPOCH
            + time::Duration::seconds(8 + DECISION_BUDGET_SECONDS as i64);
        session.decide_items(&shared, None, late);
        assert_eq!(session.decisions.get("game"), Some(&ItemDecision::Shared));
    }

    /// A variant's source is live, so it connects at the item's air time and
    /// closes its first segment about ten seconds later, sometimes more. The
    /// decision has to still be open when a slow one lands: pinning shared
    /// first and upgrading afterwards is exactly what puts slate on screen
    /// over the top of a presentation that did arrive.
    ///
    /// Measured on 2026-08-12, a variant's startup ran 8.6s to 11.2s past
    /// the item's stamp over 22 clean windows, and the windows that failed
    /// were the ones that ran past 12s.
    #[test]
    fn decision_waits_out_a_slow_variant_instead_of_pinning_shared() {
        let mut session = SessionPlaylist::default();
        let shared = shared_with_templated_item();
        // the templated item's first shared segment is stamped +8s
        let item_pdt = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(8);

        // nothing from the variant yet, right through the window in which a
        // healthy one would already have appeared
        for elapsed in [8, 10, 12, 14, 16] {
            session.decide_items(&shared, None, item_pdt + time::Duration::seconds(elapsed));
            assert!(
                session.decisions.is_empty(),
                "pinned shared at +{elapsed}s, before a live variant's startup \
                 can honestly be called lost",
            );
        }

        // it lands late and is still served from its own first frame, with
        // no shared pin to upgrade away from and so no slate on screen
        session.decide_items(
            &shared,
            Some(&variant_for_game()),
            item_pdt + time::Duration::seconds(18),
        );
        assert_eq!(session.decisions.get("game"), Some(&variant_decision(0, 0)));
    }

    /// Content produced faster than realtime must not drag the composed edge
    /// past wall clock. This is the shape of a slate window: the shared
    /// session has the next two minutes on disk already.
    #[test]
    fn composition_stops_at_the_trailing_edge_of_worked_ahead_content() {
        let mut session = SessionPlaylist::default();
        let shared = PlaylistSidecar {
            segments: (0..30i64)
                .map(|i| seg(&format!("live{i:06}.ts"), "show", i * 4, i == 0))
                .collect(),
            pipelines: vec![pipeline("show", 0, false)],
        };
        // 40s of wall time has passed against 120s of produced media
        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(40);

        session.advance_and_render(&shared, None, "variants/x/", Some(0), now, 4, |s| {
            s.to_owned()
        });

        let newest = session.entries.back().expect("entries");
        assert_eq!(
            newest.program_date_time,
            now - time::Duration::seconds(COMPOSE_TRAIL_SECONDS as i64),
            "the composed edge trails wall clock instead of following production"
        );
    }

    /// The 14:40 star break, in miniature. The shared session has the whole
    /// templated window on disk before it airs (slate), while the variant's
    /// first segment only exists ten seconds after air. Trailing composition
    /// must not reach the window until the variant is anchored, so the
    /// cohort joins the variant at position zero rather than watching slate.
    #[test]
    fn a_trailing_edge_reaches_a_slate_window_only_once_its_variant_exists() {
        let mut session = SessionPlaylist::with_label(String::from("cohort"));
        let mut segments: Vec<SidecarSegment> = (0..10i64)
            .map(|i| seg(&format!("live{i:06}.ts"), "logo", i * 4, i == 0))
            .collect();
        segments
            .extend((10..48i64).map(|i| seg(&format!("live{i:06}.ts"), "star", i * 4, i == 10)));
        let mut shared = PlaylistSidecar {
            segments,
            pipelines: vec![pipeline("logo", 0, false), pipeline("star", 40_000, true)],
        };
        shared.pipelines[1].fallback = true;

        // air is 40s; at 44s the variant has produced nothing yet, and the
        // trailing edge has not reached the window either
        let air = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(40);
        session.advance_and_render(
            &shared,
            None,
            "variants/x/",
            Some(0),
            air + time::Duration::seconds(4),
            4,
            |s| s.to_owned(),
        );
        assert!(
            session.entries.iter().all(|e| !e.variant),
            "nothing substituted yet"
        );
        assert!(
            session
                .entries
                .back()
                .is_some_and(|e| e.program_date_time < air),
            "the trailing edge has not reached the window"
        );

        // the variant's first segments land ten seconds after air; by the
        // time the trailing edge arrives, they are there to substitute
        let variant = PlaylistSidecar {
            segments: (0..6i64)
                .map(|i| seg(&format!("live{i:06}.ts"), "star", 40 + i * 4, i == 0))
                .collect(),
            pipelines: vec![{
                let mut p = pipeline("star", 40_000, true);
                p.fallback = false;
                p
            }],
        };
        let rendered = session.advance_and_render(
            &shared,
            Some(&variant),
            "variants/x/",
            Some(0),
            air + time::Duration::seconds(16),
            4,
            |s| s.to_owned(),
        );

        assert!(
            rendered.contains("#EXT-X-MEDIA-SEQUENCE:"),
            "a playlist is still rendered"
        );
        let star_positions: Vec<_> = session
            .entries
            .iter()
            .filter(|e| e.sequence >= 10)
            .map(|e| (e.sequence, e.variant))
            .collect();
        assert_eq!(
            star_positions,
            vec![(10, true), (11, true), (12, true)],
            "every composed position of the window came from the variant, \
             starting at the window's first position"
        );
        assert_eq!(
            session.decisions.get("star"),
            Some(&ItemDecision::Variant {
                join_ms: 0,
                anchor_ms: 0
            }),
            "no slate position was committed ahead of the variant"
        );
    }

    #[test]
    fn the_unanchored_reason_names_each_failure_mode() {
        let shared_pipeline = pipeline("game", 8_000, true);

        assert_eq!(
            unanchored_reason(&shared_pipeline, None),
            "no variant sidecar published"
        );

        let unproduced = PlaylistSidecar {
            segments: Vec::new(),
            pipelines: vec![pipeline("game", 8_000, true)],
        };
        assert_eq!(
            unanchored_reason(&shared_pipeline, Some(&unproduced)),
            "variant has produced no segments"
        );

        let mut wrong_item = variant_for_game();
        wrong_item.pipelines[0].item_id = String::from("other");
        assert_eq!(
            unanchored_reason(&shared_pipeline, Some(&wrong_item)),
            "variant is anchored to item other instead"
        );

        let mut early = variant_for_game();
        early.pipelines[0].pts_offset_ms = 7_000;
        assert_eq!(
            unanchored_reason(&shared_pipeline, Some(&early)),
            "variant pts_offset 7000ms precedes the shared envelope's 8000ms"
        );
    }

    #[test]
    fn variant_anchored_before_the_item_is_not_chosen() {
        let mut session = SessionPlaylist::default();
        let shared = shared_with_templated_item();
        let mut variant = variant_for_game();
        variant.pipelines[0].pts_offset_ms = 7_000;

        let late = OffsetDateTime::UNIX_EPOCH
            + time::Duration::seconds(8 + DECISION_BUDGET_SECONDS as i64);
        session.decide_items(&shared, Some(&variant), late);

        assert_eq!(session.decisions.get("game"), Some(&ItemDecision::Shared));
    }

    #[test]
    fn variant_anchored_mid_item_becomes_a_late_join() {
        let mut session = SessionPlaylist::default();
        let shared = shared_with_templated_item();
        let mut variant = variant_for_game();
        // anchored one grid position into the item
        variant.pipelines[0].pts_offset_ms = 12_000;

        let now = OffsetDateTime::UNIX_EPOCH;
        session.decide_items(&shared, Some(&variant), now);

        assert_eq!(
            session.decisions.get("game"),
            Some(&variant_decision(4_000, 4_000))
        );
    }

    #[test]
    fn shared_soft_pin_upgrades_when_anchored_output_appears() {
        let mut session = SessionPlaylist::default();
        let shared = shared_with_templated_item();

        // deadline passes with no variant: soft-pinned shared
        let late = OffsetDateTime::UNIX_EPOCH
            + time::Duration::seconds(8 + DECISION_BUDGET_SECONDS as i64);
        session.decide_items(&shared, None, late);
        assert_eq!(session.decisions.get("game"), Some(&ItemDecision::Shared));

        // anchored variant output appears mid-item: upgrade to a late join
        let mut variant = variant_for_game();
        variant.pipelines[0].pts_offset_ms = 12_000;
        session.decide_items(&shared, Some(&variant), late);
        assert_eq!(
            session.decisions.get("game"),
            Some(&variant_decision(4_000, 4_000))
        );
    }

    /// The defect this guards against: a soft-pinned item whose first
    /// positions were already served from the shared feed, upgraded when the
    /// variant appeared. The join must start past the served positions, and
    /// the variant's own segments for those positions must be consumed
    /// unserved so no position is served twice and none is served shifted.
    #[test]
    fn upgrade_never_reserves_an_already_served_position() {
        let mut session = SessionPlaylist::default();
        let late = OffsetDateTime::UNIX_EPOCH
            + time::Duration::seconds(8 + DECISION_BUDGET_SECONDS as i64);

        // the shared session has produced only the item's first position when
        // the deadline passes: shared pinned, that position served
        let partial = PlaylistSidecar {
            segments: vec![
                seg("live000000.ts", "before", 0, false),
                seg("live000001.ts", "before", 4, false),
                seg("live000002.ts", "game", 8, true),
            ],
            pipelines: vec![pipeline("before", 0, false), pipeline("game", 8_000, true)],
        };
        let first =
            session.advance_and_render(&partial, None, "variants/abc/", Some(0), late, 4, |s| {
                s.to_owned()
            });
        assert_eq!(session.decisions.get("game"), Some(&ItemDecision::Shared));
        assert!(first.contains("live000002.ts"));

        // the variant appears anchored at the item start; the upgrade must
        // join past the served position
        let shared = shared_with_templated_item();
        let variant = variant_for_game();
        let second = session.advance_and_render(
            &shared,
            Some(&variant),
            "variants/abc/",
            Some(0),
            late + time::Duration::seconds(2),
            4,
            |s| s.to_owned(),
        );

        assert_eq!(
            session.decisions.get("game"),
            Some(&variant_decision(4_000, 0))
        );
        // position 0 stays the shared segment; position 1 is the variant's
        // SECOND segment, because its first belongs to the served position
        assert!(second.contains("live000002.ts"));
        assert!(!second.contains("variants/abc/live000000.ts"));
        assert!(second.contains("variants/abc/live000001.ts"));

        // and no sequence number appears twice across the whole history
        let sequences: Vec<u64> = session.entries.iter().map(|e| e.sequence).collect();
        let mut deduped = sequences.clone();
        deduped.dedup();
        assert_eq!(sequences, deduped);
    }

    #[test]
    fn late_join_substitutes_only_from_the_join_position() {
        let shared = shared_with_templated_item();
        // variant anchored at the second game position, one segment produced
        let variant = PlaylistSidecar {
            segments: vec![seg("live000000.ts", "game", 0, true)],
            pipelines: vec![pipeline("game", 12_000, true)],
        };

        let timeline = compose_timeline(
            &shared,
            Some(&variant),
            "variants/abc/",
            "",
            &decided("game", variant_decision(4_000, 4_000)),
            &bases_of(&shared),
            ComposeResume::default(),
            NO_HORIZON,
        );

        let paths: Vec<&str> = timeline.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(
            paths,
            vec![
                "live000000.ts",
                "live000001.ts",
                "live000002.ts",
                "variants/abc/live000000.ts",
                "live000004.ts",
            ]
        );
        // the join carries a discontinuity for players that honor them
        assert!(timeline[3].discontinuity);
    }

    #[test]
    fn session_playlist_is_append_only_across_advances() {
        let mut session = SessionPlaylist::default();
        let shared = shared_with_templated_item();
        let variant = variant_for_game();
        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(28);

        let first = session.advance_and_render(
            &shared,
            Some(&variant),
            "variants/abc/",
            None,
            now,
            4,
            |s| s.to_owned(),
        );
        assert!(first.contains("variants/abc/live000000.ts"));

        // a later advance with the same inputs must not duplicate entries
        let second = session.advance_and_render(
            &shared,
            Some(&variant),
            "variants/abc/",
            None,
            now,
            4,
            |s| s.to_owned(),
        );
        assert_eq!(first, second);
        assert_eq!(session.entries.len(), 5);
    }

    /// The composed playlist shares the shared playlist's sequence space, so
    /// serving mirrors the shared head whenever composed content has reached
    /// it, and a client moved between the playlists sees one numbering.
    #[test]
    fn serving_mirrors_the_shared_head_when_content_is_complete() {
        let mut session = SessionPlaylist::default();
        // long enough that this session's own last full window is a real
        // position rather than the front of the timeline: with a shorter
        // one the head is pinned to `front` and mirroring cannot be seen
        let shared = long_shared_with_templated_item(16);
        let variant = long_variant_for_game(5);
        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(60);

        let rendered = session.advance_and_render(
            &shared,
            Some(&variant),
            "variants/abc/",
            Some(2),
            now,
            4,
            |s| s.to_owned(),
        );

        // the shared head is inside this timeline's own window, so it is
        // served verbatim: the numbering is neither cohort-local (0) nor
        // this timeline's own window (4)
        assert!(rendered.contains("#EXT-X-MEDIA-SEQUENCE:2\n"));
        assert_eq!(session.serve_head, Some(2));
        assert!(rendered.contains("variants/abc/live000000.ts"));
        // shared segment 1 is behind the head; the leading newline keeps
        // this from matching the variant's own live000001.ts
        assert!(!rendered.contains("\nlive000001.ts"));
    }

    /// `served_window` exists so the audit can check the exact references a
    /// viewer can request; if it and the rendered playlist ever disagree,
    /// the audit is checking files nobody serves.
    #[test]
    fn served_window_lists_exactly_what_render_published() {
        let mut session = SessionPlaylist::default();
        let shared = long_shared_with_templated_item(16);
        let variant = long_variant_for_game(5);
        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(60);

        assert!(
            session.served_window().next().is_none(),
            "nothing is served before the first render"
        );

        let rendered = session.advance_and_render(
            &shared,
            Some(&variant),
            "variants/abc/",
            Some(2),
            now,
            4,
            |s| s.to_owned(),
        );

        let published: Vec<&str> = rendered.lines().filter(|l| l.ends_with(".ts")).collect();
        let window: Vec<&str> = session.served_window().map(|e| e.path.as_str()).collect();
        assert!(!published.is_empty());
        assert_eq!(window, published);
    }

    /// The head never chases past this timeline's own last full window. The
    /// shared head runs ahead of everything composed here by design, since
    /// composition stops `COMPOSE_TRAIL_SECONDS` behind the live edge, and a
    /// head that followed it would serve a window too short to buffer.
    #[test]
    fn serving_stops_at_this_timelines_own_window() {
        let mut session = SessionPlaylist::default();
        let shared = long_shared_with_templated_item(16);
        let variant = long_variant_for_game(5);
        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(60);

        let rendered = session.advance_and_render(
            &shared,
            Some(&variant),
            "variants/abc/",
            // far past anything this timeline has composed
            Some(30),
            now,
            4,
            |s| s.to_owned(),
        );

        // composed tail is 13 (pdt 52 is the newest inside the horizon), so
        // the head stops a full window back
        assert_eq!(session.serve_head, Some(4));
        assert!(rendered.contains("#EXT-X-MEDIA-SEQUENCE:4\n"));
        assert_eq!(
            rendered.lines().filter(|l| l.ends_with(".ts")).count(),
            SERVED_SEGMENTS,
            "a head at its own window edge still serves a full window"
        );
    }

    /// When the variant lags, the head walks at playback rate instead of
    /// mirroring, so the window never jumps over content a viewer has not
    /// played. Here the shared head is far ahead of what composition has
    /// emitted; the head holds at the emission edge.
    #[test]
    fn a_lagging_head_walks_instead_of_jumping() {
        let mut session = SessionPlaylist::default();
        let shared = shared_with_templated_item();
        // no variant output yet: composition holds before the game item
        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(18);

        let rendered =
            session.advance_and_render(&shared, None, "variants/abc/", Some(4), now, 4, |s| {
                s.to_owned()
            });

        // only live000000/1 are emitted; the head holds a window open at
        // the emission edge instead of chasing shared's 4
        assert!(rendered.contains("#EXT-X-MEDIA-SEQUENCE:0\n"));
        assert!(rendered.contains("live000001.ts"));

        // two seconds later the head has not skipped anywhere
        let again = session.advance_and_render(
            &shared,
            None,
            "variants/abc/",
            Some(4),
            now + time::Duration::seconds(2),
            4,
            |s| s.to_owned(),
        );
        assert!(again.contains("#EXT-X-MEDIA-SEQUENCE:0\n"));
    }

    /// Past the lag bound the head jumps as far forward as this timeline can
    /// serve from: a bounded worst-case delay is preferred over an unbounded
    /// one. The bound is measured against the shared head, so it still fires
    /// when composition trails that head by design.
    #[test]
    fn a_head_past_the_lag_bound_jumps_to_its_own_window() {
        let mut session = SessionPlaylist::default();
        let shared = continuous_shared_with_templated_item();
        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(68);

        // decided shared so the whole timeline emits
        session
            .decisions
            .insert(String::from("game"), ItemDecision::Shared);

        session.advance_and_render(&shared, None, "variants/x/", Some(0), now, 4, |s| {
            s.to_owned()
        });
        assert_eq!(session.serve_head, Some(0));

        // the shared head leaps far ahead (a burst after a stall); the lag
        // bound caps how far behind this session may stay
        let rendered = session.advance_and_render(
            &shared,
            None,
            "variants/x/",
            Some(30),
            now + time::Duration::seconds(2),
            4,
            |s| s.to_owned(),
        );

        // gap is 30 against the shared head, past the hard bound, and the
        // head jumps to this timeline's own window: tail 15 less a full
        // window. not to the composed edge, which would serve one segment
        assert!(rendered.contains("#EXT-X-MEDIA-SEQUENCE:6\n"));
        assert_eq!(session.serve_head, Some(6));
        assert_eq!(
            rendered.lines().filter(|l| l.ends_with(".ts")).count(),
            SERVED_SEGMENTS
        );
        assert!(rendered.contains("live000015.ts"));
    }

    /// A lagging head inside the bound never jumps: what follows a window on
    /// a real channel is the next program, and a jump would eat its
    /// beginning. The head walks, one segment per segment duration.
    #[test]
    fn a_lagging_head_never_skips_content_inside_the_bound() {
        let mut session = SessionPlaylist::default();
        let shared = continuous_shared_with_templated_item();
        session
            .decisions
            .insert(String::from("game"), ItemDecision::Shared);
        // late enough that the whole 16-segment timeline is composed, so the
        // head has somewhere to walk to
        let base = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(68);

        session.advance_and_render(&shared, None, "variants/x/", Some(0), base, 4, |s| {
            s.to_owned()
        });
        assert_eq!(session.serve_head, Some(0));

        // the shared head moves 6 ahead (inside the bound); ours walks one
        // segment per segment duration instead of jumping
        let rendered = session.advance_and_render(
            &shared,
            None,
            "variants/x/",
            Some(6),
            base + time::Duration::seconds(4),
            4,
            |s| s.to_owned(),
        );
        assert!(rendered.contains("#EXT-X-MEDIA-SEQUENCE:1\n"));

        let rendered = session.advance_and_render(
            &shared,
            None,
            "variants/x/",
            Some(6),
            base + time::Duration::seconds(8),
            4,
            |s| s.to_owned(),
        );
        assert!(rendered.contains("#EXT-X-MEDIA-SEQUENCE:2\n"));
    }

    /// Past the soft bound, lag is trimmed only onto an item boundary: the
    /// viewer loses the tail of the item they were behind in and resumes at
    /// the start of the next one, never mid-content.
    #[test]
    fn excess_lag_is_trimmed_onto_an_item_boundary() {
        let mut session = SessionPlaylist::default();
        // 26 segments so two boundaries sit inside the trim's reach and a
        // third sits outside it
        let shared = long_shared_with_templated_item(26);
        session
            .decisions
            .insert(String::from("game"), ItemDecision::Shared);
        let base = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(108);

        session.advance_and_render(&shared, None, "variants/x/", Some(0), base, 4, |s| {
            s.to_owned()
        });
        assert_eq!(session.serve_head, Some(0));

        // the gap crosses the soft bound; discontinuities sit at 6, 11 and
        // 20, and the trim lands on the NEAREST one past the head, so the
        // viewer loses the tail of "before" and resumes at the start of
        // "game" rather than losing "game" entirely as well
        let rendered = session.advance_and_render(
            &shared,
            None,
            "variants/x/",
            Some(18),
            base + time::Duration::seconds(2),
            4,
            |s| s.to_owned(),
        );
        assert!(rendered.contains("#EXT-X-MEDIA-SEQUENCE:6\n"));
        assert_eq!(session.serve_head, Some(6));
        assert!(rendered.contains("live000006.ts"));
        // a full window from there, and the boundaries at 11 and 20 are not
        // jumped to
        assert!(rendered.contains("live000015.ts"));
        assert!(!rendered.contains("live000016.ts"));
    }

    /// Past the soft bound the trim still refuses to cross variant content.
    /// A head inside the substituted item plays all of it a little late
    /// rather than losing the item the cohort exists for, which is the
    /// invariant `ComposedEntry::variant` documents.
    #[test]
    fn a_trim_never_jumps_over_variant_content() {
        let mut session = SessionPlaylist::default();
        let shared = long_shared_with_templated_item(26);
        let variant = long_variant_for_game(5);
        session.decisions.insert(
            String::from("game"),
            ItemDecision::Variant {
                join_ms: 0,
                anchor_ms: 0,
            },
        );
        let base = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(108);

        session.advance_and_render(
            &shared,
            Some(&variant),
            "variants/x/",
            Some(0),
            base,
            4,
            |s| s.to_owned(),
        );

        // park the head inside the substituted span: "game" covers shared
        // positions 6..=10, so 7 is mid-variant
        session.serve_head = Some(7);
        session.head_advanced_at = Some(base + time::Duration::seconds(2));

        // the shared head is 12 ahead, past the soft bound, and the next
        // boundary (11) is inside the reachable window. Taking it would drop
        // variant positions 7..=10, which is precisely what must not happen
        let rendered = session.advance_and_render(
            &shared,
            Some(&variant),
            "variants/x/",
            Some(19),
            base + time::Duration::seconds(2),
            4,
            |s| s.to_owned(),
        );

        assert_eq!(
            session.serve_head,
            Some(7),
            "the trim crossed variant content to reach a boundary"
        );
        assert!(rendered.contains("#EXT-X-MEDIA-SEQUENCE:7\n"));
        assert!(rendered.contains("variants/x/live000001.ts"));
    }

    /// With no boundary in reach the trim defers: the head keeps walking at
    /// playback speed rather than cutting mid-content.
    #[test]
    fn a_trim_with_no_boundary_in_reach_defers() {
        let mut session = SessionPlaylist::default();
        let segments = (0..16i64)
            .map(|i| seg(&format!("live{i:06}.ts"), "show", i * 4, i == 0))
            .collect();
        let shared = PlaylistSidecar {
            segments,
            pipelines: vec![pipeline("show", 0, false)],
        };
        let base = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(20);

        session.advance_and_render(&shared, None, "variants/x/", Some(0), base, 4, |s| {
            s.to_owned()
        });
        assert_eq!(session.serve_head, Some(0));

        let rendered = session.advance_and_render(
            &shared,
            None,
            "variants/x/",
            Some(15),
            base + time::Duration::seconds(2),
            4,
            |s| s.to_owned(),
        );
        assert!(rendered.contains("#EXT-X-MEDIA-SEQUENCE:0\n"));
        assert_eq!(session.serve_head, Some(0));
    }

    /// REPRODUCTION of an audit finding, left failing-by-documentation rather
    /// than fixed here: a session with nothing composed yet still renders a
    /// playlist, and that playlist has NO segments in it.
    ///
    /// It reaches a viewer. `write_atomic` publishes whatever `render` returns
    /// with no minimum, and the legacy reader's `ReadComposedPlaylist` gates
    /// only on the file existing and being fresh, never on its contents. So a
    /// cohort tuning in during the window between its session being created
    /// and its first entry being composed is handed a playlist with headers
    /// and nothing to play.
    ///
    /// The shared session does not have this problem: upstream #202 gave it a
    /// `MIN_SEGMENTS` ready gate that counts the PUBLISHED window. The composed
    /// path has no counterpart, which is the gap.
    #[test]
    fn a_session_with_nothing_composed_publishes_an_empty_playlist() {
        let mut session = SessionPlaylist::default();
        let shared = PlaylistSidecar {
            segments: Vec::new(),
            pipelines: vec![pipeline("before", 0, false)],
        };
        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(68);

        let rendered =
            session.advance_and_render(&shared, None, "variants/x/", Some(0), now, 4, |s| {
                s.to_owned()
            });

        assert!(
            rendered.contains("#EXTM3U"),
            "it really is published as a playlist: {rendered}"
        );
        assert_eq!(
            rendered.lines().filter(|l| !l.starts_with('#')).count(),
            0,
            "and it carries no segments at all: {rendered}"
        );
    }

    /// A fresh session joining a lagging timeline starts a full window
    /// behind the emission edge, not at it: a one-segment playlist gives a
    /// player nothing to buffer.
    #[test]
    fn a_fresh_session_opens_a_full_window_behind_the_emission_edge() {
        let mut session = SessionPlaylist::default();
        let shared = continuous_shared_with_templated_item();
        session
            .decisions
            .insert(String::from("game"), ItemDecision::Shared);
        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(68);

        let rendered =
            session.advance_and_render(&shared, None, "variants/x/", Some(30), now, 4, |s| {
                s.to_owned()
            });

        // tail is 15, so the head opens a full window back at 6, never at
        // the composed edge
        assert!(rendered.contains("#EXT-X-MEDIA-SEQUENCE:6\n"));
        assert_eq!(session.serve_head, Some(15 - (SERVED_SEGMENTS as u64 - 1)));
        assert_eq!(
            rendered.lines().filter(|l| l.ends_with(".ts")).count(),
            SERVED_SEGMENTS,
            "rfc8216bis 6.2.2 wants three target durations of media in the window"
        );
        assert!(rendered.contains("#EXT-X-DISCONTINUITY-SEQUENCE:1\n"));
        assert!(rendered.contains("live000012.ts"));
        assert!(rendered.contains("live000015.ts"));

        // the head is already as far forward as this timeline reaches, and
        // the shared head is past the hard bound. a tick that cannot move
        // the head must not restart the pacing clock, or the walk never
        // accumulates the segment duration it needs and the head freezes
        session.advance_and_render(
            &shared,
            None,
            "variants/x/",
            Some(30),
            now + time::Duration::seconds(2),
            4,
            |s| s.to_owned(),
        );
        assert_eq!(session.serve_head, Some(6));
        assert_eq!(
            session.head_advanced_at,
            Some(now),
            "a tick that moves nothing must not reset the pacing clock"
        );
    }

    /// The shared sidecar trims an item's early segments once the item ages
    /// past its history. Positions derive from recorded bases, so a trimmed
    /// sidecar must map every remaining position to the same variant twin it
    /// mapped before the trim.
    #[test]
    fn positions_survive_shared_history_trimming() {
        let shared = shared_with_templated_item();
        let bases = bases_of(&shared);
        let variant = variant_for_game();
        let decisions = decided("game", variant_decision(0, 0));

        let before = compose_timeline(
            &shared,
            Some(&variant),
            "variants/abc/",
            "",
            &decisions,
            &bases,
            ComposeResume::default(),
            NO_HORIZON,
        );

        // the item's first segment ages out of the sidecar
        let mut trimmed = shared.clone();
        trimmed.segments.remove(2);
        let after = compose_timeline(
            &trimmed,
            Some(&variant),
            "variants/abc/",
            "",
            &decisions,
            &bases,
            ComposeResume::default(),
            NO_HORIZON,
        );

        // live000003 (the item's second position) keeps the SECOND variant
        // twin, exactly as before the trim
        let find = |t: &[ComposedEntry]| {
            t.iter()
                .find(|e| e.sequence == 3)
                .map(|e| e.path.clone())
                .unwrap()
        };
        assert_eq!(find(&before), "variants/abc/live000001.ts");
        assert_eq!(find(&after), "variants/abc/live000001.ts");
    }

    /// A session that held back while every sequence it needed aged out of
    /// the timeline can never resume by waiting. It re-anchors to current
    /// content: one clean break instead of a permanently frozen playlist.
    #[test]
    fn an_unfillable_gap_reanchors_instead_of_freezing() {
        let mut session = SessionPlaylist::default();
        let old = PlaylistSidecar {
            segments: vec![
                seg("live000000.ts", "before", 0, false),
                seg("live000001.ts", "before", 4, false),
            ],
            pipelines: vec![pipeline("before", 0, false)],
        };
        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(8);
        session.advance_and_render(&old, None, "variants/x/", Some(0), now, 4, |s| s.to_owned());

        // much later the sidecar only holds far newer segments
        let current = PlaylistSidecar {
            segments: vec![
                seg("live000200.ts", "before", 800, false),
                seg("live000201.ts", "before", 804, false),
            ],
            pipelines: vec![pipeline("before", 0, false)],
        };
        let later = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(812);
        let rendered =
            session.advance_and_render(&current, None, "variants/x/", Some(200), later, 4, |s| {
                s.to_owned()
            });

        assert!(rendered.contains("#EXT-X-MEDIA-SEQUENCE:200\n"));
        assert!(rendered.contains("live000200.ts"));
    }

    /// EXT-X-DISCONTINUITY-SEQUENCE does not go backwards across a re-anchor,
    /// which rfc8216bis 6.2.1 forbids between reloads of the same playlist.
    ///
    /// A 2026-08-14 audit rated it MEDIUM that `reconcile`'s re-anchor sets
    /// `head_discontinuity_sequence` back to zero while the media sequence
    /// jumps forward. The field really is zeroed, but it is not what gets
    /// published: `render` emits that base PLUS the discontinuities inside the
    /// window it is serving, so zeroing the base alone does not move the
    /// published number. Driven through the audit's own scenario the counter
    /// holds at 3 while the media sequence climbs 0 -> 400.
    ///
    /// Kept as the guard the finding implied, since the invariant is real even
    /// though the defect was not: this fails if any change makes the published
    /// value decrease.
    #[test]
    fn a_reanchor_never_sends_the_discontinuity_sequence_backwards() {
        let mut session = SessionPlaylist::default();

        // three items, so the composed history carries discontinuities and the
        // head accumulates a non-zero count as it walks past them
        let old = PlaylistSidecar {
            segments: (0..12i64)
                .map(|i| {
                    let at = i * 4;
                    let item = match at {
                        at if at < 16 => "a",
                        at if at < 32 => "b",
                        _ => "c",
                    };
                    seg(
                        &format!("live{i:06}.ts"),
                        item,
                        at,
                        at == 0 || at == 16 || at == 32,
                    )
                })
                .collect(),
            pipelines: vec![
                pipeline("a", 0, false),
                pipeline("b", 16_000, false),
                pipeline("c", 32_000, false),
            ],
        };
        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(200);
        let before = session.advance_and_render(&old, None, "variants/x/", Some(11), now, 4, |s| {
            s.to_owned()
        });
        let ds_before = discontinuity_sequence_of(&before);
        let ms_before = media_sequence_of(&before);

        // the sidecar now holds only far newer segments, so the position this
        // session still needs can never arrive and it re-anchors
        let current = PlaylistSidecar {
            segments: vec![
                seg("live000400.ts", "d", 1600, true),
                seg("live000401.ts", "d", 1604, false),
            ],
            pipelines: vec![pipeline("d", 1_600_000, false)],
        };
        let later = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1620);
        let after =
            session.advance_and_render(&current, None, "variants/x/", Some(401), later, 4, |s| {
                s.to_owned()
            });
        let ds_after = discontinuity_sequence_of(&after);
        let ms_after = media_sequence_of(&after);

        assert!(
            ms_after > ms_before,
            "the media sequence must move forward across a re-anchor: {ms_before} -> {ms_after}"
        );
        assert!(
            ds_before > 0,
            "the session needs a non-zero discontinuity count for this to prove anything"
        );
        assert!(
            ds_after >= ds_before,
            "the discontinuity sequence went backwards, {ds_before} -> {ds_after}, \
             while the media sequence went {ms_before} -> {ms_after}. \
             rfc8216bis 6.2.1 forbids it"
        );
    }

    fn media_sequence_of(playlist: &str) -> u64 {
        tag_value(playlist, "#EXT-X-MEDIA-SEQUENCE:")
    }

    fn discontinuity_sequence_of(playlist: &str) -> u64 {
        tag_value(playlist, "#EXT-X-DISCONTINUITY-SEQUENCE:")
    }

    fn tag_value(playlist: &str, tag: &str) -> u64 {
        playlist
            .lines()
            .find_map(|l| l.strip_prefix(tag))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or_else(|| panic!("{tag} missing from playlist:\n{playlist}"))
    }

    /// The variant's own playlist trims its history during a window longer
    /// than it, shifting the segment list. Substitution has to find segments
    /// by the index in their file name, or a shifted list re-serves later
    /// segments at earlier positions.
    #[test]
    fn substitution_survives_variant_history_trimming() {
        let shared = shared_with_templated_item();
        // the variant's first file has been trimmed from its sidecar
        let variant = PlaylistSidecar {
            segments: vec![seg("live000001.ts", "game", 4, false)],
            pipelines: vec![pipeline("game", 8_000, true)],
        };

        let timeline = compose_timeline(
            &shared,
            Some(&variant),
            "variants/abc/",
            "",
            &decided("game", variant_decision(4_000, 0)),
            &bases_of(&shared),
            ComposeResume::default(),
            NO_HORIZON,
        );

        let paths: Vec<&str> = timeline.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(
            paths,
            vec![
                "live000000.ts",
                "live000001.ts",
                "live000002.ts",
                "variants/abc/live000001.ts",
                "live000004.ts",
            ]
        );
    }

    #[test]
    fn media_sequence_advances_as_history_trims() {
        let mut session = SessionPlaylist::default();
        let shared = long_shared_with_templated_item(20);
        let variant = long_variant_for_game(5);

        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(88);
        session.advance_and_render(
            &shared,
            Some(&variant),
            "variants/abc/",
            Some(15),
            now,
            4,
            |s| s.to_owned(),
        );
        // nothing has aged out yet: the head opens a full window back from
        // tail 19, with the channel start and the splice-in behind it
        assert_eq!(session.serve_head, Some(10));

        // much later, history behind the head really does trim and the
        // timeline has grown; the head walks forward (through the variant
        // span, which is never jumped) and the window discontinuity count
        // carries both the rolled-off splice and the in-window ones
        let shared = long_shared_with_templated_item(30);
        let later = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(500);
        let rendered = session.advance_and_render(
            &shared,
            Some(&variant),
            "variants/abc/",
            Some(25),
            later,
            4,
            |s| s.to_owned(),
        );

        assert_eq!(session.head_sequence, 10, "history behind the head trimmed");
        assert_eq!(
            session.head_discontinuity_sequence, 2,
            "the channel start and the splice-in rolled off"
        );
        assert!(rendered.contains("#EXT-X-MEDIA-SEQUENCE:20\n"));
        // two from rolled-off history, one from the splice-out inside the
        // skipped span
        assert!(rendered.contains("#EXT-X-DISCONTINUITY-SEQUENCE:3\n"));
        assert!(rendered.contains("live000020.ts"));
    }

    #[test]
    fn subtitle_rendering_maps_paths() {
        let mut session = SessionPlaylist::default();
        let shared = shared_with_templated_item();
        let variant = variant_for_game();
        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(20);

        let rendered = session.advance_and_render(
            &shared,
            Some(&variant),
            "variants/abc/",
            None,
            now,
            4,
            |s| format!("{}.vtt", s.strip_suffix(".ts").unwrap_or(s)),
        );

        assert!(rendered.contains("variants/abc/live000000.vtt"));
        assert!(!rendered.contains(".ts\n"));
    }

    /// A session that has already emitted history at sequence 40..49, ticked
    /// against a sidecar whose retention now reaches back to 20.
    fn session_holding_40_to_49() -> SessionPlaylist {
        let mut session = SessionPlaylist::default();
        let shared = PlaylistSidecar {
            segments: (40..50i64)
                .map(|i| seg(&format!("live{i:06}.ts"), "show", i * 4, i == 40))
                .collect(),
            pipelines: vec![pipeline("show", 0, false)],
        };
        session.advance_and_render(
            &shared,
            None,
            "variants/x/",
            Some(46),
            OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(204),
            4,
            |s| s.to_owned(),
        );
        assert_eq!(session.head_sequence, 40);
        assert_eq!(session.entries.len(), 10);
        session
    }

    /// The regression seen live: retention deepened, so the sidecar reaches
    /// back past everything held, into a templated item whose variant twins
    /// have been trimmed. Composition truncates at the missing twin, below
    /// the held front. That truncation says nothing about shared numbering,
    /// and the session must hold its history, not reset to the truncated
    /// timeline.
    #[test]
    fn a_timeline_truncated_below_held_history_does_not_reset_the_session() {
        let mut session = session_holding_40_to_49();
        session
            .decisions
            .insert(String::from("star"), variant_decision(0, 0));

        let mut segments: Vec<SidecarSegment> = (20..25i64)
            .map(|i| seg(&format!("live{i:06}.ts"), "pre", i * 4, i == 20))
            .collect();
        segments
            .extend((25..35i64).map(|i| seg(&format!("live{i:06}.ts"), "star", i * 4, i == 25)));
        segments
            .extend((35..50i64).map(|i| seg(&format!("live{i:06}.ts"), "post", i * 4, i == 35)));
        let shared = PlaylistSidecar {
            segments,
            pipelines: vec![
                pipeline("pre", 0, false),
                pipeline("star", 0, true),
                pipeline("post", 0, false),
            ],
        };
        // the star item's early twins are trimmed; only a later one survives
        let variant = PlaylistSidecar {
            segments: vec![seg("live000005.ts", "star", 20, false)],
            pipelines: vec![pipeline("star", 0, true)],
        };

        let rendered = session.advance_and_render(
            &shared,
            Some(&variant),
            "variants/x/",
            Some(46),
            OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(204),
            4,
            |s| s.to_owned(),
        );

        assert_eq!(session.head_sequence, 40);
        assert_eq!(session.entries.back().map(|e| e.sequence), Some(49));
        assert!(rendered.contains("live000049.ts"));
    }

    /// The reset the guard exists for: the shared session restarted and
    /// renumbered from zero, which the sidecar itself shows. Detection must
    /// still fire now that it reads the sidecar instead of the timeline.
    #[test]
    fn a_renumbered_shared_sidecar_resets_the_session() {
        let mut session = session_holding_40_to_49();

        let shared = PlaylistSidecar {
            segments: (0..10i64)
                .map(|i| seg(&format!("live{i:06}.ts"), "encore", 200 + i * 4, i == 0))
                .collect(),
            pipelines: vec![pipeline("encore", 0, false)],
        };

        let rendered = session.advance_and_render(
            &shared,
            None,
            "variants/x/",
            Some(0),
            OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(244),
            4,
            |s| s.to_owned(),
        );

        assert_eq!(session.head_sequence, 0);
        assert_eq!(session.entries.len(), 10);
        assert!(rendered.contains("live000000.ts"));
    }

    /// A session that recorded its per-item state in a numbering space
    /// starting at 40: base 40 for the templated item, and a final `Variant`
    /// decision whose join and anchor were measured against that space's
    /// pipelines.
    fn session_decided_variant_in_the_40_space() -> SessionPlaylist {
        let mut session = SessionPlaylist::default();
        let shared = PlaylistSidecar {
            segments: (40..50i64)
                .map(|i| seg(&format!("live{i:06}.ts"), "game", i * 4, i == 40))
                .collect(),
            pipelines: vec![pipeline("game", 160_000, true)],
        };
        let variant = PlaylistSidecar {
            segments: (0..10i64)
                .map(|i| seg(&format!("live{i:06}.ts"), "game", 160 + i * 4, i == 0))
                .collect(),
            pipelines: vec![pipeline("game", 160_000, true)],
        };
        session.advance_and_render(
            &shared,
            Some(&variant),
            "variants/x/",
            Some(46),
            OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(204),
            4,
            |s| s.to_owned(),
        );
        assert_eq!(session.item_bases.get("game"), Some(&40));
        assert_eq!(session.decisions.get("game"), Some(&variant_decision(0, 0)));
        assert_eq!(session.entries.len(), 10);
        session
    }

    /// The stale-base failure the renumber reset must not leave behind: the
    /// shared session restarts, numbers from zero, and re-airs the same item
    /// id. A base recorded in the old space is huge against the new
    /// numbering, so `saturating_sub` collapses every re-aired position to
    /// zero, and zero names the same variant twin at every position: one
    /// 4-second segment aliased across the whole item. The maps must die
    /// with the numbering they were measured in, so positions derive from a
    /// base recorded in the new space.
    #[test]
    fn a_renumber_rerecords_the_base_so_a_reaired_item_gets_its_own_twins() {
        let mut session = session_decided_variant_in_the_40_space();

        // the restarted session re-airs the item from sequence zero, with
        // fresh pts envelopes and a fresh anchored variant
        let shared = PlaylistSidecar {
            segments: (0..10i64)
                .map(|i| seg(&format!("live{i:06}.ts"), "game", 400 + i * 4, i == 0))
                .collect(),
            pipelines: vec![pipeline("game", 8_000, true)],
        };
        let variant = PlaylistSidecar {
            segments: (0..10i64)
                .map(|i| seg(&format!("live{i:06}.ts"), "game", 400 + i * 4, i == 0))
                .collect(),
            pipelines: vec![pipeline("game", 8_000, true)],
        };

        session.advance_and_render(
            &shared,
            Some(&variant),
            "variants/x/",
            Some(0),
            OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(440),
            4,
            |s| s.to_owned(),
        );
        // the reset tick: decide_items ran before the reset block and
        // consulted the stale maps, and the reset dropped them anyway
        assert!(session.decisions.is_empty());
        assert!(session.item_bases.is_empty());

        // next tick: per-item state re-forms from the new sidecar
        session.advance_and_render(
            &shared,
            Some(&variant),
            "variants/x/",
            Some(0),
            OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(442),
            4,
            |s| s.to_owned(),
        );

        assert_eq!(session.item_bases.get("game"), Some(&0));
        let paths: Vec<String> = session.entries.iter().map(|e| e.path.clone()).collect();
        let expected: Vec<String> = (0..9i64)
            .map(|i| format!("variants/x/live{i:06}.ts"))
            .collect();
        assert_eq!(
            paths, expected,
            "each re-aired position substitutes its own twin"
        );
    }

    /// A final `Variant` decision is a join and an anchor measured against
    /// pts envelopes the restart discarded. After the renumber the same item
    /// re-airs with no variant published at all; the decision window must
    /// re-arm from the new airing's first segment and pin shared, instead of
    /// the dead decision driving twin arithmetic against files that no
    /// longer exist.
    #[test]
    fn a_stale_variant_decision_does_not_survive_the_renumber() {
        let mut session = session_decided_variant_in_the_40_space();

        let shared = PlaylistSidecar {
            segments: (0..10i64)
                .map(|i| seg(&format!("live{i:06}.ts"), "game", 400 + i * 4, i == 0))
                .collect(),
            pipelines: vec![pipeline("game", 8_000, true)],
        };

        // both ticks run past the re-armed deadline (the first new segment's
        // pdt + 12s), so the tick after the reset pins shared
        for step in 0..2i64 {
            session.advance_and_render(
                &shared,
                None,
                "variants/x/",
                Some(0),
                OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(440 + step * 2),
                4,
                |s| s.to_owned(),
            );
        }

        assert_eq!(session.decisions.get("game"), Some(&ItemDecision::Shared));
        assert_eq!(session.entries.back().map(|e| e.sequence), Some(8));
        assert!(
            session.entries.iter().all(|e| !e.variant),
            "the re-aired item serves the new shared feed"
        );
    }

    /// The boundary of the renumber rule. `reconcile`'s re-anchor fires in
    /// the SAME numbering space, when every sequence still needed has aged
    /// out of the timeline: the bases were recorded once precisely to
    /// survive that trimming, and the decisions still measure the live
    /// pipelines. The re-anchor must keep both maps where the renumber reset
    /// drops them, or a deep trim would shift every position of a mid-air
    /// item by the trimmed amount.
    #[test]
    fn a_reanchor_keeps_the_maps_a_renumber_drops() {
        let mut session = session_decided_variant_in_the_40_space();

        // numbering continues far past everything held; nothing regressed,
        // the history in between simply aged out
        let shared = PlaylistSidecar {
            segments: (200..210i64)
                .map(|i| seg(&format!("live{i:06}.ts"), "game", i * 4, i == 200))
                .collect(),
            pipelines: vec![pipeline("game", 160_000, true)],
        };
        // the twins for those positions, numbered from the ORIGINAL base:
        // 200 minus 40 puts the window at twins 160 through 169
        let variant = PlaylistSidecar {
            segments: (160..170i64)
                .map(|i| seg(&format!("live{i:06}.ts"), "game", (40 + i) * 4, false))
                .collect(),
            pipelines: vec![pipeline("game", 160_000, true)],
        };

        session.advance_and_render(
            &shared,
            Some(&variant),
            "variants/x/",
            Some(205),
            OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(850),
            4,
            |s| s.to_owned(),
        );

        assert_eq!(session.head_sequence, 200, "the session re-anchored");
        assert_eq!(session.item_bases.get("game"), Some(&40));
        assert_eq!(session.decisions.get("game"), Some(&variant_decision(0, 0)));
        assert_eq!(
            session.entries.front().map(|e| e.path.as_str()),
            Some("variants/x/live000160.ts"),
            "positions still derive from the base recorded before the trim"
        );
    }

    /// Composition resumes at the first position not yet held, mid
    /// substitution: the resumed twin must join without a spurious
    /// discontinuity, exactly as a continuous walk would have tagged it.
    #[test]
    fn resuming_mid_substitution_keeps_the_join_continuous() {
        let mut session = SessionPlaylist::default();
        let shared = shared_with_templated_item();
        session
            .decisions
            .insert(String::from("game"), variant_decision(0, 0));
        let base = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(20);

        // only the first twin exists yet: composition holds at position 3
        let first_twin = PlaylistSidecar {
            segments: vec![seg("live000000.ts", "game", 0, true)],
            pipelines: vec![pipeline("game", 8_000, true)],
        };
        session.advance_and_render(
            &shared,
            Some(&first_twin),
            "variants/x/",
            Some(0),
            base,
            4,
            |s| s.to_owned(),
        );
        assert_eq!(session.entries.back().map(|e| e.sequence), Some(2));
        assert!(session.entries.back().is_some_and(|e| e.variant));

        // the second twin arrives; the session resumes from position 3
        session.advance_and_render(
            &shared,
            Some(&variant_for_game()),
            "variants/x/",
            Some(0),
            base + time::Duration::seconds(2),
            4,
            |s| s.to_owned(),
        );

        let resumed = session
            .entries
            .iter()
            .find(|e| e.sequence == 3)
            .expect("position 3 composed");
        assert!(resumed.variant);
        assert!(
            !resumed.discontinuity,
            "mid-substitution resume must not tag a discontinuity"
        );
    }

    /// A head that has reached the newest position its timeline can serve
    /// from has nowhere to go, and the wait must cost nothing in either
    /// direction: the pacing clock is not restarted (which would stop the
    /// head from ever completing a step) and the wait is not banked as
    /// credit (which would be spent as one jump across everything the hold
    /// withheld, and what a hold withholds is content).
    /// The livelock of 2026-08-10, pinned as the invariant that prevents it
    /// rather than as one scenario.
    ///
    /// The hard-lag branch resets the pacing clock whenever it moves the
    /// head. That is only safe while a branch that CANNOT move the head
    /// implies the playback-rate walk had nowhere to go either, because a
    /// reset fires every tick (2s) and the walk needs a segment duration
    /// (4s) of elapsed time to step: a trim that stalls while the walk is
    /// still live starves the walk forever and freezes the head, which on
    /// the night skipped about a minute of program content once the head
    /// finally jumped.
    ///
    /// Both quantities come from `serve_bounds`, so the implication holds by
    /// construction. This asserts it over the whole state space so a future
    /// change that decouples them fails here instead of on a live channel.
    #[test]
    fn a_stalled_trim_implies_a_stalled_walk() {
        let shared_heads = [
            None,
            Some(0),
            Some(3),
            Some(9),
            Some(25),
            Some(60),
            Some(4000),
        ];

        for tail in 0..48u64 {
            for front in 0..=tail.min(6) {
                for shared_head in shared_heads {
                    let bounds = serve_bounds(front, tail, shared_head);

                    for head in front..=tail {
                        // what the hard-lag branch would force the head to
                        let target = bounds.own_window.max(head);
                        // what the playback-rate walk would advance it to
                        let upper = bounds.desired.max(head).min(tail);

                        if target <= head {
                            assert!(
                                upper <= head,
                                "trim stalled at head {head} while the walk could still \
                                 reach {upper} (front {front}, tail {tail}, \
                                 shared_head {shared_head:?}): the trim's clock reset \
                                 would starve the walk and freeze the head"
                            );
                        }

                        // and a cohort never targets content beyond what it holds
                        assert!(
                            bounds.desired <= tail && target <= tail,
                            "targeted past the composed edge (front {front}, tail {tail})"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn a_head_with_nowhere_to_go_neither_restarts_nor_banks_its_clock() {
        let mut session = SessionPlaylist::default();
        let held = PlaylistSidecar {
            segments: (0..8i64)
                .map(|i| seg(&format!("live{i:06}.ts"), "show", i * 4, i == 0))
                .collect(),
            pipelines: vec![pipeline("show", 0, false)],
        };
        let step = time::Duration::seconds(SEGMENT_SECONDS as i64);
        let mut at = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(100);

        // 60 seconds of composer ticks with the shared head far past the
        // hard bound the whole way
        for _ in 0..30 {
            session.advance_and_render(&held, None, "variants/x/", Some(40), at, 4, |s| {
                s.to_owned()
            });
            assert_eq!(session.serve_head, Some(0), "nothing to advance onto");
            assert!(
                at - session.head_advanced_at.unwrap() <= step,
                "a parked head may hold at most one segment of pacing credit"
            );
            at += time::Duration::seconds(2);
        }
    }

    /// When a hold ends, the head resumes at playback rate. The positions a
    /// hold withheld are content, not lag, so paying the wait out as one
    /// jump would skip them for every viewer on the cohort.
    #[test]
    fn a_released_hold_resumes_at_playback_rate() {
        let mut session = SessionPlaylist::default();
        let held = PlaylistSidecar {
            segments: (0..8i64)
                .map(|i| seg(&format!("live{i:06}.ts"), "show", i * 4, i == 0))
                .collect(),
            pipelines: vec![pipeline("show", 0, false)],
        };
        // the shared head stays inside both lag bounds, so nothing here is
        // a trim: this is the walk on its own
        let mut at = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(100);
        for _ in 0..30 {
            session
                .advance_and_render(&held, None, "variants/x/", Some(5), at, 4, |s| s.to_owned());
            at += time::Duration::seconds(2);
        }
        assert_eq!(session.serve_head, Some(0));

        // composition unblocks in a burst: 22 further positions arrive at
        // once. the head takes one, not the 15 that 60 seconds of banked
        // credit would have bought
        let grown = PlaylistSidecar {
            segments: (0..30i64)
                .map(|i| seg(&format!("live{i:06}.ts"), "show", i * 4, i == 0))
                .collect(),
            pipelines: vec![pipeline("show", 0, false)],
        };
        let rendered =
            session.advance_and_render(&grown, None, "variants/x/", Some(5), at, 4, |s| {
                s.to_owned()
            });
        assert_eq!(session.serve_head, Some(1));
        assert!(rendered.contains("#EXT-X-MEDIA-SEQUENCE:1\n"));
    }

    /// A variant that stops producing degrades to the shared feed. Holding
    /// is right while the variant is merely late, but a hold that never
    /// ends freezes the cohort's timeline, and with it the serve head.
    #[test]
    fn a_stalled_variant_falls_back_to_the_shared_feed() {
        let shared = long_shared_with_templated_item(30);
        // the item's first two positions were produced, then nothing
        let variant = long_variant_for_game(2);
        let compose = |horizon_secs: i64| {
            compose_timeline(
                &shared,
                Some(&variant),
                "variants/abc/",
                "",
                &decided("game", variant_decision(0, 0)),
                &bases_of(&shared),
                ComposeResume::default(),
                OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(horizon_secs),
            )
        };

        // inside the threshold the timeline holds at the missing twin,
        // exactly as it does for a variant that is only late
        let holding = compose(44);
        assert_eq!(holding.last().map(|e| e.sequence), Some(7));
        assert!(holding.last().is_some_and(|e| e.variant));

        // once shared coverage runs VARIANT_STALL_SECONDS past the hole,
        // the position falls back to the shared segment and the timeline
        // moves again
        let recovering = compose(48);
        let last = recovering.last().expect("timeline is not empty");
        assert_eq!(last.sequence, 8);
        assert_eq!(last.path, "live000008.ts");
        assert!(!last.variant);
        assert!(last.discontinuity, "the source switch is spliced");

        // and it keeps moving: the stall does not cap the timeline short of
        // the shared feed it fell back to
        assert_eq!(compose(60).last().map(|e| e.sequence), Some(15));
    }

    /// `anchor_ms` IS the progress the variant was spawned with. The worker
    /// stamps `pts_offset_ms + progress_ms` on its output, the composer reads
    /// `variant.pts_offset_ms - shared.pts_offset_ms` back off the sidecar,
    /// so the two are the same number by construction.
    ///
    /// Also: a 60000ms join can be entirely HONEST. Here the session really
    /// did compose 15 positions of the item before the variant anchored, with
    /// a base that is the item's own first position, so nothing about the
    /// span is fictional. Which of the two produced the 2026-08-12 ch15 join
    /// is not settled here and needs the join-arithmetic line from f24da71.
    #[test]
    fn the_variant_anchor_is_the_progress_it_was_spawned_with() {
        let shared = shared_with_long_templated_item();

        let composed_through = |session: &mut SessionPlaylist, last: u64| {
            for sequence in 6..=last {
                session.entries.push_back(ComposedEntry {
                    path: format!("live{sequence:06}.ts"),
                    duration: 4.0,
                    program_date_time: OffsetDateTime::UNIX_EPOCH
                        + time::Duration::seconds(sequence as i64 * 4),
                    discontinuity: sequence == 6,
                    sequence,
                    variant: false,
                });
            }
            session.head_sequence = 6;
        };

        let decide = |progress_ms: u64| {
            let mut session = SessionPlaylist::default();
            composed_through(&mut session, 20);
            session.decide_items(
                &shared,
                Some(&long_variant(1, progress_ms)),
                OffsetDateTime::UNIX_EPOCH,
            );
            let Some(ItemDecision::Variant { join_ms, anchor_ms }) =
                session.decisions.get("game").copied()
            else {
                panic!("the item is anchored, so it must decide Variant");
            };
            (join_ms, anchor_ms)
        };

        // today: the fallback shortcut spawns at progress 0, so the variant
        // claims the item's first position while the session has served 15
        let (join_ms, anchor_ms) = decide(0);
        assert_eq!(anchor_ms, 0, "progress 0 anchors at the item start");
        assert_eq!(
            join_ms, 60_000,
            "15 positions were composed off a base that is the item's own \
             first position, so the span is honest"
        );

        // spawned at the depth the session had already reached, the anchor
        // follows the progress exactly
        let (join_ms, anchor_ms) = decide(60_000);
        assert_eq!(anchor_ms, 60_000, "the anchor is the spawn progress");
        assert_eq!(join_ms, 60_000, "the join is unchanged: it is not moved");
    }

    /// THE JOIN IS A DISPLACEMENT, and nothing else. It measures the distance
    /// between where the variant anchors and where the composer's numbering
    /// has already reached when the variant's first segment appears:
    ///
    ///     join_ms = (T_anchor - PDT_item_first - COMPOSE_TRAIL) * 1000
    ///
    /// Held exactly here across a 72 second spread, slope exactly 1, with
    /// `anchor_ms` pinned at 0 throughout. Nothing about the variant's own
    /// content moves it. That is why a large join is never evidence that the
    /// composer's sums are wrong: it is a faithful readout of how far apart
    /// the two axes had drifted.
    ///
    /// Those axes are the whole defect. The variant air-locks on
    /// `item.start + join_offset`, a SCHEDULE-axis time
    /// (`channel_session::run_variant`), while the composer numbers positions
    /// from the shared session's PDT, a PRODUCTION-axis time. Whenever
    /// production is displaced from the authored schedule the two disagree,
    /// and the disagreement lands here in full.
    ///
    /// The 2026-08-12 events are two different ways into the same
    /// displacement. ch11 12206355 got there through a 27.5s production lag
    /// (a 5760x4320 source that defeated NVDEC and decoded at 0.26x). ch15
    /// 12216957 got there through a 58s `join_offset`, the shared session
    /// having joined a 198s item 58s in. The 60s row below reproduces the
    /// 60000ms that both ch15 and ch11 12206607 logged.
    ///
    /// Note the one-tick lag: `decide_items` reads `emitted` from the entries
    /// as they stood on the PREVIOUS tick, so the join trails the horizon by
    /// exactly one segment. That is why there is no `+ 1` here.
    #[test]
    fn the_join_is_the_displacement_between_the_two_axes() {
        let shared = shared_with_long_templated_item();

        // the templated item's first position carries pdt 24s, and the walk
        // stops COMPOSE_TRAIL_SECONDS behind the live edge
        const ITEM_FIRST_PDT_SECS: i64 = 24;

        let join_at = |anchor_secs: i64| -> u64 {
            let mut session = SessionPlaylist::default();

            // the variant is missing, so the cohort is served shared and the
            // composed timeline walks into the item one tick at a time
            let mut t = ITEM_FIRST_PDT_SECS;
            while t < anchor_secs {
                session.advance_and_render(
                    &shared,
                    None,
                    "variants/abc/",
                    Some(0),
                    OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(t),
                    4,
                    |s| s.to_owned(),
                );
                t += 4;
            }

            // the variant's first segment becomes visible, anchored at the
            // item start because a fallback pipeline spawns at progress 0
            session.advance_and_render(
                &shared,
                Some(&long_variant(1, 0)),
                "variants/abc/",
                Some(0),
                OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(anchor_secs),
                4,
                |s| s.to_owned(),
            );

            let Some(ItemDecision::Variant { join_ms, anchor_ms }) =
                session.decisions.get("game").copied()
            else {
                panic!("the variant is anchored at t={anchor_secs}, so it must decide Variant");
            };
            assert_eq!(
                anchor_ms, 0,
                "the anchor stays at the item start; only the displacement moves"
            );
            join_ms
        };

        for anchor_secs in [48i64, 60, 72, 92, 120] {
            let displacement_ms =
                ((anchor_secs - ITEM_FIRST_PDT_SECS - COMPOSE_TRAIL_SECONDS as i64) * 1000) as u64;
            assert_eq!(
                join_at(anchor_secs),
                displacement_ms,
                "at T_anchor={anchor_secs}s the join must equal the displacement"
            );
        }

        // slope exactly 1: every second of displacement is a second of join,
        // so nothing amplifies it
        assert_eq!(join_at(92) - join_at(72), 20_000);
        assert_eq!(join_at(120) - join_at(92), 28_000);

        // and the shape of the live events: 60s of displacement is the
        // 60000ms logged on ch15 12216957 and ch11 12206607
        assert_eq!(join_at(92), 60_000);
    }

    /// REPRODUCTION of the lost window. A variant anchored at 0 is asked for
    /// the twin of a join it did not start at, that twin is its segment
    /// `join_ms / 4000`, and it will not reach that index for another
    /// `join_ms` of wall clock because both sides advance at 1x. The composer
    /// gives up after `VARIANT_STALL_SECONDS` and serves shared for the rest,
    /// so the cohort gets NO variant frames at all.
    ///
    /// This is the 2026-08-12 ch15 shape: anchor 0ms, join 60000ms, unmet
    /// demands for segments 15 through 30+ across the whole 140000ms window.
    #[test]
    fn a_variant_anchored_at_zero_cannot_serve_a_join_past_its_start() {
        let shared = shared_with_long_templated_item();
        // the variant has started and is producing, it is simply at the
        // beginning of its own output
        let variant = long_variant(3, 0);

        let composed = compose_timeline(
            &shared,
            Some(&variant),
            "variants/abc/",
            "",
            &decided("game", variant_decision(60_000, 0)),
            &bases_of(&shared),
            ComposeResume::default(),
            OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(200),
        );

        // by the same arithmetic `render` uses for the twin index
        let anchor_ms: u64 = 0;
        let twin_demanded = (60_000 + 750 - anchor_ms) / (1000 * SEGMENT_SECONDS);
        assert_eq!(twin_demanded, 15, "the join names segment 15");
        assert!(
            variant.segments.len() <= twin_demanded as usize,
            "which the variant has not produced"
        );

        // NOT vacuous: the window really is composed, and every position of
        // it past the join is present. It is simply all shared
        let item_positions: Vec<u64> = composed
            .iter()
            .filter(|e| (6..=40).contains(&e.sequence))
            .map(|e| e.sequence)
            .collect();
        assert_eq!(
            item_positions.len(),
            35,
            "the whole 140000ms window is composed"
        );
        assert!(
            item_positions.contains(&21),
            "including the position the join names"
        );

        assert!(
            !composed.iter().any(|e| e.variant),
            "TODAY'S BEHAVIOUR: the whole window is served from shared, so \
             the cohort receives no variant frames: {:?}",
            composed
                .iter()
                .filter(|e| e.variant)
                .map(|e| e.sequence)
                .collect::<Vec<_>>()
        );
    }

    /// The CONTROL for the reproduction above, differing in one value: the
    /// variant was spawned at the depth the composer had already reached, so
    /// `anchor_ms` equals the join and the demanded twin is its FIRST
    /// segment. Same variant output, same join, window served.
    ///
    /// This is the composer-side case for spawning a fallback variant at the
    /// item's real air progress instead of 0. It does NOT show that the
    /// worker can produce correct content at that position from a live
    /// source; that is `channel_session::run_variant`'s side and is not
    /// covered here.
    #[test]
    fn a_variant_anchored_at_the_join_serves_it_from_its_first_segment() {
        let shared = shared_with_long_templated_item();
        let variant = long_variant(3, 60_000);

        let composed = compose_timeline(
            &shared,
            Some(&variant),
            "variants/abc/",
            "",
            &decided("game", variant_decision(60_000, 60_000)),
            &bases_of(&shared),
            ComposeResume::default(),
            OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(200),
        );

        let served: Vec<_> = composed
            .iter()
            .filter(|e| e.variant)
            .map(|e| (e.sequence, e.path.as_str()))
            .collect();

        assert_eq!(
            served,
            vec![
                (21, "variants/abc/live000000.ts"),
                (22, "variants/abc/live000001.ts"),
                (23, "variants/abc/live000002.ts"),
            ],
            "the join is served from the variant's first segment onward"
        );
    }
}
