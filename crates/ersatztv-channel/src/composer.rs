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

use ersatztv_core::sidecar::PlaylistSidecar;
use time::OffsetDateTime;
use time::macros::format_description;

/// Mirror of the worker's segment length; used only for envelope grid
/// arithmetic, exactly as the worker anchors its own playlist.
pub const SEGMENT_SECONDS: u64 = 4;

const SERVED_SEGMENTS: usize = 10;

/// How long an entry stays in session history after leaving the serve window.
const HISTORY_SECONDS: u64 = 120;

/// An item's decision is forced this close to the serve-window edge if the
/// variant still has no output; earlier, composition holds back instead.
const DECISION_LEAD_SECONDS: u64 = 8;

/// While the timeline is still being produced, the head stays this many
/// segments behind the newest composed entry, so the served window keeps
/// the three-target-durations of media rfc8216bis 6.2.2 requires. Worth its
/// cost in delay: a window at the emission edge serves one segment, and a
/// player cannot buffer one segment.
const EDGE_HOLD_SEGMENTS: u64 = 3;

/// How far the serve head may fall behind the shared playlist's own head
/// before it jumps forward instead of walking. A variant transcode of a live
/// source cannot outrun realtime, so a late start makes the cohort's timeline
/// lag; walking through the lag plays everything a little late, which is the
/// preferred failure. Past this bound the head skips to the newest composed
/// content, trading the skipped span for a bounded worst-case delay.
const MAX_LAG_SEGMENTS: u64 = 10;

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
    /// over shared content to catch up with the shared playlist, but never
    /// over variant content: that content is why the cohort exists, so a
    /// lagging viewer plays all of it a little late instead of losing it.
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
    decisions: HashMap<String, ItemDecision>,
    /// The sequence number of the first segment ever observed for each
    /// templated item, recorded once and never recomputed. Positions inside
    /// an item derive from this: the sidecar trims its history, so anything
    /// measured from "the first segment still listed" shifts as the item
    /// ages, and every position-based decision would shift with it.
    item_bases: HashMap<String, u64>,
}

fn parse_pdt(input: &str) -> Option<OffsetDateTime> {
    let format = format_description!(
        "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3][offset_hour sign:mandatory][offset_minute]"
    );
    OffsetDateTime::parse(input, format).ok()
}

fn format_pdt(pdt: OffsetDateTime) -> String {
    let format = format_description!(
        "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3][offset_hour sign:mandatory][offset_minute]"
    );
    pdt.format(format).unwrap_or_default()
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
        let timeline = compose_timeline(
            shared,
            variant,
            variant_prefix,
            &self.decisions,
            &self.item_bases,
        );
        self.reconcile(timeline);
        self.trim(now);
        self.render(shared_head, now, target_duration, map_path)
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
                    first_unserved_ms =
                        seq.saturating_sub(base).saturating_add(1) * 1000 * SEGMENT_SECONDS;
                }

                self.decisions.insert(
                    pipeline.item_id.clone(),
                    ItemDecision::Variant {
                        join_ms: anchor_ms.max(first_unserved_ms),
                        anchor_ms,
                    },
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
                .find(|s| s.item_id == pipeline.item_id)
                .and_then(|s| parse_pdt(&s.program_date_time));

            let Some(item_start) = first_shared else {
                // no shared output for the item yet; nothing to serve either
                // way, so wait
                continue;
            };

            // hold the decision open while there is still time for the
            // variant to produce output. viewers play a full serve window
            // behind the live edge, so the decision can stay open until
            // shortly AFTER the item's first segment pdt and still be
            // pinned before any viewer's playlist reaches the boundary
            let deadline = item_start
                + time::Duration::seconds(
                    (SEGMENT_SECONDS * 5) as i64 - DECISION_LEAD_SECONDS as i64,
                );
            if now >= deadline {
                self.decisions
                    .insert(pipeline.item_id.clone(), ItemDecision::Shared);
            }
        }
    }

    /// Appends timeline entries this session has not emitted yet. Entries are
    /// identified by sequence; history is never reordered or rewritten, so
    /// every client sees an append-only playlist.
    fn reconcile(&mut self, timeline: Vec<ComposedEntry>) {
        // a timeline whose newest entry precedes everything held means the
        // shared session restarted and renumbered from zero; composed state
        // has to follow it rather than hold a stale history forever
        if let (Some(newest), Some(front)) = (timeline.last(), self.entries.front())
            && newest.sequence < front.sequence
        {
            log::warn!("shared playlist numbering moved backwards; resetting composed session");
            self.entries.clear();
            self.serve_head = None;
            self.head_advanced_at = None;
            self.head_discontinuity_sequence = 0;
        }

        // a sequence this session still needs but the timeline can no longer
        // provide will never arrive; re-anchoring to current content is one
        // clean break for the viewer, where holding would freeze the
        // playlist for good
        if let (Some(oldest), Some(last)) = (timeline.first(), self.entries.back())
            && oldest.sequence > last.sequence + 1
        {
            log::warn!(
                "sequence {} is no longer available (timeline now starts at {}); \
                 re-anchoring composed session",
                last.sequence + 1,
                oldest.sequence
            );
            self.entries.clear();
            self.serve_head = None;
            self.head_advanced_at = None;
            self.head_discontinuity_sequence = 0;
        }

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
                    log::warn!(
                        "composed timeline skipped sequence {} to {}; holding",
                        last.sequence + 1,
                        entry.sequence
                    );
                    break;
                }
                // an already-emitted position (or a conflicting twin of one)
                // never re-enters history
                _ => {}
            }
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

        // the position the shared playlist serves from; without one to
        // mirror, fall back to this timeline's own last full window
        let desired = shared_head
            .unwrap_or_else(|| tail.saturating_sub(SERVED_SEGMENTS as u64 - 1).max(front));

        // while emission is still catching up to the shared head, the head
        // may only reach this far, holding a buffer window open at the edge
        let reachable = if desired > tail {
            tail.saturating_sub(EDGE_HOLD_SEGMENTS).max(front)
        } else {
            tail
        };

        let mut head = match self.serve_head {
            Some(head) => head.clamp(front, tail),
            None => {
                self.head_advanced_at = Some(now);
                desired.clamp(front, reachable)
            }
        };

        // walk forward at playback rate, one segment per segment duration,
        // never past the shared head or the newest composed content: a
        // lagging cohort plays through its backlog instead of having the
        // window jump over it, and a cohort never runs ahead of the shared
        // playlist into worked-ahead content
        let upper = reachable.min(desired.max(head)).max(head);
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
            self.head_advanced_at = Some(advanced_at);
        }

        // a head behind the shared one stays behind: every unplayed entry is
        // content, and what follows a window on a real channel is the next
        // program, whose beginning a jump would eat. the lag does not grow
        // window over window, since the next variant has produced by the time
        // a lagging viewer reaches it; only past the bound is a skip taken,
        // trading content for a bounded worst-case delay
        if desired > head && desired - head > MAX_LAG_SEGMENTS {
            let target = desired.min(reachable).max(head);
            log::warn!(
                "composed serve head {head} fell more than {MAX_LAG_SEGMENTS} segments \
                 behind shared head {desired}; skipping to {target}"
            );
            head = target;
            self.head_advanced_at = Some(now);
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
pub fn compose_timeline(
    shared: &PlaylistSidecar,
    variant: Option<&PlaylistSidecar>,
    variant_prefix: &str,
    decisions: &HashMap<String, ItemDecision>,
    item_bases: &HashMap<String, u64>,
) -> Vec<ComposedEntry> {
    let templated: HashMap<&str, bool> = shared
        .pipelines
        .iter()
        .map(|p| (p.item_id.as_str(), p.templated))
        .collect();

    let grid_ms = 1000 * SEGMENT_SECONDS;
    let mut result: Vec<ComposedEntry> = Vec::new();
    let mut substituting = false;

    for segment in &shared.segments {
        let Some(pdt) = parse_pdt(&segment.program_date_time) else {
            continue;
        };
        let Some(sequence) = sequence_of(&segment.path) else {
            continue;
        };

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
                        // behind that holding would stall viewers
                        let variant_edge_ms = *anchor_ms + twin * grid_ms;
                        let behind_ms = (position_ms + grid_ms).saturating_sub(variant_edge_ms);
                        if (behind_ms as f64) < VARIANT_STALL_SECONDS * 1000.0 {
                            return result;
                        }

                        log::debug!(
                            "variant for item {} stalled {}ms behind; serving shared position {}",
                            segment.item_id,
                            behind_ms,
                            position_ms
                        );
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
                    return result;
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

    result
}

#[cfg(test)]
mod tests {
    use ersatztv_core::sidecar::{SidecarPipeline, SidecarSegment};

    use super::*;

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

    #[test]
    fn substitutes_variant_segments_for_the_templated_item() {
        let shared = shared_with_templated_item();
        let variant = variant_for_game();

        let timeline = compose_timeline(
            &shared,
            Some(&variant),
            "variants/abc/",
            &decided("game", variant_decision(0, 0)),
            &bases_of(&shared),
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
            &decided("game", variant_decision(0, 0)),
            &bases_of(&shared),
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
            &decided("game", variant_decision(0, 0)),
            &bases_of(&shared),
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
            &decided("game", variant_decision(0, 0)),
            &bases_of(&shared),
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
            &decided("game", ItemDecision::Shared),
            &bases_of(&shared),
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
            &HashMap::new(),
            &bases_of(&shared),
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
            &decided("game", variant_decision(0, 0)),
            &bases_of(&shared),
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
        let late = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(8 + 12);
        session.decide_items(&shared, None, late);
        assert_eq!(session.decisions.get("game"), Some(&ItemDecision::Shared));
    }

    #[test]
    fn variant_anchored_before_the_item_is_not_chosen() {
        let mut session = SessionPlaylist::default();
        let shared = shared_with_templated_item();
        let mut variant = variant_for_game();
        variant.pipelines[0].pts_offset_ms = 7_000;

        let late = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(8 + 12);
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
        let late = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(8 + 12);
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
        let late = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(8 + 12);

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
            &decided("game", variant_decision(4_000, 4_000)),
            &bases_of(&shared),
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
        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(20);

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
        let shared = shared_with_templated_item();
        let variant = variant_for_game();
        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(20);

        let rendered = session.advance_and_render(
            &shared,
            Some(&variant),
            "variants/abc/",
            Some(2),
            now,
            4,
            |s| s.to_owned(),
        );

        assert!(rendered.contains("#EXT-X-MEDIA-SEQUENCE:2\n"));
        assert!(rendered.contains("variants/abc/live000000.ts"));
        assert!(!rendered.contains("\nlive000001.ts"));
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
        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(8);

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

    /// Past the lag bound the head jumps to the newest composed content: a
    /// bounded worst-case delay is preferred over an unbounded one.
    #[test]
    fn a_head_past_the_lag_bound_jumps_to_the_emission_edge() {
        let mut session = SessionPlaylist::default();
        let shared = continuous_shared_with_templated_item();
        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(20);

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

        assert!(rendered.contains("#EXT-X-MEDIA-SEQUENCE:12\n"));
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
        let base = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(20);

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

    /// A fresh session joining a lagging timeline starts a buffer window
    /// behind the emission edge, not at it: a one-segment playlist gives a
    /// player nothing to buffer.
    #[test]
    fn a_fresh_session_holds_a_window_open_at_the_emission_edge() {
        let mut session = SessionPlaylist::default();
        let shared = continuous_shared_with_templated_item();
        session
            .decisions
            .insert(String::from("game"), ItemDecision::Shared);
        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(20);

        let rendered =
            session.advance_and_render(&shared, None, "variants/x/", Some(30), now, 4, |s| {
                s.to_owned()
            });

        assert!(rendered.contains("#EXT-X-MEDIA-SEQUENCE:12\n"));
        assert!(rendered.contains("live000012.ts"));
        assert!(rendered.contains("live000015.ts"));
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

        let before = compose_timeline(&shared, Some(&variant), "variants/abc/", &decisions, &bases);

        // the item's first segment ages out of the sidecar
        let mut trimmed = shared.clone();
        trimmed.segments.remove(2);
        let after = compose_timeline(
            &trimmed,
            Some(&variant),
            "variants/abc/",
            &decisions,
            &bases,
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
            &decided("game", variant_decision(4_000, 0)),
            &bases_of(&shared),
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
        let shared = shared_with_templated_item();
        let variant = variant_for_game();

        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(20);
        session.advance_and_render(
            &shared,
            Some(&variant),
            "variants/abc/",
            Some(0),
            now,
            4,
            |s| s.to_owned(),
        );

        // much later the shared head has moved to the end; the head walks
        // there (through the variant span, which is never jumped) and the
        // window discontinuity count reflects the rolled-off splice
        let later = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(500);
        let rendered = session.advance_and_render(
            &shared,
            Some(&variant),
            "variants/abc/",
            Some(4),
            later,
            4,
            |s| s.to_owned(),
        );

        assert!(rendered.contains("#EXT-X-MEDIA-SEQUENCE:4\n"));
        // the splice-in discontinuity is behind the head now
        assert!(rendered.contains("#EXT-X-DISCONTINUITY-SEQUENCE:1\n"));
        assert!(rendered.contains("live000004.ts"));
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
}
