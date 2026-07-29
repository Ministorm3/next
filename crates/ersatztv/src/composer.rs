//! Per-cohort playlist composition.
//!
//! A cohort's playlist is the shared session's playlist with a templated
//! item's segments replaced by the cohort's variant segments, keyed by the
//! playout item ids recorded in each session's sidecar. Substitution is
//! all-or-nothing per item per session: the decision (variant or shared) is
//! made once, just before the item enters the served window, and pinned, so a
//! viewer never switches sources mid-item. Everything here operates on
//! recorded metadata; nothing is inferred from media timestamps.

use std::collections::{HashMap, VecDeque};

use ersatztv_core::sidecar::PlaylistSidecar;
use time::OffsetDateTime;
use time::macros::format_description;

/// Mirror of the worker's segment length; used only for serve-window
/// anchoring, exactly as the worker anchors its own playlist.
pub const SEGMENT_SECONDS: u64 = 4;

const SERVED_SEGMENTS: usize = 10;

/// How long an entry stays in session history after leaving the serve window.
const HISTORY_SECONDS: u64 = 120;

/// An item's decision is forced this close to the serve-window edge if the
/// variant still has no output; earlier, composition holds back instead.
const DECISION_LEAD_SECONDS: u64 = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemDecision {
    Variant,
    Shared,
}

#[derive(Debug, Clone)]
pub struct ComposedEntry {
    pub path: String,
    pub duration: f64,
    pub program_date_time: OffsetDateTime,
    pub discontinuity: bool,
    pub item_id: String,
}

/// Per-cohort-session playlist state. Entries are append-only; the head trims
/// forward as segments age out, advancing the media sequence exactly as
/// rfc8216bis 6.2.2 requires of a live playlist.
#[derive(Debug, Default)]
pub struct SessionPlaylist {
    entries: VecDeque<ComposedEntry>,
    head_media_sequence: u64,
    head_discontinuity_sequence: u64,
    last_served_media_sequence: u64,
    decisions: HashMap<String, ItemDecision>,
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

impl SessionPlaylist {
    /// Advances this session's timeline from the two sidecars and renders the
    /// playlist to serve. `variant_prefix` is the path prefix (relative to the
    /// shared session folder) under which the variant's segments are served.
    /// `map_path` converts a segment path for the rendered playlist (identity
    /// for media, `.ts` to `.vtt` for subtitles).
    pub fn advance_and_render(
        &mut self,
        shared: &PlaylistSidecar,
        variant: Option<&PlaylistSidecar>,
        variant_prefix: &str,
        now: OffsetDateTime,
        target_duration: u32,
        map_path: fn(&str) -> String,
    ) -> String {
        self.decide_items(shared, variant, now);
        let timeline = compose_timeline(shared, variant, variant_prefix, &self.decisions);
        self.reconcile(timeline);
        self.trim(now);
        self.render(now, target_duration, map_path)
    }

    /// Pins a decision for each templated item approaching the serve window.
    /// The variant wins if it has produced output covering the item's start;
    /// once the item is about to be served with no variant output, the shared
    /// feed wins. Either way the decision never changes afterwards.
    fn decide_items(
        &mut self,
        shared: &PlaylistSidecar,
        variant: Option<&PlaylistSidecar>,
        now: OffsetDateTime,
    ) {
        for pipeline in shared.pipelines.iter().filter(|p| p.templated) {
            if self.decisions.contains_key(&pipeline.item_id) {
                continue;
            }

            let variant_covers_start = variant.is_some_and(|v| {
                !v.segments.is_empty()
                    && v.pipelines.first().is_some_and(|vp| {
                        vp.item_id == pipeline.item_id && vp.pts_offset_ms == pipeline.pts_offset_ms
                    })
            });

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

            if variant_covers_start {
                self.decisions
                    .insert(pipeline.item_id.clone(), ItemDecision::Variant);
            } else {
                // hold the decision open while there is still time for the
                // variant to produce output; force shared at the window edge
                let deadline = item_start
                    - time::Duration::seconds(
                        (SEGMENT_SECONDS * 5) as i64 - DECISION_LEAD_SECONDS as i64,
                    );
                if now >= deadline {
                    self.decisions
                        .insert(pipeline.item_id.clone(), ItemDecision::Shared);
                }
            }
        }
    }

    /// Appends timeline entries this session has not emitted yet. Entries are
    /// identified by path; history is never reordered or rewritten, so every
    /// client sees an append-only playlist.
    fn reconcile(&mut self, timeline: Vec<ComposedEntry>) {
        for entry in timeline {
            // pinned decisions make the timeline append-only in practice, but
            // never trust that: an entry conflicting with an already-emitted
            // decision for its item is dropped rather than interleaved
            if !self.entries.iter().any(|e| e.path == entry.path) {
                let after_last = self
                    .entries
                    .back()
                    .is_none_or(|last| entry.program_date_time >= last.program_date_time);
                if after_last {
                    self.entries.push_back(entry);
                }
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
            if let Some(removed) = self.entries.pop_front() {
                self.head_media_sequence += 1;
                if removed.discontinuity {
                    self.head_discontinuity_sequence += 1;
                }
            }
        }
    }

    fn render(
        &mut self,
        now: OffsetDateTime,
        target_duration: u32,
        map_path: fn(&str) -> String,
    ) -> String {
        // serve the tail anchored a few segments behind now, mirroring the
        // shared session's own windowing
        let anchor = now - time::Duration::seconds((SEGMENT_SECONDS * 5) as i64);
        let candidate_skip = self
            .entries
            .iter()
            .position(|e| e.program_date_time >= anchor)
            .unwrap_or_else(|| self.entries.len().saturating_sub(SERVED_SEGMENTS));

        // monotonic clamp: the media sequence a client observed must never
        // move backwards between reloads
        let candidate_ms = self.head_media_sequence + candidate_skip as u64;
        let clamped_ms = candidate_ms.max(self.last_served_media_sequence);
        self.last_served_media_sequence = clamped_ms;

        let skip = ((clamped_ms - self.head_media_sequence) as usize).min(self.entries.len());

        let effective_ds = self.head_discontinuity_sequence
            + self
                .entries
                .iter()
                .take(skip)
                .filter(|e| e.discontinuity)
                .count() as u64;

        let mut playlist = String::new();
        playlist.push_str("#EXTM3U\n");
        playlist.push_str("#EXT-X-VERSION:7\n");
        playlist.push_str(&format!("#EXT-X-TARGETDURATION:{target_duration}\n"));
        playlist.push_str(&format!("#EXT-X-MEDIA-SEQUENCE:{clamped_ms}\n"));
        if effective_ds > 0 {
            playlist.push_str(&format!("#EXT-X-DISCONTINUITY-SEQUENCE:{effective_ds}\n"));
        }
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

/// Builds the cohort's timeline from the two sidecars: shared segments pass
/// through, and each item decided `Variant` has its shared segments replaced
/// by the variant's, with program date times synthesized from the item start
/// and the variant's own recorded durations.
///
/// Segments of items *after* an undecided templated item, or after a variant
/// substitution that has not yet caught up to the shared feed's coverage of
/// the item, are held back so the timeline never emits around a hole.
pub fn compose_timeline(
    shared: &PlaylistSidecar,
    variant: Option<&PlaylistSidecar>,
    variant_prefix: &str,
    decisions: &HashMap<String, ItemDecision>,
) -> Vec<ComposedEntry> {
    let templated: HashMap<&str, bool> = shared
        .pipelines
        .iter()
        .map(|p| (p.item_id.as_str(), p.templated))
        .collect();

    let mut result: Vec<ComposedEntry> = Vec::new();
    let mut substituted: Option<&str> = None;
    let mut shared_item_coverage = 0f64;
    let mut variant_coverage = 0f64;

    for segment in &shared.segments {
        let Some(pdt) = parse_pdt(&segment.program_date_time) else {
            continue;
        };

        let is_templated = templated.get(segment.item_id.as_str()).copied() == Some(true);

        if is_templated {
            match decisions.get(&segment.item_id) {
                Some(ItemDecision::Variant) => {
                    if substituted != Some(segment.item_id.as_str()) {
                        // first shared segment of the item: splice in every
                        // variant segment produced so far, anchored here
                        substituted = Some(segment.item_id.as_str());
                        shared_item_coverage = 0f64;
                        variant_coverage = 0f64;

                        let mut cursor = pdt;
                        for (index, vseg) in variant.iter().flat_map(|v| &v.segments).enumerate() {
                            result.push(ComposedEntry {
                                path: format!("{variant_prefix}{}", vseg.path),
                                duration: vseg.duration,
                                program_date_time: cursor,
                                discontinuity: index == 0 || vseg.discontinuity,
                                item_id: segment.item_id.clone(),
                            });
                            cursor += std::time::Duration::from_secs_f64(vseg.duration.max(0f64));
                            variant_coverage += vseg.duration;
                        }
                    }

                    shared_item_coverage += segment.duration;
                    continue;
                }
                Some(ItemDecision::Shared) => {
                    // fall through: the shared segment passes into the
                    // timeline like any other
                }
                None => {
                    // undecided: hold everything from here on back
                    return result;
                }
            }
        } else if let Some(item) = substituted.take() {
            // first segment after a substituted item: only proceed once the
            // variant covered what the shared feed covered, else the timeline
            // would emit around a hole the variant still has to fill
            if variant_coverage + 0.5 < shared_item_coverage {
                log::debug!(
                    "holding timeline after item {item}: variant covered {variant_coverage:.1}s of {shared_item_coverage:.1}s"
                );
                return result;
            }
        }

        result.push(ComposedEntry {
            path: segment.path.clone(),
            duration: segment.duration,
            program_date_time: pdt,
            discontinuity: segment.discontinuity,
            item_id: segment.item_id.clone(),
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
            templated,
        }
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

    fn decided(item: &str, decision: ItemDecision) -> HashMap<String, ItemDecision> {
        let mut map = HashMap::new();
        map.insert(item.to_owned(), decision);
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
            &decided("game", ItemDecision::Variant),
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
    fn variant_segments_take_pdt_from_the_item_start() {
        let shared = shared_with_templated_item();
        let variant = variant_for_game();

        let timeline = compose_timeline(
            &shared,
            Some(&variant),
            "variants/abc/",
            &decided("game", ItemDecision::Variant),
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
            &decided("game", ItemDecision::Variant),
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

        let timeline = compose_timeline(&shared, None, "variants/abc/", &HashMap::new());

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
            &decided("game", ItemDecision::Variant),
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

        assert_eq!(session.decisions.get("game"), Some(&ItemDecision::Variant));
    }

    #[test]
    fn decision_defers_then_forces_shared_at_the_window_edge() {
        let mut session = SessionPlaylist::default();
        let shared = shared_with_templated_item();

        // long before the item reaches viewers: undecided
        let early = OffsetDateTime::UNIX_EPOCH - time::Duration::seconds(60);
        session.decide_items(&shared, None, early);
        assert!(session.decisions.is_empty());

        // item start pdt is +8s; the serve window trails now by ~20s, so by
        // now = item start the decision must be forced
        let late = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(8);
        session.decide_items(&shared, None, late);
        assert_eq!(session.decisions.get("game"), Some(&ItemDecision::Shared));
    }

    #[test]
    fn variant_with_mismatched_offset_is_not_chosen() {
        let mut session = SessionPlaylist::default();
        let shared = shared_with_templated_item();
        let mut variant = variant_for_game();
        variant.pipelines[0].pts_offset_ms = 9_999;

        let late = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(8);
        session.decide_items(&shared, Some(&variant), late);

        assert_eq!(session.decisions.get("game"), Some(&ItemDecision::Shared));
    }

    #[test]
    fn session_playlist_is_append_only_across_advances() {
        let mut session = SessionPlaylist::default();
        let shared = shared_with_templated_item();
        let variant = variant_for_game();
        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(20);

        let first =
            session.advance_and_render(&shared, Some(&variant), "variants/abc/", now, 4, |s| {
                s.to_owned()
            });
        assert!(first.contains("variants/abc/live000000.ts"));

        // a later advance with the same inputs must not duplicate entries
        let second =
            session.advance_and_render(&shared, Some(&variant), "variants/abc/", now, 4, |s| {
                s.to_owned()
            });
        assert_eq!(first, second);
        assert_eq!(session.entries.len(), 5);
    }

    #[test]
    fn media_sequence_advances_as_history_trims() {
        let mut session = SessionPlaylist::default();
        let shared = shared_with_templated_item();
        let variant = variant_for_game();

        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(20);
        session.advance_and_render(&shared, Some(&variant), "variants/abc/", now, 4, |s| {
            s.to_owned()
        });

        // far in the future every entry has aged out of history
        let later = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(500);
        let rendered =
            session.advance_and_render(&shared, Some(&variant), "variants/abc/", later, 4, |s| {
                s.to_owned()
            });

        assert!(session.entries.is_empty());
        assert!(rendered.contains("#EXT-X-MEDIA-SEQUENCE:5\n"));
        // three discontinuities rolled off the head: splice-in, splice-out,
        // and none from the leading shared segments
        assert!(rendered.contains("#EXT-X-DISCONTINUITY-SEQUENCE:2\n"));
    }

    #[test]
    fn subtitle_rendering_maps_paths() {
        let mut session = SessionPlaylist::default();
        let shared = shared_with_templated_item();
        let variant = variant_for_game();
        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(20);

        let rendered =
            session.advance_and_render(&shared, Some(&variant), "variants/abc/", now, 4, |s| {
                format!("{}.vtt", s.strip_suffix(".ts").unwrap_or(s))
            });

        assert!(rendered.contains("variants/abc/live000000.vtt"));
        assert!(!rendered.contains(".ts\n"));
    }
}
