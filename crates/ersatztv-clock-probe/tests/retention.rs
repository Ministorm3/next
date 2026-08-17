//! Retention, which the first version of the check measured from the wrong
//! end and reported backwards.
//!
//! Held history has to be read from what is still on disk. A trimmed segment
//! is older than the cutoff by definition, so its age is always the whole
//! budget or more, and a check built on it calls every healthy channel
//! starved. Both directions are pinned here.

use ersatztv_clock_probe::checks::{self, Limits, Severity};
use ersatztv_clock_probe::timeline::{self, Timeline};
use ersatztv_core::clock_trace::{ClockEvent, ClockRecord, ClockSnapshot};
use time::{Duration, OffsetDateTime};

const ORIGIN: OffsetDateTime = OffsetDateTime::UNIX_EPOCH;

struct Builder {
    records: Vec<ClockRecord>,
    seq: u64,
    w: OffsetDateTime,
    mono_ms: u64,
}

impl Builder {
    fn new() -> Builder {
        let mut builder = Builder {
            records: Vec::new(),
            seq: 0,
            w: ORIGIN,
            mono_ms: 0,
        };
        builder.push(ClockEvent::SessionStart {
            w_local: ORIGIN,
            start_time_offset_ms: 0,
            s_transcoded_until: ORIGIN,
            e_last_segment_end: ORIGIN,
            segment_seconds: 4,
        });
        // three pipelines, so the drift check has something to stand on and
        // this file's failures are all retention
        for seq in 1..=3u64 {
            let at = ORIGIN + Duration::seconds(30 * seq as i64);
            builder.push(ClockEvent::ItemSelected {
                item_id: format!("item-{seq}"),
                s_transcoded_until: at,
                s_item_start: at,
                s_item_finish: at + Duration::seconds(30),
                p_scanned_pts_ms: Some(0),
                state: String::from("ZeroAndRealtime"),
                realtime: true,
                filler: false,
            });
            builder.push(ClockEvent::PipelineStart {
                pipeline_seq: seq,
                item_id: format!("item-{seq}"),
                start_at_zero: true,
                is_live: false,
                s_playout_offset_ms: 0,
                s_item_start: at,
                s_timing_finish: at + Duration::seconds(30),
                p_pts_offset_ms: Some(0),
                inputs: Vec::new(),
                is_complete_expected: true,
                snapshot: ClockSnapshot {
                    e_last_segment_end: at,
                    e_session_start: at,
                    q_media_sequence: 0,
                    q_segments_held: 30,
                },
            });
            builder.tick(30_000);
        }
        builder
    }

    fn tick(&mut self, ms: u64) {
        self.w += Duration::milliseconds(ms as i64);
        self.mono_ms += ms;
    }

    fn push(&mut self, event: ClockEvent) {
        self.records.push(ClockRecord {
            seq: self.seq,
            channel: String::from("13"),
            w_utc: self.w,
            w_mono_ms: self.mono_ms,
            event,
        });
        self.seq += 1;
    }

    /// A publish tick holding `held_ms` of history, with the live edge
    /// `edge_lag_ms` behind real time. A negative lag means the channel is
    /// running ahead, which is the normal working ahead state.
    fn publish(&mut self, held_ms: i64, edge_lag_ms: i64) {
        let edge = self.w - Duration::milliseconds(edge_lag_ms);
        self.push(ClockEvent::Publish {
            w_horizon: self.w + Duration::seconds(12),
            q_media_sequence: 5,
            q_candidate: 5,
            q_clamped: 5,
            q_last_served: 5,
            q_skip: 0,
            q_limit: 10,
            q_head: 10,
            q_segments_held: 30,
            e_head_pdt: Some(edge - Duration::seconds(4)),
            e_tail_pdt: Some(edge - Duration::seconds(40)),
            e_oldest_pdt: Some(edge - Duration::milliseconds(held_ms)),
            e_last_segment_end: edge,
        });
    }

    fn build(&self) -> Timeline {
        timeline::fold(String::from("13"), self.records.clone())
    }
}

fn failures(timeline: &Timeline) -> Vec<String> {
    checks::run(timeline, Limits::default())
        .into_iter()
        .filter(|f| f.check == "retention" && f.severity == Severity::Fail)
        .map(|f| f.message)
        .collect()
}

/// A channel working ahead keeps its whole budget and must not be called
/// starved, even though it trims on every tick.
#[test]
fn a_channel_that_is_ahead_is_not_reported_as_starved() {
    let mut builder = Builder::new();
    for _ in 0..20 {
        builder.publish(120_000, -30_000);
        builder.tick(2_000);
    }

    // trimming is normal, and reading retention from these is what produced
    // the false alarm
    builder.push(ClockEvent::SegmentTrimmed {
        q_path: String::from("live000001.ts"),
        q_media_sequence: 1,
        q_segments_held: 30,
        e_program_date_time: builder.w - Duration::minutes(3),
        w_cutoff: Some(builder.w - Duration::minutes(2)),
        e_trim_cutoff: None,
    });

    let timeline = builder.build();
    let failed = failures(&timeline);
    assert!(
        failed.is_empty(),
        "a healthy channel was called starved: {failed:?}"
    );
}

/// Once the live edge falls behind by most of the budget, the cutoff walks
/// into content clients can still ask for and the history collapses.
#[test]
fn a_channel_that_has_fallen_behind_loses_its_history() {
    let mut builder = Builder::new();
    for _ in 0..20 {
        builder.publish(8_000, 112_000);
        // the collapse is the cutoff walking into live content, so this
        // channel is necessarily trimming while it happens
        builder.push(ClockEvent::SegmentTrimmed {
            q_path: String::from("live000001.ts"),
            q_media_sequence: 1,
            q_segments_held: 2,
            e_program_date_time: builder.w - Duration::minutes(3),
            w_cutoff: Some(builder.w - Duration::minutes(2)),
            e_trim_cutoff: None,
        });
        builder.tick(2_000);
    }

    let timeline = builder.build();
    let failed = failures(&timeline);
    assert_eq!(failed.len(), 1, "the collapse was not caught: {failed:?}");
    assert!(failed[0].contains("8000ms"), "{}", failed[0]);
    assert!(failed[0].contains("112000ms"), "{}", failed[0]);
}
