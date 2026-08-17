//! A run too short to judge must not be reported as a failing one.
//!
//! Both of these fired on a seventy second smoke test of a build that is
//! known good, which is the failure mode that makes a gate useless: a reader
//! who sees FAIL on a healthy channel stops reading the output.

use ersatztv_clock_probe::checks::{self, Limits, Severity};
use ersatztv_clock_probe::timeline::{self, Timeline};
use ersatztv_core::clock_trace::{ClockEvent, ClockRecord, ClockSnapshot};
use time::{Duration, OffsetDateTime};

const ORIGIN: OffsetDateTime = OffsetDateTime::UNIX_EPOCH;

/// `items` pipelines of `slot_s` each, with `total_jitter_ms` of wobble across
/// the whole run and no trims at all.
fn short_run(items: i64, slot_s: i64, total_jitter_ms: i64) -> Timeline {
    let mut records = Vec::new();
    let mut seq = 0u64;

    let mut push = |event: ClockEvent, at: OffsetDateTime, seq: &mut u64| {
        records.push(ClockRecord {
            seq: *seq,
            channel: String::from("13"),
            w_utc: at,
            w_mono_ms: (at - ORIGIN).whole_milliseconds() as u64,
            event,
        });
        *seq += 1;
    };

    push(
        ClockEvent::SessionStart {
            w_local: ORIGIN,
            start_time_offset_ms: 0,
            s_transcoded_until: ORIGIN,
            e_last_segment_end: ORIGIN,
            segment_seconds: 4,
        },
        ORIGIN,
        &mut seq,
    );

    for n in 1..=items {
        let at = ORIGIN + Duration::seconds(slot_s * (n - 1));
        // all of the wobble lands on the final item, so the total stays small
        // while a rate extrapolated from it does not
        let jitter = if n == items { total_jitter_ms } else { 0 };
        let emitted = at + Duration::milliseconds(jitter);

        push(
            ClockEvent::ItemSelected {
                item_id: format!("item-{n}"),
                s_transcoded_until: at,
                s_item_start: at,
                s_item_finish: at + Duration::seconds(slot_s),
                p_scanned_pts_ms: Some(0),
                state: String::from("ZeroAndRealtime"),
                realtime: true,
                filler: false,
            },
            at,
            &mut seq,
        );
        push(
            ClockEvent::PipelineStart {
                pipeline_seq: n as u64,
                item_id: format!("item-{n}"),
                start_at_zero: true,
                is_live: false,
                s_playout_offset_ms: 0,
                s_item_start: at,
                s_timing_finish: at + Duration::seconds(slot_s),
                p_pts_offset_ms: Some(0),
                inputs: Vec::new(),
                is_complete_expected: true,
                snapshot: ClockSnapshot {
                    e_last_segment_end: emitted,
                    e_session_start: emitted,
                    q_media_sequence: 0,
                    q_segments_held: n as usize,
                },
            },
            at,
            &mut seq,
        );
        push(
            ClockEvent::Publish {
                w_horizon: at + Duration::seconds(12),
                q_media_sequence: 0,
                q_candidate: 0,
                q_clamped: 0,
                q_last_served: 0,
                q_skip: 0,
                q_limit: n as usize,
                q_head: n as usize,
                q_segments_held: n as usize,
                e_head_pdt: Some(emitted),
                e_tail_pdt: Some(ORIGIN),
                // nothing trimmed, so held history is everything made so far
                // and is still climbing toward the budget
                e_oldest_pdt: Some(ORIGIN),
                e_last_segment_end: emitted,
            },
            at,
            &mut seq,
        );
    }

    timeline::fold(String::from("13"), records)
}

fn failed(timeline: &Timeline, check: &str) -> Vec<String> {
    checks::run(timeline, Limits::default())
        .into_iter()
        .filter(|f| f.check == check && f.severity == Severity::Fail)
        .map(|f| f.message)
        .collect()
}

fn reading(timeline: &Timeline, check: &str) -> String {
    checks::run(timeline, Limits::default())
        .into_iter()
        .find(|f| f.check == check)
        .expect("a reading")
        .message
}

/// Seven milliseconds over seventy seconds extrapolates past the rate gate,
/// but seven milliseconds is not drift. The smoke test that found this read
/// -268ms per hour off a build that reads -17ms per hour over a longer window.
#[test]
fn a_rate_from_a_short_window_is_not_judged() {
    let timeline = short_run(8, 9, -7);

    let found = failed(&timeline, "stamp-drift");
    assert!(found.is_empty(), "a short run was called drift: {found:?}");

    let message = reading(&timeline, "stamp-drift");
    assert!(
        message.contains("not judged"),
        "the reading did not say the rate was withheld: {message}"
    );
}

/// The same shape over a window long enough to mean something still fails, so
/// the floor withholds judgement rather than removing it.
#[test]
fn a_rate_over_a_long_window_is_still_judged() {
    // thirty items of two minutes, one hour of schedule, drifting a second
    let timeline = short_run(30, 120, -1_000);

    let found = failed(&timeline, "stamp-drift");
    assert_eq!(found.len(), 1, "a real ratchet stopped being caught");
    assert!(found[0].contains("per hour of schedule"), "{}", found[0]);
}

/// Held history before the first trim is everything the channel has made, so
/// it says nothing about what the channel retains.
#[test]
fn retention_is_not_judged_before_the_first_trim() {
    let timeline = short_run(8, 9, 0);
    assert!(timeline.trims.is_empty());

    let found = failed(&timeline, "retention");
    assert!(
        found.is_empty(),
        "a channel that has discarded nothing was called starved: {found:?}"
    );
    assert!(reading(&timeline, "retention").contains("still filling"));
}
