//! Which trim a channel runs, read from the trace rather than assumed.
//!
//! The cutoff is a wall clock reading on some builds and a served position on
//! others. Those are different domains, and only the second is sound, so the
//! record names them separately and the probe reports which one it saw. A
//! single field would have made a sound build look like an unsound one.

use ersatztv_clock_probe::checks::{self, Limits, Severity};
use ersatztv_clock_probe::timeline::{self, Timeline};
use ersatztv_core::clock_trace::{ClockEvent, ClockRecord};
use time::{Duration, OffsetDateTime};

const ORIGIN: OffsetDateTime = OffsetDateTime::UNIX_EPOCH;

fn trace_with(trim: ClockEvent) -> Timeline {
    let records = vec![
        ClockRecord {
            seq: 0,
            channel: String::from("13"),
            w_utc: ORIGIN,
            w_mono_ms: 0,
            event: ClockEvent::SessionStart {
                w_local: ORIGIN,
                start_time_offset_ms: 0,
                s_transcoded_until: ORIGIN,
                e_last_segment_end: ORIGIN,
                segment_seconds: 4,
            },
        },
        ClockRecord {
            seq: 1,
            channel: String::from("13"),
            w_utc: ORIGIN + Duration::minutes(5),
            w_mono_ms: 300_000,
            event: trim,
        },
    ];
    timeline::fold(String::from("13"), records)
}

fn findings_for(timeline: &Timeline) -> Vec<(Severity, String)> {
    checks::run(timeline, Limits::default())
        .into_iter()
        .filter(|f| f.check == "trim-domain")
        .map(|f| (f.severity, f.message))
        .collect()
}

#[test]
fn a_wall_clock_cutoff_is_flagged() {
    let timeline = trace_with(ClockEvent::SegmentTrimmed {
        q_path: String::from("live000001.ts"),
        q_media_sequence: 1,
        q_segments_held: 30,
        e_program_date_time: ORIGIN,
        w_cutoff: Some(ORIGIN + Duration::minutes(3)),
        e_trim_cutoff: None,
    });

    let found = findings_for(&timeline);
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0].0, Severity::Warn);
    assert!(found[0].1.contains("wall clock"), "{}", found[0].1);
}

#[test]
fn a_served_position_cutoff_is_not_flagged() {
    let timeline = trace_with(ClockEvent::SegmentTrimmed {
        q_path: String::from("live000001.ts"),
        q_media_sequence: 1,
        q_segments_held: 30,
        e_program_date_time: ORIGIN,
        w_cutoff: None,
        e_trim_cutoff: Some(ORIGIN + Duration::minutes(3)),
    });

    let found = findings_for(&timeline);
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(
        found[0].0,
        Severity::Info,
        "a sound trim was reported as a risk: {}",
        found[0].1
    );
    assert!(found[0].1.contains("served position"), "{}", found[0].1);
}
