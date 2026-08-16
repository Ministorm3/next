//! Every check, fed a trace that carries the defect it exists to catch.
//!
//! A green result from an instrument that has never seen the defect is what
//! shipped the padding regression. So each check here is shown failing on a
//! trace with the fault present, and passing on one without it, and the two
//! traces differ only in the fault.

use ersatztv_clock_probe::checks::{self, Limits, Severity};
use ersatztv_clock_probe::timeline::{self, Timeline};
use ersatztv_core::clock_trace::{ClockEvent, ClockRecord, ClockSnapshot, TracedInput};
use time::{Duration, OffsetDateTime};

const ORIGIN: OffsetDateTime = OffsetDateTime::UNIX_EPOCH;
const SLOT_MS: i64 = 30_000;

struct Builder {
    records: Vec<ClockRecord>,
    seq: u64,
    w: OffsetDateTime,
    mono_ms: u64,
}

impl Builder {
    fn new() -> Builder {
        Builder {
            records: Vec::new(),
            seq: 0,
            w: ORIGIN,
            mono_ms: 0,
        }
    }

    /// Advances both clocks together, the way a healthy run does.
    fn tick(&mut self, ms: u64) -> &mut Builder {
        self.w += Duration::milliseconds(ms as i64);
        self.mono_ms += ms;
        self
    }

    /// Advances the wall clock alone, which is what a system clock step looks
    /// like from inside the process.
    fn step_wall(&mut self, ms: u64) -> &mut Builder {
        self.w += Duration::milliseconds(ms as i64);
        self
    }

    fn push(&mut self, event: ClockEvent) -> &mut Builder {
        self.records.push(ClockRecord {
            seq: self.seq,
            channel: String::from("13"),
            w_utc: self.w,
            w_mono_ms: self.mono_ms,
            event,
        });
        self.seq += 1;
        self
    }

    fn session(&mut self, offset_ms: i64) -> &mut Builder {
        self.push(ClockEvent::SessionStart {
            w_local: ORIGIN,
            start_time_offset_ms: offset_ms,
            s_transcoded_until: ORIGIN + Duration::milliseconds(offset_ms),
            e_last_segment_end: ORIGIN,
            segment_seconds: 4,
        })
    }

    /// One complete item: selection, pipeline, a segment, and the end.
    ///
    /// `schedule` is where the cursor stood and `emitted` is where the emitted
    /// clock stood, both as offsets from their own origins, so a caller sets
    /// the gap between them directly.
    fn item(&mut self, n: i64, schedule_ms: i64, emitted_ms: i64) -> &mut Builder {
        self.item_named(
            &format!("item-{n}"),
            n,
            schedule_ms,
            emitted_ms,
            schedule_ms,
        )
    }

    fn item_named(
        &mut self,
        id: &str,
        n: i64,
        schedule_ms: i64,
        emitted_ms: i64,
        in_point_ms: i64,
    ) -> &mut Builder {
        let session_offset = self
            .records
            .iter()
            .find_map(|r| match r.event {
                ClockEvent::SessionStart {
                    start_time_offset_ms,
                    ..
                } => Some(start_time_offset_ms),
                _ => None,
            })
            .unwrap_or(0);

        let s_cursor = ORIGIN + Duration::milliseconds(session_offset + schedule_ms);
        let s_start = s_cursor;
        let s_finish = s_cursor + Duration::milliseconds(SLOT_MS);
        let e_stamp = ORIGIN + Duration::milliseconds(emitted_ms);

        self.push(ClockEvent::ItemSelected {
            item_id: String::from(id),
            s_transcoded_until: s_cursor,
            s_item_start: s_start,
            s_item_finish: s_finish,
            p_scanned_pts_ms: Some(emitted_ms as u64),
            state: String::from("ZeroAndRealtime"),
            realtime: true,
            filler: false,
        });

        self.push(ClockEvent::PipelineStart {
            pipeline_seq: n as u64,
            item_id: String::from(id),
            start_at_zero: true,
            is_live: false,
            s_playout_offset_ms: 0,
            s_item_start: s_start,
            s_timing_finish: s_finish,
            p_pts_offset_ms: Some(emitted_ms as u64),
            inputs: vec![TracedInput {
                role: String::from("video"),
                kind: String::from("local"),
                name: Some(format!("{id}.mkv")),
                c_in_point_ms: in_point_ms as u64,
                c_out_point_ms: (in_point_ms + SLOT_MS) as u64,
                c_frame_rate: Some(String::from("24000/1001")),
                c_probed_duration_ms: Some(SLOT_MS as u64),
            }],
            is_complete_expected: true,
            snapshot: ClockSnapshot {
                e_last_segment_end: e_stamp,
                e_session_start: e_stamp,
                q_media_sequence: n as u64,
                q_segments_held: 10,
            },
        });

        self.tick(100);
        self.push(ClockEvent::SegmentAdded {
            q_path: format!("live{:06}.ts", n),
            q_media_sequence: n as u64,
            q_segments_held: 10,
            e_program_date_time: e_stamp,
            e_duration_s: SLOT_MS as f64 / 1000.0,
            e_last_segment_end: e_stamp + Duration::milliseconds(SLOT_MS),
            e_session_start: e_stamp,
            p_pts_offset_ms: emitted_ms as u64,
            p_mpegts_90khz: 0,
            discontinuity: true,
        });

        self.push(ClockEvent::PipelineEnd {
            pipeline_seq: n as u64,
            item_id: String::from(id),
            s_finish: Some(s_finish),
            is_complete: true,
            outcome: String::from("ok"),
        });

        self.tick(SLOT_MS as u64 - 100);
        self
    }

    fn build(&self) -> Timeline {
        timeline::fold(String::from("13"), self.records.clone())
    }
}

fn failures(timeline: &Timeline, check: &str) -> Vec<String> {
    checks::run(timeline, Limits::default())
        .into_iter()
        .filter(|f| f.check == check && f.severity == Severity::Fail)
        .map(|f| f.message)
        .collect()
}

/// A run where nothing is wrong, so that every failure below is attributable
/// to the one fault the test introduces.
fn healthy() -> Builder {
    let mut builder = Builder::new();
    builder.session(0);
    for n in 1..=8 {
        let at = (n - 1) * SLOT_MS;
        builder.item(n, at, at);
    }
    builder
}

#[test]
fn a_healthy_run_reports_no_failure() {
    let timeline = healthy().build();
    let findings = checks::run(&timeline, Limits::default());
    let failed: Vec<&str> = findings
        .iter()
        .filter(|f| f.severity == Severity::Fail)
        .map(|f| f.check)
        .collect();
    assert!(failed.is_empty(), "healthy run failed {failed:?}");
}

/// The defect in PR #212: the raw difference between the emitted clock and the
/// schedule cursor returns the whole virtual start offset as if it were drift.
#[test]
fn a_virtual_start_is_not_drift() {
    const OFFSET_MS: i64 = 3_600_000;

    let mut builder = Builder::new();
    builder.session(OFFSET_MS);
    for n in 1..=8 {
        let at = (n - 1) * SLOT_MS;
        builder.item(n, at, at);
    }
    let timeline = builder.build();

    assert_eq!(timeline.offset(), Duration::milliseconds(OFFSET_MS));

    let pipeline = timeline.pipelines.last().expect("a pipeline");
    let schedule = pipeline.s_transcoded_until.expect("a cursor");

    // corrected, there is no drift at all
    assert_eq!(
        timeline
            .stamp_error(pipeline.e_at_start, schedule)
            .whole_milliseconds(),
        0
    );

    // uncorrected, the whole hour reads as drift, and anything acting on that
    // number would trim or pad real content to chase it
    assert_eq!(
        timeline
            .stamp_error_uncorrected(pipeline.e_at_start, schedule)
            .whole_milliseconds(),
        -(OFFSET_MS as i128)
    );

    assert!(
        failures(&timeline, "stamp-drift").is_empty(),
        "a virtual start was reported as drift"
    );
}

/// The 2026-08-14 padding regression: every padded item runs one part frame
/// long, which is invisible per item and a staircase over many.
#[test]
fn a_per_item_overshoot_shows_as_a_climb() {
    const OVERSHOOT_MS: i64 = 23;

    let mut builder = Builder::new();
    builder.session(0);
    for n in 1..=20 {
        let scheduled = (n - 1) * SLOT_MS;
        // the emitted clock gains a part frame on every item
        let emitted = scheduled + (n - 1) * OVERSHOOT_MS;
        builder.item(n, scheduled, emitted);
    }
    let timeline = builder.build();

    let failed = failures(&timeline, "stamp-drift");
    assert_eq!(failed.len(), 1, "the climb was not caught: {failed:?}");
    assert!(
        failed[0].contains("+23ms per item"),
        "the per item step was not reported: {}",
        failed[0]
    );

    // a single item is well inside the noise, which is why the bench of the
    // day passed while the run drifted
    let single = OVERSHOOT_MS;
    assert!(single < 100, "one item alone would look like jitter");
}

/// The trim compares an emitted stamp against a wall clock cutoff, so a
/// channel running behind deletes segments a client may still ask for.
#[test]
fn a_trim_inside_the_published_window_is_caught() {
    let mut builder = healthy();

    let published_tail = ORIGIN + Duration::seconds(100);
    builder.push(ClockEvent::Publish {
        w_horizon: builder.w + Duration::seconds(12),
        q_media_sequence: 5,
        q_candidate: 5,
        q_clamped: 5,
        q_last_served: 5,
        q_skip: 0,
        q_limit: 10,
        q_head: 10,
        q_segments_held: 10,
        e_head_pdt: Some(ORIGIN + Duration::seconds(140)),
        e_tail_pdt: Some(published_tail),
        e_oldest_pdt: Some(published_tail),
        e_last_segment_end: ORIGIN + Duration::seconds(144),
    });

    builder.tick(1_000);

    // the cutoff is two minutes behind real time, but the channel has fallen
    // further behind than that, so the cutoff has walked into live content
    builder.push(ClockEvent::SegmentTrimmed {
        q_path: String::from("live000025.ts"),
        q_media_sequence: 25,
        q_segments_held: 9,
        e_program_date_time: published_tail + Duration::seconds(4),
        w_cutoff: builder.w - Duration::minutes(2),
    });

    let timeline = builder.build();
    let failed = failures(&timeline, "trim-safety");
    assert_eq!(failed.len(), 1, "the served deletion was not caught");
    assert!(failed[0].contains("live000025.ts"), "{}", failed[0]);
    assert!(failed[0].contains("404"), "{}", failed[0]);
}

#[test]
fn a_trim_behind_the_window_is_not_a_failure() {
    let mut builder = healthy();

    let published_tail = ORIGIN + Duration::seconds(100);
    builder.push(ClockEvent::Publish {
        w_horizon: builder.w + Duration::seconds(12),
        q_media_sequence: 5,
        q_candidate: 5,
        q_clamped: 5,
        q_last_served: 5,
        q_skip: 0,
        q_limit: 10,
        q_head: 10,
        q_segments_held: 10,
        e_head_pdt: Some(ORIGIN + Duration::seconds(140)),
        e_tail_pdt: Some(published_tail),
        e_oldest_pdt: Some(published_tail),
        e_last_segment_end: ORIGIN + Duration::seconds(144),
    });

    builder.tick(1_000);
    builder.push(ClockEvent::SegmentTrimmed {
        q_path: String::from("live000001.ts"),
        q_media_sequence: 1,
        q_segments_held: 9,
        // safely older than anything a client was handed
        e_program_date_time: published_tail - Duration::seconds(8),
        w_cutoff: builder.w - Duration::minutes(2),
    });

    let timeline = builder.build();
    assert!(
        failures(&timeline, "trim-safety").is_empty(),
        "a trim behind the window was called a failure"
    );
}

/// The maintainer ruling that closed PR #187: a measured output timestamp must
/// never drive an input seek. Take the schedule progress back out of the seek
/// and what remains cannot move while an item plays.
#[test]
fn a_measured_value_reaching_the_seek_is_caught() {
    let mut builder = Builder::new();
    builder.session(0);

    for (n, (playout_offset_ms, in_point_ms)) in
        [(0u64, 5_000u64), (30_000, 60_000)].into_iter().enumerate()
    {
        let n = n as u64 + 1;
        builder.push(ClockEvent::ItemSelected {
            item_id: String::from("long-item"),
            s_transcoded_until: ORIGIN + Duration::milliseconds(playout_offset_ms as i64),
            s_item_start: ORIGIN,
            s_item_finish: ORIGIN + Duration::hours(1),
            p_scanned_pts_ms: Some(30_000),
            state: String::from("SeekAndRealtime"),
            realtime: true,
            filler: false,
        });
        builder.push(ClockEvent::PipelineStart {
            pipeline_seq: n,
            item_id: String::from("long-item"),
            // a resumed item, so the seek is item in point plus progress
            start_at_zero: false,
            is_live: false,
            s_playout_offset_ms: playout_offset_ms,
            s_item_start: ORIGIN,
            s_timing_finish: ORIGIN + Duration::hours(1),
            p_pts_offset_ms: Some(30_000),
            inputs: vec![TracedInput {
                role: String::from("video"),
                kind: String::from("local"),
                name: Some(String::from("long.mkv")),
                c_in_point_ms: in_point_ms,
                c_out_point_ms: in_point_ms + 30_000,
                c_frame_rate: Some(String::from("24000/1001")),
                c_probed_duration_ms: Some(3_600_000),
            }],
            is_complete_expected: false,
            snapshot: ClockSnapshot {
                e_last_segment_end: ORIGIN + Duration::milliseconds(playout_offset_ms as i64),
                e_session_start: ORIGIN,
                q_media_sequence: n,
                q_segments_held: 10,
            },
        });
        builder.tick(30_000);
    }

    // the first pipeline seeks from a base of 5000ms, the second from 30000ms,
    // which is the scanned value having leaked into the seek
    let timeline = builder.build();
    let failed = failures(&timeline, "seek-purity");
    assert_eq!(failed.len(), 1, "the leak was not caught: {failed:?}");
    assert!(failed[0].contains("30000ms"), "{}", failed[0]);
    assert!(failed[0].contains("5000ms"), "{}", failed[0]);
}

#[test]
fn a_seek_built_only_from_the_schedule_passes() {
    let mut builder = Builder::new();
    builder.session(0);

    for (n, (playout_offset_ms, in_point_ms)) in
        [(0u64, 5_000u64), (30_000, 35_000)].into_iter().enumerate()
    {
        let n = n as u64 + 1;
        builder.push(ClockEvent::ItemSelected {
            item_id: String::from("long-item"),
            s_transcoded_until: ORIGIN + Duration::milliseconds(playout_offset_ms as i64),
            s_item_start: ORIGIN,
            s_item_finish: ORIGIN + Duration::hours(1),
            p_scanned_pts_ms: Some(30_000),
            state: String::from("SeekAndRealtime"),
            realtime: true,
            filler: false,
        });
        builder.push(ClockEvent::PipelineStart {
            pipeline_seq: n,
            item_id: String::from("long-item"),
            start_at_zero: false,
            is_live: false,
            s_playout_offset_ms: playout_offset_ms,
            s_item_start: ORIGIN,
            s_timing_finish: ORIGIN + Duration::hours(1),
            p_pts_offset_ms: Some(30_000),
            inputs: vec![TracedInput {
                role: String::from("video"),
                kind: String::from("local"),
                name: Some(String::from("long.mkv")),
                c_in_point_ms: in_point_ms,
                c_out_point_ms: in_point_ms + 30_000,
                c_frame_rate: Some(String::from("24000/1001")),
                c_probed_duration_ms: Some(3_600_000),
            }],
            is_complete_expected: false,
            snapshot: ClockSnapshot {
                e_last_segment_end: ORIGIN + Duration::milliseconds(playout_offset_ms as i64),
                e_session_start: ORIGIN,
                q_media_sequence: n,
                q_segments_held: 10,
            },
        });
        builder.tick(30_000);
    }

    let timeline = builder.build();
    assert!(
        failures(&timeline, "seek-purity").is_empty(),
        "a sound seek was called a leak"
    );
}

/// Nothing else in the system can see this. Two wall clock readings cannot
/// tell a slow hour from a stepped clock without a monotonic reading beside
/// them, and every schedule reading across a step is suspect.
#[test]
fn a_system_clock_step_is_caught() {
    let mut builder = healthy();
    builder.step_wall(45_000);
    builder.item(9, 8 * SLOT_MS, 8 * SLOT_MS);

    let timeline = builder.build();
    assert_eq!(timeline.clock_steps.len(), 1);
    assert_eq!(timeline.clock_steps[0].skew_ms(), 45_000);

    let failed = failures(&timeline, "wall-clock");
    assert_eq!(failed.len(), 1, "the step was not reported");
    assert!(failed[0].contains("+45000ms"), "{}", failed[0]);
}

#[test]
fn a_healthy_run_reports_no_clock_step() {
    assert!(healthy().build().clock_steps.is_empty());
}

/// The publish window may never carry a stamp from past the horizon.
#[test]
fn a_window_published_past_the_horizon_is_caught() {
    let mut builder = healthy();
    builder.push(ClockEvent::Publish {
        w_horizon: builder.w + Duration::seconds(12),
        q_media_sequence: 5,
        q_candidate: 5,
        q_clamped: 5,
        q_last_served: 5,
        q_skip: 0,
        q_limit: 10,
        q_head: 10,
        q_segments_held: 10,
        e_head_pdt: Some(builder.w + Duration::seconds(60)),
        e_tail_pdt: Some(builder.w),
        e_oldest_pdt: Some(builder.w),
        e_last_segment_end: builder.w + Duration::seconds(64),
    });

    let timeline = builder.build();
    assert_eq!(failures(&timeline, "publish-horizon").len(), 1);
}

/// Segments are attributed by the discontinuity marker, because the tail of an
/// item routinely lands after its own pipeline has ended.
#[test]
fn a_late_segment_belongs_to_the_item_that_produced_it() {
    let mut builder = Builder::new();
    builder.session(0);

    builder.push(ClockEvent::PipelineStart {
        pipeline_seq: 1,
        item_id: String::from("first"),
        start_at_zero: true,
        is_live: false,
        s_playout_offset_ms: 0,
        s_item_start: ORIGIN,
        s_timing_finish: ORIGIN + Duration::seconds(30),
        p_pts_offset_ms: Some(0),
        inputs: Vec::new(),
        is_complete_expected: true,
        snapshot: ClockSnapshot {
            e_last_segment_end: ORIGIN,
            e_session_start: ORIGIN,
            q_media_sequence: 0,
            q_segments_held: 0,
        },
    });
    builder.push(ClockEvent::SegmentAdded {
        q_path: String::from("live000001.ts"),
        q_media_sequence: 0,
        q_segments_held: 1,
        e_program_date_time: ORIGIN,
        e_duration_s: 4.0,
        e_last_segment_end: ORIGIN + Duration::seconds(4),
        e_session_start: ORIGIN,
        p_pts_offset_ms: 0,
        p_mpegts_90khz: 0,
        discontinuity: true,
    });
    builder.push(ClockEvent::PipelineEnd {
        pipeline_seq: 1,
        item_id: String::from("first"),
        s_finish: Some(ORIGIN + Duration::seconds(30)),
        is_complete: true,
        outcome: String::from("ok"),
    });

    // the next pipeline is announced, and only then does the publish loop pick
    // up the last segment the previous one wrote
    builder.push(ClockEvent::PipelineStart {
        pipeline_seq: 2,
        item_id: String::from("second"),
        start_at_zero: true,
        is_live: false,
        s_playout_offset_ms: 0,
        s_item_start: ORIGIN + Duration::seconds(30),
        s_timing_finish: ORIGIN + Duration::seconds(60),
        p_pts_offset_ms: Some(4_000),
        inputs: Vec::new(),
        is_complete_expected: true,
        snapshot: ClockSnapshot {
            e_last_segment_end: ORIGIN + Duration::seconds(4),
            e_session_start: ORIGIN + Duration::seconds(4),
            q_media_sequence: 0,
            q_segments_held: 1,
        },
    });
    builder.push(ClockEvent::SegmentAdded {
        q_path: String::from("live000002.ts"),
        q_media_sequence: 0,
        q_segments_held: 2,
        e_program_date_time: ORIGIN + Duration::seconds(4),
        e_duration_s: 4.0,
        e_last_segment_end: ORIGIN + Duration::seconds(8),
        e_session_start: ORIGIN,
        p_pts_offset_ms: 0,
        p_mpegts_90khz: 0,
        // no marker, so this is still the outgoing pipeline's work
        discontinuity: false,
    });
    builder.push(ClockEvent::SegmentAdded {
        q_path: String::from("live000003.ts"),
        q_media_sequence: 0,
        q_segments_held: 3,
        e_program_date_time: ORIGIN + Duration::seconds(8),
        e_duration_s: 4.0,
        e_last_segment_end: ORIGIN + Duration::seconds(12),
        e_session_start: ORIGIN + Duration::seconds(8),
        p_pts_offset_ms: 4_000,
        p_mpegts_90khz: 0,
        discontinuity: true,
    });

    let timeline = builder.build();
    let first: Vec<&str> = timeline.segments_of(1).map(|s| s.q_path.as_str()).collect();
    let second: Vec<&str> = timeline.segments_of(2).map(|s| s.q_path.as_str()).collect();

    assert_eq!(
        first,
        vec!["live000001.ts", "live000002.ts"],
        "a segment that landed after its pipeline ended was misattributed"
    );
    assert_eq!(second, vec!["live000003.ts"]);
}

/// A pipeline that emits nothing must not put every later attribution off by
/// one, which is the failure mode of counting boundaries instead of reading
/// the markers.
#[test]
fn a_pipeline_that_emits_nothing_keeps_the_rest_aligned() {
    let mut builder = Builder::new();
    builder.session(0);

    for seq in 1..=3u64 {
        builder.push(ClockEvent::PipelineStart {
            pipeline_seq: seq,
            item_id: format!("item-{seq}"),
            start_at_zero: true,
            is_live: false,
            s_playout_offset_ms: 0,
            s_item_start: ORIGIN,
            s_timing_finish: ORIGIN + Duration::seconds(30),
            p_pts_offset_ms: Some(0),
            inputs: Vec::new(),
            is_complete_expected: true,
            snapshot: ClockSnapshot {
                e_last_segment_end: ORIGIN,
                e_session_start: ORIGIN,
                q_media_sequence: 0,
                q_segments_held: 0,
            },
        });

        // the second pipeline dies before writing anything
        if seq == 2 {
            builder.push(ClockEvent::PipelineEnd {
                pipeline_seq: seq,
                item_id: format!("item-{seq}"),
                s_finish: None,
                is_complete: false,
                outcome: String::from("failed"),
            });
            continue;
        }

        builder.push(ClockEvent::SegmentAdded {
            q_path: format!("live{seq:06}.ts"),
            q_media_sequence: 0,
            q_segments_held: seq as usize,
            e_program_date_time: ORIGIN + Duration::seconds(4 * seq as i64),
            e_duration_s: 4.0,
            e_last_segment_end: ORIGIN + Duration::seconds(4 * seq as i64 + 4),
            e_session_start: ORIGIN,
            p_pts_offset_ms: 0,
            p_mpegts_90khz: 0,
            discontinuity: true,
        });
    }

    let timeline = builder.build();
    assert_eq!(timeline.segments_of(1).count(), 1);
    assert_eq!(
        timeline.segments_of(2).count(),
        0,
        "a dead pipeline claimed a segment"
    );
    assert_eq!(
        timeline.segments_of(3).next().map(|s| s.q_path.as_str()),
        Some("live000003.ts"),
        "the attribution slipped after a pipeline emitted nothing"
    );
}

/// The whole path, from a file on disk to a finding.
#[test]
fn a_trace_file_loads_and_checks() {
    let folder = tempfile::tempdir().expect("temp dir");
    let path = folder.path().join("clock-13.jsonl");

    let builder = healthy();
    let body: String = builder
        .records
        .iter()
        .map(|r| serde_json::to_string(r).expect("record") + "\n")
        .collect();
    std::fs::write(&path, body).expect("write");

    // a worker killed mid write leaves a partial line, which must not be fatal
    std::fs::write(
        folder.path().join("clock-99.jsonl"),
        "{\"seq\":0,\"channel\":\"99\",\"w_ut",
    )
    .expect("write");

    let timelines = timeline::load(&[folder.path().to_path_buf()]).expect("load");
    let thirteen = timelines
        .iter()
        .find(|t| t.channel == "13")
        .expect("channel 13");

    assert_eq!(thirteen.pipelines.len(), 8);
    assert_eq!(thirteen.segments.len(), 8);
    assert!(thirteen.origin.is_some());

    let findings = checks::run(thirteen, Limits::default());
    assert!(findings.iter().all(|f| f.severity != Severity::Fail));
}

#[test]
fn a_gap_in_the_record_numbers_is_reported() {
    let mut builder = healthy();
    builder.seq += 5;
    builder.item(9, 8 * SLOT_MS, 8 * SLOT_MS);

    let timeline = builder.build();
    assert_eq!(timeline.lost_records, 5);
}
