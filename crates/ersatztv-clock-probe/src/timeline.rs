//! Folds a stream of records back into the run that produced them.
//!
//! A record is a reading. A timeline is what you can say once the readings are
//! joined: which pipeline emitted which segments, where each cursor stood when
//! it did, and how far apart two cursors had drifted by then.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use ersatztv_core::clock_trace::{ClockEvent, ClockRecord, TracedInput};
use time::{Duration, OffsetDateTime};

/// Where every cursor started, and how far apart the two origins were.
#[derive(Debug, Clone)]
pub struct Origin {
    pub w_local: OffsetDateTime,
    /// `virtual_start - now`. The schedule cursor carries it and the emitted
    /// clock does not, so every comparison between the two has to add it back.
    pub start_time_offset: Duration,
    pub s_origin: OffsetDateTime,
    pub e_origin: OffsetDateTime,
    pub segment_seconds: u32,
}

/// One pipeline, with the readings from its selection record folded in.
#[derive(Debug, Clone)]
pub struct Pipeline {
    pub seq: u64,
    pub item_id: String,
    pub w_start: OffsetDateTime,
    pub w_end: Option<OffsetDateTime>,

    pub state: Option<String>,
    pub realtime: Option<bool>,
    pub filler: bool,
    pub start_at_zero: bool,
    pub is_live: bool,

    /// Where the schedule cursor stood when the item was picked.
    pub s_transcoded_until: Option<OffsetDateTime>,
    pub s_item_start: OffsetDateTime,
    pub s_item_finish: Option<OffsetDateTime>,
    pub s_playout_offset_ms: u64,
    pub s_timing_finish: OffsetDateTime,
    /// Where the cursor actually landed. Absent when the pipeline died.
    pub s_finish: Option<OffsetDateTime>,

    pub p_scanned_ms: Option<u64>,
    pub p_pts_offset_ms: Option<u64>,

    pub inputs: Vec<TracedInput>,

    pub e_at_start: OffsetDateTime,
    pub q_media_sequence: u64,
    pub q_segments_held: usize,

    pub is_complete_expected: bool,
    pub is_complete: Option<bool>,
    pub outcome: Option<String>,
}

impl Pipeline {
    /// The source content interval the encoder was told to read, in
    /// milliseconds. This is what becomes ffmpeg's `-t`.
    pub fn c_slot_ms(&self) -> Option<u64> {
        self.video()
            .map(|v| v.c_out_point_ms.saturating_sub(v.c_in_point_ms))
    }

    pub fn video(&self) -> Option<&TracedInput> {
        self.inputs.iter().find(|i| i.role == "video")
    }

    /// The source position the item started from, recovered by taking the
    /// schedule progress back out of the seek.
    ///
    /// Constant across every pipeline of one item on a sound build. A value
    /// that moves is a measured quantity reaching an input seek.
    pub fn c_base_in_point_ms(&self) -> Option<i64> {
        self.video()
            .map(|v| v.c_in_point_ms as i64 - self.s_playout_offset_ms as i64)
    }

    /// Frames per second, for the quantization law.
    pub fn fps(&self) -> Option<f64> {
        let rate = self.video()?.c_frame_rate.as_deref()?;
        let (num, den) = rate.split_once('/').unwrap_or((rate, "1"));
        let num: f64 = num.trim().parse().ok()?;
        let den: f64 = den.trim().parse().ok()?;
        if den == 0.0 || num == 0.0 {
            return None;
        }
        Some(num / den)
    }

    /// A short name for the source, for tables.
    pub fn label(&self) -> String {
        match self.video().and_then(|v| v.name.as_deref()) {
            Some(name) => name.chars().take(22).collect(),
            None => self.item_id.chars().take(8).collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Segment {
    pub w_utc: OffsetDateTime,
    pub q_path: String,
    pub q_media_sequence: u64,
    pub q_segments_held: usize,
    pub e_program_date_time: OffsetDateTime,
    pub e_duration_s: f64,
    pub e_last_segment_end: OffsetDateTime,
    pub e_session_start: OffsetDateTime,
    pub p_pts_offset_ms: u64,
    pub p_mpegts_90khz: u64,
    pub discontinuity: bool,
    /// The pipeline in flight when this segment landed.
    pub pipeline_seq: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct Trim {
    pub w_utc: OffsetDateTime,
    /// Set when the trim compares against the wall clock, which is the
    /// crossing that is not sound.
    pub w_cutoff: Option<OffsetDateTime>,
    /// Set when the trim compares against the served position instead, which
    /// keeps both sides on the emitted clock.
    pub e_trim_cutoff: Option<OffsetDateTime>,
    pub q_path: String,
    pub q_media_sequence: u64,
    pub q_segments_held: usize,
    pub e_program_date_time: OffsetDateTime,
}

#[derive(Debug, Clone)]
pub struct Publish {
    pub w_utc: OffsetDateTime,
    pub w_horizon: OffsetDateTime,
    pub q_media_sequence: u64,
    pub q_candidate: u64,
    pub q_clamped: u64,
    pub q_last_served: u64,
    pub q_skip: usize,
    pub q_limit: usize,
    pub q_head: usize,
    pub q_segments_held: usize,
    pub e_head_pdt: Option<OffsetDateTime>,
    pub e_tail_pdt: Option<OffsetDateTime>,
    pub e_oldest_pdt: Option<OffsetDateTime>,
    pub e_last_segment_end: OffsetDateTime,
}

impl Publish {
    /// True when the monotonic clamp held the window back this tick.
    pub fn clamped(&self) -> bool {
        self.q_clamped > self.q_candidate
    }
}

/// A wall clock reading that disagrees with the monotonic clock.
///
/// Nothing else in the system can see this. Two `OffsetDateTime` readings
/// cannot tell a slow hour from a stepped clock on their own.
#[derive(Debug, Clone)]
pub struct ClockStep {
    pub at: OffsetDateTime,
    pub w_delta_ms: i128,
    pub mono_delta_ms: i128,
}

impl ClockStep {
    pub fn skew_ms(&self) -> i128 {
        self.w_delta_ms - self.mono_delta_ms
    }
}

#[derive(Debug, Clone)]
pub struct Timeline {
    pub channel: String,
    pub origin: Option<Origin>,
    pub pipelines: Vec<Pipeline>,
    pub segments: Vec<Segment>,
    pub trims: Vec<Trim>,
    pub publishes: Vec<Publish>,
    pub clock_steps: Vec<ClockStep>,
    /// Records the writer made that never reached the file.
    pub lost_records: u64,
    pub first_seen: Option<OffsetDateTime>,
    pub last_seen: Option<OffsetDateTime>,
}

impl Timeline {
    /// The offset to add to an emitted reading before comparing it with a
    /// schedule reading. Zero unless the channel sets a virtual start.
    pub fn offset(&self) -> Duration {
        self.origin
            .as_ref()
            .map(|o| o.start_time_offset)
            .unwrap_or(Duration::ZERO)
    }

    /// How far the emitted clock stands from the schedule cursor.
    ///
    /// The offset has to come back in first. Without it the whole virtual
    /// start reads as drift, permanently, on every channel that sets one, and
    /// anything acting on the raw difference will trim or pad real content to
    /// chase a number that was never an error.
    pub fn stamp_error(&self, emitted: OffsetDateTime, schedule: OffsetDateTime) -> Duration {
        emitted + self.offset() - schedule
    }

    /// The same quantity without the correction, which is what a reader who
    /// has not met the split origin would compute.
    pub fn stamp_error_uncorrected(
        &self,
        emitted: OffsetDateTime,
        schedule: OffsetDateTime,
    ) -> Duration {
        emitted - schedule
    }

    /// Segments attributed to one pipeline.
    pub fn segments_of(&self, pipeline_seq: u64) -> impl Iterator<Item = &Segment> {
        self.segments
            .iter()
            .filter(move |s| s.pipeline_seq == Some(pipeline_seq))
    }

    /// Emitted media produced by one pipeline, measured on the emitted clock.
    pub fn emitted_by(&self, pipeline_seq: u64) -> Duration {
        let total: f64 = self.segments_of(pipeline_seq).map(|s| s.e_duration_s).sum();
        Duration::seconds_f64(total)
    }

    /// The whole run, ordered.
    pub fn span(&self) -> Option<(OffsetDateTime, OffsetDateTime)> {
        Some((self.first_seen?, self.last_seen?))
    }
}

/// Reads every record from the given files or folders.
///
/// A folder contributes every `clock-*.jsonl` inside it, so pointing at the
/// trace folder picks up whichever channels were recording.
pub fn load(paths: &[PathBuf]) -> std::io::Result<Vec<Timeline>> {
    let mut files: Vec<PathBuf> = Vec::new();

    for path in paths {
        if path.is_dir() {
            let mut found: Vec<PathBuf> = std::fs::read_dir(path)?
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| is_trace_file(p))
                .collect();
            found.sort();
            files.extend(found);
        } else {
            files.push(path.clone());
        }
    }

    let mut by_channel: HashMap<String, Vec<ClockRecord>> = HashMap::new();
    for file in &files {
        let body = std::fs::read_to_string(file)?;
        for (n, line) in body.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<ClockRecord>(line) {
                Ok(record) => by_channel
                    .entry(record.channel.clone())
                    .or_default()
                    .push(record),
                // a truncated tail is normal when a worker is killed mid write,
                // so a bad line is skipped rather than fatal
                Err(err) => eprintln!("{}:{}: skipped, {err}", file.display(), n + 1),
            }
        }
    }

    let mut timelines: Vec<Timeline> = by_channel
        .into_iter()
        .map(|(channel, mut records)| {
            records.sort_by_key(|r| (r.w_mono_ms, r.seq));
            fold(channel, records)
        })
        .collect();

    timelines.sort_by(|a, b| a.channel.cmp(&b.channel));
    Ok(timelines)
}

fn is_trace_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n.starts_with("clock-") && (n.ends_with(".jsonl") || n.ends_with(".prev")))
}

/// Joins the record stream into a timeline.
///
/// A selection record always precedes the pipeline record it belongs to, so
/// the selection is held until a pipeline claims it. The fallback path spawns
/// a replacement pipeline with no selection of its own, which is why a
/// pipeline must tolerate an absent one rather than assume the pairing.
///
/// Segments are attributed by the discontinuity marker, not by which pipeline
/// was running when the record was written. The publish loop absorbs the tail
/// of the outgoing pipeline before the incoming one raises its marker, so the
/// last segments of an item routinely arrive after its pipeline has ended.
/// Attributing by wall clock order would hand them to the wrong item, and a
/// pipeline that emits nothing at all would put every later attribution off by
/// one from then on.
pub fn fold(channel: String, records: Vec<ClockRecord>) -> Timeline {
    let mut timeline = Timeline {
        channel,
        origin: None,
        pipelines: Vec::new(),
        segments: Vec::new(),
        trims: Vec::new(),
        publishes: Vec::new(),
        clock_steps: Vec::new(),
        lost_records: 0,
        first_seen: None,
        last_seen: None,
    };

    let mut pending: Option<PendingSelection> = None;
    // the pipeline whose segments are landing now, and the one still waiting
    // for its discontinuity marker to claim them
    let mut attributed: Option<u64> = None;
    let mut awaiting: Option<u64> = None;
    let mut expect_seq: Option<u64> = None;
    let mut previous: Option<(OffsetDateTime, u64)> = None;

    for record in records {
        if timeline.first_seen.is_none() {
            timeline.first_seen = Some(record.w_utc);
        }
        timeline.last_seen = Some(record.w_utc);

        if let Some(expected) = expect_seq
            && record.seq > expected
        {
            timeline.lost_records += record.seq - expected;
        }
        expect_seq = Some(record.seq + 1);

        if let Some((w_prev, mono_prev)) = previous {
            let w_delta_ms = (record.w_utc - w_prev).whole_milliseconds();
            let mono_delta_ms = record.w_mono_ms as i128 - mono_prev as i128;
            // a second of slack absorbs the sub-second lag between taking the
            // two readings; anything past that is the system clock moving
            if (w_delta_ms - mono_delta_ms).abs() > 1_000 {
                timeline.clock_steps.push(ClockStep {
                    at: record.w_utc,
                    w_delta_ms,
                    mono_delta_ms,
                });
            }
        }
        previous = Some((record.w_utc, record.w_mono_ms));

        match record.event {
            ClockEvent::SessionStart {
                w_local,
                start_time_offset_ms,
                s_transcoded_until,
                e_last_segment_end,
                segment_seconds,
            } => {
                timeline.origin = Some(Origin {
                    w_local,
                    start_time_offset: Duration::milliseconds(start_time_offset_ms),
                    s_origin: s_transcoded_until,
                    e_origin: e_last_segment_end,
                    segment_seconds,
                });
            }

            ClockEvent::ItemSelected {
                item_id,
                s_transcoded_until,
                s_item_start: _,
                s_item_finish,
                p_scanned_pts_ms,
                state,
                realtime,
                filler,
            } => {
                pending = Some(PendingSelection {
                    item_id,
                    s_transcoded_until,
                    s_item_finish,
                    p_scanned_ms: p_scanned_pts_ms,
                    state,
                    realtime,
                    filler,
                });
            }

            ClockEvent::PipelineStart {
                pipeline_seq,
                item_id,
                start_at_zero,
                is_live,
                s_playout_offset_ms,
                s_item_start,
                s_timing_finish,
                p_pts_offset_ms,
                inputs,
                is_complete_expected,
                snapshot,
            } => {
                let claimed = pending
                    .take()
                    .filter(|selection| selection.item_id == item_id);

                awaiting = Some(pipeline_seq);
                timeline.pipelines.push(Pipeline {
                    seq: pipeline_seq,
                    item_id,
                    w_start: record.w_utc,
                    w_end: None,
                    state: claimed.as_ref().map(|s| s.state.clone()),
                    realtime: claimed.as_ref().map(|s| s.realtime),
                    filler: claimed.as_ref().is_some_and(|s| s.filler),
                    start_at_zero,
                    is_live,
                    s_transcoded_until: claimed.as_ref().map(|s| s.s_transcoded_until),
                    s_item_start,
                    s_item_finish: claimed.as_ref().map(|s| s.s_item_finish),
                    s_playout_offset_ms,
                    s_timing_finish,
                    s_finish: None,
                    p_scanned_ms: claimed.as_ref().and_then(|s| s.p_scanned_ms),
                    p_pts_offset_ms,
                    inputs,
                    e_at_start: snapshot.e_last_segment_end,
                    q_media_sequence: snapshot.q_media_sequence,
                    q_segments_held: snapshot.q_segments_held,
                    is_complete_expected,
                    is_complete: None,
                    outcome: None,
                });
            }

            ClockEvent::PipelineEnd {
                pipeline_seq,
                item_id: _,
                s_finish,
                is_complete,
                outcome,
            } => {
                if let Some(pipeline) = timeline
                    .pipelines
                    .iter_mut()
                    .rev()
                    .find(|p| p.seq == pipeline_seq)
                {
                    pipeline.w_end = Some(record.w_utc);
                    pipeline.s_finish = s_finish;
                    pipeline.is_complete = Some(is_complete);
                    pipeline.outcome = Some(outcome);
                }
            }

            ClockEvent::SegmentAdded {
                q_path,
                q_media_sequence,
                q_segments_held,
                e_program_date_time,
                e_duration_s,
                e_last_segment_end,
                e_session_start,
                p_pts_offset_ms,
                p_mpegts_90khz,
                discontinuity,
            } => {
                if discontinuity && awaiting.is_some() {
                    attributed = awaiting.take();
                }

                timeline.segments.push(Segment {
                    w_utc: record.w_utc,
                    q_path,
                    q_media_sequence,
                    q_segments_held,
                    e_program_date_time,
                    e_duration_s,
                    e_last_segment_end,
                    e_session_start,
                    p_pts_offset_ms,
                    p_mpegts_90khz,
                    discontinuity,
                    pipeline_seq: attributed,
                });
            }

            ClockEvent::SegmentTrimmed {
                q_path,
                q_media_sequence,
                q_segments_held,
                e_program_date_time,
                w_cutoff,
                e_trim_cutoff,
            } => {
                timeline.trims.push(Trim {
                    w_utc: record.w_utc,
                    w_cutoff,
                    e_trim_cutoff,
                    q_path,
                    q_media_sequence,
                    q_segments_held,
                    e_program_date_time,
                });
            }

            ClockEvent::Publish {
                w_horizon,
                q_media_sequence,
                q_candidate,
                q_clamped,
                q_last_served,
                q_skip,
                q_limit,
                q_head,
                q_segments_held,
                e_head_pdt,
                e_tail_pdt,
                e_oldest_pdt,
                e_last_segment_end,
            } => {
                timeline.publishes.push(Publish {
                    w_utc: record.w_utc,
                    w_horizon,
                    q_media_sequence,
                    q_candidate,
                    q_clamped,
                    q_last_served,
                    q_skip,
                    q_limit,
                    q_head,
                    q_segments_held,
                    e_head_pdt,
                    e_tail_pdt,
                    e_oldest_pdt,
                    e_last_segment_end,
                });
            }
        }
    }

    timeline
}

struct PendingSelection {
    item_id: String,
    s_transcoded_until: OffsetDateTime,
    s_item_finish: OffsetDateTime,
    p_scanned_ms: Option<u64>,
    state: String,
    realtime: bool,
    filler: bool,
}
