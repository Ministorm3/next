//! The record schema.
//!
//! Every field carries a one letter prefix naming the clock it is read from,
//! so a raw trace line is legible without the domain map at hand:
//!
//! - `w_` wall clock, either `now_utc` or `now_local`
//! - `s_` schedule cursor, the `transcoded_until` family
//! - `e_` emitted media clock, the `last_segment_end` family
//! - `p_` media presentation timestamps
//! - `q_` sequence counters, which are counts and not times
//! - `c_` source content positions, zero at the start of a source file
//!
//! A record reports readings only. It never reports a difference between two
//! domains. Differences belong to the analyzer, where a wrong formula can be
//! corrected without redeploying a worker, and where the correction is visible
//! in review. A worker that computed its own drift would bake one reading of
//! the rules into every trace it ever wrote.

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// One line of the trace file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClockRecord {
    /// Monotonic per process counter. A gap means records were lost.
    pub seq: u64,
    pub channel: String,
    /// Wall clock at the moment the record was written.
    #[serde(with = "time::serde::rfc3339")]
    pub w_utc: OffsetDateTime,
    /// Milliseconds on the monotonic clock since the trace opened. Compare
    /// against `w_utc` differences to catch a system clock step, which no
    /// `OffsetDateTime` reading can reveal on its own.
    pub w_mono_ms: u64,
    #[serde(flatten)]
    pub event: ClockEvent,
}

/// One input as the pipeline was told to read it.
///
/// `c_in_point_ms` and `c_out_point_ms` are positions inside the source file,
/// not schedule times. Their difference becomes ffmpeg's `-t`, which bounds an
/// emitted duration and quantizes upward to a whole frame.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TracedInput {
    /// `audio`, `video` or `subtitle`.
    pub role: String,
    /// `local`, `lavfi`, `http`, `rtsp` or `script`.
    pub kind: String,
    /// File name for a local source, or the filter graph for lavfi. Never a
    /// full path, and never a host.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub c_in_point_ms: u64,
    pub c_out_point_ms: u64,
    /// The probed frame rate as a rational string, for example `24000/1001`.
    /// The frame quantization law needs it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub c_frame_rate: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub c_probed_duration_ms: Option<u64>,
}

/// What the playlist manager held at one instant.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ClockSnapshot {
    #[serde(with = "time::serde::rfc3339")]
    pub e_last_segment_end: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub e_session_start: OffsetDateTime,
    pub q_media_sequence: u64,
    pub q_segments_held: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum ClockEvent {
    /// Written once, before any transcode. Pins the origin of every cursor.
    ///
    /// `s_transcoded_until` and `e_last_segment_end` are seeded from two
    /// different readings when `virtual_start` is set: the schedule cursor
    /// takes the shifted reading and the emitted clock takes the unshifted
    /// one. Recording both, plus the offset between them, is what lets the
    /// analyzer subtract the offset back out before calling anything drift.
    SessionStart {
        #[serde(with = "time::serde::rfc3339")]
        w_local: OffsetDateTime,
        /// `virtual_start - now`, zero unless the channel sets a virtual start.
        start_time_offset_ms: i64,
        #[serde(with = "time::serde::rfc3339")]
        s_transcoded_until: OffsetDateTime,
        #[serde(with = "time::serde::rfc3339")]
        e_last_segment_end: OffsetDateTime,
        segment_seconds: u32,
    },

    /// The schedule cursor picked an item. No emitted or sequence readings
    /// here, because taking them would need the playlist lock. The analyzer
    /// carries them forward from the last record that did hold it.
    ItemSelected {
        item_id: String,
        #[serde(with = "time::serde::rfc3339")]
        s_transcoded_until: OffsetDateTime,
        #[serde(with = "time::serde::rfc3339")]
        s_item_start: OffsetDateTime,
        #[serde(with = "time::serde::rfc3339")]
        s_item_finish: OffsetDateTime,
        /// What the scanner read off the newest segment. This is a
        /// conservative floor for the next encode, not a measurement of
        /// content produced, and it must never reach an input seek.
        p_scanned_pts_ms: Option<u64>,
        /// One of the four buffering states.
        state: String,
        realtime: bool,
        /// True when no playout item covered the cursor and black filled in.
        filler: bool,
    },

    /// ffmpeg is about to be spawned. Written at the pipeline boundary, where
    /// the playlist lock is already held, so it can carry a full snapshot.
    PipelineStart {
        pipeline_seq: u64,
        item_id: String,
        start_at_zero: bool,
        is_live: bool,
        /// How far into the item the schedule cursor already stood.
        s_playout_offset_ms: u64,
        #[serde(with = "time::serde::rfc3339")]
        s_item_start: OffsetDateTime,
        /// Where this pipeline expects to leave the schedule cursor.
        #[serde(with = "time::serde::rfc3339")]
        s_timing_finish: OffsetDateTime,
        /// The offset handed to the encoder. On a sound build this only ever
        /// keeps output timestamps monotonic across a pipeline change.
        p_pts_offset_ms: Option<u64>,
        inputs: Vec<TracedInput>,
        /// False when the work ahead limit cut the item short.
        is_complete_expected: bool,
        #[serde(flatten)]
        snapshot: ClockSnapshot,
    },

    /// ffmpeg exited.
    ///
    /// `s_finish` is where the schedule cursor is about to land, and is absent
    /// on every path that ends the pipeline without advancing it.
    PipelineEnd {
        pipeline_seq: u64,
        item_id: String,
        #[serde(default, with = "time::serde::rfc3339::option")]
        s_finish: Option<OffsetDateTime>,
        is_complete: bool,
        /// `ok`, `failed`, `stalled` or `idle_timeout`.
        outcome: String,
    },

    /// A segment reached the playlist. The only place the emitted clock moves.
    SegmentAdded {
        q_path: String,
        q_media_sequence: u64,
        q_segments_held: usize,
        /// The stamp this segment carries to clients.
        #[serde(with = "time::serde::rfc3339")]
        e_program_date_time: OffsetDateTime,
        /// The EXTINF ffmpeg reported. The emitted clock advances by exactly
        /// this and by nothing else.
        e_duration_s: f64,
        #[serde(with = "time::serde::rfc3339")]
        e_last_segment_end: OffsetDateTime,
        #[serde(with = "time::serde::rfc3339")]
        e_session_start: OffsetDateTime,
        p_pts_offset_ms: u64,
        /// The value written into the WebVTT timestamp map.
        p_mpegts_90khz: u64,
        discontinuity: bool,
    },

    /// A segment was deleted from disk.
    ///
    /// Exactly one of the two cutoffs is present, and which one says how the
    /// trim is implemented on the build that wrote the record.
    ///
    /// `w_cutoff` means the trim measured an emitted stamp against the wall
    /// clock. That crossing is not sound: the cutoff walks forward with real
    /// time while the stamps do not, so retained history is the budget minus
    /// the channel's lag and reaches zero once the lag reaches the budget.
    ///
    /// `e_trim_cutoff` means the trim measured against the position being
    /// served, which is an emitted reading on both sides and therefore sound.
    ///
    /// Recording them under separate names rather than one is what lets a
    /// reader tell from the trace alone which trim a channel is running.
    SegmentTrimmed {
        q_path: String,
        q_media_sequence: u64,
        q_segments_held: usize,
        #[serde(with = "time::serde::rfc3339")]
        e_program_date_time: OffsetDateTime,
        #[serde(default, with = "time::serde::rfc3339::option")]
        w_cutoff: Option<OffsetDateTime>,
        #[serde(default, with = "time::serde::rfc3339::option")]
        e_trim_cutoff: Option<OffsetDateTime>,
    },

    /// A window was published to clients.
    Publish {
        /// `now + PUBLISH_LEAD`, the newest stamp allowed into the window.
        #[serde(with = "time::serde::rfc3339")]
        w_horizon: OffsetDateTime,
        q_media_sequence: u64,
        /// What the window start would have been without the monotonic clamp.
        q_candidate: u64,
        /// What it became after the clamp.
        q_clamped: u64,
        /// The clamp floor, read before this publish moved it.
        q_last_served: u64,
        q_skip: usize,
        q_limit: usize,
        /// Index one past the newest segment inside the horizon.
        q_head: usize,
        q_segments_held: usize,
        /// Stamp of the newest segment in the window, the live edge.
        #[serde(default, with = "time::serde::rfc3339::option")]
        e_head_pdt: Option<OffsetDateTime>,
        /// Stamp of the oldest segment in the window. How far a client may
        /// rewind before it meets a segment the trim has already deleted.
        #[serde(default, with = "time::serde::rfc3339::option")]
        e_tail_pdt: Option<OffsetDateTime>,
        /// Stamp of the oldest segment still on disk, published or not.
        ///
        /// Against `e_last_segment_end` this is the history the channel
        /// actually retains, which is the quantity the trim erodes. It cannot
        /// be recovered from the trim records, because a trimmed segment is
        /// older than the cutoff by definition and so says nothing about how
        /// much was left behind it.
        #[serde(default, with = "time::serde::rfc3339::option")]
        e_oldest_pdt: Option<OffsetDateTime>,
        #[serde(with = "time::serde::rfc3339")]
        e_last_segment_end: OffsetDateTime,
    },
}

impl ClockEvent {
    /// The lowest level at which this event is written.
    pub fn level(&self) -> super::Level {
        match self {
            ClockEvent::SessionStart { .. }
            | ClockEvent::ItemSelected { .. }
            | ClockEvent::PipelineStart { .. }
            | ClockEvent::PipelineEnd { .. } => super::Level::Items,
            ClockEvent::SegmentAdded { .. } | ClockEvent::SegmentTrimmed { .. } => {
                super::Level::Segments
            }
            ClockEvent::Publish { .. } => super::Level::All,
        }
    }

    /// The tag written as the `event` field, for grouping and for messages.
    pub fn name(&self) -> &'static str {
        match self {
            ClockEvent::SessionStart { .. } => "session_start",
            ClockEvent::ItemSelected { .. } => "item_selected",
            ClockEvent::PipelineStart { .. } => "pipeline_start",
            ClockEvent::PipelineEnd { .. } => "pipeline_end",
            ClockEvent::SegmentAdded { .. } => "segment_added",
            ClockEvent::SegmentTrimmed { .. } => "segment_trimmed",
            ClockEvent::Publish { .. } => "publish",
        }
    }
}
