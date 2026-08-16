//! A structured record of every clock reading a channel worker takes.
//!
//! # Why this exists
//!
//! Every timing defect in this project has been a confusion between two
//! clocks rather than an arithmetic mistake. The worker keeps six of them and
//! none of the free text logging says which one a number came from, so
//! tracing a defect has meant re-deriving the domain of every value by hand.
//!
//! This module writes one JSON object per line, each field prefixed with the
//! letter of the clock it was read from. See [`event`] for the schema and for
//! the rule that records carry readings only, never differences.
//!
//! # Enabling it
//!
//! Compiled out entirely unless the `clock-trace` feature is on. With the
//! feature on it stays dormant until the environment asks for it, so an image
//! can ship with the seam present and switch it on per channel without a
//! rebuild.
//!
//! - `ETV_CLOCK_TRACE` a directory. Unset or empty means off.
//! - `ETV_CLOCK_TRACE_LEVEL` `items`, `segments` or `all`. Default `segments`.
//! - `ETV_CLOCK_TRACE_MAX_MB` roll threshold per file. Default 64.
//!
//! Each worker writes `clock-<channel>.jsonl`. At the threshold the file moves
//! to `clock-<channel>.jsonl.prev` and a fresh one opens, so a long running
//! channel costs at most twice the threshold on disk.
//!
//! # Cost
//!
//! One branch per call site when dormant, because [`ClockTrace::emit`] takes a
//! closure and never builds an event it will not write. When active, one
//! formatted write per record with no buffering. Records are flushed as they
//! are made so that a worker killed by the stall watchdog still leaves the
//! readings that led up to the kill, which is exactly the case worth tracing.

mod event;

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub use event::{ClockEvent, ClockRecord, ClockSnapshot, TracedInput};
use time::OffsetDateTime;

const DEFAULT_MAX_MB: u64 = 64;

/// How much of the run to record.
///
/// The levels are ordered by volume. A four second segment makes roughly one
/// `Segments` record every two seconds, and the publish loop makes an `All`
/// record every two seconds on top of that. `Items` costs a handful of records
/// per item and is the level to leave on for days.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Items,
    Segments,
    All,
}

impl Level {
    fn parse(value: &str) -> Option<Level> {
        match value.trim().to_ascii_lowercase().as_str() {
            "items" | "item" => Some(Level::Items),
            "segments" | "segment" => Some(Level::Segments),
            "all" => Some(Level::All),
            _ => None,
        }
    }
}

struct Inner {
    level: Level,
    channel: String,
    path: PathBuf,
    max_bytes: u64,
    opened_at: Instant,
    seq: AtomicU64,
    file: Mutex<Option<Sink>>,
}

struct Sink {
    file: File,
    bytes: u64,
}

/// A handle to the trace. Cloning is cheap and every clone writes to the same
/// file, which is what lets the playlist manager keep its own copy.
#[derive(Clone)]
pub struct ClockTrace(Option<Arc<Inner>>);

impl ClockTrace {
    /// A handle that discards everything.
    pub fn disabled() -> ClockTrace {
        ClockTrace(None)
    }

    /// Reads the environment and opens a trace file if it asks for one.
    ///
    /// Never fails the caller. A trace that cannot be opened is a trace that
    /// is off, because losing observability is always better than losing the
    /// channel it was meant to observe.
    pub fn from_env(channel: &str) -> ClockTrace {
        let Some(folder) = std::env::var("ETV_CLOCK_TRACE")
            .ok()
            .filter(|f| !f.trim().is_empty())
        else {
            return ClockTrace::disabled();
        };

        let level = std::env::var("ETV_CLOCK_TRACE_LEVEL")
            .ok()
            .and_then(|l| Level::parse(&l))
            .unwrap_or(Level::Segments);

        let max_bytes = std::env::var("ETV_CLOCK_TRACE_MAX_MB")
            .ok()
            .and_then(|m| m.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_MAX_MB)
            .saturating_mul(1024 * 1024)
            .max(1024 * 1024);

        let folder = PathBuf::from(folder);
        if let Err(err) = std::fs::create_dir_all(&folder) {
            log_failure(&format!("cannot create {}: {err}", folder.display()));
            return ClockTrace::disabled();
        }

        let path = folder.join(format!("clock-{}.jsonl", sanitize(channel)));
        let sink = match open_sink(&path) {
            Ok(sink) => sink,
            Err(err) => {
                log_failure(&format!("cannot open {}: {err}", path.display()));
                return ClockTrace::disabled();
            }
        };

        ClockTrace(Some(Arc::new(Inner {
            level,
            channel: channel.to_owned(),
            path,
            max_bytes,
            opened_at: Instant::now(),
            seq: AtomicU64::new(0),
            file: Mutex::new(Some(sink)),
        })))
    }

    /// True when a record at this level would be written.
    ///
    /// Call this before doing work that only a record needs, such as taking a
    /// lock or formatting a name.
    pub fn wants(&self, level: Level) -> bool {
        self.0.as_ref().is_some_and(|inner| level <= inner.level)
    }

    /// Writes one record, building the event only if it will be written.
    pub fn emit<F>(&self, build: F)
    where
        F: FnOnce() -> ClockEvent,
    {
        let Some(inner) = self.0.as_ref() else {
            return;
        };

        let event = build();
        if event.level() > inner.level {
            return;
        }

        let record = ClockRecord {
            seq: inner.seq.fetch_add(1, Ordering::Relaxed),
            channel: inner.channel.clone(),
            w_utc: OffsetDateTime::now_utc(),
            w_mono_ms: inner.opened_at.elapsed().as_millis() as u64,
            event,
        };

        let Ok(mut line) = serde_json::to_string(&record) else {
            return;
        };
        line.push('\n');

        inner.write(line.as_bytes());
    }
}

impl Inner {
    fn write(&self, bytes: &[u8]) {
        let Ok(mut guard) = self.file.lock() else {
            return;
        };
        let Some(sink) = guard.as_mut() else {
            return;
        };

        if sink.bytes + bytes.len() as u64 > self.max_bytes {
            match self.roll() {
                Ok(fresh) => *sink = fresh,
                Err(err) => {
                    log_failure(&format!("cannot roll {}: {err}", self.path.display()));
                    *guard = None;
                    return;
                }
            }
        }

        // a trace that cannot write is a trace that is off; a channel must
        // never die because its instrument did
        if let Err(err) = sink.file.write_all(bytes) {
            log_failure(&format!("cannot write {}: {err}", self.path.display()));
            *guard = None;
            return;
        }

        sink.bytes += bytes.len() as u64;
    }

    fn roll(&self) -> std::io::Result<Sink> {
        let previous = self.path.with_extension("jsonl.prev");
        std::fs::rename(&self.path, previous)?;
        open_sink(&self.path)
    }
}

fn open_sink(path: &Path) -> std::io::Result<Sink> {
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let bytes = file.metadata().map(|m| m.len()).unwrap_or(0);
    Ok(Sink { file, bytes })
}

/// Channel numbers are operator supplied, so keep them to characters that are
/// safe in a file name on every platform this runs on.
fn sanitize(channel: &str) -> String {
    let cleaned: String = channel
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();

    if cleaned.is_empty() {
        String::from("unknown")
    } else {
        cleaned
    }
}

fn log_failure(message: &str) {
    eprintln!("clock trace disabled: {message}");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The trace reads process wide environment variables, so the tests that
    /// set them cannot run beside each other.
    static ENV: Mutex<()> = Mutex::new(());

    #[test]
    fn level_orders_by_volume() {
        assert!(Level::Items < Level::Segments);
        assert!(Level::Segments < Level::All);
    }

    #[test]
    fn level_parses_the_documented_spellings() {
        assert_eq!(Level::parse("items"), Some(Level::Items));
        assert_eq!(Level::parse(" ALL "), Some(Level::All));
        assert_eq!(Level::parse("segment"), Some(Level::Segments));
        assert_eq!(Level::parse("verbose"), None);
    }

    #[test]
    fn a_disabled_trace_never_builds_its_event() {
        let trace = ClockTrace::disabled();
        let mut built = false;
        trace.emit(|| {
            built = true;
            ClockEvent::PipelineEnd {
                pipeline_seq: 0,
                item_id: String::new(),
                s_finish: Some(OffsetDateTime::UNIX_EPOCH),
                is_complete: true,
                outcome: String::from("ok"),
            }
        });
        assert!(!built, "the closure ran with the trace off");
        assert!(!trace.wants(Level::Items));
    }

    #[test]
    fn a_channel_number_never_escapes_its_file_name() {
        assert_eq!(sanitize("13"), "13");
        assert_eq!(sanitize("../../etc/passwd"), "______etc_passwd");
        assert_eq!(sanitize(""), "unknown");
    }

    /// The emitter and the analyzer share these types, so a round trip here is
    /// what guarantees they cannot drift apart.
    #[test]
    fn a_record_round_trips_through_the_file() {
        let _guard = ENV.lock().unwrap_or_else(|e| e.into_inner());
        let folder = tempfile::tempdir().expect("temp dir");
        // SAFETY: ENV serializes every test that touches these variables, and
        // the trace reads them once during from_env below
        unsafe {
            std::env::set_var("ETV_CLOCK_TRACE", folder.path());
            std::env::set_var("ETV_CLOCK_TRACE_LEVEL", "all");
        }

        let trace = ClockTrace::from_env("13");
        assert!(trace.wants(Level::All));

        let start = OffsetDateTime::UNIX_EPOCH;
        trace.emit(|| ClockEvent::SessionStart {
            w_local: start,
            start_time_offset_ms: -3_600_000,
            s_transcoded_until: start,
            e_last_segment_end: start,
            segment_seconds: 4,
        });
        trace.emit(|| ClockEvent::SegmentAdded {
            q_path: String::from("live000001.ts"),
            q_media_sequence: 0,
            q_segments_held: 1,
            e_program_date_time: start,
            e_duration_s: 4.004,
            e_last_segment_end: start + std::time::Duration::from_secs_f64(4.004),
            e_session_start: start,
            p_pts_offset_ms: 0,
            p_mpegts_90khz: 0,
            discontinuity: true,
        });

        unsafe {
            std::env::remove_var("ETV_CLOCK_TRACE");
            std::env::remove_var("ETV_CLOCK_TRACE_LEVEL");
        }

        let body = std::fs::read_to_string(folder.path().join("clock-13.jsonl")).expect("trace");
        let records: Vec<ClockRecord> = body
            .lines()
            .map(|l| serde_json::from_str(l).expect("record"))
            .collect();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].seq, 0);
        assert_eq!(records[1].seq, 1);
        assert_eq!(records[0].channel, "13");
        assert_eq!(records[0].event.name(), "session_start");

        // the offset is what lets an analyzer tell a virtual start apart from
        // real drift, so it has to survive the round trip intact
        let ClockEvent::SessionStart {
            start_time_offset_ms,
            ..
        } = records[0].event
        else {
            panic!("expected a session start");
        };
        assert_eq!(start_time_offset_ms, -3_600_000);

        let ClockEvent::SegmentAdded { e_duration_s, .. } = records[1].event else {
            panic!("expected a segment");
        };
        assert_eq!(e_duration_s, 4.004);
    }

    #[test]
    fn a_level_below_the_setting_is_not_written() {
        let _guard = ENV.lock().unwrap_or_else(|e| e.into_inner());
        let folder = tempfile::tempdir().expect("temp dir");
        // SAFETY: serialized by ENV above
        unsafe {
            std::env::set_var("ETV_CLOCK_TRACE", folder.path());
            std::env::set_var("ETV_CLOCK_TRACE_LEVEL", "items");
        }

        let trace = ClockTrace::from_env("7");

        unsafe {
            std::env::remove_var("ETV_CLOCK_TRACE");
            std::env::remove_var("ETV_CLOCK_TRACE_LEVEL");
        }

        assert!(trace.wants(Level::Items));
        assert!(!trace.wants(Level::Segments));

        trace.emit(|| ClockEvent::SegmentTrimmed {
            q_path: String::from("live000001.ts"),
            q_media_sequence: 1,
            q_segments_held: 0,
            e_program_date_time: OffsetDateTime::UNIX_EPOCH,
            w_cutoff: Some(OffsetDateTime::UNIX_EPOCH),
            e_trim_cutoff: None,
        });

        let body = std::fs::read_to_string(folder.path().join("clock-7.jsonl")).expect("trace");
        assert!(
            body.is_empty(),
            "a segment record was written at item level"
        );
    }
}
