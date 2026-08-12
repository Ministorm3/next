use std::path::Path;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use time::format_description::well_known::{Iso8601, iso8601};

use crate::error::PlayoutError;

const DATE_CONFIG: iso8601::EncodedConfig =
    iso8601::Config::DEFAULT.set_use_separators(false).encode();

pub const DATE_FORMAT: Iso8601<DATE_CONFIG> = Iso8601::<DATE_CONFIG>;

pub const SUPPORTED_SCHEMA: SchemaVersion = SchemaVersion {
    breaking: 0,
    compatible: 3,
};
const VERSION_URI_PREFIX: &str = "https://ersatztv.org/playout/version/0.";

// TODO: support major version post-1.0
#[derive(Debug, Clone, Copy)]
pub struct SchemaVersion {
    pub breaking: u32,
    pub compatible: u32,
}

impl SchemaVersion {
    pub fn parse(uri: &str) -> Option<SchemaVersion> {
        let rest = uri.strip_prefix(VERSION_URI_PREFIX)?;
        let (b, a) = rest.split_once('.')?;
        Some(SchemaVersion {
            breaking: b.parse().ok()?,
            compatible: a.parse().ok()?,
        })
    }
}

/// A playout schedule for a single time window.
///
/// Files should be named `{start}_{finish}.json` using compact ISO 8601
/// (no separators), e.g. `20260413T000000.000000000-0500_20260414T002131.620000000-0500.json`,
/// so that the channel can locate the correct file for the current time.
#[derive(Debug, Deserialize, Serialize)]
pub struct Playout {
    /// URI identifying the schema version, e.g. "https://ersatztv.org/playout/version/0.0.1"
    pub version: String,
    pub items: Vec<PlayoutItem>,
}

impl Playout {
    pub fn new(items: Vec<PlayoutItem>) -> Self {
        Playout {
            version: format!(
                "{}{}.{}",
                VERSION_URI_PREFIX, SUPPORTED_SCHEMA.breaking, SUPPORTED_SCHEMA.compatible
            ),
            items,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PlayoutItem {
    pub id: String,
    /// RFC3339 formatted date/time, e.g. 2026-04-13T00:24:21.527-05:00
    #[serde(with = "time::serde::rfc3339")]
    pub start: OffsetDateTime,
    /// RFC3339 formatted date/time, e.g. 2026-04-13T00:24:21.527-05:00
    #[serde(with = "time::serde::rfc3339")]
    pub finish: OffsetDateTime,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<PlayoutItemSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tracks: Option<PlayoutItemTracks>,
    /// What the shared session plays for this item in place of `source`,
    /// when `source` is templated and cannot be tuned without a viewer's
    /// query values.
    ///
    /// This substitutes the source for one session, it does not replace the
    /// item: the item keeps its id, its slot and its own `source`, because
    /// cohort viewers still get the live presentation through variant
    /// sessions, and that is only possible while the templated URL is still
    /// here to resolve per cohort.
    ///
    /// Omitted when the item has no slate, which is every item written
    /// before this field existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slate: Option<PlayoutItemSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watermark: Option<Watermark>,
}

impl PlayoutItem {
    pub fn new(
        id: String,
        start: OffsetDateTime,
        finish: OffsetDateTime,
        in_point: Option<std::time::Duration>,
        out_point: Option<std::time::Duration>,
        path: &Path,
    ) -> Result<PlayoutItem, PlayoutError> {
        Ok(PlayoutItem {
            id,
            start,
            finish,
            source: Some(PlayoutItemSource::Local {
                path: path.to_string_lossy().to_string(),
                in_point_ms: in_point.map(|d| d.as_millis() as u64),
                out_point_ms: out_point.map(|d| d.as_millis() as u64),
                probe_hint: None,
            }),
            tracks: None,
            slate: None,
            watermark: None,
        })
    }

    pub fn finish(&self) -> OffsetDateTime {
        self.finish
    }

    /// The `{query:}` variable names any of this item's sources reference,
    /// lowercased.
    ///
    /// The slate is deliberately not among them. It is what plays when no
    /// viewer query values are available, so a variable inside it names
    /// nothing a cohort could be routed by.
    pub fn query_variable_names(&self) -> std::collections::BTreeSet<String> {
        let mut names = std::collections::BTreeSet::new();

        let track_sources = self.tracks.iter().flat_map(|t| {
            [t.audio.as_ref(), t.video.as_ref(), t.subtitle.as_ref()]
                .into_iter()
                .flatten()
                .filter_map(|s| s.source.as_ref())
        });

        for source in self.source.iter().chain(track_sources) {
            names.append(&mut source.query_variable_names());
        }

        names
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PlayoutItemTracks {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<TrackSelection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video: Option<TrackSelection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subtitle: Option<TrackSelection>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TrackSelection {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<PlayoutItemSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_index: Option<u32>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Watermark {
    pub source: PlayoutItemSource,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_index: Option<u32>,
    pub location: WatermarkLocation,
    /// Scale to this percent of primary content width (0–100).
    /// Omitted = actual size.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub width_percent: Option<f32>,
    /// When `true`, position margins are measured from the edges of the source
    /// content rather than the padded output frame, so letterbox/pillarbox bars
    /// push the watermark inward and keep it inside the visible content. When
    /// `false`, margins are relative to the full padded frame, so a 0% margin
    /// can land inside the bars. Has no effect when the primary content fills
    /// the output (crop/stretch). Omitted = `false`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub within_source_content: Option<bool>,
    /// Horizontal offset from `location`, as percent of primary content width (0–100).
    /// Omitted = 0.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub horizontal_margin_percent: Option<f32>,
    /// Vertical offset from `location`, as percent of primary content height (0–100).
    /// Omitted = 0.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vertical_margin_percent: Option<f32>,
    /// Opacity as a percent (0–100). Omitted = fully opaque (100).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub opacity_percent: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timing: Option<WatermarkTiming>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WatermarkLocation {
    TopLeft,
    TopCenter,
    TopRight,
    CenterLeft,
    Center,
    CenterRight,
    BottomLeft,
    BottomCenter,
    BottomRight,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "timing_type", rename_all = "snake_case")]
pub enum WatermarkTiming {
    Periodic {
        clock: PeriodicClock,
        frequency_ms: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        phase_offset_ms: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        disable_after_ms: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        fade_ms: Option<u64>,
        hold_ms: u64,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PeriodicClock {
    Wall,
    Content,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(tag = "source_type", rename_all = "snake_case")]
pub enum PlayoutItemSource {
    Local {
        path: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        in_point_ms: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        out_point_ms: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        probe_hint: Option<ProbeHint>,
    },
    Lavfi {
        params: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        probe_hint: Option<ProbeHint>,
    },
    Http {
        /// URI template, e.g. "https://example.com/file.mkv?token={{MY_SECRET}}".
        /// Also supports single-brace stream variables resolved at playback
        /// time: {channel_number} and {query:name}, each with an optional
        /// |default (see [`crate::stream_variables`]).
        uri: String,
        /// Whether the content is live and therefore cannot seek or work
        /// ahead (default: false)
        #[serde(skip_serializing_if = "Option::is_none")]
        is_live: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        in_point_ms: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        out_point_ms: Option<u64>,
        /// Custom HTTP headers, e.g. ["Authorization: Bearer {{TOKEN}}"]
        #[serde(skip_serializing_if = "Option::is_none")]
        headers: Option<Vec<String>>,
        /// Custom user-agent string
        #[serde(skip_serializing_if = "Option::is_none")]
        user_agent: Option<String>,
        /// Socket timeout in microseconds
        #[serde(skip_serializing_if = "Option::is_none")]
        timeout_us: Option<u64>,
        /// Enable reconnect on failure (default: true)
        #[serde(skip_serializing_if = "Option::is_none")]
        reconnect: Option<bool>,
        /// Max reconnect delay in seconds
        /// Maps directly to the reconnect_delay_max ffmpeg option
        #[serde(skip_serializing_if = "Option::is_none")]
        reconnect_delay_max: Option<u32>,
        /// Enable persistent connections in ffmpeg (default: false)
        #[serde(skip_serializing_if = "Option::is_none")]
        keep_alive: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        probe_hint: Option<ProbeHint>,
    },
    Rtsp {
        /// RTSP URI template; supports the same single-brace stream variables
        /// as Http uri: {channel_number} and {query:name}, each with an
        /// optional |default.
        uri: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        timeout_us: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        probe_hint: Option<ProbeHint>,
    },
    Script {
        /// Command that writes an MPEG-TS stream to its stdout
        command: String,
        /// Optional arguments for the command
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        args: Vec<String>,
        /// Whether the content is live and therefore cannot work ahead (default: false)
        #[serde(skip_serializing_if = "Option::is_none")]
        is_live: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        probe_hint: Option<ProbeHint>,
    },
    Dynamic {
        /// URI template, e.g. "https://example.com/file.mkv?token={{MY_SECRET}}"
        uri: String,
        /// Custom HTTP headers, e.g. ["Authorization: Bearer {{TOKEN}}"]
        #[serde(skip_serializing_if = "Option::is_none")]
        headers: Option<Vec<String>>,
        /// Custom user-agent string
        #[serde(skip_serializing_if = "Option::is_none")]
        user_agent: Option<String>,
        /// Socket timeout in microseconds
        #[serde(skip_serializing_if = "Option::is_none")]
        timeout_us: Option<u64>,
    },
}

impl PlayoutItemSource {
    /// The `{query:}` variable names this source's URI references, lowercased.
    pub fn query_variable_names(&self) -> std::collections::BTreeSet<String> {
        match self {
            PlayoutItemSource::Http { uri, .. } | PlayoutItemSource::Rtsp { uri, .. } => {
                crate::stream_variables::query_variable_names(uri)
            }
            _ => std::collections::BTreeSet::new(),
        }
    }

    pub fn probe_hint(&self) -> Option<&ProbeHint> {
        match self {
            PlayoutItemSource::Local { probe_hint, .. }
            | PlayoutItemSource::Lavfi { probe_hint, .. }
            | PlayoutItemSource::Http { probe_hint, .. }
            | PlayoutItemSource::Rtsp { probe_hint, .. }
            | PlayoutItemSource::Script { probe_hint, .. } => probe_hint.as_ref(),
            PlayoutItemSource::Dynamic { .. } => None,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct ProbeHint {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub video: Vec<VideoHint>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub audio: Vec<AudioHint>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subtitle: Vec<SubtitleHint>,
    pub format_name: Option<String>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Default, Deserialize, Serialize, Clone, PartialEq)]
pub struct VideoHint {
    pub codec: String,
    pub width: u32,
    pub height: u32,
    pub pix_fmt: String,
    pub stream_index: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frame_rate: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field_order: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample_aspect_ratio: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_aspect_ratio: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub color_range: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub color_space: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub color_transfer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub color_primaries: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dv_profile: Option<u32>,
}

impl VideoHint {
    pub fn new(codec: String, width: u32, height: u32, pix_fmt: String) -> VideoHint {
        VideoHint {
            stream_index: 0,
            codec,
            width,
            height,
            pix_fmt,
            ..Default::default()
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct AudioHint {
    pub codec: String,
    pub channels: u32,
    pub stream_index: u32,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct SubtitleHint {
    pub codec: String,
    pub stream_index: u32,
}

pub struct PlayoutLoadResult {
    pub playout: Playout,
    // TODO: start, finish
}

pub async fn from_file(path: &str) -> Result<PlayoutLoadResult, PlayoutError> {
    #[derive(Deserialize)]
    struct PlayoutVersion {
        version: String,
    }

    let contents = tokio::fs::read_to_string(path).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            PlayoutError::PlayoutJsonDoesNotExist
        } else {
            PlayoutError::PlayoutJsonLoadError(e.to_string())
        }
    })?;

    let version_only: PlayoutVersion = serde_json::from_str(&contents)
        .map_err(|e| PlayoutError::PlayoutJsonLoadError(e.to_string()))?;

    let found = SchemaVersion::parse(&version_only.version)
        .ok_or_else(|| PlayoutError::UnrecognizedSchemaVersion(version_only.version.clone()))?;

    if found.breaking != SUPPORTED_SCHEMA.breaking || found.compatible > SUPPORTED_SCHEMA.compatible
    {
        return Err(PlayoutError::UnsupportedSchemaVersion(
            version_only.version,
            format!(
                "{}{}.{}",
                VERSION_URI_PREFIX, SUPPORTED_SCHEMA.breaking, SUPPORTED_SCHEMA.compatible
            ),
        ));
    }

    let playout: Playout = serde_json::from_str(&contents)
        .map_err(|e| PlayoutError::PlayoutJsonLoadError(e.to_string()))?;

    Ok(PlayoutLoadResult { playout })
}

pub fn parse_playout_filename(file_stem: &str) -> Option<(OffsetDateTime, OffsetDateTime)> {
    let split: Vec<&str> = file_stem.split("_").collect();
    if split.len() == 2 {
        let maybe_start = OffsetDateTime::parse(split[0], &DATE_FORMAT)
            .ok()
            .or_else(|| parse_unix_timestamp(split[0]));

        let maybe_finish = OffsetDateTime::parse(split[1], &DATE_FORMAT)
            .ok()
            .or_else(|| parse_unix_timestamp(split[1]));

        return match (maybe_start, maybe_finish) {
            (Some(start), Some(finish)) => Some((start, finish)),
            _ => None,
        };
    }

    None
}

fn parse_unix_timestamp(timestamp: &str) -> Option<OffsetDateTime> {
    let maybe_epoch = timestamp
        .parse::<i64>()
        .map(|i| if timestamp.len() > 10 { i / 1000 } else { i });

    if let Ok(epoch) = maybe_epoch {
        OffsetDateTime::from_unix_timestamp(epoch).ok()
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    /// A templated star window as a scheduler exports it, plus whatever
    /// slate the caller wants declared on it.
    fn templated_item(slate: Option<Value>) -> Value {
        let mut item = json!({
            "id": "12205801",
            "start": "2026-08-11T20:20:00.000-04:00",
            "finish": "2026-08-11T20:21:43.000-04:00",
            "source": {
                "source_type": "http",
                "uri": "http://host/live.ts?zip={query:zip}",
                "is_live": true
            }
        });

        if let Some(slate) = slate {
            item["slate"] = slate;
        }

        item
    }

    /// Every playout file written before this field existed omits it, and
    /// those are the files in production right now. Absence must read as
    /// "no slate", not as a parse error.
    #[test]
    fn a_playout_without_the_field_still_parses() {
        let playout: Playout = serde_json::from_value(json!({
            "version": "https://ersatztv.org/playout/version/0.0.2",
            "items": [templated_item(None)]
        }))
        .unwrap();

        assert_eq!(playout.items.len(), 1);
        assert_eq!(playout.items[0].slate, None);
    }

    /// The slate is a full source rather than a bare path, and it is the
    /// same type the item's own `source` is, so every source detail a
    /// scheduler can express (here a probe hint, which spares the worker a
    /// probe) reaches the worker unchanged.
    #[test]
    fn a_slate_parses_as_the_source_it_is() {
        let item: PlayoutItem = serde_json::from_value(templated_item(Some(json!({
            "source_type": "local",
            "path": "/bumps/fallback/WeatherSlateStatic.mp4",
            "probe_hint": { "format_name": "mov,mp4,m4a", "duration_ms": 15000 }
        }))))
        .unwrap();

        match item.slate {
            Some(PlayoutItemSource::Local {
                path, probe_hint, ..
            }) => {
                assert_eq!(path, "/bumps/fallback/WeatherSlateStatic.mp4");
                assert_eq!(probe_hint.and_then(|h| h.duration_ms), Some(15_000));
            }
            other => panic!("expected a local slate source, got {other:?}"),
        }

        // the slate substitutes a source for one session, it does not
        // replace the item: the templated URL variant sessions resolve per
        // cohort is still here
        assert!(matches!(item.source, Some(PlayoutItemSource::Http { .. })));
    }

    /// An item with no slate must serialize exactly as it did before the
    /// field existed. Playout files are read and diffed by other tools, and
    /// an explicit `null` is not the same as an absent key to any of them.
    #[test]
    fn an_absent_slate_is_omitted_rather_than_written_as_null() {
        let item: PlayoutItem = serde_json::from_value(templated_item(None)).unwrap();
        let written = serde_json::to_string(&item).unwrap();

        assert!(
            !written.contains("slate"),
            "an absent slate must not be written at all, got {written}"
        );
    }

    /// The slate names no cohort. It is what plays when a viewer's query
    /// values are unavailable, so a `{query:}` variable written inside one
    /// must not join the set of parameters that route viewers.
    #[test]
    fn a_slate_contributes_no_query_variables() {
        let item: PlayoutItem = serde_json::from_value(templated_item(Some(json!({
            "source_type": "http",
            "uri": "http://host/slate.ts?market={query:market}"
        }))))
        .unwrap();

        let names = item.query_variable_names();
        assert!(names.contains("zip"), "got {names:?}");
        assert!(!names.contains("market"), "got {names:?}");
    }
}
