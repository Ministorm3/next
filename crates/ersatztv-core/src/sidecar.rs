//! Machine-readable description of a session's generated playlist, published
//! next to it as `<playlist>.meta.json`.
//!
//! The worker's playlist manager writes this; the server's playlist composer
//! and variant spawning read it. It exists so splice points are declared at
//! production time: which playout item produced each segment, and which
//! `-output_ts_offset` each pipeline started with, are recorded rather than
//! inferred from timestamps.

use serde::{Deserialize, Serialize};

/// Suffix appended to a playlist file name to form its sidecar's name.
pub const SIDECAR_SUFFIX: &str = ".meta.json";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PlaylistSidecar {
    pub segments: Vec<SidecarSegment>,
    pub pipelines: Vec<SidecarPipeline>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SidecarSegment {
    pub path: String,
    pub duration: f64,
    pub program_date_time: String,
    pub item_id: String,
    pub discontinuity: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SidecarPipeline {
    pub item_id: String,
    pub pts_offset_ms: u64,
    /// Whether the item's source URI references `{query:}` variables, so
    /// variant sessions may transcode it with different values.
    #[serde(default)]
    pub templated: bool,
}
