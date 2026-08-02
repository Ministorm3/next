//! Variant session lifecycle.
//!
//! A variant session exists per (channel, cohort): it owns the cohort's
//! composed-playlist state, spawns variant workers for templated items as the
//! shared session's sidecar reveals them, keeps the worker alive via its
//! heartbeat file while the cohort keeps requesting, and is reaped when the
//! cohort goes idle. Identical cohorts share one session and one transcode.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Instant;

use ersatztv_core::HEARTBEAT_FILE_NAME;
use ersatztv_core::sidecar::{PlaylistSidecar, SIDECAR_SUFFIX};
use time::OffsetDateTime;
use tokio::sync::Mutex;

use crate::channel_model::ChannelModel;
use crate::channel_session::channel_binary_path;
use crate::composer::SessionPlaylist;

const MAX_SESSIONS_PER_CHANNEL: usize = 4;
const MAX_SESSIONS_TOTAL: usize = 8;

/// A cohort session is reaped after this long without a playlist request; the
/// variant worker itself exits separately once its heartbeat goes stale.
const SESSION_IDLE_SECONDS: u64 = 60;

const VARIANTS_FOLDER: &str = "variants";

pub struct VariantManager {
    sessions: Mutex<HashMap<String, VariantSession>>,
}

struct VariantSession {
    channel_number: String,
    cohort_query: String,
    folder: PathBuf,
    variant_prefix: String,
    playlist: SessionPlaylist,
    subtitle_playlist: SessionPlaylist,
    spawned_items: HashSet<String>,
    last_access: Instant,
}

impl VariantManager {
    pub fn new() -> VariantManager {
        VariantManager {
            sessions: Mutex::new(HashMap::new()),
        }
    }

    /// Serves a cohort's composed playlist, spawning variant workers as
    /// templated items appear in the shared sidecar. Returns None when no
    /// composition applies (no shared sidecar yet, or session caps reached),
    /// in which case the caller falls through to the shared playlist.
    pub async fn handle_playlist_request(
        &self,
        channel: &ChannelModel,
        cohort_query: &str,
        subtitles: bool,
    ) -> Option<String> {
        let shared = read_sidecar(&channel.output_folder().join("live.m3u8")).await?;

        let key = format!("{}|{}", channel.number(), cohort_query);
        let mut sessions = self.sessions.lock().await;

        reap_idle(&mut sessions);

        if !sessions.contains_key(&key) {
            let per_channel = sessions
                .values()
                .filter(|s| s.channel_number == channel.number())
                .count();
            if per_channel >= MAX_SESSIONS_PER_CHANNEL || sessions.len() >= MAX_SESSIONS_TOTAL {
                log::warn!(
                    "variant session cap reached; serving shared content to cohort '{cohort_query}' on channel {}",
                    channel.number()
                );
                return None;
            }

            let folder_name = cohort_folder_name(cohort_query);
            let folder = channel
                .output_folder()
                .join(VARIANTS_FOLDER)
                .join(&folder_name);

            sessions.insert(
                key.clone(),
                VariantSession {
                    channel_number: channel.number().to_owned(),
                    cohort_query: cohort_query.to_owned(),
                    folder,
                    variant_prefix: format!("{VARIANTS_FOLDER}/{folder_name}/"),
                    playlist: SessionPlaylist::default(),
                    subtitle_playlist: SessionPlaylist::default(),
                    spawned_items: HashSet::new(),
                    last_access: Instant::now(),
                },
            );
        }

        let session = sessions.get_mut(&key)?;
        session.last_access = Instant::now();

        // the worker exits when its heartbeat goes stale, so an active cohort
        // keeps its variant alive exactly like viewers keep a channel alive
        touch_heartbeat(&session.folder).await;

        spawn_missing_variants(session, channel, &shared).await;

        let variant = read_sidecar(&session.folder.join("live.m3u8")).await;

        let now = OffsetDateTime::now_utc();
        let playlist = if subtitles {
            session.subtitle_playlist.advance_and_render(
                &shared,
                variant.as_ref(),
                &session.variant_prefix,
                now,
                crate::composer::SEGMENT_SECONDS as u32,
                |s| format!("{}.vtt", s.strip_suffix(".ts").unwrap_or(s)),
            )
        } else {
            session.playlist.advance_and_render(
                &shared,
                variant.as_ref(),
                &session.variant_prefix,
                now,
                crate::composer::SEGMENT_SECONDS as u32,
                |s| s.to_owned(),
            )
        };

        Some(playlist)
    }
}

/// Spawns a variant worker for each templated item in the shared sidecar this
/// session has not spawned yet. The worker receives the shared item's
/// pts offset so both transcodes occupy the same envelope.
async fn spawn_missing_variants(
    session: &mut VariantSession,
    channel: &ChannelModel,
    shared: &PlaylistSidecar,
) {
    for pipeline in shared.pipelines.iter().filter(|p| p.templated) {
        if session.spawned_items.contains(&pipeline.item_id) {
            continue;
        }
        session.spawned_items.insert(pipeline.item_id.clone());

        let Ok(binary) = channel_binary_path() else {
            log::error!("cannot spawn variant: channel binary not found");
            continue;
        };

        if let Err(e) = tokio::fs::create_dir_all(&session.folder).await {
            log::error!("cannot create variant folder: {e}");
            continue;
        }

        // anchor the variant where the shared session's PUBLISHED coverage of
        // the item ends, not at the wall clock: a channel running behind
        // schedule would otherwise anchor the variant deep into the envelope
        // and never line up with the shared grid
        let progress_ms: u64 = shared
            .segments
            .iter()
            .filter(|s| s.item_id == pipeline.item_id)
            .map(|s| (s.duration.max(0f64) * 1000.0) as u64)
            .sum();

        // the shared session has already covered everything it declared for
        // this item, so there is nothing left for a variant to substitute
        if progress_ms >= pipeline.duration_ms {
            log::debug!(
                "not spawning variant for item {}: shared coverage {progress_ms}ms already fills its {}ms envelope",
                pipeline.item_id,
                pipeline.duration_ms
            );
            continue;
        }

        log::info!(
            "spawning variant for item {} on channel {} (cohort '{}', progress {}ms of a {}ms envelope)",
            pipeline.item_id,
            channel.number(),
            session.cohort_query,
            progress_ms,
            pipeline.duration_ms
        );

        let spawned = tokio::process::Command::new(binary)
            .arg("variant")
            .arg("--output-folder")
            .arg(&session.folder)
            .arg("--number")
            .arg(channel.number())
            .arg("--item-id")
            .arg(&pipeline.item_id)
            .arg("--pts-offset-ms")
            .arg(pipeline.pts_offset_ms.to_string())
            .arg("--progress-ms")
            .arg(progress_ms.to_string())
            .arg("--shared-duration-ms")
            .arg(pipeline.duration_ms.to_string())
            .arg("--params")
            .arg(&session.cohort_query)
            .arg(channel.config_path())
            .args(channel.overlay_paths())
            .spawn();

        match spawned {
            Ok(mut child) => {
                let item_id = pipeline.item_id.clone();
                tokio::spawn(async move {
                    let status = child.wait().await;
                    log::debug!("variant worker for item {item_id} exited: {status:?}");
                });
            }
            Err(e) => log::error!("failed to spawn variant worker: {e}"),
        }
    }
}

async fn read_sidecar(playlist_path: &Path) -> Option<PlaylistSidecar> {
    let path = PathBuf::from(format!(
        "{}{SIDECAR_SUFFIX}",
        playlist_path.to_string_lossy()
    ));
    let json = tokio::fs::read_to_string(&path).await.ok()?;
    serde_json::from_str(&json).ok()
}

async fn touch_heartbeat(folder: &Path) {
    if tokio::fs::create_dir_all(folder).await.is_ok() {
        let heartbeat = folder.join(HEARTBEAT_FILE_NAME);
        if !heartbeat.exists() {
            let _ = tokio::fs::write(&heartbeat, b"").await;
        }
        let _ = filetime::set_file_mtime(&heartbeat, filetime::FileTime::now());
    }
}

fn reap_idle(sessions: &mut HashMap<String, VariantSession>) {
    sessions.retain(|_, session| {
        let keep = session.last_access.elapsed().as_secs() < SESSION_IDLE_SECONDS;
        if !keep {
            log::info!(
                "reaping idle variant session for cohort '{}' on channel {}",
                session.cohort_query,
                session.channel_number
            );
            let folder = session.folder.clone();
            tokio::spawn(async move {
                // the worker exits on heartbeat staleness; removing the folder
                // afterwards is best-effort cleanup
                tokio::time::sleep(std::time::Duration::from_secs(120)).await;
                let _ = tokio::fs::remove_dir_all(&folder).await;
            });
        }
        keep
    });
}

/// A short, deterministic, filesystem-safe name for a cohort (fnv-1a of its
/// canonical query string).
fn cohort_folder_name(cohort_query: &str) -> String {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = FNV_OFFSET;
    for byte in cohort_query.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cohort_folder_names_are_deterministic_and_distinct() {
        assert_eq!(
            cohort_folder_name("region=west"),
            cohort_folder_name("region=west")
        );
        assert_ne!(
            cohort_folder_name("region=west"),
            cohort_folder_name("region=east")
        );
        assert_eq!(cohort_folder_name("region=west").len(), 16);
    }
}
