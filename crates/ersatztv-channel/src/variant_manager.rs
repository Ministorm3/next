//! Variant session lifecycle, owned by the channel worker.
//!
//! A variant session exists per (channel, cohort): it owns the cohort's
//! composed-playlist state, spawns variant workers for templated items as the
//! shared session's sidecar reveals them, and renders the cohort's playlists
//! to disk for whichever server is serving this channel. Identical cohorts
//! share one session and one transcode.
//!
//! Nothing here is driven by a request. The worker ticks, reads the cohort
//! requests published in its output folder, and answers them with files, so a
//! server in another process (and another language) only has to publish a
//! query and read a playlist off disk.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use ersatztv_core::HEARTBEAT_FILE_NAME;
use ersatztv_core::cohort;
use ersatztv_core::sidecar::{PlaylistSidecar, SIDECAR_SUFFIX, SidecarPipeline};
use ersatztv_core::variant_request::{
    VARIANTS_FOLDER, answers_folder, composed_playlist_name, requests_folder, stable_name,
};
use time::OffsetDateTime;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::composer::{SEGMENT_SECONDS, SessionPlaylist};

/// How many cohorts one channel transcodes variants for at once. Cohorts
/// beyond this are answered with shared content rather than queued.
const MAX_SESSIONS_PER_CHANNEL: usize = 4;

/// A cohort is dropped once no viewer has republished its request this
/// recently. Requests are republished on every playlist reload, so this is the
/// same liveness signal as a heartbeat.
const SESSION_IDLE_SECONDS: u64 = 60;

/// How long a reaped cohort's folder survives. Its variant worker exits on its
/// own heartbeat going stale, so the folder has to outlive that.
const FOLDER_CLEANUP_SECONDS: u64 = 120;

/// How often the worker answers requests and re-renders composed playlists.
///
/// Comfortably inside a segment duration, so a reloading client never waits on
/// it, without sampling the composer's monotonic media-sequence clamp more
/// often than the content can actually change.
pub const TICK_INTERVAL: Duration = Duration::from_secs(2);

/// The channel a variant session belongs to: where its shared session writes,
/// and how to launch a variant worker for it.
pub struct VariantChannel {
    /// Channel number, passed to the variant worker as `--number`.
    pub number: String,
    /// The shared session's output folder. Variants transcode into
    /// `variants/{cohort}` beneath it, so they are served by the same static
    /// file handler that serves the shared session.
    pub output_folder: PathBuf,
    /// The `ersatztv-channel` binary that transcodes a variant, normally this
    /// process's own executable.
    pub channel_binary: PathBuf,
    /// The merged channel configuration, piped to each variant worker on
    /// stdin. Passing json rather than paths is what lets a variant be
    /// configured identically whether this worker was given config files or
    /// handed its own configuration on stdin.
    pub config_json: String,
}

#[derive(Default)]
pub struct VariantManager {
    sessions: Mutex<HashMap<String, VariantSession>>,
}

struct VariantSession {
    cohort_query: String,
    folder: PathBuf,
    variant_prefix: String,
    playlist: SessionPlaylist,
    subtitle_playlist: SessionPlaylist,
    spawned_items: HashSet<String>,
}

/// A cohort request found in the output folder: the token naming it, and the
/// cohort its raw query resolves to (empty when it identifies no cohort).
struct ResolvedRequest {
    token: String,
    cohort_query: String,
}

impl VariantManager {
    pub fn new() -> VariantManager {
        VariantManager::default()
    }

    /// Answers the cohort requests currently published for this channel and
    /// re-renders every live cohort's playlists.
    ///
    /// Every step degrades to shared content rather than failing: an
    /// unanswered request, an empty answer, and a missing playlist all leave
    /// the requester serving the shared session.
    pub async fn tick(&self, channel: &VariantChannel) {
        let recognized = cohort::read_recognized_params(&channel.output_folder).await;
        let requests = read_requests(channel, &recognized).await;
        let shared = read_sidecar(&channel.output_folder.join("live.m3u8"), "shared").await;

        let mut sessions = self.sessions.lock().await;

        let admitted = admit(&requests, &sessions, shared.is_some(), &channel.number);
        answer_requests(channel, &requests, &admitted).await;

        let requested: BTreeSet<String> = requests
            .iter()
            .filter(|r| !r.cohort_query.is_empty())
            .map(|r| r.cohort_query.clone())
            .collect();
        reap(
            &mut sessions,
            &admitted,
            &requested,
            shared.is_some(),
            channel,
        )
        .await;

        let Some(shared) = shared else {
            return;
        };

        for cohort_query in admitted {
            let session = sessions
                .entry(cohort_query.clone())
                .or_insert_with(|| VariantSession::new(&cohort_query, channel));

            touch_heartbeat(&session.folder).await;
            spawn_missing_variants(session, channel, &shared).await;
            session.render(channel, &shared).await;
        }
    }
}

impl VariantSession {
    fn new(cohort_query: &str, channel: &VariantChannel) -> VariantSession {
        let folder_name = stable_name(cohort_query);

        VariantSession {
            cohort_query: cohort_query.to_owned(),
            folder: channel
                .output_folder
                .join(VARIANTS_FOLDER)
                .join(&folder_name),
            variant_prefix: format!("{VARIANTS_FOLDER}/{folder_name}/"),
            // only the media playlist carries the logging label; the subtitle
            // playlist reruns the same decisions and would double every line
            playlist: SessionPlaylist::with_label(format!(
                "ch {} cohort '{}'",
                channel.number, cohort_query
            )),
            subtitle_playlist: SessionPlaylist::default(),
            spawned_items: HashSet::new(),
        }
    }

    /// Advances this cohort's playlists and publishes them beside the shared
    /// playlist. Rendering is driven by the clock rather than by a request, so
    /// the composed playlist a viewer reads is at most one tick old.
    async fn render(&mut self, channel: &VariantChannel, shared: &PlaylistSidecar) {
        let variant = read_sidecar(&self.folder.join("live.m3u8"), "variant").await;
        let shared_head = read_media_sequence(&channel.output_folder.join("live.m3u8")).await;
        let now = OffsetDateTime::now_utc();
        let cohort = stable_name(&self.cohort_query);

        let media = self.playlist.advance_and_render(
            shared,
            variant.as_ref(),
            &self.variant_prefix,
            shared_head,
            now,
            SEGMENT_SECONDS as u32,
            |s| s.to_owned(),
        );

        let subtitles = self.subtitle_playlist.advance_and_render(
            shared,
            variant.as_ref(),
            &self.variant_prefix,
            shared_head,
            now,
            SEGMENT_SECONDS as u32,
            |s| format!("{}.vtt", s.strip_suffix(".ts").unwrap_or(s)),
        );

        write_atomic(
            &channel
                .output_folder
                .join(composed_playlist_name(&cohort, false)),
            &media,
        )
        .await;

        write_atomic(
            &channel
                .output_folder
                .join(composed_playlist_name(&cohort, true)),
            &subtitles,
        )
        .await;
    }
}

/// Reads every published cohort request, dropping the ones no viewer has
/// refreshed recently, and resolves each to a cohort. Only the worker can do
/// this: recognizing a parameter depends on the playout it is currently
/// running.
async fn read_requests(
    channel: &VariantChannel,
    recognized: &BTreeSet<String>,
) -> Vec<ResolvedRequest> {
    let folder = requests_folder(&channel.output_folder);
    let mut entries = match tokio::fs::read_dir(&folder).await {
        Ok(entries) => entries,
        // absence is the normal no-viewers case; anything else reads as
        // "no viewers" too, which reaps every session, so it has to say so
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Vec::new(),
        Err(e) => {
            log::warn!(
                "cannot scan the cohort requests folder {}: {e}; treating channel {} \
                 as having no cohort viewers this tick",
                folder.display(),
                channel.number
            );
            return Vec::new();
        }
    };

    let mut requests = Vec::new();

    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();

        if is_stale(&path).await {
            let _ = tokio::fs::remove_file(&path).await;
            if let Some(name) = path.file_name() {
                let _ =
                    tokio::fs::remove_file(answers_folder(&channel.output_folder).join(name)).await;
            }
            continue;
        }

        let (Ok(raw_query), Some(token)) = (
            tokio::fs::read_to_string(&path).await,
            path.file_name().map(|n| n.to_string_lossy().into_owned()),
        ) else {
            continue;
        };

        let query_pairs: Vec<(String, String)> = url::form_urlencoded::parse(raw_query.as_bytes())
            .into_owned()
            .collect();

        let parameters = cohort::cohort_parameters(&query_pairs, recognized);

        requests.push(ResolvedRequest {
            token,
            cohort_query: cohort::to_query_string(&parameters),
        });
    }

    requests
}

/// Chooses which cohorts get a variant transcode. Cohorts already running keep
/// their session, so a channel at its cap stays stable instead of trading
/// sessions between cohorts every tick.
fn admit(
    requests: &[ResolvedRequest],
    sessions: &HashMap<String, VariantSession>,
    have_shared: bool,
    number: &str,
) -> BTreeSet<String> {
    let mut admitted = BTreeSet::new();

    if !have_shared {
        return admitted;
    }

    let wanted: BTreeSet<&str> = requests
        .iter()
        .filter(|r| !r.cohort_query.is_empty())
        .map(|r| r.cohort_query.as_str())
        .collect();

    for cohort_query in wanted.iter().filter(|q| sessions.contains_key(**q)) {
        admitted.insert((*cohort_query).to_owned());
    }

    for cohort_query in wanted {
        if admitted.contains(cohort_query) {
            continue;
        }

        if admitted.len() >= MAX_SESSIONS_PER_CHANNEL {
            log::warn!(
                "variant session cap reached; serving shared content to cohort '{cohort_query}' on channel {number}"
            );
            continue;
        }

        admitted.insert(cohort_query.to_owned());
    }

    admitted
}

/// Publishes the cohort folder each request resolved to. A request that
/// identifies no cohort, or whose cohort was not admitted, is answered with an
/// empty file, which tells the requester to serve shared content.
async fn answer_requests(
    channel: &VariantChannel,
    requests: &[ResolvedRequest],
    admitted: &BTreeSet<String>,
) {
    if requests.is_empty() {
        return;
    }

    let folder = answers_folder(&channel.output_folder);
    if let Err(e) = tokio::fs::create_dir_all(&folder).await {
        log::warn!(
            "cannot create the answers folder {}: {e}; every cohort request on \
             channel {} goes unanswered and serves shared",
            folder.display(),
            channel.number
        );
        return;
    }

    for request in requests {
        let answer = if admitted.contains(&request.cohort_query) {
            stable_name(&request.cohort_query)
        } else {
            String::new()
        };

        let path = folder.join(&request.token);

        // rewriting an unchanged answer every tick would churn the folder for
        // no one's benefit
        if let Ok(existing) = tokio::fs::read_to_string(&path).await
            && existing == answer
        {
            continue;
        }

        if let Err(e) = tokio::fs::write(&path, &answer).await {
            log::warn!(
                "cannot write the cohort answer {}: {e}; the requester falls back \
                 to shared",
                path.display()
            );
        }
    }
}

/// Drops sessions whose cohort is no longer admitted, removing the playlists
/// immediately and the transcode folder once its worker has had time to exit.
/// `requested` and `have_shared` exist to name the reason: "idle" used to
/// cover all three causes, and a shared-sidecar read failure reaping every
/// session read as an audience walking away.
async fn reap(
    sessions: &mut HashMap<String, VariantSession>,
    admitted: &BTreeSet<String>,
    requested: &BTreeSet<String>,
    have_shared: bool,
    channel: &VariantChannel,
) {
    let dropped: Vec<String> = sessions
        .keys()
        .filter(|q| !admitted.contains(*q))
        .cloned()
        .collect();

    for cohort_query in dropped {
        let Some(session) = sessions.remove(&cohort_query) else {
            continue;
        };

        let reason = if !have_shared {
            "the shared sidecar is unavailable"
        } else if !requested.contains(&cohort_query) {
            "no fresh viewer request"
        } else {
            "not admitted at the session cap"
        };
        log::info!(
            "reaping variant session for cohort '{}' on channel {} ({reason})",
            session.cohort_query,
            channel.number
        );

        let cohort = stable_name(&session.cohort_query);
        for subtitles in [false, true] {
            let _ = tokio::fs::remove_file(
                channel
                    .output_folder
                    .join(composed_playlist_name(&cohort, subtitles)),
            )
            .await;
        }

        let folder = session.folder.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(FOLDER_CLEANUP_SECONDS)).await;
            let _ = tokio::fs::remove_dir_all(&folder).await;
        });
    }
}

/// Where the variant's coverage of the shared envelope begins: at the end of
/// the shared session's PUBLISHED coverage of the item, not at the wall
/// clock, because a channel running behind schedule would otherwise anchor
/// the variant deep into the envelope and never line up with the shared
/// grid. That equivalence only holds while the shared source is live and
/// publishes in step with air time; a fallback (slate) window is produced
/// ahead of air, so its coverage says nothing about where viewers are, and
/// the variant must cover the whole declared envelope from its start. The
/// composer's first-unserved clamp keeps already-served slate positions from
/// being re-served either way.
fn variant_progress_ms(pipeline: &SidecarPipeline, shared: &PlaylistSidecar) -> u64 {
    if pipeline.fallback {
        return 0;
    }
    shared
        .segments
        .iter()
        .filter(|s| s.item_id == pipeline.item_id)
        .map(|s| (s.duration.max(0f64) * 1000.0) as u64)
        .sum()
}

/// Spawns a variant worker for each templated item in the shared sidecar this
/// session has not spawned yet. The worker receives the shared item's
/// pts offset so both transcodes occupy the same envelope.
async fn spawn_missing_variants(
    session: &mut VariantSession,
    channel: &VariantChannel,
    shared: &PlaylistSidecar,
) {
    for pipeline in shared.pipelines.iter().filter(|p| p.templated) {
        if session.spawned_items.contains(&pipeline.item_id) {
            continue;
        }
        session.spawned_items.insert(pipeline.item_id.clone());

        if let Err(e) = tokio::fs::create_dir_all(&session.folder).await {
            log::error!(
                "cannot create the variant folder {} for item {}: {e}; this window \
                 will never get a variant (spawns are not retried)",
                session.folder.display(),
                pipeline.item_id
            );
            continue;
        }

        let progress_ms = variant_progress_ms(pipeline, shared);

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
            channel.number,
            session.cohort_query,
            progress_ms,
            pipeline.duration_ms
        );

        let spawned = tokio::process::Command::new(&channel.channel_binary)
            .arg("variant")
            .arg("--output-folder")
            .arg(&session.folder)
            .arg("--number")
            .arg(&channel.number)
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
            .arg("-")
            .stdin(std::process::Stdio::piped())
            // a shared worker that dies takes its variant workers with it,
            // instead of leaving them writing cohort folders nobody owns
            .kill_on_drop(true)
            .spawn();

        match spawned {
            Ok(mut child) => {
                let item_id = pipeline.item_id.clone();
                let cohort_query = session.cohort_query.clone();
                let number = channel.number.clone();
                let config_json = channel.config_json.clone();
                let stdin = child.stdin.take();

                tokio::spawn(async move {
                    // the child blocks reading its configuration until stdin
                    // closes, so the handle has to be dropped, not just written
                    if let Some(mut stdin) = stdin {
                        let piped = async {
                            stdin.write_all(config_json.as_bytes()).await?;
                            stdin.shutdown().await
                        }
                        .await;
                        if let Err(e) = piped {
                            log::warn!(
                                "cannot pipe the channel config to the variant worker \
                                 for item {item_id}: {e}; the worker will fail its \
                                 config parse and exit"
                            );
                        }
                    }

                    match child.wait().await {
                        Ok(status) if status.success() => {
                            log::debug!("variant worker for item {item_id} exited: {status:?}");
                        }
                        // there is no respawn: a failed variant means the
                        // composer pins shared at the decision deadline, and
                        // this line is the only durable trace of why
                        Ok(status) => log::warn!(
                            "variant worker for item {item_id} (cohort '{cohort_query}', \
                             channel {number}) exited {status}; no respawn, the cohort \
                             serves shared for this window"
                        ),
                        Err(e) => log::warn!(
                            "cannot reap the variant worker for item {item_id} (cohort \
                             '{cohort_query}', channel {number}): {e}"
                        ),
                    }
                });
            }
            Err(e) => log::error!(
                "cannot spawn the variant worker {} for item {}: {e}; this window \
                 will never get a variant (spawns are not retried)",
                channel.channel_binary.display(),
                pipeline.item_id
            ),
        }
    }
}

async fn is_stale(path: &Path) -> bool {
    let Ok(metadata) = tokio::fs::metadata(path).await else {
        return false;
    };

    metadata
        .modified()
        .ok()
        .and_then(|m| m.elapsed().ok())
        .is_some_and(|age| age.as_secs() >= SESSION_IDLE_SECONDS)
}

/// Publishes a playlist by rename, so a reader never observes a half-written
/// one.
async fn write_atomic(path: &Path, contents: &str) {
    let temporary = path.with_extension("tmp");

    // a playlist that stops being republished goes stale against the
    // server's freshness gate and every viewer silently falls back to
    // shared, so neither failure may pass without naming the playlist
    if let Err(e) = tokio::fs::write(&temporary, contents).await {
        log::warn!(
            "cannot write the composed playlist temp for {}: {e}",
            path.display()
        );
        return;
    }

    if let Err(e) = tokio::fs::rename(&temporary, path).await {
        log::warn!(
            "cannot publish the composed playlist {}: {e}",
            path.display()
        );
        let _ = tokio::fs::remove_file(&temporary).await;
    }
}

/// `context` names which sidecar this is ("shared" or "variant") in failure
/// logs: the two have opposite blast radii (an unreadable shared sidecar
/// reaps every cohort on the channel; an unreadable variant sidecar pins one
/// cohort to shared), and the old silent `.ok()?` chain hid both.
async fn read_sidecar(playlist_path: &Path, context: &str) -> Option<PlaylistSidecar> {
    let path = PathBuf::from(format!(
        "{}{SIDECAR_SUFFIX}",
        playlist_path.to_string_lossy()
    ));
    let json = match tokio::fs::read_to_string(&path).await {
        Ok(json) => json,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(e) => {
            log::warn!("cannot read the {context} sidecar {}: {e}", path.display());
            return None;
        }
    };
    match serde_json::from_str(&json) {
        Ok(sidecar) => Some(sidecar),
        Err(e) => {
            log::warn!("cannot parse the {context} sidecar {}: {e}", path.display());
            None
        }
    }
}

/// The media sequence the shared playlist currently serves from. The composed
/// playlist mirrors it whenever it can, so a client moved between the two
/// lands on the same numbering.
async fn read_media_sequence(playlist_path: &Path) -> Option<u64> {
    let playlist = match tokio::fs::read_to_string(playlist_path).await {
        Ok(playlist) => playlist,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(e) => {
            // a None here zeroes the composed lag measurement, silently
            // disabling every lag trim and the cannot-advance warning
            log::warn!(
                "cannot read the shared playlist {} for its media sequence: {e}; \
                 lag governance is disabled this tick",
                playlist_path.display()
            );
            return None;
        }
    };
    playlist
        .lines()
        .find_map(|l| l.strip_prefix("#EXT-X-MEDIA-SEQUENCE:"))
        .and_then(|v| v.trim().parse().ok())
}

async fn touch_heartbeat(folder: &Path) {
    if let Err(e) = tokio::fs::create_dir_all(folder).await {
        log::warn!(
            "cannot create the session folder {} to refresh its heartbeat: {e}; \
             the session may idle out despite viewers",
            folder.display()
        );
        return;
    }
    let heartbeat = folder.join(HEARTBEAT_FILE_NAME);
    if !heartbeat.exists()
        && let Err(e) = tokio::fs::write(&heartbeat, b"").await
    {
        log::warn!(
            "cannot create the heartbeat {}: {e}; the session may idle out \
             despite viewers",
            heartbeat.display()
        );
        return;
    }
    if let Err(e) = filetime::set_file_mtime(&heartbeat, filetime::FileTime::now()) {
        log::warn!(
            "cannot refresh the heartbeat {}: {e}; the session may idle out \
             despite viewers",
            heartbeat.display()
        );
    }
}

#[cfg(test)]
mod tests {
    use ersatztv_core::sidecar::{SidecarPipeline, SidecarSegment};

    use super::*;

    fn request(cohort_query: &str) -> ResolvedRequest {
        ResolvedRequest {
            token: stable_name(cohort_query),
            cohort_query: cohort_query.to_owned(),
        }
    }

    fn channel(folder: &Path) -> VariantChannel {
        VariantChannel {
            number: String::from("5"),
            output_folder: folder.to_path_buf(),
            channel_binary: PathBuf::from("ersatztv-channel"),
            config_json: String::from("{}"),
        }
    }

    #[test]
    fn a_request_identifying_no_cohort_is_not_admitted() {
        let requests = vec![request("")];
        let admitted = admit(&requests, &HashMap::new(), true, "5");
        assert!(admitted.is_empty());
    }

    #[test]
    fn nothing_is_admitted_before_the_shared_session_has_a_sidecar() {
        let requests = vec![request("zip=15216")];
        let admitted = admit(&requests, &HashMap::new(), false, "5");
        assert!(admitted.is_empty());
    }

    #[test]
    fn identical_cohorts_share_one_session() {
        let requests = vec![request("zip=15216"), request("zip=15216")];
        let admitted = admit(&requests, &HashMap::new(), true, "5");
        assert_eq!(admitted.len(), 1);
    }

    #[test]
    fn admission_stops_at_the_channel_cap() {
        let requests: Vec<ResolvedRequest> = (0..MAX_SESSIONS_PER_CHANNEL + 3)
            .map(|i| request(&format!("zip=1000{i}")))
            .collect();

        let admitted = admit(&requests, &HashMap::new(), true, "5");
        assert_eq!(admitted.len(), MAX_SESSIONS_PER_CHANNEL);
    }

    /// A channel at its cap must not trade sessions between cohorts each tick;
    /// a running cohort keeps its transcode until it goes idle.
    #[test]
    fn running_sessions_keep_their_place_at_the_cap() {
        let folder = tempfile::tempdir().unwrap();
        let channel = channel(folder.path());

        let running = "zip=99999";
        let mut sessions = HashMap::new();
        sessions.insert(
            String::from(running),
            VariantSession::new(running, &channel),
        );

        let mut requests: Vec<ResolvedRequest> = (0..MAX_SESSIONS_PER_CHANNEL)
            .map(|i| request(&format!("zip=1000{i}")))
            .collect();
        requests.push(request(running));

        let admitted = admit(&requests, &sessions, true, "5");

        assert_eq!(admitted.len(), MAX_SESSIONS_PER_CHANNEL);
        assert!(admitted.contains(running));
    }

    #[tokio::test]
    async fn answers_name_the_cohort_folder_and_blank_the_rest() {
        let folder = tempfile::tempdir().unwrap();
        let channel = channel(folder.path());

        let requests = vec![request("zip=15216"), request("")];
        let admitted = admit(&requests, &HashMap::new(), true, "5");
        answer_requests(&channel, &requests, &admitted).await;

        let answers = answers_folder(folder.path());
        let admitted_answer = tokio::fs::read_to_string(answers.join(stable_name("zip=15216")))
            .await
            .unwrap();
        let blank_answer = tokio::fs::read_to_string(answers.join(stable_name("")))
            .await
            .unwrap();

        assert_eq!(admitted_answer, stable_name("zip=15216"));
        assert_eq!(blank_answer, "");
    }

    #[tokio::test]
    async fn reaping_removes_the_cohorts_playlists() {
        let folder = tempfile::tempdir().unwrap();
        let channel = channel(folder.path());

        let cohort_query = "zip=15216";
        let cohort = stable_name(cohort_query);
        let playlist = folder.path().join(composed_playlist_name(&cohort, false));
        tokio::fs::write(&playlist, "#EXTM3U").await.unwrap();

        let mut sessions = HashMap::new();
        sessions.insert(
            String::from(cohort_query),
            VariantSession::new(cohort_query, &channel),
        );

        reap(
            &mut sessions,
            &BTreeSet::new(),
            &BTreeSet::new(),
            true,
            &channel,
        )
        .await;

        assert!(sessions.is_empty());
        assert!(!playlist.exists());
    }

    #[tokio::test]
    async fn a_composed_playlist_is_published_whole() {
        let folder = tempfile::tempdir().unwrap();
        let path = folder.path().join("live.abc.m3u8");

        write_atomic(&path, "#EXTM3U\n#EXT-X-VERSION:6\n").await;

        assert_eq!(
            tokio::fs::read_to_string(&path).await.unwrap(),
            "#EXTM3U\n#EXT-X-VERSION:6\n"
        );
        assert!(!folder.path().join("live.abc.tmp").exists());
    }

    fn pdt(at: OffsetDateTime) -> String {
        let format = time::macros::format_description!(
            "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3][offset_hour sign:mandatory][offset_minute]"
        );
        at.format(format).unwrap()
    }

    /// A shared session that has published three segments of an item no
    /// variant can change, so composition passes them straight through.
    fn shared_sidecar(now: OffsetDateTime) -> PlaylistSidecar {
        let segments = (0..3i64)
            .map(|i| SidecarSegment {
                path: format!("live00000{i}.ts"),
                duration: 4.0,
                program_date_time: pdt(now - time::Duration::seconds(12 - i * 4)),
                item_id: String::from("show"),
                discontinuity: i == 0,
            })
            .collect();

        PlaylistSidecar {
            segments,
            pipelines: vec![SidecarPipeline {
                item_id: String::from("show"),
                pts_offset_ms: 0,
                duration_ms: 60_000,
                templated: false,
                fallback: false,
            }],
        }
    }

    #[test]
    fn a_live_window_anchors_the_variant_at_published_coverage() {
        let mut shared = shared_sidecar(OffsetDateTime::now_utc());
        shared.pipelines[0].templated = true;

        assert_eq!(variant_progress_ms(&shared.pipelines[0], &shared), 12_000);
    }

    #[test]
    fn a_slate_window_anchors_the_variant_at_the_envelope_start() {
        // slate is produced ahead of air, so published coverage says nothing
        // about where viewers are. anchored at coverage, the variant would
        // join 12s in (or, fully published, never spawn at all) and the
        // cohort would lose the head of every star window
        let mut shared = shared_sidecar(OffsetDateTime::now_utc());
        shared.pipelines[0].templated = true;
        shared.pipelines[0].fallback = true;

        assert_eq!(variant_progress_ms(&shared.pipelines[0], &shared), 0);
    }

    async fn write_shared_session(output: &Path, recognized: &str) {
        tokio::fs::write(
            output.join(format!("live.m3u8{SIDECAR_SUFFIX}")),
            serde_json::to_string(&shared_sidecar(OffsetDateTime::now_utc())).unwrap(),
        )
        .await
        .unwrap();

        tokio::fs::write(
            output.join(ersatztv_core::RECOGNIZED_PARAMS_FILE_NAME),
            recognized,
        )
        .await
        .unwrap();
    }

    /// The whole protocol in one turn of the loop: a viewer publishes a query,
    /// and the worker answers it with a cohort whose playlist is on disk and
    /// ready to serve.
    #[tokio::test]
    async fn a_published_request_is_answered_with_a_composed_playlist() {
        let folder = tempfile::tempdir().unwrap();
        let output = folder.path();
        write_shared_session(output, r#"["zip"]"#).await;

        ersatztv_core::variant_request::publish_request(output, "zip=15216")
            .await
            .unwrap();

        VariantManager::new().tick(&channel(output)).await;

        let cohort = ersatztv_core::variant_request::read_answer(output, "zip=15216").await;
        assert_eq!(cohort, Some(stable_name("zip=15216")));

        let playlist =
            tokio::fs::read_to_string(output.join(composed_playlist_name(&cohort.unwrap(), false)))
                .await
                .unwrap();

        assert!(playlist.starts_with("#EXTM3U"));
        assert!(playlist.contains("live000000.ts"));
    }

    /// A parameter the playout never references must not mint a cohort, or
    /// every player cache buster would start a transcode.
    #[tokio::test]
    async fn a_query_the_playout_ignores_is_answered_with_shared() {
        let folder = tempfile::tempdir().unwrap();
        let output = folder.path();
        write_shared_session(output, r#"["zip"]"#).await;

        ersatztv_core::variant_request::publish_request(output, "cachebust=12345")
            .await
            .unwrap();

        VariantManager::new().tick(&channel(output)).await;

        assert_eq!(
            ersatztv_core::variant_request::read_answer(output, "cachebust=12345").await,
            None
        );

        let answer =
            tokio::fs::read_to_string(answers_folder(output).join(stable_name("cachebust=12345")))
                .await
                .unwrap();
        assert_eq!(answer, "");
    }

    /// Composition used to advance once per playlist request and now advances
    /// once per tick, several times more often. Ticking without new shared
    /// output must leave the served playlist exactly as it was, or a client
    /// polling across two ticks would see its media sequence move under it.
    #[tokio::test]
    async fn ticking_again_without_new_output_republishes_the_same_playlist() {
        let folder = tempfile::tempdir().unwrap();
        let output = folder.path();
        write_shared_session(output, r#"["zip"]"#).await;

        ersatztv_core::variant_request::publish_request(output, "zip=15216")
            .await
            .unwrap();

        let manager = VariantManager::new();
        let channel = channel(output);
        let playlist_path = output.join(composed_playlist_name(&stable_name("zip=15216"), false));

        manager.tick(&channel).await;
        let first = tokio::fs::read_to_string(&playlist_path).await.unwrap();

        manager.tick(&channel).await;
        let second = tokio::fs::read_to_string(&playlist_path).await.unwrap();

        assert_eq!(first, second);
    }

    /// Both playlists a client can ask for are published, since the master
    /// playlist points a cohort at its own subtitle rendition too.
    #[tokio::test]
    async fn subtitles_get_their_own_composed_playlist() {
        let folder = tempfile::tempdir().unwrap();
        let output = folder.path();
        write_shared_session(output, r#"["zip"]"#).await;

        ersatztv_core::variant_request::publish_request(output, "zip=15216")
            .await
            .unwrap();

        VariantManager::new().tick(&channel(output)).await;

        let cohort = stable_name("zip=15216");
        let subtitles =
            tokio::fs::read_to_string(output.join(composed_playlist_name(&cohort, true)))
                .await
                .unwrap();

        assert!(subtitles.contains("live000000.vtt"));
    }

    /// One log stream carries every channel's workers, so a cohort label that
    /// omits the channel is ambiguous the moment two channels share a query.
    #[test]
    fn a_cohort_label_names_its_channel() {
        let folder = tempfile::tempdir().unwrap();
        let session = VariantSession::new("zip=15216", &channel(folder.path()));

        assert_eq!(session.playlist.label(), "ch 5 cohort 'zip=15216'");
        // the subtitle half stays silent so decisions are reported once
        assert_eq!(session.subtitle_playlist.label(), "");
    }
}
