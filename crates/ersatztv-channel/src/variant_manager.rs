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

use crate::composer::{ComposedEntry, SEGMENT_SECONDS, SessionPlaylist, parse_pdt};
use crate::slate::{SlateFile, read_slate_file};

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
    /// Where this channel's slate side file lives, `None` when the playout
    /// folder has no parent to hold one. The manager reads only its
    /// `default` key; the shared session owns `path`.
    pub slate_file: Option<PathBuf>,
}

#[derive(Default)]
pub struct VariantManager {
    sessions: Mutex<HashMap<String, VariantSession>>,
    /// The slate default policy as it resolved last tick, kept so each named
    /// policy condition is logged once per change instead of every two
    /// seconds. The raw value participates in the comparison, so an edited
    /// file re-logs even when it resolves the same way, and a recognized set
    /// arriving after startup re-resolves without an operator edit.
    default_policy: Mutex<DefaultPolicy>,
}

/// What the slate file's `default` key resolved to against the currently
/// recognized parameters.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
enum DefaultPolicy {
    /// No slate file, or no `default` key: the pre-policy world, where a
    /// canonical-empty request is answered with shared content.
    #[default]
    Absent,
    /// The slate file did not parse. `path` is equally lost, so the shared
    /// session logs its own warning for that half.
    Malformed { error: String },
    /// The `default` value canonicalized to no cohort (garbage value, or a
    /// playout with no templated parameters): treated as no default.
    Unroutable { raw: String },
    /// The `default` value canonicalized to a cohort: canonical-empty
    /// requests are admitted to it. `without_path` marks a slate file that
    /// routes without slate media behind it, so the shared session keeps its
    /// wall-gated live-tune mechanics for templated windows.
    Routes {
        raw: String,
        cohort_query: String,
        without_path: bool,
    },
}

struct VariantSession {
    cohort_query: String,
    folder: PathBuf,
    variant_prefix: String,
    playlist: SessionPlaylist,
    subtitle_playlist: SessionPlaylist,
    spawned_items: HashSet<String>,
    /// References the served-window audit has already reported missing, so a
    /// gone file is one warning instead of one per tick. Pruned to the
    /// current window, so it cannot grow with the life of the session.
    audit_warned_missing: HashSet<String>,
    /// The deepest reach behind the wall clock among the substituted entries
    /// served so far in the current variant-content stretch, in ms. Logged
    /// and cleared when the window returns to all-shared content. This is
    /// the measured answer to how much retention a variant actually needs
    /// (see `VARIANT_HISTORY_DURATION` in the playlist manager).
    audit_reach_max_ms: Option<u64>,
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

        // the default admission policy is read from the slate side file for
        // now, but that home is provisional: it is expected to move into
        // schedule logic or remote stream configuration. admission must stay
        // decoupled from where the policy is read, so everything past this
        // point sees only the resolved cohort query, never the file
        let policy = resolve_default_policy(channel, &recognized).await;
        self.log_policy_change(&policy, channel).await;
        let default_cohort = match &policy {
            DefaultPolicy::Routes { cohort_query, .. } => Some(cohort_query.as_str()),
            _ => None,
        };

        let (requests, torn) = read_requests(channel, &recognized, default_cohort).await;
        let shared = read_sidecar(&channel.output_folder.join("live.m3u8"), "shared").await;

        // a live cohort request is a viewer of this channel. during a
        // substituted window that viewer fetches only composed playlists and
        // variant segments, none of which touch the shared session's files,
        // so without this the session idles out mid-window with an active
        // audience. requests here are already pruned of stale ones, so an
        // audience that leaves still lets the channel wind down
        if !requests.is_empty() {
            touch_heartbeat(&channel.output_folder).await;
        }

        let mut sessions = self.sessions.lock().await;

        let admitted = admit(&requests, &sessions, shared.is_some(), &channel.number);
        answer_requests(channel, &requests, &admitted).await;

        let requested: BTreeSet<String> = requests
            .iter()
            .filter(|r| !r.cohort_query.is_empty())
            .map(|r| r.cohort_query.clone())
            .collect();

        // Reaping decides from the ABSENCE of a request, so it may only run on
        // a complete view. A request caught mid-write is unreadable this tick
        // but says nothing about whether its viewer is still there, and
        // dropping a session on that evidence is the reap that kept firing
        // while a viewer polled every two seconds. Sessions that really have
        // gone away are still reaped on the next tick with an intact view.
        if torn {
            log::debug!(
                "channel {}: deferring the reap, a cohort request was read mid-write \
                 so this tick cannot tell an absent viewer from an unreadable one",
                channel.number
            );
        } else {
            reap(
                &mut sessions,
                &admitted,
                &requested,
                shared.is_some(),
                channel,
            )
            .await;
        }

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

    /// Names what the default policy resolved to, once per change. The
    /// resolution runs every tick, so logging unconditionally would repeat
    /// each condition thirty times a minute; logging only the first
    /// occurrence would hide a policy that resolves differently after a
    /// recognized set arrives or an operator edit.
    async fn log_policy_change(&self, policy: &DefaultPolicy, channel: &VariantChannel) {
        let mut last = self.default_policy.lock().await;
        if *last == *policy {
            return;
        }

        let file = channel
            .slate_file
            .as_deref()
            .map(|f| f.display().to_string())
            .unwrap_or_else(|| String::from("slate.json"));

        match policy {
            DefaultPolicy::Absent => log::info!(
                "no slate default on channel {} any more; canonical-empty viewers serve shared",
                channel.number
            ),
            DefaultPolicy::Malformed { error } => log::warn!(
                "ignoring {file}: {error}; canonical-empty viewers on channel {} \
                 serve shared",
                channel.number
            ),
            DefaultPolicy::Unroutable { raw } => log::warn!(
                "the slate default '{raw}' in {file} canonicalizes to no cohort; treating it \
                 as no default, canonical-empty viewers on channel {} serve shared",
                channel.number
            ),
            DefaultPolicy::Routes {
                raw,
                cohort_query,
                without_path,
            } => {
                if *without_path {
                    log::warn!(
                        "the slate default '{raw}' in {file} routes canonical-empty viewers on \
                         channel {} to cohort '{cohort_query}' with no slate path behind it; \
                         the shared session keeps live-tune mechanics for templated windows",
                        channel.number
                    );
                } else {
                    log::info!(
                        "the slate default '{raw}' in {file} routes canonical-empty viewers on \
                         channel {} to cohort '{cohort_query}'",
                        channel.number
                    );
                }
            }
        }

        *last = policy.clone();
    }
}

/// Resolves the slate file's `default` key to the cohort it names, through
/// the same canonicalization a real request goes through, exactly once. The
/// result is used as-is by the request loop, so a default can never be
/// substituted into itself, and 'Zip=15216' names the same cohort folder a
/// real 'zip=15216' viewer gets.
async fn resolve_default_policy(
    channel: &VariantChannel,
    recognized: &BTreeSet<String>,
) -> DefaultPolicy {
    let Some(file) = channel.slate_file.as_deref() else {
        return DefaultPolicy::Absent;
    };

    let config = match read_slate_file(file).await {
        SlateFile::Missing => return DefaultPolicy::Absent,
        SlateFile::Malformed(error) => return DefaultPolicy::Malformed { error },
        SlateFile::Present(config) => config,
    };

    let Some(raw) = config.default else {
        return DefaultPolicy::Absent;
    };

    let query_pairs: Vec<(String, String)> = url::form_urlencoded::parse(raw.as_bytes())
        .into_owned()
        .collect();
    let cohort_query =
        cohort::to_query_string(&cohort::cohort_parameters(&query_pairs, recognized));

    if cohort_query.is_empty() {
        DefaultPolicy::Unroutable { raw }
    } else {
        DefaultPolicy::Routes {
            raw,
            cohort_query,
            without_path: config.path.is_none(),
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
            audit_warned_missing: HashSet::new(),
            audit_reach_max_ms: None,
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

        self.audit_served_window(channel, variant.as_ref(), now)
            .await;
    }

    /// Audits what the media playlist just published against what actually
    /// backs it. Nothing else can see this failure: the server's static file
    /// handler serves a bare 404 without logging, and any monitor that reads
    /// the playlist sees a perfectly healthy window even while a file behind
    /// it is gone.
    ///
    /// Two measurements. Every referenced file must still exist on disk; a
    /// missing one is the trim-under-reference failure actually happening
    /// (or a twin that was never produced), warned once per file. And the
    /// deepest reach behind the wall clock into variant content, measured on
    /// the variant's own stamps because those are the clock its retention
    /// trim runs on; the maximum over a substituted stretch, logged when the
    /// stretch leaves the window, is the observed answer to how much
    /// retention a variant actually needs (see `VARIANT_HISTORY_DURATION`).
    /// Only the media window is audited: the subtitle playlist mirrors its
    /// decisions, and trims delete both files together.
    async fn audit_served_window(
        &mut self,
        channel: &VariantChannel,
        variant: Option<&PlaylistSidecar>,
        now: OffsetDateTime,
    ) {
        let entries: Vec<(String, u64, bool)> = self
            .playlist
            .served_window()
            .map(|e| (e.path.clone(), e.sequence, e.variant))
            .collect();

        let mut has_variant_entries = false;
        for (path, sequence, is_variant) in &entries {
            has_variant_entries |= is_variant;
            let file = channel.output_folder.join(path);
            let exists = tokio::fs::try_exists(&file).await.unwrap_or(false);
            if !exists && self.audit_warned_missing.insert(path.clone()) {
                log::warn!(
                    "[{}] composed window references {path} (position {sequence}, {}) but \
                     no file backs it on disk; every viewer of this cohort gets a 404 at \
                     this position",
                    self.playlist.label(),
                    if *is_variant {
                        "a variant twin"
                    } else {
                        "a shared segment"
                    },
                );
            }
        }
        self.audit_warned_missing
            .retain(|warned| entries.iter().any(|(path, ..)| path == warned));

        if has_variant_entries {
            let reach = variant.and_then(|v| {
                deepest_variant_reach_ms(
                    self.playlist.served_window(),
                    v,
                    &self.variant_prefix,
                    now,
                )
            });
            if let Some(reach) = reach {
                self.audit_reach_max_ms =
                    Some(self.audit_reach_max_ms.map_or(reach, |max| max.max(reach)));
            }
        } else if let Some(max) = self.audit_reach_max_ms.take() {
            log::info!(
                "[{}] substituted stretch left the served window; deepest composed reach \
                 into variant content was {max}ms behind the wall clock",
                self.playlist.label(),
            );
        }
    }
}

/// The deepest reach behind `now` among the served window's variant entries,
/// in ms. Measured on the variant's OWN segment stamps, never the composed
/// entry's: a substituted entry re-stamps the twin with the shared session's
/// program date time, while the variant's retention trim deletes files by the
/// stamps in its own playlist, so only the sidecar's stamp can say how close
/// a referenced file is to deletion. A twin the sidecar no longer lists
/// cannot be measured and is skipped; the existence check reports it if its
/// file is truly gone.
fn deepest_variant_reach_ms<'a>(
    window: impl Iterator<Item = &'a ComposedEntry>,
    variant: &PlaylistSidecar,
    variant_prefix: &str,
    now: OffsetDateTime,
) -> Option<u64> {
    window
        .filter(|entry| entry.variant)
        .filter_map(|entry| {
            let twin_path = entry.path.strip_prefix(variant_prefix)?;
            let stamp = variant
                .segments
                .iter()
                .find(|segment| segment.path == twin_path)?;
            let stamp = parse_pdt(&stamp.program_date_time)?;
            Some((now - stamp).whole_milliseconds().max(0) as u64)
        })
        .max()
}

/// Reads every published cohort request, dropping the ones no viewer has
/// refreshed recently, and resolves each to a cohort. Only the worker can do
/// this: recognizing a parameter depends on the playout it is currently
/// running.
///
/// `default_cohort` is the already-canonicalized cohort that canonical-empty
/// requests are admitted to, when the operator configured one.
///
/// Returns the resolved requests and whether the scan was TORN. A true second
/// element means the view is incomplete (a request was caught mid-write, or
/// the folder could not be scanned), so this tick cannot distinguish a viewer
/// who left from one whose request was momentarily unreadable, and the caller
/// must not reap on it.
async fn read_requests(
    channel: &VariantChannel,
    recognized: &BTreeSet<String>,
    default_cohort: Option<&str>,
) -> (Vec<ResolvedRequest>, bool) {
    let folder = requests_folder(&channel.output_folder);
    let mut entries = match tokio::fs::read_dir(&folder).await {
        Ok(entries) => entries,
        // absence is the normal no-viewers case; anything else reads as
        // "no viewers" too, which reaps every session, so it has to say so
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return (Vec::new(), false),
        Err(e) => {
            log::warn!(
                "cannot scan the cohort requests folder {}: {e}; treating channel {} \
                 as having no cohort viewers this tick",
                folder.display(),
                channel.number
            );
            return (Vec::new(), true);
        }
    };

    let mut requests = Vec::new();
    let mut torn = false;

    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();

        if let Some(age) = staleness(&path).await {
            // this drop is what a "no fresh viewer request" reap is made of,
            // so say it happened: a viewer that really left and a viewer
            // whose requests stopped being republished look identical
            // downstream, and only the age separates them
            log::info!(
                "dropping cohort request {} on channel {}: last republished {}s ago \
                 (idle limit {}s)",
                path.file_name().unwrap_or_default().to_string_lossy(),
                channel.number,
                age.as_secs(),
                SESSION_IDLE_SECONDS
            );
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

        // A request file's NAME is `stable_name` of its own contents, so a
        // read whose contents do not hash to its name caught the file
        // mid-write and must be ignored rather than canonicalized.
        //
        // A truncating writer (both this crate's `publish_request` and the
        // legacy app's `File.WriteAllTextAsync`) leaves the file present,
        // with a fresh modified time, and momentarily EMPTY. Reading that
        // yields an empty query, which canonicalizes to the default cohort,
        // so the cohort the viewer actually asked for is missing from this
        // tick's requests and its session is reaped as unwanted. The fresh
        // modified time means it is not reported as a stale drop either,
        // which is exactly how the reap presented: "no cohort was requested
        // this tick" while a viewer was polling every ~2s. Observed three
        // times over 2026-08-13/14, each time recovering on the next tick.
        //
        // This check cannot swallow a genuine bare query: that request is
        // named `stable_name("")` and its contents really are empty, so the
        // two agree and it is admitted exactly as before.
        if ersatztv_core::variant_request::stable_name(&raw_query) != token {
            log::debug!(
                "ignoring a torn cohort request {} on channel {}: contents do not \
                 match the name, so the file was read mid-write",
                token,
                channel.number
            );
            torn = true;
            continue;
        };

        let query_pairs: Vec<(String, String)> = url::form_urlencoded::parse(raw_query.as_bytes())
            .into_owned()
            .collect();

        let parameters = cohort::cohort_parameters(&query_pairs, recognized);
        let mut cohort_query = cohort::to_query_string(&parameters);

        // canonical-empty requests (a bare query, and a query whose
        // parameters are all unrecognized, which canonicalize identically)
        // are admitted to the default cohort here, at resolution, so
        // admission, answers, reaping, and request-file keep-alive all see
        // the substituted requester exactly as they see a real one. a
        // request that resolves to any cohort of its own is never touched.
        // the default arrives already canonicalized and is used as-is, so
        // substitution can never recurse
        if cohort_query.is_empty()
            && let Some(default_cohort) = default_cohort
        {
            cohort_query = default_cohort.to_owned();
        }

        requests.push(ResolvedRequest {
            token,
            cohort_query,
        });
    }

    (requests, torn)
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
            String::from("the shared sidecar is unavailable")
        } else if !requested.contains(&cohort_query) {
            // name what WAS requested this tick. A reap while viewers are
            // demonstrably still polling has been observed, and the three
            // ways it can happen are indistinguishable without this: no
            // requests at all (the folder went empty), requests that
            // resolved to some other cohort (recognition changed under us),
            // or this cohort's request having just been dropped as stale
            let others: Vec<&str> = requested.iter().map(String::as_str).collect();
            if others.is_empty() {
                String::from("no fresh viewer request; no cohort was requested this tick")
            } else {
                format!(
                    "no fresh viewer request; {} other cohort(s) were requested this tick: {}",
                    others.len(),
                    others.join(", ")
                )
            }
        } else {
            String::from("not admitted at the session cap")
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

/// How long ago a request file was last republished, when that is long
/// enough to drop it. `None` means it is still fresh (or unreadable, which
/// is deliberately treated as fresh rather than reaping on a stat failure).
///
/// Returns the age rather than a bool so the caller can say how stale a
/// dropped request was. A silent drop here is indistinguishable in the log
/// from a viewer genuinely leaving, and it is the step that precedes a
/// "no fresh viewer request" reap.
async fn staleness(path: &Path) -> Option<Duration> {
    let metadata = tokio::fs::metadata(path).await.ok()?;
    let age = metadata.modified().ok()?.elapsed().ok()?;
    (age.as_secs() >= SESSION_IDLE_SECONDS).then_some(age)
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
    use crate::slate::SLATE_FILE_NAME;

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
            // pointed into the output folder so a test can drop a slate file
            // beside everything else it stages; no test writes one unless
            // slate policy is what it is testing
            slate_file: Some(folder.join(SLATE_FILE_NAME)),
        }
    }

    async fn write_slate(output: &Path, contents: &str) {
        tokio::fs::write(output.join(SLATE_FILE_NAME), contents)
            .await
            .unwrap();
    }

    /// The answer file's literal contents, empty meaning serve shared, read
    /// the way a requester would after `read_answer`'s trim.
    async fn answer_for(output: &Path, raw_query: &str) -> String {
        tokio::fs::read_to_string(answers_folder(output).join(stable_name(raw_query)))
            .await
            .unwrap()
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

    /// A request caught mid-write is ignored for that tick, not read as a
    /// different cohort.
    ///
    /// A truncating writer leaves the file present, freshly modified, and
    /// empty. Reading that yields an empty query, which canonicalizes to the
    /// default cohort, so the cohort the viewer asked for goes missing from
    /// the tick and its session is reaped while the viewer is still polling.
    /// Observed three times over 2026-08-13/14, each recovering a tick later.
    #[tokio::test]
    async fn a_request_caught_mid_write_does_not_reap_its_cohort() {
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
        assert!(
            playlist_path.exists(),
            "the cohort must be serving before the torn write"
        );

        // truncate the live request exactly as a non-atomic writer does: the
        // file stays, its modified time is fresh, its contents are gone
        let request = requests_folder(output).join(stable_name("zip=15216"));
        tokio::fs::write(&request, b"").await.unwrap();

        manager.tick(&channel).await;

        assert!(
            playlist_path.exists(),
            "a torn request read as an empty query reaped the cohort mid-write"
        );
    }

    /// The torn-write check must not swallow a genuine bare query, whose
    /// contents really are empty and whose name is `stable_name("")`. Those
    /// agree, so it is a valid request and keeps its session.
    #[tokio::test]
    async fn a_genuine_bare_request_is_still_admitted() {
        let folder = tempfile::tempdir().unwrap();
        let output = folder.path();
        write_shared_session(output, r#"["zip"]"#).await;

        ersatztv_core::variant_request::publish_request(output, "")
            .await
            .unwrap();

        let request = requests_folder(output).join(stable_name(""));
        assert_eq!(
            tokio::fs::read_to_string(&request).await.unwrap(),
            "",
            "a bare request's contents are genuinely empty"
        );

        let manager = VariantManager::new();
        manager.tick(&channel(output)).await;

        assert!(
            answers_folder(output).join(stable_name("")).exists(),
            "a genuine bare request must still be answered"
        );
    }

    /// A cohort viewer's polls never touch the shared session's own files
    /// during a substituted window, so their request file is the only proof
    /// the channel has an audience. The tick must convert that proof into
    /// the shared heartbeat, or the session idles out mid-window.
    #[tokio::test]
    async fn a_live_cohort_request_keeps_the_shared_heartbeat_fresh() {
        let folder = tempfile::tempdir().unwrap();
        let output = folder.path();
        write_shared_session(output, r#"["zip"]"#).await;

        let heartbeat = output.join(HEARTBEAT_FILE_NAME);
        tokio::fs::write(&heartbeat, b"").await.unwrap();
        let stale = filetime::FileTime::from_unix_time(1_000_000, 0);
        filetime::set_file_mtime(&heartbeat, stale).unwrap();

        ersatztv_core::variant_request::publish_request(output, "zip=15216")
            .await
            .unwrap();

        VariantManager::new().tick(&channel(output)).await;

        let after = filetime::FileTime::from_last_modification_time(
            &std::fs::metadata(&heartbeat).unwrap(),
        );
        assert!(after > stale, "tick must refresh the shared heartbeat");
    }

    /// With no live requests the tick leaves the heartbeat alone, so an
    /// audience that leaves still lets the channel wind down.
    #[tokio::test]
    async fn no_requests_leave_the_shared_heartbeat_stale() {
        let folder = tempfile::tempdir().unwrap();
        let output = folder.path();
        write_shared_session(output, r#"["zip"]"#).await;

        let heartbeat = output.join(HEARTBEAT_FILE_NAME);
        tokio::fs::write(&heartbeat, b"").await.unwrap();
        let stale = filetime::FileTime::from_unix_time(1_000_000, 0);
        filetime::set_file_mtime(&heartbeat, stale).unwrap();

        VariantManager::new().tick(&channel(output)).await;

        let after = filetime::FileTime::from_last_modification_time(
            &std::fs::metadata(&heartbeat).unwrap(),
        );
        assert_eq!(after, stale, "an idle channel must still time out");
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

    /// Pins the pre-policy world: with no slate file at all, a bare query
    /// identifies no cohort and is answered with shared.
    #[tokio::test]
    async fn without_a_slate_file_a_bare_query_is_answered_with_shared() {
        let folder = tempfile::tempdir().unwrap();
        let output = folder.path();
        write_shared_session(output, r#"["zip"]"#).await;

        ersatztv_core::variant_request::publish_request(output, "")
            .await
            .unwrap();

        VariantManager::new().tick(&channel(output)).await;

        assert_eq!(answer_for(output, "").await, "");
    }

    /// A slate file that only names media must not change routing: `path`
    /// alone is today's behavior, and a bare query stays on shared.
    #[tokio::test]
    async fn a_path_only_slate_answers_a_bare_query_with_shared() {
        let folder = tempfile::tempdir().unwrap();
        let output = folder.path();
        write_shared_session(output, r#"["zip"]"#).await;
        write_slate(output, r#"{"path": "/slate.mp4"}"#).await;

        ersatztv_core::variant_request::publish_request(output, "")
            .await
            .unwrap();

        VariantManager::new().tick(&channel(output)).await;

        assert_eq!(answer_for(output, "").await, "");
    }

    /// The operator's default admits a bare-query viewer to a real cohort:
    /// the answer sits on the empty query's pinned wire token and names the
    /// same folder a real zip=15216 viewer gets, with the cohort's composed
    /// playlist on disk ready to serve. The slate media path is dead on
    /// purpose, since routing must not depend on it existing.
    #[tokio::test]
    async fn a_slate_default_routes_a_bare_query_to_its_cohort() {
        let folder = tempfile::tempdir().unwrap();
        let output = folder.path();
        write_shared_session(output, r#"["zip"]"#).await;
        write_slate(
            output,
            r#"{"path": "/no/such/slate.mp4", "default": "zip=15216"}"#,
        )
        .await;

        ersatztv_core::variant_request::publish_request(output, "")
            .await
            .unwrap();

        VariantManager::new().tick(&channel(output)).await;

        let answer = tokio::fs::read_to_string(answers_folder(output).join("cbf29ce484222325"))
            .await
            .unwrap();
        assert_eq!(answer, stable_name("zip=15216"));

        let playlist = tokio::fs::read_to_string(
            output.join(composed_playlist_name(&stable_name("zip=15216"), false)),
        )
        .await
        .unwrap();
        assert!(playlist.starts_with("#EXTM3U"));
    }

    /// A routed bare-query viewer and a real viewer of the default's cohort
    /// share one session and one transcode: both answers name one folder, and
    /// only one cohort folder is minted, so the default occupies one cap slot
    /// rather than one per viewer class.
    #[tokio::test]
    async fn a_routed_bare_viewer_shares_the_real_viewers_session() {
        let folder = tempfile::tempdir().unwrap();
        let output = folder.path();
        write_shared_session(output, r#"["zip"]"#).await;
        write_slate(output, r#"{"path": "/slate.mp4", "default": "zip=15216"}"#).await;

        ersatztv_core::variant_request::publish_request(output, "")
            .await
            .unwrap();
        ersatztv_core::variant_request::publish_request(output, "zip=15216")
            .await
            .unwrap();

        VariantManager::new().tick(&channel(output)).await;

        assert_eq!(
            answer_for(output, "").await,
            answer_for(output, "zip=15216").await
        );

        let mut cohort_folders = 0;
        let mut entries = tokio::fs::read_dir(output.join(VARIANTS_FOLDER))
            .await
            .unwrap();
        while let Ok(Some(entry)) = entries.next_entry().await {
            if !entry.file_name().to_string_lossy().starts_with('.') {
                cohort_folders += 1;
            }
        }
        assert_eq!(cohort_folders, 1);
    }

    /// An access_token-only query canonicalizes to empty exactly like a bare
    /// one, so the default admits it too: the decision rides resolution, not
    /// raw-token comparison, and the raw token keeps its own answer file.
    #[tokio::test]
    async fn a_query_of_only_unrecognized_params_is_routed_by_the_default() {
        let folder = tempfile::tempdir().unwrap();
        let output = folder.path();
        write_shared_session(output, r#"["zip"]"#).await;
        write_slate(output, r#"{"path": "/slate.mp4", "default": "zip=15216"}"#).await;

        ersatztv_core::variant_request::publish_request(output, "access_token=eyJabc")
            .await
            .unwrap();

        VariantManager::new().tick(&channel(output)).await;

        assert_eq!(
            answer_for(output, "access_token=eyJabc").await,
            stable_name("zip=15216")
        );
    }

    /// A request that resolves to a cohort of its own is never touched by the
    /// default: the real query wins.
    #[tokio::test]
    async fn a_real_query_is_never_rerouted_by_the_default() {
        let folder = tempfile::tempdir().unwrap();
        let output = folder.path();
        write_shared_session(output, r#"["zip"]"#).await;
        write_slate(output, r#"{"path": "/slate.mp4", "default": "zip=15216"}"#).await;

        ersatztv_core::variant_request::publish_request(output, "zip=10001")
            .await
            .unwrap();

        VariantManager::new().tick(&channel(output)).await;

        assert_eq!(
            answer_for(output, "zip=10001").await,
            stable_name("zip=10001")
        );
    }

    /// Routing does not require slate media: a file holding only `default`
    /// still admits bare-query viewers. The named warning it draws is log
    /// state, not behavior.
    #[tokio::test]
    async fn a_default_without_a_path_still_routes() {
        let folder = tempfile::tempdir().unwrap();
        let output = folder.path();
        write_shared_session(output, r#"["zip"]"#).await;
        write_slate(output, r#"{"default": "zip=15216"}"#).await;

        ersatztv_core::variant_request::publish_request(output, "")
            .await
            .unwrap();

        VariantManager::new().tick(&channel(output)).await;

        assert_eq!(answer_for(output, "").await, stable_name("zip=15216"));
    }

    /// A slate file that does not parse must degrade to the pre-policy world
    /// without panicking: bare queries serve shared.
    #[tokio::test]
    async fn a_malformed_slate_file_answers_a_bare_query_with_shared() {
        let folder = tempfile::tempdir().unwrap();
        let output = folder.path();
        write_shared_session(output, r#"["zip"]"#).await;
        write_slate(output, "{not json").await;

        ersatztv_core::variant_request::publish_request(output, "")
            .await
            .unwrap();

        VariantManager::new().tick(&channel(output)).await;

        assert_eq!(answer_for(output, "").await, "");
    }

    /// A default the playout does not recognize canonicalizes to no cohort
    /// and is treated as no default. Substitution happens at most once, so an
    /// unroutable default can never loop back into itself.
    #[tokio::test]
    async fn a_default_that_canonicalizes_to_empty_answers_shared() {
        let folder = tempfile::tempdir().unwrap();
        let output = folder.path();
        write_shared_session(output, r#"["zip"]"#).await;
        write_slate(
            output,
            r#"{"path": "/slate.mp4", "default": "cachebust=1"}"#,
        )
        .await;

        ersatztv_core::variant_request::publish_request(output, "")
            .await
            .unwrap();

        VariantManager::new().tick(&channel(output)).await;

        assert_eq!(answer_for(output, "").await, "");
    }

    /// The policy is re-read every tick: adding a default routes on the next
    /// tick, and removing it hands the viewer back to shared, all without a
    /// restart.
    #[tokio::test]
    async fn editing_the_slate_file_between_ticks_changes_the_answer() {
        let folder = tempfile::tempdir().unwrap();
        let output = folder.path();
        write_shared_session(output, r#"["zip"]"#).await;

        ersatztv_core::variant_request::publish_request(output, "")
            .await
            .unwrap();

        let manager = VariantManager::new();
        let channel = channel(output);

        manager.tick(&channel).await;
        assert_eq!(answer_for(output, "").await, "");

        write_slate(output, r#"{"default": "zip=15216"}"#).await;
        manager.tick(&channel).await;
        assert_eq!(answer_for(output, "").await, stable_name("zip=15216"));

        write_slate(output, r#"{"path": "/slate.mp4"}"#).await;
        manager.tick(&channel).await;
        assert_eq!(answer_for(output, "").await, "");
    }

    /// Before the shared session publishes its recognized params, the
    /// default cannot resolve; once they appear, the same file starts
    /// routing without an operator edit. This is the startup flap the
    /// once-per-change log state is keyed for.
    #[tokio::test]
    async fn the_default_starts_routing_once_recognized_params_appear() {
        let folder = tempfile::tempdir().unwrap();
        let output = folder.path();
        write_shared_session(output, "[]").await;
        write_slate(output, r#"{"path": "/slate.mp4", "default": "zip=15216"}"#).await;

        ersatztv_core::variant_request::publish_request(output, "")
            .await
            .unwrap();

        let manager = VariantManager::new();
        let channel = channel(output);

        manager.tick(&channel).await;
        assert_eq!(answer_for(output, "").await, "");

        tokio::fs::write(
            output.join(ersatztv_core::RECOGNIZED_PARAMS_FILE_NAME),
            r#"["zip"]"#,
        )
        .await
        .unwrap();

        manager.tick(&channel).await;
        assert_eq!(answer_for(output, "").await, stable_name("zip=15216"));
    }

    /// A routed bare-query viewer's request file is what keeps the default
    /// cohort's session alive, exactly like a real requester's: while the
    /// file stays fresh the session survives ticks, and once it goes stale
    /// the session reaps and its playlists leave the disk.
    #[tokio::test]
    async fn the_bare_requesters_file_keeps_the_default_cohort_session_alive() {
        let folder = tempfile::tempdir().unwrap();
        let output = folder.path();
        write_shared_session(output, r#"["zip"]"#).await;
        write_slate(output, r#"{"path": "/slate.mp4", "default": "zip=15216"}"#).await;

        ersatztv_core::variant_request::publish_request(output, "")
            .await
            .unwrap();

        let manager = VariantManager::new();
        let channel = channel(output);
        let playlist = output.join(composed_playlist_name(&stable_name("zip=15216"), false));

        manager.tick(&channel).await;
        assert!(playlist.exists());

        manager.tick(&channel).await;
        assert!(playlist.exists(), "a fresh request must hold the session");

        let request = requests_folder(output).join(stable_name(""));
        let stale = filetime::FileTime::from_unix_time(1_000_000, 0);
        filetime::set_file_mtime(&request, stale).unwrap();

        manager.tick(&channel).await;
        assert!(
            !playlist.exists(),
            "a stale request must reap the session it admitted"
        );
    }

    /// Each policy outcome carries what its once-per-change log needs: the
    /// raw value rides along so an operator edit re-logs even when it
    /// resolves the same way, and 'Zip=15216' resolves through the normal
    /// canonicalization rather than raw comparison.
    #[tokio::test]
    async fn the_default_policy_names_each_outcome() {
        let folder = tempfile::tempdir().unwrap();
        let output = folder.path();
        let channel = channel(output);
        let recognized: BTreeSet<String> = [String::from("zip")].into();

        assert_eq!(
            resolve_default_policy(&channel, &recognized).await,
            DefaultPolicy::Absent
        );

        write_slate(output, r#"{"path": "/slate.mp4"}"#).await;
        assert_eq!(
            resolve_default_policy(&channel, &recognized).await,
            DefaultPolicy::Absent
        );

        write_slate(output, "{not json").await;
        assert!(matches!(
            resolve_default_policy(&channel, &recognized).await,
            DefaultPolicy::Malformed { .. }
        ));

        write_slate(output, r#"{"default": "cachebust=1"}"#).await;
        assert_eq!(
            resolve_default_policy(&channel, &recognized).await,
            DefaultPolicy::Unroutable {
                raw: String::from("cachebust=1")
            }
        );

        write_slate(output, r#"{"default": "Zip=15216"}"#).await;
        assert_eq!(
            resolve_default_policy(&channel, &recognized).await,
            DefaultPolicy::Routes {
                raw: String::from("Zip=15216"),
                cohort_query: String::from("zip=15216"),
                without_path: true
            }
        );
    }

    /// A composed entry as the audit sees it. The program date time is the
    /// SHARED stamp, deliberately different from any variant stamp a test
    /// gives the sidecar, so measuring on the wrong clock cannot pass.
    fn served(path: &str, variant: bool, shared_stamp: OffsetDateTime) -> ComposedEntry {
        ComposedEntry {
            path: path.to_owned(),
            duration: 4.0,
            program_date_time: shared_stamp,
            discontinuity: false,
            sequence: 0,
            variant,
        }
    }

    fn twin(path: &str, stamp: OffsetDateTime) -> SidecarSegment {
        SidecarSegment {
            path: path.to_owned(),
            duration: 4.0,
            program_date_time: crate::composer::format_pdt(stamp),
            item_id: String::from("game"),
            discontinuity: false,
        }
    }

    fn variant_sidecar(segments: Vec<SidecarSegment>) -> PlaylistSidecar {
        PlaylistSidecar {
            segments,
            pipelines: Vec::new(),
        }
    }

    #[test]
    fn reach_is_measured_on_the_variants_own_stamps() {
        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1000);
        // the shared stamp says this position is current; the variant's own
        // stamp says the file is 100s old, and the trim runs on that clock
        let window = [served("variants/abc/live000003.ts", true, now)];
        let variant = variant_sidecar(vec![twin(
            "live000003.ts",
            now - time::Duration::seconds(100),
        )]);

        assert_eq!(
            deepest_variant_reach_ms(window.iter(), &variant, "variants/abc/", now),
            Some(100_000)
        );
    }

    #[test]
    fn reach_takes_the_deepest_entry_of_the_window() {
        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1000);
        let window = [
            served("variants/abc/live000003.ts", true, now),
            served("variants/abc/live000004.ts", true, now),
        ];
        let variant = variant_sidecar(vec![
            twin("live000003.ts", now - time::Duration::seconds(90)),
            twin("live000004.ts", now - time::Duration::seconds(40)),
        ]);

        assert_eq!(
            deepest_variant_reach_ms(window.iter(), &variant, "variants/abc/", now),
            Some(90_000)
        );
    }

    /// Shared entries are on the shared session's retention and are not this
    /// measurement; a twin the sidecar no longer lists cannot be measured on
    /// any clock and must not poison the maximum with a guess.
    #[test]
    fn shared_entries_and_unlisted_twins_are_not_measured() {
        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1000);
        let ancient = now - time::Duration::seconds(900);
        let window = [
            served("live000900.ts", false, ancient),
            served("variants/abc/live000007.ts", true, ancient),
        ];
        let variant = variant_sidecar(vec![twin("live000900.ts", ancient)]);

        assert_eq!(
            deepest_variant_reach_ms(window.iter(), &variant, "variants/abc/", now),
            None
        );
    }

    /// A twin stamped ahead of the wall clock is worked-ahead content, not a
    /// retention concern; it reads as zero reach rather than wrapping.
    #[test]
    fn a_twin_ahead_of_the_wall_clock_reaches_zero() {
        let now = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1000);
        let window = [served("variants/abc/live000009.ts", true, now)];
        let variant = variant_sidecar(vec![twin(
            "live000009.ts",
            now + time::Duration::seconds(30),
        )]);

        assert_eq!(
            deepest_variant_reach_ms(window.iter(), &variant, "variants/abc/", now),
            Some(0)
        );
    }
}
