//! The on-disk request protocol between a playlist requester and a channel
//! worker.
//!
//! A requester cannot resolve a cohort on its own: which query parameters
//! identify a cohort depends on the channel's current playout, and only the
//! worker tracks that. So the requester publishes the raw query it received
//! and reads back the cohort the worker resolved it to. Both directions are
//! plain files in the channel's output folder, in the same spirit as the
//! ready and heartbeat files, so neither process has to reach the other over
//! a socket.
//!
//! The requester's whole job is: publish the query, read the answer, serve the
//! named playlist, and fall back to shared content whenever any of that is
//! missing.

use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Folder beneath a channel's output folder holding every cohort's transcode.
pub const VARIANTS_FOLDER: &str = "variants";

/// How old a composed playlist may be and still be served.
///
/// The worker republishes every live cohort's playlists on each tick, so the
/// modified time is a liveness signal for the loop that produces them. Without
/// this check a worker whose variant loop stopped, while its transcode kept
/// running, would leave a frozen playlist on disk that requesters happily
/// served forever. It is the one failure that would otherwise not degrade to
/// shared content.
pub const PLAYLIST_FRESHNESS: Duration = Duration::from_secs(15);

const REQUESTS_FOLDER: &str = ".requests";
const ANSWERS_FOLDER: &str = ".answers";

/// A short, deterministic, filesystem-safe name for an arbitrary string
/// (fnv-1a). Used for request tokens and for cohort folder names, so a name
/// never has to carry query syntax onto the filesystem.
pub fn stable_name(input: &str) -> String {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = FNV_OFFSET;
    for byte in input.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    format!("{hash:016x}")
}

pub fn requests_folder(output_folder: &Path) -> PathBuf {
    output_folder.join(VARIANTS_FOLDER).join(REQUESTS_FOLDER)
}

pub fn answers_folder(output_folder: &Path) -> PathBuf {
    output_folder.join(VARIANTS_FOLDER).join(ANSWERS_FOLDER)
}

/// Records that a viewer is asking for this raw query right now. The file
/// holds the raw query, since only the worker can canonicalize it; its
/// modified time is the liveness signal, exactly like a heartbeat.
/// Published by rename, never by truncating the live file. A truncating write
/// leaves the request present, with a fresh modified time, and momentarily
/// empty; a worker scanning the folder in that window reads an empty query,
/// canonicalizes it to the default cohort, and reaps the session of the cohort
/// the viewer actually asked for. That is not theoretical: it happened three
/// times over 2026-08-13/14 on a channel being polled every two seconds, each
/// time recovering on the next tick, and once it cost an item its variant when
/// the respawn raced the reaped session's exiting worker for the folder lock.
pub async fn publish_request(output_folder: &Path, raw_query: &str) -> io::Result<()> {
    let folder = requests_folder(output_folder);
    tokio::fs::create_dir_all(&folder).await?;

    let path = folder.join(stable_name(raw_query));
    let temporary = path.with_extension("tmp");
    tokio::fs::write(&temporary, raw_query).await?;
    tokio::fs::rename(&temporary, &path).await
}

/// The cohort folder name the worker resolved this raw query to.
///
/// `None` covers every case where the caller should serve shared content: the
/// worker has not answered yet, or it answered that the query identifies no
/// cohort. The two are deliberately indistinguishable here, because a
/// requester treats them identically.
pub async fn read_answer(output_folder: &Path, raw_query: &str) -> Option<String> {
    let path = answers_folder(output_folder).join(stable_name(raw_query));
    let answer = tokio::fs::read_to_string(&path).await.ok()?;
    let answer = answer.trim();

    if answer.is_empty() {
        None
    } else {
        Some(answer.to_owned())
    }
}

/// How long a cohort's first playlist request waits for the worker to publish
/// a composed playlist before giving up and serving shared.
///
/// A cohort session is reaped when its viewer stops watching, and the reap
/// deletes the composed playlist. So on every fresh tune the file is missing,
/// and without this wait the viewer is handed the SHARED playlist, plays from
/// it, and is then moved onto the composed playlist about eleven media
/// sequences further back. Measured on channel 11 on 2026-08-12: two polls of
/// shared at media sequence 8487, then composed at 8477, the two playlists'
/// newest segments 40.04s apart. The client's position is not in the new
/// playlist at all, so it stalls and re-syncs backwards, which is the stutter
/// and repeat viewers see on every channel change.
///
/// Three worker ticks. The worker admits the cohort, renders it and publishes
/// in the same tick, and a session composing from an already running channel's
/// sidecar history is servable on its first render, so the normal wait is one
/// tick. The bound exists so a channel whose own worker is still starting, or
/// a variant loop that has stopped, still degrades to shared rather than
/// hanging: that is the behaviour this had before, just arrived at later.
pub const COMPOSED_PLAYLIST_WAIT: Duration = Duration::from_secs(6);

/// How often that wait re-checks. Short relative to the worker's own tick, so
/// the playlist is served promptly once it lands rather than on a grid of our
/// own.
const COMPOSED_PLAYLIST_POLL: Duration = Duration::from_millis(200);

/// Reads the composed playlist for `query`, waiting out the worker's next tick
/// rather than falling through to shared while the answer is still pending.
///
/// Returns None only when shared is genuinely the right answer: the query
/// names no cohort, or nothing was published inside
/// [`COMPOSED_PLAYLIST_WAIT`].
pub async fn await_composed_playlist(
    output_folder: &Path,
    query: &str,
    subtitles: bool,
) -> Option<String> {
    let deadline = tokio::time::Instant::now() + COMPOSED_PLAYLIST_WAIT;

    loop {
        match read_answer_detailed(output_folder, query).await {
            // the worker looked and this query names no cohort; shared is the
            // answer and there is nothing to wait for
            CohortAnswer::NoCohort => return None,
            CohortAnswer::Cohort(cohort) => {
                if let Some(playlist) =
                    read_composed_playlist(output_folder, &cohort, subtitles).await
                {
                    return Some(playlist);
                }
            }
            CohortAnswer::Pending => {}
        }

        if tokio::time::Instant::now() >= deadline {
            return None;
        }

        tokio::time::sleep(COMPOSED_PLAYLIST_POLL).await;
    }
}

/// What the worker has said about a raw query, keeping apart the two cases
/// [`read_answer`] deliberately collapses.
///
/// A requester that falls straight through to shared content cannot tell "the
/// worker has not looked at this yet" from "the worker looked and this query
/// names no cohort", and those need opposite handling. The first is a viewer
/// who is about to be given a composed playlist and must not be handed the
/// shared one in the meantime, because the two playlists sit about eleven
/// media sequences apart and switching between them moves a client backwards.
/// The second is a viewer whose content is shared, permanently, with nothing
/// to wait for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CohortAnswer {
    /// No answer file exists: the worker has not completed a tick since the
    /// request was published.
    Pending,
    /// The worker answered, and this query identifies no cohort.
    NoCohort,
    /// The cohort folder name this query resolves to.
    Cohort(String),
}

/// The worker's answer for a raw query, with `Pending` kept separate.
pub async fn read_answer_detailed(output_folder: &Path, raw_query: &str) -> CohortAnswer {
    let path = answers_folder(output_folder).join(stable_name(raw_query));
    match tokio::fs::read_to_string(&path).await {
        Err(_) => CohortAnswer::Pending,
        Ok(answer) => {
            let answer = answer.trim();
            if answer.is_empty() {
                CohortAnswer::NoCohort
            } else {
                CohortAnswer::Cohort(answer.to_owned())
            }
        }
    }
}

/// The composed playlist a cohort's viewers read.
///
/// It sits beside the shared playlist rather than inside the cohort's folder
/// so that the segment paths the composer emits resolve the same way for both
/// playlists: bare names for shared segments, `variants/<cohort>/` prefixed
/// names for the cohort's own.
pub fn composed_playlist_name(cohort: &str, subtitles: bool) -> String {
    if subtitles {
        format!("live_sub.{cohort}.m3u8")
    } else {
        format!("live.{cohort}.m3u8")
    }
}

/// Reads a cohort's composed playlist, but only while its worker is still
/// republishing it.
///
/// Reading and checking freshness are one operation on purpose: a requester
/// that reads the file directly would serve frozen content without noticing.
/// `None` means serve shared content, like every other miss in this protocol.
pub async fn read_composed_playlist(
    output_folder: &Path,
    cohort: &str,
    subtitles: bool,
) -> Option<String> {
    let path = output_folder.join(composed_playlist_name(cohort, subtitles));
    let modified = tokio::fs::metadata(&path).await.ok()?.modified().ok()?;

    let fresh = match modified.elapsed() {
        Ok(age) => age <= PLAYLIST_FRESHNESS,
        // a modified time in the future is a clock that ran ahead of ours,
        // never a playlist that stopped being written
        Err(_) => true,
    };

    if !fresh {
        return None;
    }

    tokio::fs::read_to_string(&path).await.ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_names_are_deterministic_and_distinct() {
        assert_eq!(stable_name("zip=15216"), stable_name("zip=15216"));
        assert_ne!(stable_name("zip=15216"), stable_name("zip=10001"));
        assert_eq!(stable_name("zip=15216").len(), 16);
    }

    #[test]
    fn stable_names_are_filesystem_safe() {
        let name = stable_name("zip=15216&region=west/east?x=1");
        assert!(name.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn composed_playlist_names_separate_media_from_subtitles() {
        assert_eq!(composed_playlist_name("abc", false), "live.abc.m3u8");
        assert_eq!(composed_playlist_name("abc", true), "live_sub.abc.m3u8");
    }

    #[tokio::test]
    async fn a_published_request_reads_back_the_workers_answer() {
        let folder = tempfile::tempdir().unwrap();
        let output = folder.path();

        publish_request(output, "zip=15216").await.unwrap();

        // the worker has not answered yet
        assert_eq!(read_answer(output, "zip=15216").await, None);

        let answers = answers_folder(output);
        tokio::fs::create_dir_all(&answers).await.unwrap();
        tokio::fs::write(answers.join(stable_name("zip=15216")), "cafe1234")
            .await
            .unwrap();

        assert_eq!(
            read_answer(output, "zip=15216").await,
            Some(String::from("cafe1234"))
        );
    }

    #[tokio::test]
    async fn an_empty_answer_means_serve_shared() {
        let folder = tempfile::tempdir().unwrap();
        let output = folder.path();

        let answers = answers_folder(output);
        tokio::fs::create_dir_all(&answers).await.unwrap();
        tokio::fs::write(answers.join(stable_name("cachebust=1")), "")
            .await
            .unwrap();

        assert_eq!(read_answer(output, "cachebust=1").await, None);
    }

    #[tokio::test]
    async fn a_freshly_published_playlist_is_served() {
        let folder = tempfile::tempdir().unwrap();
        let path = folder
            .path()
            .join(composed_playlist_name("cafe1234", false));
        tokio::fs::write(&path, "#EXTM3U\n").await.unwrap();

        assert_eq!(
            read_composed_playlist(folder.path(), "cafe1234", false).await,
            Some(String::from("#EXTM3U\n"))
        );
    }

    /// The failure this exists for: a worker still transcoding, but no longer
    /// republishing composed playlists. The stale file must not be served.
    #[tokio::test]
    async fn a_playlist_the_worker_stopped_republishing_is_not_served() {
        let folder = tempfile::tempdir().unwrap();
        let path = folder
            .path()
            .join(composed_playlist_name("cafe1234", false));
        tokio::fs::write(&path, "#EXTM3U\n").await.unwrap();

        let frozen = filetime::FileTime::from_unix_time(
            filetime::FileTime::now().unix_seconds() - (PLAYLIST_FRESHNESS.as_secs() as i64 + 5),
            0,
        );
        filetime::set_file_mtime(&path, frozen).unwrap();

        assert_eq!(
            read_composed_playlist(folder.path(), "cafe1234", false).await,
            None
        );
    }

    #[tokio::test]
    async fn a_playlist_that_was_never_published_is_not_served() {
        let folder = tempfile::tempdir().unwrap();

        assert_eq!(
            read_composed_playlist(folder.path(), "cafe1234", false).await,
            None
        );
    }

    #[tokio::test]
    async fn a_request_records_the_raw_query_for_the_worker() {
        let folder = tempfile::tempdir().unwrap();
        let output = folder.path();

        publish_request(output, "Zip=15216&access_token=abc")
            .await
            .unwrap();

        let written = tokio::fs::read_to_string(
            requests_folder(output).join(stable_name("Zip=15216&access_token=abc")),
        )
        .await
        .unwrap();

        assert_eq!(written, "Zip=15216&access_token=abc");
    }

    /// The two cases `read_answer` collapses need opposite handling, so they
    /// have to be distinguishable.
    #[tokio::test]
    async fn an_unanswered_request_is_pending_not_a_refusal() {
        let folder = tempfile::tempdir().unwrap();
        let out = folder.path();

        assert_eq!(
            read_answer_detailed(out, "zip=15216").await,
            CohortAnswer::Pending,
        );

        let answers = answers_folder(out);
        tokio::fs::create_dir_all(&answers).await.unwrap();
        tokio::fs::write(answers.join(stable_name("zip=15216")), "")
            .await
            .unwrap();
        assert_eq!(
            read_answer_detailed(out, "zip=15216").await,
            CohortAnswer::NoCohort,
        );

        tokio::fs::write(answers.join(stable_name("zip=15216")), "abc123")
            .await
            .unwrap();
        assert_eq!(
            read_answer_detailed(out, "zip=15216").await,
            CohortAnswer::Cohort(String::from("abc123")),
        );
    }

    /// The defect this guards: a fresh tune finds no composed playlist,
    /// because the reap deleted it, and is handed the shared playlist. The
    /// two sit about eleven media sequences apart, so the client is then
    /// moved backwards and replays what it already showed. The request has to
    /// wait out the worker's next tick instead.
    #[tokio::test]
    async fn a_pending_answer_waits_for_the_worker_rather_than_serving_shared() {
        let folder = tempfile::tempdir().unwrap();
        let out = folder.path().to_path_buf();

        // the worker answers and publishes one tick later
        let worker = {
            let out = out.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(400)).await;
                let answers = answers_folder(&out);
                tokio::fs::create_dir_all(&answers).await.unwrap();
                tokio::fs::write(answers.join(stable_name("zip=15216")), "abc123")
                    .await
                    .unwrap();
                tokio::fs::write(
                    out.join(composed_playlist_name("abc123", false)),
                    "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:8477\n",
                )
                .await
                .unwrap();
            })
        };

        let served = await_composed_playlist(&out, "zip=15216", false).await;
        worker.await.unwrap();

        assert_eq!(
            served.as_deref(),
            Some("#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:8477\n"),
            "a request that arrives before the worker's tick must be served \
             the composed playlist, never the shared one",
        );
    }

    /// A query the worker has looked at and rejected has nothing to wait for.
    #[tokio::test]
    async fn a_query_naming_no_cohort_is_not_waited_on() {
        let folder = tempfile::tempdir().unwrap();
        let out = folder.path();
        let answers = answers_folder(out);
        tokio::fs::create_dir_all(&answers).await.unwrap();
        tokio::fs::write(answers.join(stable_name("nope=1")), "")
            .await
            .unwrap();

        let start = tokio::time::Instant::now();
        assert_eq!(await_composed_playlist(out, "nope=1", false).await, None);
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "shared is the answer here and must be served at once",
        );
    }
}
