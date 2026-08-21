use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ersatztv_channel::error::{ChannelError, IoContext};
use ersatztv_core::sidecar::{PlaylistSidecar, SidecarPipeline, SidecarSegment};
use ersatztv_core::{HEARTBEAT_FILE_NAME, HEARTBEAT_FILE_TIMEOUT};
use ffpipeline::pipeline::PtsOffset;
use ffpipeline::web_vtt::{Cue, format_vtt_ts};
use time::OffsetDateTime;
use time::macros::format_description;

const MIN_SEGMENTS: usize = 4;

/// How much media is kept behind the live edge before segments are trimmed
/// and their files deleted. Two minutes.
const HISTORY_DURATION: Duration = Duration::from_secs(120);

/// How far past the wall clock the published window reaches. Twelve seconds.
const PUBLISH_LEAD: Duration =
    Duration::from_secs(ffpipeline::pipeline::SEGMENT_SECONDS as u64 * 3);

#[derive(Clone)]
pub struct SubtitleSource {
    pub cues: Arc<Vec<Cue>>,
    pub(crate) cursor: usize,
    pub next_segment_source_offset: Duration,
}

#[derive(Clone)]
pub struct PlaylistManager {
    output_folder: PathBuf,
    ready_file: PathBuf,
    heartbeat_file: PathBuf,
    generated_playlist_file: String,
    generated_subtitle_playlist_file: String,
    ffmpeg_playlist_file: String,
    ready: bool,

    segments: VecDeque<Segment>,
    discontinuity_before: HashSet<String>,
    media_sequence: u64,
    last_served_media_sequence: u64,
    /// The sequence at the front of the served window, or `None` before the
    /// first window is rendered. Retention measures history behind this, so
    /// it has to be the position viewers are actually reading from.
    ///
    /// The window itself is placed from the wall clock every render: it ends
    /// at the first segment whose program date time reaches `PUBLISH_LEAD`
    /// past now, and starts a full window behind that. Nothing about the
    /// placement is carried between renders, so a session that stalls
    /// resumes at wherever the clock has reached rather than accumulating
    /// permanent deficit against the schedule.
    served_head: Option<u64>,
    discontinuity_sequence: u64,
    target_duration: u32,
    target_duration_f64: f64,
    pending_discontinuity: bool,
    last_segment_end: OffsetDateTime,
    current_session_start: OffsetDateTime,

    pts_offset: Option<PtsOffset>,
    subtitle_source: Option<SubtitleSource>,

    current_item_id: String,
    pipelines: Vec<SidecarPipeline>,

    timeout: bool,
    /// When the heartbeat file was last observed to exist, so its absence
    /// can expire the session too. A worker whose heartbeat file was
    /// deleted (a rival worker's startup empties the whole folder) was
    /// previously immortal: the timeout was only ever computed from the
    /// file's mtime, so no file meant no timeout, ever.
    heartbeat_last_seen: Instant,
    heartbeat_missing_warned: bool,

    last_progress: OffsetDateTime,
}

#[derive(Clone)]
struct Segment {
    path: String,
    duration: f64,
    program_date_time: OffsetDateTime,
    item_id: String,
}

pub struct PlaylistManagerOutputFiles {
    pub generated_playlist_file: String,
    pub ffmpeg_playlist_file: String,
    pub generated_subtitle_playlist_file: String,
}

impl PlaylistManager {
    pub fn new(
        channel_start_time: OffsetDateTime,
        target_duration: u32,
        output_folder: PathBuf,
        ready_file: PathBuf,
        output_files: PlaylistManagerOutputFiles,
    ) -> PlaylistManager {
        let heartbeat_file = output_folder.join(HEARTBEAT_FILE_NAME);

        PlaylistManager {
            output_folder,
            ready_file,
            heartbeat_file,
            generated_playlist_file: output_files.generated_playlist_file,
            ffmpeg_playlist_file: output_files.ffmpeg_playlist_file,
            generated_subtitle_playlist_file: output_files.generated_subtitle_playlist_file,
            ready: false,

            segments: VecDeque::new(),
            discontinuity_before: HashSet::new(),
            media_sequence: 0,
            last_served_media_sequence: 0,
            served_head: None,
            discontinuity_sequence: 0,
            target_duration,
            target_duration_f64: target_duration as f64,
            pending_discontinuity: false,
            last_segment_end: channel_start_time,
            current_session_start: channel_start_time,

            pts_offset: None,
            subtitle_source: None,

            current_item_id: String::new(),
            pipelines: Vec::new(),

            timeout: false,
            heartbeat_last_seen: Instant::now(),
            // Starts true: the file legitimately does not exist until the
            // server first touches it, so absence is only worth a warning
            // after the file has been seen.
            heartbeat_missing_warned: true,

            last_progress: OffsetDateTime::now_utc(),
        }
    }

    pub fn timeout(&self) -> &bool {
        &self.timeout
    }

    pub fn last_progress(&self) -> &OffsetDateTime {
        &self.last_progress
    }

    /// Where the next segment's program date time will land: the end of all
    /// media emitted so far, on the stamp clock. Against `transcoded_until`
    /// (the same position on the schedule clock) this measures how far the
    /// two clocks have drifted apart.
    pub fn last_segment_end(&self) -> OffsetDateTime {
        self.last_segment_end
    }

    pub fn is_ready(&self) -> &bool {
        &self.ready
    }

    pub async fn before_new_pipeline(
        &mut self,
        new_pts_offset: Option<PtsOffset>,
        new_subtitle_source: Option<SubtitleSource>,
        item_id: &str,
        duration_ms: u64,
        templated: bool,
        fallback: bool,
    ) -> Result<(), ChannelError> {
        self.update().await?;
        self.pts_offset = new_pts_offset;
        self.subtitle_source = new_subtitle_source;
        self.pending_discontinuity = true;
        self.current_session_start = self.last_segment_end;
        self.current_item_id = item_id.to_owned();
        self.pipelines.push(SidecarPipeline {
            item_id: item_id.to_owned(),
            pts_offset_ms: new_pts_offset.unwrap_or_default().duration.as_millis() as u64,
            duration_ms,
            templated,
            fallback,
        });

        self.last_progress = OffsetDateTime::now_utc();

        // overwrite ffmpeg's playlist with a generated playlist (containing *all* segments)
        if Path::new(&self.generated_playlist_file).exists() {
            let (generated_playlist, _) =
                self.generate_playlist(|s| s.to_owned(), None, OffsetDateTime::now_utc())?;
            let temp = tempfile::NamedTempFile::new_in(&self.output_folder).io_context(
                "create a temp file for the ffmpeg playlist",
                &self.ffmpeg_playlist_file,
            )?;
            tokio::fs::write(temp.path(), generated_playlist)
                .await
                .io_context(
                    "write the ffmpeg playlist body for",
                    &self.ffmpeg_playlist_file,
                )?;
            tokio::fs::rename(temp.path(), &self.ffmpeg_playlist_file)
                .await
                .io_context("publish the ffmpeg playlist", &self.ffmpeg_playlist_file)?;
        }

        Ok(())
    }

    pub async fn update(&mut self) -> Result<(), ChannelError> {
        // scan for segments on disk
        let mut new_segment_files: VecDeque<String> = VecDeque::new();
        let mut entries = tokio::fs::read_dir(&self.output_folder)
            .await
            .io_context("scan the segment folder", &self.output_folder)?;
        while let Ok(Some(entry)) = entries.next_entry().await {
            if let Some(file_name) = entry.file_name().to_str()
                && file_name.ends_with(".ts")
                && !self.segments.iter().any(|s| s.path == file_name)
            {
                new_segment_files.push_back(file_name.to_owned());
            }
        }

        // get all segment durations from extinf tags in ffmpeg playlist
        let new_segment_durations: HashMap<String, f64> = self.get_new_segment_durations().await?;

        // filter out segments without a known duration
        let mut sorted_new_segments: Vec<String> = Vec::new();
        for segment in new_segment_files {
            if new_segment_durations.contains_key(&segment) {
                sorted_new_segments.push(segment);
            }
        }
        sorted_new_segments.sort();

        // add new segments
        for file in sorted_new_segments {
            if self.pending_discontinuity {
                self.discontinuity_before.insert(file.to_owned());
                self.pending_discontinuity = false;
            }

            let duration = new_segment_durations
                .get(&file)
                .map(|f| f.to_owned())
                .unwrap_or(self.target_duration_f64);

            // rfc8216bis 6.2.1 requires EXT-X-TARGETDURATION to stay constant,
            // and 4.4.3.1 only requires it to cover segment durations rounded
            // to the nearest integer; raise it (a spec violation players
            // tolerate better than an undersized target) only when a segment
            // genuinely exceeds the rounding allowance
            if duration.round() > (self.target_duration as f64) {
                self.target_duration = duration.round() as u32;
            }

            let program_date_time = self.last_segment_end;

            self.segments.push_back(Segment {
                path: file.clone(),
                program_date_time,
                duration,
                item_id: self.current_item_id.clone(),
            });

            self.last_segment_end += Duration::from_secs_f64(duration);
            self.last_progress = OffsetDateTime::now_utc();

            let vtt_path = format!("{}.vtt", file.strip_suffix(".ts").unwrap_or(&file));
            let vtt_full = self.output_folder.join(&vtt_path);
            let mpegts_90khz = (((self.pts_offset.unwrap_or_default().duration.as_secs_f64()
                + (program_date_time - self.current_session_start).as_seconds_f64())
                * 90_000.0) as u64)
                % 8589934592;
            if let Some(src) = &mut self.subtitle_source {
                let body = render_subtitle_segment(
                    src,
                    src.next_segment_source_offset,
                    duration,
                    mpegts_90khz,
                );
                let temp = tempfile::NamedTempFile::new_in(&self.output_folder)
                    .io_context("create a temp file for the subtitle segment", &vtt_full)?;
                tokio::fs::write(temp.path(), body)
                    .await
                    .io_context("write the subtitle segment body for", &vtt_full)?;
                tokio::fs::rename(temp.path(), &vtt_full)
                    .await
                    .io_context("publish the subtitle segment", &vtt_full)?;
                src.next_segment_source_offset += Duration::from_secs_f64(duration);
            } else {
                let body = format!(
                    "WEBVTT\nX-TIMESTAMP-MAP=LOCAL:00:00:00.000,MPEGTS:{}\n\n",
                    mpegts_90khz
                );
                let temp = tempfile::NamedTempFile::new_in(&self.output_folder).io_context(
                    "create a temp file for the empty subtitle segment",
                    &vtt_full,
                )?;
                tokio::fs::write(temp.path(), body)
                    .await
                    .io_context("write the empty subtitle segment body for", &vtt_full)?;
                tokio::fs::rename(temp.path(), &vtt_full)
                    .await
                    .io_context("publish the empty subtitle segment", &vtt_full)?;
            }
        }

        // trim old segments
        let cutoff = self.trim_cutoff();
        while !self.segments.is_empty() && self.segments[0].program_date_time < cutoff {
            if let Some(removed) = self.segments.remove(0) {
                self.media_sequence += 1;
                if self.discontinuity_before.contains(&removed.path) {
                    self.discontinuity_before.remove(&removed.path);
                    self.discontinuity_sequence += 1;
                }

                let path = self.output_folder.join(&removed.path);
                tokio::fs::remove_file(&path)
                    .await
                    .io_context("delete the trimmed segment", &path)?;

                let vtt_path = self.output_folder.join(format!(
                    "{}.vtt",
                    removed.path.strip_suffix(".ts").unwrap_or(&removed.path)
                ));
                if vtt_path.exists() {
                    tokio::fs::remove_file(&vtt_path)
                        .await
                        .io_context("delete the trimmed subtitle segment", &vtt_path)?;
                }
            }
        }

        // drop pipeline records once no remaining segment references their
        // item; the current pipeline's record always survives, even before its
        // first segment lands
        self.pipelines.retain(|p| {
            p.item_id == self.current_item_id
                || self.segments.iter().any(|s| s.item_id == p.item_id)
        });

        // generate and atomically save playlist
        let (generated_playlist, playlist_segment_count) =
            self.generate_playlist(|s| s.to_owned(), Some(10), OffsetDateTime::now_utc())?;
        let temp = tempfile::NamedTempFile::new_in(&self.output_folder).io_context(
            "create a temp file for the served playlist",
            &self.generated_playlist_file,
        )?;
        tokio::fs::write(temp.path(), generated_playlist)
            .await
            .io_context(
                "write the served playlist body for",
                &self.generated_playlist_file,
            )?;
        tokio::fs::rename(temp.path(), &self.generated_playlist_file)
            .await
            .io_context("publish the served playlist", &self.generated_playlist_file)?;

        // publish the machine-readable sidecar alongside the playlist
        let sidecar = self.generate_sidecar()?;
        let sidecar_file = format!(
            "{}{}",
            self.generated_playlist_file,
            ersatztv_core::sidecar::SIDECAR_SUFFIX
        );
        let temp = tempfile::NamedTempFile::new_in(&self.output_folder)
            .io_context("create a temp file for the sidecar", &sidecar_file)?;
        tokio::fs::write(temp.path(), sidecar)
            .await
            .io_context("write the sidecar body for", &sidecar_file)?;
        tokio::fs::rename(temp.path(), &sidecar_file)
            .await
            .io_context("publish the sidecar", &sidecar_file)?;

        // generate and atomically save subtitle playlist
        let (generated_subtitle_playlist, _) = self.generate_playlist(
            |s| format!("{}.vtt", s.strip_suffix(".ts").unwrap_or(s)),
            Some(10),
            OffsetDateTime::now_utc(),
        )?;
        let temp = tempfile::NamedTempFile::new_in(&self.output_folder).io_context(
            "create a temp file for the subtitle playlist",
            &self.generated_subtitle_playlist_file,
        )?;
        tokio::fs::write(temp.path(), generated_subtitle_playlist)
            .await
            .io_context(
                "write the subtitle playlist body for",
                &self.generated_subtitle_playlist_file,
            )?;
        tokio::fs::rename(temp.path(), &self.generated_subtitle_playlist_file)
            .await
            .io_context(
                "publish the subtitle playlist",
                &self.generated_subtitle_playlist_file,
            )?;

        if !self.ready && playlist_segment_count >= MIN_SEGMENTS {
            tokio::fs::write(&self.ready_file, b"")
                .await
                .io_context("publish the ready signal", &self.ready_file)?;
            self.ready = true;
        }

        if self.heartbeat_file.exists() {
            let metadata = tokio::fs::metadata(&self.heartbeat_file)
                .await
                .io_context("stat the heartbeat file", &self.heartbeat_file)?;
            let modified = metadata.modified().io_context(
                "read the modified time of the heartbeat file",
                &self.heartbeat_file,
            )?;
            self.heartbeat_last_seen = Instant::now();
            self.heartbeat_missing_warned = false;
            self.timeout = modified.elapsed().unwrap_or(Duration::MAX) > HEARTBEAT_FILE_TIMEOUT;
        } else {
            // A missing heartbeat is a stale heartbeat. The file starts
            // absent (the server touches it on the first viewer request),
            // so construction time anchors the same grace a fresh file gets
            // from its mtime; but a file that disappears after being seen
            // means another process emptied this folder, and a worker that
            // shrugs that off can never idle out again.
            if !self.heartbeat_missing_warned {
                log::warn!(
                    "the heartbeat file {} disappeared; treating it as stale \
                     (another process may have emptied this folder)",
                    self.heartbeat_file.display()
                );
                self.heartbeat_missing_warned = true;
            }
            self.timeout = self.heartbeat_last_seen.elapsed() > HEARTBEAT_FILE_TIMEOUT;
        }

        Ok(())
    }

    fn generate_sidecar(&self) -> Result<String, ChannelError> {
        let format = format_description!(
            "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3][offset_hour sign:mandatory][offset_minute]"
        );

        let segments = self
            .segments
            .iter()
            .map(|s| {
                Ok(SidecarSegment {
                    path: s.path.clone(),
                    duration: s.duration,
                    program_date_time: s.program_date_time.format(format)?,
                    item_id: s.item_id.clone(),
                    discontinuity: self.discontinuity_before.contains(&s.path),
                })
            })
            .collect::<Result<Vec<_>, time::error::Format>>()?;

        let sidecar = PlaylistSidecar {
            segments,
            pipelines: self.pipelines.clone(),
        };

        Ok(serde_json::to_string(&sidecar)?)
    }

    /// The program date time before which segments have aged out of the
    /// history window, measured from the segment the window is serving from.
    ///
    /// Program date times advance with produced media, not with the wall
    /// clock: `last_segment_end` is seeded from the channel start time and
    /// only ever moves by a segment duration. A channel that cannot sustain
    /// realtime, because a source underdelivers or the transcode is too slow
    /// for the hardware, therefore falls behind the wall clock and stays
    /// behind until it catches up. Measuring the cutoff against the wall
    /// clock in that state keeps only `HISTORY_DURATION` minus the lag, and
    /// keeps nothing at all once the lag reaches `HISTORY_DURATION`, which
    /// deletes segment files that playlists being served still reference.
    ///
    /// Measuring it against the live edge has the mirror failure. A file
    /// item transcodes faster than realtime, so produced media runs ahead of
    /// the head the window serves from, and the budget is spent on segments
    /// nobody has reached yet: what survives behind that head is
    /// `HISTORY_DURATION` minus the work-ahead depth, reaching zero on a
    /// channel that works far enough ahead, and deleting the same
    /// still-referenced files.
    ///
    /// The served head is the anchor that bounds history where viewers
    /// actually are, under both. Before the first render there is no head
    /// yet and nothing is being served, so the live edge stands in.
    fn trim_cutoff(&self) -> OffsetDateTime {
        let served = self
            .served_head
            .and_then(|head| {
                self.segments
                    .get(head.saturating_sub(self.media_sequence) as usize)
            })
            .map(|segment| segment.program_date_time)
            .unwrap_or(self.last_segment_end);

        served - HISTORY_DURATION
    }

    fn generate_playlist(
        &mut self,
        path_map: fn(&str) -> String,
        max_segments: Option<usize>,
        now: OffsetDateTime,
    ) -> Result<(String, usize), ChannelError> {
        let mut playlist = String::new();
        playlist.push_str("#EXTM3U\n");
        // version 6 is the lowest that carries the semantics this playlist
        // relies on: rfc8216bis 8 notes that from version 6 on,
        // EXT-X-TARGETDURATION is the maximum segment duration rounded to the
        // nearest integer. Nothing here needs 7 (no EXT-X-MAP, no INSTREAM-ID
        // SERVICE values), and 6.2.1 asks servers not to declare more than the
        // playlist requires
        playlist.push_str("#EXT-X-VERSION:6\n");
        playlist.push_str(&format!("#EXT-X-TARGETDURATION:{}\n", self.target_duration));

        let (skip, limit) = match max_segments {
            Some(max) => {
                let horizon = now + PUBLISH_LEAD;

                // index one past the newest segment we want to publish
                let head = self
                    .segments
                    .iter()
                    .position(|s| s.program_date_time >= horizon)
                    .unwrap_or(self.segments.len());

                // monotonic clamp, in absolute media-sequence space
                let candidate_ms = self.media_sequence + head.saturating_sub(max) as u64;
                let clamped_ms = candidate_ms.max(self.last_served_media_sequence);
                self.last_served_media_sequence = clamped_ms;
                self.served_head = Some(clamped_ms);

                let skip = ((clamped_ms - self.media_sequence) as usize).min(head);
                (skip, head - skip)
            }
            None => (0, self.segments.len()),
        };
        let effective_media_sequence = self.media_sequence + skip as u64;
        let effective_discontinuity_sequence = self.discontinuity_sequence
            + self
                .segments
                .iter()
                .take(skip)
                .filter(|s| self.discontinuity_before.contains(&s.path))
                .count() as u64;

        playlist.push_str(&format!(
            "#EXT-X-MEDIA-SEQUENCE:{}\n",
            effective_media_sequence
        ));
        // rfc8216bis 6.2.2 requires this tag in any playlist that removes
        // segments and contains EXT-X-DISCONTINUITY, with no exemption while
        // the value is still zero
        playlist.push_str(&format!(
            "#EXT-X-DISCONTINUITY-SEQUENCE:{}\n",
            effective_discontinuity_sequence
        ));
        playlist.push_str("#EXT-X-INDEPENDENT-SEGMENTS\n");

        let format = format_description!(
            "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3][offset_hour sign:mandatory][offset_minute]"
        );

        for segment in self.segments.iter().skip(skip).take(limit) {
            if self.discontinuity_before.contains(&segment.path) {
                playlist.push_str("#EXT-X-DISCONTINUITY\n");
            }
            playlist.push_str(&format!("#EXTINF:{:.6},\n", segment.duration));
            playlist.push_str(&format!(
                "#EXT-X-PROGRAM-DATE-TIME:{}\n",
                segment.program_date_time.format(format)?
            ));
            playlist.push_str(&format!("{}\n", path_map(&segment.path)));
        }

        Ok((playlist, limit))
    }

    async fn get_new_segment_durations(&self) -> Result<HashMap<String, f64>, ChannelError> {
        let mut result: HashMap<String, f64> = HashMap::new();

        let path = Path::new(&self.ffmpeg_playlist_file);
        if path.exists() {
            let contents = tokio::fs::read_to_string(&path)
                .await
                .io_context("read the ffmpeg playlist", path)?;
            let lines: Vec<&str> = contents.split('\n').collect();
            let mut i: usize = 0;
            while i < lines.len() {
                if lines[i].starts_with("#EXTINF:")
                    && i + 2 < lines.len()
                    && lines[i + 2].ends_with(".ts")
                {
                    let segment_name = lines[i + 2];
                    let inf_split: Vec<&str> =
                        lines[i].split(':').map(|s| s.trim_matches(',')).collect();
                    if let Ok(duration) = inf_split[1].parse::<f64>() {
                        result.insert(segment_name.to_owned(), duration);
                    }
                }

                i += 1;
            }
        }

        Ok(result)
    }
}

fn render_subtitle_segment(
    src: &mut SubtitleSource,
    seg_start_src: Duration,
    duration: f64,
    mpegts_90khz: u64,
) -> String {
    let seg_end_src = seg_start_src + Duration::from_secs_f64(duration);

    // cue times are written on the source timeline, with X-TIMESTAMP-MAP
    // anchoring that timeline to this segment's pts. rfc8216bis 3.1.4
    // requires each cue to carry its total display time even where the range
    // extends outside the segment, and a segment-relative timeline cannot
    // express that for a cue that started before the segment did
    let mut out = format!(
        "WEBVTT\nX-TIMESTAMP-MAP=LOCAL:{},MPEGTS:{}\n\n",
        format_vtt_ts(seg_start_src),
        mpegts_90khz
    );

    let mut segment_cursor = src.cursor;

    while let Some(cue) = src.cues.get(segment_cursor)
        && cue.start < seg_end_src
    {
        if cue.end > seg_start_src {
            out.push_str(&format!(
                "{} --> {}\n{}\n\n",
                format_vtt_ts(cue.start),
                format_vtt_ts(cue.end),
                cue.text
            ));
        }

        // walk persistent cursor if this cue will never display again
        if src.cursor == segment_cursor && cue.end <= seg_end_src {
            src.cursor += 1;
        }

        segment_cursor += 1;
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manager() -> PlaylistManager {
        let folder = std::env::temp_dir();
        PlaylistManager::new(
            OffsetDateTime::UNIX_EPOCH,
            4,
            folder.clone(),
            folder.join(".ready-test"),
            PlaylistManagerOutputFiles {
                generated_playlist_file: String::from("live.m3u8"),
                ffmpeg_playlist_file: String::from("ffmpeg.m3u8"),
                generated_subtitle_playlist_file: String::from("live_sub.m3u8"),
            },
        )
    }

    /// A manager writing into a real folder, so `update` reaches the disk.
    fn manager_in(folder: &Path) -> PlaylistManager {
        PlaylistManager::new(
            OffsetDateTime::UNIX_EPOCH,
            4,
            folder.to_path_buf(),
            folder.join(".ready"),
            PlaylistManagerOutputFiles {
                generated_playlist_file: folder.join("live.m3u8").display().to_string(),
                ffmpeg_playlist_file: folder.join("ffmpeg.m3u8").display().to_string(),
                generated_subtitle_playlist_file: folder
                    .join("live_sub.m3u8")
                    .display()
                    .to_string(),
            },
        )
    }

    fn manager_with_segments(segment_count: usize) -> PlaylistManager {
        let mut m = manager();
        for i in 0..segment_count {
            m.segments
                .push_back(segment(&format!("live{i:06}.ts"), "item-a", i as i64 * 4));
        }
        m
    }

    fn segment(path: &str, item_id: &str, start_offset_secs: i64) -> Segment {
        Segment {
            path: path.to_owned(),
            duration: 4.0,
            program_date_time: OffsetDateTime::UNIX_EPOCH
                + Duration::from_secs(start_offset_secs as u64),
            item_id: item_id.to_owned(),
        }
    }

    /// An io failure inside `update` is one of the two ways an item airs
    /// black, and every one of them used to render as the same sentence about
    /// loading a channel config. These pin the two facts a log line has to
    /// carry on its own: what was being done, and to what.

    #[tokio::test]
    async fn scanning_a_missing_segment_folder_names_the_folder() {
        let folder = tempfile::tempdir().unwrap();
        let missing = folder.path().join("gone");
        let mut manager = manager_in(&missing);

        let message = manager.update().await.unwrap_err().to_string();

        assert!(
            message.contains("scan the segment folder"),
            "message does not name the operation: {message}"
        );
        assert!(
            message.contains(&missing.display().to_string()),
            "message does not name the folder: {message}"
        );
        assert!(
            !message.contains("channel config"),
            "message blames the channel config: {message}"
        );
    }

    /// The incident's signature: a trimmed segment whose file is already gone
    /// aborts `update`, fails the item, and airs black. The line has to name
    /// the segment, or the only way to reach it is to read this module.
    #[tokio::test]
    async fn trimming_a_segment_whose_file_is_gone_names_the_segment() {
        let folder = tempfile::tempdir().unwrap();
        let mut manager = manager_in(folder.path());

        // one held segment, aged past the retention window, with no file on
        // disk behind it
        manager
            .segments
            .push_back(segment("live000042.ts", "item-a", 0));
        manager.last_segment_end =
            OffsetDateTime::UNIX_EPOCH + HISTORY_DURATION + Duration::from_secs(60);

        let message = manager.update().await.unwrap_err().to_string();

        assert!(
            message.contains("delete the trimmed segment"),
            "message does not name the operation: {message}"
        );
        assert!(
            message.contains("live000042.ts"),
            "message does not name the segment: {message}"
        );
        assert!(
            !message.contains("channel config"),
            "message blames the channel config: {message}"
        );
    }

    /// The window ends at the first segment whose program date time reaches
    /// `PUBLISH_LEAD` past the wall clock, and starts a full window behind
    /// that. Segments are four seconds and `PUBLISH_LEAD` is twelve, so at
    /// t=40s the horizon is t=52s, which is segment 13, and a ten segment
    /// window starts at 3.
    #[test]
    fn window_is_placed_from_the_wall_clock() {
        let mut m = manager_with_segments(20);
        let start = OffsetDateTime::UNIX_EPOCH + Duration::from_secs(40);

        let (first, count) = m
            .generate_playlist(|s| s.to_owned(), Some(10), start)
            .unwrap();
        assert!(first.contains("#EXT-X-MEDIA-SEQUENCE:3\n"));
        assert_eq!(count, 10);

        // eight seconds later the horizon has moved two segments, and the
        // window moves with it rather than with any produced media
        let (later, _) = m
            .generate_playlist(|s| s.to_owned(), Some(10), start + Duration::from_secs(8))
            .unwrap();
        assert!(later.contains("#EXT-X-MEDIA-SEQUENCE:5\n"));
    }

    /// The anti-ratchet property. The window carries no placement between
    /// renders, so a session that stalls does not accumulate deficit against
    /// the schedule: however long the clock runs past the newest segment, the
    /// next render serves the newest full window rather than resuming from
    /// wherever a paced head had crept to.
    ///
    /// This is the property that makes a serve head unable to fall
    /// permanently behind the wall clock, and it is the reason the window is
    /// not paced at 1x.
    #[test]
    fn window_recovers_the_live_edge_after_a_long_stall() {
        let mut m = manager_with_segments(20);
        let start = OffsetDateTime::UNIX_EPOCH + Duration::from_secs(40);

        m.generate_playlist(|s| s.to_owned(), Some(10), start)
            .unwrap();

        // no new segments arrive for several minutes
        let (after, count) = m
            .generate_playlist(|s| s.to_owned(), Some(10), start + Duration::from_secs(400))
            .unwrap();

        // the newest ten segments, right up to the live edge
        assert!(after.contains("#EXT-X-MEDIA-SEQUENCE:10\n"));
        assert!(after.contains("live000010.ts"));
        assert!(after.contains("live000019.ts"));
        assert!(!after.contains("live000009.ts"));
        assert_eq!(count, 10);
    }

    #[test]
    fn sidecar_maps_segments_to_items_and_pipelines_to_offsets() {
        let mut m = manager();
        m.current_item_id = String::from("item-b");
        m.pipelines = vec![
            SidecarPipeline {
                item_id: String::from("item-a"),
                pts_offset_ms: 0,
                duration_ms: 8000,
                templated: false,
                fallback: false,
            },
            SidecarPipeline {
                item_id: String::from("item-b"),
                pts_offset_ms: 8000,
                duration_ms: 8000,
                templated: true,
                fallback: true,
            },
        ];
        m.segments.push_back(segment("seg0.ts", "item-a", 0));
        m.segments.push_back(segment("seg1.ts", "item-a", 4));
        m.segments.push_back(segment("seg2.ts", "item-b", 8));
        m.discontinuity_before.insert(String::from("seg2.ts"));

        let json = m.generate_sidecar().unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();

        let segments = value["segments"].as_array().unwrap();
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0]["item_id"], "item-a");
        assert_eq!(segments[0]["discontinuity"], false);
        assert_eq!(segments[2]["item_id"], "item-b");
        assert_eq!(segments[2]["discontinuity"], true);
        assert_eq!(segments[2]["duration"], 4.0);
        assert!(
            segments[2]["program_date_time"]
                .as_str()
                .unwrap()
                .starts_with("1970-01-01T00:00:08.000")
        );

        let pipelines = value["pipelines"].as_array().unwrap();
        assert_eq!(pipelines.len(), 2);
        assert_eq!(pipelines[1]["item_id"], "item-b");
        assert_eq!(pipelines[1]["pts_offset_ms"], 8000);
        assert_eq!(pipelines[0]["fallback"], false);
        assert_eq!(pipelines[1]["fallback"], true);
    }

    #[test]
    fn pipeline_records_prune_with_their_segments() {
        let mut m = manager();
        m.current_item_id = String::from("item-b");
        m.pipelines = vec![
            SidecarPipeline {
                item_id: String::from("item-a"),
                pts_offset_ms: 0,
                duration_ms: 8000,
                templated: false,
                fallback: false,
            },
            SidecarPipeline {
                item_id: String::from("item-b"),
                pts_offset_ms: 8000,
                duration_ms: 8000,
                templated: true,
                fallback: false,
            },
        ];
        // item-a segments have been trimmed from the window already
        m.segments.push_back(segment("seg2.ts", "item-b", 8));

        m.pipelines.retain(|p| {
            p.item_id == m.current_item_id || m.segments.iter().any(|s| s.item_id == p.item_id)
        });

        assert_eq!(m.pipelines.len(), 1);
        assert_eq!(m.pipelines[0].item_id, "item-b");
    }

    /// A window of `segment_count` four-second segments starting at
    /// `channel_start`, with the live edge advanced exactly as `update`
    /// advances it when it appends a segment.
    fn window_anchored_at(channel_start: OffsetDateTime, segment_count: u64) -> PlaylistManager {
        let mut m = manager();
        for i in 0..segment_count {
            m.segments.push_back(Segment {
                path: format!("live{i:06}.ts"),
                duration: 4.0,
                program_date_time: channel_start + Duration::from_secs(i * 4),
                item_id: String::from("item-a"),
            });
        }
        m.last_segment_end = channel_start + Duration::from_secs(segment_count * 4);
        m
    }

    /// The leading segments `update` would trim and delete.
    fn expired(m: &PlaylistManager) -> usize {
        let cutoff = m.trim_cutoff();
        m.segments
            .iter()
            .take_while(|s| s.program_date_time < cutoff)
            .count()
    }

    #[test]
    fn keeps_two_minutes_of_media_behind_the_live_edge() {
        // 40 four-second segments is 160s of media, so 40s of it has aged out
        let m = window_anchored_at(OffsetDateTime::UNIX_EPOCH, 40);

        assert_eq!(expired(&m), 10);
        assert_eq!(
            (m.segments.len() - expired(&m)) as u64 * 4,
            HISTORY_DURATION.as_secs()
        );
    }

    #[test]
    fn history_survives_a_channel_running_behind_the_wall_clock() {
        // a channel whose source underdelivers falls behind the wall clock,
        // and its program date times fall behind with it. the retained window
        // must not shrink by the lag, because these files are still
        // referenced by the playlists being served
        let on_time = window_anchored_at(OffsetDateTime::UNIX_EPOCH, 40);
        let behind = window_anchored_at(
            OffsetDateTime::now_utc() - Duration::from_secs(97 + 160),
            40,
        );

        assert_eq!(expired(&behind), expired(&on_time));
    }

    #[test]
    fn history_survives_production_running_ahead_of_the_served_head() {
        // a file item transcodes faster than realtime, so produced media runs
        // past the head the window serves from. measuring the cutoff from the
        // live edge spends the whole budget on media nobody has reached and
        // deletes everything behind that head, which served playlists still
        // reference: 60 segments produced, serving from segment 30, so 120s
        // of media sits ahead of the head
        let mut m = window_anchored_at(OffsetDateTime::UNIX_EPOCH, 60);
        m.served_head = Some(30);

        assert_eq!(expired(&m), 0);

        let behind_head = m.served_head.unwrap() - m.media_sequence - expired(&m) as u64;
        assert_eq!(behind_head * 4, HISTORY_DURATION.as_secs());
    }

    #[test]
    fn history_behind_the_served_head_stays_bounded() {
        // the mirror of the above: 160s of media sits behind the head, 40s
        // more than the budget, and the excess still ages out
        let mut m = window_anchored_at(OffsetDateTime::UNIX_EPOCH, 40);
        m.served_head = Some(39);

        assert_eq!(expired(&m), 9);
    }

    #[test]
    fn nothing_is_trimmed_before_the_window_fills() {
        // 10 segments is 40s of media, well inside the history window
        let m = window_anchored_at(OffsetDateTime::UNIX_EPOCH, 10);

        assert_eq!(expired(&m), 0);
    }

    fn source(cues: Vec<Cue>) -> SubtitleSource {
        SubtitleSource {
            cues: Arc::new(cues),
            cursor: 0,
            next_segment_source_offset: Duration::ZERO,
        }
    }

    fn cue(start: f64, end: f64, text: &str) -> Cue {
        Cue {
            start: Duration::from_secs_f64(start),
            end: Duration::from_secs_f64(end),
            text: String::from(text),
        }
    }

    #[test]
    fn spanning_cue_keeps_its_full_range_in_every_segment() {
        // a cue from 2s to 10s covers all of the second segment and part of
        // the first and third
        let mut src = source(vec![cue(2.0, 10.0, "spanning")]);

        let first = render_subtitle_segment(&mut src, Duration::ZERO, 4.0, 0);
        let second = render_subtitle_segment(&mut src, Duration::from_secs(4), 4.0, 360_000);
        let third = render_subtitle_segment(&mut src, Duration::from_secs(8), 4.0, 720_000);

        for segment in [&first, &second, &third] {
            assert!(
                segment.contains("00:00:02.000 --> 00:00:10.000"),
                "expected the full cue range, got:\n{segment}"
            );
        }
    }

    #[test]
    fn timestamp_map_anchors_the_source_timeline_to_the_segment() {
        let mut src = source(vec![cue(6.0, 7.0, "later")]);

        let segment = render_subtitle_segment(&mut src, Duration::from_secs(4), 4.0, 360_000);

        assert!(
            segment.starts_with("WEBVTT\nX-TIMESTAMP-MAP=LOCAL:00:00:04.000,MPEGTS:360000\n\n")
        );
        assert!(segment.contains("00:00:06.000 --> 00:00:07.000"));
    }

    #[test]
    fn cue_that_ended_before_the_segment_is_not_emitted() {
        let mut src = source(vec![cue(0.5, 1.5, "early"), cue(5.0, 6.0, "later")]);

        let first = render_subtitle_segment(&mut src, Duration::ZERO, 4.0, 0);
        let second = render_subtitle_segment(&mut src, Duration::from_secs(4), 4.0, 360_000);

        assert!(first.contains("early"));
        assert!(!second.contains("early"));
        assert!(second.contains("later"));
    }

    /// A worker whose heartbeat file was deleted (a rival worker's startup
    /// empties the whole folder) used to be immortal: the timeout was only
    /// computed from the file's mtime, so no file meant no timeout, ever.
    /// Absence past the grace now expires the session like staleness does.
    #[tokio::test]
    async fn a_missing_heartbeat_arms_the_idle_timeout_after_the_grace() {
        let folder = tempfile::tempdir().unwrap();
        let mut manager = manager_in(folder.path());
        manager.heartbeat_last_seen =
            Instant::now() - HEARTBEAT_FILE_TIMEOUT - Duration::from_secs(1);

        manager.update().await.unwrap();

        assert!(
            *manager.timeout(),
            "a heartbeat missing past the grace must expire the session"
        );
    }

    #[tokio::test]
    async fn a_missing_heartbeat_within_the_grace_does_not_time_out() {
        let folder = tempfile::tempdir().unwrap();
        let mut manager = manager_in(folder.path());

        manager.update().await.unwrap();

        assert!(
            !*manager.timeout(),
            "a fresh session must get the same grace a fresh heartbeat file gets"
        );
    }

    #[tokio::test]
    async fn a_heartbeat_that_reappears_rearms_the_grace() {
        let folder = tempfile::tempdir().unwrap();
        let mut manager = manager_in(folder.path());
        manager.heartbeat_last_seen =
            Instant::now() - HEARTBEAT_FILE_TIMEOUT - Duration::from_secs(1);
        std::fs::write(folder.path().join(HEARTBEAT_FILE_NAME), b"").unwrap();

        manager.update().await.unwrap();
        assert!(!*manager.timeout());

        // and once seen, disappearance restarts the clock from the sighting
        std::fs::remove_file(folder.path().join(HEARTBEAT_FILE_NAME)).unwrap();
        manager.update().await.unwrap();
        assert!(
            !*manager.timeout(),
            "a just-deleted heartbeat must get the full grace before expiring"
        );
    }
}
