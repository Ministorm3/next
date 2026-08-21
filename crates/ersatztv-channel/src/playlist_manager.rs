use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ersatztv_channel::error::{ChannelError, IoContext};
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

    timeout: bool,

    last_progress: OffsetDateTime,
}

#[derive(Clone)]
struct Segment {
    path: String,
    duration: f64,
    program_date_time: OffsetDateTime,
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

            timeout: false,

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
    ) -> Result<(), ChannelError> {
        self.update().await?;
        self.pts_offset = new_pts_offset;
        self.subtitle_source = new_subtitle_source;
        self.pending_discontinuity = true;
        self.current_session_start = self.last_segment_end;

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
            self.timeout = modified.elapsed().unwrap_or(Duration::MAX) > HEARTBEAT_FILE_TIMEOUT;
        }

        Ok(())
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
        playlist.push_str("#EXT-X-VERSION:7\n");
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
        if effective_discontinuity_sequence > 0 {
            playlist.push_str(&format!(
                "#EXT-X-DISCONTINUITY-SEQUENCE:{}\n",
                effective_discontinuity_sequence
            ));
        }
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

    let mut out = format!(
        "WEBVTT\nX-TIMESTAMP-MAP=LOCAL:00:00:00.000,MPEGTS:{}\n\n",
        mpegts_90khz
    );

    let mut segment_cursor = src.cursor;

    while let Some(cue) = src.cues.get(segment_cursor)
        && cue.start < seg_end_src
    {
        if cue.end > seg_start_src {
            let local_start = cue.start.saturating_sub(seg_start_src);
            let local_end = cue
                .end
                .saturating_sub(seg_start_src)
                .min(Duration::from_secs_f64(duration));
            out.push_str(&format!(
                "{} --> {}\n{}\n\n",
                format_vtt_ts(local_start),
                format_vtt_ts(local_end),
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
                .push_back(segment(&format!("live{i:06}.ts"), i as i64 * 4));
        }
        m
    }

    fn segment(path: &str, start_offset_secs: i64) -> Segment {
        Segment {
            path: path.to_owned(),
            duration: 4.0,
            program_date_time: OffsetDateTime::UNIX_EPOCH
                + Duration::from_secs(start_offset_secs as u64),
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
        manager.segments.push_back(segment("live000042.ts", 0));
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
}
