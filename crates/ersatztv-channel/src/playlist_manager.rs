use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ersatztv_channel::error::ChannelError;
use ersatztv_core::{HEARTBEAT_FILE_NAME, HEARTBEAT_FILE_TIMEOUT};
use ffpipeline::pipeline::PtsOffset;
use ffpipeline::web_vtt::{Cue, format_vtt_ts};
use time::OffsetDateTime;
use time::macros::format_description;

const MIN_SEGMENTS: usize = 4;

// 12s
const PUBLISH_LEAD: Duration =
    Duration::from_secs(ffpipeline::pipeline::SEGMENT_SECONDS as u64 * 3);

/// How much media to keep behind the segment the published window starts at.
const HISTORY_DURATION: Duration = Duration::from_mins(2);

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
            let (generated_playlist, _) = self.generate_playlist(|s| s.to_owned(), None)?;
            let temp = tempfile::NamedTempFile::new_in(&self.output_folder)?;
            tokio::fs::write(temp.path(), generated_playlist).await?;
            tokio::fs::rename(temp.path(), &self.ffmpeg_playlist_file).await?;
        }

        Ok(())
    }

    pub async fn update(&mut self) -> Result<(), ChannelError> {
        // scan for segments on disk
        let mut new_segment_files: VecDeque<String> = VecDeque::new();
        let mut entries = tokio::fs::read_dir(&self.output_folder).await?;
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
                let temp = tempfile::NamedTempFile::new_in(&self.output_folder)?;
                tokio::fs::write(temp.path(), body).await?;
                tokio::fs::rename(temp.path(), &vtt_full).await?;
                src.next_segment_source_offset += Duration::from_secs_f64(duration);
            } else {
                let body = format!(
                    "WEBVTT\nX-TIMESTAMP-MAP=LOCAL:00:00:00.000,MPEGTS:{}\n\n",
                    mpegts_90khz
                );
                let temp = tempfile::NamedTempFile::new_in(&self.output_folder)?;
                tokio::fs::write(temp.path(), body).await?;
                tokio::fs::rename(temp.path(), &vtt_full).await?;
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
                tokio::fs::remove_file(&path).await?;

                let vtt_path = self.output_folder.join(format!(
                    "{}.vtt",
                    removed.path.strip_suffix(".ts").unwrap_or(&removed.path)
                ));
                if vtt_path.exists() {
                    tokio::fs::remove_file(&vtt_path).await?;
                }
            }
        }

        // generate and atomically save playlist
        let (generated_playlist, playlist_segment_count) =
            self.generate_playlist(|s| s.to_owned(), Some(10))?;
        let temp = tempfile::NamedTempFile::new_in(&self.output_folder)?;
        tokio::fs::write(temp.path(), generated_playlist).await?;
        tokio::fs::rename(temp.path(), &self.generated_playlist_file).await?;

        // generate and atomically save subtitle playlist
        let (generated_subtitle_playlist, _) = self.generate_playlist(
            |s| format!("{}.vtt", s.strip_suffix(".ts").unwrap_or(s)),
            Some(10),
        )?;
        let temp = tempfile::NamedTempFile::new_in(&self.output_folder)?;
        tokio::fs::write(temp.path(), generated_subtitle_playlist).await?;
        tokio::fs::rename(temp.path(), &self.generated_subtitle_playlist_file).await?;

        if !self.ready && playlist_segment_count >= MIN_SEGMENTS {
            tokio::fs::write(&self.ready_file, b"").await?;
            self.ready = true;
        }

        if self.heartbeat_file.exists() {
            let metadata = tokio::fs::metadata(&self.heartbeat_file).await?;
            let modified = metadata.modified()?;
            self.timeout = modified.elapsed().unwrap_or(Duration::MAX) > HEARTBEAT_FILE_TIMEOUT;
        }

        Ok(())
    }

    /// The oldest program date time to keep on disk.
    ///
    /// Segment program date times run on the media timeline. `last_segment_end`
    /// is seeded from the channel start time and only ever moves forward by a
    /// segment duration, so it tracks produced media and not the wall clock.
    /// Comparing those timestamps against a wall clock cutoff mixes the two,
    /// and the retained history becomes the budget minus however far the
    /// channel runs behind realtime.
    ///
    /// Measuring from the newest segment has the opposite failure. A file item
    /// transcodes faster than realtime, so produced media runs ahead of the
    /// segment the window starts at, and the budget is spent on segments no
    /// viewer has reached. What survives behind the window start is the
    /// budget minus the work-ahead depth.
    ///
    /// `last_served_media_sequence` is the media sequence the last published
    /// window started at, which is the oldest segment a viewer can be
    /// positioned at. Measuring from there bounds history where viewers
    /// actually are, under both. Before the first window is published it is
    /// zero, which selects the oldest segment held and so trims nothing.
    fn trim_cutoff(&self) -> OffsetDateTime {
        let start_index = self
            .last_served_media_sequence
            .saturating_sub(self.media_sequence) as usize;

        let window_start = self
            .segments
            .get(start_index)
            .map(|segment| segment.program_date_time)
            .unwrap_or(self.last_segment_end);

        window_start - HISTORY_DURATION
    }

    fn generate_playlist(
        &mut self,
        path_map: fn(&str) -> String,
        max_segments: Option<usize>,
    ) -> Result<(String, usize), ChannelError> {
        let mut playlist = String::new();
        playlist.push_str("#EXTM3U\n");
        playlist.push_str("#EXT-X-VERSION:7\n");
        playlist.push_str(&format!("#EXT-X-TARGETDURATION:{}\n", self.target_duration));

        let (skip, limit) = match max_segments {
            Some(max) => {
                let horizon = OffsetDateTime::now_utc() + PUBLISH_LEAD;

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
            let contents = tokio::fs::read_to_string(&path).await?;
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

    fn manager(channel_start_time: OffsetDateTime) -> PlaylistManager {
        PlaylistManager::new(
            channel_start_time,
            4,
            std::env::temp_dir(),
            std::env::temp_dir().join("ready"),
            PlaylistManagerOutputFiles {
                generated_playlist_file: String::new(),
                ffmpeg_playlist_file: String::new(),
                generated_subtitle_playlist_file: String::new(),
            },
        )
    }

    /// Appends `count` four second segments on the media timeline, exactly as
    /// the publish loop does.
    fn push_segments(m: &mut PlaylistManager, count: usize) {
        for i in 0..count {
            m.segments.push_back(Segment {
                path: format!("live{i:06}.ts"),
                duration: 4.0,
                program_date_time: m.last_segment_end,
            });
            m.last_segment_end += Duration::from_secs(4);
        }
    }

    /// Applies the trim, exactly as the publish loop does minus the file io.
    fn trim(m: &mut PlaylistManager) {
        let cutoff = m.trim_cutoff();
        while !m.segments.is_empty() && m.segments[0].program_date_time < cutoff {
            m.segments.pop_front();
            m.media_sequence += 1;
        }
    }

    /// How many leading segments the trim loop would delete.
    fn would_trim(m: &PlaylistManager) -> usize {
        let cutoff = m.trim_cutoff();
        m.segments
            .iter()
            .take_while(|s| s.program_date_time < cutoff)
            .count()
    }

    #[test]
    fn trims_nothing_before_a_window_is_published() {
        let mut m = manager(OffsetDateTime::UNIX_EPOCH);
        push_segments(&mut m, 100);

        assert_eq!(would_trim(&m), 0);
    }

    #[test]
    fn a_channel_behind_the_wall_clock_keeps_its_history() {
        // media timestamps an hour behind the wall clock, which is what a
        // channel that cannot sustain realtime looks like
        let mut m = manager(OffsetDateTime::now_utc() - Duration::from_secs(3600));
        push_segments(&mut m, 60);
        m.last_served_media_sequence = 50;

        // 120s of history behind segment 50 is 30 segments, so 20 expire
        assert_eq!(would_trim(&m), 20);
    }

    #[test]
    fn retained_history_does_not_move_with_the_wall_clock() {
        // the same media, presented at six different lags behind the wall
        // clock, has to retain the same history every time
        let retained: Vec<usize> = [0u64, 30, 60, 97, 119, 130]
            .into_iter()
            .map(|lag| {
                let mut m = manager(
                    OffsetDateTime::now_utc() - Duration::from_secs(lag) - Duration::from_secs(400),
                );
                push_segments(&mut m, 100);
                m.last_served_media_sequence = 85;
                100 - would_trim(&m)
            })
            .collect();

        assert_eq!(retained, vec![45; 6], "retained history varied with lag");
    }

    #[test]
    fn history_survives_production_running_ahead_of_the_window_start() {
        let mut m = manager(OffsetDateTime::UNIX_EPOCH);
        push_segments(&mut m, 100);
        m.last_served_media_sequence = 40;

        // measured behind segment 40, not behind the newest segment produced
        assert_eq!(would_trim(&m), 10);
    }

    #[test]
    fn never_trims_the_window_start() {
        let mut m = manager(OffsetDateTime::UNIX_EPOCH);
        push_segments(&mut m, 100);

        for start in [0, 1, 30, 60, 99] {
            m.last_served_media_sequence = start;
            let trimmed = would_trim(&m) as u64;
            assert!(
                trimmed <= start,
                "trimmed {trimmed} segments with the window starting at {start}"
            );
        }
    }

    #[test]
    fn retention_stays_bounded_over_many_publish_cycles() {
        // the window front is set by the publish horizon and only moves
        // forward, so retention has to reach a steady state and hold it
        // however long the channel runs
        let mut m = manager(OffsetDateTime::UNIX_EPOCH);
        let mut steady = Vec::new();

        for cycle in 0..1000 {
            push_segments(&mut m, 1);
            let newest = m.media_sequence + m.segments.len() as u64;
            m.last_served_media_sequence = newest.saturating_sub(10);
            trim(&mut m);
            if cycle > 200 {
                steady.push(m.segments.len());
            }
        }

        let low = *steady.iter().min().unwrap();
        let high = *steady.iter().max().unwrap();
        assert_eq!(low, high, "retained segment count drifted over time");
        assert!(high <= 45, "retained {high} segments, which is not bounded");
    }

    #[test]
    fn falls_back_to_the_newest_media_when_the_window_start_is_missing() {
        let mut m = manager(OffsetDateTime::UNIX_EPOCH);
        push_segments(&mut m, 10);
        m.last_served_media_sequence = 999;

        assert_eq!(m.trim_cutoff(), m.last_segment_end - HISTORY_DURATION);
    }
}
