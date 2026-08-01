use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ersatztv_channel::error::ChannelError;
use ersatztv_core::sidecar::{PlaylistSidecar, SidecarPipeline, SidecarSegment};
use ersatztv_core::{HEARTBEAT_FILE_NAME, HEARTBEAT_FILE_TIMEOUT};
use ffpipeline::pipeline::PtsOffset;
use ffpipeline::web_vtt::{Cue, format_vtt_ts};
use time::OffsetDateTime;
use time::macros::format_description;

const MIN_SEGMENTS: usize = 4;

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

    current_item_id: String,
    pipelines: Vec<SidecarPipeline>,

    timeout: bool,
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
        }
    }

    pub fn timeout(&self) -> &bool {
        &self.timeout
    }

    pub async fn before_new_pipeline(
        &mut self,
        new_pts_offset: Option<PtsOffset>,
        new_subtitle_source: Option<SubtitleSource>,
        item_id: &str,
        templated: bool,
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
            templated,
        });

        // overwrite ffmpeg's playlist with a generated playlist (containing *all* segments)
        if Path::new(&self.generated_playlist_file).exists() {
            let generated_playlist = self.generate_playlist(|s| s.to_owned(), None)?;
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
                item_id: self.current_item_id.clone(),
            });

            self.last_segment_end += Duration::from_secs_f64(duration);

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
        let cutoff = OffsetDateTime::now_utc() - Duration::from_mins(2);
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

        // drop pipeline records once no remaining segment references their
        // item; the current pipeline's record always survives, even before its
        // first segment lands
        self.pipelines.retain(|p| {
            p.item_id == self.current_item_id
                || self.segments.iter().any(|s| s.item_id == p.item_id)
        });

        // generate and atomically save playlist
        let generated_playlist = self.generate_playlist(|s| s.to_owned(), Some(10))?;
        let temp = tempfile::NamedTempFile::new_in(&self.output_folder)?;
        tokio::fs::write(temp.path(), generated_playlist).await?;
        tokio::fs::rename(temp.path(), &self.generated_playlist_file).await?;

        // publish the machine-readable sidecar alongside the playlist
        let sidecar = self.generate_sidecar()?;
        let temp = tempfile::NamedTempFile::new_in(&self.output_folder)?;
        tokio::fs::write(temp.path(), sidecar).await?;
        tokio::fs::rename(
            temp.path(),
            format!(
                "{}{}",
                self.generated_playlist_file,
                ersatztv_core::sidecar::SIDECAR_SUFFIX
            ),
        )
        .await?;

        // generate and atomically save subtitle playlist
        let generated_subtitle_playlist = self.generate_playlist(
            |s| format!("{}.vtt", s.strip_suffix(".ts").unwrap_or(s)),
            Some(10),
        )?;
        let temp = tempfile::NamedTempFile::new_in(&self.output_folder)?;
        tokio::fs::write(temp.path(), generated_subtitle_playlist).await?;
        tokio::fs::rename(temp.path(), &self.generated_subtitle_playlist_file).await?;

        if !self.ready && self.segments.len() >= MIN_SEGMENTS {
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

    fn generate_playlist(
        &mut self,
        path_map: fn(&str) -> String,
        max_segments: Option<usize>,
    ) -> Result<String, ChannelError> {
        let mut playlist = String::new();
        playlist.push_str("#EXTM3U\n");
        playlist.push_str("#EXT-X-VERSION:7\n");
        playlist.push_str(&format!("#EXT-X-TARGETDURATION:{}\n", self.target_duration));

        let (skip, limit) = match max_segments {
            Some(max) => {
                let anchor = OffsetDateTime::now_utc()
                    - Duration::from_secs(ffpipeline::pipeline::SEGMENT_SECONDS as u64 * 5u64);

                let candidate_skip = self
                    .segments
                    .iter()
                    .position(|s| s.program_date_time >= anchor)
                    .unwrap_or_else(|| self.segments.len().saturating_sub(max));

                // monotonic clamp
                let candidate_ms = self.media_sequence + candidate_skip as u64;
                let clamped_ms = candidate_ms.max(self.last_served_media_sequence);
                self.last_served_media_sequence = clamped_ms;

                let skip = (clamped_ms - self.media_sequence) as usize;
                let skip = skip.min(self.segments.len());
                (skip, max)
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

        Ok(playlist)
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

    fn segment(path: &str, item_id: &str, start_offset_secs: i64) -> Segment {
        Segment {
            path: path.to_owned(),
            duration: 4.0,
            program_date_time: OffsetDateTime::UNIX_EPOCH
                + Duration::from_secs(start_offset_secs as u64),
            item_id: item_id.to_owned(),
        }
    }

    #[test]
    fn sidecar_maps_segments_to_items_and_pipelines_to_offsets() {
        let mut m = manager();
        m.current_item_id = String::from("item-b");
        m.pipelines = vec![
            SidecarPipeline {
                item_id: String::from("item-a"),
                pts_offset_ms: 0,
                templated: false,
            },
            SidecarPipeline {
                item_id: String::from("item-b"),
                pts_offset_ms: 8000,
                templated: true,
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
    }

    #[test]
    fn pipeline_records_prune_with_their_segments() {
        let mut m = manager();
        m.current_item_id = String::from("item-b");
        m.pipelines = vec![
            SidecarPipeline {
                item_id: String::from("item-a"),
                pts_offset_ms: 0,
                templated: false,
            },
            SidecarPipeline {
                item_id: String::from("item-b"),
                pts_offset_ms: 8000,
                templated: true,
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
}
