use std::borrow::Cow;
use std::collections::VecDeque;
use std::fmt::Formatter;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ersatztv_channel::config::ChannelConfig;
use ersatztv_channel::error::{ChannelError, IoContext};
use ersatztv_channel::slate::{self, SlateFile};
use ersatztv_channel::variant_manager;
use ersatztv_channel::variant_manager::{VariantChannel, VariantManager};
use ersatztv_core::{READY_FILE_NAME, empty_folder};
use ersatztv_playout::playout::{
    AudioHint, PeriodicClock, PlayoutItem, PlayoutItemSource, PlayoutItemTracks, ProbeHint,
    TrackSelection, VideoHint, WatermarkLocation, WatermarkTiming,
};
use ersatztv_playout::template::expand_template;
use ffpipeline::ffmpeg_info::FfmpegInfo;
use ffpipeline::frame_rate::FrameRate;
use ffpipeline::frame_size::FrameSize;
use ffpipeline::input::{
    FfmpegInputArgs, GraphicsInput, HttpInputOptions, HttpInputSource, InputSettings, InputSource,
    LavfiInputSource, LocalInputSource, ProbedInput, RtspInputOptions, RtspInputSource,
};
use ffpipeline::output_settings::{AudioOutputSettings, OutputSettings, SubtitleMode};
use ffpipeline::pipeline::{AudioFormat, Hz, Kbps, PtsOffset, SEGMENT_SECONDS, VideoFormat};
use ffpipeline::probe::{
    CodecType, ProbeResult, ProbeResultAudioStream, ProbeResultColorParams, ProbeResultStream,
    ProbeResultVideoStream, Probeable,
};
use ffpipeline::web_vtt::Cue;
use ffpipeline::{pipeline, probe};
use futures_util::future::try_join_all;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, USER_AGENT};
use time::OffsetDateTime;
use tokio::io::AsyncBufReadExt;
use tokio::sync::Mutex;

use crate::dossier::DossierBuilder;
use crate::local_proxy::{LocalProxyServer, ScriptCommand};
use crate::playlist_manager::{
    PlaylistManager, PlaylistManagerOutputFiles, SubtitleSource, VARIANT_HISTORY_DURATION,
};
use crate::playout_loader::PlayoutLoader;
use crate::pts_scanner::{PtsScanner, PtsTime};

const STDERR_RING_LINES: usize = 2_000;
const STALL_THRESHOLD: Duration = Duration::from_secs(60);
const PLAYLIST_UPDATE_INTERVAL: Duration = Duration::from_secs(2);
const PLAYLIST_UPDATE_INTERVAL_STARTUP: Duration = Duration::from_millis(200);

#[derive(Copy, Clone, PartialEq)]
enum ChannelSessionState {
    SeekAndWorkAhead,
    ZeroAndWorkAhead,
    SeekAndRealtime,
    ZeroAndRealtime,
}

impl std::fmt::Display for ChannelSessionState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ChannelSessionState::SeekAndWorkAhead => write!(f, "SeekAndWorkAhead"),
            ChannelSessionState::ZeroAndWorkAhead => write!(f, "ZeroAndWorkAhead"),
            ChannelSessionState::SeekAndRealtime => write!(f, "SeekAndRealtime"),
            ChannelSessionState::ZeroAndRealtime => write!(f, "ZeroAndRealtime"),
        }
    }
}

struct TimingResult {
    in_point: Duration,
    out_point: Duration,
    finish: OffsetDateTime,
    is_complete: bool,
}

/// Plain-data inputs for [`ChannelSession::build_output_settings`]. Every
/// field is something a test can construct, which is the point: while this
/// construction lived inline in `transcode_item`, reverting a decision in it
/// failed nothing, and the 2026-08-14 padding regression shipped through
/// exactly that blindness.
struct OutputSettingsPlan<'a> {
    channel_config: &'a ChannelConfig,
    accel: Option<ffpipeline::hw_accel::HardwareAccel>,
    output_file: String,
    output_segment_template: String,
    troubleshoot: bool,
    pts_duration: Option<Duration>,
    realtime: bool,
    slate: bool,
    is_live: bool,
    video_is_still_image: bool,
}

/// Plain-data inputs for [`ChannelSession::plan_timings`].
struct TimingPlan<'a> {
    current_item: &'a PlayoutItem,
    audio_source: &'a PlayoutItemSource,
    video_source: &'a PlayoutItemSource,
    subtitle_source: Option<&'a PlayoutItemSource>,
    start_at_zero: bool,
    realtime: bool,
    slate: bool,
    is_live: bool,
    is_templated: bool,
    transcoded_until: OffsetDateTime,
    stamp_error_ms: i64,
}

/// What [`ChannelSession::plan_timings`] decided: the per-stream input
/// timings with the emission trim already applied, and the envelope the
/// sidecar will declare, computed the same way the pipeline computes its
/// own -t.
struct PlannedTimings {
    audio: TimingResult,
    video: TimingResult,
    subtitle: Option<TimingResult>,
    declared_duration_ms: u64,
    trim_ms: i64,
}

pub struct ChannelSession {
    channel_config: ChannelConfig,
    playout_loader: PlayoutLoader,
    pts_scanner: PtsScanner,
    playlist_manager: Arc<Mutex<PlaylistManager>>,
    local_proxy_server: LocalProxyServer,

    ffmpeg_path: PathBuf,
    ffprobe_path: PathBuf,
    ffmpeg_info: FfmpegInfo,
    hw_accel: Option<ffpipeline::hw_accel::HardwareAccel>,

    transcoded_until: OffsetDateTime,
    ready_file: PathBuf,

    output_file: String,
    output_segment_template: String,

    start_time_offset: time::Duration,
    state: ChannelSessionState,

    timeout_notify: Arc<tokio::sync::Notify>,

    cached_subtitles: Option<(String, Arc<Vec<Cue>>)>,
    dynamic_http_client: reqwest::Client,

    published_recognized_params: Option<Vec<String>>,

    /// Caller-supplied values for `{query:}` variables. Empty for the shared
    /// channel session; a variant session carries its cohort's values.
    query_parameters: std::collections::HashMap<String, String>,

    /// Exclusive ownership of the output folder, held for the life of this
    /// session so a second worker for the same folder refuses to start.
    _output_folder_lock: ersatztv_core::FolderLock,
}

impl ChannelSession {
    pub async fn new(channel_config: ChannelConfig) -> Result<ChannelSession, ChannelError> {
        let now = OffsetDateTime::now_local()?;

        let start_time_offset = if let Some(virtual_start) = channel_config.playout.virtual_start {
            virtual_start - now
        } else {
            time::Duration::ZERO
        };

        let output_folder = channel_config.expanded_output_folder().to_owned();

        // Two workers writing one output folder alternate their numbering
        // regimes on disk and corrupt every consumer; refuse to be the
        // second one. The owner releases only by exiting.
        let output_folder_lock = match ersatztv_core::lock_folder_exclusive(&output_folder) {
            Ok(lock) => lock,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                return Err(ChannelError::ChannelStartup(format!(
                    "another channel worker already owns the output folder {}; \
                     refusing to start a second writer",
                    output_folder.display()
                )));
            }
            Err(e) => {
                return Err(e).io_context("lock the output folder", &output_folder);
            }
        };

        let generated_output_file = output_folder
            .join("live.m3u8")
            .into_os_string()
            .into_string()
            .map_err(|p| ChannelError::OutputPathNotUtf8 {
                file: "live.m3u8",
                path: p.to_string_lossy().into_owned(),
            })?;

        let generated_subtitle_output_file = output_folder
            .join("live_sub.m3u8")
            .into_os_string()
            .into_string()
            .map_err(|p| ChannelError::OutputPathNotUtf8 {
                file: "live_sub.m3u8",
                path: p.to_string_lossy().into_owned(),
            })?;

        let ffmpeg_output_file = output_folder
            .join("ffmpeg.m3u8")
            .into_os_string()
            .into_string()
            .map_err(|p| ChannelError::OutputPathNotUtf8 {
                file: "ffmpeg.m3u8",
                path: p.to_string_lossy().into_owned(),
            })?;

        let output_segment_template = output_folder
            .join("live%06d.ts")
            .into_os_string()
            .into_string()
            .map_err(|p| ChannelError::OutputPathNotUtf8 {
                file: "live%06d.ts",
                path: p.to_string_lossy().into_owned(),
            })?;

        let ready_file = output_folder.join(READY_FILE_NAME);

        let playout_loader = PlayoutLoader::new(&channel_config);
        let pts_scanner = PtsScanner::new(&channel_config);
        let playlist_manager = PlaylistManager::new(
            now,
            SEGMENT_SECONDS,
            output_folder.to_owned(),
            ready_file.to_owned(),
            PlaylistManagerOutputFiles {
                generated_playlist_file: generated_output_file,
                generated_subtitle_playlist_file: generated_subtitle_output_file,
                ffmpeg_playlist_file: ffmpeg_output_file.to_owned(),
            },
        );

        let playlist_manager = Arc::new(Mutex::new(playlist_manager));

        let default_ffprobe_path = Path::new("ffprobe").to_path_buf();
        let default_ffmpeg_path = Path::new("ffmpeg").to_path_buf();

        let ffprobe_path = channel_config
            .ffmpeg
            .ffprobe_path
            .clone()
            .unwrap_or(default_ffprobe_path);
        let ffmpeg_path = channel_config
            .ffmpeg
            .ffmpeg_path
            .clone()
            .unwrap_or(default_ffmpeg_path);

        let local_proxy_server = LocalProxyServer::start().await?;

        let dynamic_http_client = reqwest::Client::builder()
            .build()
            .map_err(|e| ChannelError::ChannelStartup(format!("http client: {e}")))?;

        Ok(ChannelSession {
            channel_config,
            playout_loader,
            pts_scanner,
            playlist_manager,
            local_proxy_server,
            ffmpeg_path: ffmpeg_path.to_owned(),
            ffprobe_path: ffprobe_path.to_owned(),
            ffmpeg_info: FfmpegInfo::default(),
            hw_accel: None,
            transcoded_until: now + start_time_offset,
            ready_file,
            output_file: ffmpeg_output_file,
            output_segment_template,
            start_time_offset,
            state: ChannelSessionState::SeekAndWorkAhead,
            timeout_notify: Arc::new(tokio::sync::Notify::new()),
            cached_subtitles: None,
            dynamic_http_client,
            published_recognized_params: None,
            query_parameters: std::collections::HashMap::new(),
            _output_folder_lock: output_folder_lock,
        })
    }

    /// Sets the cohort's `{query:}` values for a variant session.
    pub fn with_query_parameters(
        mut self,
        query_parameters: std::collections::HashMap<String, String>,
    ) -> ChannelSession {
        self.query_parameters = query_parameters;
        self
    }

    /// Spawns the loop that publishes segments to viewers, which is the only
    /// thing that does.
    ///
    /// Shared by the channel session and the variant session on purpose. This
    /// existed as two identical copies until upstream changed the cadence in
    /// #202: the copy in `run` picked up the faster startup interval and the
    /// copy in `run_variant` silently did not, because nothing links two
    /// blocks of copied code. A variant's first sidecar is what the composer's
    /// decision reads, so that copy was the worse one to leave behind. One
    /// function means the next upstream change to this loop reaches both.
    ///
    /// Distinct failures are reported once rather than thirty times a minute,
    /// and recovery is reported too, so a persistent fault stays visible
    /// without burying the log.
    fn spawn_playlist_publisher(
        pm: Arc<Mutex<PlaylistManager>>,
        tn: Arc<tokio::sync::Notify>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut last_failure: Option<String> = None;
            loop {
                let mut playlist_manager = pm.lock().await;
                match playlist_manager.update().await {
                    Ok(()) => {
                        if last_failure.take().is_some() {
                            log::info!("playlist update recovered");
                        }
                    }
                    Err(e) => {
                        let message = e.to_string();
                        if last_failure.as_deref() != Some(message.as_str()) {
                            log::warn!("playlist update failed: {message}");
                            last_failure = Some(message);
                        }
                    }
                }
                if *playlist_manager.timeout() {
                    tn.notify_one();
                    break;
                }
                // publish fast until the window is ready, so the first
                // playlist (and a variant's first sidecar) lands promptly
                let interval = if *playlist_manager.is_ready() {
                    PLAYLIST_UPDATE_INTERVAL
                } else {
                    PLAYLIST_UPDATE_INTERVAL_STARTUP
                };
                drop(playlist_manager);
                tokio::time::sleep(interval).await;
            }
        })
    }

    pub async fn run(&mut self, troubleshoot: bool) -> Result<(), ChannelError> {
        self.prep_output_folder(troubleshoot).await?;

        self.ffmpeg_info = FfmpegInfo::load(
            &self.ffmpeg_path,
            &self.channel_config.ffmpeg.disabled_filters,
            &self.channel_config.ffmpeg.preferred_filters,
        )
        .await?;

        log::debug!("ffmpeg info: {:?}", self.ffmpeg_info);

        self.hw_accel = self
            .channel_config
            .normalization
            .video
            .accel
            .as_ref()
            .and_then(|a| a.to_pipeline(&self.channel_config));

        Self::spawn_playlist_publisher(self.playlist_manager.clone(), self.timeout_notify.clone());

        self.spawn_variant_loop();

        // always work ahead initially
        let realtime = false;
        self.transcode(realtime, troubleshoot).await?;

        if troubleshoot {
            log::debug!("troubleshooting complete; terminating.");
            return Ok(());
        }

        let pm = self.playlist_manager.clone();
        let tn = self.timeout_notify.clone();

        loop {
            if *pm.lock().await.timeout() {
                tn.notify_one();
                return Err(ChannelError::IdleTimeout(
                    self.channel_config.number().to_owned(),
                ));
            }

            let now = OffsetDateTime::now_local()? + self.start_time_offset;
            let transcoded_buffer =
                std::cmp::max(time::Duration::ZERO, self.transcoded_until - now);
            log::debug!(
                "transcoded buffer: {}m {}s",
                transcoded_buffer.whole_minutes(),
                transcoded_buffer.whole_seconds() % 60
            );
            if transcoded_buffer <= time::Duration::minutes(1) {
                // only use realtime when we're at least 30 seconds ahead
                let realtime = transcoded_buffer >= time::Duration::seconds(30);
                self.transcode(realtime, troubleshoot).await?;
            } else {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                    _ = tn.notified() => {
                        return Err(ChannelError::IdleTimeout(
                            self.channel_config.number().to_owned()
                        )                        );
                    }
                }
            }
        }
    }

    /// Transcodes a single playout item as a stream variant: the cohort's
    /// `{query:}` values steer the item's templated URL, the PTS envelope is
    /// anchored to the shared session's offset for the same item, and the
    /// session exits when the item is fully transcoded (or reaps on heartbeat
    /// staleness like any session).
    pub async fn run_variant(
        &mut self,
        item_id: &str,
        pts_offset_ms: u64,
        progress_ms: u64,
        shared_duration_ms: u64,
    ) -> Result<(), ChannelError> {
        self.prep_output_folder(false).await?;

        // STOPGAP: a variant's twins are consumed on the cohort's serve
        // timeline, which trails this session's production by the shared
        // session's serve lag; the extended budget keeps twins alive across
        // that lag instead of trimming them out from under composed
        // playlists. TODO: remove when the composer owns twin lifetimes
        // (see VARIANT_HISTORY_DURATION).
        self.playlist_manager
            .lock()
            .await
            .set_history_duration(VARIANT_HISTORY_DURATION);

        self.ffmpeg_info = FfmpegInfo::load(
            &self.ffmpeg_path,
            &self.channel_config.ffmpeg.disabled_filters,
            &self.channel_config.ffmpeg.preferred_filters,
        )
        .await?;

        self.hw_accel = self
            .channel_config
            .normalization
            .video
            .accel
            .as_ref()
            .and_then(|a| a.to_pipeline(&self.channel_config));

        Self::spawn_playlist_publisher(self.playlist_manager.clone(), self.timeout_notify.clone());

        let item = self
            .playout_loader
            .get_item_by_id(item_id, &self.transcoded_until)
            .await?;

        // the variant's position in the item comes from the shared session's
        // published coverage, not the wall clock: both transcodes must sit on
        // the same envelope grid even when the channel runs behind schedule.
        //
        // that coverage is measured from wherever the shared session started
        // reading, which is not the item's start when the session joined the
        // item partway through. `shared_duration_ms` is the envelope the
        // shared session declared, so the distance from the item's end back
        // to it gives the point its pts offset corresponds to. anchoring at
        // the item's start instead would push the variant's envelope past
        // the shared one by exactly that join distance
        let item_duration_ms = (item.finish - item.start).whole_milliseconds().max(0) as u64;
        let join_offset_ms = shared_join_offset_ms(item_duration_ms, shared_duration_ms);
        let anchor = item.start + time::Duration::milliseconds(join_offset_ms as i64);

        // a live source starts producing on connect, so a variant spawned
        // with lead time must not open it before the position its output
        // claims: connecting early would shift the content off the envelope.
        // file sources seek, so they may start whenever they are ready
        let live_item = [
            Self::resolve_source(&item, |t| t.video.as_ref()),
            Self::resolve_source(&item, |t| t.audio.as_ref()),
        ]
        .iter()
        .flatten()
        .any(source_is_live);

        // and by the same token it must not claim a position the wall clock
        // has already passed: a cohort that tuned in mid-window, or a shared
        // session that reached the item late, would otherwise order a variant
        // at position 0 for an item the composer is already deep into
        let spawned_progress_ms = progress_ms;
        let progress_ms = variant_start_progress_ms(
            spawned_progress_ms,
            anchor,
            OffsetDateTime::now_local()? + self.start_time_offset,
            item.finish,
            live_item,
        );

        // the spawn line in variant_manager reports the progress this worker
        // was ORDERED with, which is not what it ends up claiming once the
        // clock has moved. Reading a join off that line alone is how a
        // misdiagnosis starts, so say where the claim actually landed
        if progress_ms != spawned_progress_ms {
            let envelope_ms = (item.finish - anchor).whole_milliseconds().max(0) as u64;
            if progress_ms >= envelope_ms {
                log::info!(
                    "variant for item {item_id} opened past its {envelope_ms}ms envelope; \
                     there is nothing left to substitute, so no transcode is started"
                );
            } else {
                log::info!(
                    "variant for item {item_id} opened {progress_ms}ms into its {envelope_ms}ms \
                     envelope and claims that position, not the {spawned_progress_ms}ms it was \
                     spawned with"
                );
            }
        }

        self.transcoded_until =
            (anchor + time::Duration::milliseconds(progress_ms as i64)).min(item.finish);
        self.state = if progress_ms == 0 {
            ChannelSessionState::ZeroAndRealtime
        } else {
            ChannelSessionState::SeekAndRealtime
        };

        if live_item {
            let wait_tn = self.timeout_notify.clone();
            loop {
                let now = OffsetDateTime::now_local()? + self.start_time_offset;
                if now >= self.transcoded_until {
                    break;
                }
                let remaining: Duration = (self.transcoded_until - now)
                    .try_into()
                    .unwrap_or(Duration::from_secs(1));
                tokio::select! {
                    _ = tokio::time::sleep(remaining.min(Duration::from_secs(1))) => {}
                    _ = wait_tn.notified() => {
                        return Err(ChannelError::IdleTimeout(
                            self.channel_config.number().to_owned(),
                        ));
                    }
                }
            }
        }

        // the claim was measured at open; everything between it and ffmpeg
        // actually connecting is startup the claim does not cover, and the
        // window's content-to-stamp offset is this gap plus ffmpeg's own
        // connect and keyframe wait. Hand-traced 2026-08-15: 45ms on a late
        // open, 3ms on an early one. This line exists so the instruments can
        // watch the whole distribution instead of two traced events; a value
        // that grows is a variant startup problem coming back
        {
            let now = OffsetDateTime::now_local()? + self.start_time_offset;
            let claim_lag_ms = (now - self.transcoded_until).whole_milliseconds();
            log::info!(
                "variant for item {item_id} begins transcoding {claim_lag_ms}ms past its claimed position"
            );
        }

        let base_offset = Duration::from_millis(pts_offset_ms);

        while self.transcoded_until < item.finish {
            let progress = self.transcoded_until - anchor;
            let pts =
                base_offset + Duration::from_millis(progress.whole_milliseconds().max(0) as u64);

            // a variant failure is terminal: the consumer falls back to the
            // shared feed, which is strictly better than substituted filler
            let (finish, _is_complete) = self
                .transcode_item(&item, true, false, Some(pts), false)
                .await?;

            if finish <= self.transcoded_until {
                return Err(ChannelError::StreamFailure(String::from(
                    "variant transcode made no progress",
                )));
            }

            self.transcoded_until = finish;
            self.state = ChannelSessionState::SeekAndRealtime;
        }

        // let the playlist manager pick up the final segments before exiting
        self.playlist_manager.lock().await.update().await?;

        Ok(())
    }

    /// Serves cohort requests for the lifetime of this session: answering
    /// which cohort a viewer's query resolves to, spawning variant transcodes,
    /// and publishing each cohort's composed playlists.
    ///
    /// Only a shared session does this. A variant session is itself the
    /// product of one, and must never spawn further variants.
    fn spawn_variant_loop(&self) {
        let channel_binary = match std::env::current_exe() {
            Ok(binary) => binary,
            Err(e) => {
                // without our own path there is nothing to spawn variants
                // with, so cohorts quietly keep receiving shared content
                log::warn!("cannot locate the channel binary, disabling stream variants: {e}");
                return;
            }
        };

        let channel = VariantChannel {
            number: self.channel_config.number().to_owned(),
            output_folder: self.channel_config.expanded_output_folder().clone(),
            channel_binary,
            config_json: self.channel_config.merged_source_json(),
            slate_file: slate::slate_file(self.channel_config.expanded_playout_folder()),
        };

        tokio::spawn(async move {
            let variant_loop = tokio::spawn(async move {
                let variants = VariantManager::new();
                loop {
                    variants.tick(&channel).await;
                    tokio::time::sleep(variant_manager::TICK_INTERVAL).await;
                }
            });

            // the loop never returns on its own, so reaching here means it
            // panicked. cohorts stop being served the moment their playlists
            // go stale, but the channel keeps streaming shared content, so
            // nothing else would reveal that this happened
            let Err(e) = variant_loop.await;
            log::error!(
                "stream variant loop stopped: {e}. cohorts on this channel now fall back to shared content"
            );
        });
    }

    /// Publishes the `{query:}` variable names the current playout references
    /// (the parameters that identify a viewer cohort) next to the ready file,
    /// rewriting only when the set changes.
    async fn publish_recognized_params(&mut self) {
        let names = match self
            .playout_loader
            .query_variable_names(&self.transcoded_until)
            .await
        {
            Ok(names) => names.into_iter().collect::<Vec<_>>(),
            Err(e) => {
                log::debug!("failed to collect recognized params: {e}");
                return;
            }
        };

        if self.published_recognized_params.as_ref() == Some(&names) {
            return;
        }

        let path = self
            .channel_config
            .expanded_output_folder()
            .join(ersatztv_core::RECOGNIZED_PARAMS_FILE_NAME);

        match serde_json::to_string(&names) {
            Ok(json) => match tokio::fs::write(&path, json).await {
                Ok(()) => self.published_recognized_params = Some(names),
                Err(e) => log::warn!("failed to publish recognized params: {e}"),
            },
            Err(e) => log::warn!("failed to serialize recognized params: {e}"),
        }
    }

    async fn prep_output_folder(&self, troubleshoot: bool) -> Result<(), ChannelError> {
        let output_folder = self.channel_config.expanded_output_folder();

        if self.ready_file.exists() {
            tokio::fs::remove_file(&self.ready_file)
                .await
                .io_context("remove the stale ready file", &self.ready_file)?;
        }

        if output_folder.exists() {
            if !troubleshoot {
                empty_folder(output_folder)
                    .await
                    .io_context("empty the output folder", output_folder)?;
            }
        } else {
            tokio::fs::create_dir(output_folder)
                .await
                .io_context("create the output folder", output_folder)?;
        }

        Ok(())
    }

    async fn transcode(&mut self, realtime: bool, troubleshoot: bool) -> Result<(), ChannelError> {
        if !realtime {
            log::debug!("channel session will work ahead");

            let next_state = match self.state {
                ChannelSessionState::SeekAndRealtime => ChannelSessionState::SeekAndWorkAhead,
                ChannelSessionState::ZeroAndRealtime => ChannelSessionState::ZeroAndWorkAhead,
                _ => self.state,
            };

            if next_state != self.state {
                log::debug!(
                    "channel session is accelerating {} => {}",
                    self.state,
                    next_state
                );
                self.state = next_state;
            }
        } else {
            log::debug!("channel session will NOT work ahead");

            // throttle to realtime if needed
            let next_state = match self.state {
                ChannelSessionState::SeekAndWorkAhead => ChannelSessionState::SeekAndRealtime,
                ChannelSessionState::ZeroAndWorkAhead => ChannelSessionState::ZeroAndRealtime,
                _ => self.state,
            };

            if next_state != self.state {
                log::debug!(
                    "channel session is throttling {} => {}",
                    self.state,
                    next_state
                );
                self.state = next_state;
            }
        }

        log::debug!("channel session state: {}", self.state);

        // get last pts offset
        let mut pts_time: Option<PtsTime> = None;
        match self.pts_scanner.get_last_pts().await {
            Ok(scanned_pts_time) => pts_time = Some(scanned_pts_time),
            Err(e) => log::debug!("{e}"),
        }

        self.publish_recognized_params().await;

        let mut current_item_result = self
            .playout_loader
            .get_current_item(&self.transcoded_until)
            .await;

        if let Ok(
            item @ PlayoutItem {
                source: Some(PlayoutItemSource::Dynamic { .. }),
                ..
            },
        ) = current_item_result
        {
            current_item_result = self
                .resolve_dynamic_item(&self.transcoded_until, &item)
                .await;
        }

        let current_item = match current_item_result {
            Ok(playout_item) => playout_item,
            Err(ChannelError::PlayoutJsonNoItem { next_start }) => {
                log::debug!(
                    "no playout item covers {}, replacing with black/silence until {}",
                    self.transcoded_until,
                    next_start.map_or_else(
                        || String::from("the next reload"),
                        |start| start.to_string()
                    )
                );
                self.fake_playout_item(next_start)
            }
            Err(err) => {
                log::error!(
                    "no item could be selected for {}, replacing with black/silence: {}",
                    self.transcoded_until,
                    err
                );
                self.fake_playout_item(None)
            }
        };

        // slate-on-shared: a templated window plays configured slate content
        // on the shared session instead of tuning the live source. The item
        // keeps its identity, so the sidecar still declares a templated
        // envelope and cohort viewers still get the live presentation through
        // variant sessions; what changes is only what the shared stream shows
        let mut slate = false;
        let current_item = match Self::item_is_templated(&current_item) {
            true => match self.resolve_slate(&current_item).await {
                Some((slate_source, origin)) => {
                    log::info!(
                        "item {}: shared session plays slate {} from {} for this templated window",
                        current_item.id,
                        slate_label(&slate_source),
                        origin
                    );
                    slate = true;
                    slate_item(current_item, slate_source)
                }
                None => current_item,
            },
            false => {
                // slate answers a templated window and nothing else: an
                // ordinary item already names the media its slot plays, and
                // there is no live source here to stand in for. The key did
                // nothing, so it says so rather than leaving an operator to
                // wonder why the slate never aired
                if let Some(declared) = current_item.slate.as_ref() {
                    log::warn!(
                        "item {} declares slate {} but nothing about it is templated; the slate is ignored and the item plays its own source",
                        current_item.id,
                        slate_label(declared)
                    );
                }
                current_item
            }
        };

        let pts_duration = pts_time.map(|p| p.duration);

        let result = self
            .transcode_item(&current_item, realtime, troubleshoot, pts_duration, slate)
            .await;

        let (finish, is_complete) = match result {
            Ok(ok) => ok,
            Err(e @ ChannelError::IdleTimeout(_)) => return Err(e),
            Err(e @ ChannelError::Stalled(_)) => return Err(e),
            Err(e) if troubleshoot => return Err(e),
            Err(e) => {
                log::error!(
                    "item {} ({} .. {}) failed, replacing with black/silence: {}",
                    current_item.id,
                    current_item.start,
                    current_item.finish,
                    e
                );
                let fake_item = self.fake_playout_item(Some(current_item.finish));
                self.transcode_item(&fake_item, realtime, troubleshoot, pts_duration, false)
                    .await?
            }
        };

        self.transcoded_until = finish;
        log::debug!("transcoded until: {}", self.transcoded_until);

        self.state = Self::next_state(self.state, is_complete);

        Ok(())
    }

    async fn transcode_item(
        &mut self,
        current_item: &PlayoutItem,
        realtime: bool,
        troubleshoot: bool,
        pts_duration: Option<Duration>,
        slate: bool,
    ) -> Result<(OffsetDateTime, bool), ChannelError> {
        // prioritize source from audio tracks, then default source
        let audio_source = Self::resolve_source(current_item, |t| t.audio.as_ref())
            .ok_or(ChannelError::PlayoutJsonAudioSourceRequired)?;

        // prioritize source from video tracks, then default source
        let video_source = Self::resolve_source(current_item, |t| t.video.as_ref())
            .ok_or(ChannelError::PlayoutJsonVideoSourceRequired)?;

        // prioritize source from subtitle tracks, then default source
        let subtitle_source = Self::resolve_source(current_item, |t| t.subtitle.as_ref());

        let audio_source_is_video_source = audio_source == video_source;
        let subtitle_source_is_video_source =
            subtitle_source.as_ref().is_some_and(|s| s == &video_source);

        let audio_input_source = self.playout_source_to_input_source(audio_source.clone())?;
        let video_input_source = if audio_source_is_video_source {
            audio_input_source.clone()
        } else {
            self.playout_source_to_input_source(video_source.clone())?
        };
        let subtitle_input_source = if subtitle_source_is_video_source {
            Some(video_input_source.clone())
        } else {
            subtitle_source
                .clone()
                .and_then(|s| self.playout_source_to_input_source(s.clone()).ok())
        };

        let session: &ChannelSession = self;
        let audio_fut = session.resolve_probe(&audio_source, &audio_input_source);
        let video_fut = async {
            if audio_source_is_video_source {
                Ok::<_, ChannelError>(None)
            } else {
                session
                    .resolve_probe(&video_source, &video_input_source)
                    .await
                    .map(Some)
            }
        };
        let subtitle_fut = async {
            if subtitle_source_is_video_source {
                Ok::<_, ChannelError>(None)
            } else if let (Some(src), Some(s)) =
                (subtitle_source.as_ref(), subtitle_input_source.as_ref())
            {
                session.resolve_probe(src, s).await.map(Some)
            } else {
                Ok(None)
            }
        };

        let graphics_fut = try_join_all(current_item.effective_graphics().enumerate().map(
            |(layer_index, layer)| async move {
                let source = cosmetic_source(layer.source.clone());
                let input_source = session.playout_source_to_input_source(source.clone())?;
                let location = playout_location_to_pipeline(&layer.location);
                let timing = playout_timing_to_pipeline(layer.timing.as_ref());
                let probe_result = session.resolve_probe(&source, &input_source).await?;
                Ok::<_, ChannelError>(GraphicsInput {
                    layer_index,
                    input_source,
                    probe_result,
                    stream_index: layer.stream_index,
                    location,
                    width_percent: layer.width_percent,
                    within_source_content: layer.within_source_content,
                    horizontal_margin_percent: layer.horizontal_margin_percent,
                    vertical_margin_percent: layer.vertical_margin_percent,
                    opacity_percent: layer.opacity_percent,
                    timing,
                })
            },
        ));

        let (audio_probe_result, video_probe_opt, subtitle_probe_opt, graphics_inputs) =
            tokio::try_join!(audio_fut, video_fut, subtitle_fut, graphics_fut)?;

        let video_probe_result = video_probe_opt.unwrap_or_else(|| audio_probe_result.clone());
        let subtitle_probe_result = if subtitle_source_is_video_source {
            Some(video_probe_result.clone())
        } else {
            subtitle_probe_opt
        };

        // consider an item to be live if any of its sources are live;
        // live sources can never seek or work ahead
        let is_live = source_is_live(&video_source) || source_is_live(&audio_source);

        // slate stands in for a templated source, so it must keep the
        // templated envelope contract (sidecar declaration, pts padding)
        // even though the slate source itself is a plain file
        let is_templated =
            slate || source_is_templated(&video_source) || source_is_templated(&audio_source);

        // generate pipeline
        let output_settings = Self::build_output_settings(OutputSettingsPlan {
            channel_config: &self.channel_config,
            accel: self.hw_accel.clone(),
            output_file: self.output_file.clone(),
            output_segment_template: self.output_segment_template.clone(),
            troubleshoot,
            pts_duration,
            realtime,
            slate,
            is_live,
            video_is_still_image: video_probe_result.is_still_image(),
        });

        let start_at_zero = matches!(
            self.state,
            ChannelSessionState::ZeroAndWorkAhead | ChannelSessionState::ZeroAndRealtime
        );

        // measure how far the stamp clock has run past the schedule clock;
        // plan_timings hands it back on this pipeline's output duration.
        // update() first: the previous pipeline's final segments may not have
        // been scanned yet, and an unscanned segment would hide exactly the
        // error this is measuring
        let stamp_error_ms = {
            let mut playlist_manager = self.playlist_manager.lock().await;
            playlist_manager.update().await?;
            Self::stamp_error_ms(
                playlist_manager.last_segment_end(),
                self.transcoded_until,
                self.start_time_offset,
            )
        };
        let PlannedTimings {
            audio: audio_timing,
            video: video_timing,
            subtitle: subtitle_timing,
            declared_duration_ms,
            trim_ms,
        } = Self::plan_timings(TimingPlan {
            current_item,
            audio_source: &audio_source,
            video_source: &video_source,
            subtitle_source: subtitle_source.as_ref(),
            start_at_zero,
            realtime,
            slate,
            is_live,
            is_templated,
            transcoded_until: self.transcoded_until,
            stamp_error_ms,
        });
        if trim_ms != 0 {
            log::debug!(
                "emission trim {trim_ms}ms for item {} (stamp clock is {stamp_error_ms}ms past the schedule)",
                current_item.id
            );
        }

        let video_index = current_item
            .tracks
            .as_ref()
            .and_then(|t| t.video.as_ref())
            .and_then(|v| v.stream_index);

        let audio_index = current_item
            .tracks
            .as_ref()
            .and_then(|t| t.audio.as_ref())
            .and_then(|a| a.stream_index);

        let subtitle_index = current_item
            .tracks
            .as_ref()
            .and_then(|t| t.subtitle.as_ref())
            .and_then(|s| s.stream_index);

        let subtitle_input = match (
            subtitle_probe_result.clone(),
            subtitle_input_source,
            subtitle_timing,
        ) {
            (Some(s_probe), Some(s_in), Some(s_time)) => Some(ProbedInput {
                input_source: s_in,
                in_point: s_time.in_point,
                out_point: s_time.out_point,
                probe_result: s_probe,
                stream_index: subtitle_index,
                loop_when_exhausted: false,
            }),
            _ => None,
        };

        // every input is read once through, which is what the schedule asked
        // for; the slate window then says otherwise just below
        let mut input_settings = InputSettings {
            start: current_item.start,
            playout_offset: if start_at_zero {
                Duration::ZERO
            } else {
                Duration::from_millis(
                    (self.transcoded_until - current_item.start)
                        .whole_milliseconds()
                        .max(0) as u64,
                )
            },
            audio_input: ProbedInput {
                input_source: audio_input_source,
                in_point: audio_timing.in_point,
                out_point: audio_timing.out_point,
                probe_result: audio_probe_result.clone(),
                stream_index: audio_index,
                loop_when_exhausted: false,
            },
            video_input: ProbedInput {
                input_source: video_input_source,
                in_point: if video_probe_result.is_still_image() {
                    Duration::ZERO
                } else {
                    video_timing.in_point
                },
                out_point: video_timing.out_point,
                probe_result: video_probe_result.clone(),
                stream_index: video_index,
                loop_when_exhausted: false,
            },
            subtitle_input,
            graphics_inputs,
        };
        repeat_media_inputs_for_slate(&mut input_settings, slate);

        let mut subtitle_source: Option<SubtitleSource> = None;
        if output_settings.subtitle_mode == SubtitleMode::Convert
            && let Some(subtitle_stream) = input_settings.select_subtitle_stream()
            && !subtitle_stream.is_subtitle_image()
            && let Some(input) = input_settings.subtitle_input.as_ref()
            && let Some(cues) = match &self.cached_subtitles {
                Some((id, c)) if id == &current_item.id => Some(Arc::clone(c)),
                _ => {
                    self.extract_and_convert_subs(input, subtitle_stream, current_item)
                        .await
                }
            }
        {
            subtitle_source = Some(SubtitleSource {
                cues,
                cursor: 0,
                next_segment_source_offset: input.in_point,
            });
        }

        let skip_embedded_text_subtitles = output_settings.subtitle_mode == SubtitleMode::Burn
            && input_settings
                .subtitle_input
                .as_ref()
                .is_some_and(|i| i.probe_result.streams.len() > 1)
            && input_settings
                .select_subtitle_stream()
                .is_some_and(|s| !s.is_subtitle_image());

        if skip_embedded_text_subtitles {
            log::warn!(
                "skipping embedded text subtitles for item {}; scheduler must extract to a sidecar file",
                current_item.id
            );
            input_settings.subtitle_input = None;
        }

        let pts_offset = output_settings.pts_offset;
        let mut pipeline_result =
            pipeline::generate_pipeline(&self.ffmpeg_info, input_settings, output_settings)?;
        pipeline_result.optimize();
        let args = pipeline_result.args();
        let envs = pipeline_result.envs();
        log::debug!("optimized pipeline: {}", args.join(" "));

        self.playlist_manager
            .lock()
            .await
            .before_new_pipeline(
                pts_offset,
                subtitle_source,
                &current_item.id,
                declared_duration_ms,
                is_templated,
                slate,
            )
            .await?;

        // stream current item
        let mut ffmpeg_child = tokio::process::Command::new(&self.ffmpeg_path)
            .args(args.iter().map(Cow::as_ref))
            .envs(
                envs.iter()
                    .map(|env| (env.key.as_str(), env.value.as_str())),
            )
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            // a worker that dies takes its transcoder with it: an orphaned
            // ffmpeg keeps writing segments into a folder a replacement
            // worker now owns
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| {
                ChannelError::StreamFailure(format!(
                    "failed to spawn ffmpeg {}: {e}",
                    self.ffmpeg_path.display()
                ))
            })?;

        let stderr = ffmpeg_child
            .stderr
            .take()
            .ok_or(ChannelError::CaptureFFmpegStderrFailure)?;
        let ring = Arc::new(std::sync::Mutex::new(VecDeque::<String>::with_capacity(
            STDERR_RING_LINES,
        )));

        let reader_ring = Arc::clone(&ring);
        let reader_handle = tokio::spawn(async move {
            let mut lines = tokio::io::BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                eprintln!("{line}");
                if let Ok(mut buf) = reader_ring.lock() {
                    if buf.len() == STDERR_RING_LINES {
                        buf.pop_front();
                    }
                    buf.push_back(line);
                }
            }
        });

        log::debug!("waiting for ffmpeg to terminate...");

        tokio::select! {
            status = ffmpeg_child.wait() => {
                let status = status.map_err(|e| ChannelError::StreamFailure(e.to_string()))?;
                let _ = reader_handle.await;
                if !status.success() {
                    self.write_dossier(                        current_item,
                        &video_probe_result,                        &audio_probe_result,
                        subtitle_probe_result.as_ref(),
                        &ring,
                        format!("ffmpeg exited with code {status}")).await;
                    return Err(ChannelError::StreamFailure(format!(
                        "ffmpeg exited {status}"
                    )));
                } else if troubleshoot {
                    self.write_dossier(current_item, &video_probe_result,
                        &audio_probe_result, subtitle_probe_result.as_ref(),
                        &ring, "ffmpeg exited successfully".to_string()).await;
                } else {
                    self.cleanup_old_report().await;
                }
            }
            _ = self.timeout_notify.notified() => {
                ffmpeg_child.kill().await.ok();
                let _ = reader_handle.await;
                self.cleanup_old_report().await;
                return Err(ChannelError::IdleTimeout(self.channel_config.number().to_owned()));
            }
            _ = async {
                    loop {
                        let playlist_manager = self.playlist_manager.lock().await;
                        if OffsetDateTime::now_utc() - *playlist_manager.last_progress() > STALL_THRESHOLD {
                            break;
                        }
                        drop(playlist_manager);
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                } => {
                ffmpeg_child.kill().await.ok();
                let _ = reader_handle.await;
                self.write_dossier(current_item, &video_probe_result, &audio_probe_result,
                    subtitle_probe_result.as_ref(), &ring, "ffmpeg stalled".to_string()).await;
                return Err(ChannelError::Stalled(self.channel_config.number().to_owned()));
            }
        }

        let finish = std::cmp::min(audio_timing.finish, video_timing.finish);
        let is_complete = is_live || (audio_timing.is_complete && video_timing.is_complete);

        Ok((finish, is_complete))
    }

    fn next_state(state: ChannelSessionState, is_complete: bool) -> ChannelSessionState {
        let result = match state {
            // after seeking and NOT completing the item, seek again,
            // transcode will accelerate if needed
            ChannelSessionState::SeekAndWorkAhead if !is_complete => {
                ChannelSessionState::SeekAndRealtime
            }

            // after seeking and completing the item, start at zero
            ChannelSessionState::SeekAndWorkAhead => ChannelSessionState::ZeroAndWorkAhead,

            // after starting at zero and NOT completing the item, seek,
            // transcode will accelerate if needed
            ChannelSessionState::ZeroAndWorkAhead if !is_complete => {
                ChannelSessionState::SeekAndRealtime
            }

            // after starting at zero and completing the item, start at zero again,
            // transcode method will throttle if needed
            ChannelSessionState::ZeroAndWorkAhead => ChannelSessionState::ZeroAndWorkAhead,

            // realtime will always complete items, so start next at zero
            ChannelSessionState::SeekAndRealtime => ChannelSessionState::ZeroAndRealtime,

            // realtime will always complete items, so start next at zero
            ChannelSessionState::ZeroAndRealtime => ChannelSessionState::ZeroAndRealtime,
        };

        log::debug!("channel session state {} => {}", state, result);

        result
    }

    async fn resolve_probe(
        &self,
        src: &PlayoutItemSource,
        input: &InputSource,
    ) -> Result<ProbeResult, ChannelError> {
        match src.probe_hint() {
            Some(hint) => {
                let path = input.input_path().ok_or(ChannelError::ProbeHintFailure)?;
                Ok(probe_hint_to_result(hint, path))
            }
            None => self.probe_source(input).await,
        }
    }

    async fn probe_source(&self, source: &InputSource) -> Result<ProbeResult, ChannelError> {
        let probe_deps = probe::ProbeDeps {
            ffprobe_path: &self.ffprobe_path,
            ffmpeg_path: &self.ffmpeg_path,
        };

        Ok(source.probe(&probe_deps).await?)
    }

    /// Expands `{channel_number}` and `{query:name|default}` variables in a
    /// source URL. Query values arrive only in variant sessions; the shared
    /// channel session resolves every `query:` variable to its default.
    fn expand_stream_variables_url(&self, uri: &str) -> String {
        ersatztv_playout::stream_variables::expand_url(
            uri,
            Some(self.channel_config.number()),
            &self.query_parameters,
        )
    }

    fn playout_source_to_input_source(
        &self,
        source: PlayoutItemSource,
    ) -> Result<InputSource, ChannelError> {
        match source {
            PlayoutItemSource::Local { path, .. } => {
                Ok(InputSource::Local(LocalInputSource { path }))
            }
            PlayoutItemSource::Lavfi { params, .. } => {
                Ok(InputSource::Lavfi(LavfiInputSource { params }))
            }
            PlayoutItemSource::Http {
                uri,
                headers,
                user_agent,
                timeout_us,
                reconnect,
                reconnect_delay_max,
                keep_alive,
                ..
            } => {
                let expanded_uri = self.expand_stream_variables_url(&expand_template(&uri)?);
                let expanded_headers: Vec<String> = headers
                    .unwrap_or_default()
                    .iter()
                    .map(|h| expand_template(h))
                    .collect::<Result<Vec<_>, _>>()?;
                let expanded_ua = user_agent.as_deref().map(expand_template).transpose()?;

                Ok(InputSource::Http(HttpInputSource {
                    uri: expanded_uri,
                    options: HttpInputOptions {
                        headers: expanded_headers,
                        user_agent: expanded_ua,
                        timeout_us,
                        reconnect: reconnect.unwrap_or(true),
                        reconnect_delay_max,
                        keep_alive,
                    },
                }))
            }
            PlayoutItemSource::Rtsp {
                uri, timeout_us, ..
            } => {
                let expanded_uri = self.expand_stream_variables_url(&expand_template(&uri)?);

                Ok(InputSource::Rtsp(RtspInputSource {
                    uri: expanded_uri,
                    options: RtspInputOptions { timeout_us },
                }))
            }
            PlayoutItemSource::Script { command, args, .. } => {
                let url = self.local_proxy_server.register_script(ScriptCommand {
                    command: expand_template(&command)?,
                    args: args
                        .iter()
                        .map(|a| expand_template(a))
                        .collect::<Result<_, _>>()?,
                })?;
                Ok(InputSource::Http(HttpInputSource {
                    uri: url,
                    options: HttpInputOptions {
                        reconnect: false,
                        ..Default::default()
                    },
                }))
            }
            PlayoutItemSource::Dynamic { .. } => {
                Err(ChannelError::DynamicSourceCannotBePlayedDirectly)
            }
        }
    }

    /// The timing decision, split out from the session so it can be tested.
    /// `transcoded_until` was the only session state it ever read.
    ///
    /// The live branch below is load bearing beyond its own correctness: it is
    /// what keeps a non-zero `progress_ms` from turning into an input seek.
    /// Such a progress selects `SeekAndRealtime`, which makes `start_at_zero`
    /// false, which would otherwise derive `in_point` from `transcoded_until`.
    /// That is output offset driving input seeking, the pattern that closed
    /// PR #187. Live sources return `in_point: ZERO` before any of that is
    /// consulted, and every templated source is live (9716 of 9716 across the
    /// install on 2026-08-14), so the rule holds. It holds because of this
    /// branch, not because of the data, and there is now a test on it.
    fn input_timing_at(
        current_item: &PlayoutItem,
        source: &PlayoutItemSource,
        start_at_zero: bool,
        realtime: bool,
        is_live: bool,
        transcoded_until: OffsetDateTime,
    ) -> TimingResult {
        let mut is_complete = true;

        let item_start = current_item.start;
        let item_finish = current_item.finish;
        let item_duration = current_item.finish - current_item.start;
        let item_slot_ms = item_duration.whole_milliseconds().max(0) as u64;
        let item_in_point_base_ms = match source {
            PlayoutItemSource::Local { in_point_ms, .. }
            | PlayoutItemSource::Http { in_point_ms, .. } => in_point_ms.unwrap_or(0),
            _ => 0,
        };

        // live content never seeks. limit it to the remaining schedule interval
        // so pipeline duration and graphics timing end at the same point.
        if is_live {
            let live_now = if start_at_zero {
                item_start
            } else {
                transcoded_until
            };
            let remaining = item_finish - live_now;

            return TimingResult {
                in_point: Duration::ZERO,
                out_point: Duration::from_millis(remaining.whole_milliseconds().max(0) as u64),
                finish: item_finish,
                is_complete: true,
            };
        }

        let explicit_out_point_ms = match source {
            PlayoutItemSource::Local { out_point_ms, .. }
            | PlayoutItemSource::Http { out_point_ms, .. } => *out_point_ms,
            _ => None,
        };
        let (item_out_point_ms, overrun_ms) =
            effective_out_point_ms(explicit_out_point_ms, item_in_point_base_ms, item_slot_ms);
        if overrun_ms > 0 {
            log::warn!(
                "item {} out_point overruns its {}ms slot by {}ms; clamping to the slot",
                current_item.id,
                item_slot_ms,
                overrun_ms
            );
        }

        let effective_now = if start_at_zero {
            item_start
        } else {
            transcoded_until
        };

        // the live guard used to be repeated here, upstream's copy sitting
        // below the fork's. It was unreachable, because the branch above
        // returns for every live source before this point, and it was the
        // weaker of the two: it read `transcoded_until` raw where the branch
        // above clamps it into the item. Two guards also meant neither could
        // be pinned by a test, since deleting either one left the other
        // covering for it.

        let progress_ms = if start_at_zero {
            0
        } else {
            (effective_now - item_start).whole_milliseconds().max(0) as u64
        };
        let effective_in_point = Duration::from_millis(item_in_point_base_ms + progress_ms);

        let duration =
            Duration::from_millis((item_finish - effective_now).whole_milliseconds() as u64);

        let limit = if realtime {
            Duration::ZERO
        } else {
            Duration::from_secs(SEGMENT_SECONDS as u64 * 11u64)
        };

        let mut finish = item_finish;
        let mut out_point = Duration::from_millis(item_out_point_ms);

        if limit > Duration::ZERO && duration > limit {
            finish = effective_now + limit;
            out_point = effective_in_point + limit;
            is_complete = false;
        }

        TimingResult {
            in_point: effective_in_point,
            out_point,
            finish,
            is_complete,
        }
    }

    /// How far the stamp clock has run past the schedule clock, in
    /// milliseconds.
    ///
    /// The two clocks are seeded from the same reading at channel start, but
    /// `transcoded_until` also carries `start_time_offset`, the distance to a
    /// configured `virtual_start`. That offset is deliberate and permanent, so
    /// it has to be added back before the two can be compared.
    ///
    /// Subtracting them raw would report the whole virtual start offset as
    /// error. Since the correction below drives that error toward zero, a
    /// channel with `virtual_start` set would have its content trimmed or
    /// padded by the clamp on every item until the offset closed.
    fn stamp_error_ms(
        last_segment_end: OffsetDateTime,
        transcoded_until: OffsetDateTime,
        start_time_offset: time::Duration,
    ) -> i64 {
        (last_segment_end + start_time_offset - transcoded_until).whole_milliseconds() as i64
    }

    /// How much of this pipeline's output duration to give back to the
    /// schedule, in milliseconds. Positive shortens the pipeline's -t,
    /// negative lengthens it.
    ///
    /// `stamp_error_ms` is the same timeline position read on the stamp clock
    /// and on the schedule clock, with the virtual start offset taken back out.
    /// The -t cut is frame-quantized upward (the frame straddling the cut is
    /// emitted whole), so with every pipeline padded to its clamp each item
    /// emits up to one frame more than its slot, and the stamp clock
    /// integrates that forever: +531ms/hour on ch11, +262ms/hour on ch13,
    /// live on 2026-08-14/15. Handing the measured error back to the next
    /// pipeline's output duration bounds it at about one frame instead.
    ///
    /// Templated items are exempt: a variant transcode must fill exactly the
    /// envelope the shared session declares, so their -t stays a pure
    /// function of the item. They cost nothing to exempt; their slate slots
    /// are frame-aligned and contribute no quantization error.
    ///
    /// The correction is clamped so a wild clock (a failed pipeline, a
    /// corrupted playlist) slews back over several items instead of opening
    /// one large hole, and it never eats more than half the pipeline it is
    /// applied to.
    fn emission_trim_ms(stamp_error_ms: i64, pipeline_ms: u64, is_templated: bool) -> i64 {
        const MAX_CORRECTION_MS: i64 = 500;
        if is_templated {
            return 0;
        }
        stamp_error_ms
            .clamp(-MAX_CORRECTION_MS, MAX_CORRECTION_MS)
            .min((pipeline_ms / 2) as i64)
    }

    /// Applies an emission trim to one input's timing. Only the emitted
    /// duration moves: `in_point` (where reading starts) and `finish` (how
    /// far the schedule advances) are schedule-derived and stay untouched,
    /// which is what keeps this on the right side of the PR #187 line.
    fn apply_emission_trim(timing: TimingResult, trim_ms: i64) -> TimingResult {
        let out_point = if trim_ms >= 0 {
            timing
                .out_point
                .saturating_sub(Duration::from_millis(trim_ms as u64))
        } else {
            timing.out_point + Duration::from_millis(trim_ms.unsigned_abs())
        };
        TimingResult {
            out_point,
            ..timing
        }
    }

    /// The output side of the pipeline as a pure function of plain inputs,
    /// split out of `transcode_item` so the decisions in it can be pinned by
    /// tests. `transcode_item` launches a real ffmpeg, so while a decision
    /// lived inline there, reverting it failed nothing: that is how the
    /// 2026-08-14 padding regression shipped, and the drift meter in
    /// production was the first thing able to see it.
    fn build_output_settings(plan: OutputSettingsPlan) -> OutputSettings {
        let audio_norm = &plan.channel_config.normalization.audio;
        let video_norm = &plan.channel_config.normalization.video;

        let video_size = match (video_norm.width, video_norm.height) {
            (Some(width), Some(height)) => Some(FrameSize { width, height }),
            _ => None,
        };

        OutputSettings {
            audio: AudioOutputSettings {
                format: audio_norm.format.clone().map(AudioFormat::from),
                bitrate: audio_norm.bitrate_kbps.map(Kbps),
                buffer: audio_norm.buffer_kbps.map(Kbps),
                channels: audio_norm.channels,
                sample_rate: audio_norm.sample_rate_hz.map(Hz),
                loudness: if audio_norm.normalize_loudness {
                    Some(
                        audio_norm
                            .loudness
                            .as_ref()
                            .map(|l| l.into())
                            .unwrap_or_default(),
                    )
                } else {
                    None
                },
            },
            video_format: video_norm.format.clone().map(VideoFormat::from),
            bit_depth: video_norm.bit_depth,
            video_bitrate: video_norm.bitrate_kbps.map(Kbps),
            video_buffer: video_norm.buffer_kbps.map(Kbps),
            video_size,
            scaling_mode: video_norm.scaling_mode.into(),
            filter_options: video_norm.filters.clone().into(),
            deinterlace: video_norm.deinterlace,
            accel: plan.accel,
            format: ffpipeline::output_format::OutputFormat::Hls {
                playlist: plan.output_file,
                segment_template: plan.output_segment_template,
                troubleshoot: plan.troubleshoot,
            },
            pts_offset: plan.pts_duration.map(|duration| PtsOffset { duration }),
            // Two jobs. A templated item may be transcoded in parallel by
            // variant sessions with different query values; padding both
            // transcodes to the -t clamp keeps their PTS envelopes identical,
            // so one can be substituted for the other at the playlist layer.
            //
            // Every other item needs it too. A file whose video stream ends
            // before its container does books more slot than the video can
            // fill, and the shortfall is lost permanently because
            // last_segment_end only advances by emitted EXTINF; about 20% of
            // the bumps library is built that way. ch13 had looked immune only
            // because its watermark input was looped and clamped to the item's
            // -t, which incidentally held the pipeline open; upstream #211
            // made a still image a single frame, so that masking is gone.
            //
            // Unconditional padding alone RAN THE TIMELINE LONG at +531ms/hour
            // (2026-08-14 overnight, reverted that morning): with tpad the
            // video never reaches EOF, so the -t cut decides the emitted
            // duration, and that cut is frame-quantized UPWARD because the
            // frame straddling it is emitted whole. Every item whose slot is
            // not frame-aligned emits ceil(slot * fps) / fps, up to one frame
            // long, verified per-item against the drift meter to 0.2ms mean
            // error. The emission trim (emission_trim_ms, applied by
            // plan_timings) is what makes this flag safe: it measures the
            // accumulated stamp-clock error and hands it back on the next
            // pipeline's -t, so the quantization can no longer integrate. Do
            // not set this to a condition again; the trim assumes every
            // non-templated pipeline is padded, because only a padded
            // pipeline can EXTEND to cover a negative error.
            pad_to_duration: true,
            // Slate paces like every other pipeline. It ran unpaced for
            // months on the strength of a 0.65x padded-under-pacing
            // measurement that was withdrawn on 2026-08-14 (it was one
            // oversized file failing hardware decode, not the padding), and
            // since 2026-08-15 every production pipeline runs padded and
            // paced at 1x, which is the same combination at far larger
            // scale. If slate pacing ever regresses, the symptom is the
            // transcoded buffer shrinking during templated windows.
            realtime: plan.realtime,
            is_live: plan.is_live,
            frame_rate: if plan.video_is_still_image {
                Some(FrameRate::default())
            } else {
                None
            },
            subtitle_mode: plan.channel_config.normalization.subtitle.mode.into(),
            fonts_folder: plan
                .channel_config
                .normalization
                .subtitle
                .fonts_folder
                .clone(),
            subtitle_force_style: plan
                .channel_config
                .normalization
                .subtitle
                .force_style
                .clone(),
            reports_folder: plan.channel_config.ffmpeg.reports_folder.clone(),
            report_id: Some(plan.channel_config.number().to_owned()),
        }
    }

    /// The input timings and declared envelope for one pipeline, as a pure
    /// function of plain inputs. Same seam and same reason as
    /// [`Self::build_output_settings`]: the emission trim wiring lived
    /// inline in `transcode_item`, where no test could observe whether it
    /// was actually applied to what the -t consumes.
    fn plan_timings(plan: TimingPlan) -> PlannedTimings {
        // a slate item must fill its whole remaining window in one pipeline
        // invocation: work-ahead chunking would declare one sidecar envelope
        // per chunk, and a variant reading the first chunk's duration would
        // mistake it for a mid-item join. input_timing_at's realtime flag
        // only controls that chunking, so slate forces it; output pacing
        // (build_output_settings) still follows the real `realtime`
        let whole_window = plan.realtime || plan.slate;

        let audio = Self::input_timing_at(
            plan.current_item,
            plan.audio_source,
            plan.start_at_zero,
            whole_window,
            plan.is_live,
            plan.transcoded_until,
        );
        let video = Self::input_timing_at(
            plan.current_item,
            plan.video_source,
            plan.start_at_zero,
            whole_window,
            plan.is_live,
            plan.transcoded_until,
        );
        let subtitle = plan.subtitle_source.map(|s| {
            Self::input_timing_at(
                plan.current_item,
                s,
                plan.start_at_zero,
                whole_window,
                plan.is_live,
                plan.transcoded_until,
            )
        });

        let pipeline_ms = std::cmp::min(
            audio.out_point.saturating_sub(audio.in_point),
            video.out_point.saturating_sub(video.in_point),
        )
        .as_millis() as u64;
        let trim_ms = Self::emission_trim_ms(plan.stamp_error_ms, pipeline_ms, plan.is_templated);
        let audio = Self::apply_emission_trim(audio, trim_ms);
        let video = Self::apply_emission_trim(video, trim_ms);

        // the envelope this pipeline will fill, computed the same way the
        // pipeline computes its own -t. A variant of this item has to fill
        // the same range, and cannot work it out from the item alone once
        // this session has joined the item partway through
        let declared_duration_ms = std::cmp::min(
            audio.out_point.saturating_sub(audio.in_point),
            video.out_point.saturating_sub(video.in_point),
        )
        .as_millis() as u64;

        PlannedTimings {
            audio,
            video,
            subtitle,
            declared_duration_ms,
            trim_ms,
        }
    }

    /// Whether any of the item's resolved sources is templated, judged on
    /// the original playout item before any slate substitution.
    fn item_is_templated(item: &PlayoutItem) -> bool {
        Self::resolve_source(item, |t| t.video.as_ref())
            .as_ref()
            .is_some_and(source_is_templated)
            || Self::resolve_source(item, |t| t.audio.as_ref())
                .as_ref()
                .is_some_and(source_is_templated)
    }

    /// The slate this templated window plays on the shared session, and the
    /// configuration it came from.
    ///
    /// The side file is read only when the item declares no slate of its
    /// own: a file that is never read cannot warn about a channel-wide
    /// setting this window was never going to use.
    async fn resolve_slate(&self, item: &PlayoutItem) -> Option<(PlayoutItemSource, SlateOrigin)> {
        let item_slate = usable_item_slate(item).await;
        let side_file_slate = match item_slate {
            Some(_) => None,
            None => self.load_slate_path().await,
        };
        choose_slate(item_slate, side_file_slate)
    }

    /// The configured slate for this channel's templated windows, if any,
    /// with the file that configured it so a log line can name it.
    ///
    /// The side file ([`slate::SlateConfig`], next to the playout folder)
    /// predates the schedule carrying slate, and outlives it: legacy pipes
    /// the channel config over stdin and rebuilds it per session, so a file
    /// the operator owns is the one place a channel-wide setting survives in
    /// both deployment shapes, and it still answers for every templated
    /// window a schedule says nothing about. Re-read at every templated
    /// window, so slate can be added or removed without a restart. Only
    /// `path` matters here; the variant manager owns the `default` key on
    /// its own cadence.
    async fn load_slate_path(&self) -> Option<(String, PathBuf)> {
        let file = slate::slate_file(self.channel_config.expanded_playout_folder())?;
        let path = match slate::read_slate_file(&file).await {
            SlateFile::Missing => None,
            SlateFile::Malformed(err) => {
                log::warn!("ignoring {}: {err}", file.display());
                None
            }
            SlateFile::Present(config) => config.path,
        }?;
        if tokio::fs::metadata(&path).await.is_err() {
            log::warn!(
                "slate {} configured in {} does not exist; tuning the live source instead",
                path,
                file.display()
            );
            return None;
        }
        Some((path, file))
    }

    fn fake_playout_item(&self, next_start: Option<OffsetDateTime>) -> PlayoutItem {
        let width = self
            .channel_config
            .normalization
            .video
            .width
            .unwrap_or(1920);

        let height = self
            .channel_config
            .normalization
            .video
            .height
            .unwrap_or(1080);

        let duration = Duration::from_mins(1);

        PlayoutItem {
            id: uuid::Uuid::new_v4().to_string(),
            start: self.transcoded_until,
            finish: next_start.unwrap_or(self.transcoded_until + duration),
            source: None,
            tracks: Some(PlayoutItemTracks {
                audio: Some(TrackSelection {
                    source: Some(PlayoutItemSource::Lavfi {
                        params: String::from("anullsrc=channel_layout=stereo:sample_rate=48000"),
                        probe_hint: Some(ProbeHint {
                            video: Vec::new(),
                            audio: vec![AudioHint {
                                stream_index: 0,
                                codec: String::from("pcm_s16le"),
                                channels: 2,
                            }],
                            subtitle: Vec::new(),
                            format_name: Some(String::from("mpegts")),
                            duration_ms: Some(duration.as_millis() as u64),
                        }),
                    }),
                    stream_index: None,
                }),
                video: Some(TrackSelection {
                    source: Some(PlayoutItemSource::Lavfi {
                        params: format!("color=c=black:s={}x{}", width, height),
                        probe_hint: Some(ProbeHint {
                            video: vec![VideoHint::new(
                                String::from("rawvideo"),
                                width,
                                height,
                                String::from("yuv420p"),
                            )],
                            audio: Vec::new(),
                            subtitle: Vec::new(),
                            format_name: Some(String::from("mpegts")),
                            duration_ms: Some(duration.as_millis() as u64),
                        }),
                    }),
                    stream_index: None,
                }),
                subtitle: None,
            }),
            slate: None,
            watermark: None,
            graphics: Vec::new(),
        }
    }

    async fn resolve_dynamic_item(
        &self,
        start: &OffsetDateTime,
        dynamic_item: &PlayoutItem,
    ) -> Result<PlayoutItem, ChannelError> {
        let Some(PlayoutItemSource::Dynamic {
            uri,
            headers,
            user_agent,
            timeout_us,
        }) = &dynamic_item.source
        else {
            return Err(ChannelError::DynamicSourceRequired);
        };

        let expanded_uri = expand_template(uri)?;
        let expanded_headers: Vec<String> = headers
            .iter()
            .flatten()
            .map(|h| expand_template(h))
            .collect::<Result<Vec<_>, _>>()?;
        let expanded_ua = user_agent.as_deref().map(expand_template).transpose()?;

        let mut header_map = HeaderMap::new();
        for h in &expanded_headers {
            let Some((name, value)) = h.split_once(':') else {
                continue;
            };
            let name = HeaderName::from_bytes(name.trim().as_bytes())
                .map_err(|e| ChannelError::DynamicSourceFailure(format!("bad header name: {e}")))?;
            let value = HeaderValue::from_str(value.trim()).map_err(|e| {
                ChannelError::DynamicSourceFailure(format!("bad header value: {e}"))
            })?;
            header_map.insert(name, value);
        }
        if let Some(ua) = expanded_ua.as_deref() {
            header_map.insert(
                USER_AGENT,
                HeaderValue::from_str(ua).map_err(|e| {
                    ChannelError::DynamicSourceFailure(format!("bad user agent: {e}"))
                })?,
            );
        }

        header_map.insert(
            HeaderName::from_static("x-etv-dynamic-id"),
            HeaderValue::from_str(&dynamic_item.id).map_err(|e| {
                ChannelError::DynamicSourceFailure(format!("bad dynamic source id {e}"))
            })?,
        );

        header_map.insert(
            HeaderName::from_static("x-etv-channel"),
            HeaderValue::from_str(self.channel_config.number()).map_err(|e| {
                ChannelError::DynamicSourceFailure(format!("bad channel number {e}"))
            })?,
        );

        header_map.insert(
            HeaderName::from_static("x-etv-now"),
            HeaderValue::from_str(&start.format(&time::format_description::well_known::Rfc3339)?)
                .map_err(|e| ChannelError::DynamicSourceFailure(format!("bad time value: {e}")))?,
        );

        header_map.insert(
            HeaderName::from_static("x-etv-until"),
            HeaderValue::from_str(
                &dynamic_item
                    .finish
                    .format(&time::format_description::well_known::Rfc3339)?,
            )
            .map_err(|e| ChannelError::DynamicSourceFailure(format!("bad time value: {e}")))?,
        );

        let timeout = timeout_us
            .map(Duration::from_micros)
            .unwrap_or_else(|| Duration::from_secs(10));

        let mut item: PlayoutItem = self
            .dynamic_http_client
            .get(&expanded_uri)
            .headers(header_map)
            .timeout(timeout)
            .send()
            .await
            .map_err(|e| ChannelError::DynamicSourceFailure(e.to_string()))?
            .error_for_status()
            .map_err(|e| ChannelError::DynamicSourceFailure(e.to_string()))?
            .json()
            .await
            .map_err(|e| ChannelError::DynamicSourceFailure(e.to_string()))?;

        // always start at the requested time
        if item.start != *start {
            let duration = item.finish - item.start;
            item.start = *start;
            item.finish = *start + duration;
        }

        // always clamp the finish time
        if item.finish > dynamic_item.finish {
            item.finish = dynamic_item.finish;
        }

        if item.finish <= item.start {
            return Err(ChannelError::DynamicSourceNoRemainingTime);
        }

        if let Some(PlayoutItemSource::Dynamic { .. }) = item.source {
            return Err(ChannelError::DynamicSourceCannotRecurse);
        }

        if let Some(tracks) = &item.tracks {
            if let Some(audio) = &tracks.audio
                && let Some(PlayoutItemSource::Dynamic { .. }) = audio.source
            {
                return Err(ChannelError::DynamicSourceCannotRecurse);
            }

            if let Some(video) = &tracks.video
                && let Some(PlayoutItemSource::Dynamic { .. }) = video.source
            {
                return Err(ChannelError::DynamicSourceCannotRecurse);
            }

            if let Some(subtitle) = &tracks.subtitle
                && let Some(PlayoutItemSource::Dynamic { .. }) = subtitle.source
            {
                return Err(ChannelError::DynamicSourceCannotRecurse);
            }
        }

        if let Some(watermark) = &item.watermark
            && let PlayoutItemSource::Dynamic { .. } = watermark.source
        {
            return Err(ChannelError::DynamicSourceCannotRecurse);
        }

        if item
            .graphics
            .iter()
            .any(|layer| matches!(layer.source, PlayoutItemSource::Dynamic { .. }))
        {
            return Err(ChannelError::DynamicSourceCannotRecurse);
        }

        Ok(item)
    }

    fn resolve_source<F>(item: &PlayoutItem, pick: F) -> Option<PlayoutItemSource>
    where
        F: FnOnce(&PlayoutItemTracks) -> Option<&TrackSelection>,
    {
        item.tracks
            .as_ref()
            .and_then(pick)
            .and_then(|sel| sel.source.clone())
            .or_else(|| item.source.clone())
    }

    async fn extract_and_convert_subs(
        &mut self,
        input: &ProbedInput,
        subtitle_stream: &ProbeResultVideoStream,
        current_item: &PlayoutItem,
    ) -> Option<Arc<Vec<Cue>>> {
        {
            match ffpipeline::web_vtt::convert_to_vtt(&self.ffmpeg_path, input, subtitle_stream)
                .await
            {
                Ok(temp_file) => match ffpipeline::web_vtt::parse_file(temp_file.path()).await {
                    Ok(extracted_cues) => {
                        let arc = Arc::new(extracted_cues);
                        self.cached_subtitles = Some((current_item.id.clone(), Arc::clone(&arc)));
                        Some(arc)
                    }
                    Err(err) => {
                        log::warn!("error parsing converted vtt: {err}");
                        None
                    }
                },
                Err(err) => {
                    log::warn!("error converting subtitle to vtt: {err}");
                    None
                }
            }
        }
    }

    async fn cleanup_old_report(&self) {
        if let Some(reports_folder) = &self.channel_config.ffmpeg.reports_folder {
            let report_file = PathBuf::from(reports_folder)
                .join(format!(".in-flight-{}.log", self.channel_config.number()));
            if report_file.exists() {
                let _ = tokio::fs::remove_file(report_file).await;
            }
        }
    }

    async fn write_dossier(
        &self,
        current_item: &PlayoutItem,
        video_probe_result: &ProbeResult,
        audio_probe_result: &ProbeResult,
        subtitle_probe_result: Option<&ProbeResult>,
        ring: &Arc<std::sync::Mutex<VecDeque<String>>>,
        outcome: String,
    ) {
        let stderr_tail: Vec<_> = ring
            .lock()
            .map(|r| r.iter().cloned().collect())
            .unwrap_or_default();

        let mut builder = DossierBuilder::new(&self.channel_config, &self.ffmpeg_info)
            .item(current_item)
            .stderr(stderr_tail)
            .video(video_probe_result)
            .audio(audio_probe_result)
            .outcome(outcome);

        if let Some(accel) = &self.hw_accel {
            builder = builder.accel(accel);
        }

        if let Some(subtitle_probe_result) = subtitle_probe_result {
            builder = builder.subtitle(subtitle_probe_result);
        }

        if let Some(report_source_file) =
            self.channel_config
                .ffmpeg
                .reports_folder
                .as_ref()
                .map(|folder| {
                    PathBuf::from(folder)
                        .join(format!(".in-flight-{}.log", self.channel_config.number()))
                })
        {
            builder = builder.report_source(report_source_file);
        }

        let dossier = builder.build();
        if let Err(err) = dossier.write().await {
            log::error!("failed to save dossier: {err}");
        }
    }
}

fn playout_location_to_pipeline(value: &WatermarkLocation) -> ffpipeline::input::WatermarkLocation {
    match value {
        WatermarkLocation::TopLeft => ffpipeline::input::WatermarkLocation::TopLeft,
        WatermarkLocation::TopCenter => ffpipeline::input::WatermarkLocation::TopCenter,
        WatermarkLocation::TopRight => ffpipeline::input::WatermarkLocation::TopRight,
        WatermarkLocation::CenterLeft => ffpipeline::input::WatermarkLocation::CenterLeft,
        WatermarkLocation::Center => ffpipeline::input::WatermarkLocation::Center,
        WatermarkLocation::CenterRight => ffpipeline::input::WatermarkLocation::CenterRight,
        WatermarkLocation::BottomLeft => ffpipeline::input::WatermarkLocation::BottomLeft,
        WatermarkLocation::BottomCenter => ffpipeline::input::WatermarkLocation::BottomCenter,
        WatermarkLocation::BottomRight => ffpipeline::input::WatermarkLocation::BottomRight,
    }
}

fn playout_timing_to_pipeline(
    value: Option<&WatermarkTiming>,
) -> Option<ffpipeline::input::WatermarkTiming> {
    value.map(|timing| {
        let WatermarkTiming::Periodic {
            clock,
            frequency_ms,
            phase_offset_ms,
            disable_after_ms,
            fade_ms,
            hold_ms,
        } = timing;

        let clock = match clock {
            PeriodicClock::Content => ffpipeline::input::PeriodicClock::Content,
            PeriodicClock::Wall => ffpipeline::input::PeriodicClock::Wall,
        };

        let periodic_timing = ffpipeline::input::PeriodicTiming {
            clock,
            frequency_ms: *frequency_ms,
            phase_offset_ms: *phase_offset_ms,
            disable_after_ms: *disable_after_ms,
            fade_ms: *fade_ms,
            hold_ms: *hold_ms,
        };

        ffpipeline::input::WatermarkTiming::Periodic(periodic_timing)
    })
}

/// Strips streaming options from a source used for decoration. A watermark
/// fetch is one small read, not a stream: `reconnect` and friends belong to
/// the media inputs, and the image demuxer that reads a still watermark
/// rejects them outright, taking down the whole transcode with it.
fn cosmetic_source(source: PlayoutItemSource) -> PlayoutItemSource {
    match source {
        PlayoutItemSource::Http {
            uri,
            in_point_ms,
            out_point_ms,
            headers,
            user_agent,
            timeout_us,
            probe_hint,
            ..
        } => PlayoutItemSource::Http {
            uri,
            is_live: None,
            in_point_ms,
            out_point_ms,
            headers,
            user_agent,
            timeout_us,
            reconnect: Some(false),
            reconnect_delay_max: None,
            keep_alive: None,
            probe_hint,
        },
        other => other,
    }
}

/// Whether a source's URI references `{query:}` variables, meaning the item
/// may be transcoded more than once with different values and its PTS
/// envelope must be exact.
fn source_is_templated(source: &PlayoutItemSource) -> bool {
    match source {
        PlayoutItemSource::Http { uri, .. } | PlayoutItemSource::Rtsp { uri, .. } => {
            ersatztv_playout::stream_variables::has_query_variables(uri)
        }
        _ => false,
    }
}

/// Where a templated window's slate was configured. There are two places to
/// look now, so a line reporting that slate is on air has to say which one
/// put it there, or it sends an operator to edit the file that lost.
#[derive(Clone, Debug, PartialEq, Eq)]
enum SlateOrigin {
    /// Declared on the playout item itself.
    Schedule,
    /// The channel's slate side file, named by its path.
    SideFile(PathBuf),
}

impl std::fmt::Display for SlateOrigin {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SlateOrigin::Schedule => write!(f, "the schedule"),
            SlateOrigin::SideFile(file) => write!(f, "{}", file.display()),
        }
    }
}

/// Which of the two configurations this window's slate comes from.
///
/// The item wins. A schedule can say which window gets which slate, where
/// the side file can only say what the channel falls back to, so an item
/// that names a slate has said the more specific thing. An item that names
/// none leaves the channel-wide answer standing, exactly as before the
/// schedule could carry slate at all.
fn choose_slate(
    item_slate: Option<PlayoutItemSource>,
    side_file_slate: Option<(String, PathBuf)>,
) -> Option<(PlayoutItemSource, SlateOrigin)> {
    match (item_slate, side_file_slate) {
        (Some(source), _) => Some((source, SlateOrigin::Schedule)),
        (None, Some((path, file))) => Some((local_slate(path), SlateOrigin::SideFile(file))),
        (None, None) => None,
    }
}

/// The item's own slate, when it names media this session can play.
///
/// A local path that does not exist is not a slate: substituting it fails
/// the window into the black fake item, where falling through leaves the
/// side file and then the live source, both of which are better air. Only
/// local paths are checked, because they are the only kind that can be
/// answered for free; every other kind is opened by ffmpeg, and reaching it
/// from here would cost a round trip on every templated window.
async fn usable_item_slate(item: &PlayoutItem) -> Option<PlayoutItemSource> {
    let source = item.slate.as_ref()?;

    if let PlayoutItemSource::Local { path, .. } = source
        && tokio::fs::metadata(path).await.is_err()
    {
        log::warn!(
            "slate {} declared on item {} does not exist; falling back to the slate side file, and tuning the live source if that names nothing usable either",
            path,
            item.id
        );
        return None;
    }

    Some(source.clone())
}

/// A slate configured as a bare path, which is all the side file can say.
fn local_slate(path: String) -> PlayoutItemSource {
    PlayoutItemSource::Local {
        path,
        in_point_ms: None,
        out_point_ms: None,
        probe_hint: None,
    }
}

/// What to call a slate source in a line an operator reads: the part of it
/// they typed. A slate is almost always a local file, so this is almost
/// always its path.
fn slate_label(source: &PlayoutItemSource) -> &str {
    match source {
        PlayoutItemSource::Local { path, .. } => path,
        PlayoutItemSource::Lavfi { params, .. } => params,
        PlayoutItemSource::Http { uri, .. }
        | PlayoutItemSource::Rtsp { uri, .. }
        | PlayoutItemSource::Dynamic { uri, .. } => uri,
        PlayoutItemSource::Script { command, .. } => command,
    }
}

/// The item the shared session transcodes in place of a templated window:
/// the same identity and slot, so the sidecar, variant spawning, and
/// composition all see the window unchanged, with the slate as its only
/// source.
///
/// The substituted item carries no slate of its own. The substitution has
/// already happened, and an item that still advertised one would offer a
/// second round of it to anything that read the item back.
fn slate_item(item: PlayoutItem, slate_source: PlayoutItemSource) -> PlayoutItem {
    let slate_source = whole_window_slate(slate_source, &item.id);

    PlayoutItem {
        id: item.id,
        start: item.start,
        finish: item.finish,
        source: Some(slate_source),
        tracks: None,
        slate: None,
        // the slate stands in for the window, so it keeps the window's
        // decoration: every graphics layer carries over exactly as the
        // legacy watermark already did
        watermark: item.watermark,
        graphics: item.graphics,
    }
}

/// The slate as its window will play it: from the top, for the whole slot.
///
/// A slate arrives as a full source, so the wire can carry in and out points
/// on it, and they are discarded here. Trim points are media coordinates
/// that describe the item a scheduler picked them for: the scheduler read a
/// duration off that media and cut a slot to hold it, which is why
/// [`effective_out_point_ms`] can trust one to narrow a slot and only has to
/// defend against one that widens it. A slate is not that item. The schedule
/// declared the window, and the slate is only what the shared session shows
/// across it, looping for as long as the window lasts, so no slot was ever
/// measured against these points and there is nothing for them to narrow.
///
/// Clamping them instead would answer a media question with the schedule's
/// number, since the only clamp available is the window's own length, and a
/// short out_point would still have to be overwritten to keep the window
/// whole. Discarding leaves each domain saying what it owns: the schedule
/// owns how long the window runs, the slate owns only what plays inside it.
///
/// An out_point honoured here ends the shared transcode and the sidecar
/// envelope early while the run loop advances past the whole window, which
/// is dead air for every viewer and a variant left waiting on segments that
/// are never coming, so the discard is loud.
fn whole_window_slate(mut source: PlayoutItemSource, item_id: &str) -> PlayoutItemSource {
    let declared = match &source {
        PlayoutItemSource::Local {
            in_point_ms,
            out_point_ms,
            ..
        }
        | PlayoutItemSource::Http {
            in_point_ms,
            out_point_ms,
            ..
        } => (*in_point_ms, *out_point_ms),
        _ => (None, None),
    };

    if declared == (None, None) {
        return source;
    }

    log::warn!(
        "slate {} on item {} declares in_point {} and out_point {}; a slate plays its whole window, so both are discarded",
        slate_label(&source),
        item_id,
        trim_point_label(declared.0),
        trim_point_label(declared.1)
    );

    if let PlayoutItemSource::Local {
        in_point_ms,
        out_point_ms,
        ..
    }
    | PlayoutItemSource::Http {
        in_point_ms,
        out_point_ms,
        ..
    } = &mut source
    {
        *in_point_ms = None;
        *out_point_ms = None;
    }

    source
}

/// A trim point as an operator reading the warning wants to see it.
fn trim_point_label(point_ms: Option<u64>) -> String {
    match point_ms {
        Some(point_ms) => format!("{point_ms}ms"),
        None => String::from("none"),
    }
}

/// Repeat the media inputs for as long as the window runs, which only a
/// slate does.
///
/// A slate stands in for a window rather than filling a slot of its own, so
/// any library item works as one whatever its length: the input is reopened
/// until the output `-t` ends the window. Scheduled content is never
/// repeated, because playing an item twice is not what the schedule said.
/// The subtitle input keeps whatever it was built with: its cues are timed
/// once against the source, and a slate carries none of them anyway.
fn repeat_media_inputs_for_slate(input_settings: &mut InputSettings, slate: bool) {
    input_settings.audio_input.loop_when_exhausted = slate;
    input_settings.video_input.loop_when_exhausted = slate;
}

/// How far into an item the shared session began reading, derived from the
/// envelope it declared for that item.
///
/// A session that starts an item from its beginning declares the item's whole
/// duration, giving an offset of zero. One that joins the item partway through
/// declares only the remainder, and the shortfall is how far in it started.
///
/// This is what makes a variant's `progress_ms` usable. That value counts the
/// shared session's published output, measured from wherever it began reading
/// rather than from the item's start, so it is an item offset only when the
/// two coincide.
fn shared_join_offset_ms(item_duration_ms: u64, shared_duration_ms: u64) -> u64 {
    item_duration_ms.saturating_sub(shared_duration_ms)
}

/// The envelope position a variant's output may honestly claim when it opens.
///
/// A live source produces from the instant it connects, so a variant opening
/// at wall time `now` is carrying content for position `now - anchor`,
/// whatever it was spawned believing. Claiming an earlier position does not
/// move the content back. It only makes the composer demand a twin index this
/// worker will not reach for exactly that distance, and both sides then
/// advance at 1x so the gap never closes; the cohort is served shared for the
/// rest of the window. See the composer's
/// `the_join_is_the_displacement_between_the_two_axes`, which pins that the
/// join is precisely this distance.
///
/// SCHEDULE DERIVED, deliberately. `anchor` is the item's authored start plus
/// the declared join offset, and `now` is the wall clock; no measured
/// presentation timestamp takes part. That keeps `transcoded_until` out from
/// under measurement, which is the rule that closed PR #187. Nor can it seek:
/// `input_timing` forces a live item's `in_point` to zero before the state
/// this progress selects is ever consulted.
///
/// A variant that opens at or before its anchor keeps exactly what it was
/// given, so a cohort present from the item's start is unaffected.
fn variant_start_progress_ms(
    spawned_progress_ms: u64,
    anchor: OffsetDateTime,
    now: OffsetDateTime,
    item_finish: OffsetDateTime,
    live: bool,
) -> u64 {
    // a file source seeks, so where it opens says nothing about which
    // position it will emit
    if !live {
        return spawned_progress_ms;
    }

    let elapsed_ms = (now - anchor).whole_milliseconds().max(0) as u64;
    let envelope_ms = (item_finish - anchor).whole_milliseconds().max(0) as u64;
    spawned_progress_ms.max(elapsed_ms).min(envelope_ms)
}

/// An explicit out_point may narrow what an item plays from its source, but
/// must never widen it past the item's scheduled slot. Emitted media is
/// appended to one continuous timeline with no later reconciliation against
/// the schedule, so every surplus millisecond permanently delays everything
/// after it for viewers. Legacy playouts really do carry such out_points on
/// fallback filler items (the value belongs to a longer item the scheduler
/// considered and rejected), which surfaced as schedule drift of roughly 80
/// seconds per hour of session uptime.
///
/// Returns the effective out_point and how much was clamped away.
fn effective_out_point_ms(
    explicit_out_point_ms: Option<u64>,
    in_point_base_ms: u64,
    slot_ms: u64,
) -> (u64, u64) {
    let slot_out_point_ms = in_point_base_ms + slot_ms;
    match explicit_out_point_ms {
        Some(out_point_ms) if out_point_ms > slot_out_point_ms => {
            (slot_out_point_ms, out_point_ms - slot_out_point_ms)
        }
        Some(out_point_ms) => (out_point_ms, 0),
        None => (slot_out_point_ms, 0),
    }
}

fn source_is_live(source: &PlayoutItemSource) -> bool {
    matches!(
        source,
        PlayoutItemSource::Http {
            is_live: Some(true),
            ..
        } | PlayoutItemSource::Script {
            is_live: Some(true),
            ..
        } | PlayoutItemSource::Rtsp { .. }
    )
}

fn probe_hint_to_result(hint: &ProbeHint, path: String) -> ProbeResult {
    let video = hint.video.iter().map(|v| {
        ProbeResultStream::Video(Box::new(ProbeResultVideoStream {
            stream_index: v.stream_index,
            codec: v.codec.to_lowercase(),
            codec_type: CodecType::Video,
            dv_profile: v.dv_profile,
            profile: v.profile.clone().unwrap_or_default().to_lowercase(),
            height: Some(v.height),
            width: Some(v.width),
            pix_fmt: v.pix_fmt.clone(),
            color_params: ProbeResultColorParams {
                color_range: v.color_range.clone(),
                color_space: v.color_space.clone(),
                color_transfer: v.color_transfer.clone(),
                color_primaries: v.color_primaries.clone(),
            },
            field_order: v.field_order.clone(),
            frame_rate: v
                .frame_rate
                .as_deref()
                .map(FrameRate::parse)
                .unwrap_or_default(),
            sample_aspect_ratio: v.sample_aspect_ratio.clone(),
            display_aspect_ratio: v.display_aspect_ratio.clone(),
        }))
    });

    let audio = hint.audio.iter().map(|a| {
        ProbeResultStream::Audio(ProbeResultAudioStream {
            stream_index: a.stream_index,
            codec: a.codec.to_lowercase(),
            channels: a.channels,
        })
    });

    let subtitle = hint.subtitle.iter().map(|s| {
        ProbeResultStream::Video(Box::new(ProbeResultVideoStream {
            stream_index: s.stream_index,
            codec: s.codec.to_lowercase(),
            codec_type: CodecType::Subtitle,
            dv_profile: None,
            profile: String::new(),
            height: None,
            width: None,
            pix_fmt: String::new(),
            color_params: ProbeResultColorParams::default(),
            field_order: None,
            frame_rate: FrameRate::default(),
            sample_aspect_ratio: None,
            display_aspect_ratio: None,
        }))
    });

    ProbeResult {
        path,
        streams: video.chain(audio).chain(subtitle).collect(),
        duration: hint.duration_ms.map(Duration::from_millis),
        format_name: hint.format_name.clone().or(Some(String::from("mpegts"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(seconds)
    }

    #[test]
    fn stamp_error_measures_emission_against_the_schedule() {
        // 25s more media emitted than the slot called for
        assert_eq!(
            ChannelSession::stamp_error_ms(at(1025), at(1000), time::Duration::ZERO),
            25_000
        );
        assert_eq!(
            ChannelSession::stamp_error_ms(at(995), at(1000), time::Duration::ZERO),
            -5_000
        );
    }

    #[test]
    fn a_virtual_start_offset_is_not_a_stamp_error() {
        // ChannelSession::new seeds last_segment_end at `now` and
        // transcoded_until at `now + start_time_offset`, so the offset is
        // present from the first pipeline and never goes away. reporting it
        // as error would make emission_trim_ms claw back the whole virtual
        // start offset, 500ms of content per item, for as long as it took
        for offset_secs in [-604_800i64, -3600, -1, 1, 3600, 604_800] {
            let offset = time::Duration::seconds(offset_secs);
            assert_eq!(
                ChannelSession::stamp_error_ms(at(1000), at(1000) + offset, offset),
                0,
                "a {offset_secs}s virtual start offset was measured as stamp error"
            );
        }
    }

    #[test]
    fn a_real_error_is_still_measured_through_a_virtual_start_offset() {
        let offset = time::Duration::hours(1);
        assert_eq!(
            ChannelSession::stamp_error_ms(at(1025), at(1000) + offset, offset),
            25_000
        );
    }

    #[test]
    fn a_virtual_start_offset_produces_no_emission_trim() {
        // the end to end shape of the bug: measurement into correction
        let offset = time::Duration::hours(1);
        let error = ChannelSession::stamp_error_ms(at(1000), at(1000) + offset, offset);
        assert_eq!(ChannelSession::emission_trim_ms(error, 30_000, false), 0);
    }

    /// The steady state the trim exists for: each padded item overshoots its
    /// slot by up to one frame, and the whole accumulated error comes back on
    /// the very next pipeline.
    #[test]
    fn a_small_stamp_clock_error_is_returned_in_full() {
        assert_eq!(ChannelSession::emission_trim_ms(27, 11_021, false), 27);
        assert_eq!(ChannelSession::emission_trim_ms(-31, 11_021, false), -31);
        assert_eq!(ChannelSession::emission_trim_ms(0, 11_021, false), 0);
    }

    /// A wild clock slews back over several items rather than opening one
    /// large hole in a single pipeline.
    #[test]
    fn a_large_stamp_clock_error_is_clamped_in_both_directions() {
        assert_eq!(ChannelSession::emission_trim_ms(6_500, 60_000, false), 500);
        assert_eq!(
            ChannelSession::emission_trim_ms(-6_500, 60_000, false),
            -500
        );
    }

    /// A short pipeline gives back at most half of itself, so a bump can
    /// never be trimmed into nothing.
    #[test]
    fn a_trim_never_eats_more_than_half_the_pipeline() {
        assert_eq!(ChannelSession::emission_trim_ms(400, 600, false), 300);
    }

    /// A variant transcode must fill exactly the envelope the shared session
    /// declares, so a templated item's -t stays a pure function of the item.
    #[test]
    fn a_templated_item_is_never_trimmed() {
        assert_eq!(ChannelSession::emission_trim_ms(400, 103_000, true), 0);
    }

    /// Only the emitted duration moves. `in_point` is where reading starts
    /// and `finish` is how far the schedule advances; a trim that touched
    /// either would be a measurement driving a seek, the PR #187 pattern.
    #[test]
    fn a_trim_moves_only_the_out_point() {
        let finish = OffsetDateTime::parse(
            "2026-08-15T12:00:11.021-04:00",
            &time::format_description::well_known::Iso8601::DEFAULT,
        )
        .unwrap();
        let timing = TimingResult {
            in_point: Duration::from_millis(2_000),
            out_point: Duration::from_millis(13_021),
            finish,
            is_complete: true,
        };

        let trimmed = ChannelSession::apply_emission_trim(timing, 27);
        assert_eq!(trimmed.in_point, Duration::from_millis(2_000));
        assert_eq!(trimmed.out_point, Duration::from_millis(12_994));
        assert_eq!(trimmed.finish, finish);
        assert!(trimmed.is_complete);

        let extended = ChannelSession::apply_emission_trim(
            TimingResult {
                in_point: Duration::from_millis(2_000),
                out_point: Duration::from_millis(13_021),
                finish,
                is_complete: true,
            },
            -40,
        );
        assert_eq!(extended.in_point, Duration::from_millis(2_000));
        assert_eq!(extended.out_point, Duration::from_millis(13_061));
    }

    /// The channel configuration exactly as the scaffolder writes it, which
    /// is the shape every deployment starts from.
    fn test_channel_config() -> ChannelConfig {
        serde_json::from_value(serde_json::json!({
            "playout": { "folder": "/tmp/playout" },
            "ffmpeg": {},
            "normalization": {
                "audio": {
                    "format": "aac", "bitrate_kbps": 192, "buffer_kbps": 384,
                    "channels": 2, "sample_rate_hz": 48000,
                    "normalize_loudness": false
                },
                "video": {
                    "format": "h264", "bit_depth": 8,
                    "width": 1920, "height": 1080,
                    "bitrate_kbps": 2000, "buffer_kbps": 4000
                },
                "subtitle": { "mode": "burn" }
            }
        }))
        .expect("the scaffolded channel config shape deserializes")
    }

    fn output_settings(realtime: bool, slate: bool, is_live: bool, still: bool) -> OutputSettings {
        ChannelSession::build_output_settings(OutputSettingsPlan {
            channel_config: &test_channel_config(),
            accel: None,
            output_file: String::from("/tmp/out/live.m3u8"),
            output_segment_template: String::from("/tmp/out/live%06d.ts"),
            troubleshoot: false,
            pts_duration: Some(Duration::from_millis(1234)),
            realtime,
            slate,
            is_live,
            video_is_still_image: still,
        })
    }

    /// The one-line caller change behind the 2026-08-14 regression, now
    /// pinned: every pipeline is padded, whatever kind of item it is.
    #[test]
    fn every_pipeline_is_padded_to_its_clamp() {
        for realtime in [false, true] {
            for slate in [false, true] {
                for is_live in [false, true] {
                    assert!(
                        output_settings(realtime, slate, is_live, false).pad_to_duration,
                        "realtime={realtime} slate={slate} is_live={is_live} must be padded"
                    );
                }
            }
        }
    }

    /// Slate paces like every other pipeline: the unpaced special case
    /// rested on a withdrawn measurement, and a padded paced pipeline is
    /// what all of production runs. Pacing follows the caller alone.
    #[test]
    fn slate_paces_like_every_other_pipeline() {
        assert!(output_settings(true, true, false, false).realtime);
        assert!(output_settings(true, false, false, false).realtime);
        assert!(!output_settings(false, false, false, false).realtime);
        assert!(!output_settings(false, true, false, false).realtime);
    }

    /// A still image decodes as a single frame since #211, so the encoder
    /// must be told a rate to emit it at.
    #[test]
    fn a_still_image_forces_an_output_frame_rate() {
        assert!(
            output_settings(true, false, false, true)
                .frame_rate
                .is_some()
        );
        assert!(
            output_settings(true, false, false, false)
                .frame_rate
                .is_none()
        );
    }

    /// The scanned pts offset must reach the encoder unchanged; it is the
    /// only thing keeping output timestamps monotonic across items.
    #[test]
    fn the_pts_offset_reaches_the_encoder() {
        let settings = output_settings(true, false, false, false);
        assert_eq!(
            settings.pts_offset.expect("offset is declared").duration,
            Duration::from_millis(1234)
        );
    }

    /// A plain file item occupying an 11.021s slot, the shape of the logo
    /// bump whose shortfall started the drift investigation.
    fn file_item() -> PlayoutItem {
        serde_json::from_value(serde_json::json!({
            "id": "file-item",
            "start": "2026-08-15T12:00:00.000-04:00",
            "finish": "2026-08-15T12:00:11.021-04:00",
            "source": { "source_type": "local", "path": "/bumps/logo.mp4" }
        }))
        .expect("a local file item deserializes")
    }

    fn plan_for(
        item: &PlayoutItem,
        slate: bool,
        is_templated: bool,
        stamp_error_ms: i64,
    ) -> PlannedTimings {
        let audio_source =
            ChannelSession::resolve_source(item, |t| t.audio.as_ref()).expect("audio source");
        let video_source =
            ChannelSession::resolve_source(item, |t| t.video.as_ref()).expect("video source");
        let is_live = source_is_live(&video_source) || source_is_live(&audio_source);
        ChannelSession::plan_timings(TimingPlan {
            current_item: item,
            audio_source: &audio_source,
            video_source: &video_source,
            subtitle_source: None,
            start_at_zero: true,
            realtime: true,
            slate,
            is_live,
            is_templated,
            transcoded_until: item.start,
            stamp_error_ms,
        })
    }

    /// The trim must land on what the -t actually consumes: both streams'
    /// out points, and the envelope the sidecar declares. A trim that was
    /// computed but not applied here is exactly the wiring gap that made
    /// the padding regression invisible to the test suite.
    #[test]
    fn the_trim_reaches_every_stream_the_t_reads() {
        let item = file_item();
        let planned = plan_for(&item, false, false, 27);
        assert_eq!(planned.trim_ms, 27);
        assert_eq!(planned.audio.out_point, Duration::from_millis(10_994));
        assert_eq!(planned.video.out_point, Duration::from_millis(10_994));
        assert_eq!(planned.declared_duration_ms, 10_994);
        assert_eq!(planned.audio.in_point, Duration::ZERO);
        assert_eq!(planned.video.in_point, Duration::ZERO);
    }

    /// A templated item's envelope is a pure function of the item, whatever
    /// the stamp clock says: a variant must be able to compute the same
    /// envelope from the item alone.
    #[test]
    fn a_templated_plan_ignores_the_stamp_error() {
        let item = templated_item_with_slate(None);
        let planned = plan_for(&item, false, true, 400);
        assert_eq!(planned.trim_ms, 0);
        assert_eq!(planned.declared_duration_ms, 150_000);
        assert_eq!(planned.video.out_point, Duration::from_millis(150_000));
    }

    /// Slate must fill its whole remaining window in one pipeline even in a
    /// work-ahead state: chunking would declare one sidecar envelope per
    /// chunk, and a variant reading the first chunk's duration would mistake
    /// it for a mid-item join.
    #[test]
    fn slate_fills_its_whole_window_in_one_pipeline() {
        let item: PlayoutItem = serde_json::from_value(serde_json::json!({
            "id": "slate-item",
            "start": "2026-08-15T12:00:00.000-04:00",
            "finish": "2026-08-15T12:05:00.000-04:00",
            "source": { "source_type": "local", "path": "/bumps/fallback/slate.mp4" }
        }))
        .expect("a slate stand-in deserializes");

        let audio_source =
            ChannelSession::resolve_source(&item, |t| t.audio.as_ref()).expect("audio source");
        let video_source =
            ChannelSession::resolve_source(&item, |t| t.video.as_ref()).expect("video source");
        for (slate, expect_whole) in [(true, true), (false, false)] {
            let planned = ChannelSession::plan_timings(TimingPlan {
                current_item: &item,
                audio_source: &audio_source,
                video_source: &video_source,
                subtitle_source: None,
                start_at_zero: true,
                realtime: false,
                slate,
                is_live: false,
                is_templated: slate,
                transcoded_until: item.start,
                stamp_error_ms: 0,
            });
            if expect_whole {
                assert!(planned.video.is_complete, "slate must not be chunked");
                assert_eq!(planned.declared_duration_ms, 300_000);
            } else {
                assert!(
                    !planned.video.is_complete,
                    "work-ahead chunks a long file item"
                );
                assert_eq!(planned.declared_duration_ms, 44_000);
            }
        }
    }

    /// What a variant ends up producing, given the item and what the shared
    /// session declared and has published so far. Mirrors what `run_variant`
    /// derives and what `input_timing` then computes for a live source.
    fn variant_envelope(
        item_duration_ms: u64,
        shared_duration_ms: u64,
        progress_ms: u64,
    ) -> (u64, u64) {
        let join = shared_join_offset_ms(item_duration_ms, shared_duration_ms);
        let position = (join + progress_ms).min(item_duration_ms);
        (progress_ms, item_duration_ms - position)
    }

    /// A star-window item as legacy exports it: a templated live URI, and
    /// whatever slate the schedule declares on it.
    fn templated_item_with_slate(slate: Option<serde_json::Value>) -> PlayoutItem {
        let mut item = serde_json::json!({
            "id": "star",
            "start": "2026-08-10T12:35:10.000-04:00",
            "finish": "2026-08-10T12:37:40.000-04:00",
            "source": {
                "source_type": "http",
                "uri": "http://host:8000/live.ts?sid=ch{channel_number}&zip={query:zip|10001}",
                "is_live": true
            }
        });

        if let Some(slate) = slate {
            item["slate"] = slate;
        }

        serde_json::from_value(item).unwrap()
    }

    fn templated_item() -> PlayoutItem {
        templated_item_with_slate(None)
    }

    /// The side file as `load_slate_path` reports it: the media it names,
    /// and the file that named it.
    fn side_file_slate(path: &str) -> Option<(String, PathBuf)> {
        Some((String::from(path), PathBuf::from("/channels/5/slate.json")))
    }

    #[test]
    fn a_templated_window_is_recognized_before_slate_substitution() {
        assert!(ChannelSession::item_is_templated(&templated_item()));

        // after substitution the sources are plain files, which is why the
        // decision is judged on the original item and carried as a flag
        let slated = slate_item(templated_item(), local_slate(String::from("/slate.mp4")));
        assert!(!ChannelSession::item_is_templated(&slated));
    }

    /// A slate declared on the item is the one that plays, and the log line
    /// says it came from the schedule. The item is the more specific of the
    /// two configurations: it names this window's slate, where the side file
    /// can only name the channel's.
    #[tokio::test]
    async fn an_item_carrying_a_slate_plays_it_over_the_side_file() {
        let folder = tempfile::tempdir().unwrap();
        let declared = folder.path().join("WeatherSlate.mp4");
        tokio::fs::write(&declared, b"slate").await.unwrap();

        let item = templated_item_with_slate(Some(serde_json::json!({
            "source_type": "local",
            "path": declared.to_string_lossy()
        })));

        let chosen = choose_slate(
            usable_item_slate(&item).await,
            side_file_slate("/generic/OffAir.mp4"),
        );

        let (source, origin) = chosen.expect("a declared slate must be chosen");
        assert_eq!(slate_label(&source), declared.to_string_lossy());
        assert_eq!(origin, SlateOrigin::Schedule);
        assert_eq!(origin.to_string(), "the schedule");

        // and it is what the shared session actually transcodes, while the
        // window keeps its identity
        let slated = slate_item(item, source);
        assert_eq!(slated.id, "star");
        match slated.source {
            Some(PlayoutItemSource::Local { path, .. }) => {
                assert_eq!(path, declared.to_string_lossy())
            }
            other => panic!("expected the declared slate, got {other:?}"),
        }
    }

    /// An item that declares nothing leaves the channel-wide answer
    /// standing, which is the whole world before a schedule could carry
    /// slate. The log line names the file so an operator knows which
    /// configuration to edit.
    #[tokio::test]
    async fn an_item_without_a_slate_falls_back_to_the_side_file() {
        let item = templated_item();

        let chosen = choose_slate(
            usable_item_slate(&item).await,
            side_file_slate("/generic/OffAir.mp4"),
        );

        let (source, origin) = chosen.expect("the side file must still answer");
        assert_eq!(slate_label(&source), "/generic/OffAir.mp4");
        assert_eq!(
            origin,
            SlateOrigin::SideFile(PathBuf::from("/channels/5/slate.json"))
        );
        assert_eq!(origin.to_string(), "/channels/5/slate.json");
    }

    /// With slate configured in neither place there is no substitution at
    /// all: the shared session tunes the live source, templated URL and all,
    /// exactly as it did before slate existed.
    #[tokio::test]
    async fn with_no_slate_anywhere_the_live_source_is_tuned() {
        let item = templated_item();

        assert!(choose_slate(usable_item_slate(&item).await, None).is_none());

        // nothing swapped the source out, so what the session transcodes is
        // still the templated live source
        match ChannelSession::resolve_source(&item, |t| t.video.as_ref()) {
            Some(source @ PlayoutItemSource::Http { .. }) => {
                assert!(source_is_templated(&source));
                assert!(source_is_live(&source));
            }
            other => panic!("expected the templated live source, got {other:?}"),
        }
    }

    /// A schedule can name a file that is not on this box (a slate folder
    /// that did not sync, a typo). Substituting it would fail the window
    /// into the black fake item, so it is not a usable slate and the ladder
    /// continues to the side file and then to the live source.
    #[tokio::test]
    async fn a_slate_naming_media_that_is_not_there_falls_through() {
        let folder = tempfile::tempdir().unwrap();

        let item = templated_item_with_slate(Some(serde_json::json!({
            "source_type": "local",
            "path": folder.path().join("Gone.mp4").to_string_lossy()
        })));

        assert!(usable_item_slate(&item).await.is_none());
        assert_eq!(
            choose_slate(
                usable_item_slate(&item).await,
                side_file_slate("/generic/OffAir.mp4")
            )
            .map(|(source, _)| slate_label(&source).to_owned()),
            Some(String::from("/generic/OffAir.mp4"))
        );
    }

    #[test]
    fn a_slate_item_keeps_the_window_identity_and_swaps_only_the_source() {
        let item = templated_item();
        let (id, start, finish) = (item.id.clone(), item.start, item.finish);

        let slated = slate_item(item, local_slate(String::from("/slate.mp4")));

        assert_eq!(slated.id, id);
        assert_eq!(slated.start, start);
        assert_eq!(slated.finish, finish);
        assert!(slated.tracks.is_none());
        match slated.source {
            Some(PlayoutItemSource::Local {
                path,
                in_point_ms,
                out_point_ms,
                ..
            }) => {
                assert_eq!(path, "/slate.mp4");
                assert_eq!(in_point_ms, None);
                assert_eq!(out_point_ms, None);
            }
            other => panic!("expected a local slate source, got {other:?}"),
        }
    }

    /// The trim points `input_timing` reads off the source it is handed, in
    /// the two shapes that can carry them. Mirrors the two matches there, so
    /// a slate that reaches it clean here reaches it clean there.
    fn trim_points(source: &PlayoutItemSource) -> (u64, Option<u64>) {
        match source {
            PlayoutItemSource::Local {
                in_point_ms,
                out_point_ms,
                ..
            }
            | PlayoutItemSource::Http {
                in_point_ms,
                out_point_ms,
                ..
            } => (in_point_ms.unwrap_or(0), *out_point_ms),
            _ => (0, None),
        }
    }

    /// A slate is a full source, so a scheduler can hang trim points on it,
    /// and they must not shorten the window it stands in for. An out_point
    /// honoured here ends the shared transcode and the sidecar envelope
    /// short of a slot the run loop still advances past: black to the end of
    /// the window, and a declared envelope no variant can ever fill. The
    /// clamp is no defence, because a short out_point is not an overrun.
    #[tokio::test]
    async fn a_declared_slate_plays_its_whole_window_whatever_trim_points_it_carries() {
        let folder = tempfile::tempdir().unwrap();
        let declared = folder.path().join("WeatherSlate.mp4");
        tokio::fs::write(&declared, b"slate").await.unwrap();

        let item = templated_item_with_slate(Some(serde_json::json!({
            "source_type": "local",
            "path": declared.to_string_lossy(),
            "in_point_ms": 30_000,
            "out_point_ms": 45_000
        })));
        let slot_ms = (item.finish - item.start).whole_milliseconds() as u64;

        let source = usable_item_slate(&item)
            .await
            .expect("the declared slate is on disk");
        let slated = slate_item(item, source);

        let substituted = slated.source.as_ref().expect("the slate is the source");
        assert_eq!(slate_label(substituted), declared.to_string_lossy());

        // what input_timing then reads off the substituted source: no seek,
        // no explicit end, so the window plays whole and nothing is clamped
        let (in_point_base_ms, explicit_out_point_ms) = trim_points(substituted);
        assert_eq!(in_point_base_ms, 0);
        assert_eq!(explicit_out_point_ms, None);
        assert_eq!(
            effective_out_point_ms(explicit_out_point_ms, in_point_base_ms, slot_ms),
            (slot_ms, 0)
        );
    }

    /// The same for a slate that is not a local file. Trim points ride on
    /// http sources too, and reach `input_timing` by the same match.
    #[tokio::test]
    async fn a_remote_slate_loses_its_trim_points_the_same_way() {
        let item = templated_item_with_slate(Some(serde_json::json!({
            "source_type": "http",
            "uri": "http://slates/OffAir.mkv",
            "out_point_ms": 20_000
        })));
        let slot_ms = (item.finish - item.start).whole_milliseconds() as u64;

        let source = usable_item_slate(&item)
            .await
            .expect("a remote slate is taken at its word");
        let slated = slate_item(item, source);

        let substituted = slated.source.as_ref().expect("the slate is the source");
        assert_eq!(trim_points(substituted), (0, None));
        assert_eq!(
            effective_out_point_ms(None, 0, slot_ms),
            (slot_ms, 0),
            "the whole window, as the schedule declared it"
        );
    }

    /// Every input the pipeline reads for one item, before the slate window
    /// says otherwise.
    fn one_pass_input_settings() -> InputSettings {
        let probed = |path: &str| ProbedInput {
            input_source: InputSource::Local(LocalInputSource {
                path: String::from(path),
            }),
            probe_result: ProbeResult {
                path: String::from(path),
                streams: Vec::new(),
                duration: None,
                format_name: None,
            },
            in_point: Duration::ZERO,
            out_point: Duration::from_secs(150),
            stream_index: None,
            loop_when_exhausted: false,
        };

        InputSettings {
            start: templated_item().start,
            audio_input: probed("/slate/WeatherSlate.mp4"),
            video_input: probed("/slate/WeatherSlate.mp4"),
            subtitle_input: Some(probed("/slate/WeatherSlate.mp4")),
            graphics_inputs: Vec::new(),
            playout_offset: Duration::ZERO,
        }
    }

    /// The single line joining the two halves of the feature: the run loop
    /// decides this window plays slate, and the pipeline's media inputs have
    /// to repeat for that to mean anything. Without it a slate shorter than
    /// its window ends the transcode early and takes the rest of the slot
    /// with it, which is the whole reason any library item can be slate.
    #[test]
    fn a_slate_window_repeats_its_media_inputs_and_a_scheduled_one_never_does() {
        let mut slated = one_pass_input_settings();
        repeat_media_inputs_for_slate(&mut slated, true);

        assert!(
            slated.audio_input.loop_when_exhausted,
            "slate audio must repeat until the window ends"
        );
        assert!(
            slated.video_input.loop_when_exhausted,
            "slate video must repeat until the window ends"
        );
        assert!(
            !slated
                .subtitle_input
                .expect("the subtitle input is left alone")
                .loop_when_exhausted
        );

        let mut scheduled = one_pass_input_settings();
        repeat_media_inputs_for_slate(&mut scheduled, false);

        assert!(
            !scheduled.audio_input.loop_when_exhausted,
            "scheduled content must never play twice"
        );
        assert!(
            !scheduled.video_input.loop_when_exhausted,
            "scheduled content must never play twice"
        );
    }

    #[test]
    fn an_item_without_an_explicit_out_point_plays_exactly_its_slot() {
        assert_eq!(effective_out_point_ms(None, 0, 50_209), (50_209, 0));
    }

    #[test]
    fn an_out_point_inside_the_slot_narrows_what_plays() {
        assert_eq!(effective_out_point_ms(Some(23_000), 0, 50_209), (23_000, 0));
    }

    #[test]
    fn an_out_point_past_the_slot_is_clamped_to_the_slot() {
        // channel 13, item 12124970: a 50.209s fallback filler slot carrying
        // out_point 60.007s from the item the legacy scheduler rejected; the
        // 9.798s surplus accrued as permanent schedule drift every block
        assert_eq!(
            effective_out_point_ms(Some(60_007), 0, 50_209),
            (50_209, 9_798)
        );
    }

    #[test]
    fn an_out_point_clamp_respects_an_explicit_in_point() {
        // the slot is measured from the in_point, so a mid-file window may
        // legitimately end past slot_ms alone
        assert_eq!(
            effective_out_point_ms(Some(80_000), 30_000, 50_000),
            (80_000, 0)
        );
        assert_eq!(
            effective_out_point_ms(Some(90_000), 30_000, 50_000),
            (80_000, 10_000)
        );
    }

    #[test]
    fn a_shared_session_that_started_the_item_has_no_join_offset() {
        assert_eq!(shared_join_offset_ms(113_000, 113_000), 0);
    }

    /// A 103000ms templated window, the shape every cohort variant runs in.
    fn window() -> (OffsetDateTime, OffsetDateTime) {
        let anchor = OffsetDateTime::UNIX_EPOCH;
        (anchor, anchor + time::Duration::seconds(103))
    }

    /// THE REGRESSION GUARD for ordering a variant at the position the wall
    /// clock has reached. A cohort that is present when the item starts must
    /// be completely unaffected, and that is the case every healthy
    /// substitution takes: 135 of 135 measured on 2026-08-14 joined at 0.
    ///
    /// Both directions matter. Spawned early, the variant waits out the
    /// air-lock and still opens at its anchor, so nothing moves. Spawned
    /// exactly at the anchor, likewise.
    #[test]
    fn a_variant_that_opens_on_time_keeps_the_progress_it_was_given() {
        let (anchor, finish) = window();

        for lead in [0i64, 1, 30, 45] {
            let now = anchor - time::Duration::seconds(lead);
            assert_eq!(
                variant_start_progress_ms(0, anchor, now, finish, true),
                0,
                "a variant opening {lead}s before its anchor claims the item start"
            );
        }

        // and a non-zero progress handed down for a genuine mid-item shared
        // join is passed through untouched
        assert_eq!(
            variant_start_progress_ms(20_000, anchor, anchor, finish, true),
            20_000
        );
    }

    /// A cohort that tuned in partway through the window, or a shared session
    /// that reached the item late, opens against a wall clock already inside
    /// the envelope. ch11 item 12206607 on 2026-08-12: cohort 'zip=90210'
    /// spawned at 21:31:02, about 59s into a window that began airing at
    /// 21:30:03, and its variant claimed position 0 while the composer was
    /// demanding 60000ms.
    #[test]
    fn a_live_variant_opening_late_claims_where_the_wall_clock_stands() {
        let (anchor, finish) = window();

        assert_eq!(
            variant_start_progress_ms(
                0,
                anchor,
                anchor + time::Duration::seconds(59),
                finish,
                true
            ),
            59_000
        );

        // it never moves backwards: a larger declared progress wins
        assert_eq!(
            variant_start_progress_ms(
                80_000,
                anchor,
                anchor + time::Duration::seconds(59),
                finish,
                true
            ),
            80_000
        );
    }

    /// THE PR #187 GUARD, and the reason it now needs to be a test rather than
    /// an argument.
    ///
    /// A non-zero `progress_ms` selects `SeekAndRealtime`, which makes
    /// `start_at_zero` false, which would otherwise derive `in_point` from
    /// `transcoded_until`. That is an output offset driving an input seek, and
    /// it is the pattern jasongdove rejected when he closed PR #187: output
    /// timestamp offsets and source read positions are deliberately decoupled,
    /// and the read position comes from the SCHEDULE, never from a
    /// measurement.
    ///
    /// Before 02a05f7 nothing reached that branch, because a fallback pipeline
    /// always spawned at progress 0. `variant_start_progress_ms` is what makes
    /// progress non-zero, so the decoupling is now load bearing. What holds it
    /// is the live branch in `input_timing_at`, which returns `in_point` ZERO
    /// before the state is consulted at all.
    ///
    /// It is worth being precise about why this is safe today: every templated
    /// source is live, 9716 of 9716 across the install on 2026-08-14. But that
    /// is a property of what the scheduler emits, not of this worker, so the
    /// guard has to be the branch and not the data.
    #[test]
    fn a_live_source_never_seeks_however_far_the_session_has_progressed() {
        let item = templated_item();
        let source = item.source.clone().expect("the fixture carries a source");

        for elapsed in [0i64, 8, 59, 149] {
            let timing = ChannelSession::input_timing_at(
                &item,
                &source,
                // false is the seeking branch: this is what a non-zero
                // progress selects
                false,
                true,
                true,
                item.start + time::Duration::seconds(elapsed),
            );

            assert_eq!(
                timing.in_point,
                Duration::ZERO,
                "a live source must not seek, {elapsed}s into the item"
            );
        }

        // and it covers only the remainder, so a session joining partway
        // through keeps its output inside the item's envelope. The fixture
        // window is 150s
        let at_59 = ChannelSession::input_timing_at(
            &item,
            &source,
            false,
            true,
            true,
            item.start + time::Duration::seconds(59),
        );
        assert_eq!(at_59.out_point, Duration::from_millis(91_000));
    }

    /// A file source seeks, so where it opens says nothing about which
    /// position it emits, and the wall clock must not touch it.
    #[test]
    fn a_file_variant_is_never_moved_by_the_wall_clock() {
        let (anchor, finish) = window();
        assert_eq!(
            variant_start_progress_ms(
                0,
                anchor,
                anchor + time::Duration::seconds(59),
                finish,
                false
            ),
            0
        );
    }

    /// Past the end of the envelope there is nothing left to substitute, so
    /// the claim saturates rather than running past the window.
    ///
    /// That saturation is what stops the wasted encode. `run_variant` sets
    /// `transcoded_until` to `(anchor + progress).min(item.finish)` and then
    /// loops `while transcoded_until < item.finish`, so a claim that reaches
    /// the envelope leaves the loop with nothing to do and no ffmpeg is
    /// started at all. On 2026-08-12 item 12206607 a full 105s GPU encode ran
    /// to completion and was discarded because the variant was ordered at
    /// position 0 for a window the cohort had almost entirely missed.
    #[test]
    fn a_late_open_cannot_claim_past_the_envelope() {
        let (anchor, finish) = window();
        let progress = variant_start_progress_ms(
            0,
            anchor,
            anchor + time::Duration::seconds(500),
            finish,
            true,
        );
        assert_eq!(progress, 103_000);

        // the consequence: transcoded_until lands exactly on the item's
        // finish, so the transcode loop never runs
        let transcoded_until = (anchor + time::Duration::milliseconds(progress as i64)).min(finish);
        assert_eq!(
            transcoded_until, finish,
            "a variant with nothing left to cover must start no encode"
        );
    }

    #[test]
    fn a_shared_session_that_joined_late_reports_how_far_in_it_started() {
        // channel 32, item 11866757: the item's slot is 113s and the shared
        // session declared 58.58s, so it began 54.42s into the item
        assert_eq!(shared_join_offset_ms(113_000, 58_580), 54_420);
    }

    #[test]
    fn a_variant_of_an_item_started_from_zero_fills_the_whole_remainder() {
        // channel 11, item 11866490: shared declared the full 113s at
        // progress 0, and the variant matched it exactly
        let (pts_from, duration) = variant_envelope(113_000, 113_000, 0);

        assert_eq!(pts_from, 0);
        assert_eq!(duration, 113_000);
    }

    #[test]
    fn a_variant_of_a_late_joined_item_stops_where_the_shared_envelope_stops() {
        // channel 32: the variant used to be given the item's whole remaining
        // slot (97s) against a shared envelope of only 58.58s. It must fill
        // the 42.58s the shared session has left instead
        let (pts_from, duration) = variant_envelope(113_000, 58_580, 16_000);

        assert_eq!(pts_from, 16_000);
        assert_eq!(duration, 42_580);
        assert_eq!(
            pts_from + duration,
            58_580,
            "must end with the shared envelope"
        );
    }

    #[test]
    fn a_variant_produces_nothing_once_the_shared_envelope_is_covered() {
        // channel 13, item 11865804: the shared session joined 4.755s before
        // the item ended and had already published past that, yet the variant
        // was handed 131s of work
        let (_, duration) = variant_envelope(136_000, 4_755, 4_766);

        assert_eq!(duration, 0);
    }

    /// A realtime pipeline covers its whole remaining slot in one
    /// invocation, and both streams agree on the range.
    #[test]
    fn a_realtime_item_fills_its_slot_in_one_pipeline() {
        let item = file_item();
        let planned = plan_for(&item, false, false, 0);
        assert_eq!(planned.audio.in_point, Duration::ZERO);
        assert_eq!(planned.audio.out_point, Duration::from_millis(11_021));
        assert_eq!(planned.video.out_point, Duration::from_millis(11_021));
        assert!(planned.video.is_complete);
        assert_eq!(planned.video.finish, item.finish);
    }

    /// While working ahead, a long item is transcoded in chunks so the
    /// buffer builds up quickly; the chunk boundary advances the schedule
    /// by exactly the chunk.
    #[test]
    fn work_ahead_chunks_a_long_item() {
        let item: PlayoutItem = serde_json::from_value(serde_json::json!({
            "id": "long-item",
            "start": "2026-08-15T12:00:00.000-04:00",
            "finish": "2026-08-15T12:05:00.000-04:00",
            "source": { "source_type": "local", "path": "/media/episode.mp4" }
        }))
        .expect("a local file item deserializes");

        let limit = Duration::from_secs(SEGMENT_SECONDS as u64 * 11);
        let audio_source =
            ChannelSession::resolve_source(&item, |t| t.audio.as_ref()).expect("audio source");
        let video_source =
            ChannelSession::resolve_source(&item, |t| t.video.as_ref()).expect("video source");
        let planned = ChannelSession::plan_timings(TimingPlan {
            current_item: &item,
            audio_source: &audio_source,
            video_source: &video_source,
            subtitle_source: None,
            start_at_zero: true,
            realtime: false,
            slate: false,
            is_live: false,
            is_templated: false,
            transcoded_until: item.start,
            stamp_error_ms: 0,
        });
        assert_eq!(planned.video.in_point, Duration::ZERO);
        assert_eq!(planned.video.out_point, limit);
        assert!(!planned.video.is_complete);
        assert_eq!(planned.video.finish, item.start + limit);
    }
}

#[cfg(test)]
mod cosmetic_source_tests {
    use super::*;

    /// The exact failure this guards against: a watermark whose artwork is a
    /// url inherited `reconnect` (which defaults on for http sources), the
    /// image2 demuxer rejected the option, and ffmpeg refused to start,
    /// taking the item down with it.
    #[test]
    fn a_cosmetic_http_source_carries_no_streaming_options() {
        let source = PlayoutItemSource::Http {
            uri: String::from("http://localhost:8409/iptv/logos/gen?text=Test"),
            is_live: Some(true),
            in_point_ms: None,
            out_point_ms: None,
            headers: None,
            user_agent: None,
            timeout_us: None,
            reconnect: None,
            reconnect_delay_max: Some(2),
            keep_alive: Some(true),
            probe_hint: None,
        };

        let PlayoutItemSource::Http {
            reconnect,
            reconnect_delay_max,
            keep_alive,
            is_live,
            uri,
            ..
        } = cosmetic_source(source)
        else {
            panic!("http stays http");
        };

        assert_eq!(reconnect, Some(false));
        assert_eq!(reconnect_delay_max, None);
        assert_eq!(keep_alive, None);
        assert_eq!(is_live, None);
        assert_eq!(uri, "http://localhost:8409/iptv/logos/gen?text=Test");
    }

    #[test]
    fn local_sources_pass_through_unchanged() {
        let source = PlayoutItemSource::Local {
            path: String::from("/bumps/logo.mp4"),
            in_point_ms: None,
            out_point_ms: None,
            probe_hint: None,
        };
        assert!(matches!(
            cosmetic_source(source),
            PlayoutItemSource::Local { path, .. } if path == "/bumps/logo.mp4"
        ));
    }
}
