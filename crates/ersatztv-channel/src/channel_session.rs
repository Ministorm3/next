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
    FfmpegInputArgs, HttpInputOptions, HttpInputSource, InputSettings, InputSource,
    LavfiInputSource, LocalInputSource, ProbedInput, RtspInputOptions, RtspInputSource,
    WatermarkInput,
};
use ffpipeline::output_settings::{AudioOutputSettings, OutputSettings, SubtitleMode};
use ffpipeline::pipeline::{AudioFormat, Hz, Kbps, PtsOffset, SEGMENT_SECONDS, VideoFormat};
use ffpipeline::probe::{
    CodecType, ProbeResult, ProbeResultAudioStream, ProbeResultColorParams, ProbeResultStream,
    ProbeResultVideoStream, Probeable,
};
use ffpipeline::web_vtt::Cue;
use ffpipeline::{pipeline, probe};
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

        let pm = self.playlist_manager.clone();
        let tn = self.timeout_notify.clone();

        tokio::spawn(async move {
            // this loop is the only thing that publishes segments to viewers,
            // and it runs every two seconds: report each distinct failure once
            // rather than thirty times a minute, and report recovery, so a
            // persistent fault is visible without burying the log
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
                drop(playlist_manager);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });

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

        let pm = self.playlist_manager.clone();
        let tn = self.timeout_notify.clone();

        tokio::spawn(async move {
            // this loop is the only thing that publishes segments to viewers,
            // and it runs every two seconds: report each distinct failure once
            // rather than thirty times a minute, and report recovery, so a
            // persistent fault is visible without burying the log
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
                drop(playlist_manager);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });

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

        self.transcoded_until =
            (anchor + time::Duration::milliseconds(progress_ms as i64)).min(item.finish);
        self.state = if progress_ms == 0 {
            ChannelSessionState::ZeroAndRealtime
        } else {
            ChannelSessionState::SeekAndRealtime
        };

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
                log::error!("{}", no_item_message(self.transcoded_until, next_start));
                self.fake_playout_item(next_start)
            }
            Err(err) => {
                log::error!("{}", item_unselectable_message(self.transcoded_until, &err));
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
                log::error!("{}", item_failed_message(&current_item, &e));
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

        let audio_fut = self.resolve_probe(&audio_source, &audio_input_source);
        let video_fut = async {
            if audio_source_is_video_source {
                Ok::<_, ChannelError>(None)
            } else {
                self.resolve_probe(&video_source, &video_input_source)
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
                self.resolve_probe(src, s).await.map(Some)
            } else {
                Ok(None)
            }
        };

        let watermark_fut = async {
            if let Some(w) = current_item.watermark.as_ref() {
                // a watermark is cosmetic: an unreadable or unprobeable
                // artwork file must not take down the item it decorates
                let prepared = async {
                    let source = cosmetic_source(w.source.clone());
                    let input_source = self.playout_source_to_input_source(source.clone())?;
                    let probe_result = self.resolve_probe(&source, &input_source).await?;
                    Ok::<_, ChannelError>((input_source, probe_result))
                }
                .await;

                let (input_source, probe_result) = match prepared {
                    Ok(prepared) => prepared,
                    Err(e) => {
                        log::warn!("skipping watermark for item {}: {e}", current_item.id);
                        return Ok(None);
                    }
                };

                let location = playout_location_to_pipeline(&w.location);
                let timing = playout_timing_to_pipeline(w.timing.as_ref());

                Ok(Some(WatermarkInput {
                    input_source,
                    probe_result,
                    stream_index: w.stream_index,
                    location,
                    width_percent: w.width_percent,
                    within_source_content: w.within_source_content,
                    horizontal_margin_percent: w.horizontal_margin_percent,
                    vertical_margin_percent: w.vertical_margin_percent,
                    opacity_percent: w.opacity_percent,
                    timing,
                }))
            } else {
                Ok(None)
            }
        };

        let (audio_probe_result, video_probe_opt, subtitle_probe_opt, watermark_input) =
            tokio::try_join!(audio_fut, video_fut, subtitle_fut, watermark_fut)?;

        let video_probe_result = video_probe_opt.unwrap_or_else(|| audio_probe_result.clone());
        let subtitle_probe_result = if subtitle_source_is_video_source {
            Some(video_probe_result.clone())
        } else {
            subtitle_probe_opt
        };

        let audio_norm = &self.channel_config.normalization.audio;
        let video_norm = &self.channel_config.normalization.video;

        let video_size = match (video_norm.width, video_norm.height) {
            (Some(width), Some(height)) => Some(FrameSize { width, height }),
            _ => None,
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
        let output_settings = OutputSettings {
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
            accel: self.hw_accel.clone(),
            format: ffpipeline::output_format::OutputFormat::Hls {
                playlist: self.output_file.clone(),
                segment_template: self.output_segment_template.clone(),
                troubleshoot,
            },
            pts_offset: pts_duration.map(|duration| PtsOffset { duration }),
            // a templated item may be transcoded in parallel by variant
            // sessions with different query values; padding both transcodes to
            // the -t clamp keeps their PTS envelopes identical, so one can be
            // substituted for the other at the playlist layer
            pad_to_duration: is_templated,
            // slate is never readrate-paced: pacing this padded pipeline runs
            // measurably BELOW realtime on real hardware (0.65x live, 0.80x
            // in isolation, 3.5x unpaced on the same box), which starves the
            // served window for the whole slate slot. Unpaced production is
            // safe here because the run loop's schedule-coordinate throttle
            // sleeps once the completed slot puts the buffer over its cap
            realtime: realtime && !slate,
            is_live,
            frame_rate: if video_probe_result.is_still_image() {
                Some(FrameRate::default())
            } else {
                None
            },
            subtitle_mode: self.channel_config.normalization.subtitle.mode.into(),
            fonts_folder: self
                .channel_config
                .normalization
                .subtitle
                .fonts_folder
                .clone(),
            subtitle_force_style: self
                .channel_config
                .normalization
                .subtitle
                .force_style
                .clone(),
            reports_folder: self.channel_config.ffmpeg.reports_folder.clone(),
            report_id: Some(self.channel_config.number().to_owned()),
        };

        let start_at_zero = matches!(
            self.state,
            ChannelSessionState::ZeroAndWorkAhead | ChannelSessionState::ZeroAndRealtime
        );

        // a slate item must fill its whole remaining window in one pipeline
        // invocation: work-ahead chunking would declare one sidecar envelope
        // per chunk, and a variant reading the first chunk's duration would
        // mistake it for a mid-item join. input_timing's realtime flag only
        // controls that chunking, so slate forces it; output pacing below
        // still follows the real `realtime`
        let whole_window = realtime || slate;
        let audio_timing = self.input_timing(
            current_item,
            &audio_source,
            start_at_zero,
            whole_window,
            is_live,
        );
        let video_timing = self.input_timing(
            current_item,
            &video_source,
            start_at_zero,
            whole_window,
            is_live,
        );
        let subtitle_timing = subtitle_source
            .as_ref()
            .map(|s| self.input_timing(current_item, s, start_at_zero, whole_window, is_live));

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
            watermark_input,
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

        // the envelope this pipeline will fill, computed the same way the
        // pipeline computes its own -t. A variant of this item has to fill
        // the same range, and cannot work it out from the item alone once
        // this session has joined the item partway through
        let pipeline_duration_ms = std::cmp::min(
            audio_timing.out_point.saturating_sub(audio_timing.in_point),
            video_timing.out_point.saturating_sub(video_timing.in_point),
        )
        .as_millis() as u64;

        self.playlist_manager
            .lock()
            .await
            .before_new_pipeline(
                pts_offset,
                subtitle_source,
                &current_item.id,
                pipeline_duration_ms,
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

    fn input_timing(
        &self,
        current_item: &PlayoutItem,
        source: &PlayoutItemSource,
        start_at_zero: bool,
        realtime: bool,
        is_live: bool,
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

        // live content never seeks and is always a complete transcode; a
        // session joining mid-item covers only the remainder, so its output
        // stays inside the item's PTS envelope
        if is_live {
            let live_now = if start_at_zero {
                item_start
            } else {
                self.transcoded_until.clamp(item_start, item_finish)
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
            self.transcoded_until
        };

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

/// The three ways a slot airs black, worded so one grep for
/// `replacing with black/silence` is a complete census and each line names
/// the slot it lost. They are separate faults with separate remedies: a
/// schedule gap is expected, an unreadable playout and a failed transcode
/// are not, and the earlier wording distinguished none of them.
fn no_item_message(at: OffsetDateTime, next_start: Option<OffsetDateTime>) -> String {
    format!(
        "no playout item covers {at}, replacing with black/silence until {}",
        next_start.map_or_else(
            || String::from("the next reload"),
            |start| start.to_string()
        )
    )
}

fn item_unselectable_message(at: OffsetDateTime, error: &ChannelError) -> String {
    format!("no item could be selected for {at}, replacing with black/silence: {error}")
}

fn item_failed_message(item: &PlayoutItem, error: &ChannelError) -> String {
    format!(
        "item {} ({} .. {}) failed, replacing with black/silence: {error}",
        item.id, item.start, item.finish
    )
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
        watermark: item.watermark,
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
            watermark_input: None,
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

    #[test]
    fn a_variant_envelope_always_ends_with_the_shared_one() {
        // the invariant the whole substitution rests on, over a spread of
        // join distances and coverage points
        for shared_duration in [1_000u64, 4_755, 58_580, 112_999, 113_000] {
            for progress in [0u64, 1, 4_000, 16_000, 58_000] {
                let (pts_from, duration) = variant_envelope(113_000, shared_duration, progress);
                if progress <= shared_duration {
                    assert_eq!(
                        pts_from + duration,
                        shared_duration,
                        "shared={shared_duration} progress={progress}"
                    );
                }
            }
        }
    }
    /// One grep has to find every black-air line, and each has to say which
    /// slot it lost: 144 anonymous lines cannot be told apart from one slot
    /// failing 144 times.
    #[test]
    fn every_black_air_line_names_its_slot_and_shares_one_phrase() {
        let item = templated_item();
        let at = item.start;

        let messages = [
            no_item_message(at, Some(item.finish)),
            item_unselectable_message(at, &ChannelError::CaptureFFmpegStderrFailure),
            item_failed_message(&item, &ChannelError::CaptureFFmpegStderrFailure),
        ];

        for message in &messages {
            assert!(
                message.contains("replacing with black/silence"),
                "black-air line is not greppable: {message}"
            );
        }

        assert!(messages[0].contains(&at.to_string()));
        assert!(messages[0].contains(&item.finish.to_string()));
        assert!(messages[1].contains(&at.to_string()));
        assert!(messages[2].contains(&item.id));
        assert!(messages[2].contains(&item.start.to_string()));
        assert!(messages[2].contains(&item.finish.to_string()));
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
