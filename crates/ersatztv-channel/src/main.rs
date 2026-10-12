mod channel_session;
mod dossier;
mod dynamic_retry;
mod fallback;
mod local_proxy;
mod logging;
mod playlist_manager;
mod playout_loader;
mod pts_scanner;

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use ersatztv_channel::config::ChannelConfig;
use ersatztv_channel::error::ChannelError;
use ersatztv_core::SHUTDOWN_DEADLINE;
use ersatztv_core::process::{kill_descendants_on_exit, parent_exit, shutdown_signal};
use ffpipeline::ffmpeg_info::FfmpegInfo;

use crate::channel_session::ChannelSession;

#[derive(Parser, Debug)]
#[command(version = ersatztv_core::VERSION, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Print debug information using the provided configuration
    Debug {
        #[arg(required = true, num_args = 1..)]
        config_paths: Vec<PathBuf>,
    },
    /// Run the channel using the provided configuration
    Run {
        #[arg(required = true, num_args = 1..)]
        config_paths: Vec<PathBuf>,
        #[arg(short, long)]
        output_folder: PathBuf,
        #[arg(short, long)]
        number: String,
        #[arg(short, long)]
        troubleshoot: bool,
        /// Keep running if the parent process exits
        #[arg(long)]
        detached: bool,
    },
    /// Transcode a single playout item as a stream variant, with cohort
    /// query values steering its templated URL
    Variant {
        #[arg(required = true, num_args = 1..)]
        config_paths: Vec<PathBuf>,
        #[arg(short, long)]
        output_folder: PathBuf,
        #[arg(short, long)]
        number: String,
        /// Id of the playout item to transcode
        #[arg(long)]
        item_id: String,
        /// The -output_ts_offset the shared session used for this item, so
        /// both transcodes occupy the same PTS envelope
        #[arg(long)]
        pts_offset_ms: u64,
        /// How far into the item the shared session's published coverage
        /// already extends; the variant anchors just past it on the same
        /// segment grid
        #[arg(long, default_value_t = 0)]
        progress_ms: u64,
        /// How much output the shared session declared for this item. The
        /// variant fills the same envelope, which is shorter than the item
        /// whenever the shared session joined the item partway through
        #[arg(long, default_value_t = 0)]
        shared_duration_ms: u64,
        /// Cohort query values as a url-encoded query string,
        /// e.g. "region=west&lang=en"
        #[arg(long, default_value = "")]
        params: String,
    },
}

pub fn main() -> ExitCode {
    // first, so a parent that dies during startup is still seen
    let parent_exit = parent_exit();

    // clap may exit here; nothing is spawned yet
    let args = Args::parse();

    logging::init();
    let span = match &args.command {
        Commands::Run { number, .. } | Commands::Variant { number, .. } => {
            tracing::info_span!("channel", number = %number)
        }
        Commands::Debug { .. } => tracing::Span::none(),
    };
    // tasks spawned from here need .in_current_span() to keep the channel number
    let _span = span.enter();

    kill_descendants_on_exit();

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => {
            log::error!("failed to start runtime: {err}");
            return ExitCode::FAILURE;
        }
    };

    // a variant's parent is the channel worker that owns its session
    let watch_parent = matches!(
        args.command,
        Commands::Run {
            detached: false,
            ..
        } | Commands::Variant { .. }
    );

    let result = runtime.block_on(run_until_shutdown(args, watch_parent, parent_exit));

    // process::exit skips drops, so kill_on_drop children would outlive us
    runtime.shutdown_timeout(SHUTDOWN_DEADLINE);

    match result {
        Ok(()) => ExitCode::SUCCESS,
        // no viewers is not a failure; supervisors treat non-zero as a crash
        Err(err @ ChannelError::IdleTimeout(_)) => {
            log::info!("{err}");
            ExitCode::SUCCESS
        }
        Err(err) => {
            log::error!("{err}");
            ExitCode::FAILURE
        }
    }
}

async fn run_until_shutdown(
    args: Args,
    watch_parent: bool,
    parent_exit: impl Future<Output = ()>,
) -> Result<(), ChannelError> {
    // tokio never restores the default signal action, so a second SIGTERM can't kill us;
    // shutdown_timeout is the backstop
    tokio::select! {
        result = run(args) => result,
        signal = shutdown_signal() => {
            // dropping run() killed ffmpeg
            log::info!("received {signal}; shutting down");
            Ok(())
        }
        // a killed parent can't stop us, and .heartbeat goes stale
        _ = parent_exit, if watch_parent => {
            log::warn!("parent process exited; shutting down");
            Ok(())
        }
    }
}

async fn run(args: Args) -> Result<(), ChannelError> {
    match args.command {
        Commands::Run {
            config_paths,
            output_folder,
            number,
            troubleshoot,
            ..
        } => {
            let channel_config =
                ChannelConfig::from_sources(&config_paths, &output_folder, &number).await?;

            // start channel session
            let mut channel_session = ChannelSession::new(channel_config).await?;
            channel_session.run(troubleshoot).await
        }
        Commands::Variant {
            config_paths,
            output_folder,
            number,
            item_id,
            pts_offset_ms,
            progress_ms,
            shared_duration_ms,
            params,
        } => {
            let channel_config =
                ChannelConfig::from_sources(&config_paths, &output_folder, &number).await?;

            let query_parameters: std::collections::HashMap<String, String> =
                url::form_urlencoded::parse(params.as_bytes())
                    .into_owned()
                    .collect();

            let mut channel_session = ChannelSession::new(channel_config)
                .await?
                .with_query_parameters(query_parameters);
            channel_session
                .run_variant(&item_id, pts_offset_ms, progress_ms, shared_duration_ms)
                .await
        }
        Commands::Debug { config_paths } => {
            let channel_config =
                ChannelConfig::from_sources(&config_paths, &std::env::temp_dir(), "debug").await?;

            log::debug!("{:?}", channel_config);

            let ffmpeg_path = channel_config
                .ffmpeg
                .ffmpeg_path
                .as_deref()
                .unwrap_or(Path::new("ffmpeg"));
            let ffmpeg_info = FfmpegInfo::load(
                ffmpeg_path,
                &channel_config.ffmpeg.disabled_filters,
                &channel_config.ffmpeg.preferred_filters,
            )
            .await?;

            log::debug!("{:?}", ffmpeg_info);

            if let Some(accel) = &channel_config.normalization.video.accel {
                let _ = accel.to_pipeline(&channel_config);
            }

            Ok(())
        }
    }
}
