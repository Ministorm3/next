mod channel_session;
mod dossier;
mod local_proxy;
mod playlist_manager;
mod playout_loader;
mod pts_scanner;

use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use ersatztv_channel::config::ChannelConfig;
use ersatztv_channel::error::ChannelError;
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

#[tokio::main]
pub async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug")).init();

    if let Err(err) = run().await {
        match err {
            ChannelError::IdleTimeout(_) => log::info!("{err}"),
            _ => log::error!("{err}"),
        };

        std::process::exit(1);
    }
}

async fn run() -> Result<(), ChannelError> {
    let args = Args::parse();

    match args.command {
        Commands::Run {
            config_paths,
            output_folder,
            number,
        } => {
            let channel_config =
                ChannelConfig::from_sources(&config_paths, &output_folder, &number).await?;

            // start channel session
            let mut channel_session = ChannelSession::new(channel_config).await?;
            channel_session.run().await
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
