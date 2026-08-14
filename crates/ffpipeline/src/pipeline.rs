use std::fmt::Formatter;
use std::path::PathBuf;
use std::time::Duration;

use serde::Serialize;
use strum::{Display, EnumString};

use crate::ArgVec;
use crate::audio_codec::AudioCodec;
use crate::audio_decoder::AudioDecoder;
use crate::audio_filter::AudioFilter;
use crate::error::FFPipelineError;
use crate::ffmpeg_info::FfmpegInfo;
use crate::filter_chain::{FilterChain, PipelineFilter};
use crate::frame_rate::FrameRate;
use crate::frame_size::FrameSize;
use crate::global_option::{GlobalOption, LogLevel};
use crate::hw_accel::{HardwareAccel, HwAccel};
use crate::input::{FfmpegInputArgs, GraphicsInput, InputSettings, InputSource};
use crate::output_option::OutputOption;
use crate::output_settings::{
    OutputSettings, ScalingMode, SubtitleMode, VideoFilterOptions, YadifOptions,
};
use crate::overlay_filter::{OverlayFilter, OverlaySource, SoftwareOverlay};
use crate::probe::ProbeResultVideoStream;
use crate::video_codec::VideoCodec;
use crate::video_decoder::VideoDecoder;
use crate::video_filter::{
    ColorChannelMixerFilter, CropFilter, DeinterlaceFilter, Dv5WorkaroundFilter, FadeFilter,
    FormatFilter, LoopFilter, PadFilter, ScaleFilter, SoftwareDeinterlaceFilter,
    SoftwareDeinterlaceOptions, SubtitleImageScaleFilter, SubtitlesFilter, TPadFilter,
    ToneMapFilter, VideoFilter,
};

pub const KEYFRAME_INTERVAL_SECONDS: u32 = 2;
pub const SEGMENT_SECONDS: u32 = 4;

#[derive(Debug, Clone, Copy, Display, EnumString)]
#[strum(serialize_all = "lowercase")]
pub enum AudioFormat {
    Aac,
    Ac3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Kbps(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Hz(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, EnumString, Serialize)]
#[strum(serialize_all = "lowercase")]
pub enum VideoFormat {
    Av1,
    H264,
    Hevc,
    Mpeg2Video,
    Vc1,
    Vp8,
    Vp9,
}

#[derive(Debug, Copy, Clone)]
pub struct PtsOffset {
    pub duration: Duration,
}

impl Default for PtsOffset {
    fn default() -> Self {
        PtsOffset {
            duration: Duration::ZERO,
        }
    }
}

pub(crate) struct OutputContext {
    pub(crate) media_frame_rate: FrameRate,
    pub(crate) audio_codec: AudioCodec,
    pub(crate) audio_channels: Option<u32>,
    pub(crate) video_codec: VideoCodec,
    pub(crate) pts_offset: Option<PtsOffset>,
    pub(crate) preferred_surface: FrameSurface,
    pub(crate) preferred_pixel_format: Option<PixelFormat>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::Display)]
pub enum FrameSurface {
    System,
    Amf,
    Cuda,
    Qsv,
    Rkmpp,
    Vaapi,
    VideoToolbox,
    Vulkan,
    OpenCL,
}

impl FrameSurface {
    pub(crate) fn device_name(&self) -> Option<&'static str> {
        match self {
            FrameSurface::Amf => Some("amf"),
            FrameSurface::Cuda => Some("cuda"),
            FrameSurface::OpenCL => Some("opencl"),
            FrameSurface::Qsv => Some("qsv"),
            FrameSurface::Rkmpp => Some("rkmpp"),
            FrameSurface::Vaapi => Some("vaapi"),
            FrameSurface::Vulkan => Some("vulkan"),
            FrameSurface::VideoToolbox => Some("videotoolbox"),
            FrameSurface::System => None,
        }
    }
}

pub type SurfaceSet = std::collections::HashSet<FrameSurface>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PixelFormat {
    Bgra,
    Yuv420p,
    Yuv420p10le,
    Yuva420p,
    Yuva420p10le,
    Nv12,
    Nv15,
    P010le,
    P016,
}

gen_subset!(HwPixelFormat, PixelFormat, Nv12, Nv15, P010le);

impl PixelFormat {
    pub(crate) fn parse(pix_fmt: &str) -> PixelFormat {
        match pix_fmt.to_lowercase().as_str() {
            "bgra" => PixelFormat::Bgra,
            "yuv420p" => PixelFormat::Yuv420p,
            "yuv420p10le" => PixelFormat::Yuv420p10le,
            "yuva420p" => PixelFormat::Yuva420p,
            "yuva420p10le" => PixelFormat::Yuva420p10le,
            "nv12" => PixelFormat::Nv12,
            "nv15" => PixelFormat::Nv15,
            "p010le" => PixelFormat::P010le,
            _ => {
                log::warn!("assuming unknown pixel format {} is yuv420p", pix_fmt);
                PixelFormat::Yuv420p
            }
        }
    }

    pub(crate) fn bit_depth(&self) -> u8 {
        match self {
            PixelFormat::Bgra
            | PixelFormat::Yuv420p
            | PixelFormat::Yuva420p
            | PixelFormat::Nv12 => 8,
            PixelFormat::Yuv420p10le
            | PixelFormat::Yuva420p10le
            | PixelFormat::P010le
            | PixelFormat::Nv15 => 10,
            PixelFormat::P016 => 16,
        }
    }

    pub(crate) fn has_alpha(&self) -> bool {
        matches!(
            self,
            PixelFormat::Bgra | PixelFormat::Yuva420p | PixelFormat::Yuva420p10le
        )
    }

    pub(crate) fn as_arg(&self) -> &str {
        match self {
            PixelFormat::Bgra => "bgra",
            PixelFormat::Yuv420p => "yuv420p",
            PixelFormat::Yuv420p10le => "yuv420p10le",
            PixelFormat::Yuva420p => "yuva420p",
            PixelFormat::Yuva420p10le => "yuva420p10le",
            PixelFormat::Nv12 => "nv12",
            PixelFormat::Nv15 => "nv15",
            PixelFormat::P010le => "p010le",
            PixelFormat::P016 => "p016",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum HdrFormat {
    None,
    Pq,
    Hlg,
    Dv5,
}

#[derive(Clone, Debug, derive_more::Display)]
#[display(
    "FrameState(size={},is_anamorphic={},surface={})",
    size,
    is_anamorphic,
    surface
)]
pub struct FrameState {
    pub(crate) size: FrameSize,
    pub(crate) is_anamorphic: bool,
    pub(crate) is_interlaced: bool,
    pub(crate) sample_aspect_ratio: Option<String>,
    pub(crate) display_aspect_ratio: Option<String>,
    pub(crate) surface: FrameSurface,
    pub(crate) pixel_format: PixelFormat,
    pub(crate) hdr_format: HdrFormat,
}

pub enum PipelineInput {
    Audio {
        input_source: InputSource,
        index: u32,
        path: String,
        seek: Duration,
        channels: u32,
        decoder: AudioDecoder,
        loop_when_exhausted: bool,
    },
    Video {
        input_source: InputSource,
        index: u32,
        path: String,
        seek: Duration,
        realtime: bool,
        decoder: VideoDecoder,
        loop_when_exhausted: bool,
    },
    Subtitle {
        input_source: InputSource,
        index: u32,
        path: String,
        seek: Duration,
    },
    Graphics {
        input: GraphicsInput,
        layer_index: usize,
        index: u32,
        path: String,
        extra_input_args: ArgVec,
    },
}

impl PipelineInput {
    fn sort_order(&self) -> usize {
        match self {
            PipelineInput::Video { .. } => 0,
            PipelineInput::Audio { .. } => 1,
            PipelineInput::Subtitle { .. } => 2,
            PipelineInput::Graphics { layer_index, .. } => 3 + *layer_index,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EnvironmentVariable {
    pub key: String,
    pub value: String,
}

pub struct Pipeline {
    ffmpeg_info: FfmpegInfo,
    accel: Option<HardwareAccel>,
    filter_options: VideoFilterOptions,
    initial_state: FrameState,

    global_options: Vec<GlobalOption>,
    inputs: Vec<PipelineInput>,
    filter_chain: FilterChain,
    output_options: Vec<OutputOption>,
    env_vars: Vec<EnvironmentVariable>,

    output_context: OutputContext,
}

impl Pipeline {
    fn full(
        ffmpeg_info: &FfmpegInfo,
        input_settings: InputSettings,
        output_settings: OutputSettings,
    ) -> Result<Pipeline, FFPipelineError> {
        let mut final_output_settings = output_settings;

        if let Some(accel) = &final_output_settings.accel
            && accel
                .known_accel()
                .map(|a| !ffmpeg_info.has_hw_accel(a))
                .unwrap_or(false)
        {
            log::warn!("ffmpeg does not support requested accel {:?}", accel);
            final_output_settings.accel = None;
        }

        let duration = std::cmp::min(
            input_settings.audio_input.out_point - input_settings.audio_input.in_point,
            input_settings.video_input.out_point - input_settings.video_input.in_point,
        );

        let audio_codec = match final_output_settings.audio.format {
            Some(AudioFormat::Aac) => AudioCodec::Aac,
            Some(AudioFormat::Ac3) => AudioCodec::Ac3,
            _ => AudioCodec::Copy,
        };

        let video_stream = input_settings.select_video_stream()?;
        let audio_stream = input_settings.select_audio_stream()?;
        let subtitle_stream = input_settings.select_subtitle_stream();
        let graphics_streams: Vec<_> = input_settings
            .graphics_inputs
            .iter()
            .map(|input| input_settings.select_graphics_stream(input))
            .collect();

        // TODO: add target profile to config
        let video_codec = match (
            final_output_settings.accel.as_ref(),
            final_output_settings.video_format,
        ) {
            (Some(a), Some(format)) => a
                .codec_for_format(
                    &format,
                    final_output_settings.bit_depth.unwrap_or(8),
                    final_output_settings.video_size,
                )
                .filter(|_| a.can_encode(&format, final_output_settings.bit_depth.unwrap_or(8)))
                .unwrap_or(match format {
                    VideoFormat::Hevc => VideoCodec::libx265(),
                    VideoFormat::H264 => VideoCodec::libx264(),
                    _ => VideoCodec::copy(),
                }),
            (_, Some(VideoFormat::H264)) => VideoCodec::libx264(),
            (_, Some(VideoFormat::Hevc)) => VideoCodec::libx265(),
            _ => VideoCodec::copy(),
        };

        let is_still_image = input_settings.video_input.probe_result.is_still_image();
        let video_decoder = VideoDecoder::new(
            ffmpeg_info,
            video_stream,
            is_still_image,
            &final_output_settings,
        );

        let hdr = match (
            video_stream.dv_profile,
            video_stream.color_params.color_transfer.as_deref(),
        ) {
            (Some(5), _) => HdrFormat::Dv5,
            (_, Some("smpte2084")) => HdrFormat::Pq,
            (_, Some("arib-std-b67")) => HdrFormat::Hlg,
            _ => HdrFormat::None,
        };

        let initial_state = FrameState {
            size: FrameSize {
                width: video_stream
                    .width
                    .ok_or(FFPipelineError::VideoInputIsRequired)?,
                height: video_stream
                    .height
                    .ok_or(FFPipelineError::VideoInputIsRequired)?,
            },
            is_anamorphic: video_stream.is_anamorphic(),
            // if user does not want to deinterlace, pretend content is not interlaced
            is_interlaced: final_output_settings.deinterlace && video_stream.is_interlaced(),
            sample_aspect_ratio: video_stream.sample_aspect_ratio.to_owned(),
            display_aspect_ratio: video_stream.display_aspect_ratio.to_owned(),
            surface: video_decoder.output_surface(),
            pixel_format: video_decoder
                .output_format(&PixelFormat::parse(video_stream.pix_fmt.as_str())),
            hdr_format: hdr,
        };

        let preferred_pixel_format = match final_output_settings.bit_depth {
            Some(10) => video_codec.preferred_pixel_format_10bit,
            Some(8) => video_codec.preferred_pixel_format_8bit,
            _ => None,
        };

        let output_context = OutputContext {
            audio_codec,
            audio_channels: final_output_settings.audio.channels,
            video_codec: video_codec.clone(),
            pts_offset: final_output_settings.pts_offset,
            media_frame_rate: video_stream.frame_rate.to_owned(),
            preferred_surface: video_codec.preferred_surface,
            preferred_pixel_format,
        };

        let mut filters = vec![
            PipelineFilter::Audio(AudioFilter::LoudNorm {
                settings: final_output_settings.audio.loudness.clone(),
                sample_rate: final_output_settings.audio.sample_rate,
            }),
            PipelineFilter::Audio(AudioFilter::Resample),
            PipelineFilter::Audio(AudioFilter::Pad),
        ];

        if final_output_settings.pad_to_duration {
            // pad an under-running source to the output -t clamp by cloning
            // its last frame, so the item still fills its scheduled duration
            // (audio is always padded; see AudioFilter::Pad)
            filters.push(PipelineFilter::Video(TPadFilter.into()));
        }

        filters.extend([
            PipelineFilter::Video(LoopFilter { is_still_image }.into()),
            PipelineFilter::Video(Dv5WorkaroundFilter.into()),
            PipelineFilter::Video(
                ToneMapFilter {
                    algorithm: final_output_settings.filter_options.tonemap.tonemap.clone(),
                    output_format: match final_output_settings.bit_depth {
                        Some(10) => PixelFormat::Yuv420p10le,
                        _ => PixelFormat::Yuv420p,
                    },
                }
                .into(),
            ),
            PipelineFilter::Video(
                DeinterlaceFilter {
                    filter: SoftwareDeinterlaceFilter::Yadif(YadifOptions::default()),
                    options: SoftwareDeinterlaceOptions {
                        bwdif: final_output_settings.filter_options.bwdif.clone(),
                        w3fdif: final_output_settings.filter_options.w3fdif.clone(),
                        yadif: final_output_settings.filter_options.yadif.clone(),
                    },
                    input_is_interlaced: initial_state.is_interlaced,
                }
                .into(),
            ),
            PipelineFilter::Video(
                ScaleFilter {
                    size: final_output_settings.video_size,
                    scaling_mode: final_output_settings.scaling_mode,
                    input_is_anamorphic: initial_state.is_anamorphic,
                    force_original_aspect_ratio: None,
                }
                .into(),
            ),
            PipelineFilter::Video(
                PadFilter {
                    size: final_output_settings.video_size.to_owned(),
                    scaling_mode: final_output_settings.scaling_mode,
                }
                .into(),
            ),
            PipelineFilter::Video(
                CropFilter {
                    size: final_output_settings.video_size.to_owned(),
                    scaling_mode: final_output_settings.scaling_mode,
                }
                .into(),
            ),
        ]);

        let mut inputs = vec![
            PipelineInput::Audio {
                input_source: input_settings.audio_input.input_source.to_owned(),
                index: audio_stream.stream_index,
                path: input_settings.audio_input.probe_result.path.to_owned(),
                seek: input_settings.audio_input.in_point,
                channels: audio_stream.channels,
                decoder: AudioDecoder::new(audio_stream, &final_output_settings),
                loop_when_exhausted: input_settings.audio_input.loop_when_exhausted,
            },
            PipelineInput::Video {
                input_source: input_settings.video_input.input_source.to_owned(),
                index: video_stream.stream_index,
                path: input_settings.video_input.probe_result.path.to_owned(),
                seek: if is_still_image {
                    Duration::ZERO
                } else {
                    input_settings.video_input.in_point
                },
                realtime: final_output_settings.realtime && !final_output_settings.is_live,
                decoder: video_decoder,
                loop_when_exhausted: input_settings.video_input.loop_when_exhausted,
            },
        ];

        if let Some(subtitle_stream) = subtitle_stream
            && let Some(subtitle_input) = input_settings.subtitle_input.as_ref()
        {
            if subtitle_stream.is_subtitle_image()
                && let Some(size) = final_output_settings.video_size
            {
                inputs.push(PipelineInput::Subtitle {
                    input_source: subtitle_input.input_source.to_owned(),
                    index: subtitle_stream.stream_index,
                    path: subtitle_input.probe_result.path.to_owned(),
                    seek: subtitle_input.in_point,
                });

                let secondary_initial_state = FrameState {
                    size,
                    is_anamorphic: subtitle_stream.is_anamorphic(),
                    is_interlaced: false,
                    sample_aspect_ratio: subtitle_stream.sample_aspect_ratio.to_owned(),
                    display_aspect_ratio: subtitle_stream.display_aspect_ratio.to_owned(),
                    surface: FrameSurface::System,
                    pixel_format: if subtitle_stream.pix_fmt.is_empty() {
                        PixelFormat::Bgra
                    } else {
                        PixelFormat::parse(&subtitle_stream.pix_fmt)
                    },
                    hdr_format: HdrFormat::None,
                };

                filters.push(PipelineFilter::Overlay(OverlayFilter {
                    kind: SoftwareOverlay::default().into(),
                    secondary: vec![SubtitleImageScaleFilter { size }.into()],
                    secondary_initial_state,
                    secondary_source: OverlaySource::Subtitle,
                    location: None,
                }));
            } else if !subtitle_stream.is_subtitle_image()
                && final_output_settings.subtitle_mode == SubtitleMode::Burn
            {
                // only use force_style with SRT, which doesn't have any styling of its own
                let mut final_force_style = None;
                if subtitle_stream.codec == "srt" || subtitle_stream.codec == "subrip" {
                    final_force_style = final_output_settings.subtitle_force_style;
                }

                filters.push(PipelineFilter::Video(
                    SubtitlesFilter {
                        path: subtitle_input.probe_result.path.to_owned(),
                        seek: subtitle_input.in_point,
                        fonts_folder: final_output_settings.fonts_folder.to_owned(),
                        force_style: final_force_style,
                    }
                    .into(),
                ))
            }
        }

        for (graphics_input, graphics_stream) in
            input_settings.graphics_inputs.iter().zip(graphics_streams)
        {
            let Some(graphics_stream) = graphics_stream else {
                return Err(FFPipelineError::GraphicsStreamNotFound(
                    graphics_input.layer_index,
                ));
            };
            let (Some(height), Some(width)) = (graphics_stream.height, graphics_stream.width)
            else {
                return Err(FFPipelineError::GraphicsStreamNotFound(
                    graphics_input.layer_index,
                ));
            };
            // upstream inlines these three branches. The fork keeps them in
            // `watermark_input_args` because its still-image branch also
            // passes `-f image2`, so calling the helper here preserves that.
            // Dropping it would be a behavior change riding along with a
            // merge, which is not what this commit is for
            let extra_input_args = watermark_input_args(
                graphics_stream,
                &output_context.media_frame_rate.r_frame_rate,
                duration,
            );

            inputs.push(PipelineInput::Graphics {
                input: graphics_input.clone(),
                layer_index: graphics_input.layer_index,
                index: graphics_stream.stream_index,
                path: graphics_input.probe_result.path.to_owned(),
                extra_input_args,
            });

            let secondary_initial_state = FrameState {
                size: FrameSize { width, height },
                is_anamorphic: false,
                is_interlaced: false,
                sample_aspect_ratio: Some(String::from("1:1")),
                display_aspect_ratio: None,
                surface: FrameSurface::System,
                pixel_format: if graphics_stream.pix_fmt.is_empty() {
                    PixelFormat::Bgra
                } else {
                    PixelFormat::parse(&graphics_stream.pix_fmt)
                },
                hdr_format: HdrFormat::None,
            };

            let video_size = final_output_settings
                .video_size
                .as_ref()
                .unwrap_or(&initial_state.size);

            let source_content_size = match final_output_settings.scaling_mode {
                ScalingMode::ScaleAndPad => video_size.square_pixel_size_contain(&initial_state),
                ScalingMode::Crop | ScalingMode::Stretch => *video_size,
            };

            let scaled_size = graphics_input.scaled_size(
                FrameSize { width, height },
                final_output_settings.video_size,
            );

            let location =
                Some(graphics_input.frame_location(&source_content_size, &scaled_size, video_size));

            let mut secondary_filters: Vec<VideoFilter> = vec![
                ColorChannelMixerFilter {
                    alpha: graphics_input.opacity_percent.unwrap_or(100f32) / 100.0f32,
                }
                .into(),
                FormatFilter {
                    format: match secondary_initial_state.pixel_format.bit_depth() {
                        10 => PixelFormat::Yuva420p10le,
                        _ => PixelFormat::Yuva420p,
                    },
                }
                .into(),
                ScaleFilter {
                    size: Some(scaled_size),
                    scaling_mode: ScalingMode::ScaleAndPad,
                    input_is_anamorphic: false,
                    force_original_aspect_ratio: None,
                }
                .into(),
            ];

            let fade_filters = FadeFilter::for_graphics(
                graphics_input.timing.as_ref(),
                input_settings.start,
                input_settings.playout_offset,
                duration,
            );

            secondary_filters.extend(fade_filters.iter().map(|f| f.clone().into()));

            filters.push(PipelineFilter::Overlay(OverlayFilter {
                kind: SoftwareOverlay::default().into(),
                secondary: secondary_filters,
                secondary_initial_state,
                secondary_source: OverlaySource::Graphics(graphics_input.layer_index),
                location,
            }));
        }

        let mut env_vars = Vec::new();

        if let Some(reports_folder) = final_output_settings
            .reports_folder
            .as_deref()
            .filter(|s| !s.is_empty())
            && let Some(report_id) = final_output_settings
                .report_id
                .as_deref()
                .filter(|s| !s.is_empty())
        {
            let folder = PathBuf::from(reports_folder);
            if let Err(err) = std::fs::create_dir_all(&folder) {
                log::warn!("failed to create ffmpeg reports folder: {err}; will not save report");
            } else {
                let file = folder
                    .join(format!(".in-flight-{}.log", report_id))
                    .to_string_lossy()
                    .to_string()
                    .replace(r"%", r"%%");

                #[cfg(target_os = "windows")]
                let mut file = file;

                #[cfg(target_os = "windows")]
                {
                    file = file.replace(r"\", r"/").replace(r":/", r"\:/");
                }

                env_vars = vec![EnvironmentVariable {
                    key: String::from("FFREPORT"),
                    value: format!("file={file}:level=32"),
                }]
            }
        }

        Ok(Pipeline {
            ffmpeg_info: ffmpeg_info.clone(),
            accel: final_output_settings.accel.clone(),
            filter_options: final_output_settings.filter_options,
            initial_state: initial_state.clone(),
            global_options: vec![
                // hardware accel should use a single thread
                GlobalOption::Threads(match &final_output_settings.accel {
                    Some(_) => 1,
                    _ => 0,
                }),
                GlobalOption::NoStdIn,
                GlobalOption::HideBanner,
                GlobalOption::LogLevel(LogLevel::Error),
                GlobalOption::StandardFormatFlags,
            ],
            inputs,
            filter_chain: FilterChain::new(filters),
            output_options: vec![
                OutputOption::NoDemuxDecodeDelay,
                OutputOption::MovFlagsFastStart,
                OutputOption::CudaNoAutoScale,
                OutputOption::AudioCodec(audio_codec),
                OutputOption::AudioBitrate(final_output_settings.audio.bitrate),
                OutputOption::AudioBuffer(final_output_settings.audio.buffer),
                OutputOption::AudioChannels(final_output_settings.audio.channels),
                OutputOption::AudioSampleRate(final_output_settings.audio.sample_rate),
                OutputOption::VideoCodec(video_codec),
                OutputOption::VideoBitrate(final_output_settings.video_bitrate),
                OutputOption::VideoBuffer(final_output_settings.video_buffer),
                OutputOption::DoNotMapMetadata,
                OutputOption::Duration(duration),
                OutputOption::TsOffset(final_output_settings.pts_offset),
                OutputOption::VideoTrackTimeScale(90_000),
                OutputOption::FrameRate(final_output_settings.frame_rate.clone()),
                OutputOption::Format(final_output_settings.format),
            ],
            output_context,
            env_vars,
        })
    }

    pub fn optimize(&mut self) {
        // audio copy shouldn't have bitrate etc
        if self.output_context.audio_codec == AudioCodec::Copy {
            self.output_options.retain(|o| {
                !matches!(
                    o,
                    OutputOption::AudioBitrate(_)
                        | OutputOption::AudioBuffer(_)
                        | OutputOption::AudioChannels(_)
                        | OutputOption::AudioSampleRate(_)
                )
            });

            self.filter_chain.disable_audio();
        };

        // remove audio channels output option if input channel count matches
        if let Some(audio_channels) = self.inputs.iter().find_map(|s| match s {
            PipelineInput::Audio { channels, .. } => Some(channels),
            _ => None,
        }) && Some(audio_channels) == self.output_context.audio_channels.as_ref()
        {
            self.output_options
                .retain(|o| !matches!(o, OutputOption::AudioChannels(_)));
        }

        // video copy shouldn't have bitrate, etc
        if self.output_context.video_codec.codec_name == VideoCodec::COPY {
            self.output_options.retain(|o| {
                !matches!(
                    o,
                    OutputOption::VideoBitrate(_) | OutputOption::VideoBuffer(_)
                )
            });

            self.filter_chain.disable_video();
        }

        self.filter_chain
            .evaluate(&self.initial_state, &self.ffmpeg_info);
        self.filter_chain.resolve(
            &self.ffmpeg_info,
            &self.accel,
            &self.filter_options,
            &self.initial_state,
            &self.output_context.preferred_surface,
            &self.output_context.preferred_pixel_format,
        );

        // prepend decoder filters;
        // this is a special case that's only really needed for CUDA's hwupload workaround
        if let Some(video_decoder) = self.inputs.iter().find_map(|s| match s {
            PipelineInput::Video { decoder, .. } => Some(decoder),
            _ => None,
        }) {
            self.filter_chain.prepend(video_decoder.filters());
        }

        self.filter_chain.optimize();

        if let Some(accel) = &self.accel {
            let mut surfaces = self.filter_chain.surfaces().clone();
            surfaces.insert(self.initial_state.surface);
            surfaces.insert(self.output_context.preferred_surface);
            if surfaces.iter().any(|s| *s != FrameSurface::System) {
                let args = accel.init_hw_device(&surfaces);
                self.global_options.push(GlobalOption::InitHwDevice(args));
            }
        }
    }

    pub fn args(&self) -> ArgVec {
        let mut result: ArgVec = Vec::new();

        let mut audio_label = String::from("0:a");
        let mut video_label = String::from("0:v");
        let mut subtitle_label = None;
        let mut graphics_labels = vec![
            None;
            self.inputs
                .iter()
                .filter_map(|i| match i {
                    PipelineInput::Graphics { layer_index, .. } => Some(*layer_index),
                    _ => None,
                })
                .max()
                .map_or(0, |i| i + 1)
        ];

        let mut input_paths: Vec<&str> = Vec::new();

        let mut sorted_inputs: Vec<&PipelineInput> = self.inputs.iter().collect();
        sorted_inputs.sort_by_key(|i| i.sort_order());

        result.extend(self.global_options.iter().flat_map(|o| o.as_arg()));

        for input in sorted_inputs.iter() {
            match input {
                PipelineInput::Video {
                    input_source,
                    index,
                    path,
                    seek,
                    realtime,
                    decoder,
                    loop_when_exhausted,
                    ..
                } => {
                    input_paths.push(path.as_str());

                    result.extend(decoder.as_arg());

                    let video_input_index = input_paths.iter().position(|p| p == path).unwrap_or(0);
                    video_label = format!("{}:{}", video_input_index, index);

                    result.extend(loop_input_args(*loop_when_exhausted));

                    if !seek.is_zero() {
                        result.extend(args!["-ss", format!("{}ms", seek.as_millis())]);
                    }

                    if *realtime {
                        result.extend(args!["-readrate", "1.0"]);
                    }

                    result.extend(input_source.args_for_input());
                    // TODO: if audio has same input and args, should use here

                    result.extend(args!["-i", path.to_owned()]);
                }
                PipelineInput::Audio {
                    input_source,
                    index,
                    path,
                    decoder,
                    loop_when_exhausted,
                    ..
                } => {
                    // if we haven't yet used this input, add it
                    if !input_paths.contains(&path.as_str()) {
                        input_paths.push(path.as_str());

                        result.extend(decoder.as_arg());

                        // TODO: seek?

                        result.extend(loop_input_args(*loop_when_exhausted));

                        result.extend(input_source.args_for_input());
                        result.extend(args!["-i", path.to_owned()]);
                    }

                    let audio_input_index = input_paths.iter().position(|p| p == path).unwrap_or(0);
                    audio_label = format!("{}:{}", audio_input_index, index);
                }
                PipelineInput::Subtitle {
                    input_source,
                    index,
                    path,
                    seek,
                    ..
                } => {
                    if !input_paths.contains(&path.as_str()) {
                        input_paths.push(path.as_str());

                        if !seek.is_zero() {
                            result.extend(args!["-ss", format!("{}ms", seek.as_millis())]);
                        }

                        result.extend(input_source.args_for_input());
                        result.extend(args!["-i", path.to_owned()]);
                    }

                    let subtitle_input_index =
                        input_paths.iter().position(|p| p == path).unwrap_or(0);
                    subtitle_label = Some(format!("{}:{}", subtitle_input_index, index));
                }
                PipelineInput::Graphics {
                    input,
                    layer_index,
                    index,
                    path,
                    extra_input_args,
                } => {
                    input_paths.push(path.as_str());
                    result.extend(input.input_source.args_for_input());
                    result.extend(extra_input_args.clone());
                    result.extend(args!["-i", path.to_owned()]);
                    let graphics_input_index = input_paths.len() - 1;
                    graphics_labels[*layer_index] =
                        Some(format!("{}:{}", graphics_input_index, index));
                }
            }
        }

        let mut filter_chain = self.filter_chain.to_owned();
        filter_chain.build(
            &audio_label,
            &video_label,
            subtitle_label.as_ref(),
            &graphics_labels,
        );

        result.extend(filter_chain.as_arg());

        result.extend(args!["-map", filter_chain.video_label().to_owned()]);
        result.extend(args!["-map", filter_chain.audio_label().to_owned()]);

        result.extend(
            self.output_options
                .iter()
                .flat_map(|o| o.as_arg(&self.output_context)),
        );

        result
    }

    pub fn envs(&self) -> Vec<EnvironmentVariable> {
        let mut result = self.env_vars.clone();

        if let Some(a) = &self.accel {
            result.extend(a.envs())
        }

        result
    }
}

impl std::fmt::Display for Pipeline {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "args: {}", self.args().join(" "))
    }
}

pub fn generate_pipeline(
    ffmpeg_info: &FfmpegInfo,
    input_settings: InputSettings,
    output_settings: OutputSettings,
) -> Result<Pipeline, FFPipelineError> {
    Pipeline::full(ffmpeg_info, input_settings, output_settings)
}

/// Input arguments that repeat an input for as long as it is read.
///
/// `-stream_loop -1` reopens the input at its start on every EOF, so an input
/// shorter than the window it stands in for keeps producing frames rather than
/// running dry and leaving the rest of the window to the padding filters (a
/// frozen last frame over silence). It changes nothing about when the transcode
/// ends: the output `-t` remains the only bound, so an input longer than the
/// window is read exactly as far as it was before and never reaches its first
/// loop. This is the same pairing the video watermark path already relies on.
fn loop_input_args(loop_when_exhausted: bool) -> ArgVec {
    if loop_when_exhausted {
        args!["-stream_loop", "-1"]
    } else {
        Vec::new()
    }
}

/// Input arguments for a watermark, chosen by how the watermark decodes.
///
/// Still images pin `-f image2`. Without it ffmpeg probes the file and picks a
/// `*_pipe` demuxer, which reads the input as a stream of concatenated images
/// rather than one image. Anything the decoder cannot consume as a complete
/// image is then treated as the start of the next one, and under `-loop 1` that
/// is retried forever, so the watermark input never yields a frame and the
/// filter graph starves. Legacy stores channel artwork hash-named with no
/// extension, so there is no filename for ffmpeg to infer the format from.
fn watermark_input_args(
    watermark_stream: &ProbeResultVideoStream,
    frame_rate: &str,
    duration: Duration,
) -> ArgVec {
    if watermark_stream.is_still_image() {
        args![
            "-f",
            "image2",
            "-loop",
            "1",
            "-framerate",
            frame_rate.to_owned(),
            "-t",
            format!("{}ms", duration.as_millis())
        ]
    } else if watermark_stream.codec == "gif" || watermark_stream.codec == "apng" {
        args![
            "-ignore_loop",
            "0",
            "-t",
            format!("{}ms", duration.as_millis())
        ]
    } else {
        args![
            "-stream_loop",
            "-1",
            "-t",
            format!("{}ms", duration.as_millis())
        ]
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use time::OffsetDateTime;

    use super::*;
    use crate::input::{GraphicsLocation, LocalInputSource, ProbedInput};
    use crate::output_format::OutputFormat;
    use crate::output_settings::AudioOutputSettings;
    use crate::probe::{
        CodecType, ProbeResult, ProbeResultAudioStream, ProbeResultColorParams, ProbeResultStream,
    };

    const SLATE_PATH: &str = "/bumps/fallback/WeatherSlateStatic.mp4";

    /// An ordinary library item: one h264 video stream and one aac audio
    /// stream in an mp4, of whatever length the caller is describing.
    fn slate_probe(media: Duration) -> ProbeResult {
        ProbeResult {
            path: String::from(SLATE_PATH),
            streams: vec![
                ProbeResultStream::Video(Box::new(ProbeResultVideoStream {
                    stream_index: 0,
                    codec: String::from("h264"),
                    codec_type: CodecType::Video,
                    profile: String::from("high"),
                    height: Some(1080),
                    width: Some(1920),
                    frame_rate: FrameRate::parse("30"),
                    sample_aspect_ratio: None,
                    display_aspect_ratio: None,
                    pix_fmt: String::from("yuv420p"),
                    color_params: ProbeResultColorParams::default(),
                    field_order: None,
                    dv_profile: None,
                })),
                ProbeResultStream::Audio(ProbeResultAudioStream {
                    stream_index: 1,
                    codec: String::from("aac"),
                    channels: 2,
                }),
            ],
            duration: Some(media),
            format_name: Some(String::from("mov,mp4,m4a")),
        }
    }

    /// The pipeline the shared session builds for one templated window: a
    /// single local file standing in for the whole slot, padded to the -t
    /// clamp because the window's PTS envelope has to match every variant's.
    fn slate_pipeline_args(media: Duration, window: Duration, loops: bool) -> ArgVec {
        slate_pipeline_args_with_graphics(media, window, loops, Vec::new())
    }

    fn slate_pipeline_args_with_graphics(
        media: Duration,
        window: Duration,
        loops: bool,
        graphics_inputs: Vec<GraphicsInput>,
    ) -> ArgVec {
        let probe = slate_probe(media);
        let input_source = InputSource::Local(LocalInputSource {
            path: String::from(SLATE_PATH),
        });

        let probed = |source: InputSource, probe: ProbeResult| ProbedInput {
            input_source: source,
            probe_result: probe,
            in_point: Duration::ZERO,
            out_point: window,
            stream_index: None,
            loop_when_exhausted: loops,
        };

        let input_settings = InputSettings {
            start: OffsetDateTime::UNIX_EPOCH,
            audio_input: probed(input_source.clone(), probe.clone()),
            video_input: probed(input_source, probe),
            subtitle_input: None,
            graphics_inputs,
            playout_offset: Duration::ZERO,
        };

        let output_settings = OutputSettings {
            audio: AudioOutputSettings {
                format: Some(AudioFormat::Aac),
                bitrate: Some(Kbps(192)),
                buffer: Some(Kbps(384)),
                channels: Some(2),
                sample_rate: Some(Hz(48000)),
                loudness: None,
            },
            video_format: Some(VideoFormat::H264),
            bit_depth: Some(8),
            video_bitrate: Some(Kbps(5000)),
            video_buffer: Some(Kbps(10000)),
            video_size: None,
            scaling_mode: ScalingMode::ScaleAndPad,
            filter_options: VideoFilterOptions::default(),
            deinterlace: false,
            accel: None,
            format: OutputFormat::Hls {
                playlist: String::from("/session/live.m3u8"),
                segment_template: String::from("/session/live%05d.ts"),
                troubleshoot: false,
            },
            pts_offset: None,
            pad_to_duration: true,
            realtime: false,
            is_live: false,
            frame_rate: None,
            subtitle_mode: SubtitleMode::Burn,
            fonts_folder: None,
            subtitle_force_style: None,
            reports_folder: None,
            report_id: None,
        };

        let ffmpeg_info = FfmpegInfo {
            hwaccels: HashSet::new(),
            video_filters: HashSet::new(),
            preferred_filters: HashMap::new(),
        };

        let mut pipeline =
            generate_pipeline(&ffmpeg_info, input_settings, output_settings).unwrap();
        pipeline.optimize();
        pipeline.args()
    }

    fn position_of(args: &ArgVec, value: &str) -> Option<usize> {
        args.iter().position(|a| a.as_ref() == value)
    }

    /// A slate is chosen for what it shows, not for how long it runs, so a
    /// 15 second bumper has to hold a 103 second window. Looping the input
    /// is what makes any library item usable as one: without it the source
    /// runs dry and the padding filters hold its last frame over silence for
    /// the remaining minute and a half.
    #[test]
    fn a_slate_shorter_than_its_window_loops_to_fill_it() {
        let args = slate_pipeline_args(Duration::from_secs(15), Duration::from_secs(103), true);

        assert!(
            has_pair(&args, "-stream_loop", "-1"),
            "a slate must repeat, got {args:?}"
        );
        assert!(
            has_pair(&args, "-t", "103000ms"),
            "the window is still what ends the transcode, got {args:?}"
        );

        // -stream_loop is an input option, so ffmpeg only reads it before
        // the -i it belongs to
        let (loop_at, input_at) = (
            position_of(&args, "-stream_loop").unwrap(),
            position_of(&args, "-i").unwrap(),
        );
        assert!(loop_at < input_at, "got {args:?}");

        // audio and video are the same file here, which is the only shape a
        // slate item has: it must be opened, and looped, exactly once
        assert_eq!(
            args.iter().filter(|a| a.as_ref() == "-stream_loop").count(),
            1,
            "got {args:?}"
        );
    }

    /// The loop is bounded by the same -t as everything else, so a slate
    /// longer than its window never reaches its first repeat and is read
    /// exactly as far as it was before looping existed. The flag is
    /// unconditional precisely so this case needs no handling of its own.
    #[test]
    fn a_slate_longer_than_its_window_is_cut_by_the_window_as_before() {
        let short = slate_pipeline_args(Duration::from_secs(15), Duration::from_secs(103), true);
        let long = slate_pipeline_args(Duration::from_secs(600), Duration::from_secs(103), true);

        assert_eq!(
            short, long,
            "the media's length must not steer the pipeline"
        );
        assert!(has_pair(&long, "-t", "103000ms"), "got {long:?}");
    }

    /// Scheduled content is never repeated: playing an item twice is not
    /// what the schedule said. Only a source standing in for a window opts
    /// into looping, so an ordinary item's pipeline is untouched.
    #[test]
    fn an_item_that_is_not_slate_is_never_looped() {
        let args = slate_pipeline_args(Duration::from_secs(15), Duration::from_secs(103), false);

        assert!(
            !args.iter().any(|a| a.as_ref() == "-stream_loop"),
            "got {args:?}"
        );
        assert!(has_pair(&args, "-t", "103000ms"), "got {args:?}");
    }

    fn watermark_stream(codec: &str) -> ProbeResultVideoStream {
        ProbeResultVideoStream {
            stream_index: 0,
            codec: String::from(codec),
            codec_type: CodecType::Video,
            profile: String::new(),
            height: Some(64),
            width: Some(64),
            frame_rate: FrameRate::parse("25"),
            sample_aspect_ratio: None,
            display_aspect_ratio: None,
            pix_fmt: String::from("rgba"),
            color_params: ProbeResultColorParams::default(),
            field_order: None,
            dv_profile: None,
        }
    }

    fn has_pair(args: &ArgVec, flag: &str, value: &str) -> bool {
        args.windows(2)
            .any(|w| w[0].as_ref() == flag && w[1].as_ref() == value)
    }

    #[test]
    fn still_image_watermark_pins_the_image2_demuxer() {
        for codec in ["png", "mjpeg", "bmp", "tiff"] {
            let args = watermark_input_args(
                &watermark_stream(codec),
                "24000/1001",
                Duration::from_secs(44),
            );

            assert!(
                has_pair(&args, "-f", "image2"),
                "{codec} watermark must pin -f image2, got {args:?}"
            );
            assert!(has_pair(&args, "-loop", "1"), "{codec} must loop");
            assert!(
                has_pair(&args, "-framerate", "24000/1001"),
                "{codec} must carry the output frame rate"
            );
            assert!(has_pair(&args, "-t", "44000ms"), "{codec} must be bounded");
        }
    }

    /// The `-f image2` pin has to reach the BUILT PIPELINE, not just the
    /// helper that formats it.
    ///
    /// Upstream inlines these three branches without the pin, so a merge that
    /// takes its version wholesale drops the demuxer silently: the helper's
    /// own tests keep passing because they call it directly, and nothing else
    /// looked at the emitted args. Verified by mutation on the 2026-08-14
    /// upstream merge, where inlining upstream's version failed no test at
    /// all. This asserts the wiring rather than the formatting.
    #[test]
    fn a_still_image_layer_pins_image2_in_the_built_pipeline() {
        let args = slate_pipeline_args_with_graphics(
            Duration::from_secs(15),
            Duration::from_secs(103),
            true,
            vec![GraphicsInput {
                layer_index: 0,
                input_source: InputSource::Local(LocalInputSource {
                    path: String::from("/bumps/logo.png"),
                }),
                probe_result: ProbeResult {
                    path: String::from("/bumps/logo.png"),
                    streams: vec![ProbeResultStream::Video(Box::new(watermark_stream("png")))],
                    duration: None,
                    format_name: Some(String::from("png_pipe")),
                },
                stream_index: None,
                location: GraphicsLocation::TopLeft,
                width_percent: None,
                within_source_content: None,
                horizontal_margin_percent: None,
                vertical_margin_percent: None,
                opacity_percent: None,
                timing: None,
            }],
        );

        assert!(
            has_pair(&args, "-f", "image2"),
            "the built pipeline must pin -f image2 for a still image layer, got {args:?}"
        );
    }

    #[test]
    fn animated_and_video_watermarks_do_not_pin_a_demuxer() {
        for codec in ["gif", "apng", "h264"] {
            let args =
                watermark_input_args(&watermark_stream(codec), "25", Duration::from_secs(10));

            assert!(
                !args.iter().any(|a| a.as_ref() == "image2"),
                "{codec} is not a still image and must not pin image2, got {args:?}"
            );
            assert!(has_pair(&args, "-t", "10000ms"), "{codec} must be bounded");
        }
    }

    #[test]
    fn animated_watermarks_loop_without_reopening_the_input() {
        for codec in ["gif", "apng"] {
            let args = watermark_input_args(&watermark_stream(codec), "25", Duration::from_secs(1));
            assert!(has_pair(&args, "-ignore_loop", "0"), "{codec} must loop");
        }
    }

    #[test]
    fn device_name_returns_correct_ffmpeg_device_strings() {
        assert_eq!(FrameSurface::Cuda.device_name(), Some("cuda"));
        assert_eq!(FrameSurface::OpenCL.device_name(), Some("opencl"));
        assert_eq!(FrameSurface::Qsv.device_name(), Some("qsv"));
        assert_eq!(FrameSurface::Vaapi.device_name(), Some("vaapi"));
        assert_eq!(FrameSurface::Vulkan.device_name(), Some("vulkan"));
        assert_eq!(
            FrameSurface::VideoToolbox.device_name(),
            Some("videotoolbox")
        );
        assert_eq!(FrameSurface::System.device_name(), None);
    }
}
