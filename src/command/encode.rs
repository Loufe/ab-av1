use crate::{
    command::{
        PROGRESS_CHARS, SmallDuration,
        args::{self, Encoder},
    },
    console_ext::style,
    ffmpeg,
    ffprobe::{self, Ffprobe},
    log::ProgressLogger,
    process::FfmpegOut,
    temporary::{self, TempKind},
};
use clap::Parser;
use console::style;
use indicatif::{HumanBytes, ProgressBar, ProgressStyle};
use log::info;
use same_file::is_same_file;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::fs;
use tokio_stream::{Stream, StreamExt};

/// Invoke ffmpeg to encode a video or image.
#[derive(Parser)]
#[group(skip)]
pub struct Args {
    #[clap(flatten)]
    pub args: args::Encode,

    /// Encoder constant rate factor (1-63). Lower means better quality.
    #[arg(long)]
    pub crf: f32,

    #[clap(flatten)]
    pub encode: args::EncodeToOutput,
}

pub async fn encode(args: Args) -> anyhow::Result<()> {
    let bar = ProgressBar::new(1).with_style(
        ProgressStyle::default_bar()
            .template("{spinner:.cyan.bold} {elapsed_precise:.bold} {wide_bar:.cyan/blue} ({msg}eta {eta})")?
            .progress_chars(PROGRESS_CHARS)
    );
    bar.enable_steady_tick(Duration::from_millis(100));

    let probe = Arc::new(ffprobe::probe(&args.args.input));
    if args.encode.output.is_none() {
        let output = default_output_name(&args.args.input, &args.args.encoder, probe.is_image);
        let out = shell_escape::escape(output.display().to_string().into());
        bar.println(style!("Encoding {out}").dim().to_string());
    }
    consume(args, probe, &bar).await
}

#[derive(Debug, Clone, PartialEq)]
pub enum Update {
    Progress {
        frame: u64,
        fps: f32,
        time: Duration,
    },
    StreamSizes {
        video: u64,
        audio: u64,
        subtitle: u64,
        other: u64,
    },
    Done {
        output: PathBuf,
        input_size: u64,
        output_size: u64,
    },
}

pub fn run(
    Args {
        args,
        crf,
        encode:
            args::EncodeToOutput {
                output,
                audio_codec,
                downmix_to_stereo,
                video_only,
                overwrite_input,
            },
    }: Args,
    probe: Arc<Ffprobe>,
) -> impl Stream<Item = anyhow::Result<Update>> {
    async_stream::try_stream! {
    let output = output.unwrap_or_else(|| {
        default_output_name(&args.input, &args.encoder, probe.is_image)
    });

    if !overwrite_input && is_same_file(&output, &args.input).unwrap_or(false) {
        Err(anyhow::anyhow!(
            "Input and Output are specified as the same file. Not proceeding. \
             Pass in `--overwrite-input` to allow this."
        ))?;
    }

    // output is temporary until encoding has completed successfully
    temporary::add(&output, TempKind::NotKeepable);

    let mut enc_args = args.to_ffmpeg_args(crf, &probe)?;
    enc_args.video_only = video_only;
    let has_audio = probe.has_audio;

    // only downmix if achannels > 3
    let stereo_downmix = downmix_to_stereo && probe.max_audio_channels.is_some_and(|c| c > 3);
    let audio_codec = audio_codec.as_deref();
    if stereo_downmix && audio_codec == Some("copy") {
        Err(anyhow::anyhow!(
            "--stereo-downmix cannot be used with --acodec copy"
        ))?;
    }

    info!(
        "encoding {}",
        output.file_name().and_then(|n| n.to_str()).unwrap_or("")
    );

    let mut enc = ffmpeg::encode(enc_args, &output, has_audio, audio_codec, stereo_downmix)?;
    while let Some(progress) = enc.next().await {
        match progress? {
            FfmpegOut::Progress { frame, fps, time } => {
                yield Update::Progress { frame, fps, time };
            }
            FfmpegOut::StreamSizes {
                video,
                audio,
                subtitle,
                other,
            } => yield Update::StreamSizes { video, audio, subtitle, other },
        }
    }
    enc.wait().await?; // ensure process has exited

    // successful encode, so don't delete it!
    temporary::unadd(&output);

    let output_size = fs::metadata(&output).await?.len();
    let input_size = fs::metadata(&args.input).await?.len();
    yield Update::Done { output, input_size, output_size };
    }
}

pub(crate) async fn consume(
    args: Args,
    probe: Arc<Ffprobe>,
    bar: &ProgressBar,
) -> anyhow::Result<()> {
    bar.set_message("encoding, ");
    if let Ok(d) = &probe.duration {
        bar.set_length(d.as_micros_u64().max(1));
    }

    let duration = probe.duration.as_ref().ok().copied();
    let mut logger = ProgressLogger::new(module_path!(), Instant::now());
    let mut stream_sizes = None;
    let mut updates = std::pin::pin!(run(args, probe));
    while let Some(update) = updates.next().await {
        match update? {
            Update::Progress { fps, time, .. } => {
                if fps > 0.0 {
                    bar.set_message(format!("{fps} fps, "));
                }
                if let Some(d) = duration {
                    bar.set_position(time.as_micros_u64());
                    logger.update(d, time, fps);
                }
            }
            Update::StreamSizes {
                video,
                audio,
                subtitle,
                other,
            } => stream_sizes = Some((video, audio, subtitle, other)),
            Update::Done {
                input_size,
                output_size,
                ..
            } => print_done(input_size, output_size, stream_sizes),
        }
    }
    bar.finish();
    Ok(())
}

fn print_done(input_size: u64, output_size: u64, stream_sizes: Option<(u64, u64, u64, u64)>) {
    let output_percent = 100.0 * output_size as f64 / input_size as f64;
    let output_size = style(HumanBytes(output_size)).dim().bold();
    let output_percent = style!("{}%", output_percent.round()).dim().bold();
    eprint!(
        "{} {output_size} {}{output_percent}",
        style("Encoded").dim(),
        style("(").dim(),
    );
    if let Some((video, audio, subtitle, other)) = stream_sizes
        && (audio > 0 || subtitle > 0 || other > 0)
    {
        for (label, size) in [
            ("video:", video),
            ("audio:", audio),
            ("subs:", subtitle),
            ("other:", other),
        ] {
            if size > 0 {
                let size = style(HumanBytes(size)).dim();
                eprint!("{} {}{size}", style(",").dim(), style(label).dim(),);
            }
        }
    }
    eprintln!("{}", style(")").dim());
}

/// * vid.mp4 -> "mp4"
/// * vid.??? -> "mkv"
/// * image.??? -> "avif"
pub fn default_output_ext(input: &Path, encoder: &Encoder, is_image: bool) -> &'static str {
    if is_image {
        return encoder.default_image_ext();
    }
    match input.extension().and_then(|e| e.to_str()) {
        Some("mp4") => "mp4",
        _ => "mkv",
    }
}

/// E.g. vid.mkv -> "vid.av1.mkv"
pub fn default_output_name(input: &Path, encoder: &Encoder, is_image: bool) -> PathBuf {
    let pre = ffmpeg::pre_extension_name(encoder.as_str());
    let ext = default_output_ext(input, encoder, is_image);
    input.with_extension(format!("{pre}.{ext}"))
}
