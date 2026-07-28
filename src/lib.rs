pub mod command;
mod console_ext;
pub mod ffmpeg;
pub mod ffprobe;
mod float;
mod log;
pub mod process;
mod sample;
pub mod temporary;
mod vmaf;
mod xpsnr;

use clap::Parser;

/// Initialise cli logging.
#[cfg(feature = "cli")]
pub fn init_cli_logging() {
    use std::io::IsTerminal;

    env_logger::builder()
        .filter_module(
            "ab_av1",
            match std::io::stderr().is_terminal() {
                true => ::log::LevelFilter::Off,
                false => ::log::LevelFilter::Info,
            },
        )
        .parse_default_env()
        .init();
}

#[derive(Parser)]
#[command(version, about)]
pub enum Command {
    SampleEncode(command::sample_encode::Args),
    Vmaf(command::vmaf::Args),
    Xpsnr(command::xpsnr::Args),
    Encode(command::encode::Args),
    CrfSearch(command::crf_search::Args),
    AutoEncode(command::auto_encode::Args),
    PrintCompletions(command::print_completions::Args),
}

impl Command {
    /// This decides what commands will keep temp files.
    ///
    /// # Important
    ///
    /// Add commands using the sample sub-args here referencing the `keep` flag,
    /// or the temp files will be removed anyways.
    pub fn keep_temp_files(&self) -> bool {
        match self {
            Self::SampleEncode(args) => args.sample.keep,
            Self::CrfSearch(args) => args.search.sample.keep,
            Self::AutoEncode(args) => args.search.sample.keep,
            _ => false,
        }
    }
}
