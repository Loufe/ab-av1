#![allow(unused_crate_dependencies)]

use ab_av1::{Command, command, process, temporary};
use anyhow::anyhow;
use clap::Parser;
use futures_util::FutureExt;
use tokio::signal;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    ab_av1::init_cli_logging();

    let action = Command::parse();
    let keep = action.keep_temp_files();

    let local = tokio::task::LocalSet::new();
    let command = local.run_until(match action {
        Command::SampleEncode(args) => command::sample_encode(args).boxed_local(),
        Command::Vmaf(args) => command::vmaf(args).boxed_local(),
        Command::Xpsnr(args) => command::xpsnr(args).boxed_local(),
        Command::Encode(args) => command::encode(args).boxed_local(),
        Command::CrfSearch(args) => command::crf_search(args).boxed_local(),
        Command::AutoEncode(args) => command::auto_encode(args).boxed_local(),
        Command::PrintCompletions(args) => return command::print_completions(args),
    });

    let out = tokio::select! {
        r = command => r,
        _ = signal::ctrl_c() => Err(anyhow!("ctrl_c")),
    };
    drop(local);

    process::child::wait().await;

    // Final cleanup. Samples are already deleted (if wished by the user) during `command::sample_encode::run`.
    temporary::clean(keep).await;

    if let Err(err) = out {
        eprintln!("Error: {err}");
        std::process::exit(1);
    }
}
