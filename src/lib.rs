#[cfg(feature = "cli")]
pub mod cli;
pub mod command;
mod console_ext;
mod ffmpeg;
pub mod ffprobe;
mod float;
mod log;
mod process;
mod sample;
mod temporary;
mod tools;
mod vmaf;
mod xpsnr;

pub use tools::{ToolPaths, with_tool_paths};

/// Finish one library job after its command stream has completed.
///
/// The library currently tracks child processes and temporary files globally,
/// so callers must run at most one job at a time and call this exactly once
/// after dropping the job stream.
pub async fn finish_job(keep_temp_files: bool) -> anyhow::Result<()> {
    finalize(
        process::child::wait().await,
        temporary::clean(keep_temp_files).await,
    )
}

/// Cancel one library job after first dropping its command stream.
///
/// This terminates all registered child processes and removes all registered
/// temporary files. Library jobs must not overlap.
pub async fn cancel_job() -> anyhow::Result<()> {
    finalize(
        process::child::kill_all().await,
        temporary::clean_all().await,
    )
}

fn finalize(processes: anyhow::Result<()>, temporary: anyhow::Result<()>) -> anyhow::Result<()> {
    match (processes, temporary) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(process_error), Err(temp_error)) => {
            anyhow::bail!("{process_error}\n{temp_error}")
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn finalization_reports_process_and_temporary_errors() {
        let error = super::finalize(
            Err(anyhow::anyhow!("process cleanup")),
            Err(anyhow::anyhow!("temporary cleanup")),
        )
        .expect_err("both failures must be returned");

        let message = error.to_string();
        assert!(message.contains("process cleanup"));
        assert!(message.contains("temporary cleanup"));
    }
}
