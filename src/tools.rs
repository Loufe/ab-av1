use std::{cell::RefCell, future::Future, path::PathBuf};

/// Executables used by a library job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolPaths {
    pub ffmpeg: PathBuf,
    pub ffprobe: PathBuf,
}

impl Default for ToolPaths {
    fn default() -> Self {
        Self {
            ffmpeg: "ffmpeg".into(),
            ffprobe: "ffprobe".into(),
        }
    }
}

thread_local! {
    static JOB_TOOLS: RefCell<ToolPaths> = RefCell::new(ToolPaths::default());
}

/// Scope one library job to an explicit FFmpeg toolchain.
pub async fn with_tool_paths<T>(tools: ToolPaths, future: impl Future<Output = T>) -> T {
    struct RestoreTools(Option<ToolPaths>);

    impl Drop for RestoreTools {
        fn drop(&mut self) {
            if let Some(tools) = self.0.take() {
                JOB_TOOLS.with(|slot| *slot.borrow_mut() = tools);
            }
        }
    }

    let previous = JOB_TOOLS.with(|slot| slot.replace(tools));
    let restore = RestoreTools(Some(previous));
    let output = future.await;
    drop(restore);
    output
}

pub(crate) fn ffmpeg() -> PathBuf {
    JOB_TOOLS.with(|tools| tools.borrow().ffmpeg.clone())
}

pub(crate) fn ffprobe() -> PathBuf {
    JOB_TOOLS.with(|tools| tools.borrow().ffprobe.clone())
}
