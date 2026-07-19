use log::info;
use std::{
    io::IsTerminal,
    mem,
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::{LazyLock, Mutex},
    task::{Context, Poll},
    time::Duration,
};
use tokio::time::{Instant, timeout_at};
use tokio_process_stream::ProcessChunkStream;

static RUNNING: LazyLock<Mutex<Vec<ProcessChunkStream>>> = LazyLock::new(<_>::default);

/// Add a child process so it may be waited on before exiting.
pub fn add(mut child: ProcessChunkStream) {
    let mut running = RUNNING.lock().unwrap();

    // remove any that have exited already
    running.retain_mut(|c| !c.exited());

    if !child.exited() {
        running.push(child);
    }
}

/// Wait for all child processes, that were added with [`add`], to exit.
pub async fn wait() -> anyhow::Result<()> {
    // if waiting takes >500ms log what's happening
    let mut log_deadline = Some(Instant::now() + Duration::from_millis(500));
    let procs = mem::take(&mut *RUNNING.lock().unwrap());
    let mut errors = Vec::new();

    for (index, mut proc) in procs.into_iter().enumerate() {
        if let Some(child) = proc.child_mut() {
            let waited = match log_deadline {
                Some(deadline) => match timeout_at(deadline, child.wait()).await {
                    Ok(waited) => waited,
                    Err(_) => {
                        log_waiting();
                        log_deadline = None;
                        child.wait().await
                    }
                },
                None => child.wait().await,
            };
            if let Err(error) = waited {
                errors.push(format!("child {index}: {error}"));
            }
        }
    }

    collected_errors("failed to wait for child processes", errors)
}

/// Terminate every registered child process and wait for it to exit.
pub async fn kill_all() -> anyhow::Result<()> {
    let procs = mem::take(&mut *RUNNING.lock().unwrap());
    let mut errors = Vec::new();

    for (index, mut proc) in procs.into_iter().enumerate() {
        if let Some(child) = proc.child_mut() {
            match child.try_wait() {
                Ok(Some(_)) => continue,
                Ok(None) => {
                    if let Err(error) = child.kill().await {
                        errors.push(format!("child {index} termination: {error}"));
                        if let Err(error) = child.wait().await {
                            errors.push(format!("child {index} wait: {error}"));
                        }
                    }
                }
                Err(error) => errors.push(format!("child {index} status: {error}")),
            }
        }
    }

    collected_errors("failed to terminate child processes", errors)
}

fn log_waiting() {
    match std::io::stderr().is_terminal() {
        true => eprintln!("Waiting for child processes to exit..."),
        _ => info!("Waiting for child processes to exit"),
    }
}

fn collected_errors(context: &str, errors: Vec<String>) -> anyhow::Result<()> {
    if errors.is_empty() {
        Ok(())
    } else {
        anyhow::bail!("{context}:\n{}", errors.join("\n"))
    }
}

/// Wrapper that [`add`]s the inner on drop.
#[derive(Debug)]
pub struct AddOnDropChunkStream(Option<ProcessChunkStream>);

impl From<ProcessChunkStream> for AddOnDropChunkStream {
    fn from(v: ProcessChunkStream) -> Self {
        Self(Some(v))
    }
}

impl Drop for AddOnDropChunkStream {
    fn drop(&mut self) {
        if let Some(child) = self.0.take() {
            add(child);
        }
    }
}

impl Deref for AddOnDropChunkStream {
    type Target = ProcessChunkStream;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().unwrap() // only none after drop
    }
}

impl DerefMut for AddOnDropChunkStream {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().unwrap() // only none after drop
    }
}

impl tokio_stream::Stream for AddOnDropChunkStream {
    type Item = <ProcessChunkStream as tokio_stream::Stream>::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let inner = self
            .get_mut()
            .0
            .as_mut()
            .expect("inner process stream is present until drop");
        Pin::new(inner).poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0
            .as_ref()
            .map_or((0, Some(0)), tokio_stream::Stream::size_hint)
    }
}

trait Exited {
    /// Returns true if the child process has exited.
    fn exited(&mut self) -> bool;
}

impl Exited for ProcessChunkStream {
    fn exited(&mut self) -> bool {
        let Some(child) = self.child_mut() else {
            return true; // no child process
        };
        child.try_wait().is_ok_and(|s| s.is_some())
    }
}
