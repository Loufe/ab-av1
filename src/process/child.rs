use command_group::{AsyncCommandGroup, AsyncGroupChild};
use log::info;
use std::{
    io::{self, IsTerminal},
    mem,
    pin::Pin,
    process::{Output, Stdio},
    sync::{Arc, LazyLock, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, BufReader},
    process::{ChildStderr, ChildStdout, Command},
    sync::Mutex as AsyncMutex,
    time::{Instant, timeout_at},
};
use tokio_stream::{Stream, StreamExt};

use super::Item;

type ChildHandle = Arc<AsyncMutex<AsyncGroupChild>>;

static RUNNING: LazyLock<Mutex<Vec<ChildHandle>>> = LazyLock::new(<_>::default);

/// Output stream for a process registered for job finalization.
pub struct ManagedChunkStream {
    stream: Pin<Box<dyn Stream<Item = Item>>>,
    child: ChildHandle,
}

impl ManagedChunkStream {
    pub fn child(&self) -> &ChildHandle {
        &self.child
    }
}

impl Stream for ManagedChunkStream {
    type Item = Item;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

/// Spawn a command in an OS process group and register it immediately.
pub fn spawn(mut command: Command) -> io::Result<ManagedChunkStream> {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());

    let mut group = command.group();
    group.kill_on_drop(true);
    #[cfg(windows)]
    group.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    let mut child = group.spawn()?;
    let stdout = child
        .inner()
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("child stdout was not piped"))?;
    let stderr = child
        .inner()
        .stderr
        .take()
        .ok_or_else(|| io::Error::other("child stderr was not piped"))?;
    let child = Arc::new(AsyncMutex::new(child));
    RUNNING.lock().unwrap().push(Arc::clone(&child));

    let stream_child = Arc::clone(&child);
    let stream = Box::pin(chunk_stream(stdout, stderr, stream_child));
    Ok(ManagedChunkStream { stream, child })
}

/// Run a registered command to completion and collect its output.
pub async fn output(command: Command) -> io::Result<Output> {
    let mut stream = spawn(command)?;
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let status = loop {
        match stream.next().await {
            Some(Item::Stdout(chunk)) => stdout.extend(chunk),
            Some(Item::Stderr(chunk)) => stderr.extend(chunk),
            Some(Item::Done(status)) => break status?,
            None => return Err(io::Error::other("child ended without an exit status")),
        }
    };
    Ok(Output {
        status,
        stdout,
        stderr,
    })
}

fn chunk_stream(
    stdout: ChildStdout,
    stderr: ChildStderr,
    child: ChildHandle,
) -> impl Stream<Item = Item> {
    async_stream::stream! {
        let mut stdout = BufReader::new(stdout);
        let mut stderr = BufReader::new(stderr);
        let mut stdout_open = true;
        let mut stderr_open = true;
        let mut stdout_buf = vec![0; 4096];
        let mut stderr_buf = vec![0; 4096];

        while stdout_open || stderr_open {
            tokio::select! {
                read = stdout.read(&mut stdout_buf), if stdout_open => match read {
                    Ok(0) => stdout_open = false,
                    Ok(len) => yield Item::Stdout(stdout_buf[..len].to_vec()),
                    Err(error) => {
                        yield Item::Done(Err(error));
                        return;
                    }
                },
                read = stderr.read(&mut stderr_buf), if stderr_open => match read {
                    Ok(0) => stderr_open = false,
                    Ok(len) => yield Item::Stderr(stderr_buf[..len].to_vec()),
                    Err(error) => {
                        yield Item::Done(Err(error));
                        return;
                    }
                },
            }
        }

        yield Item::Done(child.lock().await.wait().await);
    }
}

/// Wait for all registered process groups to exit.
pub async fn wait() -> anyhow::Result<()> {
    let mut log_deadline = Some(Instant::now() + Duration::from_millis(500));
    let processes = mem::take(&mut *RUNNING.lock().unwrap());
    let mut errors = Vec::new();

    for (index, process) in processes.into_iter().enumerate() {
        let mut process = process.lock().await;
        let waited = match log_deadline {
            Some(deadline) => match timeout_at(deadline, process.wait()).await {
                Ok(waited) => waited,
                Err(_) => {
                    log_waiting();
                    log_deadline = None;
                    process.wait().await
                }
            },
            None => process.wait().await,
        };
        if let Err(error) = waited {
            errors.push(format!("child {index}: {error}"));
        }
    }

    collected_errors("failed to wait for child processes", errors)
}

/// Terminate every registered process group and wait for it to exit.
pub async fn kill_all() -> anyhow::Result<()> {
    let processes = mem::take(&mut *RUNNING.lock().unwrap());
    let mut errors = Vec::new();

    for (index, process) in processes.into_iter().enumerate() {
        let mut process = process.lock().await;
        match process.try_wait() {
            Ok(Some(_)) => {}
            Ok(None) => {
                if let Err(error) = process.kill().await {
                    errors.push(format!("child {index} termination: {error}"));
                }
                if let Err(error) = process.wait().await {
                    errors.push(format!("child {index} wait: {error}"));
                }
            }
            Err(error) => errors.push(format!("child {index} status: {error}")),
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
