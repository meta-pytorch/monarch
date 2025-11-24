/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::future::Future;
use std::io::Read;
use std::io::Write;
use std::process::Stdio;
use std::thread;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::forward;
use hyperactor::mailbox::OncePortHandle;
use monarch_messages::controller::WorkerError;
use monarch_types::PyTree;
use nix::sys::wait::WaitStatus;
use nix::unistd::Pid;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::process::Child;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::task;
use torch_sys::RValue;

use crate::ResolvableFunction;

/// Simple communication channel to send/recv objects over an async stream.
pub trait AsyncPipe<T> {
    fn send(&mut self, val: T) -> impl Future<Output = Result<()>>;
    fn recv(&mut self) -> impl Future<Output = Result<T>>;
}

/// Simple communication channel to send/recv objects over a synchronous stream.
/// NOTE: This synchronous specialization is mainly useful when wrapped w/ the
/// `PyPipe` struct, which is also synchronous (via Python).
pub trait Pipe<T> {
    fn send(&mut self, val: T) -> Result<()>;
    fn recv(&mut self) -> Result<T>;
}

#[derive(Serialize, Deserialize)]
pub struct OutOfProcessSetupParams {
    pub sizes: HashMap<String, usize>,
    pub ranks: HashMap<String, usize>,
    pub function: ResolvableFunction,
    pub args: Vec<PyTree<RValue>>,
    pub kwargs: HashMap<String, PyTree<RValue>>,
}

impl<T: Send + Sync + 'static> AsyncPipe<T>
    for (mpsc::UnboundedSender<T>, mpsc::UnboundedReceiver<T>)
{
    async fn send(&mut self, val: T) -> Result<()> {
        Ok(self.0.send(val)?)
    }

    async fn recv(&mut self) -> Result<T> {
        Ok(self
            .1
            .recv()
            .await
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::UnexpectedEof, ""))?)
    }
}

impl<T: Send + Sync + 'static> Pipe<T> for (mpsc::UnboundedSender<T>, mpsc::UnboundedReceiver<T>) {
    fn send(&mut self, val: T) -> Result<()> {
        Ok(self.0.send(val)?)
    }

    fn recv(&mut self) -> Result<T> {
        Ok(self
            .1
            .blocking_recv()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::UnexpectedEof, ""))?)
    }
}

/// Return a pair of parent/child pipes connected via unbounded tokio mpsc
/// queues.
pub fn create_local_pipe<T>() -> (
    (mpsc::UnboundedSender<T>, mpsc::UnboundedReceiver<T>),
    (mpsc::UnboundedSender<T>, mpsc::UnboundedReceiver<T>),
) {
    let (t1, r1) = mpsc::unbounded_channel();
    let (t2, r2) = mpsc::unbounded_channel();
    ((t1, r2), (t2, r1))
}

pub trait AsyncWriteDebug: std::fmt::Debug + AsyncWrite + Sync + Send + Unpin {}
impl<T: std::fmt::Debug + AsyncWrite + Unpin + Sync + Send> AsyncWriteDebug for T {}

#[derive(Debug)]
pub struct AsyncStreamPipe {
    writer: Box<dyn AsyncWriteDebug>,
    channel_reader: mpsc::Receiver<Vec<u8>>,
}

impl AsyncStreamPipe {
    /// Create a new `AsyncStreamPipe` from a reader/writer pair.
    /// The pipe will run a background task to read-ahead up to max_messages
    /// messages to make them immediately available to read.
    /// When reader is closed, the background task will exit and further
    /// reads will return an error.
    pub fn new(
        mut reader: impl AsyncRead + Unpin + Send + 'static,
        writer: impl AsyncWriteDebug + 'static,
        max_messages: usize,
    ) -> Self {
        let (channel_writer, channel_reader) = mpsc::channel::<Vec<u8>>(max_messages);

        task::spawn(async move {
            loop {
                let mut buf = vec![0; 8];
                match reader.read_exact(&mut buf).await {
                    Ok(_) => (),
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    // Other errors should not be expected. We should perhaps log and break
                    // instead of panicking.
                    Err(e) => panic!("preamble read failed: {}", e),
                }
                let len = u64::from_be_bytes(buf.try_into().unwrap());
                buf = vec![0; len as usize];
                match reader.read_exact(&mut buf).await {
                    Ok(_) => (),
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    // Other errors should not be expected. We should perhaps log and break
                    // instead of panicking.
                    Err(e) => panic!("read failed: {}", e),
                }
                if channel_writer.send(buf).await.is_err() {
                    // receiver closed AsyncStreamPipe dropped, so we can break out of the loop
                    break;
                }
            }
        });

        AsyncStreamPipe {
            writer: Box::new(writer),
            channel_reader,
        }
    }
}

impl<T: Serialize + DeserializeOwned> AsyncPipe<T> for AsyncStreamPipe {
    async fn send(&mut self, val: T) -> Result<()> {
        let bytes = bincode::serialize(&val)?;
        let len = bytes.len();
        self.writer.write_all(&len.to_be_bytes()).await?;
        self.writer.write_all(&bytes).await?;
        Ok(())
    }

    async fn recv(&mut self) -> Result<T> {
        let buf = self.channel_reader.recv().await.expect("recv failed");
        Ok(bincode::deserialize(&buf)?)
    }
}

pub trait WriteDebug: std::fmt::Debug + Write + Sync + Send {}
impl<T: std::fmt::Debug + Write + Sync + Send> WriteDebug for T {}

pub struct StreamPipe {
    writer: Box<dyn WriteDebug>,
    channel_reader: std::sync::Arc<std::sync::Mutex<::std::sync::mpsc::Receiver<Vec<u8>>>>,
}

impl StreamPipe {
    /// Create a new `AsyncStreamPipe` from a reader/writer pair.
    /// The pipe will run a background thread to read-ahead up to max_messages
    /// messages to make them immediately available to read.
    /// When reader is closed, the background thread will exit and further
    /// reads will return an error.
    pub fn new(
        mut reader: impl Read + Send + 'static,
        writer: impl WriteDebug + 'static,
        max_messages: usize,
    ) -> Self {
        let (channel_writer, channel_reader) =
            ::std::sync::mpsc::sync_channel::<Vec<u8>>(max_messages);

        thread::spawn(move || {
            loop {
                let mut buf = vec![0; 8];
                match reader.read_exact(&mut buf) {
                    Ok(_) => (),
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    // Other errors should not be expected. We should perhaps log and break
                    // instead of panicking.
                    Err(e) => panic!("preamble read failed: {}", e),
                }
                let len = u64::from_be_bytes(buf.try_into().unwrap());
                buf = vec![0; len as usize];
                match reader.read_exact(&mut buf) {
                    Ok(_) => (),
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => panic!("preamble read failed: {}", e),
                }
                if channel_writer.send(buf).is_err() {
                    // receiver closed StreamPipe dropped, so we can break out of the loop
                    break;
                }
            }
        });

        StreamPipe {
            writer: Box::new(writer),
            channel_reader: std::sync::Arc::new(std::sync::Mutex::new(channel_reader)),
        }
    }
}

impl<T: Serialize + DeserializeOwned> Pipe<T> for StreamPipe {
    fn send(&mut self, val: T) -> Result<()> {
        let bytes = bincode::serialize(&val)?;
        let len = bytes.len();
        self.writer.write_all(&len.to_be_bytes())?;
        self.writer.write_all(&bytes)?;
        self.writer.flush()?;
        Ok(())
    }

    fn recv(&mut self) -> Result<T> {
        let buf = self
            .channel_reader
            .lock()
            .unwrap()
            .recv()
            .expect("recv failed");
        Ok(bincode::deserialize(&buf)?)
    }
}

#[allow(dead_code)]
#[derive(Handler, HandleClient, Debug)]
pub enum PipeMessage {
    SendValue(Result<PyTree<RValue>, WorkerError>),

    RecvValue(#[reply] OncePortHandle<PyTree<RValue>>),
}

#[derive(Debug)]
pub struct PipeActor {
    // NOTE: Use `Option` wrappers to allow moving in `Drop` impl below.
    pipe: Option<AsyncStreamPipe>,
    handle: Child,
}

/// Initialization parameters for `PipeActor`.
#[derive(Debug, Clone)]
pub struct PipeParams {
    pub function: ResolvableFunction,
    pub max_messages: i64,
    pub ranks: HashMap<String, usize>,
    pub sizes: HashMap<String, usize>,
    pub args: Vec<PyTree<RValue>>,
    pub kwargs: HashMap<String, PyTree<RValue>>,
}

impl Actor for PipeActor {}

impl PipeActor {
    pub async fn new(params: PipeParams) -> Result<Self> {
        let mut command = Command::new(
            std::env::var("MONARCH_TENSOR_WORKER_EXE")
                .map_err(|e| anyhow!("could not get var MONARCH_TENSOR_WORKER_EXE: {}", e))?,
        );
        if let Ok(main) = std::env::var("MONARCH_TENSOR_WORKER_MAIN") {
            if std::env::var("FB_XAR_INVOKED_NAME").is_ok() {
                command.env("PAR_MAIN_OVERRIDE", main);
            } else {
                command.arg("-m").arg(main);
            }
        }

        // Spawn server process.
        let mut handle = command
            .arg("pipe")
            .stdout(Stdio::piped())
            .stdin(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;

        // Send init args.
        let mut pipe = AsyncStreamPipe::new(
            handle.stdout.take().unwrap(),
            handle.stdin.take().unwrap(),
            params.max_messages as usize,
        );
        let params = OutOfProcessSetupParams {
            ranks: params.ranks,
            sizes: params.sizes,
            function: params.function,
            args: params.args,
            kwargs: params.kwargs,
        };
        tokio::select! {
            res = handle.wait() => bail!("pipe server exited: {:?}", res),
            res = pipe.send(params) => res?,
        }

        Ok(Self {
            pipe: Some(pipe),
            handle,
        })
    }
}

impl PipeActor {
    /// Forcibly kill and cleanup the pipe server. Avoids `await` to be usable
    /// in `Drop`.
    fn kill_pipe_server(&mut self) -> Result<()> {
        self.handle.start_kill()?;

        // NOT(agallagher): Since this is called from `drop()`, we can't
        // use the async `wait()` method (is there a way to convert to
        // `std::process::Child`?).
        let pid = Pid::from_raw(self.handle.id().context("cannot get pid")? as i32);
        match nix::sys::wait::waitpid(pid, None)? {
            WaitStatus::Exited(_, 0) => (),
            status => bail!("exited abnormally: {:?}", status),
        }
        Ok(())
    }
}

// TODO(agallager): It'd be nice if the `Actor` API had a `shutdown` mechanism
// which could allow for preserving error propagation in cases like this.
impl Drop for PipeActor {
    fn drop(&mut self) {
        // Close the pipe first, which should make the server end get an EPIPE
        // and die.
        self.pipe.take();

        // Kill/cleanup the server.
        if let Err(err) = self.kill_pipe_server() {
            tracing::warn!("error cleaning up pipe server: {}", err);
        }
    }
}

#[async_trait]
#[forward(PipeMessage)]
impl PipeMessageHandler for PipeActor {
    async fn send_value(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        val: Result<PyTree<RValue>, WorkerError>,
    ) -> Result<()> {
        // TODO(agallagher): Propagate failures and use a timeout and handle worker errors?
        let val = val.map_err(|err| anyhow::anyhow!(err.backtrace).context("worker error"))?;
        tokio::select! {
            res = self.handle.wait() => bail!("pipe server exited: {:?}", res),
            res = self.pipe.as_mut().unwrap().send(val) => res?,
        };
        Ok(())
    }

    async fn recv_value(&mut self, _cx: &hyperactor::Context<Self>) -> Result<PyTree<RValue>> {
        // TODO(agallagher): Propagate failures and use a timeout?
        tokio::select! {
            res = self.handle.wait() => bail!("pipe server exited: {:?}", res),
            res = self.pipe.as_mut().unwrap().recv() => res
        }
    }
}
