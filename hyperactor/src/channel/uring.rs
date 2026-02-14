/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Experimental Linux `io_uring` channel primitives with a future-based API.
//!
//! This module intentionally does not depend on `tokio-uring`; it runs a
//! dedicated `io_uring` reactor thread and exposes async `send` / `recv`
//! methods that compose with the existing Tokio runtime.
//!
//! Current scope:
//! - TCP only.
//! - Single accepted connection per `accept` call.
//! - Length-prefixed bincode payloads (`u64` big-endian length + payload).

use core::net::SocketAddr;
use std::marker::PhantomData;

use crate::RemoteMessage;
use crate::channel::ChannelError;
use crate::channel::SendError;

#[cfg(target_os = "linux")]
mod linux_impl {
    use std::collections::HashMap;
    use std::collections::VecDeque;
    use std::future::Future;
    use std::io;
    use std::io::ErrorKind;
    use std::os::fd::AsRawFd;
    use std::os::fd::RawFd;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::Condvar;
    use std::sync::Mutex as StdMutex;
    use std::sync::OnceLock;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use io_uring::IoUring;
    use io_uring::opcode;
    use io_uring::types;
    use tokio::sync::Mutex;

    use super::*;

    const RING_QUEUE_DEPTH: u32 = 256;

    #[derive(Debug, Copy, Clone)]
    enum OpDirection {
        Read,
        Write,
    }

    enum OpResult {
        Read(Vec<u8>),
        Write,
    }

    struct OpStateInner {
        result: Option<io::Result<OpResult>>,
        waker: Option<Waker>,
    }

    struct OpState(StdMutex<OpStateInner>);

    impl OpState {
        fn new() -> Self {
            Self(StdMutex::new(OpStateInner {
                result: None,
                waker: None,
            }))
        }

        fn complete(&self, result: io::Result<OpResult>) {
            let mut inner = self.0.lock().expect("op state lock poisoned");
            if inner.result.is_some() {
                return;
            }
            inner.result = Some(result);
            if let Some(waker) = inner.waker.take() {
                waker.wake();
            }
        }

        fn poll_read(&self, cx: &mut Context<'_>) -> Poll<io::Result<Vec<u8>>> {
            let mut inner = self.0.lock().expect("op state lock poisoned");
            if let Some(result) = inner.result.take() {
                return match result {
                    Ok(OpResult::Read(data)) => Poll::Ready(Ok(data)),
                    Ok(OpResult::Write) => Poll::Ready(Err(io::Error::other(
                        "driver bug: write completion returned for read op",
                    ))),
                    Err(err) => Poll::Ready(Err(err)),
                };
            }
            inner.waker = Some(cx.waker().clone());
            Poll::Pending
        }

        fn poll_write(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            let mut inner = self.0.lock().expect("op state lock poisoned");
            if let Some(result) = inner.result.take() {
                return match result {
                    Ok(OpResult::Write) => Poll::Ready(Ok(())),
                    Ok(OpResult::Read(_)) => Poll::Ready(Err(io::Error::other(
                        "driver bug: read completion returned for write op",
                    ))),
                    Err(err) => Poll::Ready(Err(err)),
                };
            }
            inner.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    struct ReadExactFuture {
        state: Arc<OpState>,
    }

    impl Future for ReadExactFuture {
        type Output = io::Result<Vec<u8>>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.state.poll_read(cx)
        }
    }

    struct WriteAllFuture {
        state: Arc<OpState>,
    }

    impl Future for WriteAllFuture {
        type Output = io::Result<()>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.state.poll_write(cx)
        }
    }

    struct PendingOp {
        id: u64,
        fd: RawFd,
        direction: OpDirection,
        buffer: Vec<u8>,
        offset: usize,
        state: Arc<OpState>,
    }

    struct QueueState {
        stopped: bool,
        queue: VecDeque<PendingOp>,
    }

    struct DriverState {
        next_id: AtomicU64,
        queue: StdMutex<QueueState>,
        cv: Condvar,
    }

    impl DriverState {
        fn new() -> Self {
            Self {
                next_id: AtomicU64::new(1),
                queue: StdMutex::new(QueueState {
                    stopped: false,
                    queue: VecDeque::new(),
                }),
                cv: Condvar::new(),
            }
        }

        fn enqueue(&self, op: PendingOp) -> io::Result<Arc<OpState>> {
            let op_state = op.state.clone();
            let mut queue_state = self.queue.lock().expect("driver queue lock poisoned");
            if queue_state.stopped {
                return Err(io::Error::new(
                    ErrorKind::BrokenPipe,
                    "io_uring driver is stopped",
                ));
            }
            queue_state.queue.push_back(op);
            self.cv.notify_one();
            Ok(op_state)
        }

        fn next_op_id(&self) -> u64 {
            self.next_id.fetch_add(1, Ordering::Relaxed)
        }
    }

    struct DriverHandle {
        state: Arc<DriverState>,
    }

    static DRIVER: OnceLock<Result<DriverHandle, String>> = OnceLock::new();

    impl DriverHandle {
        fn start() -> io::Result<Self> {
            let state = Arc::new(DriverState::new());
            let ready = Arc::new((StdMutex::new(None::<io::Result<()>>), Condvar::new()));

            let worker_state = state.clone();
            let worker_ready = ready.clone();
            std::thread::Builder::new()
                .name("hyperactor-io-uring".to_string())
                .spawn(move || worker_loop(worker_state, worker_ready))
                .map_err(|err| {
                    io::Error::other(format!("failed to spawn io_uring worker thread: {err}"))
                })?;

            let (ready_lock, ready_cv) = &*ready;
            let mut ready_state = ready_lock.lock().expect("driver ready lock poisoned");
            while ready_state.is_none() {
                ready_state = ready_cv
                    .wait(ready_state)
                    .expect("driver ready condvar lock poisoned");
            }
            match ready_state.take().expect("ready state must be set") {
                Ok(()) => Ok(Self { state }),
                Err(err) => Err(err),
            }
        }

        fn read_exact(&self, fd: RawFd, len: usize) -> io::Result<ReadExactFuture> {
            let op_state = Arc::new(OpState::new());
            if len == 0 {
                op_state.complete(Ok(OpResult::Read(Vec::new())));
                return Ok(ReadExactFuture { state: op_state });
            }

            let op = PendingOp {
                id: self.state.next_op_id(),
                fd,
                direction: OpDirection::Read,
                buffer: vec![0u8; len],
                offset: 0,
                state: op_state,
            };
            let state = self.state.enqueue(op)?;
            Ok(ReadExactFuture { state })
        }

        fn write_all(&self, fd: RawFd, data: Vec<u8>) -> io::Result<WriteAllFuture> {
            let op_state = Arc::new(OpState::new());
            if data.is_empty() {
                op_state.complete(Ok(OpResult::Write));
                return Ok(WriteAllFuture { state: op_state });
            }

            let op = PendingOp {
                id: self.state.next_op_id(),
                fd,
                direction: OpDirection::Write,
                buffer: data,
                offset: 0,
                state: op_state,
            };
            let state = self.state.enqueue(op)?;
            Ok(WriteAllFuture { state })
        }
    }

    fn driver() -> Result<&'static DriverHandle, ChannelError> {
        match DRIVER.get_or_init(|| DriverHandle::start().map_err(|err| err.to_string())) {
            Ok(handle) => Ok(handle),
            Err(err) => Err(ChannelError::Other(anyhow::anyhow!(
                "failed to initialize io_uring driver: {err}"
            ))),
        }
    }

    fn set_ready(ready: &Arc<(StdMutex<Option<io::Result<()>>>, Condvar)>, result: io::Result<()>) {
        let (ready_lock, ready_cv) = &**ready;
        let mut ready_state = ready_lock.lock().expect("driver ready lock poisoned");
        *ready_state = Some(result);
        ready_cv.notify_one();
    }

    fn make_io_error(kind: ErrorKind, msg: &str) -> io::Error {
        io::Error::new(kind, msg.to_string())
    }

    fn fail_op(op: PendingOp, kind: ErrorKind, msg: &str) {
        op.state.complete(Err(make_io_error(kind, msg)));
    }

    fn fail_all_queued(state: &Arc<DriverState>, kind: ErrorKind, msg: &str) {
        let mut queue_state = state.queue.lock().expect("driver queue lock poisoned");
        queue_state.stopped = true;
        while let Some(op) = queue_state.queue.pop_front() {
            fail_op(op, kind, msg);
        }
        state.cv.notify_all();
    }

    fn submit_op(ring: &mut IoUring, op: &mut PendingOp) -> io::Result<bool> {
        let remaining = op.buffer.len() - op.offset;
        if remaining == 0 {
            return Ok(true);
        }

        let entry = match op.direction {
            OpDirection::Read => opcode::Read::new(
                types::Fd(op.fd),
                op.buffer[op.offset..].as_mut_ptr(),
                remaining as _,
            )
            .build()
            .user_data(op.id),
            OpDirection::Write => opcode::Write::new(
                types::Fd(op.fd),
                op.buffer[op.offset..].as_ptr(),
                remaining as _,
            )
            .build()
            .user_data(op.id),
        };

        // SAFETY:
        // - `op.buffer` remains allocated and unmoved while the operation is in-flight.
        // - `entry` is copied into the ring by `push`.
        unsafe {
            match ring.submission().push(&entry) {
                Ok(()) => Ok(true),
                Err(_) => Ok(false),
            }
        }
    }

    fn process_completions(
        ring: &mut IoUring,
        inflight: &mut HashMap<u64, PendingOp>,
        resubmit: &mut VecDeque<u64>,
    ) {
        let mut completions = Vec::new();
        for cqe in ring.completion() {
            completions.push((cqe.user_data(), cqe.result()));
        }

        for (id, result) in completions {
            let Some(mut op) = inflight.remove(&id) else {
                continue;
            };

            if result < 0 {
                let err = io::Error::from_raw_os_error(-result);
                op.state.complete(Err(err));
                continue;
            }

            let n = result as usize;
            if n == 0 {
                let err = match op.direction {
                    OpDirection::Read => {
                        io::Error::new(ErrorKind::UnexpectedEof, "unexpected EOF while reading")
                    }
                    OpDirection::Write => {
                        io::Error::new(ErrorKind::WriteZero, "write returned zero bytes")
                    }
                };
                op.state.complete(Err(err));
                continue;
            }

            op.offset += n;
            if op.offset >= op.buffer.len() {
                match op.direction {
                    OpDirection::Read => op.state.complete(Ok(OpResult::Read(op.buffer))),
                    OpDirection::Write => op.state.complete(Ok(OpResult::Write)),
                }
            } else {
                let id = op.id;
                inflight.insert(id, op);
                resubmit.push_back(id);
            }
        }
    }

    fn worker_loop(
        state: Arc<DriverState>,
        ready: Arc<(StdMutex<Option<io::Result<()>>>, Condvar)>,
    ) {
        let mut ring = match IoUring::new(RING_QUEUE_DEPTH) {
            Ok(ring) => {
                set_ready(&ready, Ok(()));
                ring
            }
            Err(err) => {
                set_ready(&ready, Err(err));
                return;
            }
        };

        let mut inflight: HashMap<u64, PendingOp> = HashMap::new();
        let mut resubmit: VecDeque<u64> = VecDeque::new();

        loop {
            {
                let mut queue_state = state.queue.lock().expect("driver queue lock poisoned");
                while queue_state.queue.is_empty()
                    && inflight.is_empty()
                    && resubmit.is_empty()
                    && !queue_state.stopped
                {
                    queue_state = state
                        .cv
                        .wait(queue_state)
                        .expect("driver queue condvar lock poisoned");
                }

                while let Some(op) = queue_state.queue.pop_front() {
                    let id = op.id;
                    inflight.insert(id, op);
                    resubmit.push_back(id);
                }

                if queue_state.stopped && inflight.is_empty() && resubmit.is_empty() {
                    break;
                }
            }

            while let Some(id) = resubmit.pop_front() {
                let Some(op) = inflight.get_mut(&id) else {
                    continue;
                };
                match submit_op(&mut ring, op) {
                    Ok(true) => {}
                    Ok(false) => {
                        resubmit.push_front(id);
                        break;
                    }
                    Err(err) => {
                        let msg = format!("failed to submit io_uring operation: {err}");
                        fail_all_queued(&state, ErrorKind::BrokenPipe, &msg);
                        for (_, pending) in inflight.drain() {
                            fail_op(pending, ErrorKind::BrokenPipe, &msg);
                        }
                        return;
                    }
                }
            }

            if inflight.is_empty() {
                continue;
            }

            if let Err(err) = ring.submit_and_wait(1) {
                let msg = format!("submit_and_wait failed: {err}");
                fail_all_queued(&state, ErrorKind::BrokenPipe, &msg);
                for (_, pending) in inflight.drain() {
                    fail_op(pending, ErrorKind::BrokenPipe, &msg);
                }
                return;
            }

            process_completions(&mut ring, &mut inflight, &mut resubmit);
        }

        fail_all_queued(&state, ErrorKind::BrokenPipe, "io_uring worker stopped");
        for (_, pending) in inflight.drain() {
            fail_op(pending, ErrorKind::BrokenPipe, "io_uring worker stopped");
        }
    }

    fn io_to_channel_err(err: io::Error, context: &'static str) -> ChannelError {
        ChannelError::Other(anyhow::anyhow!("{context}: {err}"))
    }

    #[derive(Debug)]
    struct WriterState {
        stream: Arc<std::net::TcpStream>,
        lock: Mutex<()>,
    }

    /// A typed `io_uring` sender.
    ///
    /// Clones share the same underlying connection and serialize writes.
    #[derive(Debug, Clone)]
    pub struct UringTx<M: RemoteMessage> {
        writer: Arc<WriterState>,
        _marker: PhantomData<M>,
    }

    impl<M: RemoteMessage> UringTx<M> {
        /// Send a typed message. Completes once bytes are written by `io_uring`.
        pub async fn send(&self, message: M) -> Result<(), SendError<M>> {
            let payload = match bincode::serialize(&message) {
                Ok(payload) => payload,
                Err(err) => {
                    return Err(SendError {
                        error: ChannelError::from(err),
                        message,
                        reason: Some("bincode serialization failed".to_string()),
                    });
                }
            };

            let max_frame = hyperactor_config::global::get(crate::config::CODEC_MAX_FRAME_LENGTH);
            if payload.len() > max_frame {
                return Err(SendError {
                    error: ChannelError::Other(anyhow::anyhow!(
                        "frame size {} exceeds CODEC_MAX_FRAME_LENGTH {}",
                        payload.len(),
                        max_frame
                    )),
                    message,
                    reason: Some("frame exceeds max configured size".to_string()),
                });
            }

            let driver = match driver() {
                Ok(driver) => driver,
                Err(error) => {
                    return Err(SendError {
                        error,
                        message,
                        reason: Some("io_uring driver unavailable".to_string()),
                    });
                }
            };

            let header = (payload.len() as u64).to_be_bytes().to_vec();
            let fd = self.writer.stream.as_raw_fd();
            let _write_guard = self.writer.lock.lock().await;

            let header_write = match driver.write_all(fd, header) {
                Ok(future) => future,
                Err(err) => {
                    return Err(SendError {
                        error: io_to_channel_err(err, "failed scheduling header write"),
                        message,
                        reason: None,
                    });
                }
            };
            if let Err(err) = header_write.await {
                return Err(SendError {
                    error: io_to_channel_err(err, "failed writing frame header"),
                    message,
                    reason: None,
                });
            }

            let payload_write = match driver.write_all(fd, payload) {
                Ok(future) => future,
                Err(err) => {
                    return Err(SendError {
                        error: io_to_channel_err(err, "failed scheduling payload write"),
                        message,
                        reason: None,
                    });
                }
            };
            if let Err(err) = payload_write.await {
                return Err(SendError {
                    error: io_to_channel_err(err, "failed writing frame payload"),
                    message,
                    reason: None,
                });
            }

            Ok(())
        }
    }

    /// A typed `io_uring` receiver.
    #[derive(Debug)]
    pub struct UringRx<M: RemoteMessage> {
        stream: Arc<std::net::TcpStream>,
        _marker: PhantomData<M>,
    }

    impl<M: RemoteMessage> UringRx<M> {
        /// Receive the next typed message from the stream.
        pub async fn recv(&mut self) -> Result<M, ChannelError> {
            let driver = driver()?;
            let fd = self.stream.as_raw_fd();

            let header_read = driver
                .read_exact(fd, std::mem::size_of::<u64>())
                .map_err(|err| io_to_channel_err(err, "failed scheduling frame header read"))?;
            let header = header_read
                .await
                .map_err(|err| io_to_channel_err(err, "failed reading frame header"))?;
            let len = u64::from_be_bytes(
                header
                    .try_into()
                    .map_err(|_| ChannelError::Other(anyhow::anyhow!("malformed frame header")))?,
            ) as usize;

            let max_frame = hyperactor_config::global::get(crate::config::CODEC_MAX_FRAME_LENGTH);
            if len > max_frame {
                return Err(ChannelError::Other(anyhow::anyhow!(
                    "incoming frame size {} exceeds CODEC_MAX_FRAME_LENGTH {}",
                    len,
                    max_frame
                )));
            }

            let payload_read = driver
                .read_exact(fd, len)
                .map_err(|err| io_to_channel_err(err, "failed scheduling frame payload read"))?;
            let payload = payload_read
                .await
                .map_err(|err| io_to_channel_err(err, "failed reading frame payload"))?;
            bincode::deserialize(&payload).map_err(ChannelError::from)
        }
    }

    /// A TCP listener for accepting `io_uring` receive channels.
    #[derive(Debug)]
    pub struct UringListener<M: RemoteMessage> {
        listener: tokio::net::TcpListener,
        _marker: PhantomData<M>,
    }

    impl<M: RemoteMessage> UringListener<M> {
        /// Bind a TCP listener.
        pub async fn bind(addr: SocketAddr) -> Result<Self, ChannelError> {
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .map_err(|err| io_to_channel_err(err, "failed to bind uring listener"))?;
            Ok(Self {
                listener,
                _marker: PhantomData,
            })
        }

        /// Get the local address that was bound.
        pub fn local_addr(&self) -> Result<SocketAddr, ChannelError> {
            self.listener
                .local_addr()
                .map_err(|err| io_to_channel_err(err, "failed to query listener local addr"))
        }

        /// Accept one incoming connection and create a typed receiver.
        pub async fn accept(&self) -> Result<UringRx<M>, ChannelError> {
            let (stream, _) = self
                .listener
                .accept()
                .await
                .map_err(|err| io_to_channel_err(err, "failed to accept uring connection"))?;
            let std_stream = stream
                .into_std()
                .map_err(|err| io_to_channel_err(err, "failed converting accepted socket"))?;
            std_stream.set_nonblocking(false).map_err(|err| {
                io_to_channel_err(err, "failed to set accepted socket blocking mode")
            })?;
            Ok(UringRx {
                stream: Arc::new(std_stream),
                _marker: PhantomData,
            })
        }
    }

    /// Dial a remote TCP address and create a typed `io_uring` sender.
    pub async fn dial<M: RemoteMessage>(addr: SocketAddr) -> Result<UringTx<M>, ChannelError> {
        let stream = tokio::net::TcpStream::connect(addr)
            .await
            .map_err(|err| io_to_channel_err(err, "failed connecting uring channel"))?;
        stream
            .set_nodelay(true)
            .map_err(|err| io_to_channel_err(err, "failed setting TCP_NODELAY"))?;
        let std_stream = stream
            .into_std()
            .map_err(|err| io_to_channel_err(err, "failed converting connected socket"))?;
        std_stream.set_nonblocking(false).map_err(|err| {
            io_to_channel_err(err, "failed to set connected socket blocking mode")
        })?;
        let writer = Arc::new(WriterState {
            stream: Arc::new(std_stream),
            lock: Mutex::new(()),
        });
        Ok(UringTx {
            writer,
            _marker: PhantomData,
        })
    }

    /// Convenience helper for one-shot server accept workflows.
    pub async fn serve_once<M: RemoteMessage>(
        addr: SocketAddr,
    ) -> Result<(SocketAddr, UringRx<M>), ChannelError> {
        let listener = UringListener::<M>::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        let rx = listener.accept().await?;
        Ok((local_addr, rx))
    }
}

pub use linux_impl::*;
