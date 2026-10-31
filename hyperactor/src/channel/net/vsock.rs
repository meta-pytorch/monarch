/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Vsock channel links: native `AF_VSOCK` endpoints (Linux only), and guest
//! ports reached through a hybrid-vsock VMM's host socket.

use std::future::Future;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use backoff::ExponentialBackoffBuilder;
use backoff::backoff::Backoff;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::time::Instant;

use super::ClientError;
use super::Link;
use super::ProtocolKind;
use super::SessionId;
use super::write_link_init;
use crate::channel::ChannelAddr;
use crate::channel::VsockAddr;
use crate::config;

/// Bound on one connect attempt, including any hybrid negotiation and the
/// link init.
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

/// Longest reply line accepted from a hybrid-vsock VMM.
const MAX_REPLY_LEN: usize = 64;

/// Runs `attempt` until it succeeds or `budget` has elapsed since the call.
///
/// Each attempt is bounded by [`ATTEMPT_TIMEOUT`] and by the deadline, and
/// failed attempts back off without sleeping past the deadline. A result
/// that arrives after its attempt's bound is discarded. At the deadline,
/// returns [`ClientError::ConnectTimeout`] carrying the last attempt's error.
pub(crate) async fn connect_until<S, F, Fut>(
    dest: ChannelAddr,
    budget: Duration,
    mut attempt: F,
) -> Result<S, ClientError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = io::Result<S>>,
{
    let timed_out = || io::Error::new(io::ErrorKind::TimedOut, "connect attempt timed out");
    let deadline = Instant::now() + budget;
    let mut backoff = ExponentialBackoffBuilder::new()
        .with_initial_interval(Duration::from_millis(1))
        .with_multiplier(2.0)
        .with_randomization_factor(0.1)
        .with_max_interval(Duration::from_millis(1000))
        .with_max_elapsed_time(None)
        .build();
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(ClientError::ConnectTimeout(dest, budget, timed_out()));
        }
        let attempt_deadline = std::cmp::min(now + ATTEMPT_TIMEOUT, deadline);
        let err = match tokio::time::timeout_at(attempt_deadline, attempt()).await {
            // `timeout_at` polls the attempt before its timer, so a result can
            // arrive after the attempt's bound; such a result is dropped.
            Ok(Ok(stream)) if Instant::now() < attempt_deadline => return Ok(stream),
            Ok(Ok(_)) | Err(_) => timed_out(),
            Ok(Err(err)) => err,
        };
        tracing::debug!(%dest, error = %err, "vsock connect failed, backing off");
        if Instant::now() >= deadline {
            return Err(ClientError::ConnectTimeout(dest, budget, err));
        }
        let delay = backoff
            .next_backoff()
            .expect("should back off without an elapsed-time limit");
        tokio::time::sleep_until(std::cmp::min(Instant::now() + delay, deadline)).await;
        if Instant::now() >= deadline {
            return Err(ClientError::ConnectTimeout(dest, budget, err));
        }
    }
}

/// Connects to the hybrid-vsock VMM socket `uds` and asks the VMM to forward
/// the connection to guest port `port`. Returns the stream once the VMM
/// accepts; bytes after the VMM's reply remain unread in the stream.
pub(crate) async fn hybrid_connect(uds: &Path, port: u32) -> io::Result<UnixStream> {
    let mut stream = UnixStream::connect(uds).await?;
    stream
        .write_all(format!("CONNECT {port}\n").as_bytes())
        .await?;

    let mut line = Vec::with_capacity(MAX_REPLY_LEN);
    while line.last() != Some(&b'\n') {
        if line.len() == MAX_REPLY_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("hybrid vsock reply exceeds {MAX_REPLY_LEN} bytes"),
            ));
        }
        line.push(stream.read_u8().await?);
    }

    let accepted = line
        .strip_prefix(b"OK ")
        .and_then(|rest| rest.strip_suffix(b"\n"))
        .is_some_and(|digits| !digits.is_empty() && digits.iter().all(u8::is_ascii_digit));
    if !accepted {
        return Err(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            format!(
                "hybrid vsock connect to port {port} refused: {:?}",
                String::from_utf8_lossy(&line)
            ),
        ));
    }
    Ok(stream)
}

/// A link that dials a guest port through a hybrid-vsock VMM's host socket.
#[derive(Debug)]
pub(crate) struct HybridVsockLink {
    uds: PathBuf,
    port: u32,
    session_id: SessionId,
    stream_id: u8,
    kind: ProtocolKind,
}

#[async_trait]
impl Link for HybridVsockLink {
    type Stream = UnixStream;

    fn dest(&self) -> ChannelAddr {
        ChannelAddr::Vsock(VsockAddr::Hybrid {
            uds: self.uds.clone(),
            port: self.port,
        })
    }

    fn link_id(&self) -> SessionId {
        self.session_id
    }

    async fn next(&mut self) -> Result<Self::Stream, ClientError> {
        let budget = hyperactor_config::global::get(config::CHANNEL_RECONNECT_TIMEOUT);
        connect_until(self.dest(), budget, || async {
            let mut stream = hybrid_connect(&self.uds, self.port).await?;
            write_link_init(&mut stream, self.session_id, self.stream_id, self.kind).await?;
            Ok(stream)
        })
        .await
    }
}

/// Create a link to guest port `port` behind the hybrid-vsock VMM socket
/// `uds`.
pub(crate) fn hybrid_link(
    uds: PathBuf,
    port: u32,
    session_id: SessionId,
    stream_id: u8,
    kind: ProtocolKind,
) -> HybridVsockLink {
    HybridVsockLink {
        uds,
        port,
        session_id,
        stream_id,
        kind,
    }
}

#[cfg(target_os = "linux")]
pub(crate) use native::VsockLink;
#[cfg(target_os = "linux")]
pub(crate) use native::VsockSocketListener;
#[cfg(target_os = "linux")]
pub(crate) use native::listen;
#[cfg(target_os = "linux")]
pub(crate) use native::native_link;

/// Native `AF_VSOCK` links and listeners. Every descriptor is created
/// close-on-exec, so a child process never inherits a connection.
#[cfg(target_os = "linux")]
mod native {
    use std::io;
    use std::os::fd::AsRawFd;
    use std::os::fd::FromRawFd;
    use std::os::fd::OwnedFd;

    use async_trait::async_trait;
    use nix::sys::socket::AddressFamily;
    use nix::sys::socket::SockFlag;
    use nix::sys::socket::SockType;
    use nix::sys::socket::sockopt;
    use tokio::io::unix::AsyncFd;

    use super::ClientError;
    use super::Link;
    use super::ProtocolKind;
    use super::SessionId;
    use super::connect_until;
    use super::write_link_init;
    use crate::channel::ChannelAddr;
    use crate::channel::VsockAddr;
    use crate::channel::net::Listener;
    use crate::channel::net::ServerError;
    use crate::config;

    /// Creates a non-blocking, close-on-exec vsock stream socket.
    pub(super) fn stream_socket() -> io::Result<OwnedFd> {
        Ok(nix::sys::socket::socket(
            AddressFamily::Vsock,
            SockType::Stream,
            SockFlag::SOCK_CLOEXEC | SockFlag::SOCK_NONBLOCK,
            None,
        )?)
    }

    /// Connects to `cid:port`.
    pub(super) async fn connect(cid: u32, port: u32) -> io::Result<tokio_vsock::VsockStream> {
        let socket = stream_socket()?;
        match nix::sys::socket::connect(
            socket.as_raw_fd(),
            &nix::sys::socket::VsockAddr::new(cid, port),
        ) {
            Ok(()) | Err(nix::errno::Errno::EINPROGRESS) => {}
            Err(errno) => return Err(errno.into()),
        }

        let socket = AsyncFd::new(socket)?;
        let _writable = socket.writable().await?;
        let error = nix::sys::socket::getsockopt(socket.get_ref(), sockopt::SocketError)?;
        if error != 0 {
            return Err(io::Error::from_raw_os_error(error));
        }
        tokio_vsock::VsockStream::new(vsock::VsockStream::from(socket.into_inner()))
    }

    /// A link that dials a native `AF_VSOCK` endpoint.
    #[derive(Debug)]
    pub(crate) struct VsockLink {
        cid: u32,
        port: u32,
        session_id: SessionId,
        stream_id: u8,
        kind: ProtocolKind,
    }

    #[async_trait]
    impl Link for VsockLink {
        type Stream = tokio_vsock::VsockStream;

        fn dest(&self) -> ChannelAddr {
            ChannelAddr::Vsock(VsockAddr::Native {
                cid: self.cid,
                port: self.port,
            })
        }

        fn link_id(&self) -> SessionId {
            self.session_id
        }

        async fn next(&mut self) -> Result<Self::Stream, ClientError> {
            let budget = hyperactor_config::global::get(config::CHANNEL_RECONNECT_TIMEOUT);
            connect_until(self.dest(), budget, || async {
                let mut stream = connect(self.cid, self.port).await?;
                write_link_init(&mut stream, self.session_id, self.stream_id, self.kind).await?;
                Ok(stream)
            })
            .await
        }
    }

    /// Create a link to the native vsock endpoint `cid:port`.
    pub(crate) fn native_link(
        cid: u32,
        port: u32,
        session_id: SessionId,
        stream_id: u8,
        kind: ProtocolKind,
    ) -> VsockLink {
        VsockLink {
            cid,
            port,
            session_id,
            stream_id,
            kind,
        }
    }

    /// Server-side listener for native vsock sockets.
    #[derive(Debug)]
    pub(crate) struct VsockSocketListener {
        inner: AsyncFd<vsock::VsockListener>,
        addr: VsockAddr,
    }

    #[async_trait]
    impl Listener for VsockSocketListener {
        type Stream = tokio_vsock::VsockStream;

        async fn accept(&mut self) -> Result<(Self::Stream, ChannelAddr), ServerError> {
            loop {
                let mut guard =
                    self.inner.readable().await.map_err(|err| {
                        ServerError::Io(ChannelAddr::Vsock(self.addr.clone()), err)
                    })?;
                match guard.try_io(|listener| accept(listener.get_ref())) {
                    Ok(Ok(Some(accepted))) => return Ok(accepted),
                    // The peer disconnected before its address could be read.
                    Ok(Ok(None)) => continue,
                    Ok(Err(err)) if err.kind() == io::ErrorKind::Interrupted => continue,
                    Ok(Err(err)) => {
                        return Err(ServerError::Io(ChannelAddr::Vsock(self.addr.clone()), err));
                    }
                    Err(_would_block) => continue,
                }
            }
        }
    }

    /// Accepts one connection on a descriptor that is close-on-exec from
    /// creation.
    fn accept(
        listener: &vsock::VsockListener,
    ) -> io::Result<Option<(tokio_vsock::VsockStream, ChannelAddr)>> {
        let raw = nix::sys::socket::accept4(
            listener.as_raw_fd(),
            SockFlag::SOCK_CLOEXEC | SockFlag::SOCK_NONBLOCK,
        )?;
        // SAFETY: `accept4` returned a new descriptor that nothing else owns.
        let socket = unsafe { OwnedFd::from_raw_fd(raw) };
        let Ok(peer) =
            nix::sys::socket::getpeername::<nix::sys::socket::VsockAddr>(socket.as_raw_fd())
        else {
            return Ok(None);
        };
        let stream = tokio_vsock::VsockStream::new(vsock::VsockStream::from(socket))?;
        Ok(Some((
            stream,
            ChannelAddr::Vsock(VsockAddr::Native {
                cid: peer.cid(),
                port: peer.port(),
            }),
        )))
    }

    /// Binds a native vsock listener at `cid:port`, returning it with the
    /// address it is bound to.
    pub(crate) fn listen(cid: u32, port: u32) -> io::Result<(VsockSocketListener, VsockAddr)> {
        // `vsock::VsockListener::bind` creates its socket close-on-exec.
        let listener = vsock::VsockListener::bind_with_cid_port(cid, port)?;
        listener.set_nonblocking(true)?;
        let local = listener.local_addr()?;
        let addr = VsockAddr::Native {
            cid: local.cid(),
            port: local.port(),
        };
        Ok((
            VsockSocketListener {
                inner: AsyncFd::new(listener)?,
                addr: addr.clone(),
            },
            addr,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    #[cfg(target_os = "linux")]
    use std::os::fd::AsRawFd;
    #[cfg(target_os = "linux")]
    use std::os::fd::BorrowedFd;
    #[cfg(target_os = "linux")]
    use std::os::fd::RawFd;
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Poll;
    use std::time::Duration;

    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    use tokio::net::UnixListener;
    use tokio::net::UnixStream;
    use tokio::sync::Notify;
    use tokio::sync::oneshot;
    use tokio::task::JoinSet;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::channel::ChannelAddr;
    use crate::channel::Rx;
    use crate::channel::Tx;
    use crate::channel::VsockAddr;
    use crate::channel::dial;
    use crate::channel::net::ClientError;
    use crate::channel::net::Link;
    #[cfg(target_os = "linux")]
    use crate::channel::net::Listener;
    use crate::channel::net::ProtocolKind;
    use crate::channel::net::SessionId;
    use crate::channel::net::Stream;
    use crate::channel::net::unix;
    use crate::channel::serve;
    use crate::channel::serve_with_listener;

    const BOUND: Duration = Duration::from_secs(10);

    async fn bounded<F: Future>(what: &str, future: F) -> F::Output {
        tokio::time::timeout(BOUND, future)
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
    }

    /// What the fake VMM does with a connection after reading its
    /// `CONNECT` line.
    #[derive(Clone)]
    enum Action {
        /// Close the connection without replying.
        Close,
        /// Write these bytes, then hold the connection until shutdown.
        Reply(Vec<u8>),
        /// Hold the connection without replying, and signal client EOF.
        Silent,
        /// Reply `OK` and splice the connection to the target.
        Forward,
    }

    /// Where the fake VMM forwards connections.
    #[derive(Clone)]
    enum Target {
        None,
        Unix(PathBuf),
        #[cfg(target_os = "linux")]
        Vsock(u32, u32),
    }

    #[derive(Default)]
    struct FakeVmmState {
        lines: Mutex<Vec<String>>,
        connections: AtomicUsize,
        connect_seen: Notify,
        client_eof: Notify,
    }

    /// A stand-in for a hybrid-vsock VMM's host socket. Connection `i`
    /// follows `actions[i]`; later connections repeat the last action.
    struct FakeVmm {
        uds: PathBuf,
        state: Arc<FakeVmmState>,
        cancel: CancellationToken,
        tasks: JoinSet<()>,
        _dir: tempfile::TempDir,
    }

    impl FakeVmm {
        fn start(actions: Vec<Action>, target: Target) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let uds = dir.path().join("v.sock");
            let listener = UnixListener::bind(&uds).unwrap();
            let state = Arc::new(FakeVmmState::default());
            let cancel = CancellationToken::new();
            let mut tasks = JoinSet::new();
            tasks.spawn(Self::accept_loop(
                listener,
                actions,
                target,
                Arc::clone(&state),
                cancel.clone(),
            ));
            Self {
                uds,
                state,
                cancel,
                tasks,
                _dir: dir,
            }
        }

        async fn accept_loop(
            listener: UnixListener,
            actions: Vec<Action>,
            target: Target,
            state: Arc<FakeVmmState>,
            cancel: CancellationToken,
        ) {
            let mut conns = JoinSet::new();
            loop {
                let stream = tokio::select! {
                    _ = cancel.cancelled() => break,
                    accepted = listener.accept() => match accepted {
                        Ok((stream, _)) => stream,
                        Err(_) => break,
                    },
                };
                let index = state.connections.fetch_add(1, Ordering::SeqCst);
                let action = actions
                    .get(index)
                    .or(actions.last())
                    .cloned()
                    .unwrap_or(Action::Close);
                conns.spawn(Self::serve_conn(
                    stream,
                    action,
                    target.clone(),
                    Arc::clone(&state),
                    cancel.clone(),
                ));
            }
            conns.shutdown().await;
        }

        async fn serve_conn(
            mut stream: UnixStream,
            action: Action,
            target: Target,
            state: Arc<FakeVmmState>,
            cancel: CancellationToken,
        ) {
            let mut line = Vec::new();
            loop {
                let mut byte = [0u8; 1];
                match stream.read(&mut byte).await {
                    Ok(1) => {
                        line.push(byte[0]);
                        if byte[0] == b'\n' {
                            break;
                        }
                    }
                    _ => return,
                }
            }
            state
                .lines
                .lock()
                .unwrap()
                .push(String::from_utf8_lossy(&line).into_owned());
            state.connect_seen.notify_one();

            match action {
                Action::Close => {}
                Action::Reply(bytes) => {
                    let _ = stream.write_all(&bytes).await;
                    cancel.cancelled().await;
                }
                Action::Silent => {
                    let mut buf = [0u8; 16];
                    tokio::select! {
                        _ = cancel.cancelled() => {}
                        read = stream.read(&mut buf) => {
                            if matches!(read, Ok(0)) {
                                state.client_eof.notify_one();
                            }
                        }
                    }
                }
                Action::Forward => {
                    let mut upstream: Box<dyn Stream> = match target {
                        Target::None => return,
                        Target::Unix(path) => match UnixStream::connect(path).await {
                            Ok(upstream) => Box::new(upstream),
                            Err(_) => return,
                        },
                        #[cfg(target_os = "linux")]
                        Target::Vsock(cid, port) => match tokio_vsock::VsockStream::connect(
                            tokio_vsock::VsockAddr::new(cid, port),
                        )
                        .await
                        {
                            Ok(upstream) => Box::new(upstream),
                            Err(_) => return,
                        },
                    };
                    if stream.write_all(b"OK 1073741824\n").await.is_err() {
                        return;
                    }
                    tokio::select! {
                        _ = cancel.cancelled() => {}
                        _ = tokio::io::copy_bidirectional(&mut stream, &mut upstream) => {}
                    }
                }
            }
        }

        fn lines(&self) -> Vec<String> {
            self.state.lines.lock().unwrap().clone()
        }

        fn connections(&self) -> usize {
            self.state.connections.load(Ordering::SeqCst)
        }

        async fn shutdown(mut self) {
            self.cancel.cancel();
            bounded("fake vmm shutdown", async {
                while self.tasks.join_next().await.is_some() {}
            })
            .await;
        }
    }

    fn dest() -> ChannelAddr {
        ChannelAddr::Vsock(VsockAddr::Hybrid {
            uds: "/test/v.sock".into(),
            port: 1024,
        })
    }

    fn hybrid(uds: &Path, port: u32) -> ChannelAddr {
        ChannelAddr::Vsock(VsockAddr::Hybrid {
            uds: uds.to_path_buf(),
            port,
        })
    }

    #[tokio::test(start_paused = true)]
    async fn test_vsock_connect_until_succeeds_after_failures() {
        let attempts = AtomicUsize::new(0);
        let result = bounded(
            "connect_until",
            connect_until(dest(), Duration::from_secs(10), || {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt < 2 {
                        Err(io::Error::from(io::ErrorKind::ConnectionRefused))
                    } else {
                        Ok(42)
                    }
                }
            }),
        )
        .await;
        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn test_vsock_connect_until_deadline_covers_pending_attempt() {
        let budget = Duration::from_millis(300);
        let start = tokio::time::Instant::now();
        let result = bounded(
            "connect_until",
            connect_until(dest(), budget, std::future::pending::<io::Result<()>>),
        )
        .await;
        let Err(ClientError::ConnectTimeout(addr, elapsed, err)) = result else {
            panic!("expected ConnectTimeout, got {result:?}");
        };
        assert_eq!(addr, dest());
        assert_eq!(elapsed, budget);
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        assert!(
            start.elapsed() < Duration::from_millis(301),
            "the deadline must cut off a pending attempt: {:?}",
            start.elapsed()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_vsock_connect_until_preserves_last_error() {
        let result = bounded(
            "connect_until",
            connect_until(dest(), Duration::from_millis(200), || async {
                Err::<(), _>(io::Error::from(io::ErrorKind::ConnectionRefused))
            }),
        )
        .await;
        let Err(ClientError::ConnectTimeout(_, _, err)) = result else {
            panic!("expected ConnectTimeout, got {result:?}");
        };
        assert_eq!(err.kind(), io::ErrorKind::ConnectionRefused);
    }

    #[tokio::test(start_paused = true)]
    async fn test_vsock_connect_until_rejects_late_result() {
        let (ready, gate) = oneshot::channel::<()>();
        let mut gate = Some(gate);
        let mut connecting = Box::pin(connect_until(dest(), Duration::from_millis(300), || {
            let gate = gate.take();
            async move {
                match gate {
                    Some(gate) => {
                        let _ = gate.await;
                        Ok(42)
                    }
                    None => std::future::pending::<io::Result<i32>>().await,
                }
            }
        }));
        assert!(futures::poll!(connecting.as_mut()).is_pending());

        // The deadline passes while the helper is not polled; the attempt
        // then becomes ready before the next poll.
        tokio::time::advance(Duration::from_millis(400)).await;
        ready.send(()).unwrap();

        let Poll::Ready(result) = futures::poll!(connecting.as_mut()) else {
            panic!("an expired helper must resolve on its next poll");
        };
        let Err(ClientError::ConnectTimeout(_, _, err)) = result else {
            panic!("a result that arrives after the deadline must be rejected: {result:?}");
        };
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test(start_paused = true)]
    async fn test_vsock_connect_until_retries_after_late_attempt() {
        let (ready, gate) = oneshot::channel::<()>();
        let mut gate = Some(gate);
        let attempts = AtomicUsize::new(0);
        let mut connecting = Box::pin(connect_until(dest(), Duration::from_secs(60), || {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            let gate = gate.take();
            async move {
                if let Some(gate) = gate {
                    let _ = gate.await;
                }
                Ok(attempt)
            }
        }));
        assert!(futures::poll!(connecting.as_mut()).is_pending());

        // The first attempt outlives its own bound unpolled, then becomes
        // ready; its result must be discarded in favor of a fresh attempt.
        tokio::time::advance(ATTEMPT_TIMEOUT + Duration::from_secs(1)).await;
        ready.send(()).unwrap();

        let result = bounded("connect_until", connecting).await;
        assert_eq!(
            result.unwrap(),
            1,
            "the late first attempt must be discarded"
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_vsock_hybrid_connect_ok() {
        let vmm = FakeVmm::start(
            vec![Action::Reply(b"OK 1073741824\nSUFFIX".to_vec())],
            Target::None,
        );
        let mut stream = bounded("hybrid_connect", hybrid_connect(&vmm.uds, 1024))
            .await
            .unwrap();
        let mut suffix = [0u8; 6];
        bounded("suffix", stream.read_exact(&mut suffix))
            .await
            .unwrap();
        assert_eq!(
            &suffix, b"SUFFIX",
            "bytes after the OK line must stay in the stream"
        );
        assert_eq!(vmm.lines(), vec!["CONNECT 1024\n".to_string()]);
        drop(stream);
        vmm.shutdown().await;
    }

    #[tokio::test]
    async fn test_vsock_hybrid_connect_rejects_bad_replies() {
        let long_ok = format!("OK {}\n", "1".repeat(60)).into_bytes();
        assert_eq!(long_ok.len(), 64);
        let cases: Vec<(Action, Option<io::ErrorKind>)> = vec![
            (Action::Close, Some(io::ErrorKind::UnexpectedEof)),
            (
                Action::Reply(b"NO 1\n".to_vec()),
                Some(io::ErrorKind::ConnectionRefused),
            ),
            (
                Action::Reply(b"OK\n".to_vec()),
                Some(io::ErrorKind::ConnectionRefused),
            ),
            (
                Action::Reply(b"OK \n".to_vec()),
                Some(io::ErrorKind::ConnectionRefused),
            ),
            (
                Action::Reply(b"OK x\n".to_vec()),
                Some(io::ErrorKind::ConnectionRefused),
            ),
            (
                Action::Reply(b"OK 1 extra\n".to_vec()),
                Some(io::ErrorKind::ConnectionRefused),
            ),
            (
                Action::Reply(vec![b'x'; 64]),
                Some(io::ErrorKind::InvalidData),
            ),
            (Action::Reply(long_ok), None),
        ];
        for (action, expected) in cases {
            let vmm = FakeVmm::start(vec![action], Target::None);
            let result = bounded("hybrid_connect", hybrid_connect(&vmm.uds, 1024)).await;
            match expected {
                Some(kind) => {
                    let Err(err) = result else {
                        panic!("expected {kind:?}, got a stream");
                    };
                    assert_eq!(err.kind(), kind, "{err}");
                }
                None => {
                    result.unwrap();
                }
            }
            vmm.shutdown().await;
        }
    }

    #[tokio::test]
    async fn test_vsock_hybrid_connect_cancel_closes_stream() {
        let vmm = FakeVmm::start(
            vec![Action::Silent, Action::Reply(b"OK 1\n".to_vec())],
            Target::None,
        );
        let uds = vmm.uds.clone();
        let pending = tokio::spawn(async move { hybrid_connect(&uds, 1024).await });
        bounded("CONNECT", vmm.state.connect_seen.notified()).await;
        pending.abort();
        assert!(bounded("abort", pending).await.unwrap_err().is_cancelled());
        bounded("client EOF", vmm.state.client_eof.notified()).await;

        bounded("hybrid_connect", hybrid_connect(&vmm.uds, 1024))
            .await
            .unwrap();
        vmm.shutdown().await;
    }

    #[tokio::test]
    async fn test_vsock_hybrid_link_next_retries() {
        let vmm = FakeVmm::start(
            vec![
                Action::Close,
                Action::Close,
                Action::Reply(b"OK 1\n".to_vec()),
            ],
            Target::None,
        );
        let mut link = hybrid_link(
            vmm.uds.clone(),
            1024,
            SessionId::random(),
            0,
            ProtocolKind::Simplex,
        );
        let stream = bounded("link next", link.next()).await.unwrap();
        assert_eq!(
            vmm.connections(),
            3,
            "one call to next should retry past two refused negotiations"
        );
        drop(stream);
        vmm.shutdown().await;
    }

    #[tokio::test]
    async fn test_vsock_hybrid_dial_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target.sock");
        let (_, mut rx) = serve::<u64>(ChannelAddr::Unix(
            unix::SocketAddr::from_pathname(target.to_str().unwrap()).unwrap(),
        ))
        .unwrap();
        let vmm = FakeVmm::start(vec![Action::Forward], Target::Unix(target));

        let tx = dial::<u64>(hybrid(&vmm.uds, 1024)).unwrap();
        tx.post(7);
        tx.post(8);
        assert_eq!(bounded("first message", rx.recv()).await.unwrap(), 7);
        assert_eq!(bounded("second message", rx.recv()).await.unwrap(), 8);

        drop(tx);
        bounded("receiver join", rx.join()).await;
        vmm.shutdown().await;
    }

    #[tokio::test]
    async fn test_vsock_hybrid_link_retries_until_ok() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target.sock");
        let (_, mut rx) = serve::<u64>(ChannelAddr::Unix(
            unix::SocketAddr::from_pathname(target.to_str().unwrap()).unwrap(),
        ))
        .unwrap();
        let vmm = FakeVmm::start(
            vec![Action::Close, Action::Close, Action::Forward],
            Target::Unix(target),
        );

        let tx = dial::<u64>(hybrid(&vmm.uds, 1024)).unwrap();
        tx.post(7);
        assert_eq!(bounded("message", rx.recv()).await.unwrap(), 7);
        assert!(
            vmm.connections() >= 3,
            "the link should retry past refused negotiations: {} connections",
            vmm.connections()
        );

        drop(tx);
        bounded("receiver join", rx.join()).await;
        vmm.shutdown().await;
    }

    /// The vsock loopback context identifier (`VMADDR_CID_LOCAL`).
    #[cfg(target_os = "linux")]
    const CID_LOCAL: u32 = 1;

    /// Returns why vsock loopback cannot carry a connection here, if it
    /// cannot.
    #[cfg(target_os = "linux")]
    async fn loopback_unavailable() -> Option<String> {
        let probe = async {
            let listener = tokio_vsock::VsockListener::bind(tokio_vsock::VsockAddr::new(
                CID_LOCAL,
                VsockAddr::PORT_ANY,
            ))?;
            let port = listener.local_addr()?.port();
            let (connected, accepted) = tokio::join!(
                tokio_vsock::VsockStream::connect(tokio_vsock::VsockAddr::new(CID_LOCAL, port)),
                listener.accept(),
            );
            connected?;
            accepted?;
            io::Result::Ok(())
        };
        match tokio::time::timeout(Duration::from_secs(1), probe).await {
            Ok(Ok(())) => None,
            Ok(Err(err)) => Some(err.to_string()),
            Err(_) => Some("timed out".to_string()),
        }
    }

    /// Whether `fd` has `FD_CLOEXEC` set.
    #[cfg(target_os = "linux")]
    fn is_cloexec(fd: RawFd) -> bool {
        // SAFETY: the caller keeps `fd` open for the duration of the call.
        let fd = unsafe { BorrowedFd::borrow_raw(fd) };
        let flags = nix::fcntl::fcntl(fd, nix::fcntl::FcntlArg::F_GETFD).unwrap();
        nix::fcntl::FdFlag::from_bits_truncate(flags).contains(nix::fcntl::FdFlag::FD_CLOEXEC)
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_vsock_native_descriptors_are_cloexec() {
        if let Some(reason) = loopback_unavailable().await {
            eprintln!("skipping: vsock loopback unavailable: {reason}");
            return;
        }
        let socket = native::stream_socket().unwrap();
        assert!(
            is_cloexec(socket.as_raw_fd()),
            "a socket must be close-on-exec before connect begins"
        );

        let (mut listener, addr) = listen(CID_LOCAL, VsockAddr::PORT_ANY).unwrap();
        let VsockAddr::Native { port, .. } = addr else {
            panic!("expected a native vsock address, got {addr}");
        };
        let (connected, accepted) = bounded("connect and accept", async {
            tokio::join!(native::connect(CID_LOCAL, port), listener.accept())
        })
        .await;
        let connected = connected.unwrap();
        let (accepted, _) = accepted.unwrap();
        assert!(
            is_cloexec(connected.as_raw_fd()),
            "connected descriptors must be close-on-exec"
        );
        assert!(
            is_cloexec(accepted.as_raw_fd()),
            "accepted descriptors must be close-on-exec"
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_vsock_native_round_trip() {
        if let Some(reason) = loopback_unavailable().await {
            eprintln!("skipping: vsock loopback unavailable: {reason}");
            return;
        }
        let (addr, mut rx) = serve::<u64>(ChannelAddr::Vsock(VsockAddr::Native {
            cid: CID_LOCAL,
            port: VsockAddr::PORT_ANY,
        }))
        .unwrap();
        let ChannelAddr::Vsock(VsockAddr::Native { cid, port }) = addr.clone() else {
            panic!("expected a native vsock address, got {addr}");
        };
        assert_eq!(cid, CID_LOCAL);
        assert_ne!(
            port,
            VsockAddr::PORT_ANY,
            "the bound port should be reported"
        );

        let tx = dial::<u64>(addr).unwrap();
        tx.post(7);
        assert_eq!(bounded("message", rx.recv()).await.unwrap(), 7);

        drop(tx);
        bounded("receiver join", rx.join()).await;
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_vsock_alias_through_hybrid() {
        if let Some(reason) = loopback_unavailable().await {
            eprintln!("skipping: vsock loopback unavailable: {reason}");
            return;
        }
        let port = tokio_vsock::VsockListener::bind(tokio_vsock::VsockAddr::new(
            CID_LOCAL,
            VsockAddr::PORT_ANY,
        ))
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
        let vmm = FakeVmm::start(vec![Action::Forward], Target::Vsock(CID_LOCAL, port));

        let alias = ChannelAddr::Alias {
            dial_to: Box::new(hybrid(&vmm.uds, port)),
            bind_to: Box::new(ChannelAddr::Vsock(VsockAddr::Native {
                cid: CID_LOCAL,
                port,
            })),
        };
        let (advertised, mut rx) = serve::<u64>(alias).unwrap();
        assert_eq!(advertised, hybrid(&vmm.uds, port));

        let tx = dial::<u64>(advertised).unwrap();
        tx.post(7);
        assert_eq!(bounded("message", rx.recv()).await.unwrap(), 7);

        drop(tx);
        bounded("receiver join", rx.join()).await;
        vmm.shutdown().await;
    }

    #[tokio::test]
    async fn test_vsock_serve_rejects_prebound() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = ChannelAddr::Vsock(VsockAddr::Native { cid: 1, port: 5000 });
        let Err(err) = serve_with_listener::<u64>(addr, Some(listener)) else {
            panic!("serving vsock with a pre-opened listener should fail");
        };
        assert!(
            err.to_string()
                .contains("pre-opened listener not supported for vsock transport"),
            "unexpected error: {err}"
        );
    }
}
