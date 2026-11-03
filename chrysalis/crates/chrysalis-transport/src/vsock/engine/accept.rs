/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::io;
use std::os::fd::OwnedFd;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::unix::AsyncFd;
use tokio::net::UnixListener;
use tokio::sync::mpsc;

use super::Cancel;
use super::ConnectionHandle;
use super::ConnectionResources;
use super::QUEUE_CAPACITY;
use super::TaskCancellation;
use super::VsockDatagramEngine;
use crate::DatagramAddr;
use crate::vsock::address::VSOCK_SCHEME;
use crate::vsock::address::VsockDatagramAddr;
use crate::vsock::frame::FrameKind;
use crate::vsock::frame::read_frame;
use crate::vsock::lock_or_recover;
use crate::vsock::native::native_accept;

/// Delay before retrying a listener after an accept error.
const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(10);

impl VsockDatagramEngine {
    /// Admits one accepted stream endpoint and registers its peer connection.
    ///
    /// The peer's first frame must be a HELLO containing a valid `node:<name>`
    /// label. Invalid or missing HELLO closes only this endpoint. A valid label
    /// gets a per-connection send queue and a new connection ID.
    ///
    /// If another connection already owns the same label, this connection
    /// replaces and cancels it. The accepted endpoint's read/write halves and
    /// send queue are then transferred to [`Self::run_connection`].
    fn accept<Endpoint>(self: &Arc<Self>, endpoint: Endpoint)
    where
        Endpoint: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        #[cfg(test)]
        self.accepted
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let engine = self.clone();

        // Before HELLO supplies a peer label and connection-specific
        // cancellation signal, only engine shutdown can stop this admission
        // task.
        self.spawn(TaskCancellation::ShutdownOnly, async move {
            // Split this engine's local endpoint into independent read and
            // write halves. This admission task reads HELLO first; after
            // validation, `run_connection` transfers the read half to the
            // reader task and the write half to the writer.
            let (mut read, write) = tokio::io::split(endpoint);

            // HELLO must be the first frame and contain a valid node label.
            let Ok((FrameKind::Hello, payload)) = read_frame(&mut read).await else {
                return;
            };
            let label = DatagramAddr::new(VSOCK_SCHEME, &payload[..]);
            let Ok(VsockDatagramAddr::Node(_)) = VsockDatagramAddr::try_from(&label) else {
                return;
            };

            // Each connection has its own queue of frames waiting for its
            // writer.
            let (send_tx, send_rx) = mpsc::channel(QUEUE_CAPACITY);
            let id = engine.next_id();
            let cancel = Arc::new(Cancel::default());

            // Register this connection before starting its reader and writer
            // tasks.
            let replaced = lock_or_recover(&engine.connections).insert(
                label.clone(),
                ConnectionHandle {
                    id,
                    send_tx,
                    cancel: cancel.clone(),
                },
            );

            // A reconnect under the same name takes over from the older
            // connection.
            if let Some(replaced) = replaced {
                replaced.cancel.cancel();
            }

            // Transfer the endpoint halves, send queue, and cleanup
            // responsibility to the established connection tasks.
            engine.run_connection(ConnectionResources {
                label,
                id,
                cancel,
                read,
                write,
                send_rx,
            });
        });
    }
}

/// Accepts Unix stream endpoints until the enclosing engine task is cancelled.
///
/// On the Bob → Relay carrier link, this task runs in Relay's host process.
/// Relay listens on `<uds_B>_5001`; when Bob connects from the guest to host CID
/// 2, port 5001, Firecracker presents that connection on this Unix listener.
/// The accepted `UnixStream` is handed to the engine for HELLO validation and
/// connection registration.
///
/// A failed accept waits briefly before retrying to avoid a busy loop.
pub(super) async fn accept_unix(engine: Arc<VsockDatagramEngine>, listener: UnixListener) {
    // TODO: report accept errors once this crate has logging; until then a
    // persistent failure such as `EMFILE` retries silently.
    loop {
        match listener.accept().await {
            Ok((endpoint, _peer_addr)) => engine.accept(endpoint),
            Err(_) => tokio::time::sleep(ACCEPT_RETRY_DELAY).await,
        }
    }
}

/// Accepts native AF_VSOCK endpoints until the enclosing engine task is
/// cancelled.
///
/// On the Relay → Alice carrier link, this task runs in Alice's guest. Alice
/// listens on native-vsock port 5000; when Relay connects through Firecracker's
/// host-side `hybrid:` socket, Firecracker presents that connection to this
/// listener. `native_accept` returns Alice's local `NativeStreamEndpoint`, which
/// is handed to the engine for HELLO validation and connection registration.
///
/// `AsyncFd` first waits until the listener may be readable. `try_io` then
/// performs one nonblocking accept:
///
/// - an accepted endpoint is handed to the engine;
/// - an interrupted accept retries immediately;
/// - another OS error waits briefly before retrying;
/// - `WouldBlock` clears stale readiness and waits again.
pub(super) async fn accept_native(engine: Arc<VsockDatagramEngine>, listener: AsyncFd<OwnedFd>) {
    // TODO: report readiness and accept errors once this crate has logging;
    // until then a persistent failure such as `EMFILE` retries silently.
    loop {
        let Ok(mut ready) = listener.readable().await else {
            tokio::time::sleep(ACCEPT_RETRY_DELAY).await;
            continue;
        };
        match ready.try_io(|listener| native_accept(listener.get_ref())) {
            Ok(Ok(endpoint)) => engine.accept(endpoint),
            Ok(Err(error)) if error.kind() == io::ErrorKind::Interrupted => {}
            Ok(Err(_)) => tokio::time::sleep(ACCEPT_RETRY_DELAY).await,
            Err(_would_block) => {}
        }
    }
}
