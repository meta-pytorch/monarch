/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::sync::Arc;

use bytes::Bytes;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt as _;
use tokio::net::UnixStream;
use tokio::sync::mpsc;

use super::Cancel;
use super::ConnectionResources;
use super::VsockDatagramEngine;
use crate::DatagramAddr;
use crate::vsock::address::VsockDatagramAddr;
use crate::vsock::hybrid::hybrid_connect;
use crate::vsock::native::native_connect;

/// Owns cleanup while one physical destination is being connected.
///
/// For example, Relay 1 reaches its parent Alice through Alice's `hybrid:`
/// address:
///
/// 1. Relay 1's engine, running in the host process, reserves Alice's
///    `hybrid:` address in its local connection map.
/// 2. While the connection is being opened, Relay 1 stores datagrams for Alice
///    in that connection's send queue.
/// 3. `Dial` connects to Firecracker's Unix socket, requests Alice's guest
///    port, and sends Relay 1's HELLO frame.
/// 4. After HELLO is sent, the engine splits Relay 1's connected `UnixStream`—
///    Relay's local handle for this connection—into read and write halves and
///    starts one task for each. The writer drains the queued datagrams; both
///    tasks share cancellation and map cleanup.
/// 5. If connecting or sending HELLO fails, or setup is cancelled, dropping
///    `Dial` removes the reserved map entry and drops the queued datagrams. A
///    later send may try again.
///
/// `remove_on_drop` records whether cleanup ownership has been transferred to
/// the established connection tasks.
pub(super) struct Dial {
    /// Engine whose connection map contains this dial's reserved entry.
    engine: Arc<VsockDatagramEngine>,

    /// Physical destination reserved for this attempt, such as Alice's
    /// `hybrid:` address.
    destination: DatagramAddr,

    /// Unique ID used to ensure this attempt cannot remove a newer connection
    /// that replaced it under the same destination.
    id: u64,

    /// `true` while `Dial` owns failure cleanup; set to `false` after the
    /// established reader and writer tasks take over that responsibility.
    remove_on_drop: bool,
}

impl Dial {
    /// Creates cleanup for a connection-map entry already reserved by
    /// [`VsockDatagramEngine::queue_or_dial`].
    ///
    /// Cleanup starts enabled. If connecting or sending HELLO fails, dropping
    /// this value removes `destination` only when `id` still owns that entry.
    /// Successful setup disables this cleanup after the reader and writer tasks
    /// have started.
    pub(super) fn new(
        engine: Arc<VsockDatagramEngine>,
        destination: DatagramAddr,
        id: u64,
    ) -> Self {
        Self {
            engine,
            destination,
            id,
            remove_on_drop: true,
        }
    }

    /// Attempts to open the local stream endpoint described by `stream_addr`.
    ///
    /// For Relay 1 connecting to Alice:
    ///
    /// - `stream_addr` is Alice's `hybrid:` address;
    /// - `send_rx` is the receiving half of Relay 1's send queue for this
    ///   connection. It holds datagram frames whose next carrier hop is Alice.
    ///   `try_send_to` writes those frames through the matching `send_tx`, and
    ///   the established writer task drains `send_rx` onto Relay 1's stream
    ///   endpoint;
    /// - `cancel` becomes the shared cancellation signal for the established
    ///   reader and writer tasks.
    ///
    /// Datagrams whose next carrier hop is Alice follow this path:
    ///
    /// ```text
    /// Relay 1 try_send_to(Alice)
    /// → send_tx
    /// → per-connection send queue
    /// → send_rx
    /// → Relay 1 writer task
    /// → Relay 1's UnixStream endpoint
    /// → Firecracker bridge
    /// → Alice's native-vsock endpoint
    /// ```
    ///
    /// `Native`, `Hybrid`, and `Unix` each perform one connection attempt. For
    /// the Relay 1 example, `Hybrid` connects to Firecracker's Unix socket and
    /// returns Relay 1's local `UnixStream` endpoint.
    ///
    /// On success, the endpoint, cancellation signal, and `send_rx` are passed
    /// to [`Self::established`]. On failure, this method returns without
    /// disabling cleanup, so `Drop` removes the reserved map entry and drops
    /// `send_rx`, discarding its queued datagrams.
    ///
    /// `Node` cannot be dialed and is rejected before a `Dial` is created; that
    /// arm is defensive.
    pub(super) async fn run(
        mut self,
        stream_addr: VsockDatagramAddr,
        cancel: Arc<Cancel>,
        send_rx: mpsc::Receiver<Bytes>,
    ) {
        match stream_addr {
            VsockDatagramAddr::Native { cid, port } => {
                if let Ok(endpoint) = native_connect(cid, port).await {
                    self.established(endpoint, cancel, send_rx).await;
                }
            }
            VsockDatagramAddr::Hybrid { port, uds } => {
                if let Ok(endpoint) = hybrid_connect(&uds, port).await {
                    self.established(endpoint, cancel, send_rx).await;
                }
            }
            VsockDatagramAddr::Unix(path) => {
                if let Ok(endpoint) = UnixStream::connect(&path).await {
                    self.established(endpoint, cancel, send_rx).await;
                }
            }
            VsockDatagramAddr::Node(_) => {}
        }
    }

    /// Completes setup after the physical stream endpoint has connected.
    ///
    /// For Relay 1 connecting to Alice, `endpoint` is Relay 1's connected
    /// `UnixStream`, `send_rx` contains the datagrams waiting to cross that
    /// connection, and `cancel` will stop both established connection tasks.
    ///
    /// HELLO must be the first frame written to the endpoint. If that write
    /// fails, this method returns while `Dial` still owns cleanup. After HELLO
    /// succeeds, the endpoint halves, send queue, cancellation signal,
    /// destination, and connection ID are transferred together to
    /// [`VsockDatagramEngine::run_connection`]. Only then is `Dial` cleanup
    /// disabled.
    async fn established<Endpoint>(
        &mut self,
        endpoint: Endpoint,
        cancel: Arc<Cancel>,
        send_rx: mpsc::Receiver<Bytes>,
    ) where
        Endpoint: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        // Reader and writer tasks need independent access to Relay 1's local
        // endpoint.
        let (read, mut write) = tokio::io::split(endpoint);

        // HELLO must precede every datagram on a connection opened by this
        // engine.
        if write.write_all(&self.engine.hello).await.is_err() {
            return;
        }

        // Transfer all established-connection resources as one valid bundle.
        self.engine.run_connection(ConnectionResources {
            label: self.destination.clone(),
            id: self.id,
            cancel,
            read,
            write,
            send_rx,
        });

        // The reader/writer task guards now own map cleanup.
        self.remove_on_drop = false;
    }
}

impl Drop for Dial {
    fn drop(&mut self) {
        if self.remove_on_drop {
            self.engine.remove_if_owned(&self.destination, self.id);
        }
    }
}
