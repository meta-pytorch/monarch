/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::sync::Arc;

use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt as _;

use super::Cancel;
use super::ConnectionResources;
use super::TaskCancellation;
use super::VsockDatagramEngine;
use crate::DatagramAddr;
use crate::vsock::frame::FrameKind;
use crate::vsock::frame::read_frame;

impl VsockDatagramEngine {
    /// Starts the reader and writer tasks for one established local stream
    /// endpoint.
    ///
    /// ```text
    /// ConnectionHandle::send_tx → send_rx → writer → stream endpoint
    /// stream endpoint → reader → deliver → shared receive queue
    /// ```
    ///
    /// If either task ends, it cancels the other and removes the map entry only
    /// if `id` still owns it.
    pub(super) fn run_connection<Reader, Writer>(
        self: &Arc<Self>,
        resources: ConnectionResources<Reader, Writer>,
    ) where
        Reader: AsyncRead + Send + Unpin + 'static,
        Writer: AsyncWrite + Send + Unpin + 'static,
    {
        // Move the established endpoint's resources into the two tasks that
        // will own them.
        let ConnectionResources {
            label,
            id,
            cancel,
            mut read,
            mut write,
            mut send_rx,
        } = resources;

        // Each task gets a cleanup guard. Dropping either guard cancels the
        // sibling task and removes the map entry if this connection ID still
        // owns it.
        let writer_guard = ConnectionTaskGuard {
            engine: self.clone(),
            label: label.clone(),
            id,
            cancel: cancel.clone(),
        };
        self.spawn(TaskCancellation::Connection(cancel.clone()), async move {
            // Keep cleanup ownership for the writer task's entire lifetime.
            let _guard = writer_guard;

            // Preserve queue order while writing complete frames to the local
            // endpoint. EOF on the queue or any write error ends this
            // connection.
            while let Some(frame) = send_rx.recv().await {
                if write.write_all(&frame).await.is_err() {
                    return;
                }
            }
        });

        let reader_guard = ConnectionTaskGuard {
            engine: self.clone(),
            label: label.clone(),
            id,
            cancel: cancel.clone(),
        };
        let engine = self.clone();
        self.spawn(TaskCancellation::Connection(cancel), async move {
            // Keep cleanup ownership for the reader task's entire lifetime.
            let _guard = reader_guard;

            // HELLO was consumed during admission. Every later frame must
            // contain one datagram. EOF, malformed input, or another HELLO ends
            // this connection.
            while let Ok((FrameKind::Datagram, payload)) = read_frame(&mut read).await {
                if !engine.deliver(label.clone(), payload).await {
                    return;
                }
            }
        });
    }
}

/// Cleanup guard held for the lifetime of one connection reader or writer task.
///
/// Each connection creates one guard for its reader and one for its writer.
/// When either task drops its guard, the guard cancels the sibling task and
/// removes the connection-map entry only if this connection ID still owns it.
struct ConnectionTaskGuard {
    /// Engine containing the connection map and task lifecycle.
    engine: Arc<VsockDatagramEngine>,

    /// Peer address used as the connection-map key.
    label: DatagramAddr,

    /// Connection incarnation this guard is allowed to remove.
    id: u64,

    /// Shared signal used to stop the sibling task.
    cancel: Arc<Cancel>,
}

impl Drop for ConnectionTaskGuard {
    /// Stops the sibling task and releases this connection's map entry.
    ///
    /// `remove_if_owned` checks the connection ID so cleanup from an older,
    /// replaced connection cannot remove its successor.
    fn drop(&mut self) {
        self.cancel.cancel();
        self.engine.remove_if_owned(&self.label, self.id);
    }
}
