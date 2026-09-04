/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Multiplexes independent protocols over authenticated link-local QUIC streams.

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::num::NonZeroUsize;
use std::sync::Arc;

use chrysalis_core::Pid;
use thiserror::Error;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::task::JoinSet;

use crate::DatagramAddr;
use crate::DatagramSocket;
use crate::IncomingStream;
use crate::QuicTransport;
use crate::QuicTransportError;
use crate::Stream;
use crate::shutdown::CompletionGuard;
use crate::shutdown::ShutdownState;

/// Number of bytes in the protocol selector at the start of a link-local stream.
pub const LINK_LOCAL_PROTOCOL_ID_LEN: usize = 16;

const DEFAULT_INCOMING_CAPACITY: usize = 1024;

/// A stable identifier for one link-local stream protocol.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LinkLocalProtocolId([u8; LINK_LOCAL_PROTOCOL_ID_LEN]);

impl LinkLocalProtocolId {
    /// Constructs a protocol identifier from its wire representation.
    pub const fn from_bytes(bytes: [u8; LINK_LOCAL_PROTOCOL_ID_LEN]) -> Self {
        Self(bytes)
    }

    /// Returns the protocol identifier's wire representation.
    pub const fn as_bytes(&self) -> &[u8; LINK_LOCAL_PROTOCOL_ID_LEN] {
        &self.0
    }
}

impl fmt::Debug for LinkLocalProtocolId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "LinkLocalProtocolId(")?;
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        write!(formatter, ")")
    }
}

/// A link-local stream registration or I/O failure.
#[derive(Debug, Error)]
pub enum LinkLocalError {
    /// The same protocol identifier was registered more than once.
    #[error("duplicate link-local protocol registration: {0:?}")]
    DuplicateProtocol(LinkLocalProtocolId),

    /// The underlying authenticated QUIC transport failed.
    #[error(transparent)]
    Transport(#[from] QuicTransportError),

    /// The protocol selector could not be written.
    #[error("failed to write link-local protocol selector: {0}")]
    SelectorWrite(#[from] io::Error),

    /// The protocol no longer accepts streams.
    #[error("link-local protocol is closed")]
    Closed,
}

/// One registered link-local protocol.
pub struct LinkLocalProtocol<T: DatagramSocket> {
    id: LinkLocalProtocolId,
    transport: Arc<QuicTransport<T>>,
    incoming: Arc<AsyncMutex<mpsc::Receiver<IncomingStream>>>,
}

impl<T: DatagramSocket> LinkLocalProtocol<T> {
    /// Returns this registration's protocol identifier.
    pub const fn id(&self) -> LinkLocalProtocolId {
        self.id
    }

    /// Opens an authenticated stream to an adjacent peer and writes this protocol's selector.
    pub async fn dial(&self, peer: Pid, address: DatagramAddr) -> Result<Stream, LinkLocalError> {
        let mut stream = self.transport.dial_link_local(peer, address).await?;
        stream.send_mut().write_all(self.id.as_bytes()).await?;
        Ok(stream)
    }

    /// Opens a stream without a PID pin and returns the certificate-derived peer PID.
    pub async fn dial_unpinned(
        &self,
        address: DatagramAddr,
    ) -> Result<(Pid, Stream), LinkLocalError> {
        let (peer, mut stream) = self.transport.dial_link_local_unpinned(address).await?;
        stream.send_mut().write_all(self.id.as_bytes()).await?;
        Ok((peer, stream))
    }

    /// Accepts the next authenticated stream selected for this protocol.
    pub async fn accept(&self) -> Result<IncomingStream, LinkLocalError> {
        self.incoming
            .lock()
            .await
            .recv()
            .await
            .ok_or(LinkLocalError::Closed)
    }
}

impl<T: DatagramSocket> Clone for LinkLocalProtocol<T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            transport: self.transport.clone(),
            incoming: self.incoming.clone(),
        }
    }
}

impl<T: DatagramSocket> fmt::Debug for LinkLocalProtocol<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LinkLocalProtocol")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

/// Dispatches PID 0 streams to a fixed set of registered protocols.
pub struct LinkLocalMux<T: DatagramSocket> {
    protocols: HashMap<LinkLocalProtocolId, LinkLocalProtocol<T>>,
    shutdown_state: Arc<ShutdownState>,
    supervisor_task: JoinHandle<()>,
}

impl<T: DatagramSocket + 'static> LinkLocalMux<T> {
    /// Spawns a mux with one isolated incoming queue per protocol.
    pub fn spawn(
        transport: Arc<QuicTransport<T>>,
        protocols: impl IntoIterator<Item = LinkLocalProtocolId>,
    ) -> Result<Self, LinkLocalError> {
        let capacity = NonZeroUsize::new(DEFAULT_INCOMING_CAPACITY)
            .expect("default incoming capacity is nonzero");
        Self::spawn_with_incoming_capacity(transport, protocols, capacity)
    }

    /// Spawns a mux with an explicit per-protocol incoming queue capacity.
    pub fn spawn_with_incoming_capacity(
        transport: Arc<QuicTransport<T>>,
        protocols: impl IntoIterator<Item = LinkLocalProtocolId>,
        incoming_capacity: NonZeroUsize,
    ) -> Result<Self, LinkLocalError> {
        let mut registrations = HashMap::new();
        let mut handles = HashMap::new();
        for id in protocols {
            if registrations.contains_key(&id) {
                return Err(LinkLocalError::DuplicateProtocol(id));
            }
            let (send, recv) = mpsc::channel(incoming_capacity.get());
            registrations.insert(id, send);
            handles.insert(
                id,
                LinkLocalProtocol {
                    id,
                    transport: transport.clone(),
                    incoming: Arc::new(AsyncMutex::new(recv)),
                },
            );
        }
        let shutdown_state = Arc::new(ShutdownState::default());
        let task_state = shutdown_state.clone();
        let task_transport = transport.clone();
        let supervisor_task = tokio::spawn(async move {
            supervise(task_transport, registrations, task_state).await;
        });
        Ok(Self {
            protocols: handles,
            shutdown_state,
            supervisor_task,
        })
    }

    /// Returns a handle for one registered protocol.
    pub fn protocol(&self, id: LinkLocalProtocolId) -> Option<LinkLocalProtocol<T>> {
        self.protocols.get(&id).cloned()
    }

    /// Idempotently requests mux shutdown.
    pub fn shutdown(&self) {
        self.shutdown_state.shutdown();
    }

    /// Waits for dispatch and the underlying transport to terminate.
    pub async fn join(&self) {
        self.shutdown_state.join().await;
    }
}

impl<T: DatagramSocket> Drop for LinkLocalMux<T> {
    fn drop(&mut self) {
        self.shutdown_state.shutdown();
        self.supervisor_task.abort();
    }
}

async fn supervise<T: DatagramSocket + 'static>(
    transport: Arc<QuicTransport<T>>,
    registrations: HashMap<LinkLocalProtocolId, mpsc::Sender<IncomingStream>>,
    shutdown_state: Arc<ShutdownState>,
) {
    let _completion = CompletionGuard::new(&shutdown_state);
    let registrations = Arc::new(registrations);
    let mut classifiers = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            () = shutdown_state.cancelled() => break,
            completed = classifiers.join_next(), if !classifiers.is_empty() => {
                if completed.is_some_and(|result| result.is_err()) {
                    shutdown_state.shutdown();
                    break;
                }
            }
            incoming = transport.accept_link_local() => {
                let Ok(incoming) = incoming else {
                    break;
                };
                classifiers.spawn(classify(incoming, registrations.clone()));
            }
        }
    }
    classifiers.abort_all();
    while classifiers.join_next().await.is_some() {}
}

async fn classify(
    mut incoming: IncomingStream,
    registrations: Arc<HashMap<LinkLocalProtocolId, mpsc::Sender<IncomingStream>>>,
) {
    let mut bytes = [0; LINK_LOCAL_PROTOCOL_ID_LEN];
    if incoming
        .stream_mut()
        .recv_mut()
        .read_exact(&mut bytes)
        .await
        .is_err()
    {
        reject(&mut incoming).await;
        return;
    }
    let id = LinkLocalProtocolId::from_bytes(bytes);
    let Some(delivery) = registrations.get(&id) else {
        reject(&mut incoming).await;
        return;
    };
    if let Err(error) = delivery.send(incoming).await {
        let mut incoming = error.0;
        reject(&mut incoming).await;
    }
}

async fn reject(incoming: &mut IncomingStream) {
    let _ = incoming.stream_mut().send_mut().finish().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_id_debug_is_stable_hex() {
        let id = LinkLocalProtocolId::from_bytes([0xab; LINK_LOCAL_PROTOCOL_ID_LEN]);
        assert_eq!(
            format!("{id:?}"),
            "LinkLocalProtocolId(abababababababababababababababab)"
        );
    }
}
