/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! TCP manager actor for RDMA fallback transport.
//!
//! Transfers buffer data over the default hyperactor channel transport
//! in chunks controlled by
//! [`RDMA_MAX_CHUNK_SIZE_MB`](crate::config::RDMA_MAX_CHUNK_SIZE_MB).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use bytes::BytesMut;
use dashmap::DashMap;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::OncePortHandle;
use hyperactor::PortHandle;
use hyperactor::RefClient;
use hyperactor::actor::ActorError;
use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::ChannelTx;
use hyperactor::channel::Rx;
use hyperactor::channel::Tx;
use hyperactor::context;
use hyperactor::context::Actor as _;
use hyperactor::reference;
use hyperactor::reference::OncePortRef;
use hyperactor_mesh::transport::default_transport;
use serde::Deserialize;
use serde::Serialize;
use serde_multipart::Part;
use tokio::time::timeout as tokio_timeout;
use tokio_util::sync::CancellationToken;
use typeuri::Named;

use super::TcpOp;
use crate::RdmaLocalMemory;
use crate::RdmaOp;
use crate::RdmaOpType;
use crate::RdmaTransportLevel;
use crate::backend::RdmaBackend;
use crate::rdma_manager_actor::GetTcpActorRefClient;
use crate::rdma_manager_actor::RdmaManagerActor;
use crate::rdma_manager_actor::RdmaManagerMessageClient;

/// [`Named`] wrapper around [`Part`] for use as a reply type.
///
/// [`Part`] itself does not implement [`Named`], which is required by
/// [`OncePortRef`]. This newtype adds the missing trait.
#[derive(Debug, Clone, Serialize, Deserialize, Named)]
pub struct TcpChunk(Part);
wirevalue::register_type!(TcpChunk);

/// Data chunk sent over direct parallel channels.
#[derive(Debug, Clone, Serialize, Deserialize, Named)]
struct TcpDataChunk {
    // Which specific transfer this chunk is associated with.
    transfer_id: usize,
    // Offset into the buffer for this chunk.
    offset: usize,
    data: Part,
}
wirevalue::register_type!(TcpDataChunk);

/// Tracks the progress of a single parallel transfer (read or write).
///
/// Shared between channel receive loops and actor message handlers
/// via a [`DashMap`].
#[derive(Debug)]
struct TransferState {
    /// Buffer backing this transfer, provided at construction.
    local_memory: Arc<dyn RdmaLocalMemory>,

    /// Number of chunks received so far.
    chunks_received: usize,

    /// Total chunks expected for this transfer.
    total_chunks: usize,

    /// Completion reply port. Fired when all chunks arrive
    /// or an error occurs.
    done: OncePortRef<Result<(), String>>,
}

impl TransferState {
    fn new(
        total_chunks: usize,
        local_memory: Arc<dyn RdmaLocalMemory>,
        done: OncePortRef<Result<(), String>>,
    ) -> Self {
        Self {
            local_memory,
            chunks_received: 0,
            total_chunks,
            done,
        }
    }
}

/// Sends the result of a completed transfer to the caller's reply port.
///
/// Sending an actor message from the spawned receiver task requires the
/// loop to own a dummy [`context::Actor`] impl. If the task sent directly
/// to a remote [`OncePortRef`], the message would appear to come from
/// this dummy context, and undeliverable messages wouldn't be handled
/// properly. This intermediate message lets us use a [`PortHandle`]
/// whose message cannot be undeliverable; the handler then forwards the
/// result using the real actor's context.
#[derive(Debug, Serialize, Deserialize, Named)]
struct SendTransferResult {
    done: OncePortRef<Result<(), String>>,
    result: Result<(), String>,
}

/// Fatal error from the receive loop.
///
/// The handler logs the error and returns `Err`, which triggers a
/// supervision event and crashes the actor.
#[derive(Debug, Serialize, Deserialize, Named)]
struct TransferError {
    message: String,
}

/// Set up the local TcpManagerActor to receive a parallel transfer from
/// a remote TcpManagerActor.
#[derive(Debug)]
struct RegisterTransferLocal {
    local_memory: Arc<dyn RdmaLocalMemory>,
    total_chunks: usize,
    done: OncePortRef<Result<(), String>>,
    // The transfer ID
    reply: OncePortHandle<usize>,
}

/// Tell the local TcpManagerActor to read local memory and push
/// chunks to `dest_addr`.
#[derive(Debug)]
struct ExecuteTransferLocal {
    transfer_id: usize,
    local_memory: Arc<dyn RdmaLocalMemory>,
    chunk_size: usize,
    dest_addr: ChannelAddr,
}

/// Serializable messages for the [`TcpManagerActor`].
///
/// These travel over the wire between processes. The [`Part`] payload
/// is transferred via the multipart codec without an extra copy.
#[derive(Handler, HandleClient, RefClient, Debug, Serialize, Deserialize, Named)]
enum TcpManagerMessage {
    /// Write a chunk of data into a registered buffer at the given offset.
    WriteChunk {
        buf_id: usize,
        offset: usize,
        data: Part,
        #[reply]
        reply: OncePortRef<Result<(), String>>,
    },
    /// Read a chunk of data from a registered buffer at the given offset.
    ReadChunk {
        buf_id: usize,
        offset: usize,
        size: usize,
        #[reply]
        reply: OncePortRef<Result<TcpChunk, String>>,
    },
    /// Return the channel address served by this actor for parallel transfers.
    /// `None` when parallelism is 1.
    GetChannelAddress {
        #[reply]
        reply: OncePortRef<Option<ChannelAddr>>,
    },
    /// Set up a remote TcpManagerActor to receive a parallel transfer from
    /// the sender.
    RegisterTransferRemote {
        buf_id: usize,
        total_chunks: usize,
        done: OncePortRef<Result<(), String>>,
        #[reply]
        reply: OncePortRef<Result<usize, String>>,
    },
    /// Tell the remote TcpManagerActor to read its local memory and push
    /// chunks to the dest_addr provided by the sender.
    ExecuteTransferRemote {
        transfer_id: usize,
        buf_id: usize,
        chunk_size: usize,
        dest_addr: ChannelAddr,
        #[reply]
        reply: OncePortRef<Result<(), String>>,
    },
}
wirevalue::register_type!(TcpManagerMessage);

/// TCP fallback RDMA backend actor.
///
/// Spawned as a child of [`RdmaManagerActor`]. Transfers buffer data
/// over the default hyperactor channel transport in chunks.
#[derive(Debug)]
#[hyperactor::export(
    handlers = [TcpManagerMessage],
)]
pub struct TcpManagerActor {
    owner: OnceLock<ActorHandle<RdmaManagerActor>>,
    next_transfer_id: usize,
    transfers: Arc<DashMap<usize, TransferState>>,
    /// Address of the direct channel served for parallel transfers.
    /// `None` when parallelism is 1 (default).
    channel_addr: Option<ChannelAddr>,
    /// Cached outbound connections keyed by remote channel address.
    outbound: HashMap<ChannelAddr, Vec<Arc<ChannelTx<TcpDataChunk>>>>,
    /// Cancellation token for spawned tasks.
    cancel: CancellationToken,
    /// Signaled when the parallel receive loop exits.
    receiver_done: Option<tokio::sync::oneshot::Receiver<()>>,
}

impl TcpManagerActor {
    pub fn new() -> Self {
        Self {
            owner: OnceLock::new(),
            next_transfer_id: 0,
            transfers: Arc::new(DashMap::new()),
            channel_addr: None,
            outbound: HashMap::new(),
            cancel: CancellationToken::new(),
            receiver_done: None,
        }
    }

    fn register_transfer(
        &mut self,
        local_memory: Arc<dyn RdmaLocalMemory>,
        total_chunks: usize,
        done: OncePortRef<Result<(), String>>,
    ) -> usize {
        let transfer_id = self.next_transfer_id;
        self.next_transfer_id += 1;
        self.transfers.insert(
            transfer_id,
            TransferState::new(total_chunks, local_memory, done),
        );
        transfer_id
    }

    fn execute_transfer(
        &mut self,
        cx: &Context<Self>,
        transfer_id: usize,
        local_memory: Arc<dyn RdmaLocalMemory>,
        chunk_size: usize,
        dest_addr: ChannelAddr,
    ) -> Result<()> {
        let parallelism =
            hyperactor_config::global::get(crate::config::RDMA_TCP_FALLBACK_PARALLELISM);

        if !self.outbound.contains_key(&dest_addr) {
            let conns = (0..parallelism)
                .map(|_| {
                    channel::dial::<TcpDataChunk>(dest_addr.clone())
                        .map(Arc::new)
                        .map_err(anyhow::Error::from)
                })
                .collect::<Result<Vec<_>>>()?;
            self.outbound.insert(dest_addr.clone(), conns);
        }
        let conns = self.outbound.get(&dest_addr).unwrap();

        let size = local_memory.size();
        let total_chunks = size.div_ceil(chunk_size);

        let chunk_index = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let error_port: PortHandle<TransferError> = cx.port();
        let proc = cx.instance().proc().clone();
        let cancel = self.cancel.clone();

        for conn in conns.clone() {
            let mem = local_memory.clone();
            let transfer_id = transfer_id;
            let chunk_index = chunk_index.clone();
            let error_port = error_port.clone();
            let proc = proc.clone();
            let cancel = cancel.clone();

            tokio::spawn(async move {
                let sender_name = reference::name::Name::generate(
                    reference::name::Ident::new("tcp_manager_actor".into()).unwrap(),
                    reference::name::Ident::new("tcp_chunk_sender".into()).unwrap(),
                );
                let (instance, _handle) = proc
                    .instance(&sender_name.to_string())
                    .expect("failed to create sender instance");

                loop {
                    if cancel.is_cancelled() {
                        return;
                    }

                    let idx = chunk_index.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if idx >= total_chunks {
                        break;
                    }

                    let offset = idx * chunk_size;
                    let len = std::cmp::min(chunk_size, size - offset);
                    let mut buf = BytesMut::zeroed(len);
                    if let Err(e) = mem.read_at(offset, &mut buf) {
                        error_port
                            .send(
                                &instance,
                                TransferError {
                                    message: format!("read_at failed at offset {offset}: {e}"),
                                },
                            )
                            .unwrap();
                        return;
                    }

                    let chunk = TcpDataChunk {
                        transfer_id,
                        offset,
                        data: Part::from(buf.freeze()),
                    };

                    if let Err(e) = conn.send(chunk).await {
                        error_port
                            .send(
                                &instance,
                                TransferError {
                                    message: format!(
                                        "failed to send chunk at offset {offset}: {e}"
                                    ),
                                },
                            )
                            .unwrap();
                        return;
                    }
                }
            });
        }

        Ok(())
    }

    /// Construct an [`ActorHandle`] for the local [`TcpManagerActor`]
    /// by querying the local [`RdmaManagerActor`].
    pub async fn local_handle(
        client: &(impl context::Actor + Send + Sync),
    ) -> Result<ActorHandle<Self>, anyhow::Error> {
        let rdma_handle = RdmaManagerActor::local_handle(client);
        let tcp_ref = rdma_handle.get_tcp_actor_ref(client).await?;
        tcp_ref
            .downcast_handle(client)
            .ok_or_else(|| anyhow::anyhow!("TcpManagerActor is not in the local process"))
    }
}

#[async_trait]
impl Actor for TcpManagerActor {
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        let owner = this.parent_handle().ok_or_else(|| {
            anyhow::anyhow!("RdmaManagerActor not found as parent of TcpManagerActor")
        })?;
        self.owner
            .set(owner)
            .map_err(|_| anyhow::anyhow!("TcpManagerActor owner already set"))?;

        let parallelism =
            hyperactor_config::global::get(crate::config::RDMA_TCP_FALLBACK_PARALLELISM);
        if parallelism > 1 {
            let addr = ChannelAddr::any(default_transport());
            let (bound_addr, mut rx) = channel::serve::<TcpDataChunk>(addr)?;
            self.channel_addr = Some(bound_addr);

            let transfers = self.transfers.clone();
            let proc = this.proc().clone();
            let result_port: PortHandle<SendTransferResult> = this.port();
            let error_port: PortHandle<TransferError> = this.port();
            let cancel = self.cancel.clone();
            let receiver_name = reference::name::Name::generate(
                reference::name::Ident::new("tcp_manager_actor".into()).unwrap(),
                reference::name::Ident::new("tcp_chunk_receiver".into()).unwrap(),
            );

            let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
            self.receiver_done = Some(done_rx);

            tokio::spawn(async move {
                let (instance, _handle) = proc
                    .instance(&receiver_name.to_string())
                    .expect("failed to create receiver instance");

                loop {
                    let chunk = tokio::select! {
                        _ = cancel.cancelled() => break,
                        result = rx.recv() => match result {
                            Ok(chunk) => chunk,
                            Err(e) => {
                                error_port
                                    .send(
                                        &instance,
                                        TransferError {
                                            message: format!(
                                                "parallel channel receive error: {e}"
                                            ),
                                        },
                                    )
                                    .unwrap();
                                break;
                            }
                        },
                    };

                    let mut entry = match transfers.get_mut(&chunk.transfer_id) {
                        Some(entry) => entry,
                        None => {
                            tracing::warn!(
                                "received chunk for unknown transfer {:?}",
                                chunk.transfer_id,
                            );
                            continue;
                        }
                    };

                    let mut write_offset = chunk.offset;
                    let fragments = chunk.data.into_inner();
                    let write_err = fragments.iter().find_map(|fragment| {
                        let result = entry.local_memory.write_at(write_offset, fragment);
                        write_offset += fragment.len();
                        result.err()
                    });
                    if let Some(e) = write_err {
                        let transfer_id = chunk.transfer_id;
                        drop(entry);
                        let (_, state) = transfers.remove(&transfer_id).unwrap();
                        result_port
                            .send(
                                &instance,
                                SendTransferResult {
                                    done: state.done,
                                    result: Err(e.to_string()),
                                },
                            )
                            .unwrap();
                        continue;
                    }

                    entry.chunks_received += 1;
                    if entry.chunks_received == entry.total_chunks {
                        let transfer_id = chunk.transfer_id;
                        drop(entry);
                        let (_, state) = transfers.remove(&transfer_id).unwrap();
                        result_port
                            .send(
                                &instance,
                                SendTransferResult {
                                    done: state.done,
                                    result: Ok(()),
                                },
                            )
                            .unwrap();
                    }
                }
                rx.join().await;
                done_tx.send(()).unwrap();
            });
        }

        Ok(())
    }

    async fn cleanup(
        &mut self,
        _this: &Instance<Self>,
        _err: Option<&ActorError>,
    ) -> Result<(), anyhow::Error> {
        self.cancel.cancel();
        if let Some(done_rx) = self.receiver_done.take() {
            done_rx.await?;
        }
        Ok(())
    }
}

#[async_trait]
#[hyperactor::handle(TcpManagerMessage)]
impl TcpManagerMessageHandler for TcpManagerActor {
    async fn write_chunk(
        &mut self,
        cx: &Context<Self>,
        buf_id: usize,
        offset: usize,
        data: Part,
    ) -> Result<Result<(), String>, anyhow::Error> {
        let owner = self.owner.get().expect("TcpManagerActor owner not set");
        let mem = match owner.request_local_memory(cx, buf_id).await {
            Ok(Some(mem)) => mem,
            Ok(None) => return Ok(Err(format!("buffer {buf_id} not found"))),
            Err(e) => return Ok(Err(e.to_string())),
        };

        let bytes = data.into_bytes();
        if let Err(e) = mem.write_at(offset, &bytes) {
            return Ok(Err(e.to_string()));
        }

        Ok(Ok(()))
    }

    async fn read_chunk(
        &mut self,
        cx: &Context<Self>,
        buf_id: usize,
        offset: usize,
        size: usize,
    ) -> Result<Result<TcpChunk, String>, anyhow::Error> {
        let owner = self.owner.get().expect("TcpManagerActor owner not set");
        let mem = match owner.request_local_memory(cx, buf_id).await {
            Ok(Some(mem)) => mem,
            Ok(None) => return Ok(Err(format!("buffer {buf_id} not found"))),
            Err(e) => return Ok(Err(e.to_string())),
        };

        let mut buf = BytesMut::zeroed(size);
        if let Err(e) = mem.read_at(offset, &mut buf) {
            return Ok(Err(e.to_string()));
        }
        Ok(Ok(TcpChunk(Part::from(buf.freeze()))))
    }

    async fn get_channel_address(
        &mut self,
        _cx: &Context<Self>,
    ) -> Result<Option<ChannelAddr>, anyhow::Error> {
        Ok(self.channel_addr.clone())
    }

    async fn register_transfer_remote(
        &mut self,
        cx: &Context<Self>,
        buf_id: usize,
        total_chunks: usize,
        done: OncePortRef<Result<(), String>>,
    ) -> Result<Result<usize, String>, anyhow::Error> {
        let owner = self.owner.get().expect("TcpManagerActor owner not set");
        let mem = match owner.request_local_memory(cx, buf_id).await {
            Ok(Some(mem)) => mem,
            Ok(None) => return Ok(Err(format!("buffer {buf_id} not found"))),
            Err(e) => return Ok(Err(e.to_string())),
        };
        let transfer_id = self.register_transfer(mem, total_chunks, done);
        Ok(Ok(transfer_id))
    }

    async fn execute_transfer_remote(
        &mut self,
        cx: &Context<Self>,
        transfer_id: usize,
        buf_id: usize,
        chunk_size: usize,
        dest_addr: ChannelAddr,
    ) -> Result<Result<(), String>, anyhow::Error> {
        let owner = self.owner.get().expect("TcpManagerActor owner not set");
        let mem = match owner.request_local_memory(cx, buf_id).await {
            Ok(Some(mem)) => mem,
            Ok(None) => return Ok(Err(format!("buffer {buf_id} not found"))),
            Err(e) => return Ok(Err(e.to_string())),
        };
        self.execute_transfer(cx, transfer_id, mem, chunk_size, dest_addr)?;
        Ok(Ok(()))
    }
}

#[async_trait]
impl Handler<RegisterTransferLocal> for TcpManagerActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: RegisterTransferLocal,
    ) -> Result<(), anyhow::Error> {
        let transfer_id =
            self.register_transfer(message.local_memory, message.total_chunks, message.done);
        message.reply.send(cx, transfer_id)?;
        Ok(())
    }
}

#[async_trait]
impl Handler<ExecuteTransferLocal> for TcpManagerActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: ExecuteTransferLocal,
    ) -> Result<(), anyhow::Error> {
        self.execute_transfer(
            cx,
            message.transfer_id,
            message.local_memory,
            message.chunk_size,
            message.dest_addr,
        )
    }
}

#[async_trait]
impl Handler<SendTransferResult> for TcpManagerActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: SendTransferResult,
    ) -> Result<(), anyhow::Error> {
        Ok(message.done.send(cx, message.result)?)
    }
}

#[async_trait]
impl Handler<TransferError> for TcpManagerActor {
    async fn handle(
        &mut self,
        _cx: &Context<Self>,
        message: TransferError,
    ) -> Result<(), anyhow::Error> {
        tracing::error!("fatal transfer error: {}", message.message);
        Err(anyhow::anyhow!(message.message))
    }
}

/// Wrapper around [`ActorHandle<TcpManagerActor>`] that moves the TCP
/// data-plane (chunked reads/writes) off the actor loop while keeping
/// buffer resolution serialized through actor messages.
///
/// Because submit logic now runs outside the actor loop, same-process
/// messages no longer deadlock — the actor loop is free to handle
/// `WriteChunk`/`ReadChunk` messages.
#[derive(Debug, Clone)]
pub struct TcpBackend(pub ActorHandle<TcpManagerActor>);

impl std::ops::Deref for TcpBackend {
    type Target = ActorHandle<TcpManagerActor>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TcpBackend {
    /// Execute a parallel write: register the transfer on the remote
    /// side, then execute locally to push chunks over direct channels.
    async fn execute_parallel_write(
        &self,
        cx: &(impl context::Actor + Send + Sync),
        op: &TcpOp,
        chunk_size: usize,
        deadline: Instant,
    ) -> Result<()> {
        let size = op.local_memory.size();
        let total_chunks = size.div_ceil(chunk_size);

        let (done_handle, done_rx) = hyperactor::mailbox::open_once_port::<Result<(), String>>(cx);
        let done_ref = done_handle.bind();

        let remaining = deadline.saturating_duration_since(Instant::now());
        let transfer_id = tokio_timeout(
            remaining,
            op.remote_tcp_manager.register_transfer_remote(
                cx,
                op.remote_buf_id,
                total_chunks,
                done_ref,
            ),
        )
        .await
        .map_err(|_| anyhow::anyhow!("register_transfer_remote timed out"))??
        .map_err(|e| anyhow::anyhow!(e))?;

        let dest_addr = tokio_timeout(
            deadline.saturating_duration_since(Instant::now()),
            op.remote_tcp_manager.get_channel_address(cx),
        )
        .await
        .map_err(|_| anyhow::anyhow!("get_channel_address timed out"))??
        .ok_or_else(|| anyhow::anyhow!("remote does not have parallel channels enabled"))?;

        self.0.send(
            cx,
            ExecuteTransferLocal {
                transfer_id,
                local_memory: op.local_memory.clone(),
                chunk_size,
                dest_addr,
            },
        )?;

        let remaining = deadline.saturating_duration_since(Instant::now());
        let result = tokio_timeout(remaining, done_rx.recv())
            .await
            .map_err(|_| anyhow::anyhow!("parallel write timed out"))?
            .map_err(|e| anyhow::anyhow!(e))?;
        result.map_err(|e| anyhow::anyhow!(e))
    }

    /// Execute a parallel read: register the transfer locally, then
    /// ask the remote side to push chunks to our channel.
    async fn execute_parallel_read(
        &self,
        cx: &(impl context::Actor + Send + Sync),
        op: &TcpOp,
        chunk_size: usize,
        deadline: Instant,
    ) -> Result<()> {
        let size = op.local_memory.size();
        let total_chunks = size.div_ceil(chunk_size);

        let (done_handle, done_rx) = hyperactor::mailbox::open_once_port::<Result<(), String>>(cx);
        let done_ref = done_handle.bind();

        let (id_handle, id_rx) = hyperactor::mailbox::open_once_port::<usize>(cx);

        self.0.send(
            cx,
            RegisterTransferLocal {
                local_memory: op.local_memory.clone(),
                total_chunks,
                done: done_ref,
                reply: id_handle,
            },
        )?;

        let transfer_id = id_rx
            .recv()
            .await
            .map_err(|e| anyhow::anyhow!("failed to receive transfer id: {e}"))?;

        let my_channel_addr = self
            .0
            .get_channel_address(cx)
            .await?
            .ok_or_else(|| anyhow::anyhow!("local parallel channels not enabled"))?;

        let remaining = deadline.saturating_duration_since(Instant::now());
        tokio_timeout(
            remaining,
            op.remote_tcp_manager.execute_transfer_remote(
                cx,
                transfer_id,
                op.remote_buf_id,
                chunk_size,
                my_channel_addr,
            ),
        )
        .await
        .map_err(|_| anyhow::anyhow!("execute_transfer_remote timed out"))??
        .map_err(|e| anyhow::anyhow!(e))?;

        let remaining = deadline.saturating_duration_since(Instant::now());
        let result = tokio_timeout(remaining, done_rx.recv())
            .await
            .map_err(|_| anyhow::anyhow!("parallel read timed out"))?
            .map_err(|e| anyhow::anyhow!(e))?;
        result.map_err(|e| anyhow::anyhow!(e))
    }

    /// Execute a write operation: read local memory in chunks and write
    /// them into the remote buffer via actor messages.
    async fn execute_write(
        &self,
        cx: &(impl context::Actor + Send + Sync),
        op: &TcpOp,
        chunk_size: usize,
        deadline: Instant,
    ) -> Result<()> {
        let size = op.local_memory.size();
        let mut offset = 0;

        while offset < size {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                anyhow::bail!("tcp write timed out");
            }

            let len = std::cmp::min(chunk_size, size - offset);

            let mut buf = vec![0u8; len];
            op.local_memory.read_at(offset, &mut buf)?;
            let data = Part::from(Bytes::from(buf));

            tokio_timeout(
                remaining,
                op.remote_tcp_manager
                    .write_chunk(cx, op.remote_buf_id, offset, data),
            )
            .await
            .map_err(|_| anyhow::anyhow!("tcp write chunk timed out"))??
            .map_err(|e| anyhow::anyhow!(e))?;

            offset += len;
        }

        Ok(())
    }

    /// Execute a read operation: request chunks from the remote buffer
    /// and write them into local memory via actor messages.
    async fn execute_read(
        &self,
        cx: &(impl context::Actor + Send + Sync),
        op: &TcpOp,
        chunk_size: usize,
        deadline: Instant,
    ) -> Result<()> {
        let size = op.local_memory.size();
        let mut offset = 0;

        while offset < size {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                anyhow::bail!("tcp read timed out");
            }

            let len = std::cmp::min(chunk_size, size - offset);

            let chunk = tokio_timeout(
                remaining,
                op.remote_tcp_manager
                    .read_chunk(cx, op.remote_buf_id, offset, len),
            )
            .await
            .map_err(|_| anyhow::anyhow!("tcp read chunk timed out"))??
            .map_err(|e| anyhow::anyhow!(e))?;
            let data = chunk.0.into_bytes();

            anyhow::ensure!(
                data.len() == len,
                "tcp read chunk size mismatch: expected {len}, got {}",
                data.len()
            );

            op.local_memory.write_at(offset, &data)?;

            offset += len;
        }

        Ok(())
    }
}

#[async_trait]
impl RdmaBackend for TcpBackend {
    type TransportInfo = ();

    /// Submit a batch of RDMA operations over TCP.
    ///
    /// Each operation's remote buffer is resolved to its TCP backend
    /// context, then executed directly — sending chunked write/read
    /// messages to the remote [`TcpManagerActor`].
    async fn submit(
        &mut self,
        cx: &(impl context::Actor + Send + Sync),
        ops: Vec<RdmaOp>,
        timeout: Duration,
    ) -> Result<()> {
        let chunk_size =
            hyperactor_config::global::get(crate::config::RDMA_MAX_CHUNK_SIZE_MB) * 1024 * 1024;
        let parallelism =
            hyperactor_config::global::get(crate::config::RDMA_TCP_FALLBACK_PARALLELISM);
        let deadline = Instant::now() + timeout;

        for op in ops {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                anyhow::bail!("tcp submit timed out");
            }

            let (remote_tcp_mgr, remote_buf_id) = op.remote.resolve_tcp()?;
            let tcp_op = TcpOp {
                op_type: op.op_type.clone(),
                local_memory: op.local,
                remote_tcp_manager: remote_tcp_mgr,
                remote_buf_id,
            };

            if parallelism > 1 {
                match tcp_op.op_type {
                    RdmaOpType::WriteFromLocal => {
                        self.execute_parallel_write(cx, &tcp_op, chunk_size, deadline)
                            .await?;
                    }
                    RdmaOpType::ReadIntoLocal => {
                        self.execute_parallel_read(cx, &tcp_op, chunk_size, deadline)
                            .await?;
                    }
                }
            } else {
                match tcp_op.op_type {
                    RdmaOpType::WriteFromLocal => {
                        self.execute_write(cx, &tcp_op, chunk_size, deadline)
                            .await?;
                    }
                    RdmaOpType::ReadIntoLocal => {
                        self.execute_read(cx, &tcp_op, chunk_size, deadline).await?;
                    }
                }
            }
        }

        Ok(())
    }

    fn transport_level(&self) -> RdmaTransportLevel {
        RdmaTransportLevel::Tcp
    }

    fn transport_info(&self) -> Option<Self::TransportInfo> {
        None
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use hyperactor::ActorHandle;
    use hyperactor::Proc;
    use hyperactor::RemoteSpawn;
    use hyperactor::channel::ChannelAddr;
    use hyperactor_config::Flattrs;

    use super::TcpBackend;
    use super::TcpManagerActor;
    use crate::RdmaManagerMessageClient;
    use crate::RdmaOp;
    use crate::RdmaOpType;
    use crate::backend::RdmaBackend;
    use crate::local_memory::RdmaLocalMemory;
    use crate::local_memory::UnsafeLocalMemory;
    use crate::rdma_manager_actor::GetTcpActorRefClient;
    use crate::rdma_manager_actor::RdmaManagerActor;

    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    struct TcpTestProcEnv {
        proc: Proc,
        rdma_handle: ActorHandle<RdmaManagerActor>,
        instance: hyperactor::Instance<()>,
        tcp_backend: TcpBackend,
        rdma_remote_buf: crate::RdmaRemoteBuffer,
        local_memory: Arc<dyn RdmaLocalMemory>,
        cpu_buf: Box<[u8]>,
    }

    impl TcpTestProcEnv {
        /// Create a standalone test environment with its own proc and rdma manager.
        async fn new(buffer_size: usize) -> anyhow::Result<Self> {
            let id = COUNTER.fetch_add(1, Ordering::Relaxed);
            let proc = Proc::direct(
                ChannelAddr::any(hyperactor::channel::ChannelTransport::Unix),
                format!("tcp_test_{id}"),
            )?;
            let (instance, _) = proc.instance("client")?;

            let rdma_actor = RdmaManagerActor::new(None, Flattrs::default()).await?;
            let rdma_handle = proc.spawn("rdma_manager", rdma_actor)?;

            let tcp_ref = rdma_handle.get_tcp_actor_ref(&instance).await?;
            let tcp_backend = TcpBackend(
                tcp_ref
                    .downcast_handle(&instance)
                    .ok_or_else(|| anyhow::anyhow!("tcp actor not local"))?,
            );

            let (local_memory, cpu_buf, rdma_remote_buf) =
                Self::alloc_buffer(&instance, &rdma_handle, buffer_size).await?;

            Ok(Self {
                proc,
                rdma_handle,
                instance,
                tcp_backend,
                rdma_remote_buf,
                local_memory,
                cpu_buf,
            })
        }

        /// Create a buffer on an existing proc's rdma manager.
        async fn on_proc(
            proc: &Proc,
            rdma_handle: &ActorHandle<RdmaManagerActor>,
            tcp_backend: TcpBackend,
            buffer_size: usize,
        ) -> anyhow::Result<Self> {
            let id = COUNTER.fetch_add(1, Ordering::Relaxed);
            let (instance, _) = proc.instance(&format!("client_{id}"))?;

            let (local_memory, cpu_buf, rdma_remote_buf) =
                Self::alloc_buffer(&instance, rdma_handle, buffer_size).await?;

            Ok(Self {
                proc: proc.clone(),
                rdma_handle: rdma_handle.clone(),
                instance,
                tcp_backend,
                rdma_remote_buf,
                local_memory,
                cpu_buf,
            })
        }

        async fn alloc_buffer(
            instance: &hyperactor::Instance<()>,
            rdma_handle: &ActorHandle<RdmaManagerActor>,
            buffer_size: usize,
        ) -> anyhow::Result<(Arc<dyn RdmaLocalMemory>, Box<[u8]>, crate::RdmaRemoteBuffer)>
        {
            let mut cpu_buf = vec![0u8; buffer_size].into_boxed_slice();
            let ptr = cpu_buf.as_mut_ptr() as usize;
            let local_memory: Arc<dyn RdmaLocalMemory> =
                Arc::new(UnsafeLocalMemory::new(ptr, buffer_size));
            let rdma_remote_buf = rdma_handle
                .request_buffer(instance, local_memory.clone())
                .await?;
            Ok((local_memory, cpu_buf, rdma_remote_buf))
        }
    }

    /// Two separate procs, one buffer each.
    async fn setup_tcp_env(buf_size: usize) -> anyhow::Result<Vec<TcpTestProcEnv>> {
        Ok(vec![
            TcpTestProcEnv::new(buf_size).await?,
            TcpTestProcEnv::new(buf_size).await?,
        ])
    }

    /// Single proc, two buffers.
    async fn setup_same_proc_tcp_env(buf_size: usize) -> anyhow::Result<Vec<TcpTestProcEnv>> {
        let first = TcpTestProcEnv::new(buf_size).await?;
        let second = TcpTestProcEnv::on_proc(
            &first.proc,
            &first.rdma_handle,
            first.tcp_backend.clone(),
            buf_size,
        )
        .await?;
        Ok(vec![first, second])
    }

    /// Two procs, two buffers each (4 total). For concurrent tests that
    /// need independent source/dest pairs.
    async fn setup_tcp_env_pairs(buf_size: usize) -> anyhow::Result<Vec<TcpTestProcEnv>> {
        let e0 = TcpTestProcEnv::new(buf_size).await?;
        let e1 = TcpTestProcEnv::new(buf_size).await?;
        let e2 =
            TcpTestProcEnv::on_proc(&e0.proc, &e0.rdma_handle, e0.tcp_backend.clone(), buf_size)
                .await?;
        let e3 =
            TcpTestProcEnv::on_proc(&e1.proc, &e1.rdma_handle, e1.tcp_backend.clone(), buf_size)
                .await?;
        Ok(vec![e0, e1, e2, e3])
    }

    // --- Non-parallel two-proc tests ---

    /// Write from local buffer 0 into remote buffer 1.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_write_from_local() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let buf_size = 4096;
        let mut envs = setup_tcp_env(buf_size).await?;

        for (i, byte) in envs[0].cpu_buf.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }

        let remote = envs[1].rdma_remote_buf.clone();
        let env = &mut envs[0];
        env.tcp_backend
            .submit(
                &env.instance,
                vec![RdmaOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local: env.local_memory.clone(),
                    remote: remote.clone(),
                }],
                Duration::from_secs(30),
            )
            .await?;

        for (i, byte) in envs[1].cpu_buf.iter().enumerate() {
            assert_eq!(*byte, (i % 256) as u8, "mismatch at offset {i} after write");
        }

        Ok(())
    }

    /// Read from remote buffer 1 into local buffer 0.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_read_into_local() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let buf_size = 2048;
        let mut envs = setup_tcp_env(buf_size).await?;

        for (i, byte) in envs[1].cpu_buf.iter_mut().enumerate() {
            *byte = ((i * 7 + 3) % 256) as u8;
        }

        let remote = envs[1].rdma_remote_buf.clone();
        let env = &mut envs[0];
        env.tcp_backend
            .submit(
                &env.instance,
                vec![RdmaOp {
                    op_type: RdmaOpType::ReadIntoLocal,
                    local: env.local_memory.clone(),
                    remote: remote.clone(),
                }],
                Duration::from_secs(30),
            )
            .await?;

        for (i, byte) in envs[0].cpu_buf.iter().enumerate() {
            assert_eq!(
                *byte,
                ((i * 7 + 3) % 256) as u8,
                "mismatch at offset {i} after read"
            );
        }

        Ok(())
    }

    /// Write, clear, read-back, verify round-trip.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_write_then_read_back() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let buf_size = 4096;
        let mut envs = setup_tcp_env(buf_size).await?;

        for (i, byte) in envs[0].cpu_buf.iter_mut().enumerate() {
            *byte = ((i * 13 + 5) % 256) as u8;
        }

        let remote = envs[1].rdma_remote_buf.clone();
        let env = &mut envs[0];
        env.tcp_backend
            .submit(
                &env.instance,
                vec![RdmaOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local: env.local_memory.clone(),
                    remote: remote.clone(),
                }],
                Duration::from_secs(30),
            )
            .await?;

        for byte in envs[0].cpu_buf.iter_mut() {
            *byte = 0;
        }

        let env = &mut envs[0];
        env.tcp_backend
            .submit(
                &env.instance,
                vec![RdmaOp {
                    op_type: RdmaOpType::ReadIntoLocal,
                    local: env.local_memory.clone(),
                    remote: remote.clone(),
                }],
                Duration::from_secs(30),
            )
            .await?;

        for (i, byte) in envs[0].cpu_buf.iter().enumerate() {
            assert_eq!(
                *byte,
                ((i * 13 + 5) % 256) as u8,
                "mismatch at offset {i} after read-back"
            );
        }

        Ok(())
    }

    /// Multi-chunk write (1 MiB chunks, 1.5 MiB buffer).
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_multi_chunk_write() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);
        let _chunk_guard = config.override_key(crate::config::RDMA_MAX_CHUNK_SIZE_MB, 1);

        let buf_size = 3 * 1024 * 512;
        let mut envs = setup_tcp_env(buf_size).await?;

        for (i, byte) in envs[0].cpu_buf.iter_mut().enumerate() {
            *byte = (i % 251) as u8;
        }

        let remote = envs[1].rdma_remote_buf.clone();
        let env = &mut envs[0];
        env.tcp_backend
            .submit(
                &env.instance,
                vec![RdmaOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local: env.local_memory.clone(),
                    remote: remote.clone(),
                }],
                Duration::from_secs(30),
            )
            .await?;

        for (i, byte) in envs[1].cpu_buf.iter().enumerate() {
            assert_eq!(*byte, (i % 251) as u8, "mismatch at offset {i}");
        }

        Ok(())
    }

    /// Multi-chunk read.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_multi_chunk_read() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);
        let _chunk_guard = config.override_key(crate::config::RDMA_MAX_CHUNK_SIZE_MB, 1);

        let buf_size = 3 * 1024 * 512;
        let mut envs = setup_tcp_env(buf_size).await?;

        for (i, byte) in envs[1].cpu_buf.iter_mut().enumerate() {
            *byte = ((i * 3 + 17) % 256) as u8;
        }

        let remote = envs[1].rdma_remote_buf.clone();
        let env = &mut envs[0];
        env.tcp_backend
            .submit(
                &env.instance,
                vec![RdmaOp {
                    op_type: RdmaOpType::ReadIntoLocal,
                    local: env.local_memory.clone(),
                    remote: remote.clone(),
                }],
                Duration::from_secs(30),
            )
            .await?;

        for (i, byte) in envs[0].cpu_buf.iter().enumerate() {
            assert_eq!(*byte, ((i * 3 + 17) % 256) as u8, "mismatch at offset {i}");
        }

        Ok(())
    }

    /// Multi-chunk write-then-read round-trip.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_multi_chunk_round_trip() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);
        let _chunk_guard = config.override_key(crate::config::RDMA_MAX_CHUNK_SIZE_MB, 1);

        let buf_size = 5 * 1024 * 512;
        let mut envs = setup_tcp_env(buf_size).await?;

        for (i, byte) in envs[0].cpu_buf.iter_mut().enumerate() {
            *byte = ((i * 41 + 7) % 256) as u8;
        }

        let remote = envs[1].rdma_remote_buf.clone();
        let env = &mut envs[0];
        env.tcp_backend
            .submit(
                &env.instance,
                vec![RdmaOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local: env.local_memory.clone(),
                    remote: remote.clone(),
                }],
                Duration::from_secs(30),
            )
            .await?;

        for byte in envs[0].cpu_buf.iter_mut() {
            *byte = 0;
        }

        let env = &mut envs[0];
        env.tcp_backend
            .submit(
                &env.instance,
                vec![RdmaOp {
                    op_type: RdmaOpType::ReadIntoLocal,
                    local: env.local_memory.clone(),
                    remote: remote.clone(),
                }],
                Duration::from_secs(30),
            )
            .await?;

        for (i, byte) in envs[0].cpu_buf.iter().enumerate() {
            assert_eq!(
                *byte,
                ((i * 41 + 7) % 256) as u8,
                "mismatch at offset {i} after multi-chunk round-trip"
            );
        }

        Ok(())
    }

    /// resolve_tcp finds the Tcp backend context in a buffer.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_resolve_tcp() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let envs = setup_tcp_env(64).await?;

        for (i, env) in envs.iter().enumerate() {
            let (tcp_ref, id) = env.rdma_remote_buf.resolve_tcp()?;
            assert_eq!(id, env.rdma_remote_buf.id, "buf id mismatch for env {i}");
            let expected: hyperactor::ActorRef<TcpManagerActor> = env.tcp_backend.bind();
            assert_eq!(tcp_ref.actor_id(), expected.actor_id());
        }

        Ok(())
    }

    /// Write to a released buffer returns an error without crashing.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_write_to_released_buffer() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let buf_size = 64;
        let mut envs = setup_tcp_env(buf_size).await?;

        for (i, byte) in envs[0].cpu_buf.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }

        // Normal write should succeed.
        let remote = envs[1].rdma_remote_buf.clone();
        let env = &mut envs[0];
        env.tcp_backend
            .submit(
                &env.instance,
                vec![RdmaOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local: env.local_memory.clone(),
                    remote: remote.clone(),
                }],
                Duration::from_secs(10),
            )
            .await?;

        // Release the remote buffer.
        use crate::rdma_manager_actor::ReleaseBufferClient;
        let owner_ref = envs[1].rdma_remote_buf.owner.clone();
        owner_ref
            .release_buffer(&envs[0].instance, envs[1].rdma_remote_buf.id)
            .await?;

        // Writing to the released buffer should fail.
        let env = &mut envs[0];
        let result = env
            .tcp_backend
            .submit(
                &env.instance,
                vec![RdmaOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local: env.local_memory.clone(),
                    remote: remote.clone(),
                }],
                Duration::from_secs(10),
            )
            .await;
        assert!(result.is_err(), "expected error writing to released buffer");

        Ok(())
    }

    /// Read from a released buffer returns an error without crashing.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_read_from_released_buffer() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let buf_size = 64;
        let mut envs = setup_tcp_env(buf_size).await?;

        // Release the remote buffer.
        use crate::rdma_manager_actor::ReleaseBufferClient;
        let owner_ref = envs[1].rdma_remote_buf.owner.clone();
        owner_ref
            .release_buffer(&envs[0].instance, envs[1].rdma_remote_buf.id)
            .await?;

        // Reading from the released buffer should fail.
        let remote = envs[1].rdma_remote_buf.clone();
        let env = &mut envs[0];
        let result = env
            .tcp_backend
            .submit(
                &env.instance,
                vec![RdmaOp {
                    op_type: RdmaOpType::ReadIntoLocal,
                    local: env.local_memory.clone(),
                    remote: remote.clone(),
                }],
                Duration::from_secs(10),
            )
            .await;
        assert!(
            result.is_err(),
            "expected error reading from released buffer"
        );

        Ok(())
    }

    // --- Non-parallel same-proc tests ---

    /// Same-process write.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_same_process_write() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let buf_size = 4096;
        let mut envs = setup_same_proc_tcp_env(buf_size).await?;

        for (i, byte) in envs[0].cpu_buf.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }

        let remote = envs[1].rdma_remote_buf.clone();
        let env = &mut envs[0];
        env.tcp_backend
            .submit(
                &env.instance,
                vec![RdmaOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local: env.local_memory.clone(),
                    remote: remote.clone(),
                }],
                Duration::from_secs(10),
            )
            .await?;

        for (i, byte) in envs[1].cpu_buf.iter().enumerate() {
            assert_eq!(*byte, (i % 256) as u8, "mismatch at offset {i}");
        }

        Ok(())
    }

    /// Same-process read.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_same_process_read() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let buf_size = 2048;
        let mut envs = setup_same_proc_tcp_env(buf_size).await?;

        for (i, byte) in envs[1].cpu_buf.iter_mut().enumerate() {
            *byte = ((i * 7 + 3) % 256) as u8;
        }

        let remote = envs[1].rdma_remote_buf.clone();
        let env = &mut envs[0];
        env.tcp_backend
            .submit(
                &env.instance,
                vec![RdmaOp {
                    op_type: RdmaOpType::ReadIntoLocal,
                    local: env.local_memory.clone(),
                    remote: remote.clone(),
                }],
                Duration::from_secs(10),
            )
            .await?;

        for (i, byte) in envs[0].cpu_buf.iter().enumerate() {
            assert_eq!(*byte, ((i * 7 + 3) % 256) as u8, "mismatch at offset {i}");
        }

        Ok(())
    }

    /// Same-process write-then-read round-trip.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_same_process_round_trip() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let buf_size = 4096;
        let mut envs = setup_same_proc_tcp_env(buf_size).await?;

        for (i, byte) in envs[0].cpu_buf.iter_mut().enumerate() {
            *byte = ((i * 13 + 5) % 256) as u8;
        }

        let remote = envs[1].rdma_remote_buf.clone();
        let env = &mut envs[0];
        env.tcp_backend
            .submit(
                &env.instance,
                vec![RdmaOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local: env.local_memory.clone(),
                    remote: remote.clone(),
                }],
                Duration::from_secs(10),
            )
            .await?;

        for byte in envs[0].cpu_buf.iter_mut() {
            *byte = 0;
        }

        let env = &mut envs[0];
        env.tcp_backend
            .submit(
                &env.instance,
                vec![RdmaOp {
                    op_type: RdmaOpType::ReadIntoLocal,
                    local: env.local_memory.clone(),
                    remote: remote.clone(),
                }],
                Duration::from_secs(10),
            )
            .await?;

        for (i, byte) in envs[0].cpu_buf.iter().enumerate() {
            assert_eq!(
                *byte,
                ((i * 13 + 5) % 256) as u8,
                "mismatch at offset {i} after round-trip"
            );
        }

        Ok(())
    }

    /// When TCP fallback is disabled and ibverbs is unavailable,
    /// RdmaManagerActor::new returns an error.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_fallback_disabled_fails() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, false);

        let result = RdmaManagerActor::new(None, Flattrs::default()).await;
        if crate::ibverbs_supported() {
            assert!(result.is_ok());
        } else {
            assert!(result.is_err());
        }

        Ok(())
    }

    /// Stopping the RdmaManagerActor with parallelism enabled cleanly
    /// shuts down the TcpManagerActor's receive loop without hanging.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_parallel_clean_shutdown() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);
        let _par_guard = config.override_key(crate::config::RDMA_TCP_FALLBACK_PARALLELISM, 2);
        let _chunk_guard = config.override_key(crate::config::RDMA_MAX_CHUNK_SIZE_MB, 1);

        let buf_size = 3 * 1024 * 1024;
        let mut envs = setup_tcp_env(buf_size).await?;

        // Do a transfer so the receive loop and outbound connections are live.
        for (i, byte) in envs[0].cpu_buf.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }
        let remote = envs[1].rdma_remote_buf.clone();
        let env = &mut envs[0];
        env.tcp_backend
            .submit(
                &env.instance,
                vec![RdmaOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local: env.local_memory.clone(),
                    remote: remote.clone(),
                }],
                Duration::from_secs(30),
            )
            .await?;

        // Stop the RdmaManagerActor, which cascades to TcpManagerActor.
        // The test timeout ensures we detect hangs in the cleanup path.
        envs[0].rdma_handle.drain_and_stop("test")?;
        envs[0].rdma_handle.clone().await;
        envs[1].rdma_handle.drain_and_stop("test")?;
        envs[1].rdma_handle.clone().await;

        Ok(())
    }

    // --- Parallel transfer tests ---

    /// Parallel write via direct channels.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_parallel_write() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);
        let _par_guard = config.override_key(crate::config::RDMA_TCP_FALLBACK_PARALLELISM, 2);
        let _chunk_guard = config.override_key(crate::config::RDMA_MAX_CHUNK_SIZE_MB, 1);

        // 3 MiB, 3 chunks spread across 2 workers.
        let buf_size = 3 * 1024 * 1024;
        let mut envs = setup_tcp_env(buf_size).await?;

        for (i, byte) in envs[0].cpu_buf.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }

        let remote = envs[1].rdma_remote_buf.clone();
        let env = &mut envs[0];
        env.tcp_backend
            .submit(
                &env.instance,
                vec![RdmaOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local: env.local_memory.clone(),
                    remote: remote.clone(),
                }],
                Duration::from_secs(30),
            )
            .await?;

        for (i, byte) in envs[1].cpu_buf.iter().enumerate() {
            assert_eq!(
                *byte,
                (i % 256) as u8,
                "mismatch at offset {i} after parallel write"
            );
        }

        Ok(())
    }

    /// Parallel read via direct channels.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_parallel_read() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);
        let _par_guard = config.override_key(crate::config::RDMA_TCP_FALLBACK_PARALLELISM, 2);
        let _chunk_guard = config.override_key(crate::config::RDMA_MAX_CHUNK_SIZE_MB, 1);

        let buf_size = 3 * 1024 * 1024;
        let mut envs = setup_tcp_env(buf_size).await?;

        for (i, byte) in envs[1].cpu_buf.iter_mut().enumerate() {
            *byte = ((i * 7 + 3) % 256) as u8;
        }

        let remote = envs[1].rdma_remote_buf.clone();
        let env = &mut envs[0];
        env.tcp_backend
            .submit(
                &env.instance,
                vec![RdmaOp {
                    op_type: RdmaOpType::ReadIntoLocal,
                    local: env.local_memory.clone(),
                    remote: remote.clone(),
                }],
                Duration::from_secs(30),
            )
            .await?;

        for (i, byte) in envs[0].cpu_buf.iter().enumerate() {
            assert_eq!(
                *byte,
                ((i * 7 + 3) % 256) as u8,
                "mismatch at offset {i} after parallel read"
            );
        }

        Ok(())
    }

    /// Parallel write-then-read round-trip.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_parallel_round_trip() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);
        let _par_guard = config.override_key(crate::config::RDMA_TCP_FALLBACK_PARALLELISM, 2);
        let _chunk_guard = config.override_key(crate::config::RDMA_MAX_CHUNK_SIZE_MB, 1);

        let buf_size = 3 * 1024 * 1024;
        let mut envs = setup_tcp_env(buf_size).await?;

        for (i, byte) in envs[0].cpu_buf.iter_mut().enumerate() {
            *byte = ((i * 13 + 5) % 256) as u8;
        }

        let remote = envs[1].rdma_remote_buf.clone();
        let env = &mut envs[0];
        env.tcp_backend
            .submit(
                &env.instance,
                vec![RdmaOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local: env.local_memory.clone(),
                    remote: remote.clone(),
                }],
                Duration::from_secs(30),
            )
            .await?;

        for byte in envs[0].cpu_buf.iter_mut() {
            *byte = 0;
        }

        let env = &mut envs[0];
        env.tcp_backend
            .submit(
                &env.instance,
                vec![RdmaOp {
                    op_type: RdmaOpType::ReadIntoLocal,
                    local: env.local_memory.clone(),
                    remote: remote.clone(),
                }],
                Duration::from_secs(30),
            )
            .await?;

        for (i, byte) in envs[0].cpu_buf.iter().enumerate() {
            assert_eq!(
                *byte,
                ((i * 13 + 5) % 256) as u8,
                "mismatch at offset {i} after parallel round-trip"
            );
        }

        Ok(())
    }

    /// Same-process parallel write.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_parallel_same_process_write() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);
        let _par_guard = config.override_key(crate::config::RDMA_TCP_FALLBACK_PARALLELISM, 2);
        let _chunk_guard = config.override_key(crate::config::RDMA_MAX_CHUNK_SIZE_MB, 1);

        let buf_size = 3 * 1024 * 1024;
        let mut envs = setup_same_proc_tcp_env(buf_size).await?;

        for (i, byte) in envs[0].cpu_buf.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }

        let remote = envs[1].rdma_remote_buf.clone();
        let env = &mut envs[0];
        env.tcp_backend
            .submit(
                &env.instance,
                vec![RdmaOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local: env.local_memory.clone(),
                    remote: remote.clone(),
                }],
                Duration::from_secs(10),
            )
            .await?;

        for (i, byte) in envs[1].cpu_buf.iter().enumerate() {
            assert_eq!(*byte, (i % 256) as u8, "mismatch at offset {i}");
        }

        Ok(())
    }

    /// Same-process parallel read.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_parallel_same_process_read() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);
        let _par_guard = config.override_key(crate::config::RDMA_TCP_FALLBACK_PARALLELISM, 2);
        let _chunk_guard = config.override_key(crate::config::RDMA_MAX_CHUNK_SIZE_MB, 1);

        let buf_size = 3 * 1024 * 1024;
        let mut envs = setup_same_proc_tcp_env(buf_size).await?;

        for (i, byte) in envs[1].cpu_buf.iter_mut().enumerate() {
            *byte = ((i * 7 + 3) % 256) as u8;
        }

        let remote = envs[1].rdma_remote_buf.clone();
        let env = &mut envs[0];
        env.tcp_backend
            .submit(
                &env.instance,
                vec![RdmaOp {
                    op_type: RdmaOpType::ReadIntoLocal,
                    local: env.local_memory.clone(),
                    remote: remote.clone(),
                }],
                Duration::from_secs(10),
            )
            .await?;

        for (i, byte) in envs[0].cpu_buf.iter().enumerate() {
            assert_eq!(*byte, ((i * 7 + 3) % 256) as u8, "mismatch at offset {i}");
        }

        Ok(())
    }

    // --- Concurrent parallel tests (4 envs, 2 independent pairs) ---

    /// Two concurrent parallel writes to independent buffer pairs.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_parallel_concurrent_writes() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);
        let _par_guard = config.override_key(crate::config::RDMA_TCP_FALLBACK_PARALLELISM, 2);
        let _chunk_guard = config.override_key(crate::config::RDMA_MAX_CHUNK_SIZE_MB, 1);

        let buf_size = 3 * 1024 * 1024;
        let mut envs = setup_tcp_env_pairs(buf_size).await?;

        // Fill source buffers with distinct patterns.
        for (i, byte) in envs[0].cpu_buf.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }
        for (i, byte) in envs[2].cpu_buf.iter_mut().enumerate() {
            *byte = ((i * 3 + 7) % 256) as u8;
        }

        // Pair 1: envs[0] -> envs[1], Pair 2: envs[2] -> envs[3].
        let remote_1 = envs[1].rdma_remote_buf.clone();
        let remote_3 = envs[3].rdma_remote_buf.clone();
        let mut h0 = envs[0].tcp_backend.clone();
        let mut h2 = envs[2].tcp_backend.clone();
        let inst_0 = &envs[0].instance;
        let inst_2 = &envs[2].instance;
        let mem_0 = envs[0].local_memory.clone();
        let mem_2 = envs[2].local_memory.clone();
        let (r1, r2) = tokio::join!(
            h0.submit(
                inst_0,
                vec![RdmaOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local: mem_0,
                    remote: remote_1,
                }],
                Duration::from_secs(30),
            ),
            h2.submit(
                inst_2,
                vec![RdmaOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local: mem_2,
                    remote: remote_3,
                }],
                Duration::from_secs(30),
            ),
        );
        r1?;
        r2?;

        for (i, byte) in envs[1].cpu_buf.iter().enumerate() {
            assert_eq!(*byte, (i % 256) as u8, "pair 1 mismatch at offset {i}");
        }
        for (i, byte) in envs[3].cpu_buf.iter().enumerate() {
            assert_eq!(
                *byte,
                ((i * 3 + 7) % 256) as u8,
                "pair 2 mismatch at offset {i}"
            );
        }

        Ok(())
    }

    /// Two concurrent parallel reads from independent buffer pairs.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_parallel_concurrent_reads() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);
        let _par_guard = config.override_key(crate::config::RDMA_TCP_FALLBACK_PARALLELISM, 2);
        let _chunk_guard = config.override_key(crate::config::RDMA_MAX_CHUNK_SIZE_MB, 1);

        let buf_size = 3 * 1024 * 1024;
        let mut envs = setup_tcp_env_pairs(buf_size).await?;

        // Fill remote buffers with distinct patterns.
        for (i, byte) in envs[1].cpu_buf.iter_mut().enumerate() {
            *byte = ((i * 11 + 3) % 256) as u8;
        }
        for (i, byte) in envs[3].cpu_buf.iter_mut().enumerate() {
            *byte = ((i * 5 + 13) % 256) as u8;
        }

        // Pair 1: envs[0] <- envs[1], Pair 2: envs[2] <- envs[3].
        let remote_1 = envs[1].rdma_remote_buf.clone();
        let remote_3 = envs[3].rdma_remote_buf.clone();
        let mut h0 = envs[0].tcp_backend.clone();
        let mut h2 = envs[2].tcp_backend.clone();
        let inst_0 = &envs[0].instance;
        let inst_2 = &envs[2].instance;
        let mem_0 = envs[0].local_memory.clone();
        let mem_2 = envs[2].local_memory.clone();
        let (r1, r2) = tokio::join!(
            h0.submit(
                inst_0,
                vec![RdmaOp {
                    op_type: RdmaOpType::ReadIntoLocal,
                    local: mem_0,
                    remote: remote_1,
                }],
                Duration::from_secs(30),
            ),
            h2.submit(
                inst_2,
                vec![RdmaOp {
                    op_type: RdmaOpType::ReadIntoLocal,
                    local: mem_2,
                    remote: remote_3,
                }],
                Duration::from_secs(30),
            ),
        );
        r1?;
        r2?;

        for (i, byte) in envs[0].cpu_buf.iter().enumerate() {
            assert_eq!(
                *byte,
                ((i * 11 + 3) % 256) as u8,
                "pair 1 mismatch at offset {i}"
            );
        }
        for (i, byte) in envs[2].cpu_buf.iter().enumerate() {
            assert_eq!(
                *byte,
                ((i * 5 + 13) % 256) as u8,
                "pair 2 mismatch at offset {i}"
            );
        }

        Ok(())
    }

    /// Concurrent parallel write and read on independent buffer pairs.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_parallel_concurrent_write_and_read() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);
        let _par_guard = config.override_key(crate::config::RDMA_TCP_FALLBACK_PARALLELISM, 2);
        let _chunk_guard = config.override_key(crate::config::RDMA_MAX_CHUNK_SIZE_MB, 1);

        let buf_size = 3 * 1024 * 1024;
        let mut envs = setup_tcp_env_pairs(buf_size).await?;

        // Fill source buffers.
        for (i, byte) in envs[0].cpu_buf.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }
        for (i, byte) in envs[3].cpu_buf.iter_mut().enumerate() {
            *byte = ((i * 7 + 13) % 256) as u8;
        }

        // Write envs[0] -> envs[1], read envs[2] <- envs[3] concurrently.
        let remote_1 = envs[1].rdma_remote_buf.clone();
        let remote_3 = envs[3].rdma_remote_buf.clone();
        let mut h0 = envs[0].tcp_backend.clone();
        let mut h2 = envs[2].tcp_backend.clone();
        let inst_0 = &envs[0].instance;
        let inst_2 = &envs[2].instance;
        let mem_0 = envs[0].local_memory.clone();
        let mem_2 = envs[2].local_memory.clone();
        let (write_result, read_result) = tokio::join!(
            h0.submit(
                inst_0,
                vec![RdmaOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local: mem_0,
                    remote: remote_1,
                }],
                Duration::from_secs(30),
            ),
            h2.submit(
                inst_2,
                vec![RdmaOp {
                    op_type: RdmaOpType::ReadIntoLocal,
                    local: mem_2,
                    remote: remote_3,
                }],
                Duration::from_secs(30),
            ),
        );
        write_result?;
        read_result?;

        for (i, byte) in envs[1].cpu_buf.iter().enumerate() {
            assert_eq!(*byte, (i % 256) as u8, "write mismatch at offset {i}");
        }
        for (i, byte) in envs[2].cpu_buf.iter().enumerate() {
            assert_eq!(
                *byte,
                ((i * 7 + 13) % 256) as u8,
                "read mismatch at offset {i}"
            );
        }

        Ok(())
    }
}
