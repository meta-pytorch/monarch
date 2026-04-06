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

use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use bytes::BytesMut;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::RefClient;
use hyperactor::context;
use hyperactor::reference::OncePortRef;
use serde::Deserialize;
use serde::Serialize;
use serde_multipart::Part;
use tokio::time::timeout as tokio_timeout;
use typeuri::Named;

use super::TcpOp;
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
pub struct TcpChunk(pub Part);
wirevalue::register_type!(TcpChunk);

/// Serializable messages for the [`TcpManagerActor`].
///
/// These travel over the wire between processes. The [`Part`] payload
/// is transferred via the multipart codec without an extra copy.
#[derive(Handler, HandleClient, RefClient, Debug, Serialize, Deserialize, Named)]
pub enum TcpManagerMessage {
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
}

impl TcpManagerActor {
    pub fn new() -> Self {
        Self {
            owner: OnceLock::new(),
        }
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
    use std::time::Duration;

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
    use crate::local_memory::Keepalive;
    use crate::local_memory::KeepaliveLocalMemory;
    use crate::local_memory::RdmaLocalMemory;
    use crate::rdma_manager_actor::GetTcpActorRefClient;
    use crate::rdma_manager_actor::RdmaManagerActor;

    impl Keepalive for Box<[u8]> {}

    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    struct TcpTestProcEnv {
        proc: Proc,
        rdma_handle: ActorHandle<RdmaManagerActor>,
        instance: hyperactor::Instance<()>,
        tcp_backend: TcpBackend,
        rdma_remote_buf: crate::RdmaRemoteBuffer,
        local_memory: Arc<dyn RdmaLocalMemory>,
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

            let (local_memory, rdma_remote_buf) =
                Self::alloc_cpu_buffer(&instance, &rdma_handle, buffer_size).await?;

            Ok(Self {
                proc,
                rdma_handle,
                instance,
                tcp_backend,
                rdma_remote_buf,
                local_memory,
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

            let (local_memory, rdma_remote_buf) =
                Self::alloc_cpu_buffer(&instance, rdma_handle, buffer_size).await?;

            Ok(Self {
                proc: proc.clone(),
                rdma_handle: rdma_handle.clone(),
                instance,
                tcp_backend,
                rdma_remote_buf,
                local_memory,
            })
        }

        async fn alloc_cpu_buffer(
            instance: &hyperactor::Instance<()>,
            rdma_handle: &ActorHandle<RdmaManagerActor>,
            buffer_size: usize,
        ) -> anyhow::Result<(Arc<dyn RdmaLocalMemory>, crate::RdmaRemoteBuffer)> {
            let cpu_buf = vec![0u8; buffer_size].into_boxed_slice();
            let ptr = cpu_buf.as_ptr() as usize;
            let local_memory: Arc<dyn RdmaLocalMemory> = Arc::new(KeepaliveLocalMemory::new(
                ptr,
                buffer_size,
                Arc::new(cpu_buf),
            ));
            let rdma_remote_buf = rdma_handle
                .request_buffer(instance, local_memory.clone())
                .await?;
            Ok((local_memory, rdma_remote_buf))
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

    // --- Shared test helpers ---

    /// Fill envs[0], write to envs[1], verify.
    async fn do_write_test(
        envs: &mut [TcpTestProcEnv],
        buf_size: usize,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        let mut src = vec![0u8; buf_size];
        for (i, byte) in src.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }
        envs[0].local_memory.write_at(0, &src)?;

        let remote = envs[1].rdma_remote_buf.clone();
        let env = &mut envs[0];
        env.tcp_backend
            .submit(
                &env.instance,
                vec![RdmaOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local: env.local_memory.clone(),
                    remote,
                }],
                timeout,
            )
            .await?;

        let mut dst = vec![0u8; buf_size];
        envs[1].local_memory.read_at(0, &mut dst)?;
        for (i, byte) in dst.iter().enumerate() {
            assert_eq!(*byte, (i % 256) as u8, "mismatch at offset {i} after write");
        }
        Ok(())
    }

    /// Fill envs[1], read into envs[0], verify.
    async fn do_read_test(
        envs: &mut [TcpTestProcEnv],
        buf_size: usize,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        let mut src = vec![0u8; buf_size];
        for (i, byte) in src.iter_mut().enumerate() {
            *byte = ((i * 7 + 3) % 256) as u8;
        }
        envs[1].local_memory.write_at(0, &src)?;

        let remote = envs[1].rdma_remote_buf.clone();
        let env = &mut envs[0];
        env.tcp_backend
            .submit(
                &env.instance,
                vec![RdmaOp {
                    op_type: RdmaOpType::ReadIntoLocal,
                    local: env.local_memory.clone(),
                    remote,
                }],
                timeout,
            )
            .await?;

        let mut dst = vec![0u8; buf_size];
        envs[0].local_memory.read_at(0, &mut dst)?;
        for (i, byte) in dst.iter().enumerate() {
            assert_eq!(
                *byte,
                ((i * 7 + 3) % 256) as u8,
                "mismatch at offset {i} after read"
            );
        }
        Ok(())
    }

    /// Write, clear, read-back, verify round-trip.
    async fn do_round_trip_test(
        envs: &mut [TcpTestProcEnv],
        buf_size: usize,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        let mut src = vec![0u8; buf_size];
        for (i, byte) in src.iter_mut().enumerate() {
            *byte = ((i * 13 + 5) % 256) as u8;
        }
        envs[0].local_memory.write_at(0, &src)?;

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
                timeout,
            )
            .await?;

        envs[0].local_memory.write_at(0, &vec![0u8; buf_size])?;

        let env = &mut envs[0];
        env.tcp_backend
            .submit(
                &env.instance,
                vec![RdmaOp {
                    op_type: RdmaOpType::ReadIntoLocal,
                    local: env.local_memory.clone(),
                    remote,
                }],
                timeout,
            )
            .await?;

        let mut dst = vec![0u8; buf_size];
        envs[0].local_memory.read_at(0, &mut dst)?;
        for (i, byte) in dst.iter().enumerate() {
            assert_eq!(
                *byte,
                ((i * 13 + 5) % 256) as u8,
                "mismatch at offset {i} after round-trip"
            );
        }
        Ok(())
    }

    // --- Non-parallel two-proc tests ---

    /// Write from local buffer 0 into remote buffer 1.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_write_from_local() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let mut envs = setup_tcp_env(4096).await?;
        do_write_test(&mut envs, 4096, Duration::from_secs(30)).await
    }

    /// Read from remote buffer 1 into local buffer 0.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_read_into_local() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let mut envs = setup_tcp_env(2048).await?;
        do_read_test(&mut envs, 2048, Duration::from_secs(30)).await
    }

    /// Write, clear, read-back, verify round-trip.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_write_then_read_back() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let mut envs = setup_tcp_env(4096).await?;
        do_round_trip_test(&mut envs, 4096, Duration::from_secs(30)).await
    }

    /// Multi-chunk write (1 MiB chunks, 1.5 MiB buffer).
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_multi_chunk_write() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);
        let _chunk_guard = config.override_key(crate::config::RDMA_MAX_CHUNK_SIZE_MB, 1);

        let buf_size = 3 * 1024 * 512;
        let mut envs = setup_tcp_env(buf_size).await?;

        let mut src = vec![0u8; buf_size];
        for (i, byte) in src.iter_mut().enumerate() {
            *byte = (i % 251) as u8;
        }
        envs[0].local_memory.write_at(0, &src)?;

        let remote = envs[1].rdma_remote_buf.clone();
        let env = &mut envs[0];
        env.tcp_backend
            .submit(
                &env.instance,
                vec![RdmaOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local: env.local_memory.clone(),
                    remote,
                }],
                Duration::from_secs(30),
            )
            .await?;

        let mut dst = vec![0u8; buf_size];
        envs[1].local_memory.read_at(0, &mut dst)?;
        for (i, byte) in dst.iter().enumerate() {
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

        let mut src = vec![0u8; buf_size];
        for (i, byte) in src.iter_mut().enumerate() {
            *byte = ((i * 3 + 17) % 256) as u8;
        }
        envs[1].local_memory.write_at(0, &src)?;

        let remote = envs[1].rdma_remote_buf.clone();
        let env = &mut envs[0];
        env.tcp_backend
            .submit(
                &env.instance,
                vec![RdmaOp {
                    op_type: RdmaOpType::ReadIntoLocal,
                    local: env.local_memory.clone(),
                    remote,
                }],
                Duration::from_secs(30),
            )
            .await?;

        let mut dst = vec![0u8; buf_size];
        envs[0].local_memory.read_at(0, &mut dst)?;
        for (i, byte) in dst.iter().enumerate() {
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

        let mut src = vec![0u8; buf_size];
        for (i, byte) in src.iter_mut().enumerate() {
            *byte = ((i * 41 + 7) % 256) as u8;
        }
        envs[0].local_memory.write_at(0, &src)?;

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

        envs[0].local_memory.write_at(0, &vec![0u8; buf_size])?;

        let env = &mut envs[0];
        env.tcp_backend
            .submit(
                &env.instance,
                vec![RdmaOp {
                    op_type: RdmaOpType::ReadIntoLocal,
                    local: env.local_memory.clone(),
                    remote,
                }],
                Duration::from_secs(30),
            )
            .await?;

        let mut dst = vec![0u8; buf_size];
        envs[0].local_memory.read_at(0, &mut dst)?;
        for (i, byte) in dst.iter().enumerate() {
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

        let mut src = vec![0u8; buf_size];
        for (i, byte) in src.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }
        envs[0].local_memory.write_at(0, &src)?;

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
                    remote,
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

        let mut envs = setup_same_proc_tcp_env(4096).await?;
        do_write_test(&mut envs, 4096, Duration::from_secs(10)).await
    }

    /// Same-process read.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_same_process_read() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let mut envs = setup_same_proc_tcp_env(2048).await?;
        do_read_test(&mut envs, 2048, Duration::from_secs(10)).await
    }

    /// Same-process write-then-read round-trip.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_same_process_round_trip() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let mut envs = setup_same_proc_tcp_env(4096).await?;
        do_round_trip_test(&mut envs, 4096, Duration::from_secs(10)).await
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
}
