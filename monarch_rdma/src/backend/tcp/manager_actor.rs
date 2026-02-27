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
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::OncePortHandle;
use hyperactor::OncePortRef;
use hyperactor::RefClient;
use hyperactor::context;
use serde::Deserialize;
use serde::Serialize;
use serde_multipart::Part;
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
        reply: OncePortRef<()>,
    },
    /// Read a chunk of data from a registered buffer at the given offset.
    ReadChunk {
        buf_id: usize,
        offset: usize,
        size: usize,
        #[reply]
        reply: OncePortRef<TcpChunk>,
    },
}
wirevalue::register_type!(TcpManagerMessage);

/// Local-only submit message for the [`TcpManagerActor`].
///
/// Not serializable because [`TcpOp`] contains `Arc<dyn RdmaLocalMemory>`.
#[derive(Handler, HandleClient, Debug)]
pub struct TcpSubmit {
    pub ops: Vec<TcpOp>,
    pub timeout: Duration,
    #[reply]
    pub reply: OncePortHandle<Result<(), String>>,
}

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

    /// Execute a write operation: read local memory in chunks and send
    /// them to the remote [`TcpManagerActor`].
    ///
    /// Each chunk is wrapped in a [`Part`] so the multipart codec can
    /// transfer the bytes without an extra serialization copy.
    async fn execute_write(
        &self,
        cx: &Context<'_, Self>,
        op: &TcpOp,
        chunk_size: usize,
    ) -> Result<()> {
        let addr = op.local_memory.addr();
        let size = op.local_memory.size();
        let mut offset = 0;

        while offset < size {
            let len = std::cmp::min(chunk_size, size - offset);

            // SAFETY: `RdmaLocalMemory` guarantees the allocation is
            // valid for the lifetime of the trait object. The caller guarantees that
            // the memory range is contained within a single allocation.
            let slice = unsafe { std::slice::from_raw_parts((addr + offset) as *const u8, len) };
            let data = Part::from(slice);

            op.remote_tcp_manager
                .write_chunk(cx, op.remote_buf_id, offset, data)
                .await?;

            offset += len;
        }

        Ok(())
    }

    /// Execute a read operation: request chunks from the remote
    /// [`TcpManagerActor`] and write them into local memory.
    async fn execute_read(
        &self,
        cx: &Context<'_, Self>,
        op: &TcpOp,
        chunk_size: usize,
    ) -> Result<()> {
        let addr = op.local_memory.addr();
        let size = op.local_memory.size();
        let mut offset = 0;

        while offset < size {
            let len = std::cmp::min(chunk_size, size - offset);

            let chunk = op
                .remote_tcp_manager
                .read_chunk(cx, op.remote_buf_id, offset, len)
                .await?;
            let data = chunk.0.into_bytes();

            anyhow::ensure!(
                data.len() == len,
                "tcp read chunk size mismatch: expected {len}, got {}",
                data.len()
            );

            // SAFETY: `RdmaLocalMemory` guarantees the allocation is
            // valid for the lifetime of the trait object.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    data.as_ptr(),
                    (addr + offset) as *mut u8,
                    data.len(),
                );
            }

            offset += len;
        }

        Ok(())
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
    ) -> Result<(), anyhow::Error> {
        let owner = self.owner.get().expect("TcpManagerActor owner not set");
        let mem = owner
            .request_local_memory(cx, buf_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("buffer {buf_id} not found"))?;

        let bytes = data.into_bytes();
        anyhow::ensure!(
            offset + bytes.len() <= mem.size(),
            "write chunk out of bounds: offset={offset} len={} buf_size={}",
            bytes.len(),
            mem.size()
        );

        let dest = mem.addr() + offset;

        // SAFETY: `RdmaLocalMemory` guarantees the allocation is valid.
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), dest as *mut u8, bytes.len());
        }

        Ok(())
    }

    async fn read_chunk(
        &mut self,
        cx: &Context<Self>,
        buf_id: usize,
        offset: usize,
        size: usize,
    ) -> Result<TcpChunk, anyhow::Error> {
        let owner = self.owner.get().expect("TcpManagerActor owner not set");
        let mem = owner
            .request_local_memory(cx, buf_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("buffer {buf_id} not found"))?;

        anyhow::ensure!(
            offset + size <= mem.size(),
            "read chunk out of bounds: offset={offset} size={size} buf_size={}",
            mem.size()
        );

        let src = mem.addr() + offset;

        // SAFETY: `RdmaLocalMemory` guarantees the allocation is valid.
        let slice = unsafe { std::slice::from_raw_parts(src as *const u8, size) };
        Ok(TcpChunk(Part::from(Bytes::copy_from_slice(slice))))
    }
}

#[async_trait]
#[hyperactor::handle(TcpSubmit)]
impl TcpSubmitHandler for TcpManagerActor {
    async fn tcp_submit(
        &mut self,
        cx: &Context<Self>,
        ops: Vec<TcpOp>,
        timeout: Duration,
    ) -> Result<Result<(), String>, anyhow::Error> {
        let chunk_size =
            hyperactor_config::global::get(crate::config::RDMA_MAX_CHUNK_SIZE_MB) * 1024 * 1024;
        let deadline = Instant::now() + timeout;
        let mut result = Ok(());

        for op in &ops {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                result = Err("tcp submit timed out".to_string());
                break;
            }

            let op_result = match op.op_type {
                RdmaOpType::WriteFromLocal => self.execute_write(cx, op, chunk_size).await,
                RdmaOpType::ReadIntoLocal => self.execute_read(cx, op, chunk_size).await,
            };

            if let Err(e) = op_result {
                result = Err(e.to_string());
                break;
            }
        }

        Ok(result)
    }
}

#[async_trait]
impl RdmaBackend for ActorHandle<TcpManagerActor> {
    type TransportInfo = ();

    /// Submit a batch of RDMA operations over TCP.
    ///
    /// Each operation's remote buffer is resolved to its TCP backend
    /// context, then the batch is delegated to the local
    /// [`TcpManagerActor`] via [`TcpSubmit`].
    async fn submit(
        &mut self,
        cx: &(impl context::Actor + Send + Sync),
        ops: Vec<RdmaOp>,
        timeout: Duration,
    ) -> Result<()> {
        let mut tcp_ops = Vec::with_capacity(ops.len());

        for op in ops {
            let (remote_tcp_mgr, remote_buf_id) = op.remote.resolve_tcp()?;
            tcp_ops.push(TcpOp {
                op_type: op.op_type,
                local_memory: op.local,
                remote_tcp_manager: remote_tcp_mgr,
                remote_buf_id,
            });
        }

        <Self as TcpSubmitClient>::tcp_submit(self, cx, tcp_ops, timeout)
            .await?
            .map_err(|e| anyhow::anyhow!(e))
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

    use super::TcpManagerActor;
    use crate::RawLocalMemory;
    use crate::RdmaLocalMemory;
    use crate::RdmaManagerMessageClient;
    use crate::RdmaOp;
    use crate::RdmaOpType;
    use crate::backend::RdmaBackend;
    use crate::rdma_manager_actor::GetTcpActorRefClient;
    use crate::rdma_manager_actor::RdmaManagerActor;

    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    struct TcpTestEnv {
        _proc_1: Proc,
        _proc_2: Proc,
        instance_1: hyperactor::Instance<()>,
        _instance_2: hyperactor::Instance<()>,
        tcp_handle_1: ActorHandle<TcpManagerActor>,
        tcp_handle_2: ActorHandle<TcpManagerActor>,
        rdma_buf_1: crate::RdmaRemoteBuffer,
        rdma_buf_2: crate::RdmaRemoteBuffer,
        local_mem_1: Arc<dyn RdmaLocalMemory>,
        _local_mem_2: Arc<dyn RdmaLocalMemory>,
        cpu_buf_1: Box<[u8]>,
        cpu_buf_2: Box<[u8]>,
    }

    /// Set up a test environment with two RdmaManagerActors, CPU buffers,
    /// and resolved `ActorHandle<TcpManagerActor>` for direct backend testing.
    ///
    /// The caller must hold a `ConfigValueGuard` for
    /// `RDMA_ALLOW_TCP_FALLBACK = true` for the duration of the test.
    async fn setup_tcp_env(buffer_size: usize) -> anyhow::Result<TcpTestEnv> {
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);

        let proc_1 = Proc::direct(
            ChannelAddr::any(hyperactor::channel::ChannelTransport::Unix),
            format!("tcp_test_{id}_a"),
        )?;
        let proc_2 = Proc::direct(
            ChannelAddr::any(hyperactor::channel::ChannelTransport::Unix),
            format!("tcp_test_{id}_b"),
        )?;

        let (instance_1, _ch1) = proc_1.instance("client")?;
        let (instance_2, _ch2) = proc_2.instance("client")?;

        let rdma_actor_1 = RdmaManagerActor::new(None, Flattrs::default()).await?;
        let rdma_handle_1 = proc_1.spawn("rdma_manager", rdma_actor_1)?;

        let rdma_actor_2 = RdmaManagerActor::new(None, Flattrs::default()).await?;
        let rdma_handle_2 = proc_2.spawn("rdma_manager", rdma_actor_2)?;

        // Resolve local TcpManagerActor handles for direct backend access.
        let tcp_ref_1 = rdma_handle_1.get_tcp_actor_ref(&instance_1).await?;
        let tcp_handle_1 = tcp_ref_1
            .downcast_handle(&instance_1)
            .ok_or_else(|| anyhow::anyhow!("tcp actor 1 not local"))?;

        let tcp_ref_2 = rdma_handle_2.get_tcp_actor_ref(&instance_2).await?;
        let tcp_handle_2 = tcp_ref_2
            .downcast_handle(&instance_2)
            .ok_or_else(|| anyhow::anyhow!("tcp actor 2 not local"))?;

        // Allocate CPU buffers.
        let mut cpu_buf_1 = vec![0u8; buffer_size].into_boxed_slice();
        let ptr_1 = cpu_buf_1.as_mut_ptr() as usize;
        let local_mem_1: Arc<dyn RdmaLocalMemory> =
            Arc::new(RawLocalMemory::new(ptr_1, buffer_size));

        let mut cpu_buf_2 = vec![0u8; buffer_size].into_boxed_slice();
        let ptr_2 = cpu_buf_2.as_mut_ptr() as usize;
        let local_mem_2: Arc<dyn RdmaLocalMemory> =
            Arc::new(RawLocalMemory::new(ptr_2, buffer_size));

        // Register buffers.
        let rdma_buf_1 = rdma_handle_1
            .request_buffer(&instance_1, local_mem_1.clone())
            .await?;

        let rdma_buf_2 = rdma_handle_2
            .request_buffer(&instance_2, local_mem_2.clone())
            .await?;

        Ok(TcpTestEnv {
            _proc_1: proc_1,
            _proc_2: proc_2,
            instance_1,
            _instance_2: instance_2,
            tcp_handle_1,
            tcp_handle_2,
            rdma_buf_1,
            rdma_buf_2,
            local_mem_1,
            _local_mem_2: local_mem_2,
            cpu_buf_1,
            cpu_buf_2,
        })
    }

    /// Write from local buffer 1 into remote buffer 2 using the TCP backend directly.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_write_from_local() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let buf_size = 4096;
        let mut env = setup_tcp_env(buf_size).await?;

        for (i, byte) in env.cpu_buf_1.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }

        env.tcp_handle_1
            .submit(
                &env.instance_1,
                vec![RdmaOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local: env.local_mem_1.clone(),
                    remote: env.rdma_buf_2.clone(),
                }],
                Duration::from_secs(30),
            )
            .await?;

        for (i, byte) in env.cpu_buf_2.iter().enumerate() {
            assert_eq!(*byte, (i % 256) as u8, "mismatch at offset {i} after write");
        }

        Ok(())
    }

    /// Read from remote buffer 2 into local buffer 1 using the TCP backend directly.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_read_into_local() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let buf_size = 2048;
        let mut env = setup_tcp_env(buf_size).await?;

        for (i, byte) in env.cpu_buf_2.iter_mut().enumerate() {
            *byte = ((i * 7 + 3) % 256) as u8;
        }

        env.tcp_handle_1
            .submit(
                &env.instance_1,
                vec![RdmaOp {
                    op_type: RdmaOpType::ReadIntoLocal,
                    local: env.local_mem_1.clone(),
                    remote: env.rdma_buf_2.clone(),
                }],
                Duration::from_secs(30),
            )
            .await?;

        for (i, byte) in env.cpu_buf_1.iter().enumerate() {
            assert_eq!(
                *byte,
                ((i * 7 + 3) % 256) as u8,
                "mismatch at offset {i} after read"
            );
        }

        Ok(())
    }

    /// Write, clear, read-back, verify round-trip via TCP backend directly.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_write_then_read_back() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let buf_size = 4096;
        let mut env = setup_tcp_env(buf_size).await?;

        for (i, byte) in env.cpu_buf_1.iter_mut().enumerate() {
            *byte = ((i * 13 + 5) % 256) as u8;
        }

        // Write local_mem_1 -> rdma_buf_2.
        env.tcp_handle_1
            .submit(
                &env.instance_1,
                vec![RdmaOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local: env.local_mem_1.clone(),
                    remote: env.rdma_buf_2.clone(),
                }],
                Duration::from_secs(30),
            )
            .await?;

        // Clear buffer 1.
        for byte in env.cpu_buf_1.iter_mut() {
            *byte = 0;
        }

        // Read rdma_buf_2 -> local_mem_1.
        env.tcp_handle_1
            .submit(
                &env.instance_1,
                vec![RdmaOp {
                    op_type: RdmaOpType::ReadIntoLocal,
                    local: env.local_mem_1.clone(),
                    remote: env.rdma_buf_2.clone(),
                }],
                Duration::from_secs(30),
            )
            .await?;

        for (i, byte) in env.cpu_buf_1.iter().enumerate() {
            assert_eq!(
                *byte,
                ((i * 13 + 5) % 256) as u8,
                "mismatch at offset {i} after read-back"
            );
        }

        Ok(())
    }

    /// Test with a buffer larger than the chunk size to exercise multi-chunk
    /// write and read paths. Sets chunk size to 1 MiB and uses a ~1.5 MiB
    /// buffer so the transfer requires 2 chunks.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_multi_chunk_write() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);
        let _chunk_guard = config.override_key(crate::config::RDMA_MAX_CHUNK_SIZE_MB, 1);

        // 1.5 MiB = 1572864 bytes > 1 MiB chunk -> 2 chunks.
        let buf_size = 3 * 1024 * 512;
        let mut env = setup_tcp_env(buf_size).await?;

        for (i, byte) in env.cpu_buf_1.iter_mut().enumerate() {
            *byte = (i % 251) as u8;
        }

        env.tcp_handle_1
            .submit(
                &env.instance_1,
                vec![RdmaOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local: env.local_mem_1.clone(),
                    remote: env.rdma_buf_2.clone(),
                }],
                Duration::from_secs(30),
            )
            .await?;

        for (i, byte) in env.cpu_buf_2.iter().enumerate() {
            assert_eq!(*byte, (i % 251) as u8, "mismatch at offset {i}");
        }

        Ok(())
    }

    /// Same as above but for multi-chunk reads.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_multi_chunk_read() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);
        let _chunk_guard = config.override_key(crate::config::RDMA_MAX_CHUNK_SIZE_MB, 1);

        let buf_size = 3 * 1024 * 512; // 1.5 MiB
        let mut env = setup_tcp_env(buf_size).await?;

        for (i, byte) in env.cpu_buf_2.iter_mut().enumerate() {
            *byte = ((i * 3 + 17) % 256) as u8;
        }

        env.tcp_handle_1
            .submit(
                &env.instance_1,
                vec![RdmaOp {
                    op_type: RdmaOpType::ReadIntoLocal,
                    local: env.local_mem_1.clone(),
                    remote: env.rdma_buf_2.clone(),
                }],
                Duration::from_secs(30),
            )
            .await?;

        for (i, byte) in env.cpu_buf_1.iter().enumerate() {
            assert_eq!(*byte, ((i * 3 + 17) % 256) as u8, "mismatch at offset {i}");
        }

        Ok(())
    }

    /// Multi-chunk write-then-read round-trip with chunk size smaller than
    /// the buffer. Uses 1 MiB chunks and a ~2.5 MiB buffer (3 chunks).
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_multi_chunk_round_trip() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);
        let _chunk_guard = config.override_key(crate::config::RDMA_MAX_CHUNK_SIZE_MB, 1);

        // 2.5 MiB -> 3 chunks.
        let buf_size = 5 * 1024 * 512;
        let mut env = setup_tcp_env(buf_size).await?;

        for (i, byte) in env.cpu_buf_1.iter_mut().enumerate() {
            *byte = ((i * 41 + 7) % 256) as u8;
        }

        // Write.
        env.tcp_handle_1
            .submit(
                &env.instance_1,
                vec![RdmaOp {
                    op_type: RdmaOpType::WriteFromLocal,
                    local: env.local_mem_1.clone(),
                    remote: env.rdma_buf_2.clone(),
                }],
                Duration::from_secs(30),
            )
            .await?;

        // Clear local.
        for byte in env.cpu_buf_1.iter_mut() {
            *byte = 0;
        }

        // Read back.
        env.tcp_handle_1
            .submit(
                &env.instance_1,
                vec![RdmaOp {
                    op_type: RdmaOpType::ReadIntoLocal,
                    local: env.local_mem_1.clone(),
                    remote: env.rdma_buf_2.clone(),
                }],
                Duration::from_secs(30),
            )
            .await?;

        for (i, byte) in env.cpu_buf_1.iter().enumerate() {
            assert_eq!(
                *byte,
                ((i * 41 + 7) % 256) as u8,
                "mismatch at offset {i} after multi-chunk round-trip"
            );
        }

        Ok(())
    }

    /// Test that resolve_tcp finds the Tcp backend context in a buffer
    /// and that the resolved actor ref matches the actual TcpManagerActor.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_tcp_resolve_tcp() -> anyhow::Result<()> {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(crate::config::RDMA_ALLOW_TCP_FALLBACK, true);

        let env = setup_tcp_env(64).await?;

        let (tcp_ref_1, id_1) = env.rdma_buf_1.resolve_tcp()?;
        assert_eq!(id_1, env.rdma_buf_1.id);
        let expected_1: hyperactor::ActorRef<TcpManagerActor> = env.tcp_handle_1.bind();
        assert_eq!(tcp_ref_1.actor_id(), expected_1.actor_id());

        let (tcp_ref_2, id_2) = env.rdma_buf_2.resolve_tcp()?;
        assert_eq!(id_2, env.rdma_buf_2.id);
        let expected_2: hyperactor::ActorRef<TcpManagerActor> = env.tcp_handle_2.bind();
        assert_eq!(tcp_ref_2.actor_id(), expected_2.actor_id());

        Ok(())
    }

    /// Test that when TCP fallback is disabled and ibverbs is unavailable,
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
