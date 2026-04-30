/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Tests for the RDMA Manager Actor
//!
//! This module contains tests for the RDMA Manager Actor functionality.
//! Tests are split into two categories:
//! 1. CPU-only tests that don't require CUDA
//! 2. CUDA tests that require GPU access

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use hyperactor::Proc;
    use hyperactor::RemoteSpawn;
    use hyperactor::channel::ChannelAddr;
    use hyperactor::channel::ChannelTransport;
    use hyperactor_config::Flattrs;

    use super::super::PollTarget;
    use super::super::manager_actor::IbvBackend;
    use super::super::manager_actor::IbvManagerActor;
    use super::super::manager_actor::IbvManagerLocalMessageClient;
    use super::super::manager_actor::IbvManagerMessageClient;
    use super::super::primitives::IbvQpType;
    use super::super::primitives::get_all_devices;
    use super::super::test_utils::IbvTestEnv;
    use super::super::test_utils::*;
    use crate::IbvConfig;
    use crate::RdmaManagerMessageClient;
    use crate::RdmaOp;
    use crate::RdmaOpType;
    use crate::RdmaRemoteBuffer;
    use crate::backend::RdmaBackend;
    use crate::backend::cuda_test_utils::CudaAllocation;
    use crate::backend::cuda_test_utils::CudaAllocator;
    use crate::backend::cuda_test_utils::cuda_allocator_scanner;
    use crate::local_memory::KeepaliveLocalMemory;
    use crate::local_memory::RdmaLocalMemory;
    use crate::rdma_components::register_segment_scanner;
    use crate::rdma_components::validate_execution_context;
    use crate::rdma_manager_actor::RdmaManagerActor;

    /// Allocate a single host-resident `KeepaliveLocalMemory` of `size`
    /// bytes pre-filled with `pattern`. The backing `Vec<u8>` is moved
    /// into the keepalive (`monarch_rdma::local_memory` provides
    /// `Keepalive for Vec<u8>` already), so the address stays valid for
    /// the lifetime of the returned handle.
    fn alloc_cpu_local(size: usize, pattern: u8) -> Arc<dyn RdmaLocalMemory> {
        let v = vec![pattern; size];
        let addr = v.as_ptr() as usize;
        Arc::new(KeepaliveLocalMemory::new(addr, size, Arc::new(v)))
    }

    fn alloc_cuda_local(
        device: i32,
        size: usize,
        pattern: u8,
        cuda_allocs: &mut Vec<CudaAllocation>,
    ) -> Result<Arc<dyn RdmaLocalMemory>, anyhow::Error> {
        let alloc = CudaAllocator::get().allocate(device, size);
        cuda_allocs.push(alloc.clone());
        let addr = alloc.ptr();
        let mem = KeepaliveLocalMemory::new(addr, size, Arc::new(alloc));
        let pattern_buf = vec![pattern; size];
        mem.write_at(0, &pattern_buf)?;
        Ok(Arc::new(mem))
    }

    fn cleanup_cuda_allocations(cuda_allocs: Vec<CudaAllocation>) {
        assert!(
            cuda_allocs.into_iter().all(|alloc| alloc.try_free()),
            "failed to free all CUDA allocations",
        );
    }

    /// Allocate `n` independent local-memory handles of `size` bytes
    /// each, every one pre-filled with the caller-supplied per-buffer
    /// pattern. `accel` is a `cpu:n` or `cuda:n` string; the index
    /// matters only for CUDA (host allocations ignore it). CUDA
    /// allocations are appended to `cuda_allocs` so the caller can
    /// `try_free` them once every reference (including the keepalive
    /// inside the returned handles) has been dropped.
    fn alloc_locals_with_patterns(
        accel: &str,
        n: usize,
        size: usize,
        pattern_for_index: impl Fn(usize) -> u8,
        cuda_allocs: &mut Vec<CudaAllocation>,
    ) -> Result<Vec<Arc<dyn RdmaLocalMemory>>, anyhow::Error> {
        let (kind, idx_str) = accel
            .split_once(':')
            .ok_or_else(|| anyhow::anyhow!("expected accel like 'cpu:0' or 'cuda:0'"))?;
        let idx: i32 = idx_str.parse()?;
        (0..n)
            .map(|i| match kind {
                "cpu" => Ok(alloc_cpu_local(size, pattern_for_index(i))),
                "cuda" => alloc_cuda_local(idx, size, pattern_for_index(i), cuda_allocs),
                _ => Err(anyhow::anyhow!("unknown accel kind: {}", kind)),
            })
            .collect()
    }

    /// Read the full `mem` and assert every byte equals `expected`.
    fn assert_buffer_uniform(
        mem: &Arc<dyn RdmaLocalMemory>,
        expected: u8,
    ) -> Result<(), anyhow::Error> {
        let mut buf = vec![0u8; mem.size()];
        mem.read_at(0, &mut buf)?;
        for (i, b) in buf.iter().enumerate() {
            if *b != expected {
                return Err(anyhow::anyhow!(
                    "byte {}: got {:#x}, expected {:#x}",
                    i,
                    b,
                    expected,
                ));
            }
        }
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_rdma_read_loopback() -> Result<(), anyhow::Error> {
        const BSIZE: usize = 32;
        // Skip test if RDMA devices are not available
        let devices = get_all_devices();
        if devices.is_empty() {
            println!("Skipping test: RDMA devices not available");
            return Ok(());
        }
        let env = IbvTestEnv::setup(BSIZE, "cpu:0", "cpu:0").await?;
        let mut qp_1 = env
            .ibv_actor_1
            .request_queue_pair(
                &env.client_1,
                env.ibv_actor_2.clone(),
                env.ibv_buffer_1.device_name.clone(),
                env.ibv_buffer_2.device_name.clone(),
            )
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;
        let wr_id = qp_1.put(env.ibv_buffer_1.clone(), env.ibv_buffer_2.clone())?;

        // Poll for completion
        wait_for_completion(&mut qp_1, PollTarget::Send, &wr_id, 2).await?;

        env.ibv_actor_1
            .release_queue_pair(
                &env.client_1,
                env.ibv_actor_2.clone(),
                env.ibv_buffer_1.device_name.clone(),
                env.ibv_buffer_2.device_name.clone(),
                qp_1,
            )
            .await?;

        env.verify_buffers(BSIZE, 0).await?;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_rdma_write_loopback() -> Result<(), anyhow::Error> {
        const BSIZE: usize = 32;
        // Skip test if RDMA devices are not available
        let devices = get_all_devices();
        if devices.is_empty() {
            println!("Skipping test: RDMA devices not available");
            return Ok(());
        }
        let env = IbvTestEnv::setup(BSIZE, "cpu:0", "cpu:0").await?;
        let mut qp_1 = env
            .ibv_actor_1
            .request_queue_pair(
                &env.client_1,
                env.ibv_actor_2.clone(),
                env.ibv_buffer_1.device_name.clone(),
                env.ibv_buffer_2.device_name.clone(),
            )
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;
        let wr_id = qp_1.put(env.ibv_buffer_1.clone(), env.ibv_buffer_2.clone())?;

        wait_for_completion(&mut qp_1, PollTarget::Send, &wr_id, 2).await?;

        env.ibv_actor_1
            .release_queue_pair(
                &env.client_1,
                env.ibv_actor_2.clone(),
                env.ibv_buffer_1.device_name.clone(),
                env.ibv_buffer_2.device_name.clone(),
                qp_1,
            )
            .await?;

        env.verify_buffers(BSIZE, 0).await?;
        Ok(())
    }

    // Test that RDMA read can be performed between two actors on separate devices.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_rdma_read_separate_devices() -> Result<(), anyhow::Error> {
        const BSIZE: usize = 32;
        let devices = get_all_devices();
        if devices.len() < 4 {
            println!(
                "skipping this test as it is only configured on H100 nodes with backend network"
            );
            return Ok(());
        }
        let env = IbvTestEnv::setup(BSIZE, "cpu:0", "cpu:1").await?;
        let mut qp_1 = env
            .ibv_actor_1
            .request_queue_pair(
                &env.client_1,
                env.ibv_actor_2.clone(),
                env.ibv_buffer_1.device_name.clone(),
                env.ibv_buffer_2.device_name.clone(),
            )
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;
        let wr_id = qp_1.get(env.ibv_buffer_1.clone(), env.ibv_buffer_2.clone())?;

        // Poll for completion
        wait_for_completion(&mut qp_1, PollTarget::Send, &wr_id, 2).await?;

        env.verify_buffers(BSIZE, 0).await?;
        Ok(())
    }

    // Test that RDMA write can be performed between two actors on separate devices.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_rdma_write_separate_devices() -> Result<(), anyhow::Error> {
        const BSIZE: usize = 32;
        let devices = get_all_devices();
        if devices.len() < 5 {
            println!(
                "skipping this test as it is only configured on H100 nodes with backend network"
            );
            return Ok(());
        }
        let env = IbvTestEnv::setup(BSIZE, "cpu:0", "cpu:1").await?;
        let mut qp_1 = env
            .ibv_actor_1
            .request_queue_pair(
                &env.client_1,
                env.ibv_actor_2.clone(),
                env.ibv_buffer_1.device_name.clone(),
                env.ibv_buffer_2.device_name.clone(),
            )
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;
        let wr_id = qp_1.put(env.ibv_buffer_1.clone(), env.ibv_buffer_2.clone())?;
        wait_for_completion(&mut qp_1, PollTarget::Send, &wr_id, 2).await?;

        env.verify_buffers(BSIZE, 0).await?;
        env.cleanup().await?;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_rdma_write_recv_separate_devices() -> Result<(), anyhow::Error> {
        const BSIZE: usize = 1024 * 1024;
        let devices = get_all_devices();
        if devices.len() < 5 {
            println!(
                "skipping this test as it is only configured on H100 nodes with backend network"
            );
            return Ok(());
        }
        let env = IbvTestEnv::setup(BSIZE, "cpu:0", "cpu:1").await?;
        let mut qp_1 = env
            .ibv_actor_1
            .request_queue_pair(
                &env.client_1,
                env.ibv_actor_2.clone(),
                env.ibv_buffer_1.device_name.clone(),
                env.ibv_buffer_2.device_name.clone(),
            )
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;
        let mut qp_2 = env
            .ibv_actor_2
            .request_queue_pair(
                &env.client_2,
                env.ibv_actor_1.clone(),
                env.ibv_buffer_2.device_name.clone(),
                env.ibv_buffer_1.device_name.clone(),
            )
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;
        qp_1.recv(env.ibv_buffer_1.clone(), env.ibv_buffer_2.clone())?;
        let wr_id = qp_2.put_with_recv(env.ibv_buffer_2.clone(), env.ibv_buffer_1.clone())?;
        wait_for_completion(&mut qp_2, PollTarget::Send, &wr_id, 5).await?;
        env.verify_buffers(BSIZE, 0).await?;
        env.cleanup().await?;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    #[ignore = "This test needed to be run in isolation"]
    async fn test_rdma_write_separate_devices_db() -> Result<(), anyhow::Error> {
        const BSIZE: usize = 1024;
        let devices = get_all_devices();
        if devices.len() < 4 {
            println!(
                "skipping this test as it is only configured on H100 nodes with backend network"
            );
            return Ok(());
        }
        let env = IbvTestEnv::setup(BSIZE, "cpu:0", "cpu:0").await?;
        let mut qp_1 = env
            .ibv_actor_1
            .request_queue_pair(
                &env.client_1,
                env.ibv_actor_2.clone(),
                env.ibv_buffer_1.device_name.clone(),
                env.ibv_buffer_2.device_name.clone(),
            )
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;
        let wr_id = qp_1.enqueue_put(env.ibv_buffer_1.clone(), env.ibv_buffer_2.clone())?;
        qp_1.ring_doorbell()?;
        // Poll for completion
        wait_for_completion(&mut qp_1, PollTarget::Send, &wr_id, 5).await?;

        env.verify_buffers(BSIZE, 0).await?;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    #[ignore = "This test needed to be run in isolation"]
    async fn test_rdma_read_separate_devices_db_check() -> Result<(), anyhow::Error> {
        const BSIZE: usize = 1024;
        let devices = get_all_devices();
        if devices.len() < 4 {
            println!(
                "skipping this test as it is only configured on H100 nodes with backend network"
            );
            return Ok(());
        }
        let env = IbvTestEnv::setup(BSIZE, "cpu:0", "cpu:1").await?;
        let mut qp_2 = env
            .ibv_actor_2
            .request_queue_pair(
                &env.client_2,
                env.ibv_actor_1.clone(),
                env.ibv_buffer_2.device_name.clone(),
                env.ibv_buffer_1.device_name.clone(),
            )
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;
        let wr_id = qp_2.enqueue_get(env.ibv_buffer_2.clone(), env.ibv_buffer_1.clone())?;
        qp_2.ring_doorbell()?;
        // Poll for completion
        wait_for_completion(&mut qp_2, PollTarget::Send, &wr_id, 5).await?;

        env.verify_buffers(BSIZE, 0).await?;
        Ok(())
    }

    // Tests RdmaBuffer's `read_into` API
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_rdma_read_into_cpu_vs_cpu() -> Result<(), anyhow::Error> {
        const BSIZE: usize = 32;
        let devices = get_all_devices();
        if devices.len() < 5 {
            println!(
                "skipping this test as it is only configured on H100 nodes with backend network"
            );
            return Ok(());
        }
        let env = IbvTestEnv::setup(BSIZE, "cpu:0", "cpu:1").await?;
        env.rdma_handle_2
            .write_from_local(&env.client_1, env.local_memory_1.clone(), 2)
            .await?;

        env.verify_buffers(BSIZE, 0).await?;
        env.cleanup().await?;
        Ok(())
    }

    // Tests RdmaBuffer's `write_from` API
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_rdma_write_from_cpu_vs_cpu() -> Result<(), anyhow::Error> {
        const BSIZE: usize = 32;
        let devices = get_all_devices();
        if devices.len() < 5 {
            println!(
                "skipping this test as it is only configured on H100 nodes with backend network"
            );
            return Ok(());
        }
        let env = IbvTestEnv::setup(BSIZE, "cpu:0", "cpu:1").await?;
        env.rdma_handle_2
            .read_into_local(&env.client_1, env.local_memory_1.clone(), 5)
            .await?;

        env.verify_buffers(BSIZE, 0).await?;
        Ok(())
    }

    // CUDA tests that require GPU access

    // Helper function to check if we're running in CPU-only mode
    fn is_cpu_only_mode() -> bool {
        !crate::is_cuda_available()
    }

    // Helper function to check if GPU supports P2P
    async fn does_gpu_support_p2p() -> bool {
        validate_execution_context().await.is_ok()
    }

    // Test that RDMA write can be performed between two actors on separate devices with CUDA.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_rdma_write_separate_devices_db_device_trigger() -> Result<(), anyhow::Error> {
        if is_cpu_only_mode() {
            println!("Skipping CUDA test in CPU-only mode");
            return Ok(());
        }
        if !does_gpu_support_p2p().await {
            println!("Skipping test: GPU P2P not supported");
            return Ok(());
        }
        const BSIZE: usize = 2 * 1024 * 1024;
        let devices = get_all_devices();
        if devices.len() < 4 {
            println!(
                "skipping this test as it is only configured on H100 nodes with backend network"
            );
            return Ok(());
        }
        let env = IbvTestEnv::setup(BSIZE, "cuda:0", "cuda:1").await?;
        let mut qp_1 = env
            .ibv_actor_1
            .request_queue_pair(
                &env.client_1,
                env.ibv_actor_2.clone(),
                env.ibv_buffer_1.device_name.clone(),
                env.ibv_buffer_2.device_name.clone(),
            )
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;
        qp_1.enqueue_put(env.ibv_buffer_1.clone(), env.ibv_buffer_2.clone())?;
        ring_db_gpu(&qp_1).await?;
        // Poll for completion
        wait_for_completion_gpu(&mut qp_1, PollTarget::Send, 5).await?;

        env.verify_buffers(BSIZE, 0).await?;
        Ok(())
    }

    // Test that RDMA read can be performed between two actors on separate devices with CUDA.
    #[timed_test::async_timed_test(timeout_secs = 60)]
    #[ignore = "This test needed to be run in isolation"]
    async fn test_rdma_read_separate_devices_db_device_trigger() -> Result<(), anyhow::Error> {
        if is_cpu_only_mode() {
            println!("Skipping CUDA test in CPU-only mode");
            return Ok(());
        }
        if !does_gpu_support_p2p().await {
            println!("Skipping test: GPU P2P not supported");
            return Ok(());
        }
        const BSIZE: usize = 2 * 1024 * 1024;
        let devices = get_all_devices();
        if devices.len() < 4 {
            println!(
                "skipping this test as it is only configured on H100 nodes with backend network"
            );
            return Ok(());
        }
        let env = IbvTestEnv::setup(BSIZE, "cuda:0", "cuda:1").await?;
        let mut qp_1 = env
            .ibv_actor_1
            .request_queue_pair(
                &env.client_1,
                env.ibv_actor_2.clone(),
                env.ibv_buffer_1.device_name.clone(),
                env.ibv_buffer_2.device_name.clone(),
            )
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;
        qp_1.enqueue_get(env.ibv_buffer_1.clone(), env.ibv_buffer_2.clone())?;
        ring_db_gpu(&qp_1).await?;
        // Poll for completion
        wait_for_completion_gpu(&mut qp_1, PollTarget::Send, 5).await?;

        env.verify_buffers(BSIZE, 0).await?;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    #[ignore = "This test needed to be run in isolation"]
    async fn test_rdma_write_recv_separate_devices_db_trigger() -> Result<(), anyhow::Error> {
        if is_cpu_only_mode() {
            println!("Skipping CUDA test in CPU-only mode");
            return Ok(());
        }
        if !does_gpu_support_p2p().await {
            println!("Skipping test: GPU P2P not supported");
            return Ok(());
        }
        const BSIZE: usize = 2 * 1024 * 1024;
        let devices = get_all_devices();
        if devices.len() < 5 {
            println!(
                "skipping this test as it is only configured on H100 nodes with backend network"
            );
            return Ok(());
        }
        let env = IbvTestEnv::setup(BSIZE, "cuda:0", "cuda:1").await?;
        let mut qp_1 = env
            .ibv_actor_1
            .request_queue_pair(
                &env.client_1,
                env.ibv_actor_2.clone(),
                env.ibv_buffer_1.device_name.clone(),
                env.ibv_buffer_2.device_name.clone(),
            )
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;
        let mut qp_2 = env
            .ibv_actor_2
            .request_queue_pair(
                &env.client_2,
                env.ibv_actor_1.clone(),
                env.ibv_buffer_2.device_name.clone(),
                env.ibv_buffer_1.device_name.clone(),
            )
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;
        recv_wqe_gpu(
            &mut qp_1,
            &env.ibv_buffer_1,
            &env.ibv_buffer_2,
            rdmaxcel_sys::ibv_wc_opcode::IBV_WC_RECV,
        )
        .await?;
        send_wqe_gpu(
            &mut qp_2,
            &env.ibv_buffer_2,
            &env.ibv_buffer_1,
            rdmaxcel_sys::MLX5_OPCODE_RDMA_WRITE_IMM,
        )
        .await?;
        ring_db_gpu(&qp_2).await?;
        wait_for_completion_gpu(&mut qp_1, PollTarget::Send, 10).await?;
        wait_for_completion_gpu(&mut qp_2, PollTarget::Send, 10).await?;
        env.verify_buffers(BSIZE, 0).await?;
        env.cleanup().await?;
        Ok(())
    }

    // Test that RDMA write can be performed between two actors on separate devices.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_rdma_write_separate_devices_cuda_vs_cpu() -> Result<(), anyhow::Error> {
        if is_cpu_only_mode() {
            println!("Skipping CUDA test in CPU-only mode");
            return Ok(());
        }
        const BSIZE: usize = 2 * 1024 * 1024; // minimum size for cuda
        let devices = get_all_devices();
        if devices.len() < 5 {
            println!(
                "skipping this test as it is only configured on H100 nodes with backend network"
            );
            return Ok(());
        }
        let env = IbvTestEnv::setup(BSIZE, "cuda:0", "cpu:1").await?;
        // Pre-initialize comms, and wait for hardware to transition to send state
        let mut qp_1 = env
            .ibv_actor_1
            .request_queue_pair(
                &env.client_1,
                env.ibv_actor_2.clone(),
                env.ibv_buffer_1.device_name.clone(),
                env.ibv_buffer_2.device_name.clone(),
            )
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;
        let wr_id = qp_1.put(env.ibv_buffer_1.clone(), env.ibv_buffer_2.clone())?;

        wait_for_completion(&mut qp_1, PollTarget::Send, &wr_id, 5).await?;

        env.verify_buffers(BSIZE, 0).await?;
        env.cleanup().await?;
        Ok(())
    }

    // Test that RDMA write can be performed between two actors on separate devices.
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_rdma_write_separate_devices_cuda_vs_cuda() -> Result<(), anyhow::Error> {
        if is_cpu_only_mode() {
            println!("Skipping CUDA test in CPU-only mode");
            return Ok(());
        }
        const BSIZE: usize = 2 * 1024 * 1024; // minimum size for cuda
        let devices = get_all_devices();
        if devices.len() < 5 {
            println!(
                "skipping this test as it is only configured on H100 nodes with backend network"
            );
            return Ok(());
        }
        let env = IbvTestEnv::setup(BSIZE, "cuda:0", "cuda:1").await?;
        // Pre-initialize comms, and wait for hardware to transition to send state
        let mut qp_1 = env
            .ibv_actor_1
            .request_queue_pair(
                &env.client_1,
                env.ibv_actor_2.clone(),
                env.ibv_buffer_1.device_name.clone(),
                env.ibv_buffer_2.device_name.clone(),
            )
            .await?
            .map_err(|e| anyhow::anyhow!(e))?;
        let wr_id = qp_1.put(env.ibv_buffer_1.clone(), env.ibv_buffer_2.clone())?;

        wait_for_completion(&mut qp_1, PollTarget::Send, &wr_id, 5).await?;

        env.verify_buffers(BSIZE, 0).await?;
        env.cleanup().await?;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_rdma_read_into_cuda_vs_cpu() -> Result<(), anyhow::Error> {
        if is_cpu_only_mode() {
            println!("Skipping CUDA test in CPU-only mode");
            return Ok(());
        }
        const BSIZE: usize = 2 * 1024 * 1024; // minimum size for cuda
        let devices = get_all_devices();
        if devices.len() < 5 {
            println!(
                "skipping this test as it is only configured on H100 nodes with backend network"
            );
            return Ok(());
        }
        let env = IbvTestEnv::setup(BSIZE, "cuda:0", "cpu:0").await?;

        env.rdma_handle_2
            .write_from_local(&env.client_1, env.local_memory_1.clone(), 5)
            .await?;

        env.verify_buffers(BSIZE, 0).await?;
        env.cleanup().await?;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_rdma_read_into_cpu_vs_cuda() -> Result<(), anyhow::Error> {
        if is_cpu_only_mode() {
            println!("Skipping CUDA test in CPU-only mode");
            return Ok(());
        }
        const BSIZE: usize = 2 * 1024 * 1024; // minimum size for cuda
        let devices = get_all_devices();
        if devices.len() < 5 {
            println!(
                "skipping this test as it is only configured on H100 nodes with backend network"
            );
            return Ok(());
        }
        let env = IbvTestEnv::setup(BSIZE, "cpu:0", "cuda:1").await?;

        env.rdma_handle_2
            .write_from_local(&env.client_1, env.local_memory_1.clone(), 5)
            .await?;

        env.verify_buffers(BSIZE, 0).await?;
        env.cleanup().await?;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_rdma_read_into_cuda_vs_cuda() -> Result<(), anyhow::Error> {
        if is_cpu_only_mode() {
            println!("Skipping CUDA test in CPU-only mode");
            return Ok(());
        }
        const BSIZE: usize = 2 * 1024 * 1024; // minimum size for cuda
        let devices = get_all_devices();
        if devices.len() < 5 {
            println!(
                "skipping this test as it is only configured on H100 nodes with backend network"
            );
            return Ok(());
        }
        let env = IbvTestEnv::setup(BSIZE, "cuda:0", "cuda:1").await?;

        env.rdma_handle_2
            .write_from_local(&env.client_1, env.local_memory_1.clone(), 5)
            .await?;

        env.verify_buffers(BSIZE, 0).await?;
        env.cleanup().await?;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_rdma_write_from_cuda_vs_cuda() -> Result<(), anyhow::Error> {
        if is_cpu_only_mode() {
            println!("Skipping CUDA test in CPU-only mode");
            return Ok(());
        }
        const BSIZE: usize = 2 * 1024 * 1024; // minimum size for cuda
        let devices = get_all_devices();
        if devices.len() < 5 {
            println!(
                "skipping this test as it is only configured on H100 nodes with backend network"
            );
            return Ok(());
        }
        let env = IbvTestEnv::setup(BSIZE, "cuda:0", "cuda:1").await?;
        env.rdma_handle_2
            .read_into_local(&env.client_1, env.local_memory_1.clone(), 5)
            .await?;

        env.verify_buffers(BSIZE, 0).await?;
        env.cleanup().await?;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_concurrent_send_to_same_target() -> Result<(), anyhow::Error> {
        const BSIZE: usize = 2 * 1024 * 1024;
        let devices = get_all_devices();
        if devices.is_empty() {
            println!("Skipping test: RDMA devices not available");
            return Ok(());
        }

        let env = IbvTestEnv::setup(BSIZE, "cuda:0", "cuda:1").await?;

        let rdma_handle_2 = env.rdma_handle_2.clone();
        let rdma_handle_4 = env.rdma_handle_2.clone();
        let rdma_handle_6 = env.rdma_handle_2.clone();
        let local_memory_1 = env.local_memory_1.clone();
        let local_memory_3 = env.local_memory_1.clone();
        let local_memory_5 = env.local_memory_1.clone();
        let client = &env.client_1;

        let task1 = async {
            rdma_handle_2
                .read_into_local(client, local_memory_1.clone(), 2)
                .await
        };

        let task2 = async {
            rdma_handle_4
                .read_into_local(client, local_memory_3.clone(), 2)
                .await
        };

        let task3 = async {
            rdma_handle_6
                .read_into_local(client, local_memory_5.clone(), 2)
                .await
        };
        let (_result1, _result2, _result3) = tokio::join!(task1, task2, task3);

        env.verify_buffers(BSIZE, 0).await?;
        env.cleanup().await?;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_rdma_read_into_standard_qp() -> Result<(), anyhow::Error> {
        const BSIZE: usize = 32;
        // Skip test if RDMA devices are not available
        let devices = get_all_devices();
        if devices.is_empty() {
            println!("Skipping test: RDMA devices not available");
            return Ok(());
        }

        let env =
            IbvTestEnv::setup_with_qp_type(BSIZE, "cpu:0", "cpu:0", IbvQpType::Standard).await?;

        env.rdma_handle_2
            .write_from_local(&env.client_1, env.local_memory_1.clone(), 2)
            .await?;

        env.verify_buffers(BSIZE, 0).await?;
        env.cleanup().await?;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_rdma_write_from_standard_qp() -> Result<(), anyhow::Error> {
        const BSIZE: usize = 32;
        // Skip test if RDMA devices are not available
        let devices = get_all_devices();
        if devices.is_empty() {
            println!("Skipping test: RDMA devices not available");
            return Ok(());
        }

        let env =
            IbvTestEnv::setup_with_qp_type(BSIZE, "cpu:0", "cpu:0", IbvQpType::Standard).await?;

        env.rdma_handle_2
            .read_into_local(&env.client_1, env.local_memory_1.clone(), 2)
            .await?;

        env.verify_buffers(BSIZE, 0).await?;
        env.cleanup().await?;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_rdma_read_into_standard_qp_cuda() -> Result<(), anyhow::Error> {
        if is_cpu_only_mode() {
            println!("Skipping CUDA test in CPU-only mode");
            return Ok(());
        }
        const BSIZE: usize = 16 * 1024 * 1024;
        // Skip test if RDMA devices are not available
        let devices = get_all_devices();
        if devices.is_empty() {
            println!("Skipping test: RDMA devices not available");
            return Ok(());
        }

        let env =
            IbvTestEnv::setup_with_qp_type(BSIZE, "cuda:0", "cuda:1", IbvQpType::Standard).await?;

        env.rdma_handle_2
            .write_from_local(&env.client_1, env.local_memory_1.clone(), 5)
            .await?;

        env.verify_buffers(BSIZE, 0).await?;
        env.cleanup().await?;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_rdma_write_from_standard_qp_cuda() -> Result<(), anyhow::Error> {
        if is_cpu_only_mode() {
            println!("Skipping CUDA test in CPU-only mode");
            return Ok(());
        }
        const BSIZE: usize = 16 * 1024 * 1024;
        // Skip test if RDMA devices are not available
        let devices = get_all_devices();
        if devices.is_empty() {
            println!("Skipping test: RDMA devices not available");
            return Ok(());
        }

        let env =
            IbvTestEnv::setup_with_qp_type(BSIZE, "cuda:0", "cuda:1", IbvQpType::Standard).await?;

        env.rdma_handle_2
            .read_into_local(&env.client_1, env.local_memory_1.clone(), 5)
            .await?;

        env.verify_buffers(BSIZE, 0).await?;
        env.cleanup().await?;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 120)]
    async fn test_rdma_read_chunk() -> Result<(), anyhow::Error> {
        if is_cpu_only_mode() {
            println!("Skipping CUDA test in CPU-only mode");
            return Ok(());
        }
        // 2GB tensor size
        const BSIZE: usize = 2 * 1024 * 1024 * 1024;
        const CHUNK_SIZE: usize = 2 * 1024 * 1024; // 2MB
        let devices = get_all_devices();
        if devices.len() < 5 {
            println!(
                "Skipping test: requires H100 nodes with backend network (found {} devices)",
                devices.len()
            );
            return Ok(());
        }

        println!("Setting up 2GB CUDA tensors for RDMA read test...");
        let env = IbvTestEnv::setup(BSIZE, "cuda:0", "cuda:1").await?;

        println!("Performing RDMA read operation on 2GB tensor...");
        env.rdma_handle_2
            .write_from_local(&env.client_1, env.local_memory_1.clone(), 30)
            .await?;

        println!("Verifying first 2MB...");
        env.verify_buffers(CHUNK_SIZE, 0).await?;

        println!("Verifying last 2MB...");
        env.verify_buffers(CHUNK_SIZE, BSIZE - CHUNK_SIZE).await?;

        println!("Cleaning up...");
        env.cleanup().await?;

        println!("2GB RDMA read test completed successfully");
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_rdma_write_chunk() -> Result<(), anyhow::Error> {
        if is_cpu_only_mode() {
            println!("Skipping CUDA test in CPU-only mode");
            return Ok(());
        }
        // 2GB tensor size
        const BSIZE: usize = 2 * 1024 * 1024 * 1024;
        const CHUNK_SIZE: usize = 2 * 1024 * 1024; // 2MB
        let devices = get_all_devices();
        if devices.len() < 5 {
            println!(
                "Skipping test: requires H100 nodes with backend network (found {} devices)",
                devices.len()
            );
            return Ok(());
        }

        println!("Setting up 2GB CUDA tensors for RDMA write test...");
        let env = IbvTestEnv::setup(BSIZE, "cuda:0", "cuda:1").await?;

        println!("Performing RDMA write operation on 2GB tensor...");
        env.rdma_handle_2
            .read_into_local(&env.client_1, env.local_memory_1.clone(), 30)
            .await?;

        println!("Verifying first 2MB...");
        env.verify_buffers(CHUNK_SIZE, 0).await?;

        println!("Verifying last 2MB...");
        env.verify_buffers(CHUNK_SIZE, BSIZE - CHUNK_SIZE).await?;

        println!("Cleaning up...");
        env.cleanup().await?;

        println!("2GB RDMA write test completed successfully");
        Ok(())
    }

    /// Register the cuda allocator's segment scanner so the mlx5dv path
    /// can find `CudaAllocation`-backed buffers without going through
    /// dmabuf.
    fn ensure_cuda_segment_scanner() {
        static REGISTER: std::sync::Once = std::sync::Once::new();
        REGISTER.call_once(|| {
            register_segment_scanner(Some(cuda_allocator_scanner));
        });
    }

    /// Spawn a `Proc` hosting one `RdmaManagerActor` targeting `accel`.
    async fn spawn_rdma_manager_proc(
        accel: &str,
        proc_name: String,
    ) -> Result<
        (
            Proc,
            hyperactor::Instance<()>,
            hyperactor::ActorHandle<RdmaManagerActor>,
        ),
        anyhow::Error,
    > {
        let config = IbvConfig::targeting(accel);
        let proc = Proc::direct(ChannelAddr::any(ChannelTransport::Unix), proc_name)?;
        let (instance, _) = proc.instance("client")?;
        let actor = RdmaManagerActor::new(Some(config), Flattrs::default()).await?;
        let handle = proc.spawn("rdma_manager", actor)?;
        Ok((proc, instance, handle))
    }

    /// Tear down `procs` so MRs deregister before their backing storage
    /// is freed. Otherwise `RdmaManagerActor::handle_supervision_event`
    /// can call `std::process::exit(1)` after the test logic completes.
    async fn destroy_procs(procs: Vec<Proc>) -> Result<(), anyhow::Error> {
        let timeout = std::time::Duration::from_secs(10);
        for mut proc in procs {
            proc.destroy_and_wait::<()>(timeout, None, "test cleanup")
                .await?;
        }
        Ok(())
    }

    /// `N` concurrent ops, half reads + half writes, each touching a
    /// distinct `(local_i, remote_i)` pair. The read half (`N/2 = 32`)
    /// exceeds `max_rd_atomic = 16`, exercising read-permit recycling.
    async fn submit_parallel_rw_inner(accel: &str) -> Result<(), anyhow::Error> {
        // 2MB matches CUDA's VMM allocation granularity, which is the
        // minimum size `cuMemGetHandleForAddressRange(DMA_BUF_FD, ...)`
        // accepts inside `register_mr_impl`'s dmabuf fallback.
        const BSIZE: usize = 2 * 1024 * 1024;
        const N: usize = 64;
        assert!(
            !get_all_devices().is_empty(),
            "test requires RDMA devices to be available",
        );

        fn local_pattern(i: usize) -> u8 {
            i as u8
        }
        fn remote_pattern(i: usize) -> u8 {
            (i as u8).wrapping_add(0x80)
        }

        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let (proc_1, instance_1, _actor_1) =
            spawn_rdma_manager_proc(accel, format!("rdma_submit_rw_{id}_a")).await?;
        let (proc_2, instance_2, actor_2) =
            spawn_rdma_manager_proc(accel, format!("rdma_submit_rw_{id}_b")).await?;

        let mut cuda_allocs: Vec<CudaAllocation> = Vec::new();
        let remote_locals =
            alloc_locals_with_patterns(accel, N, BSIZE, remote_pattern, &mut cuda_allocs)?;
        let mut remote_handles: Vec<RdmaRemoteBuffer> = Vec::with_capacity(N);
        for mem in &remote_locals {
            let h = actor_2.request_buffer(&instance_2, mem.clone()).await?;
            remote_handles.push(h);
        }

        let local_mems =
            alloc_locals_with_patterns(accel, N, BSIZE, local_pattern, &mut cuda_allocs)?;

        let ops: Vec<RdmaOp> = (0..N)
            .map(|i| RdmaOp {
                op_type: if i % 2 == 0 {
                    RdmaOpType::ReadIntoLocal
                } else {
                    RdmaOpType::WriteFromLocal
                },
                local: local_mems[i].clone(),
                remote: remote_handles[i].clone(),
            })
            .collect();
        let mut backend = IbvBackend(IbvManagerActor::local_handle(&instance_1).await?);
        backend
            .submit(&instance_1, ops, std::time::Duration::from_secs(30))
            .await?;

        for i in 0..N {
            if i % 2 == 0 {
                assert_buffer_uniform(&local_mems[i], remote_pattern(i))?;
            } else {
                assert_buffer_uniform(&remote_locals[i], local_pattern(i))?;
            }
        }

        for h in &remote_handles {
            h.drop_buffer(&instance_2).await?;
        }
        // Drop the backend (and its `ActorHandle<IbvManagerActor>`)
        // before tearing down the procs so the actors can shut down.
        drop(backend);
        destroy_procs(vec![proc_1, proc_2]).await?;
        drop(remote_locals);
        drop(local_mems);
        cleanup_cuda_allocations(cuda_allocs);
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 120)]
    async fn test_submit_parallel_rw_cpu() -> Result<(), anyhow::Error> {
        submit_parallel_rw_inner("cpu:0").await
    }

    #[timed_test::async_timed_test(timeout_secs = 120)]
    async fn test_submit_parallel_rw_cuda() -> Result<(), anyhow::Error> {
        assert!(crate::is_cuda_available(), "CUDA must be available");
        ensure_cuda_segment_scanner();
        submit_parallel_rw_inner("cuda:0").await
    }

    /// `N > max_rd_atomic` reads from one remote into `N` distinct
    /// locals. Each local has a sentinel disjoint from `REMOTE_FILL`,
    /// so any missed read surfaces as a surviving sentinel.
    async fn submit_fanout_reads_inner(accel: &str) -> Result<(), anyhow::Error> {
        // 2MB matches CUDA's VMM allocation granularity, which is the
        // minimum size `cuMemGetHandleForAddressRange(DMA_BUF_FD, ...)`
        // accepts inside `register_mr_impl`'s dmabuf fallback.
        const BSIZE: usize = 2 * 1024 * 1024;
        const N: usize = 64;
        const REMOTE_FILL: u8 = 0x2A;
        assert!(
            !get_all_devices().is_empty(),
            "test requires RDMA devices to be available",
        );

        // i*37+0x55 mod 256 cycles through all 256 values (gcd(37, 256) = 1);
        // bump off REMOTE_FILL so a missed read can't masquerade as success.
        fn local_sentinel(i: usize) -> u8 {
            let v = ((i as u32).wrapping_mul(37).wrapping_add(0x55)) as u8;
            if v == REMOTE_FILL {
                v.wrapping_add(1)
            } else {
                v
            }
        }

        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let (proc_1, instance_1, _actor_1) =
            spawn_rdma_manager_proc(accel, format!("rdma_submit_fanout_{id}_a")).await?;
        let (proc_2, instance_2, actor_2) =
            spawn_rdma_manager_proc(accel, format!("rdma_submit_fanout_{id}_b")).await?;

        let mut cuda_allocs: Vec<CudaAllocation> = Vec::new();
        let remote_local =
            alloc_locals_with_patterns(accel, 1, BSIZE, |_| REMOTE_FILL, &mut cuda_allocs)?
                .into_iter()
                .next()
                .unwrap();
        let remote_handle = actor_2
            .request_buffer(&instance_2, remote_local.clone())
            .await?;

        let local_mems =
            alloc_locals_with_patterns(accel, N, BSIZE, local_sentinel, &mut cuda_allocs)?;
        let ops: Vec<RdmaOp> = local_mems
            .iter()
            .map(|m| RdmaOp {
                op_type: RdmaOpType::ReadIntoLocal,
                local: m.clone(),
                remote: remote_handle.clone(),
            })
            .collect();
        let mut backend = IbvBackend(IbvManagerActor::local_handle(&instance_1).await?);
        backend
            .submit(&instance_1, ops, std::time::Duration::from_secs(30))
            .await?;

        for mem in &local_mems {
            assert_buffer_uniform(mem, REMOTE_FILL)?;
        }

        remote_handle.drop_buffer(&instance_2).await?;
        // Drop the backend (and its `ActorHandle<IbvManagerActor>`)
        // before tearing down the procs so the actors can shut down.
        drop(backend);
        destroy_procs(vec![proc_1, proc_2]).await?;
        drop(remote_local);
        drop(local_mems);
        cleanup_cuda_allocations(cuda_allocs);
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 120)]
    async fn test_submit_fanout_reads_cpu() -> Result<(), anyhow::Error> {
        submit_fanout_reads_inner("cpu:0").await
    }

    #[timed_test::async_timed_test(timeout_secs = 120)]
    async fn test_submit_fanout_reads_cuda() -> Result<(), anyhow::Error> {
        assert!(crate::is_cuda_available(), "CUDA must be available");
        ensure_cuda_segment_scanner();
        submit_fanout_reads_inner("cuda:0").await
    }

    // Verify that the per-actor LRU cache of locally registered MRs evicts
    // its least-recently-used entry when capacity is exceeded. We pin the
    // cache size to 2, register three distinct (addr, size) keys, then
    // observe that the evicted key triggers a fresh registration (new
    // `mrv.id`) while a still-cached key is served from the cache (same
    // `mrv.id`).
    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_mr_lru_cache_evicts_on_overflow() -> Result<(), anyhow::Error> {
        // Override before IbvTestEnv::setup — IbvManagerActor::new reads
        // RDMA_MR_LRU_CACHE_SIZE once at construction time.
        let config_lock = hyperactor_config::global::lock();
        let _lru_guard = config_lock.override_key(crate::config::RDMA_MR_LRU_CACHE_SIZE, 2);

        let devices = get_all_devices();
        if devices.is_empty() {
            println!("Skipping test: RDMA devices not available");
            return Ok(());
        }

        const BSIZE: usize = 4096;
        let env = IbvTestEnv::setup(BSIZE, "cpu:0", "cpu:0").await?;

        let ibv_handle = env
            .ibv_actor_1
            .downcast_handle(&env.client_1)
            .ok_or_else(|| anyhow::anyhow!("IbvManagerActor not local to client_1"))?;

        // Hold these buffers for the whole test so the underlying MRs
        // stay valid. Distinct allocations give us distinct (addr, size)
        // cache keys.
        let buf_a = vec![0u8; BSIZE].into_boxed_slice();
        let buf_b = vec![0u8; BSIZE].into_boxed_slice();
        let buf_c = vec![0u8; BSIZE].into_boxed_slice();
        let addr_a = buf_a.as_ptr() as usize;
        let addr_b = buf_b.as_ptr() as usize;
        let addr_c = buf_c.as_ptr() as usize;

        let mr_a1 = ibv_handle
            .register_mr(&env.client_1, addr_a, BSIZE)
            .await?
            .map_err(anyhow::Error::msg)?;
        let mr_b1 = ibv_handle
            .register_mr(&env.client_1, addr_b, BSIZE)
            .await?
            .map_err(anyhow::Error::msg)?;
        // Inserting c evicts a. LRU now holds {c, b}.
        let mr_c1 = ibv_handle
            .register_mr(&env.client_1, addr_c, BSIZE)
            .await?
            .map_err(anyhow::Error::msg)?;

        // b is still cached: same mrv id.
        let mr_b2 = ibv_handle
            .register_mr(&env.client_1, addr_b, BSIZE)
            .await?
            .map_err(anyhow::Error::msg)?;
        assert_eq!(
            mr_b1.mrv.id, mr_b2.mrv.id,
            "cached (addr, size) should return the same mrv id"
        );

        // a was evicted: fresh registration yields a new id.
        let mr_a2 = ibv_handle
            .register_mr(&env.client_1, addr_a, BSIZE)
            .await?
            .map_err(anyhow::Error::msg)?;
        assert_ne!(
            mr_a1.mrv.id, mr_a2.mrv.id,
            "evicted (addr, size) should produce a fresh mrv id"
        );
        assert!(
            mr_a2.mrv.id > mr_c1.mrv.id,
            "fresh registration after eviction should receive an id past all prior registrations \
             (got mr_a2.mrv.id = {}, mr_c1.mrv.id = {})",
            mr_a2.mrv.id,
            mr_c1.mrv.id,
        );

        env.cleanup().await?;
        Ok(())
    }
}
