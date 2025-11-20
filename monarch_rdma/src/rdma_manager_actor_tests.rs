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
    use crate::PollTarget;
    use crate::ibverbs_primitives::get_all_devices;
    use crate::rdma_components::validate_execution_context;
    use crate::rdma_manager_actor::RdmaManagerMessageClient;
    use crate::test_utils::test_utils::RdmaManagerTestEnv;
    use crate::test_utils::test_utils::*;

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_rdma_read_loopback() -> Result<(), anyhow::Error> {
        const BSIZE: usize = 32;
        // Skip test if RDMA devices are not available
        let devices = get_all_devices();
        if devices.is_empty() {
            println!("Skipping test: RDMA devices not available");
            return Ok(());
        }
        let env = RdmaManagerTestEnv::setup(BSIZE, "cpu:0", "cpu:0").await?;
        let mut qp_1 = env
            .actor_1
            .request_queue_pair(
                &env.client_1,
                env.actor_2.clone(),
                env.rdma_handle_1.device_name.clone(),
                env.rdma_handle_2.device_name.clone(),
            )
            .await?;
        qp_1.put(env.rdma_handle_1.clone(), env.rdma_handle_2.clone())?;

        // Poll for completion
        wait_for_completion(&mut qp_1, PollTarget::Send, 2).await?;

        env.actor_1
            .release_queue_pair(
                &env.client_1,
                env.actor_2.clone(),
                env.rdma_handle_1.device_name.clone(),
                env.rdma_handle_2.device_name.clone(),
                qp_1,
            )
            .await?;

        env.verify_buffers(BSIZE).await?;
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
        let env = RdmaManagerTestEnv::setup(BSIZE, "cpu:0", "cpu:0").await?;
        let mut qp_1 = env
            .actor_1
            .request_queue_pair(
                &env.client_1,
                env.actor_2.clone(),
                env.rdma_handle_1.device_name.clone(),
                env.rdma_handle_2.device_name.clone(),
            )
            .await?;
        qp_1.put(env.rdma_handle_1.clone(), env.rdma_handle_2.clone())?;

        wait_for_completion(&mut qp_1, PollTarget::Send, 2).await?;

        env.actor_1
            .release_queue_pair(
                &env.client_1,
                env.actor_2.clone(),
                env.rdma_handle_1.device_name.clone(),
                env.rdma_handle_2.device_name.clone(),
                qp_1,
            )
            .await?;

        env.verify_buffers(BSIZE).await?;
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
        let env = RdmaManagerTestEnv::setup(BSIZE, "cpu:0", "cpu:1").await?;
        let mut qp_1 = env
            .actor_1
            .request_queue_pair(
                &env.client_1,
                env.actor_2.clone(),
                env.rdma_handle_1.device_name.clone(),
                env.rdma_handle_2.device_name.clone(),
            )
            .await?;
        qp_1.get(env.rdma_handle_1.clone(), env.rdma_handle_2.clone())?;

        // Poll for completion
        wait_for_completion(&mut qp_1, PollTarget::Send, 2).await?;

        env.verify_buffers(BSIZE).await?;
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
        let env = RdmaManagerTestEnv::setup(BSIZE, "cpu:0", "cpu:1").await?;
        let mut qp_1 = env
            .actor_1
            .request_queue_pair(
                &env.client_1,
                env.actor_2.clone(),
                env.rdma_handle_1.device_name.clone(),
                env.rdma_handle_2.device_name.clone(),
            )
            .await?;
        qp_1.put(env.rdma_handle_1.clone(), env.rdma_handle_2.clone())?;
        wait_for_completion(&mut qp_1, PollTarget::Send, 2).await?;

        env.verify_buffers(BSIZE).await?;
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
        let env = RdmaManagerTestEnv::setup(BSIZE, "cpu:0", "cpu:1").await?;
        let mut qp_1 = env
            .actor_1
            .request_queue_pair(
                &env.client_1,
                env.actor_2.clone(),
                env.rdma_handle_1.device_name.clone(),
                env.rdma_handle_2.device_name.clone(),
            )
            .await?;
        let mut qp_2 = env
            .actor_2
            .request_queue_pair(
                &env.client_2,
                env.actor_1.clone(),
                env.rdma_handle_2.device_name.clone(),
                env.rdma_handle_1.device_name.clone(),
            )
            .await?;
        qp_2.put_with_recv(env.rdma_handle_2.clone(), env.rdma_handle_1.clone())?;
        qp_1.recv(env.rdma_handle_1.clone(), env.rdma_handle_2.clone())?;
        wait_for_completion(&mut qp_2, PollTarget::Send, 5).await?;
        env.verify_buffers(BSIZE).await?;
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
        let env = RdmaManagerTestEnv::setup(BSIZE, "cpu:0", "cpu:0").await?;
        let mut qp_1 = env
            .actor_1
            .request_queue_pair(
                &env.client_1,
                env.actor_2.clone(),
                env.rdma_handle_1.device_name.clone(),
                env.rdma_handle_2.device_name.clone(),
            )
            .await?;
        qp_1.enqueue_put(env.rdma_handle_1.clone(), env.rdma_handle_2.clone())?;
        qp_1.ring_doorbell()?;
        // Poll for completion
        wait_for_completion(&mut qp_1, PollTarget::Send, 5).await?;

        env.verify_buffers(BSIZE).await?;
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
        let env = RdmaManagerTestEnv::setup(BSIZE, "cpu:0", "cpu:1").await?;
        let mut qp_2 = env
            .actor_2
            .request_queue_pair(
                &env.client_2,
                env.actor_1.clone(),
                env.rdma_handle_2.device_name.clone(),
                env.rdma_handle_1.device_name.clone(),
            )
            .await?;
        qp_2.enqueue_get(env.rdma_handle_2.clone(), env.rdma_handle_1.clone())?;
        qp_2.ring_doorbell()?;
        // Poll for completion
        wait_for_completion(&mut qp_2, PollTarget::Send, 5).await?;

        env.verify_buffers(BSIZE).await?;
        Ok(())
    }

    // Tests RdmaBufer's `read_into` API
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
        let env = RdmaManagerTestEnv::setup(BSIZE, "cpu:0", "cpu:1").await?;
        let /*mut*/ rdma_handle_1 = env.rdma_handle_1.clone();
        rdma_handle_1
            .read_into(env.client_1, env.rdma_handle_2.clone(), 2)
            .await?;

        env.verify_buffers(BSIZE).await?;
        env.cleanup().await?;
        Ok(())
    }

    // Tests RdmaBufer's `write_from` API
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
        let env = RdmaManagerTestEnv::setup(BSIZE, "cpu:0", "cpu:1").await?;
        let /*mut*/ rdma_handle_1 = env.rdma_handle_1.clone();
        rdma_handle_1
            .write_from(env.client_1, env.rdma_handle_2.clone(), 2)
            .await?;

        env.verify_buffers(BSIZE).await?;
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
        let env = RdmaManagerTestEnv::setup(BSIZE, "cuda:0", "cuda:1").await?;
        let mut qp_1 = env
            .actor_1
            .request_queue_pair(
                &env.client_1,
                env.actor_2.clone(),
                env.rdma_handle_1.device_name.clone(),
                env.rdma_handle_2.device_name.clone(),
            )
            .await?;
        qp_1.enqueue_put(env.rdma_handle_1.clone(), env.rdma_handle_2.clone())?;
        ring_db_gpu(&mut qp_1).await?;
        // Poll for completion
        wait_for_completion_gpu(&mut qp_1, PollTarget::Send, 5).await?;

        env.verify_buffers(BSIZE).await?;
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
        let env = RdmaManagerTestEnv::setup(BSIZE, "cuda:0", "cuda:1").await?;
        let mut qp_1 = env
            .actor_1
            .request_queue_pair(
                &env.client_1,
                env.actor_2.clone(),
                env.rdma_handle_1.device_name.clone(),
                env.rdma_handle_2.device_name.clone(),
            )
            .await?;
        qp_1.enqueue_get(env.rdma_handle_1.clone(), env.rdma_handle_2.clone())?;
        ring_db_gpu(&mut qp_1).await?;
        // Poll for completion
        wait_for_completion_gpu(&mut qp_1, PollTarget::Send, 5).await?;

        env.verify_buffers(BSIZE).await?;
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
        let env = RdmaManagerTestEnv::setup(BSIZE, "cuda:0", "cuda:1").await?;
        let mut qp_1 = env
            .actor_1
            .request_queue_pair(
                &env.client_1,
                env.actor_2.clone(),
                env.rdma_handle_1.device_name.clone(),
                env.rdma_handle_2.device_name.clone(),
            )
            .await?;
        let mut qp_2 = env
            .actor_2
            .request_queue_pair(
                &env.client_2,
                env.actor_1.clone(),
                env.rdma_handle_2.device_name.clone(),
                env.rdma_handle_1.device_name.clone(),
            )
            .await?;
        recv_wqe_gpu(
            &mut qp_1,
            &env.rdma_handle_1.clone(),
            &env.rdma_handle_2.clone(),
            rdmaxcel_sys::ibv_wc_opcode::IBV_WC_RECV,
        )
        .await?;
        send_wqe_gpu(
            &mut qp_2,
            &env.rdma_handle_2.clone(),
            &env.rdma_handle_1.clone(),
            rdmaxcel_sys::MLX5_OPCODE_RDMA_WRITE_IMM,
        )
        .await?;
        ring_db_gpu(&mut qp_2).await?;
        wait_for_completion_gpu(&mut qp_1, PollTarget::Recv, 10).await?;
        wait_for_completion_gpu(&mut qp_2, PollTarget::Send, 10).await?;
        env.verify_buffers(BSIZE).await?;
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
        let env = RdmaManagerTestEnv::setup(BSIZE, "cuda:0", "cpu:1").await?;
        // Pre-initialize comms, and wait for hardware to transition to send state
        let mut qp_1 = env
            .actor_1
            .request_queue_pair(
                &env.client_1,
                env.actor_2.clone(),
                env.rdma_handle_1.device_name.clone(),
                env.rdma_handle_2.device_name.clone(),
            )
            .await?;
        qp_1.put(env.rdma_handle_1.clone(), env.rdma_handle_2.clone())?;

        wait_for_completion(&mut qp_1, PollTarget::Send, 5).await?;

        env.verify_buffers(BSIZE).await?;
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
        let env = RdmaManagerTestEnv::setup(BSIZE, "cuda:0", "cuda:1").await?;
        // Pre-initialize comms, and wait for hardware to transition to send state
        let mut qp_1 = env
            .actor_1
            .request_queue_pair(
                &env.client_1,
                env.actor_2.clone(),
                env.rdma_handle_1.device_name.clone(),
                env.rdma_handle_2.device_name.clone(),
            )
            .await?;
        qp_1.put(env.rdma_handle_1.clone(), env.rdma_handle_2.clone())?;

        wait_for_completion(&mut qp_1, PollTarget::Send, 5).await?;

        env.verify_buffers(BSIZE).await?;
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
        let env = RdmaManagerTestEnv::setup(BSIZE, "cuda:0", "cpu:0").await?;
        let /*mut*/ rdma_handle_1 = env.rdma_handle_1.clone();

        // Pre-initialize comms, and wait for hardware to transition to send state

        rdma_handle_1
            .read_into(env.client_1, env.rdma_handle_2.clone(), 5)
            .await?;

        env.verify_buffers(BSIZE).await?;
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
        let env = RdmaManagerTestEnv::setup(BSIZE, "cpu:0", "cuda:1").await?;
        let /*mut*/ rdma_handle_1 = env.rdma_handle_1.clone();

        rdma_handle_1
            .read_into(env.client_1, env.rdma_handle_2.clone(), 5)
            .await?;

        env.verify_buffers(BSIZE).await?;
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
        let env = RdmaManagerTestEnv::setup(BSIZE, "cuda:0", "cuda:1").await?;

        let /*mut*/ rdma_handle_1 = env.rdma_handle_1.clone();
        rdma_handle_1
            .read_into(env.client_1, env.rdma_handle_2.clone(), 5)
            .await?;

        env.verify_buffers(BSIZE).await?;
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
        let env = RdmaManagerTestEnv::setup(BSIZE, "cuda:0", "cuda:1").await?;
        // Pre-initialize comms, and wait for hardware to transition to send state
        let /*mut*/ rdma_handle_1 = env.rdma_handle_1.clone();
        rdma_handle_1
            .write_from(env.client_1, env.rdma_handle_2.clone(), 5)
            .await?;

        env.verify_buffers(BSIZE).await?;
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

        let env = RdmaManagerTestEnv::setup_with_qp_type(
            BSIZE,
            "cpu:0",
            "cpu:0",
            crate::ibverbs_primitives::RdmaQpType::Standard,
        )
        .await?;

        let rdma_handle_1 = env.rdma_handle_1.clone();
        rdma_handle_1
            .read_into(env.client_1, env.rdma_handle_2.clone(), 2)
            .await?;

        env.verify_buffers(BSIZE).await?;
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

        let env = RdmaManagerTestEnv::setup_with_qp_type(
            BSIZE,
            "cpu:0",
            "cpu:0",
            crate::ibverbs_primitives::RdmaQpType::Standard,
        )
        .await?;

        let rdma_handle_1 = env.rdma_handle_1.clone();
        rdma_handle_1
            .write_from(env.client_1, env.rdma_handle_2.clone(), 2)
            .await?;

        env.verify_buffers(BSIZE).await?;
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

        let env = RdmaManagerTestEnv::setup_with_qp_type(
            BSIZE,
            "cuda:0",
            "cuda:1",
            crate::ibverbs_primitives::RdmaQpType::Standard,
        )
        .await?;

        let rdma_handle_1 = env.rdma_handle_1.clone();
        rdma_handle_1
            .read_into(env.client_1, env.rdma_handle_2.clone(), 5)
            .await?;

        env.verify_buffers(BSIZE).await?;
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

        let env = RdmaManagerTestEnv::setup_with_qp_type(
            BSIZE,
            "cuda:0",
            "cuda:1",
            crate::ibverbs_primitives::RdmaQpType::Standard,
        )
        .await?;

        let rdma_handle_1 = env.rdma_handle_1.clone();
        rdma_handle_1
            .write_from(env.client_1, env.rdma_handle_2.clone(), 5)
            .await?;

        env.verify_buffers(BSIZE).await?;
        env.cleanup().await?;
        Ok(())
    }
}
