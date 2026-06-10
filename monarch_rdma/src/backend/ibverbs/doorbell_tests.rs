/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Doorbell tests for the legacy `IbvManagerActor` test path.
//!
//! These exercise the new test-only [`RawQueuePair`] message — they bypass
//! [`QueuePairActor`] and post WRs directly on the returned [`IbvQueuePair`]
//! so we can drive the device doorbell from the test (CPU `ring_doorbell`
//! or GPU `ring_db_gpu`).

#[cfg(test)]
mod tests {
    use hyperactor::ActorHandle;
    use hyperactor::ActorRef;
    use hyperactor::context::Mailbox;

    use super::super::IbvQueuePair;
    use super::super::PollTarget;
    use super::super::doorbell_test_utils::DoorbellTestEnv;
    use super::super::doorbell_test_utils::*;
    use super::super::manager_actor::IbvManagerActor;
    use super::super::manager_actor::RawQueuePair;
    use super::super::mlx_device::MlxDevice;
    use super::super::primitives::get_all_devices;
    use crate::rdma_components::validate_execution_context;

    /// Opens a one-shot reply port and posts [`RawQueuePair`] to bring up
    /// a queue pair via the legacy test path. Replaces the old
    /// `manager_actor::request_queue_pair` helper.
    async fn request_queue_pair(
        actor: &ActorHandle<IbvManagerActor<MlxDevice>>,
        cx: &(impl hyperactor::context::Actor + Send + Sync),
        other: ActorRef<IbvManagerActor<MlxDevice>>,
        self_device: String,
        other_device: String,
    ) -> Result<Result<IbvQueuePair, String>, anyhow::Error> {
        let (reply, rx) = Mailbox::mailbox(cx).open_once_port::<Result<IbvQueuePair, String>>();
        actor.try_post(
            cx,
            RawQueuePair::<MlxDevice> {
                peer: other,
                self_device,
                peer_device: other_device,
                reply,
            },
        )?;
        rx.recv()
            .await
            .map_err(|e| anyhow::anyhow!("RawQueuePair port closed: {e}"))
    }

    fn is_cpu_only_mode() -> bool {
        !crate::is_cuda_available()
    }

    async fn does_gpu_support_p2p() -> bool {
        validate_execution_context().await.is_ok()
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_rdma_write_separate_devices_db() -> Result<(), anyhow::Error> {
        if std::env::var("MONARCH_RDMA_RUN_ISOLATED").is_err() {
            return Ok(());
        }
        const BSIZE: usize = 1024;
        let devices = get_all_devices();
        if devices.len() < 4 {
            println!(
                "skipping this test as it is only configured on H100 nodes with backend network"
            );
            return Ok(());
        }
        let env = DoorbellTestEnv::setup(BSIZE, "cpu:0", "cpu:0").await?;
        let mut qp_1 = request_queue_pair(
            &env.ibv_handle_1,
            &env.client_1,
            env.ibv_actor_2.clone(),
            env.ibv_buffer_1.device_name.clone(),
            env.ibv_buffer_2.device_name.clone(),
        )
        .await?
        .map_err(|e| anyhow::anyhow!(e))?;
        let wr_id = qp_1.enqueue_put(env.ibv_buffer_1.clone(), env.ibv_buffer_2.clone())?;
        qp_1.ring_doorbell()?;
        wait_for_completion(&mut qp_1, PollTarget::Send, &wr_id, 5).await?;

        env.verify_buffers(BSIZE, 0).await?;
        env.cleanup().await?;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_rdma_read_separate_devices_db_check() -> Result<(), anyhow::Error> {
        if std::env::var("MONARCH_RDMA_RUN_ISOLATED").is_err() {
            return Ok(());
        }
        const BSIZE: usize = 1024;
        let devices = get_all_devices();
        if devices.len() < 4 {
            println!(
                "skipping this test as it is only configured on H100 nodes with backend network"
            );
            return Ok(());
        }
        let env = DoorbellTestEnv::setup(BSIZE, "cpu:0", "cpu:1").await?;
        let mut qp_2 = request_queue_pair(
            &env.ibv_handle_2,
            &env.client_2,
            env.ibv_actor_1.clone(),
            env.ibv_buffer_2.device_name.clone(),
            env.ibv_buffer_1.device_name.clone(),
        )
        .await?
        .map_err(|e| anyhow::anyhow!(e))?;
        let wr_id = qp_2.enqueue_get(env.ibv_buffer_2.clone(), env.ibv_buffer_1.clone())?;
        qp_2.ring_doorbell()?;
        wait_for_completion(&mut qp_2, PollTarget::Send, &wr_id, 5).await?;

        env.verify_buffers(BSIZE, 0).await?;
        env.cleanup().await?;
        Ok(())
    }

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
        let env = DoorbellTestEnv::setup(BSIZE, "cuda:0", "cuda:1").await?;
        let mut qp_1 = request_queue_pair(
            &env.ibv_handle_1,
            &env.client_1,
            env.ibv_actor_2.clone(),
            env.ibv_buffer_1.device_name.clone(),
            env.ibv_buffer_2.device_name.clone(),
        )
        .await?
        .map_err(|e| anyhow::anyhow!(e))?;
        qp_1.enqueue_put(env.ibv_buffer_1.clone(), env.ibv_buffer_2.clone())?;
        ring_db_gpu(&qp_1).await?;
        wait_for_completion_gpu(&mut qp_1, PollTarget::Send, 5).await?;

        env.verify_buffers(BSIZE, 0).await?;
        env.cleanup().await?;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_rdma_read_separate_devices_db_device_trigger() -> Result<(), anyhow::Error> {
        if std::env::var("MONARCH_RDMA_RUN_ISOLATED").is_err() {
            return Ok(());
        }
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
        let env = DoorbellTestEnv::setup(BSIZE, "cuda:0", "cuda:1").await?;
        let mut qp_1 = request_queue_pair(
            &env.ibv_handle_1,
            &env.client_1,
            env.ibv_actor_2.clone(),
            env.ibv_buffer_1.device_name.clone(),
            env.ibv_buffer_2.device_name.clone(),
        )
        .await?
        .map_err(|e| anyhow::anyhow!(e))?;
        qp_1.enqueue_get(env.ibv_buffer_1.clone(), env.ibv_buffer_2.clone())?;
        ring_db_gpu(&qp_1).await?;
        wait_for_completion_gpu(&mut qp_1, PollTarget::Send, 5).await?;

        env.verify_buffers(BSIZE, 0).await?;
        env.cleanup().await?;
        Ok(())
    }

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_rdma_write_recv_separate_devices_db_trigger() -> Result<(), anyhow::Error> {
        if std::env::var("MONARCH_RDMA_RUN_ISOLATED").is_err() {
            return Ok(());
        }
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
        let env = DoorbellTestEnv::setup(BSIZE, "cuda:0", "cuda:1").await?;
        let mut qp_1 = request_queue_pair(
            &env.ibv_handle_1,
            &env.client_1,
            env.ibv_actor_2.clone(),
            env.ibv_buffer_1.device_name.clone(),
            env.ibv_buffer_2.device_name.clone(),
        )
        .await?
        .map_err(|e| anyhow::anyhow!(e))?;
        let mut qp_2 = request_queue_pair(
            &env.ibv_handle_2,
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
}
