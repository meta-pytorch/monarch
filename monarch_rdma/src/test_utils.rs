/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::sync::Once;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

/// Cached result of CUDA availability check
static CUDA_AVAILABLE: AtomicBool = AtomicBool::new(false);
static INIT: Once = Once::new();

/// Safely checks if CUDA is available on the system.
///
/// This function attempts to initialize CUDA and determine if it's available.
/// The result is cached after the first call, so subsequent calls are very fast.
///
/// # Returns
///
/// `true` if CUDA is available and can be initialized, `false` otherwise.
///
/// # Examples
///
/// ```
/// use monarch_rdma::is_cuda_available;
///
/// if is_cuda_available() {
///     println!("CUDA is available, can use GPU features");
/// } else {
///     println!("CUDA is not available, falling back to CPU-only mode");
/// }
/// ```
pub fn is_cuda_available() -> bool {
    INIT.call_once(|| {
        let available = check_cuda_available();
        CUDA_AVAILABLE.store(available, Ordering::SeqCst);
    });
    CUDA_AVAILABLE.load(Ordering::SeqCst)
}

/// Internal function that performs the actual CUDA availability check
fn check_cuda_available() -> bool {
    unsafe {
        // Try to initialize CUDA
        let result = rdmaxcel_sys::rdmaxcel_cuInit(0);

        if result != rdmaxcel_sys::CUDA_SUCCESS {
            return false;
        }

        // Check if there are any CUDA devices
        let mut device_count: i32 = 0;
        let count_result = rdmaxcel_sys::rdmaxcel_cuDeviceGetCount(&mut device_count);

        if count_result != rdmaxcel_sys::CUDA_SUCCESS || device_count <= 0 {
            return false;
        }

        // Try to get the first device to verify it's actually accessible
        let mut device: rdmaxcel_sys::CUdevice = std::mem::zeroed();
        let device_result = rdmaxcel_sys::rdmaxcel_cuDeviceGet(&mut device, 0);

        if device_result != rdmaxcel_sys::CUDA_SUCCESS {
            return false;
        }

        true
    }
}

#[cfg(test)]
pub mod test_utils {
    use std::time::Duration;
    use std::time::Instant;

    use hyperactor::Actor;
    use hyperactor::ActorRef;
    use hyperactor::Context;
    use hyperactor::HandleClient;
    use hyperactor::Handler;
    use hyperactor::RefClient;
    use hyperactor::RemoteSpawn;
    use hyperactor::channel::ChannelTransport;
    use hyperactor::clock::Clock;
    use hyperactor::clock::RealClock;
    use hyperactor_config::Attrs;
    use hyperactor_mesh::ActorMesh;
    use hyperactor_mesh::Mesh;
    use hyperactor_mesh::ProcMesh;
    use hyperactor_mesh::alloc::AllocSpec;
    use hyperactor_mesh::alloc::Allocator;
    use hyperactor_mesh::alloc::LocalAllocator;
    use hyperactor_mesh::global_root_client;
    use ndslice::View;
    use ndslice::extent;

    use crate::IbverbsConfig;
    use crate::RdmaBuffer;
    use crate::rdma_components::PollTarget;
    use crate::rdma_components::RdmaQueuePair;
    use crate::rdma_manager_actor::RdmaManagerActor;
    use crate::rdma_manager_actor::RdmaManagerMessageClient;
    use crate::validate_execution_context;

    #[derive(Debug)]
    struct SendSyncCudaContext(rdmaxcel_sys::CUcontext);
    unsafe impl Send for SendSyncCudaContext {}
    unsafe impl Sync for SendSyncCudaContext {}

    /// Actor responsible for CUDA initialization and buffer management within its own process context.  
    /// This is important because you preform CUDA operations within the same process as the RDMA operations.  
    #[hyperactor::export(
        spawn = true,
        handlers = [
            CudaActorMessage,
        ],
    )]
    #[derive(Debug)]
    pub struct CudaActor {
        device: Option<i32>,
        context: SendSyncCudaContext,
    }

    impl Actor for CudaActor {}

    #[async_trait::async_trait]
    impl RemoteSpawn for CudaActor {
        type Params = i32;

        async fn new(device_id: i32, _environment: Attrs) -> Result<Self, anyhow::Error> {
            unsafe {
                cu_check!(rdmaxcel_sys::rdmaxcel_cuInit(0));
                let mut device: rdmaxcel_sys::CUdevice = std::mem::zeroed();
                cu_check!(rdmaxcel_sys::rdmaxcel_cuDeviceGet(&mut device, device_id));
                let mut context: rdmaxcel_sys::CUcontext = std::mem::zeroed();
                cu_check!(rdmaxcel_sys::rdmaxcel_cuCtxCreate_v2(
                    &mut context,
                    0,
                    device_id
                ));

                Ok(Self {
                    device: Some(device),
                    context: SendSyncCudaContext(context),
                })
            }
        }
    }

    #[derive(
        Handler,
        HandleClient,
        RefClient,
        typeuri::Named,
        serde::Serialize,
        serde::Deserialize,
        Debug
    )]
    pub enum CudaActorMessage {
        CreateBuffer {
            size: usize,
            rdma_actor: ActorRef<RdmaManagerActor>,
            #[reply]
            reply: hyperactor::OncePortRef<(RdmaBuffer, usize)>,
        },
        FillBuffer {
            device_ptr: usize,
            size: usize,
            value: u8,
            #[reply]
            reply: hyperactor::OncePortRef<()>,
        },
        VerifyBuffer {
            cpu_buffer_ptr: usize,
            device_ptr: usize,
            size: usize,
            #[reply]
            reply: hyperactor::OncePortRef<()>,
        },
    }

    #[async_trait::async_trait]
    impl Handler<CudaActorMessage> for CudaActor {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            msg: CudaActorMessage,
        ) -> Result<(), anyhow::Error> {
            match msg {
                CudaActorMessage::CreateBuffer {
                    size,
                    rdma_actor,
                    reply,
                } => {
                    let device = self
                        .device
                        .ok_or_else(|| anyhow::anyhow!("Device not initialized"))?;

                    let (dptr, padded_size) = unsafe {
                        cu_check!(rdmaxcel_sys::rdmaxcel_cuCtxSetCurrent(self.context.0));

                        let mut dptr: rdmaxcel_sys::CUdeviceptr = std::mem::zeroed();
                        let mut handle: rdmaxcel_sys::CUmemGenericAllocationHandle =
                            std::mem::zeroed();

                        let mut granularity: usize = 0;
                        let mut prop: rdmaxcel_sys::CUmemAllocationProp = std::mem::zeroed();
                        prop.type_ = rdmaxcel_sys::CU_MEM_ALLOCATION_TYPE_PINNED;
                        prop.location.type_ = rdmaxcel_sys::CU_MEM_LOCATION_TYPE_DEVICE;
                        prop.location.id = device;
                        prop.allocFlags.gpuDirectRDMACapable = 1;
                        prop.requestedHandleTypes =
                            rdmaxcel_sys::CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR;

                        cu_check!(rdmaxcel_sys::rdmaxcel_cuMemGetAllocationGranularity(
                            &mut granularity as *mut usize,
                            &prop,
                            rdmaxcel_sys::CU_MEM_ALLOC_GRANULARITY_MINIMUM,
                        ));

                        let padded_size: usize = ((size - 1) / granularity + 1) * granularity;

                        cu_check!(rdmaxcel_sys::rdmaxcel_cuMemCreate(
                            &mut handle,
                            padded_size,
                            &prop,
                            0
                        ));

                        cu_check!(rdmaxcel_sys::rdmaxcel_cuMemAddressReserve(
                            &mut dptr,
                            padded_size,
                            0,
                            0,
                            0,
                        ));

                        assert!((dptr as usize).is_multiple_of(granularity));
                        assert!(padded_size.is_multiple_of(granularity));

                        let err = rdmaxcel_sys::rdmaxcel_cuMemMap(dptr, padded_size, 0, handle, 0);
                        if err != rdmaxcel_sys::CUDA_SUCCESS {
                            return Err(anyhow::anyhow!("Failed to map CUDA memory: {:?}", err));
                        }

                        let mut access_desc: rdmaxcel_sys::CUmemAccessDesc = std::mem::zeroed();
                        access_desc.location.type_ = rdmaxcel_sys::CU_MEM_LOCATION_TYPE_DEVICE;
                        access_desc.location.id = device;
                        access_desc.flags = rdmaxcel_sys::CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
                        cu_check!(rdmaxcel_sys::rdmaxcel_cuMemSetAccess(
                            dptr,
                            padded_size,
                            &access_desc,
                            1
                        ));

                        (dptr, padded_size)
                    };

                    let rdma_handle = rdma_actor
                        .request_buffer(cx, dptr as usize, padded_size)
                        .await?;

                    reply.send(cx, (rdma_handle, dptr as usize))?;
                    Ok(())
                }
                CudaActorMessage::FillBuffer {
                    device_ptr,
                    size,
                    value,
                    reply,
                } => {
                    unsafe {
                        cu_check!(rdmaxcel_sys::rdmaxcel_cuCtxSetCurrent(self.context.0));

                        cu_check!(rdmaxcel_sys::rdmaxcel_cuMemsetD8_v2(
                            device_ptr as rdmaxcel_sys::CUdeviceptr,
                            value,
                            size
                        ));
                    }

                    reply.send(cx, ())?;
                    Ok(())
                }
                CudaActorMessage::VerifyBuffer {
                    cpu_buffer_ptr,
                    device_ptr,
                    size,
                    reply,
                } => {
                    unsafe {
                        cu_check!(rdmaxcel_sys::rdmaxcel_cuCtxSetCurrent(self.context.0));

                        cu_check!(rdmaxcel_sys::rdmaxcel_cuMemcpyDtoH_v2(
                            cpu_buffer_ptr as *mut std::ffi::c_void,
                            device_ptr as rdmaxcel_sys::CUdeviceptr,
                            size
                        ));
                    }

                    reply.send(cx, ())?;
                    Ok(())
                }
            }
        }
    }

    /// Waits for the completion of RDMA operations.
    ///
    /// This function polls for the completion of RDMA operations by repeatedly
    /// checking the completion queue until all expected work requests complete
    /// or the specified timeout is reached.
    ///
    /// # Arguments
    /// * `qp` - The RDMA Queue Pair to poll for completion
    /// * `poll_target` - Which CQ to poll (Send or Recv)
    /// * `expected_wr_ids` - Slice of work request IDs to wait for
    /// * `timeout_secs` - Timeout in seconds
    ///
    /// # Returns
    /// `Ok(true)` if all operations complete successfully within the timeout,
    /// or an error if the timeout is reached
    pub async fn wait_for_completion(
        qp: &mut RdmaQueuePair,
        poll_target: PollTarget,
        expected_wr_ids: &[u64],
        timeout_secs: u64,
    ) -> Result<bool, anyhow::Error> {
        let timeout = Duration::from_secs(timeout_secs);
        let start_time = Instant::now();

        let mut remaining: std::collections::HashSet<u64> =
            expected_wr_ids.iter().copied().collect();

        while start_time.elapsed() < timeout {
            if remaining.is_empty() {
                return Ok(true);
            }

            let wr_ids_to_poll: Vec<u64> = remaining.iter().copied().collect();
            match qp.poll_completion(poll_target, &wr_ids_to_poll) {
                Ok(completions) => {
                    for (wr_id, _wc) in completions {
                        remaining.remove(&wr_id);
                    }
                    if remaining.is_empty() {
                        return Ok(true);
                    }
                    RealClock.sleep(Duration::from_millis(1)).await;
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(e));
                }
            }
        }
        Err(anyhow::Error::msg(format!(
            "Timeout while waiting for completion of wr_ids: {:?}",
            remaining
        )))
    }

    /// Posts a work request to the send queue of the given RDMA queue pair.
    pub async fn send_wqe_gpu(
        qp: &mut RdmaQueuePair,
        lhandle: &RdmaBuffer,
        rhandle: &RdmaBuffer,
        op_type: u32,
    ) -> Result<(), anyhow::Error> {
        unsafe {
            let ibv_qp = qp.qp as *mut rdmaxcel_sys::rdmaxcel_qp;
            let dv_qp = qp.dv_qp as *mut rdmaxcel_sys::mlx5dv_qp;
            let send_wqe_idx = rdmaxcel_sys::rdmaxcel_qp_load_send_wqe_idx(ibv_qp);
            let params = rdmaxcel_sys::wqe_params_t {
                laddr: lhandle.addr,
                length: lhandle.size,
                lkey: lhandle.lkey,
                wr_id: send_wqe_idx,
                signaled: true,
                op_type,
                raddr: rhandle.addr,
                rkey: rhandle.rkey,
                qp_num: (*(*ibv_qp).ibv_qp).qp_num,
                buf: (*dv_qp).sq.buf as *mut u8,
                wqe_cnt: (*dv_qp).sq.wqe_cnt,
                dbrec: (*dv_qp).dbrec,
                ..Default::default()
            };
            rdmaxcel_sys::launch_send_wqe(params);
            rdmaxcel_sys::rdmaxcel_qp_fetch_add_send_wqe_idx(ibv_qp);
        }
        Ok(())
    }

    /// Posts a work request to the receive queue of the given RDMA queue pair.
    pub async fn recv_wqe_gpu(
        qp: &mut RdmaQueuePair,
        lhandle: &RdmaBuffer,
        _rhandle: &RdmaBuffer,
        op_type: u32,
    ) -> Result<(), anyhow::Error> {
        // Populate params using lhandle and rhandle
        unsafe {
            let rdmaxcel_qp = qp.qp as *mut rdmaxcel_sys::rdmaxcel_qp;
            let dv_qp = qp.dv_qp as *mut rdmaxcel_sys::mlx5dv_qp;
            let recv_wqe_idx = rdmaxcel_sys::rdmaxcel_qp_load_recv_wqe_idx(rdmaxcel_qp);
            let params = rdmaxcel_sys::wqe_params_t {
                laddr: lhandle.addr,
                length: lhandle.size,
                lkey: lhandle.lkey,
                wr_id: recv_wqe_idx,
                op_type,
                signaled: true,
                qp_num: (*(*rdmaxcel_qp).ibv_qp).qp_num,
                buf: (*dv_qp).rq.buf as *mut u8,
                wqe_cnt: (*dv_qp).rq.wqe_cnt,
                dbrec: (*dv_qp).dbrec,
                ..Default::default()
            };
            rdmaxcel_sys::launch_recv_wqe(params);
            rdmaxcel_sys::rdmaxcel_qp_fetch_add_recv_wqe_idx(rdmaxcel_qp);
            rdmaxcel_sys::rdmaxcel_qp_fetch_add_recv_db_idx(rdmaxcel_qp);
        }
        Ok(())
    }

    pub async fn ring_db_gpu(qp: &RdmaQueuePair) -> Result<(), anyhow::Error> {
        RealClock.sleep(Duration::from_millis(2)).await;
        unsafe {
            let dv_qp = qp.dv_qp as *mut rdmaxcel_sys::mlx5dv_qp;
            let base_ptr = (*dv_qp).sq.buf as *mut u8;
            let wqe_cnt = (*dv_qp).sq.wqe_cnt;
            let stride = (*dv_qp).sq.stride;
            let send_wqe_idx = rdmaxcel_sys::rdmaxcel_qp_load_send_wqe_idx(
                qp.qp as *mut rdmaxcel_sys::rdmaxcel_qp,
            );
            let mut send_db_idx =
                rdmaxcel_sys::rdmaxcel_qp_load_send_db_idx(qp.qp as *mut rdmaxcel_sys::rdmaxcel_qp);
            if (wqe_cnt as u64) < (send_wqe_idx - send_db_idx) {
                return Err(anyhow::anyhow!("Overflow of WQE, possible data loss"));
            }
            while send_db_idx < send_wqe_idx {
                let offset = (send_db_idx % wqe_cnt as u64) * stride as u64;
                let src_ptr = (base_ptr as *mut u8).wrapping_add(offset as usize);
                rdmaxcel_sys::launch_db_ring((*dv_qp).bf.reg, src_ptr as *mut std::ffi::c_void);
                send_db_idx += 1;
                rdmaxcel_sys::rdmaxcel_qp_store_send_db_idx(
                    qp.qp as *mut rdmaxcel_sys::rdmaxcel_qp,
                    send_db_idx,
                );
            }
        }
        Ok(())
    }

    /// Wait for completion on a specific completion queue
    pub async fn wait_for_completion_gpu(
        qp: &mut RdmaQueuePair,
        poll_target: PollTarget,
        timeout_secs: u64,
    ) -> Result<bool, anyhow::Error> {
        unsafe {
            let start_time = Instant::now();
            let timeout = Duration::from_secs(timeout_secs);
            let ibv_qp = qp.qp as *mut rdmaxcel_sys::rdmaxcel_qp;

            while start_time.elapsed() < timeout {
                // Get the appropriate completion queue and index based on the poll target
                let (cq, idx, cq_type_str) = match poll_target {
                    PollTarget::Send => (
                        qp.dv_send_cq as *mut rdmaxcel_sys::mlx5dv_cq,
                        rdmaxcel_sys::rdmaxcel_qp_load_send_cq_idx(ibv_qp),
                        "send",
                    ),
                    PollTarget::Recv => (
                        qp.dv_recv_cq as *mut rdmaxcel_sys::mlx5dv_cq,
                        rdmaxcel_sys::rdmaxcel_qp_load_recv_cq_idx(ibv_qp),
                        "receive",
                    ),
                };

                // Poll the completion queue
                let result = rdmaxcel_sys::launch_cqe_poll(cq as *mut std::ffi::c_void, idx as i32);

                match result {
                    rdmaxcel_sys::CQE_POLL_TRUE => {
                        // Update the appropriate index based on the poll target
                        match poll_target {
                            PollTarget::Send => {
                                rdmaxcel_sys::rdmaxcel_qp_fetch_add_send_cq_idx(ibv_qp);
                            }
                            PollTarget::Recv => {
                                rdmaxcel_sys::rdmaxcel_qp_fetch_add_recv_cq_idx(ibv_qp);
                            }
                        }
                        return Ok(true);
                    }
                    rdmaxcel_sys::CQE_POLL_ERROR => {
                        return Err(anyhow::anyhow!("Error polling {} completion", cq_type_str));
                    }
                    _ => {
                        // No completion yet, sleep and try again
                        RealClock.sleep(Duration::from_millis(1)).await;
                    }
                }
            }

            Err(anyhow::Error::msg("Timeout while waiting for completion"))
        }
    }

    pub struct RdmaManagerTestEnv {
        buffer_1: Buffer,
        buffer_2: Buffer,
        // Note: Both point to the same global instance
        pub client_1: &'static hyperactor::Instance<hyperactor_mesh::GlobalClientActor>,
        pub client_2: &'static hyperactor::Instance<hyperactor_mesh::GlobalClientActor>,
        pub actor_1: ActorRef<RdmaManagerActor>,
        pub actor_2: ActorRef<RdmaManagerActor>,
        pub rdma_handle_1: RdmaBuffer,
        pub rdma_handle_2: RdmaBuffer,
        cuda_actor_1: Option<ActorRef<CudaActor>>,
        cuda_actor_2: Option<ActorRef<CudaActor>>,
        device_ptr_1: Option<usize>,
        device_ptr_2: Option<usize>,
    }

    #[derive(Debug, Clone)]
    pub struct Buffer {
        ptr: u64,
        len: usize,
        #[allow(dead_code)]
        cpu_ref: Option<Box<[u8]>>,
    }
    /// Helper function to parse accelerator strings
    async fn parse_accel(accel: &str, config: &mut IbverbsConfig) -> (String, usize) {
        let (backend, idx) = accel.split_once(':').unwrap();
        let parsed_idx = idx.parse::<usize>().unwrap();

        if backend == "cuda" {
            config.use_gpu_direct = validate_execution_context().await.is_ok();
            eprintln!("Using GPU Direct: {}", config.use_gpu_direct);
        }

        (backend.to_string(), parsed_idx)
    }

    impl RdmaManagerTestEnv {
        /// Sets up the RDMA test environment with a specified QP type.
        ///
        /// This function initializes the RDMA test environment by setting up two actor meshes
        /// with their respective RDMA configurations. It also prepares two buffers for testing
        /// RDMA operations and fills the first buffer with test data.
        ///
        /// # Arguments
        ///
        /// * `buffer_size` - The size of the buffers to be used in the test.
        /// * `accel1` - Accelerator for first actor (e.g., "cpu:0", "cuda:0")
        /// * `accel2` - Accelerator for second actor (e.g., "cpu:0", "cuda:1")
        /// * `qp_type` - The queue pair type to use (Auto, Standard, or Mlx5dv)
        pub async fn setup_with_qp_type(
            buffer_size: usize,
            accel1: &str,
            accel2: &str,
            qp_type: crate::ibverbs_primitives::RdmaQpType,
        ) -> Result<Self, anyhow::Error> {
            // Use device selection logic to find optimal RDMA devices
            let mut config1 = IbverbsConfig::targeting(accel1);
            let mut config2 = IbverbsConfig::targeting(accel2);

            // Set the QP type
            config1.qp_type = qp_type;
            config2.qp_type = qp_type;

            let parsed_accel1 = parse_accel(accel1, &mut config1).await;
            let parsed_accel2 = parse_accel(accel2, &mut config2).await;

            let alloc_1 = LocalAllocator
                .allocate(AllocSpec {
                    extent: extent! { proc = 1 },
                    constraints: Default::default(),
                    proc_name: None,
                    transport: ChannelTransport::Local,
                    proc_allocation_mode: Default::default(),
                })
                .await
                .unwrap();

            let instance = global_root_client();

            let proc_mesh_1 = Box::leak(Box::new(
                ProcMesh::allocate(instance, Box::new(alloc_1), "mesh1")
                    .await
                    .unwrap(),
            ));
            let actor_mesh_1: ActorMesh<RdmaManagerActor> = proc_mesh_1
                .spawn(instance, "rdma_manager", &Some(config1))
                .await
                .unwrap();

            let alloc_2 = LocalAllocator
                .allocate(AllocSpec {
                    extent: extent! { proc = 1 },
                    constraints: Default::default(),
                    proc_name: None,
                    transport: ChannelTransport::Local,
                    proc_allocation_mode: Default::default(),
                })
                .await
                .unwrap();

            let proc_mesh_2 = Box::leak(Box::new(
                ProcMesh::allocate(instance, Box::new(alloc_2), "mesh2")
                    .await
                    .unwrap(),
            ));
            let actor_mesh_2: ActorMesh<RdmaManagerActor> = proc_mesh_2
                .spawn(instance, "rdma_manager", &Some(config2))
                .await
                .unwrap();

            let actor_1 = actor_mesh_1.get(0).unwrap();
            let actor_2 = actor_mesh_2.get(0).unwrap();

            let mut buf_vec = Vec::new();
            let mut cuda_actor_1 = None;
            let mut cuda_actor_2 = None;
            let mut device_ptr_1: Option<usize> = None;
            let mut device_ptr_2: Option<usize> = None;

            let rdma_handle_1;
            let rdma_handle_2;

            // Process first accelerator
            if parsed_accel1.0 == "cpu" {
                let mut buffer = vec![0u8; buffer_size].into_boxed_slice();
                buf_vec.push(Buffer {
                    ptr: buffer.as_mut_ptr() as u64,
                    len: buffer.len(),
                    cpu_ref: Some(buffer),
                });
                rdma_handle_1 = actor_1
                    .request_buffer(instance, buf_vec[0].ptr as usize, buffer_size)
                    .await?;
            } else {
                // CUDA case - spawn CudaActor in the same process mesh
                let cuda_actor_mesh_1: ActorMesh<CudaActor> = proc_mesh_1
                    .spawn(instance, "cuda_init", &(parsed_accel1.1 as i32))
                    .await?;
                let cuda_actor_ref_1 = cuda_actor_mesh_1.get(0).unwrap();

                let (rdma_buf, dev_ptr) = cuda_actor_ref_1
                    .create_buffer(instance, buffer_size, actor_1.clone())
                    .await?;
                rdma_handle_1 = rdma_buf;
                device_ptr_1 = Some(dev_ptr);

                buf_vec.push(Buffer {
                    ptr: rdma_handle_1.addr as u64,
                    len: buffer_size,
                    cpu_ref: None,
                });
                cuda_actor_1 = Some(cuda_actor_ref_1);
            }

            // Process second accelerator
            if parsed_accel2.0 == "cpu" {
                let mut buffer = vec![0u8; buffer_size].into_boxed_slice();
                buf_vec.push(Buffer {
                    ptr: buffer.as_mut_ptr() as u64,
                    len: buffer.len(),
                    cpu_ref: Some(buffer),
                });
                rdma_handle_2 = actor_2
                    .request_buffer(instance, buf_vec[1].ptr as usize, buffer_size)
                    .await?;
            } else {
                // CUDA case - spawn CudaActor in the same process mesh
                let cuda_actor_mesh_2: ActorMesh<CudaActor> = proc_mesh_2
                    .spawn(instance, "cuda_init", &(parsed_accel2.1 as i32))
                    .await?;
                let cuda_actor_ref_2 = cuda_actor_mesh_2.get(0).unwrap();

                let (rdma_buf, dev_ptr) = cuda_actor_ref_2
                    .create_buffer(instance, buffer_size, actor_2.clone())
                    .await?;
                rdma_handle_2 = rdma_buf;
                device_ptr_2 = Some(dev_ptr);

                buf_vec.push(Buffer {
                    ptr: rdma_handle_2.addr as u64,
                    len: buffer_size,
                    cpu_ref: None,
                });
                cuda_actor_2 = Some(cuda_actor_ref_2);
            }

            // Fill buffer1 with test data
            if parsed_accel1.0 == "cuda" {
                cuda_actor_1
                    .clone()
                    .unwrap()
                    .fill_buffer(instance, device_ptr_1.unwrap(), buffer_size, 42)
                    .await?;
            } else {
                unsafe {
                    let ptr = buf_vec[0].ptr as *mut u8;
                    for i in 0..buf_vec[0].len {
                        *ptr.add(i) = 42_u8;
                    }
                }
            }

            let buffer_2 = buf_vec.remove(1);
            let buffer_1 = buf_vec.remove(0);

            Ok(Self {
                buffer_1,
                buffer_2,
                client_1: instance,
                client_2: instance,
                actor_1,
                actor_2,
                rdma_handle_1,
                rdma_handle_2,
                cuda_actor_1,
                cuda_actor_2,
                device_ptr_1,
                device_ptr_2,
            })
        }

        pub async fn cleanup(self) -> Result<(), anyhow::Error> {
            // Just release buffers from RDMA manager - CUDA cleanup happens automatically
            self.actor_1
                .release_buffer(self.client_1, self.rdma_handle_1.clone())
                .await?;

            self.actor_2
                .release_buffer(self.client_2, self.rdma_handle_2.clone())
                .await?;
            Ok(())
        }

        /// Sets up the RDMA test environment with auto-detected QP type.
        ///
        /// This is a convenience wrapper around `setup_with_qp_type` that uses
        /// `RdmaQpType::Auto` to automatically select the appropriate QP type.
        ///
        /// # Arguments
        ///
        /// * `buffer_size` - The size of the buffers to be used in the test.
        /// * `accel1` - Accelerator for first actor (e.g., "cpu:0", "cuda:0")
        /// * `accel2` - Accelerator for second actor (e.g., "cpu:0", "cuda:1")
        pub async fn setup(
            buffer_size: usize,
            accel1: &str,
            accel2: &str,
        ) -> Result<Self, anyhow::Error> {
            Self::setup_with_qp_type(
                buffer_size,
                accel1,
                accel2,
                crate::ibverbs_primitives::RdmaQpType::Auto,
            )
            .await
        }

        pub async fn verify_buffers(
            &self,
            size: usize,
            offset: usize,
        ) -> Result<(), anyhow::Error> {
            let mut temp_buffer_1 = vec![0u8; size];
            let mut temp_buffer_2 = vec![0u8; size];

            // Read buffer 1
            if let Some(cuda_actor) = &self.cuda_actor_1 {
                cuda_actor
                    .verify_buffer(
                        self.client_1,
                        temp_buffer_1.as_mut_ptr() as usize,
                        self.device_ptr_1.unwrap() + offset,
                        size,
                    )
                    .await?;
            } else {
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        (self.buffer_1.ptr + offset as u64) as *const u8,
                        temp_buffer_1.as_mut_ptr(),
                        size,
                    );
                }
            }

            // Read buffer 2
            if let Some(cuda_actor) = &self.cuda_actor_2 {
                cuda_actor
                    .verify_buffer(
                        self.client_2,
                        temp_buffer_2.as_mut_ptr() as usize,
                        self.device_ptr_2.unwrap() + offset,
                        size,
                    )
                    .await?;
            } else {
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        (self.buffer_2.ptr + offset as u64) as *const u8,
                        temp_buffer_2.as_mut_ptr(),
                        size,
                    );
                }
            }

            // Compare buffers
            for i in 0..size {
                if temp_buffer_1[i] != temp_buffer_2[i] {
                    return Err(anyhow::anyhow!(
                        "Buffers are not equal at index {}",
                        offset + i
                    ));
                }
            }
            Ok(())
        }
    }
}
