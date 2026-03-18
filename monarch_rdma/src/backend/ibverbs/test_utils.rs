/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use hyperactor::Actor;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::Proc;
use hyperactor::RefClient;
use hyperactor::RemoteSpawn;
use hyperactor::channel::ChannelAddr;
use hyperactor::reference;
use hyperactor_config::Flattrs;

use super::IbvBuffer;
use super::manager_actor::IbvManagerActor;
use super::manager_actor::IbvManagerMessageClient;
use super::queue_pair::IbvQueuePair;
use super::queue_pair::PollTarget;
use crate::IbvConfig;
use crate::RdmaManagerMessageClient;
use crate::RdmaRemoteBuffer;
use crate::local_memory::RdmaLocalMemory;
use crate::local_memory::UnsafeLocalMemory;
use crate::rdma_manager_actor::RdmaManagerActor;
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

    async fn new(device_id: i32, _environment: Flattrs) -> Result<Self, anyhow::Error> {
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
        rdma_actor: reference::ActorRef<RdmaManagerActor>,
        #[reply]
        reply: reference::OncePortRef<(RdmaRemoteBuffer, usize)>,
    },
    FillBuffer {
        device_ptr: usize,
        size: usize,
        value: u8,
        #[reply]
        reply: reference::OncePortRef<()>,
    },
    VerifyBuffer {
        cpu_buffer_ptr: usize,
        device_ptr: usize,
        size: usize,
        #[reply]
        reply: reference::OncePortRef<()>,
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
                    let mut handle: rdmaxcel_sys::CUmemGenericAllocationHandle = std::mem::zeroed();

                    let mut granularity: usize = 0;
                    let mut prop: rdmaxcel_sys::CUmemAllocationProp = std::mem::zeroed();
                    prop.type_ = rdmaxcel_sys::CU_MEM_ALLOCATION_TYPE_PINNED;
                    prop.location.type_ = rdmaxcel_sys::CU_MEM_LOCATION_TYPE_DEVICE;
                    prop.location.id = device;
                    prop.allocFlags.gpuDirectRDMACapable = 1;
                    // ROCm bindgen generates a different struct layout with anonymous union
                    #[cfg(feature = "rocm")]
                    {
                        prop.__bindgen_anon_1.requestedHandleTypes =
                            rdmaxcel_sys::CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR;
                    }
                    #[cfg(not(feature = "rocm"))]
                    {
                        prop.requestedHandleTypes =
                            rdmaxcel_sys::CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR;
                    }

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

                    (dptr as usize, padded_size)
                };

                // Register via RdmaManagerActor request_buffer; the ibverbs MR
                // will be registered lazily by resolve_ibv().
                let local_memory: Arc<dyn RdmaLocalMemory> =
                    Arc::new(UnsafeLocalMemory::new(dptr as usize, padded_size));
                let handle = rdma_actor
                    .downcast_handle(cx)
                    .ok_or_else(|| anyhow::anyhow!("failed to get handle"))?;
                let rdma_handle = handle.request_buffer(cx, local_memory).await?;

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
    qp: &mut IbvQueuePair,
    poll_target: PollTarget,
    expected_wr_ids: &[u64],
    timeout_secs: u64,
) -> Result<bool, anyhow::Error> {
    let timeout = Duration::from_secs(timeout_secs);
    let start_time = Instant::now();

    let mut remaining: std::collections::HashSet<u64> = expected_wr_ids.iter().copied().collect();

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
                tokio::time::sleep(Duration::from_millis(1)).await;
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
    qp: &mut IbvQueuePair,
    lhandle: &IbvBuffer,
    rhandle: &IbvBuffer,
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
    qp: &mut IbvQueuePair,
    lhandle: &IbvBuffer,
    _rhandle: &IbvBuffer,
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

pub async fn ring_db_gpu(qp: &IbvQueuePair) -> Result<(), anyhow::Error> {
    tokio::time::sleep(Duration::from_millis(2)).await;
    unsafe {
        let dv_qp = qp.dv_qp as *mut rdmaxcel_sys::mlx5dv_qp;
        let base_ptr = (*dv_qp).sq.buf as *mut u8;
        let wqe_cnt = (*dv_qp).sq.wqe_cnt;
        let stride = (*dv_qp).sq.stride;
        let send_wqe_idx =
            rdmaxcel_sys::rdmaxcel_qp_load_send_wqe_idx(qp.qp as *mut rdmaxcel_sys::rdmaxcel_qp);
        let mut send_db_idx =
            rdmaxcel_sys::rdmaxcel_qp_load_send_db_idx(qp.qp as *mut rdmaxcel_sys::rdmaxcel_qp);
        if (wqe_cnt as u64) < (send_wqe_idx - send_db_idx) {
            return Err(anyhow::anyhow!("Overflow of WQE, possible data loss"));
        }
        while send_db_idx < send_wqe_idx {
            let offset = (send_db_idx % wqe_cnt as u64) * stride as u64;
            let src_ptr = base_ptr.wrapping_add(offset as usize);
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
    qp: &mut IbvQueuePair,
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
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        }

        Err(anyhow::Error::msg("Timeout while waiting for completion"))
    }
}

#[allow(dead_code)]
pub struct IbvTestEnv {
    buffer_1: Buffer,
    buffer_2: Buffer,
    pub client_1: Instance<()>,
    pub client_2: Instance<()>,
    pub actor_1: reference::ActorRef<RdmaManagerActor>,
    pub actor_2: reference::ActorRef<RdmaManagerActor>,
    pub ibv_actor_1: reference::ActorRef<IbvManagerActor>,
    pub ibv_actor_2: reference::ActorRef<IbvManagerActor>,
    pub rdma_handle_1: RdmaRemoteBuffer,
    pub rdma_handle_2: RdmaRemoteBuffer,
    pub local_memory_1: Arc<dyn RdmaLocalMemory>,
    pub local_memory_2: Arc<dyn RdmaLocalMemory>,
    pub ibv_buffer_1: IbvBuffer,
    pub ibv_buffer_2: IbvBuffer,
    cuda_actor_1: Option<reference::ActorRef<CudaActor>>,
    cuda_actor_2: Option<reference::ActorRef<CudaActor>>,
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
async fn parse_accel(accel: &str, config: &mut IbvConfig) -> (String, usize) {
    let (backend, idx) = accel.split_once(':').unwrap();
    let parsed_idx = idx.parse::<usize>().unwrap();

    if backend == "cuda" {
        config.use_gpu_direct = validate_execution_context().await.is_ok();
        eprintln!("Using GPU Direct: {}", config.use_gpu_direct);
    }

    (backend.to_string(), parsed_idx)
}

impl IbvTestEnv {
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
        qp_type: super::primitives::IbvQpType,
    ) -> Result<Self, anyhow::Error> {
        // Use device selection logic to find optimal RDMA devices
        let mut config1 = IbvConfig::targeting(accel1);
        let mut config2 = IbvConfig::targeting(accel2);

        // Set the QP type
        config1.qp_type = qp_type;
        config2.qp_type = qp_type;

        let parsed_accel1 = parse_accel(accel1, &mut config1).await;
        let parsed_accel2 = parse_accel(accel2, &mut config2).await;

        // Unique proc names so both can have an actor named "rdma_manager".
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);

        // Create separate procs so each RdmaManagerActor lives in its
        // own proc (matching production layout). Using Proc::direct
        // gives us ActorHandles for local-only request_buffer calls.
        let proc_1 = Proc::direct(
            ChannelAddr::any(hyperactor::channel::ChannelTransport::Unix),
            format!("rdma_test_{id}_a"),
        )?;
        let proc_2 = Proc::direct(
            ChannelAddr::any(hyperactor::channel::ChannelTransport::Unix),
            format!("rdma_test_{id}_b"),
        )?;

        let (instance_1, _client_handle_1) = proc_1.instance("client")?;
        let (instance_2, _client_handle_2) = proc_2.instance("client")?;

        let rdma_actor_1 = RdmaManagerActor::new(Some(config1), Flattrs::default()).await?;
        let rdma_actor_handle_1 = proc_1.spawn("rdma_manager", rdma_actor_1)?;
        let actor_1: reference::ActorRef<RdmaManagerActor> = rdma_actor_handle_1.bind();

        let rdma_actor_2 = RdmaManagerActor::new(Some(config2), Flattrs::default()).await?;
        let rdma_actor_handle_2 = proc_2.spawn("rdma_manager", rdma_actor_2)?;
        let actor_2: reference::ActorRef<RdmaManagerActor> = rdma_actor_handle_2.bind();

        let mut buf_vec = Vec::new();
        let mut cuda_actor_1 = None;
        let mut cuda_actor_2 = None;
        let mut device_ptr_1: Option<usize> = None;
        let mut device_ptr_2: Option<usize> = None;

        let rdma_handle_1;
        let rdma_handle_2;
        let local_memory_1: Arc<dyn RdmaLocalMemory>;
        let local_memory_2: Arc<dyn RdmaLocalMemory>;

        // Process first accelerator
        if parsed_accel1.0 == "cpu" {
            let mut buffer = vec![0u8; buffer_size].into_boxed_slice();
            let ptr = buffer.as_mut_ptr() as u64;
            buf_vec.push(Buffer {
                ptr,
                len: buffer.len(),
                cpu_ref: Some(buffer),
            });
            local_memory_1 = Arc::new(UnsafeLocalMemory::new(ptr as usize, buffer_size));
            let handle_1 = actor_1
                .downcast_handle(&instance_1)
                .ok_or_else(|| anyhow::anyhow!("failed to get handle"))?;
            rdma_handle_1 = handle_1
                .request_buffer(&instance_1, local_memory_1.clone())
                .await?;
        } else {
            // CUDA case - spawn CudaActor on the same proc
            let cuda_actor = CudaActor::new(parsed_accel1.1 as i32, Flattrs::default()).await?;
            let cuda_handle = proc_1.spawn("cuda_init", cuda_actor)?;
            let cuda_actor_ref_1: reference::ActorRef<CudaActor> = cuda_handle.bind();

            let (rdma_buf, dev_ptr) = cuda_actor_ref_1
                .create_buffer(&instance_1, buffer_size, actor_1.clone())
                .await?;
            rdma_handle_1 = rdma_buf;
            device_ptr_1 = Some(dev_ptr);
            local_memory_1 = Arc::new(UnsafeLocalMemory::new(dev_ptr, buffer_size));

            buf_vec.push(Buffer {
                ptr: dev_ptr as u64,
                len: buffer_size,
                cpu_ref: None,
            });
            cuda_actor_1 = Some(cuda_actor_ref_1);
        }

        // Process second accelerator
        if parsed_accel2.0 == "cpu" {
            let mut buffer = vec![0u8; buffer_size].into_boxed_slice();
            let ptr = buffer.as_mut_ptr() as u64;
            buf_vec.push(Buffer {
                ptr,
                len: buffer.len(),
                cpu_ref: Some(buffer),
            });
            local_memory_2 = Arc::new(UnsafeLocalMemory::new(ptr as usize, buffer_size));
            let handle_2 = actor_2
                .downcast_handle(&instance_2)
                .ok_or_else(|| anyhow::anyhow!("failed to get handle"))?;
            rdma_handle_2 = handle_2
                .request_buffer(&instance_2, local_memory_2.clone())
                .await?;
        } else {
            // CUDA case - spawn CudaActor on the same proc
            let cuda_actor = CudaActor::new(parsed_accel2.1 as i32, Flattrs::default()).await?;
            let cuda_handle = proc_2.spawn("cuda_init", cuda_actor)?;
            let cuda_actor_ref_2: reference::ActorRef<CudaActor> = cuda_handle.bind();

            let (rdma_buf, dev_ptr) = cuda_actor_ref_2
                .create_buffer(&instance_2, buffer_size, actor_2.clone())
                .await?;
            rdma_handle_2 = rdma_buf;
            device_ptr_2 = Some(dev_ptr);
            local_memory_2 = Arc::new(UnsafeLocalMemory::new(dev_ptr, buffer_size));

            buf_vec.push(Buffer {
                ptr: dev_ptr as u64,
                len: buffer_size,
                cpu_ref: None,
            });
            cuda_actor_2 = Some(cuda_actor_ref_2);
        }

        // Resolve ibverbs details lazily via resolve_ibv
        let (ibv_actor_1, ibv_buffer_1) = rdma_handle_1
            .resolve_ibv(&instance_1)
            .await
            .ok_or_else(|| anyhow::anyhow!("ibverbs backend not found for buffer 1"))??;
        let (ibv_actor_2, ibv_buffer_2) = rdma_handle_2
            .resolve_ibv(&instance_2)
            .await
            .ok_or_else(|| anyhow::anyhow!("ibverbs backend not found for buffer 2"))??;

        // Fill buffer1 with test data
        if parsed_accel1.0 == "cuda" {
            cuda_actor_1
                .clone()
                .unwrap()
                .fill_buffer(&instance_1, device_ptr_1.unwrap(), buffer_size, 42)
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
            client_1: instance_1,
            client_2: instance_2,
            actor_1,
            actor_2,
            ibv_actor_1,
            ibv_actor_2,
            rdma_handle_1,
            rdma_handle_2,
            local_memory_1,
            local_memory_2,
            ibv_buffer_1,
            ibv_buffer_2,
            cuda_actor_1,
            cuda_actor_2,
            device_ptr_1,
            device_ptr_2,
        })
    }

    pub async fn cleanup(self) -> Result<(), anyhow::Error> {
        self.ibv_actor_1
            .release_buffer(&self.client_1, self.ibv_buffer_1.mr_id)
            .await?;

        self.ibv_actor_2
            .release_buffer(&self.client_2, self.ibv_buffer_2.mr_id)
            .await?;
        Ok(())
    }

    /// Sets up the RDMA test environment with auto-detected QP type.
    ///
    /// This is a convenience wrapper around `setup_with_qp_type` that uses
    /// `IbvQpType::Auto` to automatically select the appropriate QP type.
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
            super::primitives::IbvQpType::Auto,
        )
        .await
    }

    pub async fn verify_buffers(&self, size: usize, offset: usize) -> Result<(), anyhow::Error> {
        let mut temp_buffer_1 = vec![0u8; size];
        let mut temp_buffer_2 = vec![0u8; size];

        // Read buffer 1
        if let Some(cuda_actor) = &self.cuda_actor_1 {
            cuda_actor
                .verify_buffer(
                    &self.client_1,
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
                    &self.client_2,
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

// ---------------------------------------------------------------------------
// CudaAllocator: reusable CUDA VMM allocator with a segment scanner
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub(crate) struct CudaAllocation {
    pub(crate) ptr: usize,
    pub(crate) size: usize,
    pub(crate) device: i32,
    handle: u64,
}

pub(crate) struct CudaAllocator {
    allocations: Mutex<HashMap<usize, CudaAllocation>>,
}

static CUDA_ALLOCATOR: OnceLock<CudaAllocator> = OnceLock::new();

unsafe fn cuda_err(result: rdmaxcel_sys::CUresult) -> String {
    unsafe {
        let mut s: *const std::os::raw::c_char = std::ptr::null();
        rdmaxcel_sys::rdmaxcel_cuGetErrorString(result, &mut s);
        if s.is_null() {
            format!("CUDA error {result}")
        } else {
            std::ffi::CStr::from_ptr(s).to_string_lossy().into_owned()
        }
    }
}

impl CudaAllocator {
    pub(crate) fn get() -> &'static CudaAllocator {
        CUDA_ALLOCATOR.get_or_init(|| {
            unsafe {
                cu_check!(rdmaxcel_sys::rdmaxcel_cuInit(0));
            }
            CudaAllocator {
                allocations: Mutex::new(HashMap::new()),
            }
        })
    }

    /// Allocate GPU memory on `device` of at least `size` bytes via the CUDA
    /// VMM API. Returns the device pointer. Panics on failure, cleaning up
    /// any partially-allocated resources.
    pub(crate) fn allocate(&self, device: i32, size: usize) -> CudaAllocation {
        unsafe {
            // Context setup — shared primary context, nothing to roll back.
            let mut dev: rdmaxcel_sys::CUdevice = std::mem::zeroed();
            cu_check!(rdmaxcel_sys::rdmaxcel_cuDeviceGet(&mut dev, device));

            let mut ctx: rdmaxcel_sys::CUcontext = std::mem::zeroed();
            cu_check!(rdmaxcel_sys::rdmaxcel_cuDevicePrimaryCtxRetain(
                &mut ctx, dev
            ));
            cu_check!(rdmaxcel_sys::rdmaxcel_cuCtxSetCurrent(ctx));

            let mut granularity: usize = 0;
            let mut prop: rdmaxcel_sys::CUmemAllocationProp = std::mem::zeroed();
            prop.type_ = rdmaxcel_sys::CU_MEM_ALLOCATION_TYPE_PINNED;
            prop.location.type_ = rdmaxcel_sys::CU_MEM_LOCATION_TYPE_DEVICE;
            prop.location.id = device;
            prop.allocFlags.gpuDirectRDMACapable = 1;
            prop.requestedHandleTypes = rdmaxcel_sys::CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR;

            cu_check!(rdmaxcel_sys::rdmaxcel_cuMemGetAllocationGranularity(
                &mut granularity,
                &prop,
                rdmaxcel_sys::CU_MEM_ALLOC_GRANULARITY_MINIMUM,
            ));

            let padded = ((size - 1) / granularity + 1) * granularity;

            // From here each step acquires a resource; clean up predecessors
            // before panicking if a later step fails.
            let mut handle: rdmaxcel_sys::CUmemGenericAllocationHandle = std::mem::zeroed();
            cu_check!(rdmaxcel_sys::rdmaxcel_cuMemCreate(
                &mut handle,
                padded,
                &prop,
                0
            ));

            let mut dptr: rdmaxcel_sys::CUdeviceptr = std::mem::zeroed();
            let r = rdmaxcel_sys::rdmaxcel_cuMemAddressReserve(&mut dptr, padded, 0, 0, 0);
            if r != rdmaxcel_sys::CUDA_SUCCESS {
                rdmaxcel_sys::rdmaxcel_cuMemRelease(handle);
                panic!("cuMemAddressReserve: {}", cuda_err(r));
            }

            let r = rdmaxcel_sys::rdmaxcel_cuMemMap(dptr, padded, 0, handle, 0);
            if r != rdmaxcel_sys::CUDA_SUCCESS {
                rdmaxcel_sys::rdmaxcel_cuMemRelease(handle);
                rdmaxcel_sys::rdmaxcel_cuMemAddressFree(dptr, padded);
                panic!("cuMemMap: {}", cuda_err(r));
            }

            let mut access: rdmaxcel_sys::CUmemAccessDesc = std::mem::zeroed();
            access.location.type_ = rdmaxcel_sys::CU_MEM_LOCATION_TYPE_DEVICE;
            access.location.id = device;
            access.flags = rdmaxcel_sys::CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
            let r = rdmaxcel_sys::rdmaxcel_cuMemSetAccess(dptr, padded, &access, 1);
            if r != rdmaxcel_sys::CUDA_SUCCESS {
                rdmaxcel_sys::rdmaxcel_cuMemUnmap(dptr, padded);
                rdmaxcel_sys::rdmaxcel_cuMemRelease(handle);
                rdmaxcel_sys::rdmaxcel_cuMemAddressFree(dptr, padded);
                panic!("cuMemSetAccess: {}", cuda_err(r));
            }

            let ptr = dptr as usize;
            let alloc = CudaAllocation {
                ptr,
                size: padded,
                device,
                handle,
            };
            self.allocations.lock().unwrap().insert(ptr, alloc);
            alloc
        }
    }
    pub(crate) fn free(&self, ptr: usize) {
        let alloc = self
            .allocations
            .lock()
            .unwrap()
            .remove(&ptr)
            .unwrap_or_else(|| panic!("unknown allocation 0x{ptr:x}"));

        // Unmap first, then release the backing allocation, then free the VA range.
        unsafe {
            cu_check!(rdmaxcel_sys::rdmaxcel_cuMemUnmap(
                alloc.ptr as rdmaxcel_sys::CUdeviceptr,
                alloc.size
            ));
            cu_check!(rdmaxcel_sys::rdmaxcel_cuMemRelease(alloc.handle));
            cu_check!(rdmaxcel_sys::rdmaxcel_cuMemAddressFree(
                alloc.ptr as rdmaxcel_sys::CUdeviceptr,
                alloc.size
            ));
        }
    }
}

impl CudaAllocation {
    pub(crate) fn free(self) {
        CudaAllocator::get().free(self.ptr);
    }
}

/// Segment scanner callback compatible with `rdmaxcel_segment_scanner_fn`.
/// Reports all allocations tracked by `CudaAllocator`.
pub(crate) unsafe extern "C" fn cuda_allocator_scanner(
    out: *mut rdmaxcel_sys::rdmaxcel_scanned_segment_t,
    max: usize,
) -> usize {
    let Some(allocator) = CUDA_ALLOCATOR.get() else {
        return 0;
    };
    let allocs = allocator.allocations.lock().unwrap();
    let count = allocs.len();
    if max == 0 || out.is_null() {
        return count;
    }
    for (i, alloc) in allocs.values().enumerate().take(max.min(count)) {
        // SAFETY: caller guarantees `out` points to a buffer of at least `max` entries.
        unsafe {
            *out.add(i) = rdmaxcel_sys::rdmaxcel_scanned_segment_t {
                address: alloc.ptr,
                size: alloc.size,
                device: alloc.device,
                is_expandable: 1,
            };
        }
    }
    count
}

/// Finds two CUDA devices that map to different RDMA NICs via PCI topology.
/// Returns `Some((device_a, device_b))` or `None` if all devices share one NIC.
#[cfg(test_8_gpus)]
pub(crate) fn find_devices_on_different_nics() -> Option<(i32, i32)> {
    use super::device_selection::select_optimal_ibv_device;

    let mut gpu_to_nic: Vec<(i32, String)> = Vec::new();
    for gpu_idx in 0..8 {
        let hint = format!("cuda:{gpu_idx}");
        if let Some(device) = select_optimal_ibv_device(Some(&hint)) {
            gpu_to_nic.push((gpu_idx, device.name().to_string()));
        }
    }

    for i in 0..gpu_to_nic.len() {
        for j in (i + 1)..gpu_to_nic.len() {
            if gpu_to_nic[i].1 != gpu_to_nic[j].1 {
                return Some((gpu_to_nic[i].0, gpu_to_nic[j].0));
            }
        }
    }
    None
}
