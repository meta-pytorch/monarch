/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! # CUDA RDMA Ping-Pong Example
//!
//! This example demonstrates how to use RDMA to perform a ping-pong data transfer between two CUDA devices.
//! It shows a pattern for using RdmaBuffer with CUDA memory for efficient zero-copy
//! bidirectional data transfer between GPUs.
//!
//! ## Architecture
//!
//! - Two actors, each associated with a different CUDA device
//! - Each actor allocates memory on its CUDA device
//! - RDMA is used for direct memory transfer between the two CUDA devices
//! - A ping-pong pattern is implemented where data is sent back and forth between devices
//!
//! ## Flow
//!
//! 1. Initialize CUDA devices and allocate memory on each device
//! 2. Initialize data on the first device with a test value
//! 3. Establish RDMA connections between the devices
//! 4. Perform RDMA ping-pong operations between the devices:
//!    - Device 2 sends data to Device 1
//!    - Device 1 sends data back to Device 2
//! 5. Verify the data was transferred correctly in both directions
//!
//! ## Key Components
//!
//! - `CudaRdmaActor`: Manages CUDA memory and RDMA operations for a single device
//! - `RdmaBuffer`: Provides zero-copy memory access between actors
//! - `RdmaManagerActor`: Handles the underlying RDMA connections and operations
//!
//! ## To run this
//!
//! $ buck2 run @//mode/dev-nosan //monarch/monarch_rdma/examples:cuda_ping_pong_example
//!
//! Make sure your dev machine has a backend network, i.e.
//! $ cat /etc/fbwhoami | grep DEVICE_BACKEND_NETWORK_TOPOLOGY
//!
//! should not be empty - it should show something like this:
//! $ cat /etc/fbwhoami | grep DEVICE_BACKEND_NETWORK_TOPOLOGY
//! > DEVICE_BACKEND_NETWORK_TOPOLOGY=gtn2/gtn2.2C//rtsw107.c083.f00.gtn2
//!
//! Also ensure you have at least two CUDA devices available.

// RDMA requires frequent unsafe code blocks
#![allow(clippy::undocumented_unsafe_blocks)]

// Import the cuda-sys and rdmaxcel-sys crates
// FFI bindings to CUDA RDMA kernels - merged inline
use std::os::raw::c_int;

use async_trait::async_trait;
use clap::Arg;
use clap::Command as ClapCommand;
use hyperactor::Actor;
use hyperactor::ActorRef;
use hyperactor::Bind;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::OncePortRef;
use hyperactor::RemoteSpawn;
use hyperactor::Unbind;
use hyperactor::channel::ChannelTransport;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_config::Attrs;
use hyperactor_mesh::Mesh;
use hyperactor_mesh::ProcMesh;
use hyperactor_mesh::RootActorMesh;
use hyperactor_mesh::alloc::AllocSpec;
use hyperactor_mesh::alloc::Allocator;
use hyperactor_mesh::alloc::ProcessAllocator;
use hyperactor_mesh::extent;
use hyperactor_mesh::proc_mesh::global_root_client;
use monarch_rdma::IbverbsConfig;
use monarch_rdma::RdmaBuffer;
use monarch_rdma::RdmaManagerActor;
use monarch_rdma::RdmaManagerMessageClient;
use monarch_rdma::cu_check;
use serde::Deserialize;
use serde::Serialize;
use tokio::process::Command;
use typeuri::Named;

/// RDMA parameters structure for CUDA RDMA operations
#[repr(C)]
#[derive(Debug)]
pub struct rdma_params_t {
    pub cu_ptr: usize,                       // CUDA pointer
    pub laddr: usize,                        // Local buffer address
    pub lsize: usize,                        // Local buffer size
    pub lkey: u32,                           // Local memory key
    pub raddr: usize,                        // Remote buffer address
    pub rsize: usize,                        // Remote buffer size
    pub rkey: u32,                           // Remote memory key
    pub qp_num: u32,                         // Queue pair number
    pub rq_buf: *mut u8,                     // Receive queue buffer
    pub rq_cnt: u32,                         // Receive queue count
    pub sq_buf: *mut u8,                     // Send queue buffer
    pub sq_cnt: u32,                         // Send queue count
    pub qp_dbrec: *mut u32,                  // Queue pair doorbell record
    pub send_cqe_buf: *mut std::ffi::c_void, // Send completion queue entry buffer
    pub send_cqe_size: u32,                  // Send completion queue entry size
    pub send_cqe_cnt: u32,                   // Send completion queue entry count
    pub send_cqe_dbrec: *mut u32,            // Send completion queue doorbell record
    pub recv_cqe_buf: *mut std::ffi::c_void, // Receive completion queue entry buffer
    pub recv_cqe_size: u32,                  // Receive completion queue entry size
    pub recv_cqe_cnt: u32,                   // Receive completion queue entry count
    pub recv_cqe_dbrec: *mut u32,            // Receive completion queue doorbell record
    pub qp_db: *mut std::ffi::c_void,        // Queue pair doorbell register
}

unsafe extern "C" {
    pub unsafe fn launchPingPong(
        params: *mut rdma_params_t,
        iterations: c_int,
        initial_length: c_int,
        device_id: c_int,
    ) -> c_int;
}

/// Safe wrapper for performing ping-pong test with RDMA parameters
pub fn ping_pong(
    params: &mut rdma_params_t,
    iterations: i32,
    initial_length: i32,
    device_id: i32,
) -> Result<(), i32> {
    let result = unsafe { launchPingPong(params, iterations, initial_length, device_id) };

    if result == 0 { Ok(()) } else { Err(result) }
}

// Constants for default values
const DEFAULT_BUFFER_SIZE_MB: usize = 32; // `must be multiple of 2MB
const DEFAULT_ITERATIONS: i32 = 48;
const DEFAULT_INITIAL_LENGTH_KB: usize = 1;
const DATA_VALUE: u8 = 42;

// CLI Configuration
#[derive(Debug, Clone)]
struct CliConfig {
    buffer_size: usize,
    iterations: i32,
    initial_length: i32,
}

impl CliConfig {
    fn parse_args() -> Self {
        let matches = ClapCommand::new("CUDA RDMA Ping-Pong Demo")
            .version("1.0")
            .about("Demonstrates CUDA RDMA ping-pong with configurable parameters")
            .arg(
                Arg::new("buffer-size")
                    .long("buffer-size")
                    .value_name("MB")
                    .help("Size of the CUDA buffer in MB (must be multiple of 2MB)")
                    .default_value(DEFAULT_BUFFER_SIZE_MB.to_string()),
            )
            .arg(
                Arg::new("iterations")
                    .long("iterations")
                    .value_name("COUNT")
                    .help("Number of ping-pong iterations to perform")
                    .default_value(DEFAULT_ITERATIONS.to_string()),
            )
            .arg(
                Arg::new("initial-length")
                    .long("initial-length")
                    .value_name("KB")
                    .help("Initial data length to transfer in each ping-pong (in KB)")
                    .default_value(DEFAULT_INITIAL_LENGTH_KB.to_string()),
            )
            .get_matches();

        let buffer_size_mb = matches
            .get_one::<String>("buffer-size")
            .unwrap()
            .parse::<usize>()
            .expect("buffer-size must be a valid number");

        let iterations = matches
            .get_one::<String>("iterations")
            .unwrap()
            .parse::<i32>()
            .expect("iterations must be a valid number");

        let initial_length_kb = matches
            .get_one::<String>("initial-length")
            .unwrap()
            .parse::<usize>()
            .expect("initial-length must be a valid number");

        // Validate parameters
        if buffer_size_mb == 0 {
            panic!("buffer-size must be greater than 0");
        }
        if buffer_size_mb % 2 != 0 {
            panic!(
                "buffer-size must be a multiple of 2 MB (current: {} MB)",
                buffer_size_mb
            );
        }
        if iterations <= 0 {
            panic!("iterations must be greater than 0");
        }
        if initial_length_kb == 0 {
            panic!("initial-length must be greater than 0");
        }

        // Convert MB/KB to bytes
        let buffer_size_bytes = buffer_size_mb * 1024 * 1024;
        let initial_length_bytes = initial_length_kb * 1024;

        tracing::info!(
            "CLI configuration - Buffer Size: {} MB ({} bytes), Iterations: {}, Initial Length: {} KB ({} bytes)",
            buffer_size_mb,
            buffer_size_bytes,
            iterations,
            initial_length_kb,
            initial_length_bytes
        );

        Self {
            buffer_size: buffer_size_bytes,
            iterations,
            initial_length: initial_length_bytes as i32,
        }
    }
}

// CUDA RDMA Actor
#[derive(Debug)]
#[hyperactor::export(
    spawn = true,
    handlers = [
        InitializeBuffer,
        PerformPingPong,
        VerifyBuffer,
        GetBufferHandle,
    ],
)]
pub struct CudaRdmaActor {
    // CUDA device ID this actor is associated with
    device_id: usize,
    // Buffer on CUDA device,
    cpu_buffer: Box<[u8]>,
    cu_ptr: usize,
    // RDMA buffer handle for the CUDA memory
    rdma_buffer_handle: Option<RdmaBuffer>,
    // Reference to the RDMA manager actor
    rdma_manager: ActorRef<RdmaManagerActor>,
}

#[async_trait]
impl Actor for CudaRdmaActor {
    async fn handle_supervision_event(
        &mut self,
        _cx: &Instance<Self>,
        _event: &ActorSupervisionEvent,
    ) -> Result<bool, anyhow::Error> {
        tracing::error!("CudaRdmaActor supervision event: {:?}", _event);
        tracing::error!("CudaRdmaActor error occurred, stop the worker process, exit code: 1");
        std::process::exit(1);
    }
}

#[async_trait]
impl RemoteSpawn for CudaRdmaActor {
    type Params = (ActorRef<RdmaManagerActor>, usize, usize);

    async fn new(params: Self::Params, _environment: Attrs) -> Result<Self, anyhow::Error> {
        let (rdma_manager, device_id, buffer_size) = params;
        let cpu_buffer = vec![0u8; buffer_size].into_boxed_slice();

        // Allocate memory on the CUDA device
        // In a real implementation, this would use CUDA APIs to allocate device memory
        // For this example, we'll use a regular Rust allocation as a placeholder
        // The actual CUDA allocation would be handled by the monarch_rdma library
        unsafe {
            cu_check!(rdmaxcel_sys::rdmaxcel_cuInit(0));
            let mut dptr: rdmaxcel_sys::CUdeviceptr = std::mem::zeroed();
            let mut handle: rdmaxcel_sys::CUmemGenericAllocationHandle = std::mem::zeroed();

            let mut device: rdmaxcel_sys::CUdevice = std::mem::zeroed();
            cu_check!(rdmaxcel_sys::rdmaxcel_cuDeviceGet(
                &mut device,
                device_id as i32
            ));

            let mut context: rdmaxcel_sys::CUcontext = std::mem::zeroed();
            cu_check!(rdmaxcel_sys::rdmaxcel_cuCtxCreate_v2(
                &mut context,
                0,
                device_id as i32
            ));
            cu_check!(rdmaxcel_sys::rdmaxcel_cuCtxSetCurrent(context));

            let mut granularity: usize = 0;
            let mut prop: rdmaxcel_sys::CUmemAllocationProp = std::mem::zeroed();
            prop.type_ = rdmaxcel_sys::CU_MEM_ALLOCATION_TYPE_PINNED;
            prop.location.type_ = rdmaxcel_sys::CU_MEM_LOCATION_TYPE_DEVICE;
            prop.location.id = device;
            prop.allocFlags.gpuDirectRDMACapable = 1;
            prop.requestedHandleTypes = rdmaxcel_sys::CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR;

            cu_check!(rdmaxcel_sys::rdmaxcel_cuMemGetAllocationGranularity(
                &mut granularity as *mut usize,
                &prop,
                rdmaxcel_sys::CU_MEM_ALLOC_GRANULARITY_MINIMUM,
            ));

            // ensure our size is aligned
            let padded_size: usize = ((buffer_size - 1) / granularity + 1) * granularity;
            assert!(padded_size == buffer_size);

            cu_check!(rdmaxcel_sys::rdmaxcel_cuMemCreate(
                &mut handle as *mut rdmaxcel_sys::CUmemGenericAllocationHandle,
                padded_size,
                &prop,
                0
            ));
            // reserve and map the memory
            cu_check!(rdmaxcel_sys::rdmaxcel_cuMemAddressReserve(
                &mut dptr as *mut rdmaxcel_sys::CUdeviceptr,
                padded_size,
                0,
                0,
                0,
            ));
            assert!((dptr as usize).is_multiple_of(granularity));
            assert!(padded_size.is_multiple_of(granularity));

            // fails if a add cu_check macro; but passes if we don't
            let err = rdmaxcel_sys::rdmaxcel_cuMemMap(
                dptr as rdmaxcel_sys::CUdeviceptr,
                padded_size,
                0,
                handle as rdmaxcel_sys::CUmemGenericAllocationHandle,
                0,
            );
            if err != rdmaxcel_sys::CUDA_SUCCESS {
                panic!("failed reserving and mapping memory {:?}", err);
            }

            // set access
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
            Ok(Self {
                device_id,
                cpu_buffer,
                cu_ptr: dptr as usize,
                rdma_buffer_handle: None,
                rdma_manager,
            })
        }
    }
}

// Message to initialize the buffer with data
#[derive(Debug, Serialize, Deserialize, Named, Clone)]
struct InitializeBuffer(pub u8, pub OncePortRef<bool>);

// Message to perform an RDMA ping-pong operation with another actor
#[derive(Debug, Serialize, Deserialize, Named, Clone)]
struct PerformPingPong(
    pub ActorRef<CudaRdmaActor>,
    pub RdmaBuffer,
    pub i32,
    pub i32,
    pub OncePortRef<bool>,
);

// Message to verify the buffer contents
#[derive(Debug, Serialize, Deserialize, Named, Clone)]
struct VerifyBuffer(pub Box<[u8]>, pub OncePortRef<bool>);

#[async_trait]
impl Handler<InitializeBuffer> for CudaRdmaActor {
    /// Initialize the buffer with a specific value
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        InitializeBuffer(value, reply): InitializeBuffer,
    ) -> Result<(), anyhow::Error> {
        // Fill the buffer with the specified value
        self.cpu_buffer.fill(value);

        unsafe {
            let mut context: rdmaxcel_sys::CUcontext = std::mem::zeroed();
            cu_check!(rdmaxcel_sys::rdmaxcel_cuCtxCreate_v2(
                &mut context,
                0,
                self.device_id as i32
            ));
            cu_check!(rdmaxcel_sys::rdmaxcel_cuCtxSetCurrent(context));
            rdmaxcel_sys::rdmaxcel_cuCtxSynchronize();
            cu_check!(rdmaxcel_sys::rdmaxcel_cuMemcpyHtoD_v2(
                self.cu_ptr as u64,
                self.cpu_buffer.as_ptr() as *const std::ffi::c_void,
                self.cpu_buffer.len()
            ));
        }
        // Register the buffer with RDMA if not already done
        if self.rdma_buffer_handle.is_none() {
            let addr = self.cu_ptr;
            let size = self.cpu_buffer.len();
            let buffer_handle = self.rdma_manager.request_buffer(cx, addr, size).await?;
            self.rdma_buffer_handle = Some(buffer_handle);
        }

        reply.send(cx, true)?;
        Ok(())
    }
}

pub async fn validate_execution_context() -> Result<(), anyhow::Error> {
    // Check for nvidia peermem
    match std::fs::read_to_string("/proc/modules") {
        Ok(contents) => {
            if !contents.contains("nvidia_peermem") {
                return Err(anyhow::anyhow!(
                    "nvidia_peermem module not found in /proc/modules"
                ));
            }
        }
        Err(e) => {
            return Err(anyhow::anyhow!(e));
        }
    }

    // Test file access to nvidia params
    match std::fs::read_to_string("/proc/driver/nvidia/params") {
        Ok(contents) => {
            if !contents.contains("PeerMappingOverride=1") {
                return Err(anyhow::anyhow!(
                    "PeerMappingOverride=1 not found in /proc/driver/nvidia/params"
                ));
            }
        }
        Err(e) => {
            return Err(anyhow::anyhow!(e));
        }
    }
    Ok(())
}

#[async_trait]
impl Handler<PerformPingPong> for CudaRdmaActor {
    /// Perform an RDMA write operation to transfer data to another actor using a provided remote buffer
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        PerformPingPong(_target_acor, remote_buffer, iters, initial_length, reply): PerformPingPong,
    ) -> Result<(), anyhow::Error> {
        // Get the local buffer handle
        let local_buffer = self
            .rdma_buffer_handle
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Local buffer not initialized"))?;

        validate_execution_context().await?;
        unsafe {
            let mut context: rdmaxcel_sys::CUcontext = std::mem::zeroed();
            cu_check!(rdmaxcel_sys::rdmaxcel_cuCtxCreate_v2(
                &mut context,
                0,
                self.device_id as i32
            ));
            cu_check!(rdmaxcel_sys::rdmaxcel_cuCtxSetCurrent(context));
        }
        let qp = self
            .rdma_manager
            .request_queue_pair(
                cx,
                remote_buffer.owner.clone(),
                local_buffer.device_name.clone(),
                remote_buffer.device_name.clone(),
            )
            .await?;

        unsafe {
            let ibv_qp = qp.qp as *mut rdmaxcel_sys::ibv_qp;
            let dv_qp = qp.dv_qp as *mut rdmaxcel_sys::mlx5dv_qp;
            let dv_send_cq = qp.dv_send_cq as *mut rdmaxcel_sys::mlx5dv_cq;
            let dv_recv_cq = qp.dv_recv_cq as *mut rdmaxcel_sys::mlx5dv_cq;
            let mut params = rdma_params_t {
                cu_ptr: self.cu_ptr,
                laddr: local_buffer.addr,
                lsize: local_buffer.size,
                lkey: local_buffer.lkey,
                raddr: remote_buffer.addr,
                rsize: remote_buffer.size,
                rkey: remote_buffer.rkey,
                qp_num: (*ibv_qp).qp_num,
                rq_buf: (*dv_qp).rq.buf as *mut u8,
                rq_cnt: (*dv_qp).rq.wqe_cnt,
                sq_buf: (*dv_qp).sq.buf as *mut u8,
                sq_cnt: (*dv_qp).sq.wqe_cnt,
                qp_dbrec: (*dv_qp).dbrec,
                send_cqe_buf: (*dv_send_cq).buf,
                send_cqe_size: (*dv_send_cq).cqe_size,
                send_cqe_cnt: (*dv_send_cq).cqe_cnt,
                send_cqe_dbrec: (*dv_send_cq).dbrec,
                recv_cqe_buf: (*dv_recv_cq).buf,
                recv_cqe_size: (*dv_recv_cq).cqe_size,
                recv_cqe_cnt: (*dv_recv_cq).cqe_cnt,
                recv_cqe_dbrec: (*dv_recv_cq).dbrec,
                qp_db: (*dv_qp).bf.reg,
            };
            match ping_pong(
                &mut params,
                iters,
                initial_length,
                self.device_id.try_into().unwrap(),
            ) {
                Ok(_) => {}
                Err(err) => {
                    return Err(anyhow::anyhow!("Ping Pong failed: {:?}", err));
                }
            }
        }
        reply.send(cx, true)?;
        Ok(())
    }
}

#[async_trait]
impl Handler<VerifyBuffer> for CudaRdmaActor {
    /// Verify that the buffer contains the expected values
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        VerifyBuffer(expected_values, reply): VerifyBuffer,
    ) -> Result<(), anyhow::Error> {
        unsafe {
            let mut context: rdmaxcel_sys::CUcontext = std::mem::zeroed();
            cu_check!(rdmaxcel_sys::rdmaxcel_cuCtxCreate_v2(
                &mut context,
                0,
                self.device_id as i32
            ));
            cu_check!(rdmaxcel_sys::rdmaxcel_cuCtxSetCurrent(context));
            rdmaxcel_sys::rdmaxcel_cuCtxSynchronize();
            cu_check!(rdmaxcel_sys::rdmaxcel_cuMemcpyDtoH_v2(
                self.cpu_buffer.as_mut_ptr() as *mut std::ffi::c_void,
                self.cu_ptr as rdmaxcel_sys::CUdeviceptr,
                self.cpu_buffer.len(),
            ));
        }

        let verify_len = expected_values.len();

        // Check if the buffer matches the expected values up to the length of expected_values
        let all_match = self
            .cpu_buffer
            .iter()
            .take(verify_len)
            .zip(expected_values.iter())
            .all(|(actual, expected)| actual == expected);

        if all_match {
            tracing::info!(
                "cuda_rdma_actor_{} buffer verification successful (checked {} bytes)",
                self.device_id,
                verify_len
            );
        } else {
            // Find the first non-matching value and its position
            let mut first_mismatch_pos = None;
            let mut first_actual_val = 0u8;
            let mut first_expected_val = 0u8;
            let mut mismatch_count = 0;

            for (pos, (actual, expected)) in self
                .cpu_buffer
                .iter()
                .take(verify_len)
                .zip(expected_values.iter())
                .enumerate()
            {
                if actual != expected {
                    if first_mismatch_pos.is_none() {
                        first_mismatch_pos = Some(pos);
                        first_actual_val = *actual;
                        first_expected_val = *expected;
                    }
                    mismatch_count += 1;
                }
            }

            tracing::info!(
                "cuda_rdma_actor_{} found {} matching elements out of {} (mismatch: {})",
                self.device_id,
                verify_len - mismatch_count,
                verify_len,
                mismatch_count
            );

            if let Some(pos) = first_mismatch_pos {
                tracing::info!(
                    "cuda_rdma_actor_{} first non-matching value at position {}: actual={}, expected={}",
                    self.device_id,
                    pos,
                    first_actual_val,
                    first_expected_val
                );
            }
        }

        reply.send(cx, all_match)?;
        Ok(())
    }
}

// Message to get the buffer handle from an actor
#[derive(Debug, Serialize, Deserialize, Named, Clone, Bind, Unbind)]
struct GetBufferHandle(#[binding(include)] pub OncePortRef<RdmaBuffer>);

#[async_trait]
impl Handler<GetBufferHandle> for CudaRdmaActor {
    /// Return the RDMA buffer handle for this actor's buffer
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        GetBufferHandle(reply): GetBufferHandle,
    ) -> Result<(), anyhow::Error> {
        let buffer = self
            .rdma_buffer_handle
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Buffer not initialized"))?;

        reply.send(cx, buffer.clone())?;
        Ok(())
    }
}

/// Main function to run the CUDA RDMA example
pub async fn run() -> Result<(), anyhow::Error> {
    // Initialize structured logging first
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // Parse CLI arguments
    let config = CliConfig::parse_args();

    // create expected values from ping pong/sanity check ranges
    let mut expected_buffer = vec![0u8; config.buffer_size];
    let mut start = 0;
    let mut length = config.initial_length as usize;
    for _iter in 0..config.iterations {
        let end = start + length;
        assert!(end <= config.buffer_size);
        expected_buffer[start..end].fill(DATA_VALUE);
        start = end + length;
        length += config.initial_length as usize;
    }
    let expected_data_values = expected_buffer[0..start].to_vec().into_boxed_slice();

    // Get all available RDMA devices
    let devices = monarch_rdma::get_all_devices();
    // Configure RDMA for the two actors
    // For H100 machines, we use different devices for better performance
    let device_1_ibv_config: IbverbsConfig;
    let device_2_ibv_config: IbverbsConfig;

    // Check if we have enough devices for optimal configuration
    if devices.len() > 4 {
        // Use separate backend devices for H100 configuration
        device_1_ibv_config = IbverbsConfig {
            device: devices.clone().into_iter().next().unwrap(),
            ..Default::default()
        };
        // The second device used is the 3rd. Main reason is because 0 and 3 are both backend
        // devices on gtn H100 devices.
        device_2_ibv_config = IbverbsConfig {
            device: devices.clone().into_iter().nth(3).unwrap(),
            ..Default::default()
        };
    } else {
        // For other configurations, use default settings
        device_1_ibv_config = IbverbsConfig::default();
        device_2_ibv_config = IbverbsConfig::default();
    }

    let instance = global_root_client();

    // Create process allocator for spawning actors
    let mut alloc = ProcessAllocator::new(Command::new(
        buck_resources::get("monarch/monarch_rdma/examples/cuda_ping_pong/bootstrap").unwrap(),
    ));

    let device_1_proc_mesh = ProcMesh::allocate(
        alloc
            .allocate(AllocSpec {
                extent: extent! {replica=1, host=1, gpu=1},
                constraints: Default::default(),
                proc_name: None,
                transport: ChannelTransport::Unix,
                proc_allocation_mode: Default::default(),
            })
            .await?,
    )
    .await?;

    // Create process mesh for the second CUDA device
    let device_2_proc_mesh = ProcMesh::allocate(
        alloc
            .allocate(AllocSpec {
                extent: extent! {replica=1, host=1, gpu=1},
                constraints: Default::default(),
                proc_name: None,
                transport: ChannelTransport::Unix,
                proc_allocation_mode: Default::default(),
            })
            .await?,
    )
    .await?;

    // Create RDMA manager for the first device
    let device_1_rdma_manager: RootActorMesh<'_, RdmaManagerActor> = device_1_proc_mesh
        .spawn(
            &instance,
            "device_1_rdma_manager",
            &Some(device_1_ibv_config),
        )
        .await?;

    // Create RDMA manager for the second device
    let device_2_rdma_manager: RootActorMesh<'_, RdmaManagerActor> = device_2_proc_mesh
        .spawn(
            &instance,
            "device_2_rdma_manager",
            &Some(device_2_ibv_config),
        )
        .await?;

    // Get the RDMA manager actor references
    let device_1_rdma_manager_ref = device_1_rdma_manager.iter().next().unwrap();
    let device_2_rdma_manager_ref = device_2_rdma_manager.iter().next().unwrap();

    // Create the CUDA RDMA actors
    let device_1_actor_mesh: RootActorMesh<'_, CudaRdmaActor> = device_1_proc_mesh
        .spawn(
            &instance,
            "device_1_actor",
            &(device_1_rdma_manager_ref.clone(), 0, config.buffer_size),
        )
        .await?;

    let device_2_actor_mesh: RootActorMesh<'_, CudaRdmaActor> = device_2_proc_mesh
        .spawn(
            &instance,
            "device_2_actor",
            &(device_2_rdma_manager_ref.clone(), 1, config.buffer_size),
        )
        .await?;

    // Get the actor references
    let device_1_actor = device_1_actor_mesh.iter().next().unwrap();
    let device_2_actor = device_2_actor_mesh.iter().next().unwrap();

    // Initialize the buffers

    // Initialize device 1 buffer with DATA_VALUE
    let (handle_1, receiver_1) = device_1_proc_mesh.client().open_once_port::<bool>();
    device_1_actor.send(
        device_1_proc_mesh.client(),
        InitializeBuffer(DATA_VALUE, handle_1.bind()),
    )?;
    receiver_1.recv().await?;

    // Initialize device 2 buffer with 0
    let (handle_2, receiver_2) = device_2_proc_mesh.client().open_once_port::<bool>();
    device_2_actor.send(
        device_2_proc_mesh.client(),
        InitializeBuffer(0, handle_2.bind()),
    )?;
    receiver_2.recv().await?;

    // Get the remote buffer handle from device 1
    let (handle_remote, receiver_remote) =
        device_1_proc_mesh.client().open_once_port::<RdmaBuffer>();
    device_1_actor.send(
        device_1_proc_mesh.client(),
        GetBufferHandle(handle_remote.bind()),
    )?;
    let buffer_1 = receiver_remote.recv().await?;

    let (handle_remote, receiver_remote) =
        device_2_proc_mesh.client().open_once_port::<RdmaBuffer>();
    device_2_actor.send(
        device_2_proc_mesh.client(),
        GetBufferHandle(handle_remote.bind()),
    )?;
    let buffer_2 = receiver_remote.recv().await?;

    // Perform RDMA write from device 2 to device 1 using the remote buffer
    let (handle_2, receiver_2) = device_2_proc_mesh.client().open_once_port::<bool>();
    let (handle_1, receiver_1) = device_1_proc_mesh.client().open_once_port::<bool>();

    device_2_actor.send(
        device_2_proc_mesh.client(),
        PerformPingPong(
            device_1_actor.clone(),
            buffer_1,
            config.iterations,
            config.initial_length,
            handle_2.bind(),
        ),
    )?;
    receiver_2.recv().await?;
    device_1_actor.send(
        device_1_proc_mesh.client(),
        PerformPingPong(
            device_2_actor.clone(),
            buffer_2,
            config.iterations,
            config.initial_length,
            handle_1.bind(),
        ),
    )?;
    receiver_1.recv().await?;

    let (handle, receiver) = device_2_proc_mesh.client().open_once_port::<bool>();
    device_2_actor.send(
        device_2_proc_mesh.client(),
        VerifyBuffer(expected_data_values.clone(), handle.bind()),
    )?;
    let verification_result2 = receiver.recv().await?;

    let (handle, receiver) = device_1_proc_mesh.client().open_once_port::<bool>();
    device_1_actor.send(
        device_1_proc_mesh.client(),
        VerifyBuffer(expected_data_values.clone(), handle.bind()),
    )?;
    let verification_result1 = receiver.recv().await?;

    if !verification_result1 {
        return Err(anyhow::anyhow!(
            "RDMA Ping-Pong verification failed actor 1"
        ));
    }
    if !verification_result2 {
        return Err(anyhow::anyhow!(
            "RDMA Ping-Pong verification failed actor 2"
        ));
    }
    tracing::info!("CUDA RDMA example completed successfully");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[timed_test::async_timed_test(timeout_secs = 30)]
    async fn test_cuda_rdma() -> Result<(), anyhow::Error> {
        run().await
    }
}
