/*
 * Portions Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/*
 * Sections of code adapted from
 * Copyright (c) 2016 Jon Gjengset under MIT License (MIT)
*/

//! This file contains primitive data structures for interacting with ibverbs.
//!
//! Primitives:
//! - `IbverbsConfig`: Represents ibverbs specific configurations, holding parameters required to establish and
//!   manage an RDMA connection, including settings for the RDMA device, queue pair attributes, and other
//!   connection-specific parameters.
//! - `RdmaDevice`: Represents an RDMA device, i.e. 'mlx5_0'. Contains information about the device, such as:
//!   its name, vendor ID, vendor part ID, hardware version, firmware version, node GUID, and capabilities.
//! - `RdmaPort`: Represents information about the port of an RDMA device, including state, physical state,
//!   LID (Local Identifier), and GID (Global Identifier) information.
//! - `RdmaMemoryRegionView`: Represents a memory region that can be registered with an RDMA device for direct
//!   memory access operations.
//! - `RdmaOperation`: Represents the type of RDMA operation to perform (Read or Write).
//! - `RdmaQpInfo`: Contains connection information needed to establish an RDMA connection with a remote endpoint.
//! - `IbvWc`: Wrapper around ibverbs work completion structure, used to track the status of RDMA operations.
use std::ffi::CStr;
use std::fmt;
use std::sync::OnceLock;

use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

#[derive(
    Default,
    Copy,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Hash,
    serde::Serialize,
    serde::Deserialize
)]
#[repr(transparent)]
pub struct Gid {
    raw: [u8; 16],
}

impl Gid {
    #[allow(dead_code)]
    fn subnet_prefix(&self) -> u64 {
        u64::from_be_bytes(self.raw[..8].try_into().unwrap())
    }

    #[allow(dead_code)]
    fn interface_id(&self) -> u64 {
        u64::from_be_bytes(self.raw[8..].try_into().unwrap())
    }
}
impl From<rdmaxcel_sys::ibv_gid> for Gid {
    fn from(gid: rdmaxcel_sys::ibv_gid) -> Self {
        Self {
            raw: unsafe { gid.raw },
        }
    }
}

impl From<Gid> for rdmaxcel_sys::ibv_gid {
    fn from(mut gid: Gid) -> Self {
        *gid.as_mut()
    }
}

impl AsRef<rdmaxcel_sys::ibv_gid> for Gid {
    fn as_ref(&self) -> &rdmaxcel_sys::ibv_gid {
        unsafe { &*self.raw.as_ptr().cast::<rdmaxcel_sys::ibv_gid>() }
    }
}

impl AsMut<rdmaxcel_sys::ibv_gid> for Gid {
    fn as_mut(&mut self) -> &mut rdmaxcel_sys::ibv_gid {
        unsafe { &mut *self.raw.as_mut_ptr().cast::<rdmaxcel_sys::ibv_gid>() }
    }
}

/// Queue pair type for RDMA operations.
///
/// Controls whether to use standard ibverbs queue pairs, mlx5dv extended queue pairs,
/// or EFA SRD queue pairs. Auto mode automatically selects based on device capabilities.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RdmaQpType {
    /// Auto-detect based on device capabilities
    Auto,
    /// Force standard ibverbs queue pair
    Standard,
    /// Force mlx5dv extended queue pair
    Mlx5dv,
    /// Force EFA SRD queue pair
    Efa,
}

/// Converts `RdmaQpType` to the corresponding integer enum value in rdmaxcel_sys.
pub fn resolve_qp_type(qp_type: RdmaQpType) -> u32 {
    match qp_type {
        RdmaQpType::Auto => {
            if crate::efa::is_efa_device() {
                rdmaxcel_sys::RDMA_QP_TYPE_EFA
            } else if mlx5dv_supported() {
                rdmaxcel_sys::RDMA_QP_TYPE_MLX5DV
            } else {
                rdmaxcel_sys::RDMA_QP_TYPE_STANDARD
            }
        }
        RdmaQpType::Standard => rdmaxcel_sys::RDMA_QP_TYPE_STANDARD,
        RdmaQpType::Mlx5dv => rdmaxcel_sys::RDMA_QP_TYPE_MLX5DV,
        RdmaQpType::Efa => rdmaxcel_sys::RDMA_QP_TYPE_EFA,
    }
}

/// Represents ibverbs specific configurations.
///
/// This struct holds various parameters required to establish and manage an RDMA connection.
/// It includes settings for the RDMA device, queue pair attributes, and other connection-specific
/// parameters.
#[derive(Debug, Named, Clone, Serialize, Deserialize)]
pub struct IbverbsConfig {
    /// `device` - The RDMA device to use for the connection.
    pub device: RdmaDevice,
    /// `cq_entries` - The number of completion queue entries.
    pub cq_entries: i32,
    /// `port_num` - The physical port number on the device.
    pub port_num: u8,
    /// `gid_index` - The GID index for the RDMA device.
    pub gid_index: u8,
    /// `max_send_wr` - The maximum number of outstanding send work requests.
    pub max_send_wr: u32,
    /// `max_recv_wr` - The maximum number of outstanding receive work requests.
    pub max_recv_wr: u32,
    /// `max_send_sge` - Te maximum number of scatter/gather elements in a send work request.
    pub max_send_sge: u32,
    /// `max_recv_sge` - The maximum number of scatter/gather elements in a receive work request.
    pub max_recv_sge: u32,
    /// `path_mtu` - The path MTU (Maximum Transmission Unit) for the connection.
    pub path_mtu: u32,
    /// `retry_cnt` - The number of retry attempts for a connection request.
    pub retry_cnt: u8,
    /// `rnr_retry` - The number of retry attempts for a receiver not ready (RNR) condition.
    pub rnr_retry: u8,
    /// `qp_timeout` - The timeout for a queue pair operation.
    pub qp_timeout: u8,
    /// `min_rnr_timer` - The minimum RNR timer value.
    pub min_rnr_timer: u8,
    /// `max_dest_rd_atomic` - The maximum number of outstanding RDMA read operations at the destination.
    pub max_dest_rd_atomic: u8,
    /// `max_rd_atomic` - The maximum number of outstanding RDMA read operations at the initiator.
    pub max_rd_atomic: u8,
    /// `pkey_index` - The partition key index.
    pub pkey_index: u16,
    /// `psn` - The packet sequence number.
    pub psn: u32,
    /// `use_gpu_direct` - Whether to enable GPU Direct RDMA support on init.
    pub use_gpu_direct: bool,
    /// `hw_init_delay_ms` - The delay in milliseconds before initializing the hardware.
    /// This is used to allow the hardware to settle before starting the first transmission.
    pub hw_init_delay_ms: u64,
    /// `qp_type` - The type of queue pair to create (Auto, Standard, or Mlx5dv).
    pub qp_type: RdmaQpType,
}
wirevalue::register_type!(IbverbsConfig);

/// Default RDMA parameters below are based on common values from rdma-core examples
/// For high-performance or production use, consider tuning
/// based on ibv_query_device() results and workload characteristics
impl Default for IbverbsConfig {
    fn default() -> Self {
        let mut config = Self {
            device: RdmaDevice::default(),
            cq_entries: 1024,
            port_num: 1,
            gid_index: 3,
            max_send_wr: 512,
            max_recv_wr: 512,
            max_send_sge: 30,
            max_recv_sge: 30,
            path_mtu: rdmaxcel_sys::IBV_MTU_4096,
            retry_cnt: 7,
            rnr_retry: 7,
            qp_timeout: 14, // 4.096 μs * 2^14 = ~67 ms
            min_rnr_timer: 12,
            max_dest_rd_atomic: 16,
            max_rd_atomic: 16,
            pkey_index: 0,
            psn: rand::random::<u32>() & 0xffffff,
            use_gpu_direct: false, // nv_peermem enabled for cuda
            hw_init_delay_ms: 2,
            qp_type: RdmaQpType::Auto,
        };
        if crate::efa::is_efa_device() {
            crate::efa::apply_efa_defaults(&mut config);
        }
        config
    }
}

impl IbverbsConfig {
    /// Create a new IbverbsConfig targeting a specific device
    ///
    /// Device targets use a unified "type:id" format:
    /// - "cpu:N" -> finds RDMA device closest to NUMA node N
    /// - "cuda:N" -> finds RDMA device closest to CUDA device N
    /// - "nic:mlx5_N" -> returns the specified NIC directly
    ///
    /// Shortcuts:
    /// - "cpu" -> defaults to "cpu:0"
    /// - "cuda" -> defaults to "cuda:0"
    ///
    /// # Arguments
    ///
    /// * `target` - Target device specification
    ///
    /// # Returns
    ///
    /// * `IbverbsConfig` with resolved device, or default device if resolution fails
    pub fn targeting(target: &str) -> Self {
        // Normalize shortcuts
        let normalized_target = match target {
            "cpu" => "cpu:0",
            "cuda" => "cuda:0",
            _ => target,
        };

        let device = crate::device_selection::select_optimal_rdma_device(Some(normalized_target))
            .unwrap_or_else(RdmaDevice::default);

        Self {
            device,
            ..Default::default()
        }
    }
}

impl std::fmt::Display for IbverbsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "IbverbsConfig {{ device: {}, port_num: {}, gid_index: {}, max_send_wr: {}, max_recv_wr: {}, max_send_sge: {}, max_recv_sge: {}, path_mtu: {:?}, retry_cnt: {}, rnr_retry: {}, qp_timeout: {}, min_rnr_timer: {}, max_dest_rd_atomic: {}, max_rd_atomic: {}, pkey_index: {}, psn: 0x{:x} }}",
            self.device.name(),
            self.port_num,
            self.gid_index,
            self.max_send_wr,
            self.max_recv_wr,
            self.max_send_sge,
            self.max_recv_sge,
            self.path_mtu,
            self.retry_cnt,
            self.rnr_retry,
            self.qp_timeout,
            self.min_rnr_timer,
            self.max_dest_rd_atomic,
            self.max_rd_atomic,
            self.pkey_index,
            self.psn,
        )
    }
}

/// Represents an RDMA device in the system.
///
/// This struct encapsulates information about an RDMA device, including its hardware
/// characteristics, capabilities, and port information. It provides access to device
/// attributes such as vendor information, firmware version, and supported features.
///
/// # Examples
///
/// ```
/// use monarch_rdma::get_all_devices;
///
/// let devices = get_all_devices();
/// if let Some(device) = devices.first() {
///     // Access device name and firmware version
///     let device_name = device.name();
///     let firmware_version = device.fw_ver();
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RdmaDevice {
    /// `name` - The name of the RDMA device (e.g., "mlx5_0").
    pub name: String,
    /// `vendor_id` - The vendor ID of the device.
    vendor_id: u32,
    /// `vendor_part_id` - The vendor part ID of the device.
    vendor_part_id: u32,
    /// `hw_ver` - Hardware version of the device.
    hw_ver: u32,
    /// `fw_ver` - Firmware version of the device.
    fw_ver: String,
    /// `node_guid` - Node GUID (Globally Unique Identifier) of the device.
    node_guid: u64,
    /// `ports` - Vector of ports available on this device.
    ports: Vec<RdmaPort>,
    /// `max_qp` - Maximum number of queue pairs supported.
    max_qp: i32,
    /// `max_cq` - Maximum number of completion queues supported.
    max_cq: i32,
    /// `max_mr` - Maximum number of memory regions supported.
    max_mr: i32,
    /// `max_pd` - Maximum number of protection domains supported.
    max_pd: i32,
    /// `max_qp_wr` - Maximum number of work requests per queue pair.
    max_qp_wr: i32,
    /// `max_sge` - Maximum number of scatter/gather elements per work request.
    max_sge: i32,
}

impl RdmaDevice {
    /// Returns the name of the RDMA device.
    pub fn name(&self) -> &String {
        &self.name
    }

    /// Returns the first available RDMA device, if any.
    pub fn first_available() -> Option<RdmaDevice> {
        let devices = get_all_devices();
        if devices.is_empty() {
            None
        } else {
            Some(devices.into_iter().next().unwrap())
        }
    }

    /// Returns the vendor ID of the RDMA device.
    pub fn vendor_id(&self) -> u32 {
        self.vendor_id
    }

    /// Returns the vendor part ID of the RDMA device.
    pub fn vendor_part_id(&self) -> u32 {
        self.vendor_part_id
    }

    /// Returns the hardware version of the RDMA device.
    pub fn hw_ver(&self) -> u32 {
        self.hw_ver
    }

    /// Returns the firmware version of the RDMA device.
    pub fn fw_ver(&self) -> &String {
        &self.fw_ver
    }

    /// Returns the node GUID of the RDMA device.
    pub fn node_guid(&self) -> u64 {
        self.node_guid
    }

    /// Returns a reference to the vector of ports available on the RDMA device.
    pub fn ports(&self) -> &Vec<RdmaPort> {
        &self.ports
    }

    /// Returns the maximum number of queue pairs supported by the RDMA device.
    pub fn max_qp(&self) -> i32 {
        self.max_qp
    }

    /// Returns the maximum number of completion queues supported by the RDMA device.
    pub fn max_cq(&self) -> i32 {
        self.max_cq
    }

    /// Returns the maximum number of memory regions supported by the RDMA device.
    pub fn max_mr(&self) -> i32 {
        self.max_mr
    }

    /// Returns the maximum number of protection domains supported by the RDMA device.
    pub fn max_pd(&self) -> i32 {
        self.max_pd
    }

    /// Returns the maximum number of work requests per queue pair supported by the RDMA device.
    pub fn max_qp_wr(&self) -> i32 {
        self.max_qp_wr
    }

    /// Returns the maximum number of scatter/gather elements per work request supported by the RDMA device.
    pub fn max_sge(&self) -> i32 {
        self.max_sge
    }
}

impl Default for RdmaDevice {
    fn default() -> Self {
        // Try to get a smart default using device selection logic (defaults to cpu:0)
        if let Some(device) = crate::device_selection::select_optimal_rdma_device(Some("cpu:0")) {
            device
        } else {
            // Fallback to first available device
            get_all_devices()
                .into_iter()
                .next()
                .unwrap_or_else(|| panic!("No RDMA devices found"))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RdmaPort {
    /// `port_num` - The physical port number on the device.
    port_num: u8,
    /// `state` - The current state of the port.
    state: String,
    /// `physical_state` - The physical state of the port.
    physical_state: String,
    /// `base_lid` - Base Local Identifier for the port.
    base_lid: u16,
    /// `lmc` - LID Mask Control.
    lmc: u8,
    /// `sm_lid` - Subnet Manager Local Identifier.
    sm_lid: u16,
    /// `capability_mask` - Capability mask of the port.
    capability_mask: u32,
    /// `link_layer` - The link layer type (e.g., InfiniBand, Ethernet).
    link_layer: String,
    /// `gid` - Global Identifier for the port.
    gid: String,
    /// `gid_tbl_len` - Length of the GID table.
    gid_tbl_len: i32,
}

impl fmt::Display for RdmaDevice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}", self.name)?;
        writeln!(f, "\tNumber of ports: {}", self.ports.len())?;
        writeln!(f, "\tFirmware version: {}", self.fw_ver)?;
        writeln!(f, "\tHardware version: {}", self.hw_ver)?;
        writeln!(f, "\tNode GUID: 0x{:016x}", self.node_guid)?;
        writeln!(f, "\tVendor ID: 0x{:x}", self.vendor_id)?;
        writeln!(f, "\tVendor part ID: {}", self.vendor_part_id)?;
        writeln!(f, "\tMax QPs: {}", self.max_qp)?;
        writeln!(f, "\tMax CQs: {}", self.max_cq)?;
        writeln!(f, "\tMax MRs: {}", self.max_mr)?;
        writeln!(f, "\tMax PDs: {}", self.max_pd)?;
        writeln!(f, "\tMax QP WRs: {}", self.max_qp_wr)?;
        writeln!(f, "\tMax SGE: {}", self.max_sge)?;

        for port in &self.ports {
            write!(f, "{}", port)?;
        }

        Ok(())
    }
}

impl fmt::Display for RdmaPort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "\tPort {}:", self.port_num)?;
        writeln!(f, "\t\tState: {}", self.state)?;
        writeln!(f, "\t\tPhysical state: {}", self.physical_state)?;
        writeln!(f, "\t\tBase lid: {}", self.base_lid)?;
        writeln!(f, "\t\tLMC: {}", self.lmc)?;
        writeln!(f, "\t\tSM lid: {}", self.sm_lid)?;
        writeln!(f, "\t\tCapability mask: 0x{:08x}", self.capability_mask)?;
        writeln!(f, "\t\tLink layer: {}", self.link_layer)?;
        writeln!(f, "\t\tGID: {}", self.gid)?;
        writeln!(f, "\t\tGID table length: {}", self.gid_tbl_len)?;
        Ok(())
    }
}

/// Converts the given port state to a human-readable string.
///
/// # Arguments
///
/// * `state` - The port state as defined by `ffi::ibv_port_state::Type`.
///
/// # Returns
///
/// A string representation of the port state.
pub fn get_port_state_str(state: rdmaxcel_sys::ibv_port_state::Type) -> String {
    // SAFETY: We are calling a C function that returns a C string.
    unsafe {
        let c_str = rdmaxcel_sys::ibv_port_state_str(state);
        if c_str.is_null() {
            return "Unknown".to_string();
        }
        CStr::from_ptr(c_str).to_string_lossy().into_owned()
    }
}

/// Converts the given physical state to a human-readable string.
///
/// # Arguments
///
/// * `phys_state` - The physical state as a `u8`.
///
/// # Returns
///
/// A string representation of the physical state.
pub fn get_port_phy_state_str(phys_state: u8) -> String {
    match phys_state {
        1 => "Sleep".to_string(),
        2 => "Polling".to_string(),
        3 => "Disabled".to_string(),
        4 => "PortConfigurationTraining".to_string(),
        5 => "LinkUp".to_string(),
        6 => "LinkErrorRecovery".to_string(),
        7 => "PhyTest".to_string(),
        _ => "No state change".to_string(),
    }
}

/// Converts the given link layer type to a human-readable string.
///
/// # Arguments
///
/// * `link_layer` - The link layer type as a `u8`.
///
/// # Returns
///
/// A string representation of the link layer type.
pub fn get_link_layer_str(link_layer: u8) -> String {
    match link_layer {
        1 => "InfiniBand".to_string(),
        2 => "Ethernet".to_string(),
        _ => "Unknown".to_string(),
    }
}

/// Formats a GID (Global Identifier) into a human-readable string.
///
/// # Arguments
///
/// * `gid` - A reference to a 16-byte array representing the GID.
///
/// # Returns
///
/// A formatted string representation of the GID.
pub fn format_gid(gid: &[u8; 16]) -> String {
    format!(
        "{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}",
        gid[0],
        gid[1],
        gid[2],
        gid[3],
        gid[4],
        gid[5],
        gid[6],
        gid[7],
        gid[8],
        gid[9],
        gid[10],
        gid[11],
        gid[12],
        gid[13],
        gid[14],
        gid[15]
    )
}

/// Retrieves information about all available RDMA devices in the system.
///
/// This function queries the system for all available RDMA devices and returns
/// detailed information about each device, including its capabilities, ports,
/// and attributes.
///
/// # Returns
///
/// A vector of `RdmaDevice` structures, each representing an RDMA device in the system.
/// Returns an empty vector if no devices are found or if there was an error querying
/// the devices.
pub fn get_all_devices() -> Vec<RdmaDevice> {
    let mut devices = Vec::new();

    // SAFETY: We are calling several C functions from libibverbs.
    unsafe {
        let mut num_devices = 0;
        let device_list = rdmaxcel_sys::ibv_get_device_list(&mut num_devices);
        if device_list.is_null() || num_devices == 0 {
            return devices;
        }

        for i in 0..num_devices {
            let device = *device_list.add(i as usize);
            if device.is_null() {
                continue;
            }

            let context = rdmaxcel_sys::ibv_open_device(device);
            if context.is_null() {
                continue;
            }

            let device_name = CStr::from_ptr(rdmaxcel_sys::ibv_get_device_name(device))
                .to_string_lossy()
                .into_owned();

            let mut device_attr = rdmaxcel_sys::ibv_device_attr::default();
            if rdmaxcel_sys::ibv_query_device(context, &mut device_attr) != 0 {
                rdmaxcel_sys::ibv_close_device(context);
                continue;
            }

            let fw_ver = CStr::from_ptr(device_attr.fw_ver.as_ptr())
                .to_string_lossy()
                .into_owned();

            let mut rdma_device = RdmaDevice {
                name: device_name,
                vendor_id: device_attr.vendor_id,
                vendor_part_id: device_attr.vendor_part_id,
                hw_ver: device_attr.hw_ver,
                fw_ver,
                node_guid: device_attr.node_guid,
                ports: Vec::new(),
                max_qp: device_attr.max_qp,
                max_cq: device_attr.max_cq,
                max_mr: device_attr.max_mr,
                max_pd: device_attr.max_pd,
                max_qp_wr: device_attr.max_qp_wr,
                max_sge: device_attr.max_sge,
            };

            for port_num in 1..=device_attr.phys_port_cnt {
                let mut port_attr = rdmaxcel_sys::ibv_port_attr::default();
                if rdmaxcel_sys::ibv_query_port(
                    context,
                    port_num,
                    &mut port_attr as *mut rdmaxcel_sys::ibv_port_attr as *mut _,
                ) != 0
                {
                    continue;
                }
                let state = get_port_state_str(port_attr.state);
                let physical_state = get_port_phy_state_str(port_attr.phys_state);

                let link_layer = get_link_layer_str(port_attr.link_layer);

                let mut gid = rdmaxcel_sys::ibv_gid::default();
                let gid_str = if rdmaxcel_sys::ibv_query_gid(context, port_num, 0, &mut gid) == 0 {
                    format_gid(&gid.raw)
                } else {
                    "N/A".to_string()
                };

                let rdma_port = RdmaPort {
                    port_num,
                    state,
                    physical_state,
                    base_lid: port_attr.lid,
                    lmc: port_attr.lmc,
                    sm_lid: port_attr.sm_lid,
                    capability_mask: port_attr.port_cap_flags,
                    link_layer,
                    gid: gid_str,
                    gid_tbl_len: port_attr.gid_tbl_len,
                };

                rdma_device.ports.push(rdma_port);
            }

            devices.push(rdma_device);
            rdmaxcel_sys::ibv_close_device(context);
        }

        rdmaxcel_sys::ibv_free_device_list(device_list);
    }

    devices
}

/// Cached result of mlx5dv support check.
static MLX5DV_SUPPORTED_CACHE: OnceLock<bool> = OnceLock::new();

/// Checks if mlx5dv (Mellanox device-specific verbs extension) is supported.
///
/// This function attempts to open the first available RDMA device and check if
/// mlx5dv extensions can be initialized. The mlx5dv extensions are required for
/// advanced features like GPU Direct RDMA and direct queue pair manipulation.
///
/// The result is cached after the first call, making subsequent calls essentially free.
///
/// # Returns
///
/// `true` if mlx5dv extensions are supported, `false` otherwise.
pub fn mlx5dv_supported() -> bool {
    *MLX5DV_SUPPORTED_CACHE.get_or_init(mlx5dv_supported_impl)
}

fn mlx5dv_supported_impl() -> bool {
    // SAFETY: We are calling C functions from libibverbs and libmlx5.
    unsafe {
        let mut mlx5dv_supported = false;
        let mut num_devices = 0;
        let device_list = rdmaxcel_sys::ibv_get_device_list(&mut num_devices);
        if !device_list.is_null() && num_devices > 0 {
            let device = *device_list;
            if !device.is_null() {
                mlx5dv_supported = rdmaxcel_sys::mlx5dv_is_supported(device);
            }
            rdmaxcel_sys::ibv_free_device_list(device_list);
        }
        mlx5dv_supported
    }
}

/// Cached result of ibverbs support check.
static IBVERBS_SUPPORTED_CACHE: OnceLock<bool> = OnceLock::new();

/// Checks if ibverbs devices can be retrieved successfully.
///
/// This function attempts to retrieve the list of RDMA devices using the
/// `ibv_get_device_list` function from the ibverbs library. It returns `true`
/// if devices are found, and `false` otherwise.
///
/// The result is cached after the first call, making subsequent calls essentially free.
///
/// # Returns
///
/// `true` if devices are successfully retrieved, `false` otherwise.
pub fn ibverbs_supported() -> bool {
    *IBVERBS_SUPPORTED_CACHE.get_or_init(ibverbs_supported_impl)
}

fn ibverbs_supported_impl() -> bool {
    // SAFETY: We are calling a C function from libibverbs.
    unsafe {
        let mut num_devices = 0;
        let device_list = rdmaxcel_sys::ibv_get_device_list(&mut num_devices);
        if !device_list.is_null() {
            rdmaxcel_sys::ibv_free_device_list(device_list);
        }
        num_devices > 0
    }
}

/// Checks if RDMA is fully supported on this system.
///
/// This is the canonical function to check if RDMA can be used.
pub fn rdma_supported() -> bool {
    ibverbs_supported()
}

/// Represents a view of a memory region that can be registered with an RDMA device.
///
/// This is a 'view' of a registered Memory Region, allowing multiple views into a single
/// large MR registration. This is commonly used with PyTorch's caching allocator, which
/// reserves large memory blocks and provides different data pointers into that space.
///
/// # Example
/// PyTorch Caching Allocator creates a 16GB segment at virtual address `0x01000000`.
/// The underlying Memory Region registers 16GB but at RDMA address `0x0`.
/// To access virtual address `0x01100000`, we return a view at RDMA address `0x100000`.
///
/// # Safety
/// The caller must ensure the memory remains valid and is not freed, moved, or
/// overwritten while RDMA operations are in progress.

#[derive(
    Debug,
    PartialEq,
    Eq,
    std::hash::Hash,
    Serialize,
    Deserialize,
    Clone,
    Copy
)]
pub struct RdmaMemoryRegionView {
    // id should be unique with a given rdmam manager
    pub id: usize,
    /// Virtual address in the process address space.
    /// This is the pointer/address as seen by the local process.
    pub virtual_addr: usize,
    /// Memory address assigned after Memory Region (MR) registration.
    /// This is the address may be offset a base MR addr.
    pub rdma_addr: usize,
    pub size: usize,
    pub lkey: u32,
    pub rkey: u32,
}

// SAFETY: RdmaMemoryRegionView can be safely sent between threads because it only
// contains address and size information without any thread-local state. However,
// this DOES NOT provide any protection against data races in the underlying memory.
// If one thread initiates an RDMA operation while another thread modifies the same
// memory region, undefined behavior will occur. The caller is responsible for proper
// synchronization of access to the underlying memory.
unsafe impl Send for RdmaMemoryRegionView {}

// SAFETY: RdmaMemoryRegionView is safe for concurrent access by multiple threads
// as it only provides a view into memory without modifying its own state. However,
// it provides NO PROTECTION against concurrent access to the underlying memory region.
// The caller must ensure proper synchronization when:
// 1. Initiating RDMA operations while local code reads/writes the same memory
// 2. Performing multiple overlapping RDMA operations on the same memory region
// 3. Freeing or reallocating memory that has in-flight RDMA operations
unsafe impl Sync for RdmaMemoryRegionView {}

impl RdmaMemoryRegionView {
    /// Creates a new `RdmaMemoryRegionView` with the given address and size.
    pub fn new(
        id: usize,
        virtual_addr: usize,
        rdma_addr: usize,
        size: usize,
        lkey: u32,
        rkey: u32,
    ) -> Self {
        Self {
            id,
            virtual_addr,
            rdma_addr,
            size,
            lkey,
            rkey,
        }
    }
}

/// Enum representing the common RDMA operations.
///
/// This provides a more ergonomic interface to the underlying ibv_wr_opcode types.
/// RDMA operations allow for direct memory access between two machines without
/// involving the CPU of the target machine.
///
/// # Variants
///
/// * `Write` - Represents an RDMA write operation where data is written from the local
///   memory to a remote memory region.
/// * `Read` - Represents an RDMA read operation where data is read from a remote memory
///   region into the local memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RdmaOperation {
    /// RDMA write operations
    Write,
    WriteWithImm,
    /// RDMA read operation
    Read,
    /// RDMA recv operation
    Recv,
}

impl From<RdmaOperation> for rdmaxcel_sys::ibv_wr_opcode::Type {
    fn from(op: RdmaOperation) -> Self {
        match op {
            RdmaOperation::Write => rdmaxcel_sys::ibv_wr_opcode::IBV_WR_RDMA_WRITE,
            RdmaOperation::WriteWithImm => rdmaxcel_sys::ibv_wr_opcode::IBV_WR_RDMA_WRITE_WITH_IMM,
            RdmaOperation::Read => rdmaxcel_sys::ibv_wr_opcode::IBV_WR_RDMA_READ,
            RdmaOperation::Recv => panic!("Invalid wr opcode"),
        }
    }
}

impl From<rdmaxcel_sys::ibv_wc_opcode::Type> for RdmaOperation {
    fn from(op: rdmaxcel_sys::ibv_wc_opcode::Type) -> Self {
        match op {
            rdmaxcel_sys::ibv_wc_opcode::IBV_WC_RDMA_WRITE => RdmaOperation::Write,
            rdmaxcel_sys::ibv_wc_opcode::IBV_WC_RDMA_READ => RdmaOperation::Read,
            _ => panic!("Unsupported operation type"),
        }
    }
}

/// Contains information needed to establish an RDMA queue pair with a remote endpoint.
///
/// `RdmaQpInfo` encapsulates all the necessary information to establish a queue pair
/// with a remote RDMA device. This includes queue pair number, LID (Local Identifier),
/// GID (Global Identifier), remote memory address, remote key, and packet sequence number.
#[derive(Default, Named, Clone, serde::Serialize, serde::Deserialize)]
pub struct RdmaQpInfo {
    /// `qp_num` - Queue Pair Number, uniquely identifies a queue pair on the remote device
    pub qp_num: u32,
    /// `lid` - Local Identifier, used for addressing in InfiniBand subnet
    pub lid: u16,
    /// `gid` - Global Identifier, used for routing across subnets (similar to IPv6 address)
    pub gid: Option<Gid>,
    /// `psn` - Packet Sequence Number, used for ordering packets
    pub psn: u32,
}
wirevalue::register_type!(RdmaQpInfo);

impl std::fmt::Debug for RdmaQpInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RdmaQpInfo {{ qp_num: {}, lid: {}, gid: {:?}, psn: 0x{:x} }}",
            self.qp_num, self.lid, self.gid, self.psn
        )
    }
}

/// Wrapper around ibv_wc (ibverbs work completion).
///
/// This exposes only the public fields of rdmaxcel_sys::ibv_wc, allowing us to more easily
/// interact with it from Rust. Work completions are used to track the status of
/// RDMA operations and are generated when an operation completes.
#[derive(Debug, Named, Clone, serde::Serialize, serde::Deserialize)]
pub struct IbvWc {
    /// `wr_id` - Work Request ID, used to identify the completed operation
    wr_id: u64,
    /// `len` - Length of the data transferred
    len: usize,
    /// `valid` - Whether the work completion is valid
    valid: bool,
    /// `error` - Error information if the operation failed
    error: Option<(rdmaxcel_sys::ibv_wc_status::Type, u32)>,
    /// `opcode` - Type of operation that completed (read, write, etc.)
    opcode: rdmaxcel_sys::ibv_wc_opcode::Type,
    /// `bytes` - Immediate data (if any)
    bytes: Option<u32>,
    /// `qp_num` - Queue Pair Number
    qp_num: u32,
    /// `src_qp` - Source Queue Pair Number
    src_qp: u32,
    /// `pkey_index` - Partition Key Index
    pkey_index: u16,
    /// `slid` - Source LID
    slid: u16,
    /// `sl` - Service Level
    sl: u8,
    /// `dlid_path_bits` - Destination LID Path Bits
    dlid_path_bits: u8,
}
wirevalue::register_type!(IbvWc);

impl From<rdmaxcel_sys::ibv_wc> for IbvWc {
    fn from(wc: rdmaxcel_sys::ibv_wc) -> Self {
        IbvWc {
            wr_id: wc.wr_id(),
            len: wc.len(),
            valid: wc.is_valid(),
            error: wc.error(),
            opcode: wc.opcode(),
            bytes: wc.imm_data(),
            qp_num: wc.qp_num,
            src_qp: wc.src_qp,
            pkey_index: wc.pkey_index,
            slid: wc.slid,
            sl: wc.sl,
            dlid_path_bits: wc.dlid_path_bits,
        }
    }
}

impl IbvWc {
    /// Returns the Work Request ID associated with this work completion.
    ///
    /// The Work Request ID is used to identify the specific operation that completed.
    /// It is set by the application when posting the work request and is returned
    /// unchanged in the work completion.
    pub fn wr_id(&self) -> u64 {
        self.wr_id
    }

    /// Returns whether this work completion is valid.
    ///
    /// A valid work completion indicates that the operation completed successfully.
    /// If false, the `error` field may contain additional information about the failure.
    pub fn is_valid(&self) -> bool {
        self.valid
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_all_devices() {
        // Skip test if RDMA devices are not available
        let devices = get_all_devices();
        if devices.is_empty() {
            println!("Skipping test: RDMA devices not available");
            return;
        }
        // Basic validation of first device
        let device = &devices[0];
        assert!(!device.name().is_empty(), "device name should not be empty");
        assert!(
            !device.ports().is_empty(),
            "device should have at least one port"
        );
    }

    #[test]
    fn test_first_available() {
        // Skip test if RDMA is not available
        let devices = get_all_devices();
        if devices.is_empty() {
            println!("Skipping test: RDMA devices not available");
            return;
        }
        // Basic validation of first device
        let device = &devices[0];

        let dev = device;
        // Verify getters return expected values
        assert_eq!(dev.vendor_id(), dev.vendor_id);
        assert_eq!(dev.vendor_part_id(), dev.vendor_part_id);
        assert_eq!(dev.hw_ver(), dev.hw_ver);
        assert_eq!(dev.fw_ver(), &dev.fw_ver);
        assert_eq!(dev.node_guid(), dev.node_guid);
        assert_eq!(dev.max_qp(), dev.max_qp);
        assert_eq!(dev.max_cq(), dev.max_cq);
        assert_eq!(dev.max_mr(), dev.max_mr);
        assert_eq!(dev.max_pd(), dev.max_pd);
        assert_eq!(dev.max_qp_wr(), dev.max_qp_wr);
        assert_eq!(dev.max_sge(), dev.max_sge);
    }

    #[test]
    fn test_device_display() {
        if let Some(device) = RdmaDevice::first_available() {
            let display_output = format!("{}", device);
            assert!(
                display_output.contains(&device.name),
                "display should include device name"
            );
            assert!(
                display_output.contains(&device.fw_ver),
                "display should include firmware version"
            );
        }
    }

    #[test]
    fn test_port_display() {
        if let Some(device) = RdmaDevice::first_available() {
            if !device.ports().is_empty() {
                let port = &device.ports()[0];
                let display_output = format!("{}", port);
                assert!(
                    display_output.contains(&port.state),
                    "display should include port state"
                );
                assert!(
                    display_output.contains(&port.link_layer),
                    "display should include link layer"
                );
            }
        }
    }

    #[test]
    fn test_rdma_operation_conversion() {
        assert_eq!(
            rdmaxcel_sys::ibv_wr_opcode::IBV_WR_RDMA_WRITE,
            rdmaxcel_sys::ibv_wr_opcode::Type::from(RdmaOperation::Write)
        );
        assert_eq!(
            rdmaxcel_sys::ibv_wr_opcode::IBV_WR_RDMA_READ,
            rdmaxcel_sys::ibv_wr_opcode::Type::from(RdmaOperation::Read)
        );

        assert_eq!(
            RdmaOperation::Write,
            RdmaOperation::from(rdmaxcel_sys::ibv_wc_opcode::IBV_WC_RDMA_WRITE)
        );
        assert_eq!(
            RdmaOperation::Read,
            RdmaOperation::from(rdmaxcel_sys::ibv_wc_opcode::IBV_WC_RDMA_READ)
        );
    }

    #[test]
    fn test_rdma_endpoint() {
        let endpoint = RdmaQpInfo {
            qp_num: 42,
            lid: 123,
            gid: None,
            psn: 0x5678,
        };

        let debug_str = format!("{:?}", endpoint);
        assert!(debug_str.contains("qp_num: 42"));
        assert!(debug_str.contains("lid: 123"));
        assert!(debug_str.contains("psn: 0x5678"));
    }

    #[test]
    fn test_ibv_wc() {
        let mut wc = rdmaxcel_sys::ibv_wc::default();

        // SAFETY: modifies private fields through pointer manipulation
        unsafe {
            // Cast to pointer and modify the fields directly
            let wc_ptr = &mut wc as *mut rdmaxcel_sys::ibv_wc as *mut u8;

            // Set wr_id (at offset 0, u64)
            *(wc_ptr as *mut u64) = 42;

            // Set status to SUCCESS (at offset 8, u32)
            *(wc_ptr.add(8) as *mut i32) = rdmaxcel_sys::ibv_wc_status::IBV_WC_SUCCESS as i32;
        }
        let ibv_wc = IbvWc::from(wc);
        assert_eq!(ibv_wc.wr_id(), 42);
        assert!(ibv_wc.is_valid());
    }

    #[test]
    fn test_format_gid() {
        let gid = [
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
            0x77, 0x88,
        ];

        let formatted = format_gid(&gid);
        assert_eq!(formatted, "1234:5678:9abc:def0:1122:3344:5566:7788");
    }

    #[test]
    fn test_mlx5dv_supported_basic() {
        // The test just verifies the function doesn't panic
        let mlx5dv_support = mlx5dv_supported();
        println!("mlx5dv_supported: {}", mlx5dv_support);
    }
}
