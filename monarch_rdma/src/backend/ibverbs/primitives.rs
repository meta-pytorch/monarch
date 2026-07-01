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
//! - `IbvConfig`: Represents ibverbs specific configurations, holding parameters required to establish and
//!   manage an RDMA connection, including settings for the RDMA device, queue pair attributes, and other
//!   connection-specific parameters.
//! - `IbvDeviceInfo`: Represents an RDMA device, i.e. 'mlx5_0'. Contains information about the device, such as:
//!   its name, vendor ID, vendor part ID, hardware version, firmware version, node GUID, and capabilities.
//! - `IbvPort`: Represents information about the port of an RDMA device, including state, physical state,
//!   LID (Local Identifier), and GID (Global Identifier) information.
//! - `IbvOperation`: Represents the type of RDMA operation to perform (Read or Write).
//! - `IbvQpInfo`: Contains connection information needed to establish an RDMA connection with a remote endpoint.
//! - `IbvWc`: Wrapper around ibverbs work completion structure, used to track the status of RDMA operations.
use std::ffi::CStr;
use std::fmt;
use std::io::Error;
use std::sync::OnceLock;

use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::backend::ibverbs::device::IbvDeviceImpl;
use crate::backend::ibverbs::device::list_all_devices;
use crate::backend::ibverbs::device_selection::IbvDeviceTarget;
use crate::backend::ibverbs::device_selection::resolve_target;
use crate::device_selection::MemoryLocation;

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
pub enum IbvQpType {
    /// Auto-detect based on device capabilities
    Auto,
    /// Force standard ibverbs queue pair
    Standard,
    /// Force mlx5dv extended queue pair
    Mlx5dv,
    /// Force EFA SRD queue pair
    Efa,
}

/// Converts `IbvQpType` to the corresponding integer enum value in rdmaxcel_sys.
pub fn resolve_qp_type(qp_type: IbvQpType) -> u32 {
    match qp_type {
        IbvQpType::Auto => {
            if crate::efa::is_efa_device() {
                rdmaxcel_sys::RDMA_QP_TYPE_EFA
            } else if mlx5dv_supported() {
                rdmaxcel_sys::RDMA_QP_TYPE_MLX5DV
            } else {
                rdmaxcel_sys::RDMA_QP_TYPE_STANDARD
            }
        }
        IbvQpType::Standard => rdmaxcel_sys::RDMA_QP_TYPE_STANDARD,
        IbvQpType::Mlx5dv => rdmaxcel_sys::RDMA_QP_TYPE_MLX5DV,
        IbvQpType::Efa => rdmaxcel_sys::RDMA_QP_TYPE_EFA,
    }
}

/// Represents ibverbs specific configurations.
///
/// This struct holds various parameters required to establish and manage an RDMA connection.
/// It includes settings for the RDMA device, queue pair attributes, and other connection-specific
/// parameters.
#[derive(Debug, Named, Clone, Serialize, Deserialize)]
pub struct IbvConfig {
    /// `target` - Which RDMA device to use. A consumer resolves this to a
    /// concrete device for its backend via [`resolve_target`].
    pub target: IbvDeviceTarget,
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
    pub qp_type: IbvQpType,
    /// Test-only override for `register_segments`'s `max_sge`. `<= 0`
    /// (default) uses `ibv_query_device`; small positive values force
    /// `RDMAXCEL_MKEY_REG_LIMIT` to exercise the dmabuf fallback.
    pub max_sge_override: i32,
}
wirevalue::register_type!(IbvConfig);

/// rdma-core defaults below come from common rdma-core examples; tune for
/// production based on `ibv_query_device()` results and workload
/// characteristics. The default target is CPU NUMA node 0; a consumer
/// resolves it to a concrete device for its backend via [`resolve_target`].
impl Default for IbvConfig {
    fn default() -> Self {
        Self {
            target: IbvDeviceTarget::MemoryLocation(MemoryLocation::Cpu(Some(0))),
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
            qp_type: IbvQpType::Auto,
            max_sge_override: 0,
        }
    }
}

impl IbvConfig {
    /// An [`IbvConfig`] with default parameters whose device
    /// [`target`](Self::target) is `target` (see [`IbvDeviceTarget`]).
    pub fn targeting(target: IbvDeviceTarget) -> Self {
        Self {
            target,
            ..Default::default()
        }
    }
}

impl std::fmt::Display for IbvConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "IbvConfig {{ target: {:?}, port_num: {}, gid_index: {}, max_send_wr: {}, max_recv_wr: {}, max_send_sge: {}, max_recv_sge: {}, path_mtu: {:?}, retry_cnt: {}, rnr_retry: {}, qp_timeout: {}, min_rnr_timer: {}, max_dest_rd_atomic: {}, max_rd_atomic: {}, pkey_index: {}, psn: 0x{:x} }}",
            self.target,
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
/// use monarch_rdma::backend::ibverbs::device::list_all_devices;
///
/// let devices = list_all_devices();
/// if let Some(device) = devices.first() {
///     // Access device name and firmware version
///     let device_name = device.name();
///     let firmware_version = device.fw_ver();
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IbvDeviceInfo {
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
    ports: Vec<IbvPort>,
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

impl IbvDeviceInfo {
    /// Returns the name of the RDMA device.
    pub fn name(&self) -> &String {
        &self.name
    }

    /// Returns the first available RDMA device, if any.
    pub fn first_available() -> Option<IbvDeviceInfo> {
        list_all_devices().into_iter().next()
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
    pub fn ports(&self) -> &Vec<IbvPort> {
        &self.ports
    }

    /// Aggregate bandwidth (MB/s) of the device's fastest active port,
    /// derived from its IB `active_speed` / `active_width`. 0 if no port
    /// is active, which ranks the device at the worst case.
    pub fn port_speed_mbytes_per_sec(&self) -> u32 {
        self.ports
            .iter()
            .filter(|port| port.state == rdmaxcel_sys::ibv_port_state::IBV_PORT_ACTIVE)
            .map(|port| {
                ib_width_lanes(port.active_width) * ib_speed_mbits_per_lane(port.active_speed) / 8
            })
            .max()
            .unwrap_or(0)
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

impl IbvDeviceInfo {
    /// The optimal default device of backend `I`: the best NIC for CPU
    /// memory on any NUMA node. Panics if `I` has no devices.
    #[expect(
        clippy::should_implement_trait,
        reason = "generic over the backend impl, so it cannot be the parameterless Default::default"
    )]
    pub fn default<I: IbvDeviceImpl>() -> Self {
        resolve_target::<I>(&IbvDeviceTarget::MemoryLocation(MemoryLocation::Cpu(None)))
            .unwrap_or_else(|| panic!("no RDMA device for backend {}", I::backend_name()))
    }

    /// Construct an [`IbvDeviceInfo`] with only `name` set (all other
    /// fields zeroed/empty), for tests that need a named device without
    /// touching hardware.
    #[cfg(test)]
    pub(crate) fn for_test_named(name: &str) -> Self {
        Self {
            name: name.to_string(),
            vendor_id: 0,
            vendor_part_id: 0,
            hw_ver: 0,
            fw_ver: String::new(),
            node_guid: 0,
            ports: Vec::new(),
            max_qp: 0,
            max_cq: 0,
            max_mr: 0,
            max_pd: 0,
            max_qp_wr: 0,
            max_sge: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IbvPort {
    /// `port_num` - The physical port number on the device.
    port_num: u8,
    /// `state` - The raw `ibv_port_state` of the port.
    state: rdmaxcel_sys::ibv_port_state::Type,
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
    /// `active_speed` - IB active speed bitmask (one bit set).
    active_speed: u8,
    /// `active_width` - IB active width bitmask (one bit set).
    active_width: u8,
}

impl fmt::Display for IbvDeviceInfo {
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

impl fmt::Display for IbvPort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "\tPort {}:", self.port_num)?;
        writeln!(f, "\t\tState: {}", get_port_state_str(self.state))?;
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

/// Per-lane IB bandwidth (Mbit/s) for an `active_speed` bitmask,
/// indexed by its lowest set bit (SDR, DDR, QDR, QDR, FDR, EDR, HDR, NDR).
/// Values match NCCL's `ibvSpeeds` (`transport/net_ib/init.cc`). The
/// `active_speed` field is a `u8`, so NCCL's 9th rate (XDR, bit 8) is not
/// representable here; an unset value yields 0.
fn ib_speed_mbits_per_lane(active_speed: u8) -> u32 {
    const RATES: [u32; 8] = [2500, 5000, 10000, 10000, 14000, 25000, 50000, 100000];
    first_set_bit(active_speed)
        .and_then(|bit| RATES.get(bit).copied())
        .unwrap_or(0)
}

/// IB link width in lanes for an `active_width` bitmask, indexed by its
/// lowest set bit (1x, 4x, 8x, 12x, 2x); values match NCCL's `ibvWidths`
/// (`transport/net_ib/init.cc`). An unset value yields 0.
fn ib_width_lanes(active_width: u8) -> u32 {
    const WIDTHS: [u32; 5] = [1, 4, 8, 12, 2];
    first_set_bit(active_width)
        .and_then(|bit| WIDTHS.get(bit).copied())
        .unwrap_or(0)
}

/// Index of the lowest set bit, or `None` if `v` is 0.
fn first_set_bit(v: u8) -> Option<usize> {
    (v != 0).then(|| v.trailing_zeros() as usize)
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

/// Builds an [`IbvDeviceInfo`] from an already-open
/// `ibv_context`. Returns `None` if `ibv_query_device` fails.
///
/// # Safety
///
/// `device` and `context` must both be non-null and valid for
/// the duration of the call; `context` must be the result of
/// `ibv_open_device(device)`.
pub(super) unsafe fn query_device_info(
    device: *mut rdmaxcel_sys::ibv_device,
    context: *mut rdmaxcel_sys::ibv_context,
) -> Option<IbvDeviceInfo> {
    // SAFETY: `device` is non-null per the caller's contract;
    // `ibv_get_device_name` returns a null-terminated C string
    // owned by the device list.
    let device_name = unsafe { CStr::from_ptr(rdmaxcel_sys::ibv_get_device_name(device)) }
        .to_string_lossy()
        .into_owned();
    let mut device_attr = rdmaxcel_sys::ibv_device_attr::default();
    // SAFETY: `context` is a non-null context per the caller's
    // contract; `&mut device_attr` is a writable, properly
    // aligned `ibv_device_attr`.
    if unsafe { rdmaxcel_sys::ibv_query_device(context, &mut device_attr) } != 0 {
        return None;
    }
    // SAFETY: `device_attr.fw_ver` is a null-terminated C buffer
    // populated by `ibv_query_device`.
    let fw_ver = unsafe { CStr::from_ptr(device_attr.fw_ver.as_ptr()) }
        .to_string_lossy()
        .into_owned();
    let mut info = IbvDeviceInfo {
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
        // SAFETY: `context` is a valid context; `port_attr` is
        // a writable, properly aligned `ibv_port_attr`.
        if unsafe {
            rdmaxcel_sys::ibv_query_port(
                context,
                port_num,
                &mut port_attr as *mut rdmaxcel_sys::ibv_port_attr as *mut _,
            )
        } != 0
        {
            continue;
        }
        let physical_state = get_port_phy_state_str(port_attr.phys_state);
        let link_layer = get_link_layer_str(port_attr.link_layer);
        let mut gid = rdmaxcel_sys::ibv_gid::default();
        // SAFETY: `context` is a valid context; `&mut gid` is a
        // writable, properly aligned `ibv_gid`.
        let gid_str = if unsafe { rdmaxcel_sys::ibv_query_gid(context, port_num, 0, &mut gid) } == 0
        {
            // SAFETY: `gid.raw` is a union field that is
            // always initialized; `ibv_query_gid` filled it.
            let raw = unsafe { gid.raw };
            format_gid(&raw)
        } else {
            "N/A".to_string()
        };
        info.ports.push(IbvPort {
            port_num,
            state: port_attr.state,
            physical_state,
            base_lid: port_attr.lid,
            lmc: port_attr.lmc,
            sm_lid: port_attr.sm_lid,
            capability_mask: port_attr.port_cap_flags,
            link_layer,
            gid: gid_str,
            gid_tbl_len: port_attr.gid_tbl_len,
            active_speed: port_attr.active_speed,
            active_width: port_attr.active_width,
        });
    }
    Some(info)
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
pub enum IbvOperation {
    /// RDMA write operations
    Write,
    WriteWithImm,
    /// RDMA read operation
    Read,
    /// RDMA recv operation
    Recv,
}

impl From<IbvOperation> for rdmaxcel_sys::ibv_wr_opcode::Type {
    fn from(op: IbvOperation) -> Self {
        match op {
            IbvOperation::Write => rdmaxcel_sys::ibv_wr_opcode::IBV_WR_RDMA_WRITE,
            IbvOperation::WriteWithImm => rdmaxcel_sys::ibv_wr_opcode::IBV_WR_RDMA_WRITE_WITH_IMM,
            IbvOperation::Read => rdmaxcel_sys::ibv_wr_opcode::IBV_WR_RDMA_READ,
            IbvOperation::Recv => panic!("Invalid wr opcode"),
        }
    }
}

impl From<rdmaxcel_sys::ibv_wc_opcode::Type> for IbvOperation {
    fn from(op: rdmaxcel_sys::ibv_wc_opcode::Type) -> Self {
        match op {
            rdmaxcel_sys::ibv_wc_opcode::IBV_WC_RDMA_WRITE => IbvOperation::Write,
            rdmaxcel_sys::ibv_wc_opcode::IBV_WC_RDMA_READ => IbvOperation::Read,
            _ => panic!("Unsupported operation type"),
        }
    }
}

/// Contains information needed to establish an RDMA queue pair with a remote endpoint.
///
/// `IbvQpInfo` encapsulates all the necessary information to establish a queue pair
/// with a remote RDMA device. This includes queue pair number, LID (Local Identifier),
/// GID (Global Identifier), remote memory address, remote key, and packet sequence number.
#[derive(Default, Named, Clone, serde::Serialize, serde::Deserialize)]
pub struct IbvQpInfo {
    /// `qp_num` - Queue Pair Number, uniquely identifies a queue pair on the remote device
    pub qp_num: u32,
    /// `lid` - Local Identifier, used for addressing in InfiniBand subnet
    pub lid: u16,
    /// `gid` - Global Identifier, used for routing across subnets (similar to IPv6 address)
    pub gid: Option<Gid>,
    /// `psn` - Packet Sequence Number, used for ordering packets
    pub psn: u32,
}
wirevalue::register_type!(IbvQpInfo);

impl std::fmt::Debug for IbvQpInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "IbvQpInfo {{ qp_num: {}, lid: {}, gid: {:?}, psn: 0x{:x} }}",
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

    #[cfg(test)]
    pub(super) fn for_test(wr_id: u64, valid: bool) -> Self {
        Self {
            wr_id,
            len: 0,
            valid,
            error: None,
            opcode: rdmaxcel_sys::ibv_wc_opcode::IBV_WC_RDMA_WRITE,
            bytes: None,
            qp_num: 0,
            src_qp: 0,
            pkey_index: 0,
            slid: 0,
            sl: 0,
            dlid_path_bits: 0,
        }
    }
}

/// Owns an `ibv_cq`, destroying it on drop (a no-op if null). Lets callers
/// create completion queues without hand-rolling cleanup on the error or drop
/// paths.
#[derive(Debug)]
pub(super) struct IbvCq(*mut rdmaxcel_sys::ibv_cq);

// SAFETY: the only field is the raw `ibv_cq` pointer. The ibverbs CQ it names is
// not thread-affine — it may be created on one thread and used or destroyed on
// another (`Send`) — and `IbvCq` exposes no operation that mutates the CQ through
// a shared `&` (`as_ptr` only hands back the pointer value), so sharing a
// `&IbvCq` cannot race (`Sync`).
unsafe impl Send for IbvCq {}
// SAFETY: as for `Send` above.
unsafe impl Sync for IbvCq {}

impl IbvCq {
    /// Creates a completion queue with `cq_entries` entries on `context`.
    ///
    /// # Safety
    ///
    /// `context`, if non-null, must be a live `ibv_context`; a null `context`
    /// yields `Err`.
    pub(super) unsafe fn create(
        context: *mut rdmaxcel_sys::ibv_context,
        cq_entries: i32,
    ) -> Result<Self, anyhow::Error> {
        if context.is_null() {
            anyhow::bail!("cannot create a completion queue on a null context");
        }
        // SAFETY: `context` is non-null (checked above) and live (caller
        // contract); `ibv_create_cq` returns null on failure.
        let cq = unsafe {
            rdmaxcel_sys::ibv_create_cq(
                context,
                cq_entries,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                0,
            )
        };
        if cq.is_null() {
            anyhow::bail!(
                "failed to create completion queue: {}",
                Error::last_os_error()
            );
        }
        Ok(Self(cq))
    }

    /// The raw `ibv_cq`; null for a placeholder that holds no queue.
    pub(super) fn as_ptr(&self) -> *mut rdmaxcel_sys::ibv_cq {
        self.0
    }

    /// A placeholder holding no completion queue: `as_ptr` returns null and
    /// `Drop` is a no-op.
    #[cfg(test)]
    pub(super) fn null() -> Self {
        Self(std::ptr::null_mut())
    }
}

impl Drop for IbvCq {
    fn drop(&mut self) {
        if self.0.is_null() {
            return;
        }
        // SAFETY: a non-null `self.0` was returned by `ibv_create_cq` and, since
        // `IbvCq` is not `Clone`, is destroyed exactly once.
        let ret = unsafe { rdmaxcel_sys::ibv_destroy_cq(self.0) };
        if ret != 0 {
            tracing::error!(
                "failed to destroy completion queue {:p}: error code {}",
                self.0,
                ret
            );
        }
    }
}

/// Owns an `ibv_qp`, destroying it on drop (a no-op if null), so a queue pair is
/// carried by value rather than as a raw pointer and its destruction runs even
/// on an early return or panic.
#[derive(Debug)]
pub(super) struct IbvQp(*mut rdmaxcel_sys::ibv_qp);

// SAFETY: the only field is the raw `ibv_qp` pointer. The ibverbs QP it names is
// not thread-affine — it may be created on one thread and used or destroyed on
// another (`Send`) — and `IbvQp` exposes no operation that mutates the QP through
// a shared `&` (`as_ptr` only hands back the pointer value), so sharing a
// `&IbvQp` cannot race (`Sync`).
unsafe impl Send for IbvQp {}
// SAFETY: as for `Send` above.
unsafe impl Sync for IbvQp {}

impl IbvQp {
    /// Takes ownership of a raw `ibv_qp`, destroying it on drop.
    ///
    /// # Safety
    ///
    /// `qp`, if non-null, must be a live `ibv_qp` owned solely by the returned
    /// value (its `Drop` calls `ibv_destroy_qp` once).
    pub(super) unsafe fn from_raw(qp: *mut rdmaxcel_sys::ibv_qp) -> Self {
        Self(qp)
    }

    /// The raw `ibv_qp`; null for a placeholder that holds no queue pair.
    pub(super) fn as_ptr(&self) -> *mut rdmaxcel_sys::ibv_qp {
        self.0
    }

    /// A placeholder holding no queue pair: `as_ptr` returns null and `Drop` is
    /// a no-op.
    #[cfg(test)]
    pub(super) fn null() -> Self {
        Self(std::ptr::null_mut())
    }
}

impl Drop for IbvQp {
    fn drop(&mut self) {
        if self.0.is_null() {
            return;
        }
        // SAFETY: a non-null `self.0` was handed to `from_raw` as a live `ibv_qp`
        // and, since `IbvQp` is not `Clone`, is destroyed exactly once.
        let ret = unsafe { rdmaxcel_sys::ibv_destroy_qp(self.0) };
        if ret != 0 {
            tracing::error!(
                "failed to destroy queue pair {:p}: error code {}",
                self.0,
                ret
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_all_devices() {
        // Skip test if RDMA devices are not available
        let devices = list_all_devices();
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
        let devices = list_all_devices();
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
        if let Some(device) = IbvDeviceInfo::first_available() {
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
        if let Some(device) = IbvDeviceInfo::first_available()
            && !device.ports().is_empty()
        {
            let port = &device.ports()[0];
            let display_output = format!("{}", port);
            assert!(
                display_output.contains(&get_port_state_str(port.state)),
                "display should include port state"
            );
            assert!(
                display_output.contains(&port.link_layer),
                "display should include link layer"
            );
        }
    }

    #[test]
    fn test_ib_speed_mbits_per_lane() {
        // `active_speed` is a one-hot bitmask, indexed by its lowest set
        // bit. Values mirror NCCL's `ibvSpeeds` (SDR..NDR).
        assert_eq!(ib_speed_mbits_per_lane(1), 2500); // SDR
        assert_eq!(ib_speed_mbits_per_lane(2), 5000); // DDR
        assert_eq!(ib_speed_mbits_per_lane(4), 10000); // QDR
        assert_eq!(ib_speed_mbits_per_lane(8), 10000); // QDR / FDR10
        assert_eq!(ib_speed_mbits_per_lane(16), 14000); // FDR
        assert_eq!(ib_speed_mbits_per_lane(32), 25000); // EDR
        assert_eq!(ib_speed_mbits_per_lane(64), 50000); // HDR
        assert_eq!(ib_speed_mbits_per_lane(128), 100000); // NDR
        assert_eq!(ib_speed_mbits_per_lane(0), 0); // unset → unknown
    }

    #[test]
    fn test_ib_width_lanes() {
        assert_eq!(ib_width_lanes(1), 1); // 1x
        assert_eq!(ib_width_lanes(2), 4); // 4x
        assert_eq!(ib_width_lanes(4), 8); // 8x
        assert_eq!(ib_width_lanes(8), 12); // 12x
        assert_eq!(ib_width_lanes(16), 2); // 2x
        assert_eq!(ib_width_lanes(0), 0); // unset → unknown
    }

    #[test]
    fn test_port_speed_mbytes_per_sec() {
        use rdmaxcel_sys::ibv_port_state::IBV_PORT_ACTIVE;
        use rdmaxcel_sys::ibv_port_state::IBV_PORT_DOWN;
        fn mk_port(
            state: rdmaxcel_sys::ibv_port_state::Type,
            active_speed: u8,
            active_width: u8,
        ) -> IbvPort {
            IbvPort {
                port_num: 1,
                state,
                physical_state: String::new(),
                base_lid: 0,
                lmc: 0,
                sm_lid: 0,
                capability_mask: 0,
                link_layer: String::new(),
                gid: String::new(),
                gid_tbl_len: 0,
                active_speed,
                active_width,
            }
        }
        fn mk_device(ports: Vec<IbvPort>) -> IbvDeviceInfo {
            IbvDeviceInfo {
                name: "test".to_string(),
                vendor_id: 0,
                vendor_part_id: 0,
                hw_ver: 0,
                fw_ver: String::new(),
                node_guid: 0,
                ports,
                max_qp: 0,
                max_cq: 0,
                max_mr: 0,
                max_pd: 0,
                max_qp_wr: 0,
                max_sge: 0,
            }
        }

        // NDR (128) x4 (width bit 2): 100000 * 4 / 8 = 50000 MB/s.
        assert_eq!(
            mk_device(vec![mk_port(IBV_PORT_ACTIVE, 128, 2)]).port_speed_mbytes_per_sec(),
            50000
        );

        // The fastest port is DOWN, so it is ignored; the result is the
        // fastest ACTIVE port, not the (faster) down one.
        let mixed = mk_device(vec![
            mk_port(IBV_PORT_ACTIVE, 32, 2), // EDR x4 = 12500
            mk_port(IBV_PORT_ACTIVE, 64, 2), // HDR x4 = 25000 (fastest ACTIVE)
            mk_port(IBV_PORT_DOWN, 128, 2),  // NDR x4 = 50000, but DOWN → ignored
        ]);
        assert_eq!(mixed.port_speed_mbytes_per_sec(), 25000);

        // No ACTIVE port → 0 (treated as unknown, no cap).
        assert_eq!(
            mk_device(vec![mk_port(IBV_PORT_DOWN, 128, 2)]).port_speed_mbytes_per_sec(),
            0
        );
    }

    #[test]
    fn test_rdma_operation_conversion() {
        assert_eq!(
            rdmaxcel_sys::ibv_wr_opcode::IBV_WR_RDMA_WRITE,
            rdmaxcel_sys::ibv_wr_opcode::Type::from(IbvOperation::Write)
        );
        assert_eq!(
            rdmaxcel_sys::ibv_wr_opcode::IBV_WR_RDMA_READ,
            rdmaxcel_sys::ibv_wr_opcode::Type::from(IbvOperation::Read)
        );

        assert_eq!(
            IbvOperation::Write,
            IbvOperation::from(rdmaxcel_sys::ibv_wc_opcode::IBV_WC_RDMA_WRITE)
        );
        assert_eq!(
            IbvOperation::Read,
            IbvOperation::from(rdmaxcel_sys::ibv_wc_opcode::IBV_WC_RDMA_READ)
        );
    }

    #[test]
    fn test_rdma_endpoint() {
        let endpoint = IbvQpInfo {
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
