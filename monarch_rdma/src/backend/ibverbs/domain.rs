/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! ibverbs RDMA domain.
//!
//! An [`IbvDomain`] manages the device context and protection domain (PD)
//! required for RDMA operations. It provides the foundation for creating
//! queue pairs and establishing connections between RDMA devices.

use std::ffi::CStr;
use std::io::Error;
use std::result::Result;

use super::primitives::IbvDevice;

/// Manages RDMA resources including context and protection domain.
///
/// # Fields
///
/// * `context`: A pointer to the RDMA device context.
/// * `pd`: A pointer to the protection domain, which provides isolation between
///   different connections.
#[derive(Clone)]
pub struct IbvDomain {
    pub context: *mut rdmaxcel_sys::ibv_context,
    pub pd: *mut rdmaxcel_sys::ibv_pd,
}

impl std::fmt::Debug for IbvDomain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IbvDomain")
            .field("context", &format!("{:p}", self.context))
            .field("pd", &format!("{:p}", self.pd))
            .finish()
    }
}

// SAFETY:
// IbvDomain is `Send` because the raw pointers to ibverbs structs can be
// accessed from any thread, and it is safe to drop `IbvDomain` (and run the
// ibverbs destructors) from any thread.
unsafe impl Send for IbvDomain {}

// SAFETY:
// IbvDomain is `Sync` because the underlying ibverbs APIs are thread-safe.
unsafe impl Sync for IbvDomain {}

impl Drop for IbvDomain {
    fn drop(&mut self) {
        unsafe {
            rdmaxcel_sys::ibv_dealloc_pd(self.pd);
        }
    }
}

impl IbvDomain {
    /// Creates a new IbvDomain for the given device.
    ///
    /// Initializes the RDMA device context and creates a protection domain.
    ///
    /// SAFETY:
    /// Our memory region (MR) registration uses implicit ODP for RDMA access, which maps large virtual
    /// address ranges without explicit pinning. This is convenient, but it broadens the memory footprint
    /// exposed to the NIC and introduces a security liability.
    ///
    /// We currently assume a trusted, single-environment and are not enforcing finer-grained memory isolation
    /// at this layer. We plan to investigate mitigations - such as memory windows or tighter registration
    /// boundaries in future follow-ups.
    ///
    /// # Errors
    ///
    /// Returns errors if no RDMA devices are found, the specified device cannot be found,
    /// device context creation fails, or protection domain allocation fails.
    pub fn new(device: IbvDevice) -> Result<Self, anyhow::Error> {
        tracing::debug!("creating IbvDomain for device {}", device.name());
        unsafe {
            let device_name = device.name();
            let mut num_devices = 0i32;
            let devices = rdmaxcel_sys::ibv_get_device_list(&mut num_devices as *mut _);

            if devices.is_null() || num_devices == 0 {
                return Err(anyhow::anyhow!("no RDMA devices found"));
            }

            let mut device_ptr = std::ptr::null_mut();
            for i in 0..num_devices {
                let dev = *devices.offset(i as isize);
                let dev_name =
                    CStr::from_ptr(rdmaxcel_sys::ibv_get_device_name(dev)).to_string_lossy();

                if dev_name == *device_name {
                    device_ptr = dev;
                    break;
                }
            }

            if device_ptr.is_null() {
                rdmaxcel_sys::ibv_free_device_list(devices);
                return Err(anyhow::anyhow!("device '{}' not found", device_name));
            }
            tracing::info!("using RDMA device: {}", device_name);

            let context = rdmaxcel_sys::ibv_open_device(device_ptr);
            if context.is_null() {
                rdmaxcel_sys::ibv_free_device_list(devices);
                let os_error = Error::last_os_error();
                return Err(anyhow::anyhow!("failed to create context: {}", os_error));
            }

            let pd = rdmaxcel_sys::ibv_alloc_pd(context);
            if pd.is_null() {
                rdmaxcel_sys::ibv_close_device(context);
                rdmaxcel_sys::ibv_free_device_list(devices);
                let os_error = Error::last_os_error();
                return Err(anyhow::anyhow!(
                    "failed to create protection domain (PD): {}",
                    os_error
                ));
            }

            rdmaxcel_sys::ibv_free_device_list(devices);

            let domain = IbvDomain { context, pd };

            Ok(domain)
        }
    }
}
