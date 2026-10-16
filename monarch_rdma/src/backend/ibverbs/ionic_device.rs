/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! AMD Pensando (ionic) backend for [`IbvDevice`].

use std::sync::Arc;

use typeuri::Named;

use super::device::IbvDeviceImpl;
use super::ionic_domain::IonicDomain;
use super::primitives::IbvConfig;
use super::primitives::IbvContext;
use super::primitives::IbvQpType;
use crate::register_ibv_device_impl;

/// PCI vendor ID for AMD Pensando Systems.
pub(super) const PENSANDO_VENDOR_ID: u32 = 0x1dd8;

/// Scatter/gather entries per work request that ionic devices accept
/// (`ibv_query_device` reports `max_sge = 8`, below the generic default of
/// 30). [`IonicDomain`] re-checks against the queried device limit when it
/// builds a queue pair.
pub(super) const IONIC_MAX_SGE: u32 = 8;

/// AMD Pensando AINIC (`ionic` provider) backend. RoCE v2, standard RC queue
/// pairs, host and dmabuf memory registration.
#[derive(Debug, Named)]
pub struct IonicDevice;

impl IonicDevice {
    /// Whether a device reporting PCI vendor `vendor_id` is an ionic device.
    fn is_ionic_vendor(vendor_id: u32) -> bool {
        vendor_id == PENSANDO_VENDOR_ID
    }
}

impl IbvDeviceImpl for IonicDevice {
    type Domain = IonicDomain;

    fn backend_name() -> &'static str {
        "ionic"
    }

    fn is_instance(ctx: Arc<IbvContext>) -> bool {
        let mut attr = rdmaxcel_sys::ibv_device_attr::default();
        // SAFETY: `ctx.as_ptr()` is a non-null context owned by
        // the `Arc<IbvContext>` for the duration of this call;
        // `&mut attr` is a writable, properly aligned
        // `ibv_device_attr`.
        let queried = unsafe { rdmaxcel_sys::ibv_query_device(ctx.as_ptr(), &mut attr) } == 0;
        queried && Self::is_ionic_vendor(attr.vendor_id)
    }

    /// Seeds ionic limits over the generic defaults. Checked against
    /// `ibv_devinfo -v` on AINIC 25.08 (part 4099):
    ///
    /// - `qp_type`: ionic has no mlx5dv or EFA verbs. `Auto` resolves from a
    ///   process-wide probe of whichever ibverbs device enumerates first, so
    ///   on a host that also has an mlx5 NIC it can pick mlx5dv for ionic.
    ///   Only the legacy queue-pair path reads it; [`IonicDomain`] always
    ///   builds a standard RC queue pair.
    /// - `max_send_sge`/`max_recv_sge`: default 30 exceeds the device's
    ///   `max_sge` of 8, and `ibv_create_qp` rejects the excess.
    ///
    /// The other defaults already fit and are left alone: `max_send_wr` /
    /// `max_recv_wr` 512 (device `max_qp_wr` 65535, `max_cqe` 65435),
    /// `path_mtu` 4096 (device `max_mtu`/`active_mtu` 4096),
    /// `max_rd_atomic`/`max_dest_rd_atomic` 16 (device `max_qp_init_rd_atom` /
    /// `max_qp_rd_atom` 16). The source GID is chosen at queue-pair creation as
    /// the first global RoCE v2 GID (index 1 on these ports), not from config.
    fn apply_config_defaults(config: &mut IbvConfig) {
        config.qp_type = IbvQpType::Standard;
        config.max_send_sge = config.max_send_sge.min(IONIC_MAX_SGE);
        config.max_recv_sge = config.max_recv_sge.min(IONIC_MAX_SGE);
    }

    /// On AINIC 25.08 (driver 25.08.4.004, fw 1.117.1-a-63) an RDMA READ whose
    /// local buffer is GPU (dmabuf) memory completes successfully but leaves
    /// that buffer unchanged, for every size and GPU allocation kind. READs
    /// into host memory and WRITEs into GPU memory work.
    fn supports_read_into_gpu() -> bool {
        false
    }
}

register_ibv_device_impl!(IonicDevice);
