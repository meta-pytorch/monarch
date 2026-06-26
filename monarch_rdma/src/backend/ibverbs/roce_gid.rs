/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! GID-index selection for mlx5 NICs.
//!
//! Every verbs QP needs a GID index, and rdma-core defaults to index 3 — the
//! value this backend used for InfiniBand-mode mlx5 NICs before auto-detection.
//! On RoCE v2 the routable GID usually lives at a different index and mlx5 does
//! no auto-selection, so QPs would otherwise come up on an unroutable GID.
//!
//! [`MlxDevice`](super::mlx_device::MlxDevice) seeds `config.gid_index` from
//! this helper in
//! [`apply_config_defaults`](super::device::IbvDeviceImpl::apply_config_defaults):
//! it auto-detects the routable RoCE v2 GID when one is present and otherwise
//! keeps rdma-core's default, leaving InfiniBand-mode NICs on their prior
//! behavior.

use std::sync::OnceLock;

/// rdma-core's default GID index, and the value this backend previously used for
/// InfiniBand-mode mlx5 NICs. Kept as the backward-compatible fallback when
/// neither an explicit override nor sysfs auto-detection yields a RoCE v2 GID
/// (e.g. an InfiniBand-mode mlx5 NIC, which has no `RoCE v2` GID entry).
const DEFAULT_GID_INDEX: u8 = 3;

/// Root of the InfiniBand sysfs tree, walked by [`detect_roce_v2_gid_index`]
/// for per-port GID values and types. Detection degrades gracefully (returns
/// `None`) if it is absent.
const IB_SYSFS_ROOT: &str = "/sys/class/infiniband";

/// The RoCE v2 GID index to seed into [`IbvConfig`](super::primitives::IbvConfig)
/// for mlx5 NICs, resolved once and cached for the process.
///
/// Resolution order:
/// 1. `MONARCH_IB_GID_INDEX` (mirrors `NCCL_IB_GID_INDEX`) — explicit override.
/// 2. sysfs auto-detection: the first routable RoCE v2 GID across the host's
///    mlx5 ports (identical HCAs on a node, so any one is representative).
/// 3. [`DEFAULT_GID_INDEX`] (rdma-core default) if nothing matches.
pub(super) fn select_roce_v2_gid_index() -> u8 {
    static CACHE: OnceLock<u8> = OnceLock::new();
    *CACHE.get_or_init(|| {
        if let Some(idx) = env_gid_index() {
            tracing::info!("mlx5 RoCE v2: using MONARCH_IB_GID_INDEX={idx}");
            return idx;
        }
        match detect_roce_v2_gid_index() {
            Some(idx) => {
                tracing::info!("mlx5 RoCE v2: auto-detected GID index {idx}");
                idx
            }
            None => {
                tracing::info!(
                    "mlx5 RoCE v2: no routable RoCE v2 GID in sysfs (InfiniBand \
                     mode?); using GID index {DEFAULT_GID_INDEX}. Set \
                     MONARCH_IB_GID_INDEX to override."
                );
                DEFAULT_GID_INDEX
            }
        }
    })
}

/// `MONARCH_IB_GID_INDEX` parsed as a `u8`, if set and valid.
fn env_gid_index() -> Option<u8> {
    std::env::var("MONARCH_IB_GID_INDEX")
        .ok()
        .and_then(|v| v.trim().parse().ok())
}

/// Walk every mlx5 port's GID table in sysfs and return the index of the first
/// routable RoCE v2 GID, or `None` if none is present (or sysfs is
/// unavailable).
///
/// For each device under [`IB_SYSFS_ROOT`], for each port, the GID table is
/// exposed as parallel files `ports/<p>/gid_attrs/types/<i>` (the GID type, e.g.
/// `"RoCE v2"`) and `ports/<p>/gids/<i>` (the GID value). We pick the lowest
/// index whose type is RoCE v2 and whose value is routable (see
/// [`is_routable_gid`]).
///
/// To list these tables by hand:
/// `cat /sys/class/infiniband/*/ports/*/gid_attrs/types/*` (types) and
/// `cat /sys/class/infiniband/*/ports/*/gids/*` (values).
fn detect_roce_v2_gid_index() -> Option<u8> {
    let devices = std::fs::read_dir(IB_SYSFS_ROOT).ok()?;
    for dev in devices.flatten() {
        let ports_dir = dev.path().join("ports");
        let Ok(ports) = std::fs::read_dir(&ports_dir) else {
            continue;
        };
        for port in ports.flatten() {
            if let Some(idx) = first_roce_v2_gid_in_port(&port.path()) {
                return Some(idx);
            }
        }
    }
    None
}

/// The lowest GID index on `port_dir` that is RoCE v2 and routable.
fn first_roce_v2_gid_in_port(port_dir: &std::path::Path) -> Option<u8> {
    let types_dir = port_dir.join("gid_attrs").join("types");
    let gids_dir = port_dir.join("gids");

    // Collect the numeric GID indices present, then scan in ascending order so
    // the choice is deterministic (read_dir order is unspecified).
    let mut indices: Vec<u8> = std::fs::read_dir(&types_dir)
        .ok()?
        .flatten()
        .filter_map(|e| e.file_name().to_str().and_then(|s| s.parse::<u8>().ok()))
        .collect();
    indices.sort_unstable();

    for i in indices {
        let gid_type = read_trimmed(&types_dir.join(i.to_string()));
        if gid_type.as_deref() != Some("RoCE v2") {
            continue;
        }
        if let Some(gid) = read_trimmed(&gids_dir.join(i.to_string()))
            && is_routable_gid(&gid)
        {
            return Some(i);
        }
    }
    None
}

/// Read a sysfs scalar file and trim trailing whitespace/newline.
fn read_trimmed(path: &std::path::Path) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_string())
}

/// Whether a sysfs GID string is usable as the source GID for routable RoCE v2
/// traffic.
///
/// A GID is accepted unless it falls into a scope that the InfiniBand
/// Architecture defines as reserved or non-routable — any of these would put a
/// QP on a GID that cannot reach a remote host. Primary source: *InfiniBand
/// Architecture Specification, Volume 1, Release 1.2.1* (Nov 2007),
/// section 4.1.1 "GID Usage and Properties", whose numbered items are cited
/// below:
///
/// - **All-zero** `::` — the Reserved GID (section 4.1.1 item 6): never
///   assigned, never used as a destination or in a GRH. mlx5 also leaves
///   unused GID-table slots all-zero, so this doubles as the "empty slot"
///   check.
/// - **Loopback** `::1` — the loopback GID (section 4.1.1 item 7): used only by
///   raw IPv6 services and never present in IBA packets, so it cannot appear in
///   a port's GID table.
/// - **Link-local** `fe80::/10` — routers must not forward packets with a
///   link-local source or destination GID outside the local subnet
///   (section 4.1.1 item 12a; format per Figure 39: leading `1111111010`).
///   This is the default GID prefix, concatenated with the port EUI-64.
/// - **Site-local** `fec0::/10` — routers must not forward packets with a
///   site-local source or destination GID outside the site (section 4.1.1
///   item 12b; format per Figure 40: leading `1111111011`). Deprecated by
///   [RFC 3879](https://datatracker.ietf.org/doc/html/rfc3879)
///   and not expected on RoCE fabrics, but filtered for completeness as it
///   is likewise not globally routable.
///
/// Everything else is treated as routable: IPv4-mapped GIDs
/// (`::ffff:a.b.c.d`) and global / ULA IPv6 (e.g. `fd00::/8`), which the spec
/// predates but RoCE L3 routes like any global address.
///
/// Assumes sysfs's fully-expanded 8-group GID format (no `::` compression),
/// which is what `/sys/class/infiniband/.../gids/<i>` always emits.
fn is_routable_gid(gid: &str) -> bool {
    let g = gid.trim();
    if g.is_empty() {
        return false;
    }
    let segs: Vec<&str> = g.split(':').collect();

    // Reserved all-zero (`::`) and loopback (`::1`): every group is zero, except
    // that loopback's final group is `1`. `trim_matches('0')` reduces "0000" ->
    // "" so the leading groups collapse to empty; the final group is parsed so
    // that only an exact `1` matches (not e.g. "0010").
    let leading_all_zero = segs
        .iter()
        .take(segs.len().saturating_sub(1))
        .all(|seg| seg.trim_matches('0').is_empty());
    if leading_all_zero {
        match segs.last() {
            // `::` — Reserved GID.
            Some(last) if last.trim_matches('0').is_empty() => return false,
            // `::1` — loopback GID.
            Some(last) if u16::from_str_radix(last, 16) == Ok(1) => return false,
            _ => {}
        }
    }

    // Link-local (`fe80::/10`) and site-local (`fec0::/10`) share the top 10
    // bits `1111111010` / `1111111011`; masking with 0xffc0 isolates them as
    // 0xfe80 / 0xfec0.
    let lead = u16::from_str_radix(segs.first().unwrap_or(&""), 16).unwrap_or(0);
    let scope = lead & 0xffc0;
    scope != 0xfe80 && scope != 0xfec0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routable_gid_filters_zero_and_link_local() {
        // All-zero (reserved GID / unused slot).
        assert!(!is_routable_gid("0000:0000:0000:0000:0000:0000:0000:0000"));
        // Loopback ::1 (reserved).
        assert!(!is_routable_gid("0000:0000:0000:0000:0000:0000:0000:0001"));
        // ::10 is not loopback — only an exact ::1 is filtered here.
        assert!(is_routable_gid("0000:0000:0000:0000:0000:0000:0000:0010"));
        // Link-local fe80::/10.
        assert!(!is_routable_gid("fe80:0000:0000:0000:0a00:27ff:fe00:0001"));
        // Site-local fec0::/10 (deprecated, but not globally routable).
        assert!(!is_routable_gid("fec0:0000:0000:0000:0a00:27ff:fe00:0001"));
        // IPv4-mapped RoCE v2 (routable).
        assert!(is_routable_gid("0000:0000:0000:0000:0000:ffff:c0a8:0101"));
        // Global / ULA IPv6 fd00::/8 (routable).
        assert!(is_routable_gid("fd02:0000:0000:0000:0000:0000:0000:0007"));
        // Empty.
        assert!(!is_routable_gid(""));
    }

    #[test]
    fn env_override_parses() {
        // Not set in the test harness → None (best-effort; harmless if set).
        if std::env::var("MONARCH_IB_GID_INDEX").is_err() {
            assert_eq!(env_gid_index(), None);
        }
    }
}
