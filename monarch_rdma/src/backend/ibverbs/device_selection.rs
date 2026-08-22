/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! ibverbs-specific device selection: pairs a [`MemoryLocation`] with the
//! RDMA NIC(s) that have the best PCIe path to it.

use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::LazyLock;

use anyhow::Context;
use anyhow::Result;
use dashmap::DashMap;
use dashmap::DashSet;

use super::device::IbvDevice;
use super::device::IbvDeviceImpl;
use super::primitives::IbvDeviceInfo;
use crate::device_selection::MemoryLocation;
use crate::device_selection::PCIAddress;
use crate::device_selection::PciPath;
use crate::device_selection::cpu_path;
use crate::device_selection::cuda_device_count;
use crate::device_selection::cuda_pci_address;
use crate::device_selection::pci_path;

/// What an [`IbvConfig`](super::primitives::IbvConfig) targets: a memory
/// location (whose best NIC is auto-selected) or an explicit NIC by name.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum IbvDeviceTarget {
    /// Auto-select the best NIC for a CPU/GPU memory location.
    MemoryLocation(MemoryLocation),
    /// Use the NIC with this exact device name (e.g. `"mlx5_0"`).
    Nic(String),
}

impl IbvDeviceTarget {
    /// Target the best NIC for CPU memory on NUMA node `numa`.
    pub fn cpu(numa: u32) -> Self {
        Self::MemoryLocation(MemoryLocation::Cpu(Some(numa)))
    }

    /// Target the best NIC for GPU memory on CUDA ordinal `ordinal`.
    pub fn gpu(ordinal: u32) -> Self {
        Self::MemoryLocation(MemoryLocation::Gpu(Some(ordinal)))
    }

    /// Target the NIC with the given device name.
    pub fn nic(name: impl Into<String>) -> Self {
        Self::Nic(name.into())
    }
}

impl FromStr for IbvDeviceTarget {
    type Err = anyhow::Error;

    fn from_str(spec: &str) -> Result<Self> {
        let (kind, value) = spec.split_once(':').with_context(|| {
            format!(
                "ibverbs target {spec:?} must be `cpu:<numa>`, `gpu:<ordinal>`, or `nic:<name>`"
            )
        })?;

        let parse_index = |what: &str| -> Result<u32> {
            value
                .parse()
                .with_context(|| format!("{what} {value:?} must be an integer in 0..={}", u32::MAX))
        };

        match kind {
            "cpu" => Ok(Self::cpu(parse_index("cpu target NUMA node")?)),
            "gpu" => Ok(Self::gpu(parse_index("gpu target ordinal")?)),
            "nic" => {
                anyhow::ensure!(
                    !value.is_empty(),
                    "nic target must name a device, for example `nic:mlx5_0`"
                );
                Ok(Self::nic(value))
            }
            other => anyhow::bail!(
                "unknown ibverbs target kind {other:?}; expected `cpu`, `gpu`, or `nic`"
            ),
        }
    }
}

/// Return the configured ibverbs target, or `None` when it is unset.
///
/// Returns an error when the non-empty value is malformed.
pub(crate) fn configured_ibverbs_target() -> Result<Option<IbvDeviceTarget>> {
    let spec = hyperactor_config::global::get_cloned(crate::config::RDMA_IBVERBS_TARGET);
    let spec = spec.trim();
    if spec.is_empty() {
        return Ok(None);
    }
    let target = spec.parse().map_err(|error| {
        anyhow::anyhow!(
            "invalid RDMA_IBVERBS_TARGET (`rdma_ibverbs_target` in Python) value {spec:?}: {error}"
        )
    })?;
    Ok(Some(target))
}

/// Which of a peer's NICs a local NIC is allowed to pair with for a transfer.
///
/// Two NICs both being up does not mean the fabric carries traffic between
/// them. For example, some fabrics may be organized into planes, where NICs
/// can only talk to other NICs in the same plane. Instead of trying to add
/// complex, dynamic network topology detection into monarch, it's much easier
/// to simply let the user tell us the topology of their network. This is
/// configurable using the config attr.
/// [`RDMA_PEER_DEVICE_AFFINITY`](crate::config::RDMA_PEER_DEVICE_AFFINITY).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerDeviceAffinityPolicy {
    /// Every local NIC reaches every peer NIC.
    Any,
    /// A local NIC reaches a peer NIC only if the two carry the same device
    /// name.
    MatchName,
    /// A local NIC reaches a peer NIC only where some group names both. A NIC
    /// no group names reaches nothing. There may be any number of groups, of
    /// any size, and they are disjoint, so a NIC belongs to at most one.
    Groups(Vec<BTreeSet<String>>),
}

impl PeerDeviceAffinityPolicy {
    /// Pair each device in `local` with at most one device in `remote`.
    ///
    /// Entry `i` is the index in `remote` that `local[i]` should use, or `None`
    /// when it has no partner: no NIC appears in two pairs, so the shorter list
    /// bounds how many pairs there are, and the policy can rule out the rest.
    ///
    /// Same-named NICs pair first, no matter what the policy says.
    /// Whatever is left over pairs subject to the policy, taking the
    /// first NIC in `remote` it can still reach.
    ///
    /// The leftovers are matched greedily in order, so an earlier device can
    /// take the only partner a later one could have used. Maximizing the number
    /// of pairs is likely not worth a matching algorithm here.
    pub fn pairs(&self, local: &[String], remote: &[String]) -> Vec<Option<usize>> {
        let mut pairs: Vec<Option<usize>> = vec![None; local.len()];
        let mut taken = vec![false; remote.len()];

        for (i, local_device) in local.iter().enumerate() {
            let same_name = remote
                .iter()
                .position(|remote_device| remote_device == local_device)
                .filter(|j| !taken[*j]);
            if let Some(j) = same_name {
                pairs[i] = Some(j);
                taken[j] = true;
            }
        }

        for (i, local_device) in local.iter().enumerate() {
            if pairs[i].is_some() {
                continue;
            }
            let candidate = remote
                .iter()
                .enumerate()
                .find(|(j, remote_device)| !taken[*j] && self.can_pair(local_device, remote_device))
                .map(|(j, _)| j);
            if let Some(j) = candidate {
                pairs[i] = Some(j);
                taken[j] = true;
            }
        }
        pairs
    }

    /// The devices to serve one buffer from: at most `max` of `devices`, or all
    /// of them when `max` is `None`.
    ///
    /// Which devices are dropped is decided by position, so the caller decides
    /// what "first" means by how it orders `devices`.
    ///
    /// Selection logic depends on the policy:
    ///
    /// - [`Self::Any`]: any two NICs can talk to each other, so buffers should
    ///   spread across tied devices rather than always funneling through
    ///   the first. `choose` picks a random offset into `devices`, then
    ///   takes up to `max` starting from that offset, wrapping around if
    ///   necessary.
    /// - [`Self::MatchName`]: takes the first `max` NICs in the order in which
    ///   they appear in devices. Callers should pass `devices` in a known,
    ///   consistent order so that different processes agree on the choice.
    /// - [`Self::Groups`]: takes one device from every group `devices` touches
    ///   before taking a second from any of them, so a peer served by any of
    ///   those groups still finds a partner here. A device no group names comes
    ///   last: it reaches no peer device, so it is worth registering only once
    ///   nothing else fills `max`.
    pub fn choose(&self, devices: &[String], max: Option<NonZeroUsize>) -> Vec<String> {
        if devices.is_empty() {
            return Vec::new();
        }
        let limit = max
            .map_or(devices.len(), NonZeroUsize::get)
            .min(devices.len());
        match self {
            Self::Any => devices
                .iter()
                .cycle()
                .skip(rand::random_range(0..devices.len()))
                .take(limit)
                .cloned()
                .collect(),
            Self::MatchName => devices.iter().take(limit).cloned().collect(),
            Self::Groups(groups) => Self::choose_across_groups(groups, devices, limit),
        }
    }

    /// [`Self::Groups`]'s arm of [`Self::choose`].
    fn choose_across_groups(
        groups: &[BTreeSet<String>],
        devices: &[String],
        limit: usize,
    ) -> Vec<String> {
        // One bucket per group `devices` touches, created on first touch and
        // filled in `devices` order.
        let mut buckets: Vec<Vec<&String>> = Vec::new();
        let mut bucket_of: Vec<Option<usize>> = vec![None; groups.len()];
        let mut ungrouped: Vec<&String> = Vec::new();
        for device in devices {
            let Some(group) = groups.iter().position(|names| names.contains(device)) else {
                ungrouped.push(device);
                continue;
            };
            match bucket_of[group] {
                Some(bucket) => buckets[bucket].push(device),
                None => {
                    bucket_of[group] = Some(buckets.len());
                    buckets.push(vec![device]);
                }
            }
        }
        if buckets.len() > limit {
            warn_uncovered_groups(limit, buckets.len(), &buckets[limit..]);
        }

        let rounds = buckets.iter().map(Vec::len).max().unwrap_or(0);
        let mut ordered: Vec<&String> = Vec::with_capacity(devices.len());
        for round in 0..rounds {
            ordered.extend(
                buckets
                    .iter()
                    .filter_map(|bucket| bucket.get(round))
                    .copied(),
            );
        }
        ordered.extend(ungrouped);
        ordered.into_iter().take(limit).cloned().collect()
    }

    /// Whether a transfer may run between `local` and `remote`.
    fn can_pair(&self, local: &str, remote: &str) -> bool {
        match self {
            Self::Any => true,
            Self::MatchName => local == remote,
            Self::Groups(groups) => groups
                .iter()
                .any(|group| group.contains(local) && group.contains(remote)),
        }
    }
}

impl FromStr for PeerDeviceAffinityPolicy {
    type Err = anyhow::Error;

    fn from_str(spec: &str) -> Result<Self> {
        let spec = spec.trim();
        match spec {
            "" | "any" => Ok(Self::Any),
            "match_name" => Ok(Self::MatchName),
            _ => {
                let groups = spec.strip_prefix("groups:").with_context(|| {
                    format!(
                        "peer device affinity {spec:?} must be `any`, `match_name`, or `groups:` \
                         followed by `|`-separated groups of comma-separated device names"
                    )
                })?;
                let groups = groups
                    .split('|')
                    .map(|group| {
                        let names: BTreeSet<String> = group
                            .split(',')
                            .map(str::trim)
                            .filter(|name| !name.is_empty())
                            .map(str::to_owned)
                            .collect();
                        anyhow::ensure!(
                            !names.is_empty(),
                            "peer device affinity {spec:?} has a group naming no device",
                        );
                        Ok(names)
                    })
                    .collect::<Result<Vec<BTreeSet<String>>>>()?;

                let mut claimed: BTreeSet<&String> = BTreeSet::new();
                for name in groups.iter().flatten() {
                    anyhow::ensure!(
                        claimed.insert(name),
                        "peer device affinity {spec:?} names {name:?} in more than one group; \
                         groups must be disjoint",
                    );
                }
                Ok(Self::Groups(groups))
            }
        }
    }
}

/// Report, at most once per distinct set of left-out devices, that
/// `rdma_max_nics_per_buffer` is too low for a buffer to reach every
/// peer-affinity group that it could hypothetically reach.
///
/// `uncovered` holds the buckets [`PeerDeviceAffinityPolicy::choose`] had to
/// drop, one per group it could not reach.
fn warn_uncovered_groups(max: usize, groups: usize, uncovered: &[Vec<&String>]) {
    static WARNED: LazyLock<DashSet<String>> = LazyLock::new(DashSet::new);
    let mut left_out: Vec<&str> = uncovered
        .iter()
        .flatten()
        .map(|device| device.as_str())
        .collect();
    left_out.sort();
    // `insert` is true only for the first caller to claim this key, and it takes
    // the shard lock, so exactly one of them warns.
    if WARNED.insert(left_out.join(",")) {
        tracing::warn!(
            "rdma_max_nics_per_buffer is {max}, but this buffer's RDMA devices span {groups} \
             peer-affinity groups; raise it to {groups} to also serve {left_out:?}, or else \
             some peers may be unreachable.",
        );
    }
}

/// The configured [`PeerDeviceAffinityPolicy`].
///
/// Returns an error when the value is malformed.
pub(crate) fn configured_peer_device_affinity() -> Result<PeerDeviceAffinityPolicy> {
    let spec = hyperactor_config::global::get_cloned(crate::config::RDMA_PEER_DEVICE_AFFINITY);
    spec.parse().map_err(|error| {
        anyhow::anyhow!(
            "invalid RDMA_PEER_DEVICE_AFFINITY (`rdma_peer_device_affinity` in Python) value \
             {spec:?}: {error}"
        )
    })
}

/// The PCI address of an RDMA NIC, resolved from its sysfs device link
/// (`/sys/class/infiniband/<name>/device`).
pub fn get_pci_address(device: &IbvDeviceInfo) -> Result<PCIAddress> {
    let link = format!("/sys/class/infiniband/{}/device", device.name());
    let resolved =
        std::fs::canonicalize(&link).with_context(|| format!("resolving sysfs link {link}"))?;
    resolved
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(PCIAddress::parse)
        .with_context(|| format!("no PCI address in resolved path {resolved:?}"))
}

/// The NIC(s) of backend `I` with the best path to `location`, ranked by
/// [`PciPath::is_better_than`] (most local, then highest port-capped
/// bandwidth) and returning all that tie for best.
///
/// A NIC that cannot be ranked -- no ACTIVE port, unresolvable PCI address -- is
/// logged and skipped, so a host with no usable NIC yields an empty list rather
/// than an error. Errors are reserved for a memory location that cannot be
/// resolved at all, which no NIC could have compensated for.
///
/// A NIC's path bandwidth is the lesser of its PCIe-chain bottleneck and its
/// RDMA port speed. Results are cached per `(backend, location)`, empty ones
/// included, since the PCI/NUMA topology is fixed for the process lifetime. A
/// host whose links are all down at the first call therefore keeps an empty
/// answer for the rest of the process.
///
/// The NICs come back in lexicographic order by name, per
/// [`compute_optimal_ibv_devices`]; the cache hands back the same order.
/// Callers rely on it to agree with their peers on which of several tied NICs to
/// use without exchanging anything.
pub fn select_optimal_ibv_devices<I: IbvDeviceImpl>(
    location: MemoryLocation,
) -> Result<Vec<IbvDeviceInfo>> {
    static CACHE: LazyLock<DashMap<(&'static str, MemoryLocation), Vec<IbvDeviceInfo>>> =
        LazyLock::new(DashMap::new);
    let key = (I::typename(), location);
    if let Some(cached) = CACHE.get(&key) {
        return Ok(cached.value().clone());
    }
    let result = compute_optimal_ibv_devices::<I>(location)?;
    CACHE.insert(key, result.clone());
    Ok(result)
}

/// Uncached core of [`select_optimal_ibv_devices`], returning the tied-best NICs
/// in lexicographic order by device name.
///
/// That order is part of the contract rather than an artifact of how the devices
/// were enumerated: two processes ranking the same [`MemoryLocation`] over the
/// same NICs produce the same sequence, so each can pick the same element — the
/// first, say — and know the other picked its counterpart.
fn compute_optimal_ibv_devices<I: IbvDeviceImpl>(
    location: MemoryLocation,
) -> Result<Vec<IbvDeviceInfo>> {
    // Resolve the GPU side once, before ranking NICs. An unresolvable GPU
    // location is a fact about the request, not a reason to skip each NIC in
    // turn.
    let gpus = match location {
        MemoryLocation::Cpu(_) => Vec::new(),
        MemoryLocation::Gpu(ordinal) => gpu_pci_addresses(ordinal).with_context(|| {
            format!(
                "cannot rank RDMA NICs for {location:?}; if the CUDA driver is not initialized \
                 in this process, initialize it first -- allocate a tensor on the device, call \
                 torch.cuda.init(), or use your method of choice."
            )
        })?,
    };

    let mut best: Option<PciPath> = None;
    let mut devices: Vec<IbvDeviceInfo> = Vec::new();
    for nic in IbvDevice::<I>::list() {
        // A NIC that cannot be ranked is not a failure of the request; drop it
        // and say why, rather than failing the whole ranking or skipping in
        // silence.
        let path = match nic_path(&nic, location, &gpus) {
            Ok(path) => path,
            Err(error) => {
                tracing::warn!("excluding RDMA device {}: {error:#}", nic.name());
                continue;
            }
        };
        match best {
            Some(current) if current.is_better_than(&path) => continue,
            Some(current) if path.is_better_than(&current) => devices.clear(),
            _ => {}
        }
        best = Some(path);
        devices.push(nic);
    }
    devices.sort_by(|a, b| a.name().cmp(b.name()));
    Ok(devices)
}

/// The PCI addresses a [`MemoryLocation::Gpu`] names: just the one for a specific
/// ordinal, or every visible device in ordinal order when unspecified.
fn gpu_pci_addresses(ordinal: Option<u32>) -> Result<Vec<PCIAddress>> {
    match ordinal {
        Some(ordinal) => Ok(vec![cuda_pci_address(ordinal)?]),
        None => (0..cuda_device_count()? as u32)
            .map(cuda_pci_address)
            .collect(),
    }
}

/// The best [`PciPath`] from `location` to `nic`, capped by the NIC's RDMA port
/// speed. Errors when `nic` cannot be ranked, naming the reason so the caller
/// can report which device it dropped and why.
///
/// `gpus` holds the already-resolved GPU addresses for a
/// [`MemoryLocation::Gpu`] location, and is empty for a CPU location.
fn nic_path(nic: &IbvDeviceInfo, location: MemoryLocation, gpus: &[PCIAddress]) -> Result<PciPath> {
    // A NIC with no ACTIVE port cannot carry traffic, so drop it from the
    // candidate set. Leaving it in with a zero bandwidth is not equivalent:
    // `is_better_than` compares `PathType` first, so a down NIC that happens to
    // be PCIe-closer would outrank a working but more distant one.
    let port_speed_mbytes_per_sec =
        NonZeroU32::new(nic.port_speed_mbytes_per_sec()).context("no ACTIVE port")?;
    let nic_addr = get_pci_address(nic)?;
    let base = match location {
        MemoryLocation::Cpu(numa) => cpu_path(&nic_addr, numa),
        MemoryLocation::Gpu(_) => gpus
            .iter()
            .map(|gpu_addr| pci_path(gpu_addr, &nic_addr))
            .reduce(|a, b| if b.is_better_than(&a) { b } else { a })
            .context("no visible CUDA device to measure a path from")?,
    };
    Ok(cap_by_port_speed(base, port_speed_mbytes_per_sec))
}

/// Caps `path`'s bottleneck at the NIC's RDMA port speed, so a wide PCIe path
/// to a slow NIC is not ranked as though the NIC could saturate it.
fn cap_by_port_speed(path: PciPath, port_speed_mbytes_per_sec: NonZeroU32) -> PciPath {
    PciPath {
        bottleneck_mbytes_per_sec: path
            .bottleneck_mbytes_per_sec
            .min(port_speed_mbytes_per_sec.get()),
        ..path
    }
}

/// Resolves an [`IbvDeviceTarget`] to a single NIC of backend `I`: the
/// named device for [`IbvDeviceTarget::Nic`], or the best NIC for a memory
/// location. Both arms are scoped to backend `I`, so a name belonging to a
/// different backend resolves to `Ok(None)`.
///
/// Where several NICs tie for a memory location, this takes the first, which by
/// [`select_optimal_ibv_devices`]'s ordering is the lexicographically smallest
/// name. Two processes pinning the same location over the same NICs therefore
/// resolve to the same device.
///
/// `Ok(None)` means "no such device"; an error means the target could not be
/// evaluated at all, which for a GPU location includes CUDA not being
/// initialized in this process.
pub fn resolve_target<I: IbvDeviceImpl>(target: &IbvDeviceTarget) -> Result<Option<IbvDeviceInfo>> {
    match target {
        IbvDeviceTarget::Nic(name) => Ok(IbvDevice::<I>::list()
            .into_iter()
            .find(|device| device.name() == name)),
        IbvDeviceTarget::MemoryLocation(location) => {
            Ok(select_optimal_ibv_devices::<I>(*location)?
                .into_iter()
                .next())
        }
    }
}

/// The NICs of backend `I` for each CUDA runtime ordinal, indexed by ordinal.
///
/// Each entry holds every NIC tied for the best path to that ordinal, in
/// lexicographic order by name per [`select_optimal_ibv_devices`], so an entry
/// is empty when no NIC could be ranked against its ordinal. Errors only when
/// the ordinals themselves cannot be enumerated, which in practice means the
/// CUDA driver is not initialized in this process.
pub fn get_cuda_device_to_ibv_devices<I: IbvDeviceImpl>() -> Result<Vec<Vec<IbvDeviceInfo>>> {
    (0..cuda_device_count()?)
        .map(|ordinal| select_optimal_ibv_devices::<I>(MemoryLocation::Gpu(Some(ordinal as u32))))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::super::mlx_device::MlxDevice;
    use super::*;

    #[test]
    fn parses_each_target_kind() {
        assert_eq!(
            "cpu:0"
                .parse::<IbvDeviceTarget>()
                .expect("cpu target should parse"),
            IbvDeviceTarget::cpu(0),
        );
        assert_eq!(
            "gpu:1"
                .parse::<IbvDeviceTarget>()
                .expect("gpu target should parse"),
            IbvDeviceTarget::gpu(1),
        );
        assert_eq!(
            "nic:mlx5_0"
                .parse::<IbvDeviceTarget>()
                .expect("NIC target should parse"),
            IbvDeviceTarget::nic("mlx5_0"),
        );
    }

    #[test]
    fn rejects_malformed_targets() {
        for invalid in ["", "cpu", "cpu:", "cpu:x", "gpu:-1", "nic:", "other:0"] {
            assert!(
                invalid.parse::<IbvDeviceTarget>().is_err(),
                "expected {invalid:?} to be rejected",
            );
        }
    }

    #[test]
    fn empty_configured_target_is_unset() {
        let lock = hyperactor_config::global::lock();
        let _guard = lock.override_key(crate::config::RDMA_IBVERBS_TARGET, String::new());
        assert_eq!(
            configured_ibverbs_target().expect("empty target should be valid"),
            None,
        );
    }

    #[test]
    fn parses_configured_target() {
        let lock = hyperactor_config::global::lock();
        let _guard = lock.override_key(
            crate::config::RDMA_IBVERBS_TARGET,
            "  nic:mlx5_1  ".to_string(),
        );

        assert_eq!(
            configured_ibverbs_target().expect("configured target should be valid"),
            Some(IbvDeviceTarget::nic("mlx5_1")),
        );
    }

    /// Device names in [`select_optimal_ibv_devices`] order, which is sorted by
    /// name -- what [`PeerDeviceAffinityPolicy::choose`] is handed.
    fn devices<const N: usize>(names: [&str; N]) -> Vec<String> {
        assert!(names.is_sorted(), "a ranking comes back sorted by name");
        names.map(str::to_owned).to_vec()
    }

    /// Every device is paired at most once on either side.
    fn assert_one_pair_per_device(pairs: &[Option<usize>], remote_len: usize) {
        let mut used = vec![false; remote_len];
        for peer in pairs.iter().flatten() {
            assert!(
                !std::mem::replace(&mut used[*peer], true),
                "peer device {peer} was paired twice in {pairs:?}",
            );
        }
    }

    #[test]
    fn same_names_pair_before_anything_else() {
        // `Any` permits every pairing, so only the name-first pass decides this
        // one. Both sides lead with a NIC the other does not have, and the
        // shared `mlx5_5` sits at a different index on each side: pairing by
        // position would hand `mlx5_2` the peer's `mlx5_5` and leave the local
        // `mlx5_5` with nothing.
        let local = devices(["mlx5_0", "mlx5_2", "mlx5_5"]);
        let remote = devices(["mlx5_3", "mlx5_5"]);
        let pairs = PeerDeviceAffinityPolicy::Any.pairs(&local, &remote);
        assert_eq!(
            pairs,
            vec![
                // mlx5_0 takes what the name match left: the peer's mlx5_3.
                Some(0),
                // Nothing is left for mlx5_2.
                None,
                // The name match, claimed before either leftover was placed.
                Some(1),
            ],
        );
        assert_one_pair_per_device(&pairs, remote.len());
    }

    #[test]
    fn match_name_pairs_only_the_same_name() {
        let local = devices(["mlx5_0", "mlx5_9"]);
        let remote = devices(["mlx5_0", "mlx5_1"]);
        assert_eq!(
            PeerDeviceAffinityPolicy::MatchName.pairs(&local, &remote),
            // `mlx5_9` has no counterpart on the peer, and `mlx5_1` is left
            // unpaired rather than handed to it — which is what `Any` would do.
            vec![Some(0), None],
        );
    }

    #[test]
    fn groups_pair_within_a_group_only() {
        // The groups cross the two lists: `mlx5_0` may only reach the peer's
        // second NIC and `mlx5_2` only its first, so the pairing has to invert
        // the list order. No name is shared, so nothing here comes from the
        // name-first pass either.
        let policy: PeerDeviceAffinityPolicy = "groups:mlx5_0,mlx5_3|mlx5_1,mlx5_2"
            .parse()
            .expect("group spec should parse");
        let local = devices(["mlx5_0", "mlx5_2", "mlx5_7"]);
        let remote = devices(["mlx5_1", "mlx5_3"]);
        let pairs = policy.pairs(&local, &remote);
        assert_eq!(
            pairs,
            vec![
                // mlx5_0 shares a group with mlx5_3.
                Some(1),
                // mlx5_2 shares a group with mlx5_1.
                Some(0),
                // mlx5_7 is in no group, so it reaches nothing.
                None,
            ],
        );
        assert_one_pair_per_device(&pairs, remote.len());
    }

    #[test]
    fn a_choice_between_peers_takes_the_first_reachable() {
        // `mlx5_7` shares no name with the peer and can reach two of its NICs,
        // so the pick is the first of those in order — skipping `mlx5_0`, which
        // its group does not name.
        let policy: PeerDeviceAffinityPolicy = "groups:mlx5_1,mlx5_3,mlx5_7"
            .parse()
            .expect("group spec should parse");
        let local = devices(["mlx5_6", "mlx5_7"]);
        let remote = devices(["mlx5_0", "mlx5_1", "mlx5_3"]);
        let pairs = policy.pairs(&local, &remote);
        assert_eq!(
            pairs,
            // mlx5_6 is in no group, so it reaches nothing.
            vec![None, Some(1)],
        );
        assert_one_pair_per_device(&pairs, remote.len());
    }

    #[test]
    fn pairs_over_empty_lists_are_empty() {
        let local = devices(["mlx5_0", "mlx5_1"]);
        assert!(PeerDeviceAffinityPolicy::Any.pairs(&[], &local).is_empty());
        assert_eq!(
            PeerDeviceAffinityPolicy::Any.pairs(&local, &[]),
            vec![None; local.len()],
            "a peer advertising no device leaves every local NIC unpaired",
        );
    }

    /// [`PeerDeviceAffinityPolicy::choose`] with a budget of `max`.
    fn chosen(policy: &PeerDeviceAffinityPolicy, devices: &[String], max: usize) -> Vec<String> {
        policy.choose(
            devices,
            Some(NonZeroUsize::new(max).expect("a device budget is at least one")),
        )
    }

    #[test]
    fn each_group_is_reached_before_any_is_doubled_up() {
        let policy: PeerDeviceAffinityPolicy =
            "groups:mlx5_0,mlx5_1|mlx5_2,mlx5_3|mlx5_4,mlx5_5|mlx5_6,mlx5_7"
                .parse()
                .expect("group spec should parse");
        let local = devices([
            "mlx5_0", "mlx5_1", "mlx5_2", "mlx5_3", "mlx5_4", "mlx5_5", "mlx5_6", "mlx5_7",
        ]);
        // One device out of each group. A prefix of the ranking would have taken
        // mlx5_0 through mlx5_3 and left the last two groups unreachable.
        assert_eq!(
            chosen(&policy, &local, 4),
            devices(["mlx5_0", "mlx5_2", "mlx5_4", "mlx5_6"]),
        );
        // Once every group is covered the budget doubles up within them.
        assert_eq!(
            chosen(&policy, &local, 6),
            ["mlx5_0", "mlx5_2", "mlx5_4", "mlx5_6", "mlx5_1", "mlx5_3"].map(str::to_owned),
        );
        // The best-ranked device is still taken first.
        assert_eq!(chosen(&policy, &local, 1), devices(["mlx5_0"]));
    }

    #[test]
    fn a_group_the_devices_do_not_reach_costs_no_budget() {
        let policy: PeerDeviceAffinityPolicy = "groups:mlx5_0,mlx5_1|mlx5_2,mlx5_3|mlx5_4,mlx5_5"
            .parse()
            .expect("group spec should parse");
        // A buffer only two groups can serve — a GPU buffer, say — spends its
        // whole budget on those two rather than holding room for the third.
        let local = devices(["mlx5_1", "mlx5_2", "mlx5_3"]);
        assert_eq!(
            chosen(&policy, &local, 3),
            devices(["mlx5_1", "mlx5_2", "mlx5_3"]),
        );
    }

    #[test]
    fn a_device_no_group_names_is_taken_last() {
        let policy: PeerDeviceAffinityPolicy = "groups:mlx5_2,mlx5_3"
            .parse()
            .expect("group spec should parse");
        // `mlx5_0` and `mlx5_1` reach no peer device under this policy, so the
        // budget goes to the group first even though they rank higher.
        let local = devices(["mlx5_0", "mlx5_1", "mlx5_2", "mlx5_3"]);
        assert_eq!(chosen(&policy, &local, 1), devices(["mlx5_2"]));
        assert_eq!(
            chosen(&policy, &local, 3),
            ["mlx5_2", "mlx5_3", "mlx5_0"].map(str::to_owned),
        );
    }

    #[test]
    fn match_name_takes_devices_in_order() {
        // Both sides have to land on the same name.
        let local = devices(["mlx5_0", "mlx5_1", "mlx5_2"]);
        assert_eq!(
            chosen(&PeerDeviceAffinityPolicy::MatchName, &local, 2),
            devices(["mlx5_0", "mlx5_1"]),
        );
    }

    #[test]
    fn any_starts_at_a_random_device_and_wraps() {
        let policy = PeerDeviceAffinityPolicy::Any;
        let ranked = devices(["mlx5_0", "mlx5_1", "mlx5_2", "mlx5_3"]);
        // Every pair of devices adjacent in the ranking, wrapping around.
        let windows: BTreeSet<Vec<String>> = [
            ["mlx5_0", "mlx5_1"].map(str::to_owned).to_vec(),
            ["mlx5_1", "mlx5_2"].map(str::to_owned).to_vec(),
            ["mlx5_2", "mlx5_3"].map(str::to_owned).to_vec(),
            ["mlx5_3", "mlx5_0"].map(str::to_owned).to_vec(),
        ]
        .into();
        let mut seen: BTreeSet<Vec<String>> = BTreeSet::new();
        for _ in 0..64 {
            let picked = chosen(&policy, &ranked, 2);
            assert!(
                windows.contains(&picked),
                "{picked:?} does not start somewhere in {ranked:?} and run on from there",
            );
            seen.insert(picked);
        }
        // 64 draws that all miss one of four starts would be a one-in-billions
        // coincidence.
        assert_eq!(seen, windows);
    }

    #[test]
    fn no_limit_takes_every_device() {
        let local = devices(["mlx5_0", "mlx5_1", "mlx5_2", "mlx5_3"]);
        for policy in [
            PeerDeviceAffinityPolicy::Any,
            PeerDeviceAffinityPolicy::MatchName,
            "groups:mlx5_0,mlx5_1|mlx5_2,mlx5_3"
                .parse()
                .expect("group spec should parse"),
        ] {
            let mut unbounded = policy.choose(&local, None);
            unbounded.sort();
            assert_eq!(unbounded, local, "{policy:?} should leave no device behind");
            // A budget larger than the list is the same as no budget at all.
            let mut over_budget = chosen(&policy, &local, local.len() + 5);
            over_budget.sort();
            assert_eq!(
                over_budget, local,
                "{policy:?} should not repeat a device to fill its budget",
            );
        }
    }

    #[test]
    fn one_device_per_group_pairs_with_a_peer_in_any_of_them() {
        // What spreading over groups buys: a buffer that only one group can
        // serve -- a GPU buffer bound to a single NIC -- still finds a partner,
        // whichever group that NIC falls in.
        let policy: PeerDeviceAffinityPolicy =
            "groups:mlx5_0,mlx5_1|mlx5_2,mlx5_3|mlx5_4,mlx5_5|mlx5_6,mlx5_7"
                .parse()
                .expect("group spec should parse");
        let ranked = devices([
            "mlx5_0", "mlx5_1", "mlx5_2", "mlx5_3", "mlx5_4", "mlx5_5", "mlx5_6", "mlx5_7",
        ]);
        let local = chosen(&policy, &ranked, 4);
        for peer in &ranked {
            let remote = chosen(&policy, std::slice::from_ref(peer), 4);
            let pairs = policy.pairs(&local, &remote);
            assert_eq!(
                pairs.iter().flatten().count(),
                1,
                "{peer} should pair with one of {local:?}, got {pairs:?}",
            );
        }
    }

    #[test]
    fn parses_each_affinity_policy() {
        for (spec, expected) in [
            ("", PeerDeviceAffinityPolicy::Any),
            ("any", PeerDeviceAffinityPolicy::Any),
            ("  match_name  ", PeerDeviceAffinityPolicy::MatchName),
            (
                // Three groups, one of them a single device.
                "groups:mlx5_0, mlx5_1|mlx5_2|mlx5_3,mlx5_4",
                PeerDeviceAffinityPolicy::Groups(vec![
                    ["mlx5_0", "mlx5_1"].map(str::to_owned).into(),
                    ["mlx5_2"].map(str::to_owned).into(),
                    ["mlx5_3", "mlx5_4"].map(str::to_owned).into(),
                ]),
            ),
        ] {
            assert_eq!(
                spec.parse::<PeerDeviceAffinityPolicy>()
                    .unwrap_or_else(|error| panic!("{spec:?} should parse: {error}")),
                expected,
            );
        }
    }

    #[test]
    fn rejects_malformed_affinity_policies() {
        // A bare name, an unknown kind, group specs with an empty group, and
        // overlapping groups.
        for invalid in [
            "mlx5_0",
            "exact",
            "groups",
            "groups:",
            "groups:mlx5_0|",
            "groups:|mlx5_0",
            "groups:mlx5_0,mlx5_1|mlx5_1,mlx5_2",
        ] {
            assert!(
                invalid.parse::<PeerDeviceAffinityPolicy>().is_err(),
                "expected {invalid:?} to be rejected",
            );
        }
    }

    #[test]
    fn optimal_devices_come_back_in_name_order() {
        // Peers pair their NICs by position in this list, so the order is part of
        // the contract rather than a detail of how devices were enumerated. A
        // host with no NIC trivially satisfies it, which is why this is not
        // gated on hardware.
        for location in [MemoryLocation::Cpu(None), MemoryLocation::Cpu(Some(0))] {
            let names: Vec<String> = select_optimal_ibv_devices::<MlxDevice>(location)
                .expect("ranking CPU memory needs no CUDA")
                .iter()
                .map(|nic| nic.name().clone())
                .collect();
            assert!(
                names.is_sorted(),
                "{location:?} ranked NICs out of order: {names:?}",
            );
        }
    }

    #[test]
    fn nic_with_no_active_port_is_not_a_candidate() {
        // `for_test_named` builds a device with no ports, so its port speed is 0.
        // Such a NIC cannot carry traffic and must be rejected outright: capping
        // it to zero bandwidth instead would leave it eligible, and
        // `is_better_than` compares `PathType` before bandwidth.
        let down = IbvDeviceInfo::for_test_named("mlx5_down");
        assert_eq!(down.port_speed_mbytes_per_sec(), 0);
        for location in [
            MemoryLocation::Cpu(None),
            MemoryLocation::Cpu(Some(0)),
            MemoryLocation::Gpu(None),
            MemoryLocation::Gpu(Some(0)),
        ] {
            let error = format!(
                "{:#}",
                nic_path(&down, location, &[])
                    .expect_err("a NIC with no ACTIVE port must not be a selection candidate")
            );
            assert!(
                error.contains("no ACTIVE port"),
                "the error should name the reason the NIC was excluded, got: {error}"
            );
        }
    }

    #[test]
    fn malformed_configured_target_names_the_setting() {
        let lock = hyperactor_config::global::lock();
        let _guard = lock.override_key(crate::config::RDMA_IBVERBS_TARGET, "mlx5_1".to_string());

        let error = configured_ibverbs_target().expect_err("malformed target should fail");
        assert!(error.to_string().contains("RDMA_IBVERBS_TARGET"));
    }
}
