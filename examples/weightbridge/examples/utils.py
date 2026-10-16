# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import os
from collections.abc import Callable

import torch
from wbridge.utils.specgen import HFWeightFetcher


def network_env_vars(provider: str, iface: str) -> dict[str, str]:
    """Return worker environment variables for a Mooncake transport.

    The caller owns installation-specific dynamic-linker configuration.  If
    ``LD_LIBRARY_PATH`` is already set, preserve and forward it unchanged
    instead of assuming an EFA SDK location.
    """
    env: dict[str, str] = {}
    if iface:
        env["NCCL_SOCKET_IFNAME"] = iface
        env["GLOO_SOCKET_IFNAME"] = iface

    if provider == "tcp":
        env["MC_FORCE_TCP"] = "1"
        return env
    if provider != "efa":
        raise ValueError(f"Unsupported network provider: {provider}")

    existing_ld = os.environ.get("LD_LIBRARY_PATH")
    if existing_ld:
        env["LD_LIBRARY_PATH"] = existing_ld
    env.update(
        {
            "FI_PROVIDER": "efa",
            "FI_EFA_USE_DEVICE_RDMA": "1",
            "FI_EFA_ENABLE_SHM_TRANSFER": "0",
            "NCCL_DEBUG": "INFO",
            "NCCL_DEBUG_SUBSYS": "INIT,NET",
            "NCCL_NET_GDR_LEVEL": "SYS",
            "NCCL_NET_GDR_READ": "1",
        }
    )
    return env


def apply_network_env(provider: str, iface: str) -> None:
    """Apply :func:`network_env_vars` without replacing explicit settings."""
    for key, value in network_env_vars(provider, iface).items():
        os.environ.setdefault(key, value)


def visible_device_list(required: int) -> str:
    """Return a CUDA visibility list large enough for a manual-device-placement actor.

    Replay actors request no Ray GPU resource because one parent actor manages several child ranks.
    ``WB_VISIBLE_DEVICES`` lets a deployment name the physical GPUs explicitly; otherwise a contiguous
    zero-based list is derived from the capture rather than assuming an eight-GPU host.
    """
    if required <= 0:
        raise ValueError(
            f"required visible device count must be positive, got {required}"
        )
    configured = os.environ.get("WB_VISIBLE_DEVICES", "").strip()
    if configured:
        devices = [device.strip() for device in configured.split(",") if device.strip()]
        if len(devices) < required:
            raise ValueError(
                f"WB_VISIBLE_DEVICES exposes {len(devices)} devices, but the replay needs {required}"
            )
        return ",".join(devices)
    return ",".join(str(index) for index in range(required))


def get_ray_nodes(
    rollout_ip: str | None = None, trainer_ip: str | None = None, colocate: bool = False
):
    """``ray.init()`` then return rollout and trainer nodes.

    With *colocate*, both roles are pinned to ONE node (a single alive node is enough) so the trainer and
    rollout workers are co-located — the placement that lets WeightBridge move weights over NVLink with a
    direct CUDA-IPC copy instead of the network RDMA backend. Pass a single
    ``--rollout-ip``/``--trainer-ip`` to choose
    which node; they must agree.

    Returns ``(rollout_ip, trainer_ip, rollout_node_id, trainer_node_id)``.
    """
    import ray

    ray.init(address="auto")
    ray_nodes = [n for n in ray.nodes() if n["Alive"]]
    need = 1 if colocate else 2
    if len(ray_nodes) < need:
        raise RuntimeError(
            f"Need at least {need} alive Ray node(s), found {len(ray_nodes)}."
        )

    by_ip = {str(n["NodeManagerAddress"]): n for n in ray_nodes}
    if colocate:
        want = rollout_ip or trainer_ip
        if rollout_ip and trainer_ip and rollout_ip != trainer_ip:
            raise RuntimeError(
                f"--colocate needs one node, got rollout={rollout_ip} trainer={trainer_ip}"
            )
        if want is not None and want not in by_ip:
            raise RuntimeError(
                f"Requested Ray node IP not alive: {want}. Alive Ray IPs: {sorted(by_ip)}"
            )
        node = (
            by_ip[want]
            if want
            else sorted(ray_nodes, key=lambda n: str(n["NodeManagerAddress"]))[0]
        )
        rollout = trainer = node
    elif rollout_ip is not None or trainer_ip is not None:
        missing = [
            ip for ip in (rollout_ip, trainer_ip) if ip is not None and ip not in by_ip
        ]
        if missing:
            alive = ", ".join(sorted(by_ip))
            raise RuntimeError(
                f"Requested Ray node IP(s) not alive: {missing}. Alive Ray IPs: {alive}"
            )
        if rollout_ip is None or trainer_ip is None:
            raise RuntimeError("Pass both rollout_ip and trainer_ip, or neither.")
        rollout, trainer = by_ip[rollout_ip], by_ip[trainer_ip]
    else:
        rollout, trainer = sorted(
            ray_nodes, key=lambda n: str(n["NodeManagerAddress"])
        )[:2]

    return (
        str(rollout["NodeManagerAddress"]),
        str(trainer["NodeManagerAddress"]),
        str(rollout["NodeID"]),
        str(trainer["NodeID"]),
    )


def gpu_link_info() -> dict:
    """Physical identity + NVLink state of the GPU this process is bound to.

    Reported by :mod:`examples.train` alongside the transport byte counters, so a co-located run shows not
    just *that* the RDMA backend was bypassed but that the CUDA-IPC copies really had NVLink underneath them.
    ``pci_bus_id`` comes from the CUDA runtime (CUDA_VISIBLE_DEVICES-safe: ``current_device()`` is a
    *visible* index, and the runtime maps it to the physical device). NVLink state needs NVML; if
    ``pynvml`` is missing the link fields come back ``None`` and the caller just skips that part of the
    report. On NVSwitch machines the remote bus ids are the switches, not the peer GPUs, so treat the link
    COUNT — not the remote ids — as the "NVLink present" signal.
    """
    import ctypes

    info: dict = {
        "pci_bus_id": None,
        "nvlink_active_links": None,
        "nvlink_remote_bus_ids": None,
    }
    try:
        lib = ctypes.CDLL("libcudart.so")
        buf = ctypes.create_string_buffer(64)
        dev = torch.cuda.current_device()
        if (
            lib.cudaDeviceGetPCIBusId(buf, ctypes.c_int(64), ctypes.c_int(int(dev)))
            == 0
        ):
            info["pci_bus_id"] = buf.value.decode()
    except Exception:  # noqa: BLE001 — best-effort diagnostics, never fail the run
        return info
    if info["pci_bus_id"] is None:
        return info
    try:
        import pynvml

        pynvml.nvmlInit()
        h = pynvml.nvmlDeviceGetHandleByPciBusId(info["pci_bus_id"].encode())
        active, remotes = 0, []
        for link in range(pynvml.NVML_NVLINK_MAX_LINKS):
            try:
                if (
                    pynvml.nvmlDeviceGetNvLinkState(h, link)
                    != pynvml.NVML_FEATURE_ENABLED
                ):
                    continue
                active += 1
                remotes.append(
                    pynvml.nvmlDeviceGetNvLinkRemotePciInfo(h, link).busId.strip("\x00")
                )
            except pynvml.NVMLError:
                continue  # link index beyond this GPU's count
        info["nvlink_active_links"] = active
        info["nvlink_remote_bus_ids"] = sorted(set(remotes))
    except Exception:  # noqa: BLE001 — pynvml absent or NVML unavailable in the container
        pass
    return info


def make_hf_weights(
    full_cpu: dict[str, torch.Tensor],
) -> tuple[HFWeightFetcher, dict[str, tuple[int, ...]]]:
    """Build ``(HFWeightFetcher, hf_shapes)`` from a CPU tensor dict.

    Each factory returns a **clone** so :func:`~wbridge.utils.specgen.infer_load_spec` can mutate
    probe tensors (``fill_``, etc.) without corrupting the shared *full_cpu* checkpoint dict.
    """
    fetcher: HFWeightFetcher = {
        name: (lambda t=t: t.clone().contiguous()) for name, t in full_cpu.items()
    }
    shapes: dict[str, tuple[int, ...]] = {
        name: tuple(t.shape) for name, t in full_cpu.items()
    }
    return fetcher, shapes
