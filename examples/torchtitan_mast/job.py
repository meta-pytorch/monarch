# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""MAST job spec for torchtitan ``llama3_debugmodel`` on 4 h100 nodes.

Mounts a local "titan workspace" (a directory containing a uv-managed
``.venv/`` with torch + torchtitan) onto every worker via ``remote_mount``.
Workers boot using the slim monarch bootstrap fbpkg python (which carries
monarch only); ``PYTHONPATH`` is set to the mounted venv's site-packages so
``import torch`` and ``import torchtitan`` resolve from the mount without
anything being baked into the bootstrap fbpkg.

Setup (one-time per clone):
1. ``setup_env.sh`` builds the client monarch venv at
   ``~/monarch_bench_envs/client``, the torchtitan workspace at
   ``~/dev/titan_workspace``, and pip-installs this example dir into the
   client venv. The slim bootstrap fbpkg the workers boot from is built on
   demand by ``simple_mast_job.build_bootstrap`` (reusing the wheel
   setup_env.sh cached in ``/tmp/monarch_bootstrap_$USER/wheel``).

The workspace path is the default in ``pyproject.toml`` under
``[tool.torchtitan_mast.paths]``; set ``TITAN_WORKSPACE`` to override for a
single run.

Run:
```bash
~/monarch_bench_envs/client/bin/monarch apply job.job
~/monarch_bench_envs/client/bin/python3.12 train.py
~/monarch_bench_envs/client/bin/monarch kill
```
"""

from __future__ import annotations

import os
import tomllib
from pathlib import Path

from monarch._rust_bindings.monarch_hyperactor.channel import ChannelTransport
from monarch.actor import enable_transport
from monarch.config import configure
from simple_mast_job import SimpleMastJob

# The workspace default comes from pyproject.toml's
# [tool.torchtitan_mast.paths] section; it is produced by setup_env.sh at a
# well-known location (~/dev/titan_workspace) so the default works for any
# user. The TITAN_WORKSPACE env var overrides per-call.
_PYPROJECT_PATH = Path(__file__).resolve().parent / "pyproject.toml"


def _resolve_paths() -> str:
    with _PYPROJECT_PATH.open("rb") as f:
        defaults = (
            tomllib.load(f).get("tool", {}).get("torchtitan_mast", {}).get("paths", {})
        )

    workspace = os.environ.get("TITAN_WORKSPACE") or defaults.get("workspace")
    if not workspace:
        raise RuntimeError(
            "pyproject.toml is missing [tool.torchtitan_mast.paths].workspace"
        )

    workspace = os.path.abspath(os.path.expanduser(workspace))
    venv_site_packages = os.path.join(
        workspace, ".venv", "lib", "python3.12", "site-packages"
    )
    if not os.path.isdir(venv_site_packages):
        raise RuntimeError(
            f"expected a uv venv at {workspace}/.venv; run setup_env.sh "
            f"to create it, or override with TITAN_WORKSPACE=<path>"
        )
    return workspace


def _make_job() -> SimpleMastJob:
    workspace = _resolve_paths()

    configure(
        default_transport=ChannelTransport.MetaTlsWithHostname,
        message_ack_time_interval="10ms",
        stop_actor_timeout="100ms",
        process_exit_timeout="30s",
        host_spawn_ready_timeout="300s",
        mesh_proc_spawn_max_idle="300s",
        actor_spawn_max_idle="300s",
        message_delivery_timeout="600s",
        # Capture child stdio so worker proc startup errors show up in
        # ``mesh_tail_log_lines`` (we surface this in the ValueError when
        # a proc fails to configure).
        enable_log_forwarding=True,
        enable_file_capture=True,
        tail_log_lines=100,
    )
    enable_transport("metatls-hostname")

    # Workers boot on the bootstrap-fbpkg python (carries monarch only). We
    # prepend the workspace ``.venv/bin`` to PATH so bare ``python`` /
    # ``python3.12`` on workers resolve to the FUSE-mounted venv binary for
    # ``monarch exec -- python ...``; ``PYTHONPATH`` (below) makes torch +
    # torchtitan importable from the mounted venv's site-packages without
    # baking them into the bootstrap fbpkg.
    workspace_venv_bin = f"{workspace}/.venv/bin"
    workspace_site_packages = f"{workspace}/.venv/lib/python3.12/site-packages"
    # $WORKSPACE/torchtitan is a symlink to your local torchtitan checkout;
    # resolve the realpath so we can mount the target (which lives outside
    # the workspace) at the same absolute path on workers, so the editable
    # install's finder can reach it.
    torchtitan_link = os.path.join(workspace, "torchtitan")
    torchtitan_source = (
        os.path.realpath(torchtitan_link)
        if os.path.islink(torchtitan_link)
        else torchtitan_link
    )
    # Number of parallel TCP streams used by both the warm-cache pre-fetch
    # remote_mount call and the bulk-TCP fallback transport. Same value
    # exported to worker procs so submit() picks up the right parallelism.
    streams = int(os.environ.get("TITAN_STREAMS", "8"))
    job = SimpleMastJob(
        hosts=int(os.environ.get("TITAN_NUM_HOSTS", "4")),
        env={
            "PYTHONDONTWRITEBYTECODE": "1",
            "PATH": f"{workspace_venv_bin}:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            # The bootstrap-fbpkg python carries monarch only; PYTHONPATH bridges
            # torch + torchtitan in from the mounted venv's site-packages.
            "PYTHONPATH": workspace_site_packages,
            # Workers run as root so ``~`` expands to ``/root``; export the
            # workspace path so ``train.py`` (and any other consumer) doesn't
            # fall back to its ``~/dev/titan_workspace`` default.
            "TITAN_WORKSPACE": workspace,
            # Export the resolved torchtitan realpath so train.py finds it
            # without a per-run -e TITAN_TORCHTITAN: workers can't follow the
            # workspace's torchtitan symlink (FUSE drops symlinks), but the
            # realpath is mounted at the same absolute path below.
            "TITAN_TORCHTITAN": torchtitan_source,
            # llama3_debugmodel and qwen3_1_7b on torch 2.11 both hit a
            # ChunkedCELoss autograd bug; plain CE is the workaround. Bake it in
            # so the train command needs no -e TITAN_LOSS=ce (override with
            # -e TITAN_LOSS=<other> for a config that doesn't need it).
            "TITAN_LOSS": "ce",
            # Re-enabling MAST prechecks (vs the old skip-everything bench
            # defaults) lets MAST detect and avoid hosts with broken NVIDIA
            # driver state.
            "HOOKS_DISABLED": "1",
            "RUST_LOG": "info",
            # Worker-side parallelism for the bulk-TCP fallback. Without
            # this the worker's TcpBackend defaults to 1 regardless of
            # what the client passed via Python ``configured()`` -- that
            # context is thread-local on the client and doesn't propagate
            # over actor RPC. Set explicitly so submit() picks it up.
            "MONARCH_RDMA_TCP_FALLBACK_PARALLELISM": str(streams),
            # Worker-side hyperactor config. The client-side
            # ``configure(message_ack_time_interval="10ms")`` above is
            # thread-local on the client and does NOT propagate to the
            # worker procs. Workers default to 500 ms ack batching, which
            # adds ~1-2 s of dead time to small RPCs like the
            # ``mark_blocks_available`` broadcast the mount issues after
            # each on-demand push.
            "HYPERACTOR_MESSAGE_ACK_TIME_INTERVAL": "10ms",
        },
    )

    # Mount the local workspace (``.venv/`` with monarch + torch + torchtitan)
    # at the SAME path on every worker. The v2 mechanism ships the full tree
    # immediately, prefills small "code" files, and streams big .so's on demand.
    #
    # ``python_exe`` is forwarded to the mount layer, but it ALSO sets
    # ``_default_python_exe`` -- which we immediately reset to None. The
    # mount-serving FUSEActor mesh is spawned BEFORE the mount exists, so its
    # interpreter must be the bootstrap-fbpkg python (baked into the worker,
    # carries monarch). If proc meshes booted from the mounted ``.venv`` python
    # instead, the FUSEActor spawn would ENOENT (the venv isn't mounted yet) and
    # mount-open would time out. Workers boot on the bootstrap python; PYTHONPATH
    # (above) makes torch + torchtitan importable from the mount.
    chunk_size = int(os.environ.get("TITAN_CHUNK_SIZE_MB", "256")) * 1024 * 1024
    job.remote_mount(
        workspace,
        mntpoint=workspace,
        python_exe=".venv/bin/python3.12",
        backend="mast",
        chunk_size=chunk_size,
        num_parallel_streams=streams,
    )
    # Pass python_exe (above) for the mount layer, but do NOT let it become the
    # proc-bootstrap interpreter -- see the comment above. Workers boot on the
    # bootstrap-fbpkg python; torch/torchtitan come from the mount via PYTHONPATH.
    job._default_python_exe = None

    # The workspace's ``torchtitan/`` is a symlink to your local checkout
    # (resolved above into ``torchtitan_source``). The FUSE mount serves the
    # symlink as-is, but its target lives outside the workspace -- mount the
    # target separately at the same path so the symlink resolves on workers,
    # and so ``torchtitan_source`` on PYTHONPATH is reachable.
    if torchtitan_source != torchtitan_link:
        job.remote_mount(
            torchtitan_source,
            mntpoint=torchtitan_source,
            python_exe=None,
            backend="mast",
            chunk_size=chunk_size,
            num_parallel_streams=streams,
        )

    # Mount the example dir itself so ``train.py`` is reachable on workers
    # at the same absolute path it has on the client. Without this,
    # ``monarch exec -- python /path/to/train.py`` would fail with
    # ``No such file`` on the worker.
    example_dir = os.path.dirname(os.path.abspath(__file__))
    job.remote_mount(
        example_dir,
        mntpoint=example_dir,
        python_exe=None,
        backend="mast",
        chunk_size=chunk_size,
        num_parallel_streams=streams,
    )

    # Optional FineWeb-Edu sample-10BT shards (~25 GB) as a separate mount.
    # torchtitan reads this via the ``fineweb_edu_10BT`` entry in
    # ``torchtitan/hf_datasets/text_datasets.py:DATASETS``. The shards are big
    # (>= 1 block), so the v2 mechanism leaves them out of the code prefill and
    # streams each in on demand the first time a worker reads it.
    fineweb_dir = os.path.expanduser(
        os.environ.get("TITAN_FINEWEB_DIR", "~/dev/llm_data/fineweb_edu_10BT")
    )
    fineweb_dir = os.path.abspath(fineweb_dir)
    if os.path.isdir(fineweb_dir):
        job.remote_mount(
            fineweb_dir,
            mntpoint=fineweb_dir,
            python_exe=None,
            backend="mast",
            chunk_size=chunk_size,
            num_parallel_streams=streams,
        )

    # Gather mount: per-worker writable log directory exposed back on the
    # client as ``hosts_N/`` subdirs. Workers write to /tmp/torchtitan_logs;
    # client sees the gathered view under ~/torchtitan_logs/. Kept outside
    # any remote_mount source path to avoid FUSE-in-FUSE nesting.
    log_local = os.path.expanduser(os.environ.get("TITAN_LOG_DIR", "~/torchtitan_logs"))
    job.gather_mount(
        remote_mount_point="/tmp/torchtitan_logs",
        local_mount_point=os.path.abspath(log_local),
    )
    return job


job = _make_job()
