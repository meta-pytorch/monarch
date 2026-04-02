# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import time as _time_mod

_t_import_start = _time_mod.time()

import logging  # noqa: E402
import os  # noqa: E402
import subprocess  # noqa: E402
import sys  # noqa: E402
import tempfile  # noqa: E402
import time  # noqa: E402
from pathlib import Path  # noqa: E402

import cloudpickle  # noqa: E402
from monarch.actor import Actor, endpoint, this_host  # noqa: E402
from monarch.config import configure  # noqa: E402
from monarch.job import SlurmJob  # noqa: E402
from monarch.remotemount import remotemount  # noqa: E402


class BashActor(Actor):
    @endpoint
    def run(self, script: str):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sh", delete=True) as f:
            f.write(script)
            f.flush()
            result = subprocess.run(["bash", f.name], capture_output=True, text=True)
        return {
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }


def _get_mast_host_mesh(
    num_hosts,
    gpus_per_host,
    hpc_identity,
    hpc_job_oncall,
    hpc_cluster_uuid,
    rm_attribution,
    locality_constraints,
    host_type,
):
    t0 = time.time()
    from monarch.actor import enable_transport
    from monarch.job.meta import MASTJob

    t1 = time.time()
    enable_transport("metatls-hostname")
    t2 = time.time()

    if locality_constraints is None:
        locality_constraints = []

    job = MASTJob(
        hpcIdentity=hpc_identity,
        hpcJobOncall=hpc_job_oncall,
        rmAttribution=rm_attribution,
        hpcClusterUuid=hpc_cluster_uuid,
        useStrictName=True,
        localityConstraints=locality_constraints,
        env={
            "PYTHONDONTWRITEBYTECODE": "1",
            # Skip preamble health checks entirely — removes the
            # precheck pre-run step from the TW job spec so workers
            # go straight to running the payload.
            "MAST_PRECHECK_PRERUN_STEP_KILLSWITCH": "1",
            # Disable conda activate hooks — run_activate_hooks.sh
            # crashes bash with "pop_var_context" on some host images.
            "HOOKS_DISABLED": "1",
            "RUST_LOG": "info",
        },
    )
    t3 = time.time()
    job.add_mesh("workers", num_hosts, host_type=host_type)
    # A workspace directory is required to trigger conda-packing of
    # CONDA_PREFIX into an ephemeral fbpkg.  Without it the scheduler
    # deploys the base image as-is, which fails on cross-arch hosts
    # (e.g. aarch64 GB300 with an x86 base image).
    job.add_directory(tempfile.mkdtemp())
    t4 = time.time()
    host_meshes = job.state()
    t5 = time.time()
    print(
        f"remoterun timings: mast_import={t1 - t0:.2f}s, "
        f"enable_transport={t2 - t1:.2f}s, MASTJob()={t3 - t2:.2f}s, "
        f"add_mesh+dir={t4 - t3:.2f}s, job.state()={t5 - t4:.2f}s",
        flush=True,
    )
    return host_meshes.workers, {"job": job}


def _cleanup_mast_job(job_info, host_mesh, kill_job):
    from monarch.actor import shutdown_context

    host_mesh.shutdown().get()
    job = job_info["job"]
    if kill_job:
        job.kill()
        print("Killed MAST job", flush=True)

    try:
        shutdown_context().get()
    except Exception:
        pass


def main(
    source_dir: str,
    script: str,
    mount_point: str | None = None,
    run_local=False,
    verbose=False,
    chunk_size_mb: int | None = None,
    backend="slurm",
    qos="h100_lowest",
    hpc_identity="hyper_monarch",
    hpc_job_oncall="monarch",
    hpc_cluster_uuid="MastGenAICluster",
    rm_attribution="msl_infra_pytorch_dev",
    locality_constraints: str | None = None,
    kill_job: bool = False,
    num_hosts: int = 2,
    gpus_per_host: int = 8,
    host_type: str = "grandteton",
    num_parallel_streams: int = 8,
):
    t_main_start = time.time()
    if verbose:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%H:%M:%S",
        )

    # MAST workers have significant startup overhead before the
    # hyperactor host_agent is ready: NCCL health checks (~25s),
    # conda activate hooks, and Python/Rust runtime initialization.
    # The MAST scheduler reports the job as RUNNING (container-level)
    # well before the monarch worker process starts listening.  All
    # timeouts that gate on worker readiness must account for this
    # gap, including mesh_attach_config_timeout (the push_config ack
    # during attach) and the spawn idle timeouts.
    t_configure_start = time.time()
    configure(
        enable_log_forwarding=True,
        tail_log_lines=100,
        host_spawn_ready_timeout="300s",
        mesh_attach_config_timeout="300s",
        mesh_proc_spawn_max_idle="300s",
        actor_spawn_max_idle="300s",
        message_delivery_timeout="600s",
        rdma_max_chunk_size_mb=256,
    )
    t_configure_done = time.time()
    print(
        f"remoterun timings: import={t_main_start - _t_import_start:.2f}s, "
        f"configure={t_configure_done - t_configure_start:.2f}s",
        flush=True,
    )

    mount_point = source_dir if mount_point is None else mount_point
    mount_point = Path(mount_point).resolve()
    source_dir = Path(source_dir).resolve()

    if run_local and mount_point == source_dir:
        raise ValueError(
            f"If running locally mount_point and source_dir need to be different paths. Instead got source_dir {source_dir} and mount_point {mount_point}."
        )

    job_info = None

    # # Spawn a process for each GPU
    if run_local:
        host_mesh = this_host()
        procs = host_mesh.spawn_procs(per_host={"gpus": gpus_per_host})
    elif backend == "slurm":
        os.environ.pop("SLURM_CPU_BIND", None)
        # Create a slurm job with 2 hosts
        slurm_job = SlurmJob(
            {"mesh1": num_hosts},
            slurm_args=[f"--qos={qos}"],
            exclusive=False,
            gpus_per_node=gpus_per_host,
            cpus_per_task=24,
            time_limit="1:00:00",
            log_dir=os.path.expanduser("~/monarch_slurm_logs"),
            mem="100G",
        )
        host_meshes = slurm_job.state()
        host_mesh = host_meshes.mesh1
        procs = host_mesh.spawn_procs(per_host={"gpus": gpus_per_host})
    elif backend == "mast":
        lc = None
        if locality_constraints is not None and locality_constraints != "":
            lc = (
                locality_constraints.split(";")
                if ";" in locality_constraints
                else [locality_constraints]
            )

        t_mast_start = time.time()
        host_mesh, job_info = _get_mast_host_mesh(
            num_hosts,
            gpus_per_host,
            hpc_identity,
            hpc_job_oncall,
            hpc_cluster_uuid,
            rm_attribution,
            lc,
            host_type,
        )
        procs = host_mesh.spawn_procs(per_host={"gpus": gpus_per_host})
        t_mast_done = time.time()
        print(
            f"remoterun timings: mast_reconnect={t_mast_done - t_mast_start:.2f}s",
            flush=True,
        )
    else:
        raise ValueError(f"Unknown backend: {backend}. Must be 'slurm' or 'mast'.")

    if script == "stdin":
        script = sys.stdin.read()
    else:
        with open(Path(script).resolve()) as f:
            script = f.read()

    if chunk_size_mb is None:
        chunk_size_mb = 1024
    chunk_size = chunk_size_mb * 1024 * 1024

    t_rm_start = time.time()
    rm = remotemount(
        host_mesh,
        str(source_dir),
        str(mount_point),
        chunk_size=chunk_size,
        backend=backend,
        num_parallel_streams=num_parallel_streams,
    )
    rm.open()
    t_rm_open = time.time()

    bash_actors = procs.spawn("BashActor", BashActor)
    t_bash_spawn = time.time()
    results = bash_actors.run.call(script).get()
    t_bash_run = time.time()
    # Print stdout for each rank in order
    print(
        "\n".join(
            [f"== rank{i} stdout ==\n{r[1]['stdout']}" for i, r in enumerate(results)]
        )
    )
    print(
        "\n".join(
            [f"== rank{i} stderr ==\n{r[1]['stderr']}" for i, r in enumerate(results)]
        )
    )

    rm.close()
    t_rm_close = time.time()
    print(
        f"remoterun timings: open={t_rm_open - t_rm_start:.2f}s, "
        f"bash_spawn={t_bash_spawn - t_rm_open:.2f}s, "
        f"bash_run={t_bash_run - t_bash_spawn:.2f}s, "
        f"close={t_rm_close - t_bash_run:.2f}s, "
        f"total={t_rm_close - t_rm_start:.2f}s",
        flush=True,
    )

    if backend == "mast" and job_info is not None and kill_job:
        _cleanup_mast_job(job_info, host_mesh, kill_job)


# Register for pickle-by-value so BashActor is serialized to remote workers
import sys as _sys  # noqa: E402

cloudpickle.register_pickle_by_value(_sys.modules[__name__])

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Mount a local directory on remote MAST/SLURM workers via remotemount."
    )
    parser.add_argument("source_dir", help="Local directory to mount on workers")
    parser.add_argument(
        "script",
        help="Script to run on workers, or 'stdin' to read from stdin",
    )
    parser.add_argument(
        "--mount_point",
        default=None,
        help="Mount path on workers (default: same as source_dir)",
    )
    parser.add_argument(
        "--run_local", action="store_true", help="Run locally without MAST/SLURM"
    )
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--chunk_size_mb", type=int, default=None)
    parser.add_argument(
        "--backend", default="slurm", choices=["slurm", "mast", "local"]
    )
    parser.add_argument("--qos", default="h100_lowest")
    parser.add_argument("--hpc_identity", default="hyper_monarch")
    parser.add_argument("--hpc_job_oncall", default="monarch")
    parser.add_argument("--hpc_cluster_uuid", default="MastGenAICluster")
    parser.add_argument("--rm_attribution", default="msl_infra_pytorch_dev")
    parser.add_argument("--locality_constraints", default=None)
    parser.add_argument(
        "--kill_job", action="store_true", help="Kill MAST job after script finishes"
    )
    parser.add_argument("--num_hosts", type=int, default=2)
    parser.add_argument("--gpus_per_host", type=int, default=8)
    parser.add_argument("--host_type", default="grandteton")
    parser.add_argument("--num_parallel_streams", type=int, default=8)
    args = parser.parse_args()
    main(**vars(args))
