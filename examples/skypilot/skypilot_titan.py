#!/usr/bin/env python3
"""
Monarch + TorchTitan Example with SkyPilot
==========================================

This script demonstrates running TorchTitan distributed training on cloud
infrastructure provisioned by SkyPilot.

Adapted from the SLURM TorchTitan example (slurm_titan.ipynb).

Prerequisites:
    - TorchTitan installed: pip install torchtitan
    - Model config file (e.g., debug_model.toml)
    - Tokenizer files in ./tokenizer/

Usage:
    python skypilot_titan.py --num-hosts 2 --gpus-per-host 1 --config debug_model.toml
"""

import argparse
import asyncio
import logging
import os
import sys
from dataclasses import dataclass

# Set timeouts before importing monarch
os.environ["HYPERACTOR_HOST_SPAWN_READY_TIMEOUT"] = "300s"
os.environ["HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT"] = "300s"
os.environ["HYPERACTOR_MESH_PROC_SPAWN_MAX_IDLE"] = "300s"

# Check for TorchTitan
try:
    from torchtitan.train import Trainer
    from torchtitan.config import ConfigManager, JobConfig
    from torchtitan.tools.logging import init_logger, logger as titan_logger
    HAS_TORCHTITAN = True
except ImportError:
    HAS_TORCHTITAN = False
    print("WARNING: TorchTitan is not installed. Install with: pip install torchtitan")
    print("This example will show the structure but cannot run training.")

import torch
from monarch.actor import Actor, current_rank, endpoint
from monarch.utils import setup_env_for_distributed

# Import SkyPilotJob from local module
from skypilot_job import SkyPilotJob

try:
    import sky
except ImportError:
    print("ERROR: SkyPilot is not installed. Run: pip install skypilot[kubernetes]")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(name)s %(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,
)
logger = logging.getLogger(__name__)


@dataclass
class RunParams:
    """Parameters for training job."""
    training_steps: int = 50
    model_config: str = "debug_model.toml"
    dataset: str = "c4"
    num_nodes: int = 2
    gpus_per_node: int = 1


if HAS_TORCHTITAN:
    class TrainerActor(Actor):
        """A wrapper class that executes a TorchTitan trainer in a Monarch actor."""

        def __init__(self, job_config: "JobConfig") -> None:
            self.job_config = job_config
            rank = current_rank().rank
            self.uid = f"[trainer_{rank}]"

        @endpoint
        async def start_training(self) -> None:
            init_logger()
            trainer = None

            try:
                trainer = Trainer(self.job_config)
                titan_logger.info(f"{self.uid} initialized successfully and starting training")
                trainer.train()
            except Exception:
                if trainer:
                    trainer.close()
                raise
            else:
                trainer.close()
            finally:
                torch.distributed.destroy_process_group()
                titan_logger.info(f"{self.uid} trainer cleaned up")


def make_job_config(run_params: RunParams, script_dir: str) -> "JobConfig":
    """Create a job config for TorchTitan."""
    if not HAS_TORCHTITAN:
        raise RuntimeError("TorchTitan is not installed")
    
    data_parallel_shard_degree = run_params.num_nodes * run_params.gpus_per_node
    output_path = "./outputs"

    default_args = [
        "--job.config_file",
        os.path.join(script_dir, run_params.model_config),
        "--model.tokenizer_path",
        os.path.join(script_dir, "tokenizer"),
        "--comm.trace_buf_size",
        "0",
        "--metrics.log_freq",
        "1",
        "--parallelism.data_parallel_shard_degree",
        str(data_parallel_shard_degree),
        "--activation_checkpoint.mode",
        "full",
        "--comm.train_timeout_seconds",
        "60",
        "--training.steps",
        str(run_params.training_steps),
        "--training.dataset",
        run_params.dataset,
        "--job.dump_folder",
        output_path,
        "--metrics.enable_tensorboard",
    ]

    config_manager = ConfigManager()
    job_config = config_manager.parse_args(default_args)

    return job_config


def get_cloud(cloud_name: str):
    """Get SkyPilot cloud object from name."""
    clouds = {
        "kubernetes": sky.Kubernetes,
        "aws": sky.AWS,
        "gcp": sky.GCP,
        "azure": sky.Azure,
    }
    if cloud_name.lower() not in clouds:
        raise ValueError(f"Unknown cloud: {cloud_name}. Available: {list(clouds.keys())}")
    return clouds[cloud_name.lower()]()


async def main():
    parser = argparse.ArgumentParser(description="Monarch + TorchTitan with SkyPilot")
    parser.add_argument("--cloud", default="kubernetes", help="Cloud provider")
    parser.add_argument("--num-hosts", type=int, default=2, help="Number of hosts")
    parser.add_argument("--gpus-per-host", type=int, default=1, help="GPUs per host")
    parser.add_argument("--cluster-name", default="monarch-titan", help="Cluster name")
    parser.add_argument("--accelerator", default="H200:1", help="GPU accelerator")
    parser.add_argument("--config", default="debug_model.toml", help="TorchTitan config file")
    parser.add_argument("--steps", type=int, default=50, help="Training steps")
    args = parser.parse_args()

    if not HAS_TORCHTITAN:
        print("ERROR: TorchTitan is required for this example.")
        print("Install with: pip install torchtitan")
        sys.exit(1)

    print("=" * 60)
    print("Monarch + TorchTitan with SkyPilot")
    print("=" * 60)
    print(f"\nConfiguration:")
    print(f"  Cloud: {args.cloud}")
    print(f"  Hosts: {args.num_hosts}")
    print(f"  GPUs per host: {args.gpus_per_host}")
    print(f"  Accelerator: {args.accelerator}")
    print(f"  Config: {args.config}")
    print(f"  Steps: {args.steps}")

    # Setup run parameters
    run_params = RunParams(
        training_steps=args.steps,
        model_config=args.config,
        num_nodes=args.num_hosts,
        gpus_per_node=args.gpus_per_host,
    )

    script_dir = os.path.dirname(os.path.abspath(__file__))
    job_config = make_job_config(run_params, script_dir)

    # Create SkyPilot job
    job = SkyPilotJob(
        meshes={"mesh0": args.num_hosts},
        resources=sky.Resources(
            cloud=get_cloud(args.cloud),
            accelerators=args.accelerator,
        ),
        cluster_name=args.cluster_name,
        idle_minutes_to_autostop=10,
        down_on_autostop=True,
    )

    try:
        print("\n[1] Launching SkyPilot cluster...")
        job_state = job.state()
        
        print("\n[2] Creating process mesh...")
        proc_mesh = job_state.mesh0.spawn_procs({"gpus": args.gpus_per_host})
        print(f"    Process mesh extent: {proc_mesh.extent}")

        print("\n[3] Configuring remote logging...")
        await proc_mesh.logging_option(stream_to_client=True)

        print("\n[4] Setting up distributed environment...")
        await setup_env_for_distributed(proc_mesh)

        print("\n[5] Spawning TrainerActor...")
        trainer = proc_mesh.spawn("trainer_actor", TrainerActor, job_config)

        print("\n[6] Starting training...")
        await trainer.start_training.call()

        print("\n" + "=" * 60)
        print("Training completed successfully!")
        print("=" * 60)

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        print(f"\nNot cleaning up cluster for debugging...")
        print(f"    Debug with: sky ssh {args.cluster_name}")
        print(f"    Clean up: sky down {args.cluster_name}")
        raise
    else:
        print("\n[7] Cleaning up SkyPilot cluster...")
        job.kill()
        print("    Done!")


if __name__ == "__main__":
    asyncio.run(main())

