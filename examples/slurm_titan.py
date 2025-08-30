# (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

# @noautodeps
# pyre-ignore-all-errors

import argparse
import asyncio
import getpass
import json
import logging
import os
import pathlib
import sys

import cloudpickle

from compute_world_size_actor import TestActor

from monarch._rust_bindings.monarch_hyperactor.alloc import AllocConstraints, AllocSpec
# from monarch._src.actor.meta.allocator import MastAllocator, MastAllocatorConfig

from monarch.actor import ProcMesh
from monarch.tools import commands
from monarch.tools.components import hyperactor
from monarch.tools.config import Config, UnnamedAppDef
from monarch._src.actor.allocator import RemoteAllocator, StaticRemoteAllocInitializer, TorchXRemoteAllocInitializer


import math
import os

import torch
import torch.distributed as dist
import torch.nn.functional as F
from monarch.actor import Actor, current_rank, current_size, endpoint

import torchtitan
from torchtitan.config.manager import ConfigManager
from torchtitan.train import Trainer



USER = getpass.getuser()
HOME = pathlib.Path().home()
CWD = os.getcwd()
DEACTIVATE = None

logging.basicConfig(
    level=logging.INFO,
    format="%(name)s %(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,
)


logger: logging.Logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument(
    "--force-restart",
    action="store_true",
    default=False,
    help="Force restart the job (default: False)",
)

args = parser.parse_args()

NUM_HOSTS = 2
NUM_GPUS_PER_HOST = 4


def get_job_config():
    config_manager = ConfigManager()

    TORCHTITAN_REPO_PATH = "/home/ubuntu/ahmads/torchtitan"
    JOB_CONFIG_REL_PATH = "torchtitan/models/llama3/train_configs/debug_model.toml"

    manual_args = [
        "--job.config_file",
        os.path.join(TORCHTITAN_REPO_PATH, JOB_CONFIG_REL_PATH),
        # "--model.tokenizer-path",
        # f"{FUSE_DST}/Llama-3.1-8B",
        # "--training.steps",
        # "5",
        # "--training.dataset_path",
        # f"{FUSE_DST}/c4",
        # "--job.dump_folder",
        # f"{FUSE_DST}/outputs/" + mast_job_name,
    ]
    config = config_manager.parse_args(manual_args)
    return config


class TrainerActorWrapper(Actor):
    def __init__(self, job_config):
        self.job_config = job_config
        self.rank = current_rank().rank
        print(f"Initializing actor: {self.rank} {current_rank()=}")

        # In slurm we don't need to do this as this is NFS
        # if self.rank % NUM_GPUS_PER_HOST == 0:
        #     # just use one actor to do the work
        #     print(
        #         f"writing tokenizer to {tokenizer_file_path}; content size: {len(tokenizer_content)}"
        #     )
        #     with open(tokenizer_file_path, "w") as tmp_file:
        #         tmp_file.write(tokenizer_content)
        #     if not os.path.exists(mount_file_path):
        #         print(f"writing mount.sh to {mount_file_path}; content size: {len(mount)}")
        #         with open(mount_file_path, "w") as tmp_file:
        #             tmp_file.write(mount)
        #         os.chmod(mount_file_path, 0o777)
        #         result = None
        #         try:
        #             result = subprocess.run(
        #                 [mount_file_path], capture_output=True, text=True, check=True
        #             )
        #         except subprocess.CalledProcessError as e:
        #             logger.info(f"{e=} {e.stdout=} {e.stderr=}")
        #             # Give some time to flush logs. TODO: Remove this once logs are flushed at exception time.
        #             time.sleep(10)
        #             raise

    @endpoint
    def train(self):
        logger.info("Starting training")
        config = self.job_config
        trainer: Optional[Trainer] = None

        try:
            trainer = Trainer(config)
            trainer.train()

            if config.checkpoint.create_seed_checkpoint:
                assert (
                    int(os.environ["WORLD_SIZE"]) == 1
                ), "Must create seed checkpoint using a single device, to disable sharding."
                assert (
                    config.checkpoint.enable_checkpoint
                ), "Must enable checkpointing when creating a seed checkpoint."
                trainer.checkpointer.save(curr_step=0, force=True)
                logger.info("Created seed checkpoint")
            else:
                trainer.train()
        finally:
            if trainer:
                trainer.close()

            if torch.distributed.is_initialized():
                torch.distributed.destroy_process_group()
                logger.info("Process group destroyed.")
        print("Done training")


async def main():
    jobname = f"monarch-{USER}"

    # similar to Docker image; should contain a conda env in the $img_root/conda/ directory
    # when config.workspace is not None, an ephemeral fbpkg version is created
    # that conda-packs the currently active local conda env AND the directory specified by workspace
    image = "monarch_default_workspace:latest"

    num_hosts = 2
    

    appdef = hyperactor.host_mesh(
            image=image,
            # TODO: For some reason gpu.medium doens't work here
            meshes=[f"mesh0:{num_hosts}:aws_g5.12xlarge"],  # mesh_name:num_hosts:host_type
        )
    
    # TODO: Register this so we don't have to do this every time
    for role in appdef.roles:
        role.resource.memMB = 186777

    config = Config(
        scheduler="slurm",
        scheduler_args={
            # NOTE: replace with your own values
            "hpcIdentity": "pytorch_distributed",
            "hpcJobOncall": "monarch",
            "hpcClusterUuid": "MastProdCluster",
            "rmAttribution": "pytorch4all_clients_approved",
        },
        appdef=appdef,
        workspace=str(CWD),  # or None to disable building ephemeral,
    )

    # config.dryrun = True
    # o = commands.create(config)
    # print(o)
    # sys.exit(0)

    server_info = await commands.get_or_create(
        jobname,
        config,
        force_restart=args.force_restart,
    )
    # TODO: why is gpus equal to -1 in server_info?

    num_gpus_per_host = appdef.roles[0].resource.gpu
    print(f"ahmad: {server_info} {role.resource.gpu}")

    logger.info(
        "\n===== Server Info =====\n%s",
        json.dumps(server_info.to_json(), indent=2),
    )

    mesh_dimensions = {
        "host": server_info.get_mesh_spec("mesh0").num_hosts,
        "gpu": server_info.get_mesh_spec("mesh0").gpus,
    }
    # this is redundant but is here for example sake
    mesh_name = server_info.get_mesh_spec("mesh0").name

    allocator = RemoteAllocator(world_id="foo", initializer=TorchXRemoteAllocInitializer(server_info.server_handle))
    alloc = await allocator.allocate(AllocSpec(AllocConstraints(), hosts=num_hosts, gpus=num_gpus_per_host))

    proc_mesh = await ProcMesh.from_alloc(alloc)

    job_config = get_job_config()

    trainer_actor = await proc_mesh.spawn(
        "trainer_actor", TrainerActorWrapper, job_config
    )
    await trainer_actor.train.call()



#     actor = await proc_mesh.spawn("compute_world_size_actor", TestActor)

#     logger.info("computing world size...")
#     values = await actor.compute_world_size.call(
#         master_addr=server_info.host0(mesh_name),
#         master_port=29500,
#     )

#     values_by_rank = {f"rank_{p.rank}": v for p, v in list(values.flatten("rank"))}

#     logger.info(f"""computed world_sizes:
# {'-'*40}
# {json.dumps(values_by_rank, indent=2)}
# {'-'*40}""")


    commands.kill(f"slurm:///{server_info.name}")


if __name__ == "__main__":
    cloudpickle.register_pickle_by_value(sys.modules[TestActor.__module__])

    asyncio.run(main())