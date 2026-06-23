# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Client-side driver that runs torchtitan llama3_debugmodel on a live MAST job.

Run AFTER ``monarch apply job.job``.

This script runs on the local machine. It:
1. Connects to the running MAST allocation via ``load_current_job()``.
2. Spawns a process mesh with 8 procs per host (one per GPU).
3. Sets up torch-elastic env vars on each proc so torch.distributed
   can rendezvous via hyperactor mailbox.
4. Spawns ``TrainerActor`` across the mesh and calls ``start_training()``.

The actor code itself runs on the workers using the python interpreter from
the mounted ``.venv``, which has torch + torchtitan installed.
"""

from __future__ import annotations

import asyncio
import logging
import os

from monarch._src.tools.commands import load_current_job
from monarch.actor import Actor, current_rank, endpoint

logger: logging.Logger = logging.getLogger(__name__)


class TrainerActor(Actor):
    """Wraps a torchtitan ``Trainer`` instance and runs it on a single rank.

    All imports of torch / torchtitan live inside method bodies so that
    *constructing* this actor on the client side (where torch is not
    installed) doesn't fail; only ``start_training()`` on the worker
    needs the heavy deps, and at that point the worker's python_exe is
    pointed at the mounted venv that has them.
    """

    def __init__(
        self,
        model_config: str,
        training_steps: int,
        workspace: str,
        torchtitan_dir: str,
    ) -> None:
        self._model_config = model_config
        self._training_steps = training_steps
        self._workspace = workspace
        self._torchtitan_dir = torchtitan_dir
        self._trainer: object | None = None
        rank = current_rank().rank
        self._uid = f"trainer_{rank}"
        self._rank = rank

    @endpoint
    async def start_training(self) -> None:
        import os

        # ``llama3_debugmodel.hf_assets_path`` is the relative path
        # ``./tests/assets/tokenizer``, so we cd into the torchtitan
        # checkout before the trainer runs. torch + torchtitan are
        # importable here because the mount daemon's job spec puts
        # the workspace .venv on ``PYTHONPATH``.
        os.chdir(self._torchtitan_dir)

        from torchtitan.components.loss import CrossEntropyLoss
        from torchtitan.config import ConfigManager
        from torchtitan.tools.logging import init_logger, logger as titan_logger
        from torchtitan.trainer import Trainer

        init_logger()

        cfg_mgr = ConfigManager()
        job_config = cfg_mgr.parse_args(
            ["--module", "llama3", "--config", self._model_config]
        )
        job_config.training.steps = self._training_steps
        # ChunkedCELoss for debug-model hits an autograd "tensor data not
        # allocated" bug on torch 2.10-2.11 + this torchtitan version. Plain
        # CrossEntropyLoss is sufficient for the smoke-test.
        job_config.loss = CrossEntropyLoss.Config()

        self._trainer = Trainer(job_config)
        titan_logger.info(f"{self._uid}: initialized, starting training")
        self._trainer.train()
        titan_logger.info(f"{self._uid}: training complete")

    @endpoint
    async def generate(self, prompt: str, max_new_tokens: int = 32) -> str:
        """Autoregressive greedy decoding from the trained model.

        All ranks participate in the forward pass (TP/FSDP collectives), but
        only rank 0 returns the decoded string -- everyone else returns "".
        For llama3_debugmodel (random init + few training steps + tiny dataset)
        the output is gibberish by design: the goal is to verify the
        train -> infer pipeline plumbing, not language quality.
        """
        import torch
        from torchtitan.tools.logging import logger as titan_logger

        if self._trainer is None:
            return "" if self._rank != 0 else "<no trainer>"

        model = self._trainer.model_parts[0]
        tokenizer = self._trainer.tokenizer

        model.eval()
        with torch.no_grad():
            ids = tokenizer.encode(prompt, add_special_tokens=False)
            input_ids = torch.tensor([ids], device="cuda")
            for _ in range(max_new_tokens):
                logits = model(input_ids)
                if isinstance(logits, (tuple, list)):
                    logits = logits[0]
                next_tok = int(logits[0, -1].argmax().item())
                input_ids = torch.cat(
                    [input_ids, torch.tensor([[next_tok]], device="cuda")],
                    dim=-1,
                )
            out_ids = input_ids[0].tolist()

        if self._rank != 0:
            return ""
        text = tokenizer.decode(out_ids)
        titan_logger.info(f"{self._uid}: generated text = {text!r}")
        return text

    @endpoint
    async def close(self) -> None:
        import torch

        if self._trainer is not None:
            self._trainer.close()
            self._trainer = None
        if torch.distributed.is_initialized():
            torch.distributed.destroy_process_group()


async def main() -> None:
    num_hosts = int(os.environ.get("TITAN_NUM_HOSTS", "4"))
    gpus_per_host = int(os.environ.get("TITAN_GPUS_PER_HOST", "8"))
    training_steps = int(os.environ.get("TITAN_TRAINING_STEPS", "20"))
    model_config = os.environ.get("TITAN_MODEL_CONFIG", "llama3_debugmodel")

    job = load_current_job()
    state = job.state()

    # SimpleMastJob exposes its mesh as "workers"; confirm and grab it.
    if "workers" not in state._hosts:
        raise RuntimeError(f"expected a 'workers' host mesh, got: {list(state._hosts)}")
    host_mesh = state._hosts["workers"]

    logger.info(
        "spawning %d procs/host across %d hosts",
        gpus_per_host,
        num_hosts,
    )
    proc_mesh = host_mesh.spawn_procs(per_host={"gpus": gpus_per_host})
    await proc_mesh.logging_option(stream_to_client=True)

    from monarch.spmd import setup_torch_elastic_env_async

    await setup_torch_elastic_env_async(proc_mesh)

    workspace = os.path.expanduser(
        os.environ.get("TITAN_WORKSPACE", "~/dev/titan_workspace")
    )
    workspace = os.path.abspath(workspace)
    torchtitan_link = os.path.join(workspace, "torchtitan")
    torchtitan_dir = (
        os.path.realpath(torchtitan_link)
        if os.path.islink(torchtitan_link)
        else torchtitan_link
    )
    trainer = proc_mesh.spawn(
        "trainer",
        TrainerActor,
        model_config,
        training_steps,
        workspace,
        torchtitan_dir,
    )
    await trainer.start_training.call()
    logger.info("training run complete")

    prompt = os.environ.get("TITAN_GEN_PROMPT", "Once upon a time")
    max_new = int(os.environ.get("TITAN_GEN_TOKENS", "32"))
    logger.info("generating: prompt=%r max_new_tokens=%d", prompt, max_new)
    results = await trainer.generate.call(prompt, max_new)
    for _rank_key, text in results:
        if text:
            print(f"\n=== Generated text ===\n{text}\n======================\n")
            break

    await trainer.close.call()
    logger.info("done")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    asyncio.run(main())
