# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""torchtitan launcher (defaults to llama3_debugmodel; override via env).

Designed to be invoked the *same way* locally and remotely:

    # Local (single GPU smoke test, requires CUDA + the torchtitan venv active)
    python train.py

    # Remote on a live MAST job (4 hosts x 8 GPUs)
    monarch exec --all --per-host gpu=8 \\
        -e MASTER_ADDR=$(monarch exec --one -- hostname) \\
        -e MASTER_PORT=29500 \\
        -- python train.py

Rank / world-size are derived from monarch's ``MONARCH_RANK_<dim>`` /
``MONARCH_SIZE_<dim>`` env vars when present; otherwise we run as
rank 0 of a world of 1. MASTER_ADDR / MASTER_PORT are read from env
unchanged (defaults: ``127.0.0.1`` / ``29500``) -- the remote launcher
above injects rank 0's hostname for cross-host rdzv.

Env knobs:
  TITAN_MODEL_MODULE   torchtitan module name (default: ``llama3``).
  TITAN_MODEL_CONFIG   toml basename under the module's ``train_configs/``
                       (default: ``debug_model``; e.g. ``qwen3_1.7b``).
  TITAN_TRAINING_STEPS overrides cfg.training.steps (default: 20).
  TITAN_DATASET        overrides cfg.training.dataset; set to
                       ``fineweb_edu_10BT`` to read from locally mounted
                       parquet shards instead of HF ``c4``.
"""

from __future__ import annotations

import os
import sys


def _populate_torch_env() -> tuple[int, int, int]:
    """Map monarch's per-rank env vars to the ones torch.distributed wants.

    Returns ``(rank, world_size, local_rank)``. When no MONARCH_RANK_*
    variables are set, behaves as a single-rank standalone run.
    """
    h_rank = os.environ.get("MONARCH_RANK_hosts")
    g_rank = os.environ.get("MONARCH_RANK_gpu")
    h_size = os.environ.get("MONARCH_SIZE_hosts")
    g_size = os.environ.get("MONARCH_SIZE_gpu")
    if any(v is None for v in (h_rank, g_rank, h_size, g_size)):
        rank, world_size, local_rank = 0, 1, 0
    else:
        h_rank, g_rank, h_size, g_size = (
            int(h_rank),
            int(g_rank),
            int(h_size),
            int(g_size),
        )
        rank = h_rank * g_size + g_rank
        world_size = h_size * g_size
        local_rank = g_rank

    os.environ.setdefault("MASTER_ADDR", "127.0.0.1")
    os.environ.setdefault("MASTER_PORT", "29500")
    os.environ["RANK"] = str(rank)
    os.environ["WORLD_SIZE"] = str(world_size)
    os.environ["LOCAL_RANK"] = str(local_rank)
    return rank, world_size, local_rank


def main() -> None:
    rank, world_size, local_rank = _populate_torch_env()
    print(
        f"train: rank={rank} world_size={world_size} "
        f"local_rank={local_rank} master={os.environ['MASTER_ADDR']}:"
        f"{os.environ['MASTER_PORT']}",
        file=sys.stderr,
        flush=True,
    )

    # torchtitan's llama3_debugmodel loads ``./tests/assets/tokenizer`` as a
    # relative path, so we cd into the torchtitan source tree before building
    # the Trainer. ``TITAN_TORCHTITAN`` takes precedence -- when the workspace
    # is delivered via a FUSE mount that drops symlinks (workers don't see
    # ``<workspace>/torchtitan``), the launcher passes the realpath here
    # directly.
    explicit = os.environ.get("TITAN_TORCHTITAN")
    if explicit:
        torchtitan_dir = os.path.abspath(os.path.expanduser(explicit))
    else:
        workspace = os.path.abspath(
            os.path.expanduser(
                os.environ.get("TITAN_WORKSPACE", "~/dev/titan_workspace")
            )
        )
        torchtitan_link = os.path.join(workspace, "torchtitan")
        torchtitan_dir = (
            os.path.realpath(torchtitan_link)
            if os.path.islink(torchtitan_link)
            else torchtitan_link
        )
    os.chdir(torchtitan_dir)

    from torchtitan.config import ConfigManager
    from torchtitan.tools.logging import init_logger
    from torchtitan.train import Trainer

    init_logger()

    # v0.2.2 selects a model by its TOML config file, not --module/--config.
    # TITAN_MODEL_CONFIG is the toml basename under the module's train_configs/.
    model_module = os.environ.get("TITAN_MODEL_MODULE", "llama3")
    model_config = os.environ.get("TITAN_MODEL_CONFIG", "debug_model")
    config_file = f"torchtitan/models/{model_module}/train_configs/{model_config}.toml"
    cfg = ConfigManager().parse_args(["--job.config_file", config_file])
    cfg.training.steps = int(os.environ.get("TITAN_TRAINING_STEPS", "20"))
    dataset_override = os.environ.get("TITAN_DATASET")
    if dataset_override:
        cfg.training.dataset = dataset_override

    trainer = Trainer(cfg)
    trainer.train()

    # Optional: greedy-decode a short sample from the just-trained model so we
    # can see what generations the model produces. Uses the live in-memory
    # model (no checkpoint round-trip). Enable via TITAN_GENERATE=1.
    #
    # Because the model is FSDP-sharded, every ``model(...)`` call triggers an
    # all-gather collective -- ALL ranks must participate or the gather hangs.
    # Every rank runs the same greedy loop in lockstep; only rank 0 prints.
    if os.environ.get("TITAN_GENERATE"):
        import torch

        prompts = os.environ.get(
            "TITAN_GEN_PROMPTS", "The quick brown fox|In the beginning"
        ).split("|")
        max_new = int(os.environ.get("TITAN_GEN_MAX_NEW", "30"))
        model = trainer.model_parts[0]
        tokenizer = trainer.tokenizer
        device = next(model.parameters()).device
        model.eval()
        if rank == 0:
            sys.stderr.write("\n==== GENERATE ====\n")
        with torch.no_grad():
            for prompt in prompts:
                ids = tokenizer.encode(prompt)
                input_ids = torch.tensor([ids], dtype=torch.long, device=device)
                for _ in range(max_new):
                    logits = model(input_ids)
                    next_id = int(logits[0, -1].argmax().item())
                    input_ids = torch.cat(
                        [
                            input_ids,
                            torch.tensor([[next_id]], dtype=torch.long, device=device),
                        ],
                        dim=1,
                    )
                if rank == 0:
                    generated = tokenizer.decode(input_ids[0].tolist())
                    sys.stderr.write(f"prompt:    {prompt!r}\n")
                    sys.stderr.write(f"generated: {generated!r}\n\n")
                    sys.stderr.flush()
        model.train()

    trainer.close()


if __name__ == "__main__":
    main()
    # Training has completed and ``trainer.close()`` has run, but a plain
    # interpreter exit can hang here: the FineWeb/HF-datasets streaming
    # dataloader leaves a non-daemon prefetch thread alive and NCCL keeps its
    # InfiniBand event threads running, so the process never exits -- it sits
    # holding the GPUs and the MASTER_PORT, which makes ``monarch exec`` appear
    # hung and makes the next launch fail with EADDRINUSE. Flush and force-exit
    # so the process (and all of its threads) is reclaimed by the OS now.
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)
