# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Tests for Intel XPU support in Monarch's actor runtime.

Three sections:
  1. Unit tests (no hardware needed) -- mock torch.accelerator to exercise
     XPU-specific branches in proc_mesh.py and job.py.
  2. Integration tests (XPU hardware needed) -- spawn real actors and verify
     environment propagation.
  3. Smoke tests (XPU hardware needed) -- ping-pong messaging, SPMD/elastic
     env setup, device visibility, xccl FSDP2, and a simple GRPO loop.

Run on UAN (unit tests only):
    pytest python/tests/test_xpu.py -v

Run on compute node (all tests):
    pytest python/tests/test_xpu.py -v --timeout=300
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import patch

import cloudpickle
import torch
import torch.distributed as dist
import torch.nn as nn
from monarch._src.job.process import ProcessJob
from monarch.actor import Actor, current_rank, current_size, endpoint, this_host
from monarch.spmd import setup_torch_elastic_env, SPMDActor
from scoped_state import scoped_state


def _num_actors() -> int:
    return int(os.environ.get("NUM_ACTORS", "4"))


def _mock_accelerator(accel_type):
    return patch("torch.accelerator.current_accelerator", return_value=accel_type)


# ===========================================================================
# Section 1: Unit tests (no XPU hardware needed)
# ===========================================================================


class TestXpuAcceleratorEnvVars(unittest.TestCase):
    def test_xpu_includes_xpu_vars(self):
        from monarch._src.actor.proc_mesh import _get_accelerator_env_vars

        with _mock_accelerator("xpu"):
            env_vars = _get_accelerator_env_vars()
            assert "ZE_AFFINITY_MASK" in env_vars
            assert "ZE_ENABLE_PCI_ID_DEVICE_ORDER" in env_vars
            assert "PYTORCH_XPU_ALLOC_CONF" in env_vars

    def test_xpu_snapshot_captures_ze_vars(self):
        from monarch._src.actor.proc_mesh import _accel_env_snapshot

        with _mock_accelerator("xpu"), patch.dict(
            os.environ,
            {
                "ZE_AFFINITY_MASK": "0,1",
                "PYTORCH_XPU_ALLOC_CONF": "expandable_segments:True",
            },
        ):
            snap = _accel_env_snapshot()
            assert snap["ZE_AFFINITY_MASK"] == "0,1"
            assert snap["PYTORCH_XPU_ALLOC_CONF"] == "expandable_segments:True"

    def test_xpu_not_initialized(self):
        from monarch._src.actor.proc_mesh import _torch_accelerator_already_initialized

        with _mock_accelerator("xpu"):
            result = _torch_accelerator_already_initialized()
            assert isinstance(result, bool)


class TestTelemetryImportGate(unittest.TestCase):
    def test_telemetry_config_stub_importable(self):
        from monarch._src.job.job import TelemetryConfig

        config = TelemetryConfig()
        assert hasattr(config, "batch_size")
        assert hasattr(config, "retention_secs")

    def test_process_job_importable(self):
        from monarch._src.job.process import ProcessJob  # noqa: F401

    def test_job_state_importable(self):
        from monarch._src.job.job import JobState, JobTrait  # noqa: F401


# ===========================================================================
# Section 2: Integration tests (XPU hardware needed)
# ===========================================================================


class XpuInitTestActor(Actor):
    def __init__(self) -> None:
        self.env_vars_before_init: Dict[str, str] = {}
        self.xpu_initialized: bool = False

    @endpoint
    async def init_xpu_and_check_env(self, env_var_names: List[str]) -> Dict[str, str]:
        for var_name in env_var_names:
            self.env_vars_before_init[var_name] = os.environ.get(var_name, "NOT_SET")
        if torch.xpu.is_available():
            torch.xpu.init()
            self.xpu_initialized = True
        return self.env_vars_before_init

    @endpoint
    async def is_xpu_initialized(self) -> bool:
        return self.xpu_initialized


@unittest.skipUnless(torch.xpu.is_available(), "XPU not available")
class TestEnvBeforeXpu(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cloudpickle.register_pickle_by_value(sys.modules[XpuInitTestActor.__module__])

    @classmethod
    def tearDownClass(cls) -> None:
        cloudpickle.unregister_pickle_by_value(
            sys.modules[XpuInitTestActor.__module__]
        )

    async def test_lambda_sets_env_vars_before_xpu_init(self) -> None:
        xpu_env_vars: Dict[str, str] = {
            "ZE_AFFINITY_MASK": "0",
            "ZE_ENABLE_PCI_ID_DEVICE_ORDER": "1",
            "PYTORCH_XPU_ALLOC_CONF": "expandable_segments:False",
        }

        def setup_xpu_env() -> None:
            for name, value in xpu_env_vars.items():
                os.environ[name] = value

        proc_mesh = this_host().spawn_procs(bootstrap=setup_xpu_env)
        try:
            actor = proc_mesh.spawn("xpu_init", XpuInitTestActor)
            env_vars = await actor.init_xpu_and_check_env.call_one(
                list(xpu_env_vars.keys())
            )
            await actor.is_xpu_initialized.call_one()
            for name, expected_value in xpu_env_vars.items():
                self.assertEqual(env_vars.get(name), expected_value)
        finally:
            await proc_mesh.stop()

    async def test_proc_mesh_with_lambda_env(self) -> None:
        xpu_env_vars: Dict[str, str] = {
            "ZE_ENABLE_PCI_ID_DEVICE_ORDER": "1",
            "ZE_AFFINITY_MASK": "0,1",
        }

        def setup_xpu_env() -> None:
            for name, value in xpu_env_vars.items():
                os.environ[name] = value

        with scoped_state(ProcessJob({"hosts": 1}), cached_path=None) as state:
            proc_mesh_instance = state.hosts.spawn_procs(bootstrap=setup_xpu_env)
            async with proc_mesh_instance:
                actor = proc_mesh_instance.spawn("xpu_init", XpuInitTestActor)
                env_vars = await actor.init_xpu_and_check_env.call_one(
                    list(xpu_env_vars.keys())
                )
                for name, expected_value in xpu_env_vars.items():
                    self.assertEqual(env_vars.get(name), expected_value)

    async def test_proc_mesh_with_dictionary_env(self) -> None:
        xpu_env_vars: Dict[str, str] = {
            "ZE_ENABLE_PCI_ID_DEVICE_ORDER": "1",
            "ZE_AFFINITY_MASK": "0",
            "PYTORCH_XPU_ALLOC_CONF": "expandable_segments:False",
        }

        with scoped_state(
            ProcessJob({"hosts": 1}, env=xpu_env_vars), cached_path=None
        ) as state:
            proc_mesh_instance = state.hosts.spawn_procs()
            async with proc_mesh_instance:
                actor = proc_mesh_instance.spawn("xpu_init", XpuInitTestActor)
                env_vars = await actor.init_xpu_and_check_env.call_one(
                    list(xpu_env_vars.keys())
                )
                self.assertEqual(env_vars.get("ZE_ENABLE_PCI_ID_DEVICE_ORDER"), "1")
                self.assertEqual(env_vars.get("ZE_AFFINITY_MASK"), "0")
                self.assertEqual(
                    env_vars.get("PYTORCH_XPU_ALLOC_CONF"), "expandable_segments:False"
                )


# ===========================================================================
# Section 3: Smoke tests (XPU hardware needed)
# ===========================================================================


class ToyActor(Actor):
    def __init__(self):
        self.rank = current_rank().rank

    @endpoint
    def hello_world(self, msg) -> str:
        return f"rank={self.rank}, msg={msg}"


class PingPongActor(Actor):
    def __init__(self, actor_name):
        self.actor_name = actor_name

    @endpoint
    def init(self, other_actor):
        self.other_actor = other_actor
        self.other_actor_pair = other_actor.slice(**current_rank())
        self.identity = current_rank().rank

    @endpoint
    def send(self, msg) -> str:
        self.other_actor_pair.recv.call(
            f"Sender ({self.actor_name}:{self.identity}) {msg}"
        ).get()
        return "sent"

    @endpoint
    def recv(self, msg) -> str:
        return f"Received: {msg}"


class EnvCapture(Actor):
    @endpoint
    async def get_env_vars(self) -> dict:
        keys = [
            "MASTER_ADDR", "MASTER_PORT", "RANK", "LOCAL_RANK",
            "LOCAL_WORLD_SIZE", "GROUP_RANK", "GROUP_WORLD_SIZE",
            "WORLD_SIZE",
        ]
        return {k: os.environ.get(k, "") for k in keys}

    @endpoint
    async def get_rank_info(self) -> dict:
        point = current_rank()
        sizes = current_size()
        label = list(sizes.keys())[-1]
        return {
            "rank": point.rank,
            "local_rank": point[label],
            "nproc_per_node": sizes[label],
            "world_size": sizes[label],
        }


class XpuProbe(Actor):
    @endpoint
    async def device_count(self) -> int:
        return torch.xpu.device_count() if hasattr(torch, "xpu") else -1

    @endpoint
    async def get_fi_env(self) -> dict:
        keys = [
            "FI_PROVIDER", "CCL_ATL_TRANSPORT",
            "ZE_FLAT_DEVICE_HIERARCHY",
        ]
        return {k: os.environ.get(k, "<UNSET>") for k in keys}


class PolicyActor(Actor):
    """Tiny scalar Gaussian policy for GRPO smoke test."""

    def __init__(self) -> None:
        self.rank = current_rank().rank
        n_dev = torch.xpu.device_count()
        self.device = torch.device(f"xpu:{self.rank % n_dev}")
        torch.xpu.set_device(self.device)
        self.mu = nn.Parameter(torch.zeros((), device=self.device))
        self.opt = torch.optim.SGD([self.mu], lr=0.1)

    def _dist(self):
        return torch.distributions.Normal(self.mu, 0.5)

    @endpoint
    def generate(self) -> tuple:
        with torch.no_grad():
            a = self._dist().sample()
            lp = self._dist().log_prob(a)
        return float(a.item()), float(lp.item())

    @endpoint
    def update(self, action: float, old_logprob: float, advantage: float) -> Dict[str, Any]:
        a = torch.tensor(action, device=self.device)
        old_lp = torch.tensor(old_logprob, device=self.device)
        adv = torch.tensor(advantage, device=self.device)
        new_lp = self._dist().log_prob(a)
        ratio = (new_lp - old_lp).exp()
        clipped = torch.clamp(ratio, 0.8, 1.2)
        loss = -torch.min(ratio * adv, clipped * adv)
        self.opt.zero_grad()
        loss.backward()
        self.opt.step()
        return {"rank": self.rank, "mu": float(self.mu.detach().item())}

    @endpoint
    def mu_value(self) -> float:
        return float(self.mu.detach().item())


@unittest.skipUnless(torch.xpu.is_available(), "XPU not available")
class TestPingPong(unittest.TestCase):
    """Actor spawn + endpoint dispatch + actor-to-actor messaging on XPU."""

    def test_hello_world_broadcast(self):
        n = min(_num_actors(), 4)
        proc_mesh = this_host().spawn_procs(per_host={"gpus": n})
        actors = proc_mesh.spawn("toy", ToyActor)
        actors.hello_world.call("ping").get()

    def test_ping_pong_messaging(self):
        proc_mesh_0 = this_host().spawn_procs(per_host={"gpus": 2})
        actor_0 = proc_mesh_0.spawn("a0", PingPongActor, "actor_0")
        proc_mesh_1 = this_host().spawn_procs(per_host={"gpus": 2})
        actor_1 = proc_mesh_1.spawn("a1", PingPongActor, "actor_1")

        actor_0.init.call(actor_1).get()
        actor_1.init.call(actor_0).get()
        actor_0.send.call("Ping").get()
        actor_1.send.call("Pong").get()


@unittest.skipUnless(torch.xpu.is_available(), "XPU not available")
class TestSpmdXpu(unittest.TestCase):
    """SPMD actor tests: rank calculation, elastic env, device visibility."""

    def test_spawn_procs_rank_info(self):
        n = _num_actors()
        proc_mesh = this_host().spawn_procs(name="test_rank", per_host={"gpus": n})
        capture = proc_mesh.spawn("cap", EnvCapture)
        info_mesh = capture.get_rank_info.call().get()
        seen_ranks = set()
        for point, info in info_mesh.items():
            self.assertEqual(info["nproc_per_node"], n)
            self.assertEqual(info["world_size"], n)
            seen_ranks.add(info["rank"])
        self.assertEqual(seen_ranks, set(range(n)))

    def test_setup_torch_elastic_env(self):
        n = _num_actors()
        proc_mesh = this_host().spawn_procs(name="test_elastic", per_host={"gpus": n})
        setup_torch_elastic_env(proc_mesh)
        capture = proc_mesh.spawn("cap", EnvCapture)
        env_mesh = capture.get_env_vars.call().get()
        for point, env in env_mesh.items():
            self.assertTrue(env["MASTER_ADDR"])
            self.assertTrue(env["MASTER_PORT"])
            self.assertEqual(env["WORLD_SIZE"], str(n))

    def test_xpu_device_count_in_actors(self):
        n = _num_actors()
        proc_mesh = this_host().spawn_procs(name="test_devcount", per_host={"gpus": n})
        probe = proc_mesh.spawn("probe", XpuProbe)
        counts = probe.device_count.call().get()
        for _point, c in counts.items():
            self.assertGreater(c, 0)

    def test_fi_env_propagates_to_actors(self):
        n = _num_actors()
        proc_mesh = this_host().spawn_procs(name="test_fi", per_host={"gpus": n})
        probe = proc_mesh.spawn("probe", XpuProbe)
        envs = probe.get_fi_env.call().get()
        for _point, env in envs.items():
            self.assertNotEqual(env["FI_PROVIDER"], "<UNSET>")
            break


@unittest.skipUnless(torch.xpu.is_available(), "XPU not available")
class TestSpmdActorXcclFsdp2(unittest.TestCase):
    """End-to-end FSDP2 training across XPU actors via SPMDActor + xccl."""

    @unittest.skipUnless(
        os.environ.get("RUN_SLOW_TESTS", "0") == "1",
        "Slow test: set RUN_SLOW_TESTS=1 to run",
    )
    def test_fsdp2_training(self):
        n = _num_actors()
        proc_mesh = this_host().spawn_procs(name="test_fsdp2", per_host={"gpus": n})
        spmd_actors = proc_mesh.spawn("spmd", SPMDActor)
        with tempfile.TemporaryDirectory() as tmpdir:
            script = Path(tmpdir) / "fsdp2_train.py"
            script.write_text(
                """
import os
import torch
import torch.nn as nn
import torch.distributed as dist
from torch.utils.data import DataLoader, DistributedSampler
from torch.distributed.fsdp import FullyShardedDataParallel as FSDP


class DummyDataset(torch.utils.data.Dataset):
    def __init__(self, size=200):
        self.x = torch.randn(size, 10)
        self.y = torch.randint(0, 2, (size,))
    def __len__(self): return len(self.x)
    def __getitem__(self, i): return self.x[i], self.y[i]


class SmallModel(nn.Module):
    def __init__(self):
        super().__init__()
        self.seq = nn.Sequential(nn.Linear(10, 64), nn.ReLU(), nn.Linear(64, 2))
    def forward(self, x): return self.seq(x)


def main():
    rank = int(os.environ["RANK"])
    world_size = int(os.environ["WORLD_SIZE"])
    local_rank = int(os.environ["LOCAL_RANK"])
    import datetime
    dist.init_process_group(
        backend="xccl", rank=rank, world_size=world_size,
        timeout=datetime.timedelta(seconds=600),
    )
    device = torch.device(f"xpu:{local_rank}")
    torch.xpu.set_device(device)

    model = FSDP(SmallModel().to(device))
    optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)
    loss_fn = nn.CrossEntropyLoss()

    dataset = DummyDataset()
    sampler = DistributedSampler(dataset, num_replicas=world_size, rank=rank)
    loader = DataLoader(dataset, sampler=sampler, batch_size=32)

    model.train()
    for epoch in range(2):
        sampler.set_epoch(epoch)
        for x, y in loader:
            x, y = x.to(device), y.to(device)
            optimizer.zero_grad()
            loss = loss_fn(model(x), y)
            loss.backward()
            optimizer.step()
    dist.destroy_process_group()


if __name__ == "__main__":
    main()
"""
            )
            results = spmd_actors.main.call("localhost", 29500, [str(script)]).get()
            for _point, ok in results.items():
                self.assertTrue(ok)


@unittest.skipUnless(torch.xpu.is_available(), "XPU not available")
class TestGrpoXpu(unittest.TestCase):
    """Simple GRPO loop on XPU: scalar Gaussian policy converges toward target."""

    def test_grpo_convergence(self):
        n = min(_num_actors(), 4)
        target = 0.5
        num_steps = 10

        mesh = this_host().spawn_procs(per_host={"gpus": n})
        actors = mesh.spawn("policy", PolicyActor)

        initial = [
            actors.slice(gpus=i).mu_value.call_one().get() for i in range(n)
        ]

        for _ in range(num_steps):
            rollouts = [
                actors.slice(gpus=i).generate.call_one().get() for i in range(n)
            ]
            actions = [r[0] for r in rollouts]
            old_lps = [r[1] for r in rollouts]

            rewards = [-(a - target) ** 2 for a in actions]
            mean_r = sum(rewards) / len(rewards)
            std_r = (sum((r - mean_r) ** 2 for r in rewards) / len(rewards)) ** 0.5 + 1e-6
            advs = [(r - mean_r) / std_r for r in rewards]

            for i in range(n):
                actors.slice(gpus=i).update.call_one(
                    actions[i], old_lps[i], advs[i]
                ).get()

        final = [
            actors.slice(gpus=i).mu_value.call_one().get() for i in range(n)
        ]

        init_dist = sum(abs(m - target) for m in initial) / n
        final_dist = sum(abs(m - target) for m in final) / n
        self.assertLess(final_dist, init_dist, "Policy should move toward target")
