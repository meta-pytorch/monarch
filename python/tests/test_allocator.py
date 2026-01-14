# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
import contextlib
import importlib.resources
import logging
import math
import os
import subprocess
import sys
import unittest
from collections.abc import Callable
from time import sleep
from typing import Generator, Optional
from unittest import mock

import cloudpickle
import monarch.actor
import pytest
import torch
import torch.distributed as dist
import torch.nn.functional as F
from monarch._rust_bindings.monarch_hyperactor.alloc import AllocConstraints, AllocSpec
from monarch._rust_bindings.monarch_hyperactor.channel import (
    ChannelAddr,
    ChannelTransport,
)
from monarch._rust_bindings.monarch_hyperactor.shape import Extent
from monarch._src.actor.allocator import (
    ALLOC_LABEL_PROC_MESH_NAME,
    AllocateMixin,
    LocalAllocator,
    RemoteAllocator,
    StaticRemoteAllocInitializer,
    TorchXRemoteAllocInitializer,
)
from monarch._src.actor.host_mesh import _bootstrap_cmd, HostMesh
from monarch._src.actor.proc_mesh import ProcMesh
from monarch._src.actor.sync_state import fake_sync_state
from monarch.actor import Actor, current_rank, current_size, endpoint, ValueMesh
from monarch.tools.mesh_spec import MeshSpec, ServerSpec
from monarch.tools.network import get_sockaddr
from torch.distributed.elastic.utils.distributed import get_free_port
from torchx.specs import AppState

SERVER_READY = "monarch.tools.commands.server_ready"
UNUSED = "__UNUSED__"


def proc_mesh_from_alloc(
    allocator: AllocateMixin,
    spec: AllocSpec,
    setup: Optional[Callable[[], None]] = None,
    constraints: Optional[AllocConstraints] = None,
) -> ProcMesh:
    return HostMesh.allocate_nonblocking(
        "hosts",
        Extent(*zip(*list(spec.extent.items()))),
        allocator,
        constraints,
        bootstrap_cmd=_bootstrap_cmd(),
    ).spawn_procs(bootstrap=setup)


class EnvCheckActor(Actor):
    """Actor that checks for the presence of an environment variable"""

    def __init__(self) -> None:
        pass

    @endpoint
    async def get_env_var(self, var_name: str) -> str:
        """Return the value of the specified environment variable or 'NOT_SET' if not found"""
        return os.environ.get(var_name, "NOT_SET")


class TestActor(Actor):
    """Silly actor that computes the world size by all-reducing rank-hot tensors"""

    def __init__(self) -> None:
        self.rank: int = current_rank().rank
        self.world_size: int = math.prod(current_size().values())
        self.logger: logging.Logger = logging.getLogger("test_actor")
        self.logger.setLevel(logging.INFO)

    @endpoint
    async def compute_world_size(self, master_addr: str, master_port: int) -> int:
        os.environ["MASTER_ADDR"] = master_addr
        os.environ["MASTER_PORT"] = str(master_port)
        dist.init_process_group("gloo", rank=self.rank, world_size=self.world_size)

        try:
            t = F.one_hot(torch.tensor(self.rank), num_classes=dist.get_world_size())
            dist.all_reduce(t)
            return int(torch.sum(t).item())
        finally:
            dist.destroy_process_group()

    @endpoint
    async def log(self, message: str) -> None:
        print(f"Stdout LogMessage from print: {message}")
        sys.stderr.write(f"Stderr LogMessage from print: {message}\n")
        self.logger.info(f"LogMessage from logger: {message}")


@contextlib.contextmanager
def remote_process_allocator(
    addr: Optional[str] = None,
    timeout: Optional[int] = None,
    envs: Optional[dict[str, str]] = None,
) -> Generator[str, None, None]:
    """Start a remote process allocator on addr. If timeout is not None, have it
    timeout after that many seconds if no messages come in"""

    with importlib.resources.as_file(
        importlib.resources.files(__package__)
    ) as package_path:
        addr = addr or ChannelAddr.any(ChannelTransport.Unix)
        args = [
            "process_allocator",
            f"--addr={addr}",
        ]
        if timeout is not None:
            args.append(f"--timeout-sec={timeout}")

        env = {
            # prefix PATH with this test module's directory to
            # give 'process_allocator' and 'monarch_bootstrap' binary resources
            # in this test module's directory precedence over the installed ones
            # useful in BUCK where these binaries are added as 'resources' of this test target
            "PATH": f"{package_path}:{os.getenv('PATH', '')}",
            "RUST_LOG": "debug",
        }
        if envs:
            env.update(envs)
        process_allocator = subprocess.Popen(
            args=args,
            env=env,
        )
        try:
            yield addr
        finally:
            process_allocator.terminate()
            try:
                five_seconds = 5
                process_allocator.wait(timeout=five_seconds)
            except subprocess.TimeoutExpired:
                process_allocator.kill()


class TestSetupActorInAllocator(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cloudpickle.register_pickle_by_value(sys.modules[TestActor.__module__])

    @classmethod
    def tearDownClass(cls) -> None:
        cloudpickle.unregister_pickle_by_value(sys.modules[TestActor.__module__])

    async def test_setup_lambda_with_multiple_env_vars(self) -> None:
        """Test that the setup lambda can set multiple environment variables"""
        env_vars: dict[str, str] = {
            "TEST_ENV_VAR_1": "value_1",
            "TEST_ENV_VAR_2": "value_2",
            "TEST_ENV_VAR_3": "value_3",
        }

        def setup_multiple_env_vars() -> None:
            for name, value in env_vars.items():
                os.environ[name] = value

        spec = AllocSpec(AllocConstraints(), gpus=1, hosts=1)
        allocator = LocalAllocator()

        proc_mesh = proc_mesh_from_alloc(allocator, spec, setup=setup_multiple_env_vars)
        actor = proc_mesh.spawn("env_check", EnvCheckActor)

        for name, expected_value in env_vars.items():
            actual_value = await actor.get_env_var.call_one(name)
            self.assertEqual(
                actual_value,
                expected_value,
                f"Environment variable {name} was not set correctly",
            )
        await proc_mesh.stop()

    async def test_setup_lambda_with_context_info(self) -> None:
        """Test that the setup lambda can access rank information"""
        context_var_name: str = "PROC_MESH_RANK_INFO"

        def setup_with_rank() -> None:
            context_info = f"point_rank:{current_rank().rank}"
            os.environ[context_var_name] = context_info

        spec = AllocSpec(AllocConstraints(), gpus=1, hosts=1)
        allocator = LocalAllocator()

        proc_mesh = proc_mesh_from_alloc(allocator, spec, setup=setup_with_rank)

        try:
            actor = proc_mesh.spawn("env_check", EnvCheckActor)

            rank_info = await actor.get_env_var.call_one(context_var_name)

            self.assertNotEqual(
                rank_info,
                "NOT_SET",
                "Context information was not stored in the environment variable",
            )
            self.assertIn(
                "point_rank:0",
                rank_info,
                f"Context information {rank_info} does not contain point_rank",
            )
        finally:
            await proc_mesh.stop()


class TestRemoteAllocator(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cloudpickle.register_pickle_by_value(sys.modules[TestActor.__module__])

    @classmethod
    def tearDownClass(cls) -> None:
        cloudpickle.unregister_pickle_by_value(sys.modules[TestActor.__module__])

    def assert_computed_world_size(
        self, computed: ValueMesh[int], expected_world_size: int
    ) -> None:
        expected_world_sizes = {
            rank: expected_world_size for rank in range(0, expected_world_size)
        }
        computed_world_sizes = {p.rank: v for p, v in list(computed.flatten("rank"))}
        self.assertDictEqual(expected_world_sizes, computed_world_sizes)

    async def test_allocate_failure_message(self) -> None:
        # This will generate a supervision failure, and we don't want to crash
        # the test process.
        monarch.actor.unhandled_fault_hook = lambda failure: None
        spec = AllocSpec(AllocConstraints(), host=2, gpu=4)

        with self.assertRaisesRegex(
            Exception,
            r"exited with code 1: Traceback \(most recent call last\).*",
        ):
            with (
                remote_process_allocator(
                    envs={
                        "MONARCH_ERROR_DURING_BOOTSTRAP_FOR_TESTING": "1",
                        "HYPERACTOR_MESH_ENABLE_LOG_FORWARDING": "true",
                        "HYPERACTOR_MESH_ENABLE_FILE_CAPTURE": "true",
                        "HYPERACTOR_MESH_TAIL_LOG_LINES": "100",
                    }
                ) as host1,
                remote_process_allocator(
                    envs={
                        "MONARCH_ERROR_DURING_BOOTSTRAP_FOR_TESTING": "1",
                        "HYPERACTOR_MESH_ENABLE_LOG_FORWARDING": "true",
                        "HYPERACTOR_MESH_ENABLE_FILE_CAPTURE": "true",
                        "HYPERACTOR_MESH_TAIL_LOG_LINES": "100",
                    }
                ) as host2,
            ):
                allocator = RemoteAllocator(
                    world_id="test_remote_allocator",
                    initializer=StaticRemoteAllocInitializer(host1, host2),
                )
                alloc = allocator.allocate(spec)
                await alloc.initialized
                pm = HostMesh.allocate_nonblocking(
                    "hosts",
                    Extent(*zip(*list(spec.extent.items()))),
                    allocator,
                    AllocConstraints(),
                ).spawn_procs()
                await pm.initialized

    async def test_call_allocate_twice(self) -> None:
        class DeletingAllocInitializer(StaticRemoteAllocInitializer):
            """test initializer that removes the last address from the list each time initialize_alloc() is called
            used to test that the state of the initializer is preserved across calls to allocate()
            """

            async def initialize_alloc(self, match_labels: dict[str, str]) -> list[str]:
                alloc = await super().initialize_alloc(match_labels)
                self.addrs.pop(-1)
                return alloc

        with remote_process_allocator() as host1, remote_process_allocator() as host2:
            initializer = DeletingAllocInitializer(host1, host2)

            allocator = RemoteAllocator(
                world_id="test_remote_allocator",
                initializer=initializer,
            )

            spec = AllocSpec(AllocConstraints(), host=1, gpu=1)

            alloc = allocator.allocate(spec)
            await alloc.initialized

            self.assertEqual([host1], initializer.addrs)

            alloc = allocator.allocate(spec)
            await alloc.initialized
            self.assertEqual([], initializer.addrs)

    async def test_throws_when_initializer_returns_empty_addrs(self) -> None:
        class EmptyAllocInitializer(StaticRemoteAllocInitializer):
            """test initializer that returns an empty list of addresses"""

            async def initialize_alloc(self, match_labels: dict[str, str]) -> list[str]:
                _ = match_labels
                return []

        empty_initializer = EmptyAllocInitializer()
        with self.assertRaisesRegex(
            RuntimeError, r"initializer must return non-empty list of addresses"
        ):
            allocator = RemoteAllocator(
                world_id="test_remote_allocator",
                initializer=empty_initializer,
            )
            await allocator.allocate(
                AllocSpec(AllocConstraints(), host=1, gpu=1)
            ).initialized

    @pytest.mark.oss_skip  # pyre-ignore[56]: Pyre cannot infer the type of this pytest marker
    async def test_allocate_2d_mesh(self) -> None:
        hosts = 2
        gpus = 4
        world_size = hosts * gpus
        spec = AllocSpec(AllocConstraints(), host=hosts, gpu=gpus)

        # create 2x process-allocators (on their own bind addresses) to simulate 2 hosts
        with remote_process_allocator() as host1, remote_process_allocator() as host2:
            allocator = RemoteAllocator(
                world_id="test_remote_allocator",
                initializer=StaticRemoteAllocInitializer(host1, host2),
            )
            proc_mesh = proc_mesh_from_alloc(allocator, spec)
            actor = proc_mesh.spawn("test_actor", TestActor)

            values = await actor.compute_world_size.call(
                master_addr="localhost",
                master_port=get_free_port(),
            )

            self.assert_computed_world_size(values, world_size)

    async def test_stop_proc_mesh_blocking(self) -> None:
        spec = AllocSpec(AllocConstraints(), host=2, gpu=4)
        with remote_process_allocator() as host1, remote_process_allocator() as host2:
            allocator = RemoteAllocator(
                world_id="test_remote_allocator",
                initializer=StaticRemoteAllocInitializer(host1, host2),
            )

            proc_mesh = proc_mesh_from_alloc(allocator, spec)
            # XXX - it is not clear why this trying to use
            # async code in a sync context.
            with fake_sync_state():
                actor = proc_mesh.spawn("test_actor", TestActor)
                proc_mesh.stop().get()
            with self.assertRaises(
                RuntimeError, msg="`ProcMesh` has already been stopped"
            ):
                proc_mesh.spawn("test_actor", TestActor).initialized.get()
            del actor

    async def test_wrong_address(self) -> None:
        hosts = 1
        gpus = 1
        spec = AllocSpec(AllocConstraints(), host=hosts, gpu=gpus)

        # create 2x process-allocators (on their own bind addresses) to simulate 2 hosts
        with remote_process_allocator():
            wrong_host = ChannelAddr.any(ChannelTransport.Unix)
            allocator = RemoteAllocator(
                world_id="test_remote_allocator",
                initializer=StaticRemoteAllocInitializer(wrong_host),
            )
            alloc = allocator.allocate(spec)
            await alloc.initialized

            # This message gets undeliverable because it is sent to the wrong host
            # That would normally crash the client
            original_hook = monarch.actor.unhandled_fault_hook
            monarch.actor.unhandled_fault_hook = lambda failure: None
            try:
                with self.assertRaisesRegex(
                    Exception, r"no process has ever been allocated.*"
                ):
                    await proc_mesh_from_alloc(allocator, spec).initialized
            finally:
                # Restore the original hook
                monarch.actor.unhandled_fault_hook = original_hook

    async def test_init_failure(self) -> None:
        class FailInitActor(Actor):
            def __init__(self) -> None:
                if current_rank().rank == 0:
                    raise RuntimeError("fail on init")

            @endpoint
            def dummy(self) -> None:
                pass

        with remote_process_allocator() as host1, remote_process_allocator() as host2:
            allocator = RemoteAllocator(
                world_id="helloworld",
                initializer=StaticRemoteAllocInitializer(host1, host2),
            )
            spec = AllocSpec(AllocConstraints(), host=2, gpu=2)
            proc_mesh = proc_mesh_from_alloc(allocator, spec)
            actor_mesh = proc_mesh.spawn("actor", FailInitActor)

            with self.assertRaisesRegex(
                Exception,
                r"(?s)fail on init(?s)",
            ):
                await actor_mesh.dummy.call()

    @pytest.mark.skip("stop proc mesh not supported yet in v1")
    async def test_stop_proc_mesh(self) -> None:
        spec = AllocSpec(AllocConstraints(), host=2, gpu=4)

        # create 2x process-allocators (on their own bind addresses) to simulate 2 hosts
        with remote_process_allocator() as host1, remote_process_allocator() as host2:
            allocator = RemoteAllocator(
                world_id="test_remote_allocator",
                initializer=StaticRemoteAllocInitializer(host1, host2),
            )
            proc_mesh = proc_mesh_from_alloc(allocator, spec)
            actor = proc_mesh.spawn("test_actor", TestActor)

            await proc_mesh.stop()

            with self.assertRaises(
                RuntimeError, msg="`ProcMesh` has already been stopped"
            ):
                await proc_mesh.spawn("test_actor", TestActor).initialized

            # TODO(agallagher): It'd be nice to test that this just fails
            # immediately, trying to access the wrapped actor mesh, but right
            # now we doing casting without accessing the wrapped type.
            del actor

    @pytest.mark.skip("stop proc mesh not supported yet in v1")
    async def test_stop_proc_mesh_context_manager(self) -> None:
        spec = AllocSpec(AllocConstraints(), host=2, gpu=4)

        # create 2x process-allocators (on their own bind addresses) to simulate 2 hosts
        with remote_process_allocator() as host1, remote_process_allocator() as host2:
            allocator = RemoteAllocator(
                world_id="test_remote_allocator",
                initializer=StaticRemoteAllocInitializer(host1, host2),
            )
            proc_mesh = proc_mesh_from_alloc(allocator, spec)
            with self.assertRaises(ValueError, msg="foo"):
                async with proc_mesh:
                    actor = proc_mesh.spawn("test_actor", TestActor)
                    # Ensure that proc mesh is stopped when context manager exits.
                    raise ValueError("foo")

            with self.assertRaises(
                RuntimeError, msg="`ProcMesh` has already been stopped"
            ):
                await proc_mesh.spawn("test_actor", TestActor).initialized

            # TODO(agallagher): It'd be nice to test that this just fails
            # immediately, trying to access the wrapped actor mesh, but right
            # now we doing casting without accessing the wrapped type.
            del actor

    async def test_setup_lambda_sets_env_vars(self) -> None:
        """Test that the setup lambda can set environment variables during proc_mesh allocation"""
        test_var_name: str = "TEST_ENV_VAR_FOR_PROC_MESH"
        test_var_value: str = "test_value_123"

        def setup_env_vars() -> None:
            os.environ[test_var_name] = test_var_value

        hosts = 2
        gpus = 4
        spec = AllocSpec(AllocConstraints(), host=hosts, gpu=gpus)

        with remote_process_allocator() as host1, remote_process_allocator() as host2:
            allocator = RemoteAllocator(
                world_id="test_remote_allocator",
                initializer=StaticRemoteAllocInitializer(host1, host2),
            )
            proc_mesh = proc_mesh_from_alloc(allocator, spec, setup=setup_env_vars)
            await proc_mesh.initialized
            try:
                actor = proc_mesh.spawn("env_check", EnvCheckActor)

                env_var_values = await actor.get_env_var.call(test_var_name)
                env_var_value = env_var_values.item(host=0, gpu=0)

                self.assertEqual(
                    env_var_value,
                    test_var_value,
                    f"Environment variable {test_var_name} was not set correctly",
                )
            finally:
                await proc_mesh.stop()

    @pytest.mark.skip("stop proc mesh not supported yet in v1")
    async def test_stop_proc_mesh_context_manager_multiple_times(self) -> None:
        spec = AllocSpec(AllocConstraints(), host=2, gpu=4)

        # create 2x process-allocators (on their own bind addresses) to simulate 2 hosts
        with remote_process_allocator() as host1, remote_process_allocator() as host2:
            allocator = RemoteAllocator(
                world_id="test_remote_allocator",
                initializer=StaticRemoteAllocInitializer(host1, host2),
            )
            proc_mesh = proc_mesh_from_alloc(allocator, spec)
            # We can nest multiple context managers on the same mesh, the innermost
            # one closes the mesh and it cannot be used after that.
            async with proc_mesh:
                async with proc_mesh:
                    actor = proc_mesh.spawn("test_actor", TestActor)

                with self.assertRaises(
                    RuntimeError, msg="`ProcMesh` has already been stopped"
                ):
                    await proc_mesh.spawn("test_actor", TestActor).initialized
                # Exiting a second time should not raise an error.

            # TODO(agallagher): It'd be nice to test that this just fails
            # immediately, trying to access the wrapped actor mesh, but right
            # now we doing casting without accessing the wrapped type.
            del actor

    @pytest.mark.oss_skip  # pyre-ignore[56]: Pyre cannot infer the type of this pytest marker
    async def test_remote_allocator_with_no_connection(self) -> None:
        spec = AllocSpec(AllocConstraints(), host=1, gpu=4)

        with remote_process_allocator(timeout=1) as host1:
            # Wait 3 seconds without making any processes, make sure it dies.
            await asyncio.sleep(3)
            allocator = RemoteAllocator(
                world_id="test_remote_allocator",
                initializer=StaticRemoteAllocInitializer(host1),
            )
            with self.assertRaisesRegex(
                Exception, "no process has ever been allocated on"
            ):
                await proc_mesh_from_alloc(allocator, spec).initialized

    @pytest.mark.oss_skip  # pyre-ignore[56]: Pyre cannot infer the type of this pytest marker
    async def test_stacked_1d_meshes(self) -> None:
        # create two stacked actor meshes on the same host
        # each actor mesh running on separate process-allocators

        with (
            remote_process_allocator() as host1_a,
            remote_process_allocator() as host1_b,
        ):
            allocator_a = RemoteAllocator(
                world_id="a",
                initializer=StaticRemoteAllocInitializer(host1_a),
            )
            allocator_b = RemoteAllocator(
                world_id="b",
                initializer=StaticRemoteAllocInitializer(host1_b),
            )

            spec_a = AllocSpec(AllocConstraints(), host=1, gpu=2)
            spec_b = AllocSpec(AllocConstraints(), host=1, gpu=6)

            proc_mesh_a = proc_mesh_from_alloc(allocator_a, spec_a)
            proc_mesh_b = proc_mesh_from_alloc(allocator_b, spec_b)

            actor_a = proc_mesh_a.spawn("actor_a", TestActor)
            actor_b = proc_mesh_b.spawn("actor_b", TestActor)

            results_a = await actor_a.compute_world_size.call(
                master_addr="localhost", master_port=get_free_port()
            )
            results_b = await actor_b.compute_world_size.call(
                master_addr="localhost", master_port=get_free_port()
            )

            self.assert_computed_world_size(results_a, 2)  # a is a 1x2 mesh
            self.assert_computed_world_size(results_b, 6)  # b is a 1x6 mesh

    async def test_torchx_remote_alloc_initializer_no_server(self) -> None:
        with mock.patch(SERVER_READY, return_value=None):
            initializer = TorchXRemoteAllocInitializer("slurm:///123")
            allocator = RemoteAllocator(world_id="test", initializer=initializer)

            with self.assertRaisesRegex(
                RuntimeError,
                r"slurm:///123 does not exist or is in a terminal state",
            ):
                await allocator.allocate(
                    AllocSpec(AllocConstraints(), host=1, gpu=1)
                ).initialized

    async def test_torchx_remote_alloc_initializer_no_match_label_gt_1_meshes(
        self,
    ) -> None:
        # asserts that an exception is raised if no match label is specified in alloc constraints
        # but there are more than 1 mesh (hence ambiguous which mesh to allocate on)

        server = ServerSpec(
            name=UNUSED,
            scheduler=UNUSED,
            state=AppState.RUNNING,
            meshes=[MeshSpec(name="x", num_hosts=1), MeshSpec(name="y", num_hosts=1)],
        )

        with mock.patch(SERVER_READY, return_value=server):
            initializer = TorchXRemoteAllocInitializer("slurm:///123")
            allocator = RemoteAllocator(world_id="test", initializer=initializer)

            with self.assertRaisesRegex(
                RuntimeError,
                r"2 proc meshes in slurm:///123, please specify the mesh name as a match label `procmesh.monarch.meta.com/name`",
            ):
                await allocator.allocate(
                    AllocSpec(AllocConstraints(), host=1, gpu=1)
                ).initialized

    # Skipping test temporarily due to blocking OSS CI TODO: @rusch T232884876
    @pytest.mark.oss_skip  # pyre-ignore[56]: Pyre cannot infer the type of this pytest marker
    async def test_torchx_remote_alloc_initializer_no_match_label_1_mesh(self) -> None:
        server = ServerSpec(
            name=UNUSED,
            scheduler=UNUSED,
            state=AppState.RUNNING,
            meshes=[
                MeshSpec(
                    name="x",
                    num_hosts=1,
                    transport="tcp",
                    hostnames=["localhost"],
                )
            ],
        )
        port = get_free_port()

        with remote_process_allocator(
            addr=f"tcp!{get_sockaddr('localhost', port)}",
        ):
            with mock.patch(SERVER_READY, return_value=server):
                initializer = TorchXRemoteAllocInitializer("local:///test", port=port)
                allocator = RemoteAllocator(
                    world_id="test",
                    initializer=initializer,
                )
                spec = AllocSpec(AllocConstraints(), host=1, gpu=4)
                proc_mesh = proc_mesh_from_alloc(allocator, spec)
                actor = proc_mesh.spawn("test_actor", TestActor)
                results = await actor.compute_world_size.call(
                    master_addr="localhost", master_port=get_free_port()
                )
                self.assert_computed_world_size(results, 4)  # 1x4 mesh

    # Skipping test temporarily due to blocking OSS CI TODO: @rusch T232884876
    @pytest.mark.oss_skip  # pyre-ignore[56]: Pyre cannot infer the type of this pytest marker
    async def test_torchx_remote_alloc_initializer_with_match_label(self) -> None:
        server = ServerSpec(
            name=UNUSED,
            scheduler=UNUSED,
            state=AppState.RUNNING,
            meshes=[
                MeshSpec(
                    name="x",
                    num_hosts=1,
                    transport="tcp",
                    hostnames=["localhost"],
                )
            ],
        )
        port = get_free_port()

        with remote_process_allocator(
            addr=f"tcp!{get_sockaddr('localhost', port)}",
        ):
            with mock.patch(SERVER_READY, return_value=server):
                initializer = TorchXRemoteAllocInitializer("local:///test", port=port)
                allocator = RemoteAllocator(
                    world_id="test",
                    initializer=initializer,
                )
                spec = AllocSpec(
                    AllocConstraints(match_labels={ALLOC_LABEL_PROC_MESH_NAME: "x"}),
                    host=1,
                    gpu=3,
                )
                proc_mesh = proc_mesh_from_alloc(allocator, spec)
                actor = proc_mesh.spawn("test_actor", TestActor)
                results = await actor.compute_world_size.call(
                    master_addr="localhost", master_port=get_free_port()
                )
                self.assert_computed_world_size(results, 3)  # 1x3 mesh

    async def test_torchx_remote_alloc_initializer_with_match_label_no_match(
        self,
    ) -> None:
        # assert that match label with a mesh name that does not exist should error out

        server = ServerSpec(
            name="test",
            scheduler=UNUSED,
            state=AppState.RUNNING,
            meshes=[
                MeshSpec(
                    name="x",
                    num_hosts=1,
                    transport="tcp",
                    hostnames=["localhost"],
                )
            ],
        )

        with mock.patch(SERVER_READY, return_value=server):
            with self.assertRaisesRegex(RuntimeError, r"'y' not found in job: test"):
                initializer = TorchXRemoteAllocInitializer("local:///test")
                allocator = RemoteAllocator(world_id="test", initializer=initializer)
                spec = AllocSpec(
                    AllocConstraints(match_labels={ALLOC_LABEL_PROC_MESH_NAME: "y"}),
                    host=1,
                    gpu=1,
                )
                alloc = allocator.allocate(spec)
                await alloc.initialized
                await proc_mesh_from_alloc(allocator, spec).initialized

    async def test_log(self) -> None:
        # create a mesh to log to both stdout and stderr

        with remote_process_allocator() as host:
            allocator = RemoteAllocator(
                world_id="test_actor_logger",
                initializer=StaticRemoteAllocInitializer(host),
            )

            spec = AllocSpec(AllocConstraints(), host=1, gpu=2)

            proc_mesh = proc_mesh_from_alloc(allocator, spec)

            # Generate aggregated log every 1 second.
            await proc_mesh.logging_option(True, 1)
            actor = proc_mesh.spawn("actor", TestActor)
            # Run for 4 seconds, every second generates 5 logs, so we expect to see
            # 2 actors x 5 logs/actor/sec * 1 sec = 10 logs per aggregation.
            for _ in range(20):
                await actor.log.call("Expect to see [10 processes]")
                sleep(0.2)
            # Generate aggregated log every 2 seconds.
            await proc_mesh.logging_option(True, 2)
            # Run for 8 seconds, every second generates 5 logs, so we expect to see
            # 2 actors x 5 logs/actor/sec * 2 sec = 20 logs per aggregation.
            for _ in range(40):
                await actor.log.call("Expect to see [20 processes]")
                sleep(0.2)

            print("======== All Done ========")
