# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import asyncio
import unittest
from datetime import timedelta
from typing import Mapping, Optional
from unittest import mock
from unittest.mock import MagicMock

from monarch.tools import commands
from monarch.tools.commands import component_args_from_cli, server_ready, torchx_runner

from monarch.tools.config import (  # @manual=//monarch/python/monarch/tools/config/meta:defaults
    Config,
    defaults,
    EmptyWorkspaceOption,
)
from monarch.tools.mesh_spec import MeshSpec, ServerSpec
from torchx.specs import AppDef, AppDryRunInfo, AppState, AppStatus, CfgVal, Role

CMD_INFO = "monarch.tools.commands.info"
CMD_CREATE = "monarch.tools.commands.create"


class TestCommands(unittest.TestCase):
    def test_component_args_from_cli(self) -> None:
        def fn(h: str, num_hosts: int) -> AppDef:
            return AppDef("_unused_", roles=[Role("_unused_", "_unused_")])

        args = component_args_from_cli(fn, ["h=gpu.medium", "num_hosts=4"])

        # should be able to call the component function with **args as kwargs
        self.assertIsNotNone(fn(**args))
        self.assertDictEqual({"h": "gpu.medium", "num_hosts": 4}, args)

    def test_create_dryrun(self) -> None:
        scheduler = "slurm"
        config = defaults.config(scheduler)
        config.dryrun = True
        config.appdef = defaults.component_fn(scheduler)()

        dryrun_info = commands.create(config)
        # need only assert that the return type of dryrun is a dryrun info object
        # since we delegate to torchx for job submission
        self.assertIsInstance(dryrun_info, AppDryRunInfo)

    @mock.patch(
        "torchx.schedulers.slurm_scheduler.SlurmScheduler.schedule",
        return_value="test_job_id",
    )
    def test_create(self, mock_schedule: mock.MagicMock) -> None:
        scheduler = "slurm"
        config = defaults.config(scheduler)
        config.appdef = defaults.component_fn(scheduler)()
        server_handle = commands.create(config)

        mock_schedule.assert_called_once()
        self.assertEqual(server_handle, "slurm:///test_job_id")

    @mock.patch(
        "torchx.schedulers.slurm_scheduler.SlurmScheduler.schedule",
        return_value="test_job_id",
    )
    def test_create_empty_workspace(self, mock_schedule: mock.MagicMock) -> None:
        scheduler = "slurm"
        config = defaults.config(scheduler)
        config.appdef = defaults.component_fn(scheduler)()

        appdef: AppDef = AppDef("myjob", config.appdef.roles, config.appdef.metadata)
        dryrun_info: AppDryRunInfo = torchx_runner().dryrun(appdef, scheduler)

        def non_empty_workspace_dryrun(
            self: object,
            app: AppDef,
            scheduler: str,
            cfg: Optional[Mapping[str, CfgVal]] = None,
            workspace: Optional[str] = None,
            parent_run_id: Optional[str] = None,
        ) -> AppDryRunInfo:
            # Assert on workspace parameter
            assert workspace is not None, "Workspace should not be None"
            return dryrun_info

        with mock.patch(
            "monarch.tools.commands.Runner.dryrun", non_empty_workspace_dryrun
        ):
            # no workspace
            config.workspace = EmptyWorkspaceOption.NO_WORKSPACE
            server_handle = commands.create(config)
            self.assertEqual(server_handle, "slurm:///test_job_id")

        def empty_workspace_dryrun(
            self: object,
            app: AppDef,
            scheduler: str,
            cfg: Optional[Mapping[str, CfgVal]] = None,
            workspace: Optional[str] = None,
            parent_run_id: Optional[str] = None,
        ) -> AppDryRunInfo:
            # Assert on workspace parameter
            assert workspace is None, "Workspace should be None"
            return dryrun_info

        with mock.patch("monarch.tools.commands.Runner.dryrun", empty_workspace_dryrun):
            # workspace from image
            config.workspace = EmptyWorkspaceOption.USE_PROVIDED_IMAGE
            server_handle = commands.create(config)
            self.assertEqual(server_handle, "slurm:///test_job_id")

    @mock.patch("monarch.tools.commands.Runner.cancel")
    def test_kill(self, mock_cancel: mock.MagicMock) -> None:
        handle = "slurm:///test_job_id"
        commands.kill(handle)
        mock_cancel.assert_called_once_with(handle)

    @mock.patch("monarch.tools.commands.Runner.status", return_value=None)
    def test_info_non_existent_server(self, _: mock.MagicMock) -> None:
        self.assertIsNone(commands.info("slurm:///job-does-not-exist"))

    @mock.patch("monarch.tools.commands.Runner.describe")
    @mock.patch("monarch.tools.commands.Runner.status")
    def test_info(
        self, mock_status: mock.MagicMock, mock_describe: mock.MagicMock
    ) -> None:
        appstatus = AppStatus(state=AppState.RUNNING)
        mock_status.return_value = appstatus

        appdef = AppDef(
            name="monarch_test_123",
            roles=[
                Role(
                    name="trainer",
                    image="__unused__",
                    num_replicas=4,
                    port_map={"mesh": 26501},
                )
            ],
            metadata={
                "monarch/meshes/trainer/host_type": "gpu.medium",
                "monarch/meshes/trainer/gpus": "2",
            },
        )
        mock_describe.return_value = appdef

        self.assertEqual(
            ServerSpec(
                name="monarch_test_123",
                scheduler="slurm",
                state=appstatus.state,
                meshes=[
                    MeshSpec(
                        name="trainer",
                        num_hosts=4,
                        host_type="gpu.medium",
                        gpus=2,
                        port=26501,
                    )
                ],
            ),
            commands.info("slurm:///job-id"),
        )


UNUSED = "__UNUSED__"
_5_MS = timedelta(milliseconds=5)


def server(state: AppState, name: str = UNUSED) -> ServerSpec:
    mesh_x = MeshSpec(name="x", num_hosts=2, host_type=UNUSED, gpus=-1)
    mesh_y = MeshSpec(name="y", num_hosts=4, host_type=UNUSED, gpus=-1)
    meshes = [mesh_x, mesh_y]

    if state == AppState.RUNNING:
        for mesh in meshes:
            mesh.hostnames = [f"node{i}" for i in range(mesh.num_hosts)]
            mesh.state = AppState.RUNNING

    return ServerSpec(name=name, scheduler="slurm", state=state, meshes=meshes)


class TestCommandsAsync(unittest.IsolatedAsyncioTestCase):
    async def test_server_ready_server_does_not_exist(self) -> None:
        with mock.patch(
            CMD_INFO,
            return_value=None,
        ):
            server_info = await server_ready("slurm:///123", check_interval=_5_MS)
            self.assertIsNone(server_info)

    async def test_server_ready_pending_to_running(self) -> None:
        with mock.patch(
            CMD_INFO,
            side_effect=[
                server(AppState.UNSUBMITTED),
                server(AppState.SUBMITTED),
                server(AppState.PENDING),
                server(AppState.PENDING),
                server(AppState.RUNNING),
                server(AppState.CANCELLED),
            ],
        ) as mock_info:
            server_info = await server_ready("slurm:///123", check_interval=_5_MS)

            self.assertIsNotNone(server_info)
            self.assertTrue(server_info.is_running)
            self.assertEqual(server_info.state, AppState.RUNNING)

            mesh_x = server_info.get_mesh_spec("x")
            mesh_y = server_info.get_mesh_spec("y")
            self.assertListEqual(mesh_x.hostnames, ["node0", "node1"])
            self.assertListEqual(mesh_y.hostnames, ["node0", "node1", "node2", "node3"])

            mock_info.assert_called()
            # called 5 times, once for UNSUBMITTED, SUBMITTED, PENDING, PENDING, and RUNNING
            self.assertEqual(mock_info.call_count, 5)

    async def test_server_ready_pending_to_terminal(self) -> None:
        for terminal_state in [AppState.SUCCEEDED, AppState.FAILED, AppState.CANCELLED]:
            with self.subTest(terminal_state=terminal_state):
                with mock.patch(
                    CMD_INFO,
                    side_effect=[
                        server(AppState.SUBMITTED),
                        server(AppState.PENDING),
                        server(AppState.PENDING),
                        server(terminal_state),
                    ],
                ) as mock_info:
                    server_info = await server_ready(
                        "slurm:///123",
                        check_interval=_5_MS,
                    )

                    self.assertIsNotNone(server_info)
                    self.assertEqual(server_info.state, terminal_state)
                    mock_info.assert_called()
                    self.assertEqual(mock_info.call_count, 4)

    async def test_server_ready_running_but_mesh_not_running(self) -> None:
        def no_host_server(state: AppState, name: str = UNUSED) -> ServerSpec:
            mesh_x = MeshSpec(name="x", num_hosts=2, host_type=UNUSED, gpus=-1)
            mesh_y = MeshSpec(name="y", num_hosts=4, host_type=UNUSED, gpus=-1)
            meshes = [mesh_x, mesh_y]
            return ServerSpec(name=name, scheduler="slurm", state=state, meshes=meshes)

        # not ready even it's running
        with mock.patch(
            CMD_INFO,
            return_value=no_host_server(AppState.RUNNING),
        ):
            try:
                await asyncio.wait_for(
                    server_ready(
                        "slurm:///123",
                        check_interval=_5_MS,
                    ),
                    timeout=2,
                )
                raise RuntimeError("we should have timed out")
            except asyncio.TimeoutError:
                pass

        # ready only if we have hosts
        with mock.patch(
            CMD_INFO,
            side_effect=[
                server(AppState.RUNNING),
            ],
        ) as mock_info:
            server_info = await server_ready(
                "slurm:///123",
                check_interval=_5_MS,
            )

            self.assertIsNotNone(server_info)
            mock_info.assert_called()

    @mock.patch(CMD_INFO, side_effect=[server(AppState.RUNNING, name="123")])
    async def test_get_or_create_existing(self, mock_info: MagicMock) -> None:
        scheduler = "slurm"
        config = Config(
            scheduler=scheduler,
            scheduler_args={},
            appdef=defaults.component_fn(scheduler)(),
        )
        server_info = await commands.get_or_create(name="123", config=config)
        self.assertEqual(server_info.server_handle, "slurm:///123")
        mock_info.assert_called_once_with("slurm:///123")

    async def test_get_or_create(self) -> None:
        for existing_state in [
            None,
            server(AppState.FAILED, name="123"),
            server(AppState.SUCCEEDED, name="123"),
        ]:
            with self.subTest(existing_state=existing_state):
                with mock.patch(
                    CMD_INFO,
                    side_effect=[
                        # -- state for slurm:///123
                        existing_state,
                        # -- states for (new) slurm:///456
                        server(AppState.PENDING, name="456"),
                        server(AppState.RUNNING, name="456"),
                    ],
                ) as mock_info, mock.patch(
                    CMD_CREATE, return_value="slurm:///456"
                ) as mock_create:
                    config = Config(
                        scheduler="slurm",
                        scheduler_args={},
                        appdef=defaults.component_fn("slurm")(),
                    )
                    server_info = await commands.get_or_create(
                        name="123",
                        config=config,
                        check_interval=_5_MS,
                    )

                    mock_create.called_once_with(config, "123")
                    self.assertEqual(server_info.server_handle, "slurm:///456")
                    self.assertListEqual(
                        mock_info.call_args_list,
                        [
                            mock.call("slurm:///123"),
                            mock.call("slurm:///456"),
                            mock.call("slurm:///456"),
                        ],
                    )

    @mock.patch(
        CMD_INFO,
        side_effect=[
            # -- slurm:///123 not found
            None,
            # -- states for (new) slurm:///456
            server(AppState.PENDING, name="456"),
            server(AppState.FAILED, name="456"),
        ],
    )
    @mock.patch(CMD_CREATE, return_value="slurm:///456")
    async def test_get_or_create_new_server_failed(
        self,
        _1: MagicMock,
        _2: MagicMock,
    ) -> None:
        config = Config(
            scheduler="slurm",
            scheduler_args={},
            appdef=defaults.component_fn("slurm")(),
        )
        with self.assertRaises(RuntimeError):
            _ = await commands.get_or_create(
                name="123",
                config=config,
                check_interval=_5_MS,
            )

    @mock.patch(
        CMD_INFO,
        side_effect=[
            # -- slurm:///123 not found
            None,
            # -- (new) slurm:///456 goes missing
            None,
        ],
    )
    @mock.patch(CMD_CREATE, return_value="slurm:///456")
    async def test_get_or_create_new_server_missing(
        self,
        _1: MagicMock,
        _2: MagicMock,
    ) -> None:
        config = Config(
            scheduler="slurm",
            scheduler_args={},
            appdef=defaults.component_fn("slurm")(),
        )
        with self.assertRaises(RuntimeError):
            _ = await commands.get_or_create(
                name="123",
                config=config,
                check_interval=_5_MS,
            )
