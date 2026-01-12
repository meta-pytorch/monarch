# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import json
import unittest
from dataclasses import asdict

from monarch.tools.mesh_spec import (
    mesh_spec_from_metadata,
    mesh_spec_from_str,
    MeshSpec,
    ServerSpec,
    tag_as_metadata,
)
from monarch.tools.network import get_sockaddr
from torchx import specs

UNUSED = "__UNUSED_FOR_TEST__"


class TestMeshSpec(unittest.TestCase):
    def test_tag_as_metadata(self) -> None:
        mesh_spec = MeshSpec(
            name="trainer", num_hosts=2, host_type="gpu.medium", gpus=2
        )
        appdef = specs.AppDef(name=UNUSED)
        tag_as_metadata(mesh_spec, appdef)

        self.assertDictEqual(
            {
                "monarch/meshes/trainer/host_type": "gpu.medium",
                "monarch/meshes/trainer/gpus": "2",
                "monarch/meshes/trainer/transport": "tcp",
            },
            appdef.metadata,
        )

    def test_mesh_spec_from_str_incomplete_spec(self) -> None:
        for spec_str in [
            "2:gpu.medium",  # missing_mesh_name
            "trainer:gpu.medium",  # missing_num_hosts
            "trainer:2",  # missing_host_type
        ]:
            with self.assertRaisesRegex(
                AssertionError, r"not of the form 'NAME:NUM_HOSTS:HOST_TYPE'"
            ):
                mesh_spec_from_str(spec_str)

    def test_mesh_spec_from_str_num_hosts_not_an_integer(self) -> None:
        with self.assertRaisesRegex(AssertionError, "is not a number"):
            mesh_spec_from_str("trainer:four:gpu.medium")

    def test_mesh_spec_from_str(self) -> None:
        mesh_spec_str = "trainer:4:gpu.small"
        mesh_spec = mesh_spec_from_str(mesh_spec_str)

        self.assertEqual("trainer", mesh_spec.name)
        self.assertEqual(4, mesh_spec.num_hosts)
        self.assertEqual("gpu.small", mesh_spec.host_type)
        self.assertEqual(1, mesh_spec.gpus)

    def test_mesh_spec_from_metadata(self) -> None:
        appdef = specs.AppDef(
            name=UNUSED,
            roles=[specs.Role(name="trainer", image=UNUSED, num_replicas=4)],
            metadata={
                "monarch/meshes/trainer/host_type": "gpu.medium",
                "monarch/meshes/trainer/gpus": "2",
                "monarch/meshes/trainer/transport": "metatls",
            },
        )
        trainer_mesh_spec = mesh_spec_from_metadata(appdef, "trainer")
        self.assertIsNotNone(trainer_mesh_spec)
        self.assertEqual(4, trainer_mesh_spec.num_hosts)
        self.assertEqual("gpu.medium", trainer_mesh_spec.host_type)
        self.assertEqual(2, trainer_mesh_spec.gpus)
        self.assertEqual("metatls", trainer_mesh_spec.transport)

        # no generator role in appdef
        self.assertIsNone(mesh_spec_from_metadata(appdef, "generator"))

    def test_mesh_spec_can_dump_as_json(self) -> None:
        mesh_spec = MeshSpec(
            name="trainer",
            num_hosts=4,
            host_type="gpu.medium",
            gpus=2,
            hostnames=["n0", "n1", "n2", "n3"],
            image="test_pkg:123",
        )
        expected = """
{
  "name": "trainer",
  "num_hosts": 4,
  "host_type": "gpu.medium",
  "gpus": 2,
  "transport": "tcp",
  "port": 26600,
  "hostnames": [
    "n0",
    "n1",
    "n2",
    "n3"
  ],
  "state": 0,
  "image": "test_pkg:123"
}
"""
        self.assertEqual(expected.strip("\n"), json.dumps(asdict(mesh_spec), indent=2))

    def test_mesh_spec_server_addrs_empty_hostnames(self) -> None:
        for transport in ["tcp", "metatls"]:
            with self.subTest(transport=transport):
                mesh_spec = MeshSpec(name="x", num_hosts=2, transport=transport)
                self.assertListEqual([], mesh_spec.server_addrs())

    def test_mesh_spec_server_addrs_unsupported_transport(self) -> None:
        for transport in ["unix", "local"]:
            with self.subTest(transport=transport):
                mesh_spec = MeshSpec(
                    name="x",
                    num_hosts=2,
                    transport=transport,
                    hostnames=["node0", "node1"],
                )
                with self.assertRaises(ValueError):
                    mesh_spec.server_addrs()

    def test_mesh_spec_server_addrs_tcp(self) -> None:
        mesh_spec = MeshSpec(
            name="x",
            num_hosts=1,
            port=29000,
            hostnames=["localhost"],
        )

        self.assertListEqual(
            [f"tcp!{get_sockaddr('localhost', 29000)}"],
            mesh_spec.server_addrs(),
        )

    def test_mesh_spec_server_addrs_transport_port_override(self) -> None:
        mesh_spec = MeshSpec(
            name="x",
            num_hosts=1,
            port=29000,
            transport="tcp",
            hostnames=["devgpu001.abc.facebook.com"],
        )
        self.assertListEqual(
            ["metatls!devgpu001.abc.facebook.com:29001"],
            mesh_spec.server_addrs(transport="metatls", port=29001),
        )

    def test_mesh_spec_server_addrs_metatls(self) -> None:
        mesh_spec = MeshSpec(
            name="x",
            num_hosts=1,
            transport="metatls",
            port=29000,
            hostnames=["devgpu001.abc.facebook.com"],
        )
        self.assertListEqual(
            ["metatls!devgpu001.abc.facebook.com:29000"],
            mesh_spec.server_addrs(),
        )


class ServerSpecTest(unittest.TestCase):
    def get_test_server_spec(self) -> ServerSpec:
        return ServerSpec(
            name="monarch-foo-1a2b3c",
            scheduler="slurm",
            state=specs.AppState.RUNNING,
            meshes=[
                MeshSpec(name="trainer", num_hosts=4, host_type="gpu.medium", gpus=2),
                MeshSpec(name="generator", num_hosts=8, host_type="gpu.small", gpus=1),
            ],
            metadata={"job_version": "1", "owner_unixname": "johndoe"},
        )

    def test_server_handle(self) -> None:
        unused = specs.AppState.RUNNING

        server = ServerSpec(name="foo", scheduler="slurm", meshes=[], state=unused)
        self.assertEqual("slurm:///foo", server.server_handle)

        server = ServerSpec(
            name="foo", scheduler="slurm", namespace="prod", meshes=[], state=unused
        )
        self.assertEqual("slurm://prod/foo", server.server_handle)

    def test_get_mesh_spec(self) -> None:
        server_spec = self.get_test_server_spec()
        mesh_spec = server_spec.get_mesh_spec("trainer")

        self.assertEqual("trainer", mesh_spec.name)
        self.assertEqual(4, mesh_spec.num_hosts)
        self.assertEqual(2, mesh_spec.gpus)
        self.assertEqual("gpu.medium", mesh_spec.host_type)

    def test_get_mesh_spec_not_found(self) -> None:
        server_spec = self.get_test_server_spec()
        with self.assertRaisesRegex(
            ValueError,
            r"Mesh: 'worker' not found in job: monarch-foo-1a2b3c. Try one of: \['trainer', 'generator'\]",
        ):
            server_spec.get_mesh_spec("worker")

    def test_get_metadata(self) -> None:
        server_spec = self.get_test_server_spec()
        self.assertDictEqual(
            {"job_version": "1", "owner_unixname": "johndoe"}, server_spec.metadata
        )

    def _1_mesh_2_host_server_spec(self, state: specs.AppState) -> ServerSpec:
        return ServerSpec(
            name="foo",
            scheduler="slurm",
            meshes=[
                MeshSpec(
                    name="trainer",
                    num_hosts=2,
                    hostnames=["compute-node-0", "compute-node-1"],
                )
            ],
            state=state,
        )

    def test_node0(self) -> None:
        server = self._1_mesh_2_host_server_spec(specs.AppState.RUNNING)
        self.assertEqual("compute-node-0", server.host0("trainer"))

    def test_node0_server_in_terminal_state(self) -> None:
        for terminal_state in [
            specs.AppState.FAILED,
            specs.AppState.SUCCEEDED,
            specs.AppState.CANCELLED,
        ]:
            with self.subTest(terminal_state=terminal_state):
                server = self._1_mesh_2_host_server_spec(terminal_state)
                with self.assertRaisesRegex(
                    RuntimeError,
                    r"Use `monarch.tools.commands.get_or_create\(\)` to create a new server",
                ):
                    server.host0("trainer")

    def test_node0_server_in_pending_state(self) -> None:
        for pending_state in [specs.AppState.SUBMITTED, specs.AppState.PENDING]:
            with self.subTest(pending_state=pending_state):
                server = self._1_mesh_2_host_server_spec(pending_state)
                with self.assertRaisesRegex(
                    RuntimeError,
                    r"Use `monarch.tools.commands.server_ready\(\)` to wait for the server to be RUNNING",
                ):
                    server.host0("trainer")

    def test_node0_server_in_illegal_tate(self) -> None:
        for illegal_state in [specs.AppState.UNSUBMITTED, specs.AppState.UNKNOWN]:
            with self.subTest(illegal_state=illegal_state):
                server = self._1_mesh_2_host_server_spec(illegal_state)
                with self.assertRaisesRegex(
                    RuntimeError,
                    r"Please report this as a bug",
                ):
                    server.host0("trainer")
