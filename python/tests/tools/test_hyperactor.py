# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import unittest

from monarch.tools.components.hyperactor import host_mesh


class TestHostMesh(unittest.TestCase):
    def test_host_mesh_default_no_metadata(self) -> None:
        """Test that host_mesh creates roles with empty metadata by default."""
        appdef = host_mesh(meshes=["trainer:2:gpu.small"])

        self.assertEqual(1, len(appdef.roles))
        role = appdef.roles[0]
        self.assertEqual("trainer", role.name)
        self.assertEqual({}, role.metadata)

    def test_host_mesh_with_metadata(self) -> None:
        """Test that host_mesh passes metadata through to each role."""
        metadata = {"kubernetes": {"spec": {"runtimeClassName": "nvidia"}}}
        appdef = host_mesh(meshes=["trainer:2:gpu.small"], metadata=metadata)

        self.assertEqual(1, len(appdef.roles))
        role = appdef.roles[0]
        self.assertEqual("trainer", role.name)
        self.assertEqual(metadata, role.metadata)

    def test_host_mesh_metadata_applied_to_all_roles(self) -> None:
        """Test that metadata is applied to all roles when multiple meshes are specified."""
        metadata = {"kubernetes": {"spec": {"runtimeClassName": "nvidia"}}}
        appdef = host_mesh(
            meshes=["trainer:2:gpu.small", "generator:4:gpu.medium"],
            metadata=metadata,
        )

        self.assertEqual(2, len(appdef.roles))
        for role in appdef.roles:
            self.assertEqual(metadata, role.metadata)

    def test_host_mesh_with_none_metadata(self) -> None:
        """Test that passing None for metadata results in empty metadata dict."""
        appdef = host_mesh(meshes=["trainer:2:gpu.small"], metadata=None)

        self.assertEqual(1, len(appdef.roles))
        role = appdef.roles[0]
        self.assertEqual({}, role.metadata)
