# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import json
import os
import unittest
from unittest import mock

from monarch.tools.cli import config_from_cli_args, get_parser, main
from monarch.tools.config import Config, Workspace
from monarch.tools.mesh_spec import MeshSpec, ServerSpec
from tests.tools.utils import capture_stdout
from torchx.specs import AppState

_CURRENT_WORKING_DIR: str = os.getcwd()


class TestCli(unittest.TestCase):
    def test_help(self) -> None:
        with self.assertRaises(SystemExit) as cm:
            main(["--help"])
            self.assertEqual(cm.exception.code, 0)

    @mock.patch(
        # prevent images from actually being pulled during tests
        "torchx.schedulers.local_scheduler.ImageProvider.fetch",
        return_value=_CURRENT_WORKING_DIR,
    )
    def test_create_dryrun_default(self, _) -> None:
        # use local_cwd as a representative scheduler to run the test with
        main(
            [
                "create",
                "-s=local_cwd",
                "--dryrun",
                "-arg=image=_DUMMY_IMAGE:0",
            ]
        )

    @mock.patch("monarch.tools.cli.info")
    def test_info(self, mock_cmd_info: mock.MagicMock) -> None:
        job_name = "imaginary-test-job"
        mock_cmd_info.return_value = ServerSpec(
            name=job_name,
            scheduler="slurm",
            state=AppState.RUNNING,
            meshes=[
                MeshSpec(name="trainer", num_hosts=4, host_type="gpu.medium", gpus=2),
                MeshSpec(name="generator", num_hosts=16, host_type="gpu.small", gpus=1),
            ],
        )
        with capture_stdout() as buf:
            main(["info", f"slurm:///{job_name}"])
            out = buf.getvalue()
            # CLI does not pretty-print json so that the output can be piped for
            # further processing. Read the captured stdout and pretty-format
            # json so that the expected value reads better
            expected = """
{
  "name": "imaginary-test-job",
  "server_handle": "slurm:///imaginary-test-job",
  "state": "RUNNING",
  "meshes": {
    "trainer": {
      "host_type": "gpu.medium",
      "hosts": 4,
      "gpus": 2,
      "hostnames": []
    },
    "generator": {
      "host_type": "gpu.small",
      "hosts": 16,
      "gpus": 1,
      "hostnames": []
    }
  }
}
"""
            self.assertEqual(
                expected.strip("\n"),
                json.dumps(json.loads(out), indent=2),
            )

    @mock.patch("monarch.tools.cli.kill")
    def test_kill(self, mock_cmd_kill: mock.MagicMock) -> None:
        handle = "slurm:///test-job-id"
        main(["kill", handle])
        mock_cmd_kill.assert_called_once_with(handle)

    def test_config_from_cli_args(self) -> None:
        parser = get_parser()
        args = parser.parse_args(
            [
                "create",
                "--scheduler=slurm",
                # supports both 'comma'-delimited and repeated '-cfg'
                "-cfg=partition=test",
                "-cfg=mail-user=foo@bar.com,mail-type=FAIL",
                "--dryrun",
                "--workspace=/mnt/users/foo",
            ]
        )

        config = config_from_cli_args(args)
        self.assertEqual(
            Config(
                scheduler="slurm",
                scheduler_args={
                    "partition": "test",
                    "mail-user": "foo@bar.com",
                    "mail-type": "FAIL",
                },
                dryrun=True,
                workspace=Workspace(dirs={"/mnt/users/foo": ""}),
            ),
            config,
        )

    def test_bounce(self) -> None:
        with self.assertRaises(NotImplementedError):
            main(["bounce", "slurm:///test-job-id"])

    def test_stop(self) -> None:
        with self.assertRaises(NotImplementedError):
            main(["stop", "slurm:///test-job-id"])
