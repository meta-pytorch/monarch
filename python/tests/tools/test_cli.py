# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import os
import unittest

from monarch.tools.cli import get_parser, main

_CURRENT_WORKING_DIR: str = os.getcwd()


class TestCli(unittest.TestCase):
    def test_help(self) -> None:
        with self.assertRaises(SystemExit) as cm:
            main(["--help"])
            self.assertEqual(cm.exception.code, 0)

    # ── Original CLI tests (create, info, kill, bounce, stop) ──────────
    # These tests are for the old CLI subcommands that were replaced by
    # serve/exec/use/kill.  Commented out until the old commands are
    # either restored as aliases or the tests are migrated.

    # @mock.patch(
    #     # prevent images from actually being pulled during tests
    #     "torchx.schedulers.local_scheduler.ImageProvider.fetch",
    #     return_value=_CURRENT_WORKING_DIR,
    # )
    # def test_create_dryrun_default(self, _) -> None:  # type: ignore[no-untyped-def]
    #     # use local_cwd as a representative scheduler to run the test with
    #     main(
    #         [
    #             "create",
    #             "-s=local_cwd",
    #             "--dryrun",
    #             "-arg=image=_DUMMY_IMAGE:0",
    #         ]
    #     )

    # @mock.patch("monarch.tools.cli.info")
    # def test_info(self, mock_cmd_info: mock.MagicMock) -> None:
    #     job_name = "imaginary-test-job"
    #     mock_cmd_info.return_value = ServerSpec(
    #         name=job_name,
    #         scheduler="slurm",
    #         state=AppState.RUNNING,
    #         meshes=[
    #             MeshSpec(name="trainer", num_hosts=4, host_type="gpu.medium", gpus=2),
    #             MeshSpec(name="generator", num_hosts=16, host_type="gpu.small", gpus=1),
    #         ],
    #     )
    #     with capture_stdout() as buf:
    #         main(["info", f"slurm:///{job_name}"])
    #         out = buf.getvalue()
    #         expected = """
    # {
    #   "name": "imaginary-test-job",
    #   "server_handle": "slurm:///imaginary-test-job",
    #   "state": "RUNNING",
    #   "meshes": {
    #     "trainer": {
    #       "host_type": "gpu.medium",
    #       "hosts": 4,
    #       "gpus": 2,
    #       "hostnames": []
    #     },
    #     "generator": {
    #       "host_type": "gpu.small",
    #       "hosts": 16,
    #       "gpus": 1,
    #       "hostnames": []
    #     }
    #   }
    # }
    # """
    #         self.assertEqual(
    #             expected.strip("\n"),
    #             json.dumps(json.loads(out), indent=2),
    #         )

    # @mock.patch("monarch.tools.cli.kill")
    # def test_kill(self, mock_cmd_kill: mock.MagicMock) -> None:
    #     handle = "slurm:///test-job-id"
    #     main(["kill", handle])
    #     mock_cmd_kill.assert_called_once_with(handle)

    # def test_config_from_cli_args(self) -> None:
    #     parser = get_parser()
    #     args = parser.parse_args(
    #         [
    #             "create",
    #             "--scheduler=slurm",
    #             "-cfg=partition=test",
    #             "-cfg=mail-user=foo@bar.com,mail-type=FAIL",
    #             "--dryrun",
    #             "--workspace=/mnt/users/foo",
    #         ]
    #     )
    #
    #     config = config_from_cli_args(args)
    #     self.assertEqual(
    #         Config(
    #             scheduler="slurm",
    #             scheduler_args={
    #                 "partition": "test",
    #                 "mail-user": "foo@bar.com",
    #                 "mail-type": "FAIL",
    #             },
    #             dryrun=True,
    #             workspace=Workspace(dirs={"/mnt/users/foo": ""}),
    #         ),
    #         config,
    #     )

    # def test_bounce(self) -> None:
    #     with self.assertRaises(NotImplementedError):
    #         main(["bounce", "slurm:///test-job-id"])

    # def test_stop(self) -> None:
    #     with self.assertRaises(NotImplementedError):
    #         main(["stop", "slurm:///test-job-id"])

    # ── New CLI tests (serve, exec, use, rank targeting) ──────────────

    def test_exec_refresh_mount_flag(self) -> None:
        parser = get_parser()
        args = parser.parse_args(
            ["exec", "--refresh-mount", "--mount-point", "/tmp/m", "echo", "hi"]
        )
        self.assertTrue(args.refresh_mount)

    def test_exec_refresh_mount_default(self) -> None:
        parser = get_parser()
        args = parser.parse_args(["exec", "echo", "hi"])
        self.assertFalse(args.refresh_mount)

    def test_serve_name_flag(self) -> None:
        parser = get_parser()
        args = parser.parse_args(["serve", "--name", "mytest", "jobs.mast"])
        self.assertEqual(args.name, "mytest")
        self.assertEqual(args.module_path, "jobs.mast")

    def test_serve_name_default(self) -> None:
        parser = get_parser()
        args = parser.parse_args(["serve", "jobs.mast"])
        self.assertIsNone(args.name)

    def test_use_command(self) -> None:
        parser = get_parser()
        args = parser.parse_args(["use", "myjob"])
        self.assertEqual(args.name, "myjob")

    def test_exec_ranks_flag(self) -> None:
        parser = get_parser()
        args = parser.parse_args(["exec", "--ranks", "0,3,5", "echo", "hi"])
        self.assertEqual(args.ranks, "0,3,5")
        self.assertFalse(args.run_all)
        self.assertFalse(args.per_host)
        self.assertIsNone(args.hosts)

    def test_exec_ranks_default(self) -> None:
        parser = get_parser()
        args = parser.parse_args(["exec", "echo", "hi"])
        self.assertIsNone(args.ranks)
        self.assertFalse(args.run_all)

    def test_exec_per_host_flag(self) -> None:
        parser = get_parser()
        args = parser.parse_args(["exec", "--per-host", "nvidia-smi"])
        self.assertTrue(args.per_host)
        self.assertFalse(args.run_all)
        self.assertIsNone(args.ranks)

    def test_exec_hosts_flag(self) -> None:
        parser = get_parser()
        args = parser.parse_args(["exec", "--hosts", "0,2", "hostname"])
        self.assertEqual(args.hosts, "0,2")
        self.assertFalse(args.run_all)
        self.assertIsNone(args.ranks)

    def test_exec_all_and_ranks_mutually_exclusive(self) -> None:
        parser = get_parser()
        with self.assertRaises(SystemExit):
            parser.parse_args(["exec", "--all", "--ranks", "0", "echo"])

    def test_exec_all_and_per_host_mutually_exclusive(self) -> None:
        parser = get_parser()
        with self.assertRaises(SystemExit):
            parser.parse_args(["exec", "--all", "--per-host", "echo"])

    def test_exec_all_and_hosts_mutually_exclusive(self) -> None:
        parser = get_parser()
        with self.assertRaises(SystemExit):
            parser.parse_args(["exec", "--all", "--hosts", "0", "echo"])

    def test_exec_ranks_and_per_host_mutually_exclusive(self) -> None:
        parser = get_parser()
        with self.assertRaises(SystemExit):
            parser.parse_args(["exec", "--ranks", "0", "--per-host", "echo"])

    def test_help_has_job_reuse(self) -> None:
        import importlib.resources

        skill = importlib.resources.files("monarch.tools").joinpath("SKILL.md")
        content = skill.read_text(encoding="utf-8")
        self.assertIn("Job Reuse", content)
        self.assertIn("--refresh-mount", content)

    def test_help_has_rank_targeting(self) -> None:
        import importlib.resources

        skill = importlib.resources.files("monarch.tools").joinpath("SKILL.md")
        content = skill.read_text(encoding="utf-8")
        self.assertIn("--ranks", content)
        self.assertIn("--per-host", content)
        self.assertIn("--hosts", content)
        self.assertIn("Non-targeted ranks do NOT execute", content)
