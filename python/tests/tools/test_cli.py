# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import unittest

from monarch.tools.cli import get_parser, main


class TestCli(unittest.TestCase):
    def test_help(self) -> None:
        with self.assertRaises(SystemExit) as cm:
            main(["--help"])
            self.assertEqual(cm.exception.code, 0)

    def test_apply_module_path(self) -> None:
        parser = get_parser()
        args = parser.parse_args(["apply", "jobs.mast"])
        self.assertEqual(args.module_path, "jobs.mast")

    def test_context_use_command(self) -> None:
        parser = get_parser()
        args = parser.parse_args(["context", "use", "myjob"])
        self.assertEqual(args.name, "myjob")

    def test_dashboard_mast_command(self) -> None:
        parser = get_parser()
        args = parser.parse_args(["dashboard", "mast", "sample-mast-job"])
        self.assertEqual(args.job, "sample-mast-job")
        self.assertIsNone(args.role_name)
        self.assertEqual(8265, args.dashboard_port)
        self.assertFalse(hasattr(args, "relay_port"))

    def test_dashboard_mast_accepts_role_name(self) -> None:
        parser = get_parser()
        args = parser.parse_args(
            [
                "dashboard",
                "mast",
                "sample-mast-job",
                "--role-name",
                "worker",
            ]
        )
        self.assertEqual("worker", args.role_name)

    def test_dashboard_mast_accepts_dashboard_port(self) -> None:
        parser = get_parser()
        args = parser.parse_args(
            [
                "dashboard",
                "mast",
                "sample-mast-job",
                "--dashboard-port",
                "9000",
            ]
        )
        self.assertEqual(9000, args.dashboard_port)

    def test_exec_run_all_default(self) -> None:
        parser = get_parser()
        args = parser.parse_args(["exec", "echo", "hi"])
        self.assertFalse(args.run_all)

    def test_exec_per_host_flag(self) -> None:
        parser = get_parser()
        args = parser.parse_args(["exec", "--per-host", "gpu=4", "nvidia-smi"])
        self.assertEqual(args.per_host, "gpu=4")
        self.assertFalse(args.run_all)

    def test_exec_all_and_mesh_mutually_exclusive(self) -> None:
        parser = get_parser()
        with self.assertRaises(SystemExit):
            parser.parse_args(["exec", "--all", "--mesh", "workers", "echo"])

    def test_help_has_job_reuse(self) -> None:
        import importlib.resources

        skill = importlib.resources.files("monarch.tools").joinpath("SKILL.md")
        content = skill.read_text(encoding="utf-8")
        self.assertIn("Job reuse:", content)

    def test_help_has_per_host(self) -> None:
        import importlib.resources

        skill = importlib.resources.files("monarch.tools").joinpath("SKILL.md")
        content = skill.read_text(encoding="utf-8")
        self.assertIn("--per-host", content)
