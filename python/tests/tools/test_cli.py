# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import datetime
import io
import json
import tempfile
import unittest
import urllib.error
from email.message import Message
from pathlib import Path
from unittest.mock import MagicMock, patch

from monarch._src.job.persisted_telemetry import format_instances, InstanceSummary
from monarch.tools.cli import _format_query_table, get_parser, main


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

    @patch("monarch.tools.cli.exec_on_job", return_value=0)
    def test_exec_python_executable(self, exec_on_job: MagicMock) -> None:
        args = get_parser().parse_args(
            [
                "exec",
                "--all",
                "--per-host",
                "gpu=4",
                "--python-executable",
                "/opt/venv/bin/python",
                "--workdir",
                "/workspace",
                "train.py",
                "--steps",
                "10",
            ]
        )
        args.func(args)
        exec_on_job.assert_called_once_with(
            ["train.py", "--steps", "10"],
            run_all=True,
            mesh_name=None,
            point_str=None,
            env=None,
            workdir="/workspace",
            kill=False,
            script=None,
            per_host={"gpu": 4},
            python_executable="/opt/venv/bin/python",
        )

    @patch("monarch.tools.cli.exec_on_job", return_value=0)
    def test_exec_python_executable_module(self, exec_on_job: MagicMock) -> None:
        args = get_parser().parse_args(
            [
                "exec",
                "--python-executable",
                "/opt/venv/bin/python",
                "-m",
                "trainer",
                "--",
                "--help",
            ]
        )
        args.func(args)
        self.assertEqual(exec_on_job.call_args.args[0], ["-m", "trainer", "--help"])
        self.assertEqual(
            exec_on_job.call_args.kwargs["python_executable"], "/opt/venv/bin/python"
        )

    def test_exec_python_executable_default(self) -> None:
        args = get_parser().parse_args(["exec", "train.py"])
        self.assertIsNone(args.python_executable)

    def test_exec_python_executable_requires_value(self) -> None:
        with self.assertRaises(SystemExit):
            get_parser().parse_args(["exec", "--python-executable"])

    def test_exec_all_and_mesh_mutually_exclusive(self) -> None:
        parser = get_parser()
        with self.assertRaises(SystemExit):
            parser.parse_args(["exec", "--all", "--mesh", "workers", "echo"])

    def test_query_parser(self) -> None:
        parser = get_parser()
        args = parser.parse_args(
            [
                "query",
                "--format",
                "json",
                "--timeout",
                "2m",
                "SELECT * FROM actors",
            ]
        )
        self.assertEqual(args.sql, "SELECT * FROM actors")
        self.assertEqual(args.output_format, "json")
        self.assertEqual(args.timeout, 120)

    def test_query_dir_parser(self) -> None:
        args = get_parser().parse_args(
            ["query", "--dir", "/p", "--dir", "/q", "SELECT 1"]
        )
        self.assertEqual(args.dir, ["/p", "/q"])
        self.assertEqual(args.sql, "SELECT 1")

    @patch("monarch.tools.cli.job_load", side_effect=AssertionError("job_load"))
    @patch("monarch.tools.cli.open_persisted_engine")
    def test_query_dir_skips_job_load(
        self, open_persisted_engine: MagicMock, _job_load: MagicMock
    ) -> None:
        engine = MagicMock()
        engine.query.return_value.to_pylist.return_value = [{"x": 1}]
        instance = {
            "name": "h1-12",
            "segments": 1,
            "clean": True,
            "torn_bytes": 0,
            "rows": 1,
            "first_timestamp_us": 0,
            "last_timestamp_us": 0,
        }
        open_persisted_engine.return_value = (engine, [instance])
        args = get_parser().parse_args(["query", "--dir", "/p", "SELECT x FROM t"])

        with (
            patch("sys.stdout", new_callable=io.StringIO) as output,
            patch("sys.stderr", new_callable=io.StringIO) as errors,
        ):
            args.func(args)

        open_persisted_engine.assert_called_once_with(["/p"])
        engine.query.assert_called_once_with("SELECT x FROM t")
        self.assertEqual(output.getvalue(), "x\n-\n1\n(1 row)\n")
        self.assertIn("h1-12 clean", errors.getvalue())

    @patch("monarch.tools.cli.open_persisted_engine")
    def test_query_dir_reports_sql_errors(
        self, open_persisted_engine: MagicMock
    ) -> None:
        engine = MagicMock()
        # The Rust query engine raises a plain `Exception` for bad SQL.
        engine.query.side_effect = Exception("table 'nope' not found")
        open_persisted_engine.return_value = (engine, [])
        args = get_parser().parse_args(["query", "--dir", "/p", "SELECT * FROM nope"])

        with self.assertRaises(SystemExit) as raised:
            args.func(args)

        self.assertEqual(str(raised.exception), "monarch query: table 'nope' not found")

    @patch("monarch.tools.cli.open_persisted_engine")
    def test_query_dir_json_renders_arrow_values(
        self, open_persisted_engine: MagicMock
    ) -> None:
        engine = MagicMock()
        moment = datetime.datetime(2026, 10, 26, 12, tzinfo=datetime.timezone.utc)
        engine.query.return_value.to_pylist.return_value = [{"at": moment}]
        open_persisted_engine.return_value = (engine, [])
        args = get_parser().parse_args(
            ["query", "--dir", "/p", "--format", "jsonl", "SELECT at FROM t"]
        )

        with (
            patch("sys.stdout", new_callable=io.StringIO) as output,
            patch("sys.stderr", new_callable=io.StringIO),
        ):
            args.func(args)

        self.assertEqual(json.loads(output.getvalue()), {"at": str(moment)})

    def test_format_instances_marks_earliest_unclean_stop(self) -> None:
        def instance(name: str, clean: bool, last_us: int) -> InstanceSummary:
            return {
                "name": name,
                "segments": 1,
                "clean": clean,
                "torn_bytes": 0 if clean else 7,
                "rows": 4,
                "first_timestamp_us": 0,
                "last_timestamp_us": last_us,
            }

        noon_us = 1_793_016_000_000_000
        lines = format_instances(
            [
                instance("h1-12", True, noon_us - 60_000_000),
                instance("h2-13", False, noon_us),
                instance("h3-14", False, noon_us + 1_000_000),
            ]
        ).splitlines()

        self.assertEqual(
            lines,
            [
                "h1-12 clean segments=1 rows=4 last=2026-10-26T11:59:00Z torn=0B",
                "h2-13 unclean segments=1 rows=4 last=2026-10-26T12:00:00Z torn=7B first to stop",
                "h3-14 unclean segments=1 rows=4 last=2026-10-26T12:00:01Z torn=7B",
            ],
        )

    def test_dashboard_open_parser(self) -> None:
        args = get_parser().parse_args(["dashboard", "open", "/p", "/q", "--port", "0"])
        self.assertEqual(args.dirs, ["/p", "/q"])
        self.assertEqual(args.port, 0)
        self.assertEqual(args.dashboard_func.__name__, "_run_open")

    @patch("monarch.tools.cli.job_load")
    def test_query_table_output(self, job_load: MagicMock) -> None:
        client = job_load.return_value.telemetry_query_client.return_value
        client.query.return_value = {
            "rows": [
                {"actor": "trainer", "failed": 0},
                {"actor": "worker", "failed": None},
            ]
        }
        parser = get_parser()
        args = parser.parse_args(["query", "SELECT * FROM actors"])

        with patch("sys.stdout", new_callable=io.StringIO) as output:
            args.func(args)

        client.query.assert_called_once_with("SELECT * FROM actors")
        self.assertEqual(
            output.getvalue(),
            "actor   | failed\n"
            "--------+-------\n"
            "trainer | 0     \n"
            "worker  | NULL  \n"
            "(2 rows)\n",
        )

    def test_query_table_output_with_no_columns(self) -> None:
        self.assertEqual(_format_query_table({"rows": [{}]}), "(1 row)")

    @patch("monarch.tools.cli.job_load")
    def test_query_json_and_jsonl_output(self, job_load: MagicMock) -> None:
        result = {"rows": [{"failed": 0}, {"failed": 1}]}
        client = job_load.return_value.telemetry_query_client.return_value
        client.query.return_value = result
        parser = get_parser()

        with patch("sys.stdout", new_callable=io.StringIO) as output:
            args = parser.parse_args(["query", "--format", "json", "SELECT 1"])
            args.func(args)
        self.assertEqual(json.loads(output.getvalue()), result)

        with patch("sys.stdout", new_callable=io.StringIO) as output:
            args = parser.parse_args(["query", "--format", "jsonl", "SELECT failed"])
            args.func(args)
        self.assertEqual(output.getvalue(), '{"failed": 0}\n{"failed": 1}\n')

        client.query.return_value = {"rows": []}
        with patch("sys.stdout", new_callable=io.StringIO) as output:
            args.func(args)
        self.assertEqual(output.getvalue(), "")

    @patch("monarch.tools.cli.job_load")
    def test_query_reads_file_and_stdin(self, job_load: MagicMock) -> None:
        client = job_load.return_value.telemetry_query_client.return_value
        client.query.return_value = {"rows": []}
        parser = get_parser()

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "query.sql"
            path.write_text("SELECT * FROM spans", encoding="utf-8")
            args = parser.parse_args(["query", "--file", str(path)])
            with patch("sys.stdout", new_callable=io.StringIO):
                args.func(args)
        client.query.assert_called_with("SELECT * FROM spans")

        with (
            patch("sys.stdin", io.StringIO("SELECT * FROM messages")),
            patch("sys.stdout", new_callable=io.StringIO),
        ):
            args = parser.parse_args(["query", "-"])
            args.func(args)
        client.query.assert_called_with("SELECT * FROM messages")

    @patch("monarch.tools.cli.job_load")
    def test_query_accepts_large_file_and_custom_timeout(
        self, job_load: MagicMock
    ) -> None:
        client = job_load.return_value.telemetry_query_client.return_value
        client.query.return_value = {"rows": []}
        sql = "/*" + "x" * (128 * 1024) + "*/ SELECT 1"

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "large.sql"
            path.write_text(sql, encoding="utf-8")
            args = get_parser().parse_args(
                ["query", "--file", str(path), "--timeout", "30s"]
            )
            with patch("sys.stdout", new_callable=io.StringIO):
                args.func(args)

        client.query.assert_called_once_with(sql, timeout=30)

    @patch("monarch.tools.cli.job_load")
    def test_query_does_not_connect_full_job_state(self, job_load: MagicMock) -> None:
        client = job_load.return_value.telemetry_query_client.return_value
        client.query.return_value = {"rows": [{"count": 1}]}
        args = get_parser().parse_args(
            ["query", "--format", "json", "SELECT 1 AS count"]
        )
        with patch("sys.stdout", new_callable=io.StringIO) as output:
            args.func(args)

        self.assertEqual(json.loads(output.getvalue()), {"rows": [{"count": 1}]})
        job_load.return_value.state.assert_not_called()

    @patch("monarch.tools.cli.job_load", side_effect=FileNotFoundError)
    def test_query_reports_no_active_job(self, _job_load: MagicMock) -> None:
        args = get_parser().parse_args(["query", "SELECT 1"])
        with self.assertRaisesRegex(SystemExit, "no active job was found"):
            args.func(args)

    @patch("monarch.tools.cli.job_load")
    def test_query_reports_unhealthy_job(self, job_load: MagicMock) -> None:
        job_load.return_value.telemetry_query_client.side_effect = RuntimeError(
            "the active job is no longer running or healthy"
        )
        args = get_parser().parse_args(["query", "SELECT 1"])
        with self.assertRaisesRegex(SystemExit, "no longer running or healthy"):
            args.func(args)

    @patch("monarch.tools.cli.job_load")
    def test_query_reports_telemetry_not_configured(self, job_load: MagicMock) -> None:
        job_load.return_value.telemetry_query_client.side_effect = RuntimeError(
            "the active job has no distributed telemetry configured"
        )
        args = get_parser().parse_args(["query", "SELECT 1"])
        with self.assertRaisesRegex(SystemExit, "no distributed telemetry configured"):
            args.func(args)

    @patch("monarch.tools.cli.job_load")
    def test_query_failures_are_clean_cli_errors(self, job_load: MagicMock) -> None:
        client = job_load.return_value.telemetry_query_client.return_value
        args = get_parser().parse_args(["query", "--format", "json", "SELECT 1"])
        errors = [
            urllib.error.HTTPError(
                "http://telemetry", 400, "invalid SQL", Message(), None
            ),
            urllib.error.URLError("connection refused"),
            TimeoutError("timed out"),
            json.JSONDecodeError("invalid JSON", "x", 0),
            RuntimeError("invalid telemetry query response"),
        ]
        for error in errors:
            with self.subTest(error=error):
                client.query.side_effect = error
                with (
                    patch("sys.stdout", new_callable=io.StringIO) as output,
                    self.assertRaisesRegex(SystemExit, "monarch query:") as raised,
                ):
                    args.func(args)
                self.assertEqual(output.getvalue(), "")
                if isinstance(error, TimeoutError):
                    self.assertIn("increasing --timeout", str(raised.exception))
                else:
                    self.assertIn(str(error), str(raised.exception))

    @patch("monarch.tools.cli.shell_on_job", return_value=0)
    def test_shell_forwards_single_host_options(self, shell_on_job: MagicMock) -> None:
        parser = get_parser()
        args = parser.parse_args(
            [
                "shell",
                "--mesh",
                "workers",
                "--point",
                "host=2",
                "-e",
                "FOO=bar",
                "--workdir",
                "/tmp/work",
            ]
        )

        args.func(args)

        shell_on_job.assert_called_once_with(
            mesh_name="workers",
            point_str="host=2",
            env=["FOO=bar"],
            workdir="/tmp/work",
            kill=False,
        )

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
