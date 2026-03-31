# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import argparse
import importlib.resources
import json
import sys
from pathlib import Path

from monarch.tools.commands import (
    bounce,
    component_args_from_cli,
    create,
    CURRENT_FILE,
    exec_on_job,
    info,
    kill,
    serve_module,
    stop,
    torchx_runner,
)
from monarch.tools.config import (  # @manual=//monarch/python/monarch/tools/config/meta:defaults
    Config,
    defaults,
)
from torchx.specs.finder import get_component


def config_from_cli_args(args: argparse.Namespace) -> Config:
    config = defaults.config(args.scheduler, args.workspace)

    if args.scheduler_args:
        with torchx_runner() as runner:
            opts = runner.scheduler_run_opts(config.scheduler)
            for cfg_str in args.scheduler_args:
                parsed_cfg = opts.cfg_from_str(cfg_str)
                config.scheduler_args.update(parsed_cfg)

    config.dryrun = args.dryrun
    return config


class CreateCmd:
    def add_arguments(self, subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument(
            "-s",
            "--scheduler",
            type=str,
            help="Scheduler to submit to",
        )
        subparser.add_argument(
            "-cfg",
            "--scheduler_args",
            default=[],
            action="append",
            help="Scheduler args (e.g. `-cfg cluster=foo -cfg user=bar`)",
        )
        subparser.add_argument(
            "--dryrun",
            action="store_true",
            default=False,
            help="Just prints the scheduler request",
        )
        subparser.add_argument(
            "--workspace",
            help="The local directory to build into the job's image and make available on the job."
            " Pass --workspace='' to disable any default workspaces configured for the scheduler",
        )
        subparser.add_argument(
            "--component",
            help="A custom TorchX component to use",
        )
        subparser.add_argument(
            "-arg",
            "--component_args",
            default=[],
            action="append",
            help="Arguments to the component fn (e.g. `-arg a=b -arg c=d` to pass as `component_fn(a=b, c=d)`)",
        )

    def run(self, args: argparse.Namespace) -> None:
        config = config_from_cli_args(args)

        component_fn = (
            get_component(args.component).fn
            if args.component
            else defaults.component_fn(config.scheduler)
        )
        component_args = component_args_from_cli(component_fn, args.component_args)
        config.appdef = component_fn(**component_args)

        handle = create(config)
        print(handle)


class CommonArguments:
    @staticmethod
    def add_server_handle(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument(
            "server_handle",
            type=str,
            help="monarch server handle (e.g. slurm:///job_id)",
        )


class InfoCmd:
    def add_arguments(self, subparser: argparse.ArgumentParser) -> None:
        CommonArguments.add_server_handle(subparser)

    def run(self, args: argparse.Namespace) -> None:
        server_spec = info(args.server_handle)
        if server_spec is None:
            print(
                f"Server: {args.server_handle} does not exist",
                file=sys.stderr,
            )
        else:
            json.dump(server_spec.to_json(), indent=2, fp=sys.stdout)


class OldKillCmd:
    def add_arguments(self, subparser: argparse.ArgumentParser) -> None:
        CommonArguments.add_server_handle(subparser)

    def run(self, args: argparse.Namespace) -> None:
        kill(args.server_handle)


class BounceCmd:
    def add_arguments(self, subparser: argparse.ArgumentParser) -> None:
        CommonArguments.add_server_handle(subparser)

    def run(self, args: argparse.Namespace) -> None:
        bounce(args.server_handle)


class StopCmd:
    def add_arguments(self, subparser: argparse.ArgumentParser) -> None:
        CommonArguments.add_server_handle(subparser)

    def run(self, args: argparse.Namespace) -> None:
        stop(args.server_handle)


# ── New commands ──────────────────────────────────────────────────────────


class ServeCmd:
    def add_arguments(self, subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument(
            "module_path",
            type=str,
            help="Dotted Python module path with a serve() function (e.g. jobs.mast)",
        )
        subparser.add_argument(
            "--name",
            type=str,
            default=None,
            help="Name for this job (default: derived from module path)",
        )

    def run(self, args: argparse.Namespace) -> None:
        serve_module(args.module_path, name=args.name, job_path=args.job)


class ExecCmd:
    def add_arguments(self, subparser: argparse.ArgumentParser) -> None:
        # ── Rank targeting (mutually exclusive) ──
        target = subparser.add_mutually_exclusive_group()
        target.add_argument(
            "--ranks",
            type=str,
            default=None,
            help="Comma-separated list of flat ranks to run on (default: 0). "
            "Output is streamed with [rank N] prefix when multiple ranks.",
        )
        target.add_argument(
            "-a",
            "--all",
            action="store_true",
            default=False,
            dest="run_all",
            help="Run on all ranks, write per-rank logs to .monarch/logs/",
        )
        target.add_argument(
            "--per-host",
            action="store_true",
            default=False,
            help="Run once per host (rank 0 on each host). "
            "Output is streamed with [host N] prefix.",
        )
        target.add_argument(
            "--hosts",
            type=str,
            default=None,
            help="Comma-separated list of host indices to run on (one process "
            "per host). Output is streamed with [host N] prefix.",
        )

        subparser.add_argument(
            "-e",
            "--env",
            action="append",
            default=[],
            help="Extra environment variables as KEY=VALUE (can be repeated)",
        )
        subparser.add_argument(
            "-v",
            "--verbose",
            action="store_true",
            default=False,
            help="Enable verbose logging for remotemount transfers",
        )
        subparser.add_argument(
            "--source-dir",
            type=str,
            default=None,
            help="Directory to mount on workers (default: current directory)",
        )
        subparser.add_argument(
            "--mount-point",
            type=str,
            default=None,
            help="Mount point on workers (default: same as --source-dir)",
        )
        subparser.add_argument(
            "--kill",
            action="store_true",
            default=False,
            help="Kill the job after the command finishes",
        )
        subparser.add_argument(
            "--script",
            type=str,
            default=None,
            help="Read a bash script from FILE instead of cmd args (use '-' for stdin)",
        )
        subparser.add_argument(
            "--refresh-mount",
            action="store_true",
            default=False,
            help="Force-unmount stale FUSE mounts before remounting (recovers from busy mounts)",
        )
        subparser.add_argument(
            "cmd",
            nargs=argparse.REMAINDER,
            help="Command to run on workers",
        )

    def run(self, args: argparse.Namespace) -> None:
        cmd = args.cmd
        if cmd and cmd[0] == "--":
            cmd = cmd[1:]
        if not cmd and args.script is None:
            print(
                "Error: no command specified (use cmd args or --script)",
                file=sys.stderr,
            )
            sys.exit(1)

        # Parse --ranks / --hosts into a list of ints.
        target_ranks = None
        if args.ranks is not None:
            target_ranks = [int(r.strip()) for r in args.ranks.split(",")]

        target_hosts = None
        if args.hosts is not None:
            target_hosts = [int(h.strip()) for h in args.hosts.split(",")]

        rc = exec_on_job(
            cmd,
            run_all=args.run_all,  # --all
            per_host=args.per_host,  # --per-host
            ranks=target_ranks,  # --ranks
            hosts=target_hosts,  # --hosts
            env=args.env or None,
            verbose=args.verbose,
            job_path=args.job,
            source_dir=args.source_dir,
            mount_point=args.mount_point,
            kill=args.kill,  # --kill
            script=args.script,
            refresh_mount=args.refresh_mount,
        )
        if rc != 0:
            sys.exit(rc)


class UseCmd:
    def add_arguments(self, subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument(
            "name",
            type=str,
            help="Job name to activate (see 'monarch serve --name')",
        )

    def run(self, args: argparse.Namespace) -> None:
        Path(CURRENT_FILE).parent.mkdir(parents=True, exist_ok=True)
        Path(CURRENT_FILE).write_text(args.name)
        print(f"Active job set to '{args.name}'")


class KillCmd:
    def add_arguments(self, subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument(
            "name",
            nargs="?",
            default=None,
            help="Job name to kill (default: active job)",
        )

    def run(self, args: argparse.Namespace) -> None:
        from monarch._src.job.job import job_load
        from monarch._src.tools.commands import (
            _active_job_name,
            _job_path,
            DEFAULT_JOB_PATH,
        )

        name = args.name or _active_job_name()
        if name is None:
            print("No active job. Specify a job name.", file=sys.stderr)
            sys.exit(1)
        path = _job_path(name)
        if not Path(path).exists():
            path = args.job or DEFAULT_JOB_PATH
        job = job_load(path)
        job.kill()
        print(f"Killed job '{name}'")


def _load_skill_md() -> str:
    """Load SKILL.md as the help text."""
    skill_file = importlib.resources.files("monarch.tools").joinpath("SKILL.md")
    return skill_file.read_text(encoding="utf-8")


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Monarch CLI — run code on remote GPU workers",
        epilog=_load_skill_md(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-j",
        "--job",
        type=str,
        default=None,
        help="Path to cached job pickle (default: .monarch/job_state.pkl)",
    )
    subparser = parser.add_subparsers(title="COMMANDS")

    for cmd_name, cmd in {
        "serve": ServeCmd(),
        "exec": ExecCmd(),
        "use": UseCmd(),
        "kill": KillCmd(),
    }.items():
        cmd_parser = subparser.add_parser(cmd_name)
        cmd.add_arguments(cmd_parser)
        cmd_parser.set_defaults(func=cmd.run)
    return parser


def main(argv: list[str] = sys.argv[1:]) -> None:
    parser = get_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
