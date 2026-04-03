# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import argparse
import importlib.resources
import json
import os
import sys

from monarch.tools.commands import (
    apply_job,
    bounce,
    component_args_from_cli,
    context_create,
    context_ls,
    context_rm,
    context_use,
    create,
    debug,
    exec_on_job,
    info,
    kill,
    stop,
    torchx_runner,  # noqa: F401
)
from monarch.tools.config import (  # @manual=//monarch/python/monarch/tools/config/meta:defaults
    Config,
    defaults,
)
from monarch.tools.debug_env import _get_debug_server_host, _get_debug_server_port
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


class DebugCmd:
    def add_arguments(self, subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument(
            "--host",
            type=str,
            default=_get_debug_server_host(),
            help="Hostname where the debug server is running",
        )
        subparser.add_argument(
            "--port",
            type=int,
            default=_get_debug_server_port(),
            help="Port that the debug server is listening on",
        )

    def run(self, args: argparse.Namespace) -> None:
        debug(args.host, args.port)


# ── New commands ──────────────────────────────────────────────────────────


class ApplyCmd:
    def add_arguments(self, subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument(
            "module_path",
            nargs="?",
            default=None,
            type=str,
            help="Dotted import path to a job object (e.g. myjob.job). "
            "If omitted, uses the current context's saved spec.",
        )

    def run(self, args: argparse.Namespace) -> None:
        apply_job(args.module_path)


class ExecCmd:
    def add_arguments(self, subparser: argparse.ArgumentParser) -> None:
        # ── Targeting (mutually exclusive; default is --one) ──────────────
        target = subparser.add_mutually_exclusive_group()
        target.add_argument(
            "--all",
            action="store_true",
            default=False,
            dest="run_all",
            help="Run on all meshes and all ranks. Output is redirected to per-rank files.",
        )
        target.add_argument(
            "--mesh",
            type=str,
            default=None,
            metavar="NAME",
            help="Run on all ranks of the named mesh. Output is redirected to per-rank files.",
        )
        target.add_argument(
            "--one",
            action="store_true",
            default=False,
            help="Run on rank 0 of the first mesh and stream output (default).",
        )
        target.add_argument(
            "--point",
            type=str,
            default=None,
            metavar="DIM=N,DIM=N",
            help="Run on a specific coordinate, e.g. --point host=4,gpu=3. Streams output.",
        )

        subparser.add_argument(
            "--per-host",
            type=str,
            default=None,
            metavar="DIM=N",
            dest="per_host",
            help="Spawn N processes per host along the given dimension before executing "
            "(e.g. --per-host gpu=4). Each process receives MONARCH_RANK_<DIM>=<rank> "
            "and MONARCH_SIZE_<DIM>=<size> environment variables for every dimension "
            "of its rank.",
        )
        subparser.add_argument(
            "-e",
            "--env",
            action="append",
            default=[],
            help="Extra environment variables as KEY=VALUE (can be repeated)",
        )
        subparser.add_argument(
            "--workdir",
            type=str,
            default=None,
            help="Working directory on workers",
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
            help="Read a bash script from FILE (use '-' for stdin)",
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

        per_host: dict[str, int] | None = None
        if args.per_host:
            k, v = args.per_host.split("=", 1)
            per_host = {k.strip(): int(v.strip())}
        rc = exec_on_job(
            cmd,
            run_all=args.run_all,
            mesh_name=args.mesh,
            point_str=args.point,
            env=args.env or None,
            workdir=args.workdir,
            kill=args.kill,
            script=args.script,
            per_host=per_host,
        )
        if rc != 0:
            sys.exit(rc)


class ContextCmd:
    def add_arguments(self, subparser: argparse.ArgumentParser) -> None:
        sub = subparser.add_subparsers(title="CONTEXT COMMANDS", dest="context_cmd")

        # create
        p_create = sub.add_parser("create", help="Create a new context")
        p_create.add_argument("name", type=str, help="Context name")
        p_create.set_defaults(context_func=lambda a: context_create(a.name))

        # use
        p_use = sub.add_parser("use", help="Switch .monarch/job_state.pkl to a context")
        p_use.add_argument("name", type=str, help="Context name to activate")
        p_use.set_defaults(context_func=lambda a: context_use(a.name))

        # rm
        p_rm = sub.add_parser("rm", help="Remove a context (kills the job)")
        p_rm.add_argument("name", type=str, help="Context name to remove")
        p_rm.set_defaults(context_func=lambda a: context_rm(a.name))

        # ls
        p_ls = sub.add_parser("ls", help="List contexts")
        p_ls.set_defaults(context_func=lambda a: context_ls())

    def run(self, args: argparse.Namespace) -> None:
        if not hasattr(args, "context_func"):
            # No subcommand given — print help
            args._subparser.print_help()  # pyre-ignore
            sys.exit(1)
        args.context_func(args)


class KillCmd:
    def add_arguments(self, subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument(
            "name",
            nargs="?",
            default=None,
            help="Context name to kill (default: active context)",
        )

    def run(self, args: argparse.Namespace) -> None:
        from monarch._src.job.job import job_load
        from monarch._src.tools.commands import (
            _context_state,
            _current_context,
            DEFAULT_JOB_PATH,
        )

        name = args.name or _current_context()
        if name is not None:
            state_file = _context_state(name)
            if state_file.exists():
                job_load(str(state_file)).kill()
                print(f"Killed context '{name}'")
                return
        job_load(DEFAULT_JOB_PATH).kill()
        print("Killed job")


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
    subparser = parser.add_subparsers(title="COMMANDS")

    context_cmd = ContextCmd()
    context_parser = subparser.add_parser("context", help="Manage job contexts")
    context_cmd.add_arguments(context_parser)
    context_parser.set_defaults(func=context_cmd.run, _subparser=context_parser)

    for cmd_name, cmd, cmd_help in [
        ("apply", ApplyCmd(), "Provision workers from a job object (e.g. myjob.job)"),
        (
            "exec",
            ExecCmd(),
            "Run a command on workers. Sets MONARCH_RANK_<DIM> and MONARCH_SIZE_<DIM> "
            "env vars for each rank dimension.",
        ),
        ("kill", KillCmd(), "Kill the active job"),
        ("debug", DebugCmd(), "Connect to the debug server"),
    ]:
        cmd_parser = subparser.add_parser(cmd_name, help=cmd_help)
        cmd.add_arguments(cmd_parser)
        cmd_parser.set_defaults(func=cmd.run)
    return parser


def main(argv: list[str] = sys.argv[1:]) -> None:
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
    parser = get_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
