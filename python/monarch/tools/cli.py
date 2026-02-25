# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import argparse
import json
import sys

from monarch.tools.commands import (
    bounce,
    component_args_from_cli,
    create,
    debug,
    info,
    kill,
    stop,
    torchx_runner,
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


class KillCmd:
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


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Monarch CLI")
    subparser = parser.add_subparsers(title="COMMANDS")

    for cmd_name, cmd in {
        "create": CreateCmd(),
        "info": InfoCmd(),
        "kill": KillCmd(),
        "debug": DebugCmd(),
        # --- placeholder subcommands (not yet implemented) ---
        "bounce": BounceCmd(),
        "stop": StopCmd(),
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
