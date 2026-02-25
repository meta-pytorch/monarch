# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""Python ABC for custom proc launchers.

This module defines the interface that Python proc launchers must
implement to be used with ActorProcLauncher from Rust.

"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from monarch._src.actor.actor_mesh import Actor, Port
from monarch._src.actor.endpoint import endpoint


@dataclass
class LaunchOptions:
    """Options for launching a proc.

    Attributes:
        bootstrap_payload: Opaque string to set in the
            HYPERACTOR_MESH_BOOTSTRAP_MODE environment variable.
        process_name: Human-readable name to set in the
            HYPERACTOR_PROCESS_NAME environment variable.
        program: Path to the executable to run.
        arg0: Override for argv[0]. If None, use
            os.path.basename(program).
        args: Arguments to pass as argv[1..]. Does NOT include argv[0].
        env: Environment variables to set in the child process.
        want_stdio: Whether the manager wants access to stdio streams.
        tail_lines: Max stderr lines to retain for diagnostics
            (0 = none).
        log_channel: Optional ChannelAddr string for mesh log
            forwarding.
    """

    bootstrap_payload: str
    process_name: str
    program: str
    arg0: str | None
    args: list[str]
    env: dict[str, str]
    want_stdio: bool
    tail_lines: int
    log_channel: str | None


@dataclass
class ProcExitResult:
    """Terminal status of a proc.

    Exactly one of (exit_code, signal, failed_reason) should indicate
    how the proc terminated. The Rust side uses priority:
    failed_reason > signal > exit_code.
    """

    exit_code: int | None
    signal: int | None
    core_dumped: bool
    failed_reason: str | None
    stderr_tail: list[str]


class ProcLauncher(Actor, ABC):
    """ABC for Python proc launchers.

    Implementations control how procs are spawned (Docker, VMs, custom
    orchestrators, etc.) while Monarch handles lifecycle wiring.

    The launcher runs in the same proc as HostMeshAgent.
    """

    def __init__(self, params: dict[str, Any] | None = None) -> None:
        """Initialize the launcher.

        Args:
            params: Optional configuration dict from
                set_proc_launcher(params=...).
        """
        self.params = params

    @endpoint(explicit_response_port=True)
    @abstractmethod
    async def launch(
        self,
        exit_port: Port[ProcExitResult],
        proc_id: str,
        opts: LaunchOptions,
    ) -> None:
        """Launch a proc.

        Args:
            exit_port: Port to send ProcExitResult when the proc
                exits. MUST call exit_port.send(result) or
                exit_port.exception(e).
            proc_id: Unique identifier for this proc.
            opts: Launch configuration.

        Note:
            With explicit_response_port=True, exceptions are NOT
            automatically forwarded. You MUST explicitly call
            exit_port.exception(e) in except blocks.
        """
        ...

    @endpoint
    @abstractmethod
    async def terminate(self, proc_id: str, timeout_secs: float) -> None:
        """Initiate graceful termination.

        Best-effort; response is dropped (fire-and-forget from Rust
        side).
        """
        ...

    @endpoint
    @abstractmethod
    async def kill(self, proc_id: str) -> None:
        """Force-kill a proc.

        Best-effort; response is dropped (fire-and-forget from Rust
        side).
        """
        ...
