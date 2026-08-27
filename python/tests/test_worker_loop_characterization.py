# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""When the occupied-address failure of ``run_worker_loop_forever`` is published.

Constructing the raw binding against an address that is already taken succeeds;
the failure is published when the returned task is driven. That is the claim
here, and it is a claim about *publication*, not about attempt timing: this test
cannot rule out an eager bind attempt whose error was withheld until the drive.
That the bind is genuinely attempted inside the task is source-grounded instead,
by ``host(...)`` living in the future the task owns.

The drive runs in a child process under a deadline. A regression that makes the
bind succeed starts a worker that owns its process lifetime, which would hang or
exit the test process rather than fail it.
"""

import os
import signal
import socket
import subprocess
import sys

# Long enough for a child interpreter to import the bindings on a loaded host,
# short enough that a worker which wrongly starts gets reported rather than
# waited on.
_CHILD_DEADLINE_SECONDS: int = 180
_CHILD_CLEANUP_SECONDS: int = 10

# An instance-form service proc id. The bare word "service" also parses, but
# only as the legacy singleton the fallback already produces, so it would not
# show that the "proc_id@location" split was taken.
#
# The uid is a known-valid literal so the child fixture remains deterministic.
# It is not the value from the binding's address-format help: that example is a
# placeholder and is rejected as an invalid base58 uid. This one was taken from
# a generated id and round-trips. A parse failure here would surface loudly, as
# construction raising before the child ever reaches its drive.
_SERVICE_PROC_ID: str = "service<E4cgvRepadk>"

# The child reports progress as tagged single-line records, so the parent can
# require that construction succeeded *before* the drive failed and can read the
# failure text out of its own record rather than out of merged output. It
# imports only the raw bootstrap binding: reaching the same task through the
# pytokio module would add a scanned module import to a test that changes no
# production code.
_CHILD_SOURCE: str = """
import sys

from monarch._rust_bindings.monarch_hyperactor.bootstrap import (
    run_worker_loop_forever,
)

task = run_worker_loop_forever(sys.argv[1])
print("CONSTRUCTED", flush=True)

try:
    task.block_on()
except BaseException as err:
    print("EXACT_VALUE_ERROR", type(err) is ValueError, flush=True)
    print("MESSAGE", str(err).replace("\\n", " "), flush=True)
    sys.exit(0)

print("NO_RAISE", flush=True)
sys.exit(1)
"""


def _records(output: str, tag: str) -> list[str]:
    """The values of the child's ``tag`` records, ignoring any other output."""
    prefix = f"{tag} "
    return [
        line.removeprefix(prefix)
        for line in output.splitlines()
        if line.startswith(prefix)
    ]


def _kill_group(child: "subprocess.Popen[str]") -> None:
    """Signal the binding child's group; tolerate a group that is already gone."""
    try:
        os.killpg(child.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def _kill_and_reap(child: "subprocess.Popen[str]") -> None:
    """Bound cleanup without waiting for stdout held by worker descendants."""
    if child.poll() is None:
        _kill_group(child)

    # Native-launched workers create their own process groups and may inherit
    # this pipe. Closing our reader keeps their writers from delaying cleanup.
    if child.stdout is not None:
        child.stdout.close()

    try:
        child.wait(timeout=_CHILD_CLEANUP_SECONDS)
    except subprocess.TimeoutExpired:
        try:
            child.kill()
        except ProcessLookupError:
            pass
        try:
            child.wait(timeout=_CHILD_CLEANUP_SECONDS)
        except subprocess.TimeoutExpired as error:
            raise AssertionError(
                "the worker-loop test child could not be reaped within the cleanup deadline"
            ) from error


def test_occupied_numeric_address_fails_when_blocked_on() -> None:
    occupied = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    child = None
    try:
        occupied.bind(("127.0.0.1", 0))
        occupied.listen(1)
        port = occupied.getsockname()[1]

        env = {**os.environ}
        if "FB_XAR_INVOKED_NAME" in os.environ:
            env["PYTHONPATH"] = ":".join(sys.path)

        # Its own session isolates the binding child's process group. Native
        # worker children create their own groups, so cleanup below also closes
        # the stdout reader and bounds every wait instead of relying on pipe EOF.
        child = subprocess.Popen(
            [
                sys.executable,
                "-c",
                _CHILD_SOURCE,
                f"{_SERVICE_PROC_ID}@tcp://127.0.0.1:{port}",
            ],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )

        try:
            output = child.communicate(timeout=_CHILD_DEADLINE_SECONDS)[0]
        except subprocess.TimeoutExpired as error:
            partial_output = error.stdout
            if isinstance(partial_output, bytes):
                partial_output = partial_output.decode(errors="replace")
            output = partial_output or ""
            raise AssertionError(
                "the occupied address must fail when the task is driven; a "
                "child that keeps running means the failure is no longer "
                f"published by the drive. child output:\n{output}"
            ) from None

        assert child.returncode == 0, f"child did not observe a failure:\n{output}"

        lines = output.splitlines()
        assert "CONSTRUCTED" in lines, (
            "construction must succeed on an occupied address, which is what "
            f"shows the failure is not published by the call. child output:\n{output}"
        )
        kinds = _records(output, "EXACT_VALUE_ERROR")
        assert kinds == ["True"], (
            f"the drive must raise exactly ValueError. child output:\n{output}"
        )

        messages = _records(output, "MESSAGE")
        assert len(messages) == 1, (
            f"the child must report exactly one failure. child output:\n{output}"
        )
        message = messages[0]
        assert "listen:" in message, (
            f"the failure must come from the host bind, got: {message}"
        )
        assert "in use" in message, (
            f"the cause must be the occupied address, got: {message}"
        )
        assert f"127.0.0.1:{port}" in message, (
            f"the failure must name the address under test, got: {message}"
        )
    finally:
        # Reap before releasing the port, so a child that is still alive cannot
        # take the address as it goes away. Signalling is guarded on the child
        # still running: once it has been reaped its pid can be reused, and the
        # group that number names may belong to somebody else. The nested
        # finally keeps the listener from outliving a failure in that cleanup.
        try:
            if child is not None:
                _kill_and_reap(child)
        finally:
            occupied.close()
