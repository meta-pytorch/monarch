# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""Worker-loop eager-start, context rejection, errors, and signal behavior.

The native start binding returns a ``Handle`` immediately. Host failures are
published through that Handle, while dropping it does not cancel a successfully
started worker. A SIGTERM handler installed after startup must remain active
while that worker is serving.

Each case runs in a child process under a deadline because a successful forever
worker owns its process lifetime.
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

# An instance-form service proc id for the worker.
#
# The uid is a known-valid literal so the child fixture remains deterministic.
# It is not the value from the binding's address-format help: that example is a
# placeholder and is rejected as an invalid base58 uid. This one was taken from
# a generated id and round-trips. A parse failure here would surface loudly
# because its message would not describe the host bind failure required below.
_SERVICE_PROC_ID: str = "service<E4cgvRepadk>"

# The child reports the error as tagged single-line records, so the parent can
# require its exact type and read the message from its own record rather than
# from merged output. Importing the raw binding keeps the assertion on the
# native error without adding the public wrapper's argument validation.
_ERROR_CHILD_SOURCE: str = """
import sys

from monarch._rust_bindings.monarch_hyperactor.bootstrap import start_worker_loop_forever

observer = start_worker_loop_forever(sys.argv[1])
print("STARTED", flush=True)

try:
    observer.get()
except BaseException as err:
    print("EXACT_VALUE_ERROR", type(err) is ValueError, flush=True)
    print("MESSAGE", str(err).replace("\\n", " "), flush=True)
    sys.exit(0)

print("NO_RAISE", flush=True)
sys.exit(1)
"""

_LIFETIME_CHILD_SOURCE: str = """
import os
import signal
import sys
import time

from monarch.actor import attach_to_workers, start_worker_loop_forever


def sentinel(_signum, _frame):
    print("SIGTERM_HANDLED", flush=True)


signal.signal(signal.SIGTERM, sentinel)
worker = start_worker_loop_forever(
    ca="trust_all_connections",
    address=sys.argv[1],
)
signal.signal(signal.SIGTERM, sentinel)
del worker
print("OBSERVER_DROPPED", flush=True)

hosts = attach_to_workers(
    ca="trust_all_connections",
    workers=[sys.argv[1]],
)
hosts.initialized.get(timeout=30)
print("SERVING", flush=True)
print(
    "HANDLER_IS_SENTINEL",
    signal.getsignal(signal.SIGTERM) is sentinel,
    flush=True,
)
os.kill(os.getpid(), signal.SIGTERM)
hosts.shutdown().get(timeout=30)
print("SHUTDOWN_REQUESTED", flush=True)
time.sleep(30)
print("DID_NOT_EXIT", flush=True)
sys.exit(3)
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


def test_occupied_numeric_address_failure_is_published_by_handle() -> None:
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
                _ERROR_CHILD_SOURCE,
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
                "the occupied address must fail through Handle.get(); "
                "a child that keeps running means the failure is no longer "
                f"published to its observer. child output:\n{output}"
            ) from None

        assert child.returncode == 0, f"child did not observe a failure:\n{output}"

        assert "STARTED" in output.splitlines(), (
            "the binding must return Handle before the host failure is "
            f"observed. child output:\n{output}"
        )
        kinds = _records(output, "EXACT_VALUE_ERROR")
        assert kinds == ["True"], (
            f"Handle.get() must raise exactly ValueError. child output:\n{output}"
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


def test_dropped_start_keeps_serving_and_preserves_sigterm_handler() -> None:
    child = None
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reserved:
        reserved.bind(("127.0.0.1", 0))
        port = reserved.getsockname()[1]

    try:
        env = {**os.environ}
        if "FB_XAR_INVOKED_NAME" in os.environ:
            env["PYTHONPATH"] = ":".join(sys.path)

        child = subprocess.Popen(
            [
                sys.executable,
                "-c",
                _LIFETIME_CHILD_SOURCE,
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
                "the dropped observer must leave a serving worker whose "
                f"SIGTERM handler remains active. child output:\n{output}"
            ) from None

        assert child.returncode == 0, f"worker child failed:\n{output}"
        assert "DID_NOT_EXIT" not in output, (
            "the forever worker must exit the process after normal host shutdown. "
            f"child output:\n{output}"
        )
        lines = output.splitlines()
        assert "OBSERVER_DROPPED" in lines, (
            f"the child must discard the observer. child output:\n{output}"
        )
        assert "SERVING" in lines, (
            "the worker must become reachable after its observer is discarded. "
            f"child output:\n{output}"
        )
        assert "HANDLER_IS_SENTINEL True" in lines, (
            "native startup must not replace the post-start SIGTERM handler. "
            f"child output:\n{output}"
        )
        assert "SIGTERM_HANDLED" in lines, (
            "SIGTERM must run the Python handler rather than Folly's handler. "
            f"child output:\n{output}"
        )
    finally:
        if child is not None:
            _kill_and_reap(child)
