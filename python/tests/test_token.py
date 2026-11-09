# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Test connecting to existing actor meshes through a shared token string.

Most cases start `token_owner.py` in a separate Python process. That helper
creates a `Probe` actor and publishes its mesh. This test process reads the
token from a temporary file, connects with `connect(token, Probe)`, and calls
the probe's endpoints. A fresh interpreter avoids inheriting an actor runtime
from the test process through a fork.

The probe exposes a value stored in the helper process and can run trusted
test functions there. We check its PID, change the value through one
connection, and read it through another. Both connections originate from
this test process, not two independent client processes. The helper also
records each connection notification.

Two helper modes distinguish the token owner's lifetime from its process:

- `process`: publication happens outside an actor. Monarch's built-in client
  actor owns the token; exiting the helper ends it.
- `actor`: a `Publisher` actor owns the token. The test stops that actor while
  leaving the helper process running, then checks that connections time out.

Files carry the token, connection notifications, and stop acknowledgements;
stdin carries commands to the helper. Startup and cleanup waits are bounded.
Cleanup requests exit; on timeout it kills the child and makes a bounded wait
to reap it. It closes stdin even if cleanup fails.

Other cases check malformed tokens and invalid timeouts. A separate two-rank
test publishes a mesh directly and checks that connecting preserves both
actor ranks and selection by a dimension label.
"""

import base64
import json
import math
import os
import signal
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import cast, Literal, NamedTuple

import monarch.actor
import pytest
import token_owner
from monarch._src.actor.actor_mesh import ActorMesh
from monarch._src.actor.token import connect, publish
from monarch.actor import MeshFailure, this_host, this_proc
from token_owner import Probe

# Seconds allowed for helper startup, ordinary endpoint calls, and each
# cleanup wait.
WAIT: float = 30.0


class Owner(NamedTuple):
    """The helper process, its control files, and the token it published."""

    # Child interpreter hosting `Probe` and, in actor mode, `Publisher`.
    process: subprocess.Popen[str]
    # Temporary directory shared by this test and the helper through file paths.
    directory: str
    # Text read from `directory/token`, passed to `connect()`.
    token: str


def _launch(mode: Literal["process", "actor"], directory: str) -> subprocess.Popen[str]:
    """Start the publishing helper in an independent Python process.

    `mode` chooses who publishes: the helper's built-in client actor or its
    `Publisher` actor. `directory` holds the files used to talk to the test.
    """
    env = dict(os.environ)
    if "FB_XAR_INVOKED_NAME" in os.environ:
        # Meta's packaged test executable (PAR) carries Python and its
        # dependencies. Re-enter it with the helper as its main module.
        command = [sys.argv[0], mode, directory]
        env["PAR_MAIN_OVERRIDE"] = "token_owner"
        # Keep the helper and its dependencies importable in the child.
        env["PYTHONPATH"] = os.pathsep.join(sys.path)
    else:
        # Outside a packaged test, use this interpreter to run the helper file.
        command = [
            sys.executable,
            os.path.abspath(token_owner.__file__),
            mode,
            directory,
        ]
    # stdin is the control channel. Leave stdout and stderr inherited so
    # helper errors appear in the test's output rather than a hidden pipe.
    return subprocess.Popen(
        command,
        env=env,
        stdin=subprocess.PIPE,
        text=True,
    )


def _wait_for_file(path: str, process: subprocess.Popen[str]) -> str:
    """Wait for a nonempty helper file and return its text.

    Used for the published token and the acknowledgement that `Publisher`
    stopped. While waiting, fail early if the helper exits; otherwise fail
    after `WAIT` seconds.
    """
    deadline = time.monotonic() + WAIT
    while time.monotonic() < deadline:
        if os.path.exists(path) and os.path.getsize(path) > 0:
            with open(path) as f:
                return f.read().strip()
        assert process.poll() is None, f"the owner exited with {process.returncode}"
        time.sleep(0.1)
    raise AssertionError(f"{path} did not appear within {WAIT}s")


def _send(owner: Owner, command: str) -> None:
    """Send one command to the helper through its stdin, flushing it immediately."""
    assert owner.process.stdin is not None
    owner.process.stdin.write(command + "\n")
    owner.process.stdin.flush()


@contextmanager
def _owner(mode: Literal["process", "actor"]) -> Iterator[Owner]:
    """Yield the helper's published token, then stop and reap the helper.

    The token file acknowledges publication. Cleanup runs even if startup
    fails before yielding. Each graceful-exit and forced-reap wait is bounded
    by `WAIT`; stdin is closed even if either wait fails.
    """
    with tempfile.TemporaryDirectory() as directory:
        process = _launch(mode, directory)
        try:
            token = _wait_for_file(os.path.join(directory, "token"), process)
            yield Owner(process, directory, token)
        finally:
            try:
                # Ask the helper to exit and wait for it, rather than leaving
                # the publishing process alive after this test finishes.
                process.communicate("exit\n", timeout=WAIT)
            except subprocess.TimeoutExpired:
                # Force-stop our unresponsive child, then reap it within
                # another deadline. Do not signal a process group after reaping.
                process.kill()
                process.wait(timeout=WAIT)
            finally:
                # `communicate()` can time out before closing its stdin pipe.
                assert process.stdin is not None
                process.stdin.close()


def _peers(owner: Owner) -> list[str]:
    """Read connecting actors' addresses recorded by the helper in process mode.

    Repeated addresses count as separate notifications, not distinct clients.
    A missing file means nothing has been recorded yet; it does not prove that
    no connection was attempted.
    """
    path = os.path.join(owner.directory, "peers")
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return f.read().split()


def _wait_for_peers(owner: Owner, count: int) -> None:
    """Wait for exactly `count` recorded notifications, failing on extras.

    Reaching the count also shows that the helper's notification-reporting
    thread is making progress, which an empty-file check cannot establish.
    """
    deadline = time.monotonic() + WAIT
    while len(_peers(owner)) < count and time.monotonic() < deadline:
        time.sleep(0.1)
    assert len(_peers(owner)) == count, _peers(owner)


def _process_state(pid: int) -> str:
    """Return the single-letter scheduler state of `pid` (e.g. "T" when stopped)."""
    if sys.platform == "linux":
        with open(f"/proc/{pid}/stat") as f:
            # The process name in parentheses can contain spaces and parentheses.
            return f.read().rsplit(")", 1)[1].split()[0]
    # No procfs on macOS; ps reports the state first, followed by flag letters.
    return subprocess.check_output(
        ["ps", "-o", "state=", "-p", str(pid)], text=True
    ).strip()[:1]


def _wait_for_stopped(process: subprocess.Popen[str]) -> None:
    """Acknowledge SIGSTOP before sending a join; fail rather than race onward."""
    deadline = time.monotonic() + WAIT
    while time.monotonic() < deadline:
        assert process.poll() is None, "the owner exited before it could be held"
        state = _process_state(process.pid)
        if state == "T":
            return
        time.sleep(0.01)
    raise AssertionError("the owner never acknowledged SIGSTOP")


@contextmanager
def _held_owner(owner: Owner) -> Iterator[None]:
    """Hold all of the helper's threads, then resume it even on assertion failure.

    The helper owns no child processes. Its outer fixture bounds exit and reap.
    """
    try:
        owner.process.send_signal(signal.SIGSTOP)
        _wait_for_stopped(owner.process)
        yield
    finally:
        owner.process.send_signal(signal.SIGCONT)


def _token_with_region(token: str, region: dict[str, object]) -> str:
    """Replace the advertised region without changing the generic rendezvous token."""
    payload = json.loads(base64.b64decode(token.split("#", 1)[0]))
    payload["region"] = region
    return base64.b64encode(json.dumps(payload).encode()).decode()


@pytest.mark.timeout(120)
def test_connections_share_the_owner_state_and_the_owner_sees_each_peer() -> None:
    """Two connections share one remote state and produce two notifications."""
    with _owner("process") as owner:
        first = connect(owner.token, Probe)
        assert first.read.call_one().get(timeout=WAIT) == 0
        # The function executes in the helper, not in this test process.
        assert first.run.call_one(lambda state: os.getpid()).get(timeout=WAIT) == (
            owner.process.pid
        )
        assert first.run.call_one(lambda state: state.set(7)).get(timeout=WAIT) == 0

        # A new connection must see the first connection's change to the
        # helper's `State`, rather than an independently copied value.
        second = connect(owner.token, Probe)
        assert second.read.call_one().get(timeout=WAIT) == 7

        # Both requests come from the same client actor; each is reported.
        _wait_for_peers(owner, 2)


def test_a_malformed_token_is_rejected_before_connecting() -> None:
    """Malformed token text must raise `ValueError`."""
    # PYTK-1: see monarch_hyperactor/src/token.rs.
    with pytest.raises(ValueError, match="invalid token"):
        connect("not a token", Probe)


@pytest.mark.timeout(120)
@pytest.mark.parametrize("timeout", [-1.0, math.nan, math.inf, 1e300])
def test_an_invalid_timeout_is_rejected_before_connecting(timeout: float) -> None:
    """Reject invalid deadlines without sending a connection notification.

    Cover negative, non-finite, and unrepresentable durations. A subsequent
    valid connection proves that the helper's notification reader works.
    """
    # PYTK-1: see monarch_hyperactor/src/token.rs.
    with _owner("process") as owner:
        with pytest.raises(ValueError, match="invalid timeout"):
            connect(owner.token, Probe, timeout=timeout)
        # An empty file alone could mean a dead reporting thread. Establish
        # its progress with a valid connection before checking for extras.
        connect(owner.token, Probe)
        _wait_for_peers(owner, 1)
        # Allow an extra notification from the rejected call to show up.
        time.sleep(1.0)
        assert len(_peers(owner)) == 1, _peers(owner)


@pytest.mark.timeout(120)
def test_stopping_the_owning_actor_ends_the_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stopping `Publisher` ends its token without ending the helper process,
    and connecting afterwards fails only that call, not this process."""
    # Record root-actor faults instead of letting the default hook exit.
    faults: list[MeshFailure] = []
    monkeypatch.setattr(monarch.actor, "unhandled_fault_hook", faults.append)
    with _owner("actor") as owner:
        probe = connect(owner.token, Probe)
        # Establish that the token works before stopping its publishing actor.
        assert probe.read.call_one().get(timeout=WAIT) == 0
        _send(owner, "stop")
        # The helper writes this only after `Publisher` has finished stopping.
        _wait_for_file(os.path.join(owner.directory, "stopped"), owner.process)
        assert owner.process.poll() is None, "the owner's process keeps running"
        # PYTK-3: see monarch_hyperactor/src/token.rs. Failure belongs to
        # mesh observers, not connect() or the caller's root actor.
        stale = connect(owner.token, Probe, timeout=3.0)
        call = stale.read.call_one()
        with pytest.raises(TimeoutError):
            stale.initialized.get(timeout=WAIT)
        with pytest.raises(TimeoutError):
            call.get(timeout=WAIT)
        # A request returned from the stopped rendezvous would have failed
        # this process's root actor well before the timeout.
        assert faults == [], faults


@pytest.mark.timeout(120)
def test_a_token_stops_working_when_its_owner_exits() -> None:
    """An exited publishing process leaves a token that cannot accept connections."""
    with _owner("process") as owner:
        _send(owner, "exit")
        # Reap the helper before retrying: this tests an exited process,
        # not a connection racing with its shutdown.
        owner.process.wait(timeout=WAIT)
        # PYTK-3: see monarch_hyperactor/src/token.rs.
        stale = connect(owner.token, Probe, timeout=3.0)
        with pytest.raises(TimeoutError):
            stale.initialized.get(timeout=WAIT)


@pytest.mark.timeout(120)
def test_a_connection_reaches_every_rank_of_the_published_mesh() -> None:
    """Connecting preserves the published ranks and selection by dimension label."""
    # One `Probe` per proc gives each actor rank a distinct process ID.
    probes = this_host().spawn_procs(per_host={"gpus": 2}).spawn("probes", Probe)
    probes.initialized.get()
    published = publish(probes)
    connected = connect(published.token, Probe)
    pids = list(probes.run.call(lambda state: os.getpid()).get(timeout=WAIT).values())
    assert len(set(pids)) == 2
    # Compare against the original mesh in rank order, not just as a set.
    assert (
        list(connected.run.call(lambda state: os.getpid()).get(timeout=WAIT).values())
        == pids
    )
    # Selecting gpus=1 must reach the original mesh's second actor rank.
    assert (
        connected.slice(gpus=1)
        .run.call_one(lambda state: os.getpid())
        .get(timeout=WAIT)
        == pids[1]
    )


@pytest.mark.timeout(120)
def test_connect_returns_before_the_owner_answers() -> None:
    """An unanswerable join returns its mesh now and fails its observers later."""
    # PYTK-2/PYTK-3: see monarch_hyperactor/src/token.rs.
    this_proc()  # Bootstrap the caller before measuring connect itself.
    with _owner("process") as owner:
        payload = json.loads(base64.b64decode(owner.token.split("#", 1)[0]))
        _send(owner, "exit")
        owner.process.wait(timeout=WAIT)
        started = time.monotonic()
        probe = connect(owner.token, Probe, timeout=3.0)
        assert time.monotonic() - started < 1.0, "connect waited for the join timeout"
        mesh = cast(ActorMesh, probe)
        region = payload["region"]
        assert list(mesh._labels) == region["labels"]
        assert mesh._ndslice.sizes == region["slice"]["sizes"]
        assert mesh._ndslice.offset == region["slice"]["offset"]
        assert mesh._ndslice.strides == region["slice"]["strides"]
        calls = (probe.read.call_one(), probe.read.call(), probe.read.choose())
        with pytest.raises(TimeoutError):
            probe.initialized.get(timeout=WAIT)
        for call in calls:
            with pytest.raises(TimeoutError):
                call.get(timeout=WAIT)
        with pytest.raises(TimeoutError):
            probe.read.call_one().get(timeout=WAIT)


@pytest.mark.timeout(120)
def test_calls_queued_before_the_join_reach_the_published_actors() -> None:
    """A held join exposes shape but delays both ordinary and sliced calls."""
    # PYTK-2: see monarch_hyperactor/src/token.rs.
    with _owner("process") as owner:
        with _held_owner(owner):
            probe = connect(owner.token, Probe, timeout=WAIT)
            assert cast(ActorMesh, probe).sizes == {}
            with pytest.raises(TimeoutError):
                probe.initialized.get(timeout=0.1)
            with pytest.raises(ValueError, match="still being created"):
                publish(probe)
            update = probe.run.call_one(lambda state: state.set(7))
            read = probe.slice().read.call_one()
            all_ranks = probe.read.call()
            chosen = probe.read.choose()
            for pending in (update, read, all_ranks, chosen):
                with pytest.raises(TimeoutError):
                    pending.get(timeout=0.1)
        probe.initialized.get(timeout=WAIT)
        assert update.get(timeout=WAIT) == 0
        assert read.get(timeout=WAIT) == 7
        assert list(all_ranks.get(timeout=WAIT).values()) == [7]
        assert chosen.get(timeout=WAIT) == 7
        # The now-initialized reference can itself be published.
        published = publish(probe)
        assert connect(published.token, Probe).read.call_one().get(timeout=WAIT) == 7


@pytest.mark.timeout(120)
def test_a_held_owner_is_resumed_and_reaped_after_an_exception() -> None:
    """A failed test inside the join barrier still releases its only child."""
    with _owner("process") as owner:
        with pytest.raises(RuntimeError, match="injected failure"):
            with _held_owner(owner):
                raise RuntimeError("injected failure")
        assert connect(owner.token, Probe).read.call_one().get(timeout=WAIT) == 0
    assert owner.process.poll() is not None, "the helper was not reaped"


@pytest.mark.timeout(120)
def test_an_unresponsive_owner_is_killed_and_reaped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The fixture's graceful-exit timeout also works on a stopped child."""
    with _owner("process") as owner:
        owner.process.send_signal(signal.SIGSTOP)
        _wait_for_stopped(owner.process)
        # Startup used the ordinary budget; only cleanup uses this shorter one.
        monkeypatch.setattr(sys.modules[__name__], "WAIT", 1.0)
        started = time.monotonic()
    assert owner.process.returncode == -signal.SIGKILL
    assert time.monotonic() - started < 3.0, "cleanup exceeded both waits"


@pytest.mark.timeout(120)
def test_a_published_slice_keeps_its_region() -> None:
    """Connecting to a non-contiguous slice reaches the original physical ranks."""
    # PYTK-1/PYTK-2: see monarch_hyperactor/src/token.rs.
    procs = this_host().spawn_procs(per_host={"gpus": 4})
    try:
        probes = procs.spawn("probes", Probe)
        probes.initialized.get(timeout=WAIT)
        pids = list(
            probes.run.call(lambda state: os.getpid()).get(timeout=WAIT).values()
        )
        assert len(set(pids)) == 4
        selected = probes.slice(gpus=slice(1, 4, 2))
        selected.initialized.get(timeout=WAIT)
        published = publish(selected)
        connected = connect(published.token, Probe)
        assert (
            cast(ActorMesh, connected)._shape.region
            == cast(ActorMesh, selected)._shape.region
        )
        assert list(
            connected.run.call(lambda state: os.getpid()).get(timeout=WAIT).values()
        ) == [pids[1], pids[3]]
        assert (
            connected.slice(gpus=1)
            .run.call_one(lambda state: os.getpid())
            .get(timeout=WAIT)
            == pids[3]
        )
    finally:
        procs.stop().get(timeout=WAIT)


@pytest.mark.timeout(120)
def test_a_token_whose_region_disagrees_with_the_mesh_fails_its_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A valid advertised shape is not a substitute for the joined mesh's region."""
    # PYTK-2/PYTK-3: see monarch_hyperactor/src/token.rs.
    faults: list[MeshFailure] = []
    monkeypatch.setattr(monarch.actor, "unhandled_fault_hook", faults.append)
    with _owner("process") as owner:
        altered = _token_with_region(
            owner.token,
            {"labels": ["gpu"], "slice": {"offset": 0, "sizes": [1], "strides": [1]}},
        )
        with _held_owner(owner):
            probe = connect(altered, Probe, timeout=WAIT)
            call = probe.run.call_one(lambda state: state.set(7))
        with pytest.raises(RuntimeError, match="region does not match"):
            probe.initialized.get(timeout=WAIT)
        with pytest.raises(RuntimeError, match="region does not match"):
            call.get(timeout=WAIT)
        with pytest.raises(RuntimeError, match="region does not match"):
            probe.read.call_one().get(timeout=WAIT)
        assert connect(owner.token, Probe).read.call_one().get(timeout=WAIT) == 0
        assert faults == [], faults


@pytest.mark.timeout(120)
@pytest.mark.parametrize(
    "region",
    [
        {"labels": [], "slice": {"offset": 0, "sizes": [1], "strides": [1]}},
        {"labels": ["gpu"], "slice": {"offset": 0, "sizes": [2], "strides": []}},
        {
            "labels": ["gpu"],
            "slice": {"offset": 0, "sizes": [2], "strides": [sys.maxsize * 2 + 1]},
        },
        {
            "labels": ["gpu"],
            "slice": {"offset": sys.maxsize * 2 + 1, "sizes": [2], "strides": [1]},
        },
    ],
)
def test_a_malformed_region_is_rejected_before_connecting(
    region: dict[str, object],
) -> None:
    """Malformed region data fails synchronously without a peer notification."""
    # PYTK-1: see monarch_hyperactor/src/token.rs.
    with _owner("process") as owner:
        altered = _token_with_region(owner.token, region)
        with pytest.raises(ValueError, match="invalid token"):
            connect(altered, Probe)
        assert connect(owner.token, Probe).read.call_one().get(timeout=WAIT) == 0
        _wait_for_peers(owner, 1)
        time.sleep(1.0)
        assert len(_peers(owner)) == 1, _peers(owner)
