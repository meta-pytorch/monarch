# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""Fresh root-client bootstrap must not be attempted from inside Tokio.

Bootstrapping a client ends in ``block_on()``, which panics on a Tokio runtime
worker. The guard rejects fresh bootstrap from every Tokio runtime context,
including its blocking pool, while still allowing an already-initialized client
to be reused.
"""

from unittest.mock import MagicMock, patch

import pytest
from isolate_in_subprocess import isolate_in_subprocess
from monarch._rust_bindings.monarch_hyperactor.pytokio import (
    is_tokio_thread,
    PythonTask,
    WouldBlockRuntime,
)
from monarch._src.actor.host_mesh import this_host
from monarch.actor import Actor, endpoint

_EXPECTED = "WouldBlockRuntime uninitialized=True"


def test_attach_tracks_process_global_address_and_is_idempotent() -> None:
    from monarch._src.actor import actor_mesh

    client_context = actor_mesh._Lazy(MagicMock())
    context = MagicMock()
    with (
        patch.object(actor_mesh, "_client_context", client_context),
        patch.object(actor_mesh, "_client_attach_to", None),
        patch.object(actor_mesh, "_init_client_context", return_value=context) as init,
    ):
        actor_mesh.attach("tcp://host:1")
        actor_mesh.attach("tcp://host:1")

        assert actor_mesh._client_attached_to() == "tcp://host:1"
        assert client_context.try_get() is context
        init.assert_called_once_with(via="tcp://host:1")


def test_attach_rejects_different_address() -> None:
    from monarch._src.actor import actor_mesh

    with (
        patch.object(actor_mesh, "_client_context", actor_mesh._Lazy(MagicMock())),
        patch.object(actor_mesh, "_client_attach_to", None),
        patch.object(actor_mesh, "_init_client_context", return_value=MagicMock()),
    ):
        actor_mesh.attach("tcp://host:1")
        with pytest.raises(RuntimeError, match="tcp://host:1, not tcp://host:2"):
            actor_mesh.attach("tcp://host:2")


def test_attach_rejects_locally_initialized_client() -> None:
    from monarch._src.actor import actor_mesh

    client_context = actor_mesh._Lazy(MagicMock())
    client_context.get()
    with (
        patch.object(actor_mesh, "_client_context", client_context),
        patch.object(actor_mesh, "_client_attach_to", None),
    ):
        with pytest.raises(RuntimeError, match="initialized without a remote"):
            actor_mesh.attach("tcp://host:1")


def test_attach_does_not_publish_failed_initialization() -> None:
    from monarch._src.actor import actor_mesh

    with (
        patch.object(actor_mesh, "_client_context", actor_mesh._Lazy(MagicMock())),
        patch.object(actor_mesh, "_client_attach_to", None),
        patch.object(
            actor_mesh,
            "_init_client_context",
            side_effect=RuntimeError("bootstrap failed"),
        ),
    ):
        with pytest.raises(RuntimeError, match="bootstrap failed"):
            actor_mesh.attach("tcp://host:1")

        assert actor_mesh._client_attached_to() is None
        assert actor_mesh._client_context.try_get() is None


def _probe_fresh_bootstrap() -> str:
    """Ask for a context on a Tokio thread with neither actor nor client."""
    from monarch._src.actor.actor_mesh import _client_context, _context, context

    if not is_tokio_thread():
        return "precondition: not a tokio thread"
    if _client_context.try_get() is not None:
        return "precondition: client already initialized"
    # spawn_blocking propagates the caller's actor context; drop it so the
    # fresh-bootstrap branch is the one under test.
    token = _context.set(None)
    try:
        context()
    except WouldBlockRuntime:
        return f"WouldBlockRuntime uninitialized={_client_context.try_get() is None}"
    # BaseException, not Exception: PyO3's PanicException derives from it, and
    # reporting that panic by name is the point of these probes. The result is
    # transformed into the returned string, not swallowed.
    except BaseException as err:  # noqa: B036
        return f"{type(err).__name__}: {err}"
    else:
        return "no raise"
    finally:
        _context.reset(token)


async def _probe_fresh_bootstrap_on_worker() -> str:
    """The incident shape: a runtime *worker* thread, where block_on panics.

    ``spawn_blocking`` threads report ``is_tokio_thread()`` but tolerate
    ``block_on``; a worker thread does not. This is the case that motivated
    the guard.
    """
    return _probe_fresh_bootstrap()


def _probe_attach() -> str:
    """``attach()`` bootstraps directly, bypassing ``context()``."""
    from monarch._src.actor.actor_mesh import _client_context, attach

    if not is_tokio_thread():
        return "precondition: not a tokio thread"
    if _client_context.try_get() is not None:
        return "precondition: client already initialized"
    try:
        attach("tcp://127.0.0.1:1")
    except WouldBlockRuntime:
        return f"WouldBlockRuntime uninitialized={_client_context.try_get() is None}"
    # BaseException, not Exception: PyO3's PanicException derives from it, and
    # reporting that panic by name is the point of these probes. The result is
    # transformed into the returned string, not swallowed.
    except BaseException as err:  # noqa: B036
        return f"{type(err).__name__}: {err}"
    return "no raise"


def _probe_reuse() -> str:
    """An initialized client must still be handed back on a Tokio thread."""
    from monarch._src.actor.actor_mesh import _client_context, _context, context

    if not is_tokio_thread():
        return "precondition: not a tokio thread"
    expected = _client_context.try_get()
    if expected is None:
        return "precondition: client not initialized"
    token = _context.set(None)
    try:
        got = context()
    # BaseException, not Exception: PyO3's PanicException derives from it, and
    # reporting that panic by name is the point of these probes. The result is
    # transformed into the returned string, not swallowed.
    except BaseException as err:  # noqa: B036
        return f"{type(err).__name__}: {err}"
    else:
        return "reused" if got is expected else "different object"
    finally:
        _context.reset(token)


class _Prober(Actor):
    """Runs probes on Tokio worker and blocking threads in a worker process.

    A worker process never bootstraps a client of its own, so it is the only
    place the fresh-bootstrap branch is genuinely reachable.
    """

    @endpoint
    def run(self, which: str) -> str:
        if which == "fresh_worker":
            return (
                PythonTask.from_coroutine(_probe_fresh_bootstrap_on_worker())
                .spawn()
                .block_on()
            )
        probes = {"fresh": _probe_fresh_bootstrap, "attach": _probe_attach}
        return PythonTask.spawn_blocking(probes[which]).block_on()


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_context_on_tokio_without_client_refuses_fresh_bootstrap() -> None:
    pm = this_host().spawn_procs(per_host={"gpus": 1})
    try:
        prober = pm.spawn("prober", _Prober)
        assert prober.run.call_one("fresh").get() == _EXPECTED
    finally:
        pm.stop().get()


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_context_on_tokio_worker_refuses_fresh_bootstrap() -> None:
    """The incident shape: fresh bootstrap from a runtime worker thread."""
    pm = this_host().spawn_procs(per_host={"gpus": 1})
    try:
        prober = pm.spawn("prober", _Prober)
        assert prober.run.call_one("fresh_worker").get() == _EXPECTED
    finally:
        pm.stop().get()


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_attach_on_tokio_refuses_before_bootstrap() -> None:
    pm = this_host().spawn_procs(per_host={"gpus": 1})
    try:
        prober = pm.spawn("prober", _Prober)
        assert prober.run.call_one("attach").get() == _EXPECTED
    finally:
        pm.stop().get()


@pytest.mark.timeout(120)
@isolate_in_subprocess
def test_context_on_tokio_reuses_initialized_client() -> None:
    """The guard must not reject a contextless Tokio caller out of hand."""
    from monarch._src.actor.actor_mesh import _client_context, context

    context()
    assert _client_context.try_get() is not None, "client should be initialized here"
    assert PythonTask.spawn_blocking(_probe_reuse).block_on() == "reused"
