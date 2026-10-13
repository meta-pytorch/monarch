# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from pathlib import Path
from typing import Literal, Optional, Sequence, Union

from monarch._rust_bindings.monarch_hyperactor.bootstrap import (
    attach_to_workers as _attach_to_workers,
    start_worker_loop_forever as _native_start_worker_loop_forever,
    start_worker_loop_until_shutdown as _native_start_worker_loop_until_shutdown,
)
from monarch._rust_bindings.monarch_hyperactor.handle import Handle, WouldBlockRuntime
from monarch._rust_bindings.monarch_hyperactor.host_mesh import HostMesh as HyHostMesh
from monarch._rust_bindings.monarch_hyperactor.runtime import _is_in_tokio_runtime
from monarch._rust_bindings.monarch_hyperactor.shape import Extent
from monarch._src.actor.actor_mesh import _Lazy
from monarch._src.actor.future import Future
from monarch._src.actor.host_mesh import HostMesh

PrivateKey = Union[bytes, Path, None]
CA = Union[bytes, Path, Literal["trust_all_connections"]]


# Worker-process fork invariants:
#
# `fork()` copies a process's Python objects, locks and file descriptors into
# the child, but only the calling thread survives: actor event-loop threads and
# Tokio workers do not. Before `os.fork()` returns in the child, CPython's child
# prologue clears the vanished threads' states, which can run destructors, and
# runs `os.register_at_fork(after_in_child=...)` hooks. These rules apply to a
# process after it calls `start_worker_loop_forever`, `run_worker_loop_forever`
# or `run_worker_loop_until_shutdown`, to procs started by native
# `bootstrap_main()`, and to forks from actor code in any process. In a client
# process the CF-* rules in `actor_mesh.py` also apply.
#
# WF-1 (process-state-is-pid-owned): state created by `prepare_worker_loop()`,
#   native `bootstrap_main()` or a Monarch actor loop belongs to the PID that
#   started it.
# WF-2 (inherited-state-is-unusable): a child forked after WF-1 begins must not
#   drive or finalize inherited Monarch process state. An unenforced caller
#   obligation.
# WF-3 (fork-site-hard-boundary): the child never returns or suspends into
#   copied code: after `os.fork()` its branch runs to `os._exit()` or
#   `os.exec*()`. Before that it may make ordinary calls that do not touch
#   inherited state, including `asyncio.run()` on a fresh loop under asyncio's
#   default event-loop policy, whose after-fork hook resets the copied loop
#   state; a caller-installed policy is outside this. Importing a not-yet-loaded
#   module or logging can reach inherited locks, hooks or Monarch handlers.
#   `exec` closes only `CLOEXEC` descriptors, so an adopted worker listener can
#   stay open in the new image. An unenforced caller obligation, witnessed by
#   `test_worker_fork_contract.py`.
# WF-4 (parent-stays-usable): on the WF-3 path, Monarch-owned objects released by
#   the child prologue and Monarch's own child hooks leave the parent's actor-loop
#   wakeups, executor, Tokio reactor, transports and worker service usable.
#   Caller hooks that touch inherited state are outside this guarantee.
#   Witnessed by `test_worker_fork_contract.py`.
# WF-5 (no-recovery): nothing resets, reconstructs, takes over or cleans up
#   inherited worker state, and normal interpreter exit in the child is
#   unsupported.


def _validate_worker_loop_args(
    *,
    private_key: PrivateKey,
    ca: CA,
    address: str,
) -> None:
    if private_key is not None or ca != "trust_all_connections":
        raise NotImplementedError("TLS security plumbing")
    if "tcp://*" in address:
        raise NotImplementedError(
            "implementation does not get the host name right if it was specified as a wild card. We have to fix this"
        )


def _reject_blocking_worker_loop_in_tokio(operation: str) -> None:
    if _is_in_tokio_runtime():
        raise WouldBlockRuntime(
            f"{operation}() cannot block from within a Tokio runtime; "
            "invoke it from a synchronous context"
        )


def _start_worker_loop_forever(address: str) -> Future[None]:
    return Future._from_handle(_native_start_worker_loop_forever(address))


def start_worker_loop_forever(
    *,
    private_key: PrivateKey = None,
    ca: CA,
    address: str,
) -> Future[None]:
    """Start a Monarch server and return an observer of its lifetime.

    Starting the server is committed before this function returns, and the work
    continues if the returned Future is discarded. Call ``get()`` to block until
    it fails or shuts down.

    ``address`` accepts either of these string formats:

    - A ZMQ-style channel URL. This uses the legacy ``service`` proc identity.
    - A ProcAddr in ``<proc-id>@<channel-url>`` form. This uses the embedded
      proc identity. An explicit legacy address such as
      ``service@tcp://host:4444`` is equivalent to the bare channel URL.

    Channel URL examples:

        tcp://*:4444 - listen on tcp port (NYI, use tls on connection when ca="trust_all_connections")
        metatls://*:4444 - listen on tcp port use ssl encryption via metatls
        ipc://some_unique_string - unix sockets
        inproc://3423 - connection only accessible within the process

    A ProcAddr example:

        service<2MuAHeDjLCEd>@tcp://worker-fqdn:4444

    To bind to one interface but advertise a different one, append the bind
    address after ``@``:

        tcp://worker-fqdn:4444@tcp://0.0.0.0:4444

    The server binds to ``tcp://0.0.0.0:4444`` so any local interface can
    accept connections (e.g. ``localhost`` for ``kubectl port-forward``)
    while peers still address the worker by its routable FQDN. Without the
    ``@``-suffix, bind address equals advertised address. The ``@`` form is
    only for serving: after the worker starts, its host identity is the
    advertised address to the left of ``@``.

    The bind alias can also be used inside a ProcAddr:

        service<2MuAHeDjLCEd>@tcp://worker-fqdn:4444@tcp://0.0.0.0:4444

    The server will accept a connection to a new root client and enable it to
    use this machine as a host. If the client disconnects or cannot be contacted, this server
    kills all the current work and waits for a new connection.

    private_key is a tls private key file loaded as bytes used to establish secure connections.
    Things connecting to this machine must trust this private_key in the certificate authority file.

    ca is a certificate authority key file. This worker will only trust incoming connections with
    keys that are trusted by the ca.

    We can defer implementing authentication for a bit, but for any open source release, anyone
    is going to worry about worker machines opening unencrypted ports and waiting for connections
    on a service that evals python code, so we should just build it in.

    A child forked from this process after this call must not return or await
    into the copied event loop; it must end in ``os._exit()`` or
    ``os.exec*()``, and under asyncio's default event-loop policy may run a
    fresh loop with ``asyncio.run()`` first (WF-3 in
    ``monarch/_src/actor/bootstrap.py``).
    """
    _validate_worker_loop_args(private_key=private_key, ca=ca, address=address)
    return _start_worker_loop_forever(address)


def run_worker_loop_forever(
    *,
    private_key: PrivateKey = None,
    ca: CA,
    address: str,
) -> None:
    """Run a worker server until host shutdown terminates this process.

    Address and security arguments have the same meaning as in
    :func:`start_worker_loop_forever`. A child forked from this process after
    this call must not return or await into the copied event loop; it must end
    in ``os._exit()`` or ``os.exec*()``, and under asyncio's default event-loop
    policy may run a fresh loop with ``asyncio.run()`` first (WF-3).
    """
    _validate_worker_loop_args(private_key=private_key, ca=ca, address=address)
    _reject_blocking_worker_loop_in_tokio("run_worker_loop_forever")
    _start_worker_loop_forever(address).get()


def run_worker_loop_until_shutdown(
    *,
    private_key: PrivateKey = None,
    ca: CA,
    address: str,
) -> None:
    """Run a worker server until its owning ``HostMesh`` shuts it down.

    This variant returns after the host drain protocol completes instead of
    terminating the process. Address and security arguments have the same
    meaning as in :func:`run_worker_loop_forever`. A child forked from this
    process after this call must not return or await into the copied event
    loop; it must end in ``os._exit()`` or ``os.exec*()``, and under asyncio's
    default event-loop policy may run a fresh loop with ``asyncio.run()`` first
    (WF-3).
    """
    _validate_worker_loop_args(private_key=private_key, ca=ca, address=address)
    _reject_blocking_worker_loop_in_tokio("run_worker_loop_until_shutdown")
    _native_start_worker_loop_until_shutdown(address).get()


def attach_to_workers(
    *,
    private_key: PrivateKey = None,
    ca: CA,
    workers: Sequence[str | Future[str]],
    name: Optional[str] = None,
) -> HostMesh:
    """
    Create a host mesh that is connected to the list of workers
    (it starts a single dimensional mesh of 'workers' and can be reshaped).

    This returns the host mesh immediately, and allows the logic for the hosts to
    connect to happen asynchronously. A separate future such as `await mesh.initialized` is
    used to decide if we have successfully connected to all hosts. Workers may
    be strings or monarch.actor.Future objects that resolve to strings.

    Each worker string accepts either of these formats:

    - A ZMQ-style channel URL such as ``tcp://worker-fqdn:4444``. This uses the
      legacy ``service`` proc identity.
    - A ProcAddr in ``<proc-id>@<location>`` form, such as
      ``service<2MuAHeDjLCEd>@tcp://worker-fqdn:4444``. This preserves the
      embedded service proc identity and full location.

    If a worker string uses the alias form ``dial_to@bind_to``,
    ``attach_to_workers`` treats it as a reference to an already-serving
    worker. Dialing uses ``dial_to``; ``bind_to`` remains a serve-side detail.

    private_key is a tls private key file loaded as bytes used to establish secure connections.
    The workers must trust this private_key in their certificate authority file.

    ca is a certificate authority key file. This client will only trust workers with private_keys signed
    by the ca file.

    """

    if private_key is not None or ca != "trust_all_connections":
        raise NotImplementedError("TLS security plumbing")

    worker_handles = [w if isinstance(w, str) else w._as_handle() for w in workers]

    # Pass the ambient actor instance so the Rust side can push
    # client config to host agents during attach.
    from monarch._src.actor.actor_mesh import context

    instance = context().actor_instance._as_rust()
    host_mesh: Handle[HyHostMesh] = _attach_to_workers(
        instance, worker_handles, name=name
    )
    extent = Extent(["hosts"], [len(workers)])
    hm = HostMesh(
        host_mesh,
        extent.region,
        stream_logs=False,
        is_fake_in_process=False,
        code_sync_proc_mesh=None,
    )
    hm._code_sync_proc_mesh = _Lazy(lambda: hm.spawn_procs())
    return hm
