# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


from pathlib import Path
from typing import List, Literal, Optional, Union

from monarch._rust_bindings.monarch_hyperactor.bootstrap import (
    attach_to_workers as _attach_to_workers,
    run_worker_loop_forever as _run_worker_loop_forever,
)
from monarch._rust_bindings.monarch_hyperactor.host_mesh import HostMesh as HyHostMesh
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask
from monarch._rust_bindings.monarch_hyperactor.shape import Extent
from monarch._src.actor.actor_mesh import _Lazy
from monarch._src.actor.future import _Unawaited, Future
from monarch._src.actor.host_mesh import HostMesh

PrivateKey = Union[bytes, Path, None]
CA = Union[bytes, Path, Literal["trust_all_connections"]]


def _as_python_task(s: str | Future[str]) -> "PythonTask[str]":
    if isinstance(s, str):
        s_str: str = s

        async def just() -> str:
            return s_str

        return PythonTask.from_coroutine(just())
    else:
        assert isinstance(s._status, _Unawaited)
        return s._status.coro


def run_worker_loop_forever(
    *, private_key: PrivateKey = None, ca: CA, address: str
) -> None:
    """
    Start a monarch server at "address" capable of letting this machine participate in
    a monarch process.

    address is a zmq-style specifier for the place where the server will listen to connections, examples:

        tcp://*:4444 - listen on tcp port (NYI, use tls on connection when ca="trust_all_connections")
        metatls://*:4444 - listen on tcp port use ssl encryption via metatls
        ipc://some_unique_string - unix sockets
        inproc://3423 - connection only accessible within the process


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
    """
    if private_key is not None or ca != "trust_all_connections":
        raise NotImplementedError("TLS security plumbing")
    # we maybe want to actually return the future and let you do other stuff,
    # not sure ...
    if "tcp://*" in address:
        raise NotImplementedError(
            "implementation does not get the host name right if it was specified as a wild card. We have to fix this"
        )

    _run_worker_loop_forever(address).block_on()


def attach_to_workers(
    *,
    private_key: PrivateKey = None,
    ca: CA,
    workers: List[str | Future[str]],
    name: Optional[str] = None,
) -> HostMesh:
    """
    Create a host mesh that is connected to the list of workers
    (it starts a single dimensional mesh of 'workers' and can be reshaped).

    This returns the host mesh immediately, and allows the logic for the hosts to
    connect to happen asynchronously. A separate future such as `await mesh.initialized` is
    used to decide if we have successfully connected to all hosts. Workers are specified with the same zmq-style
    specifier strings described in `run_worker_loop_forever`. They may be specified as monarch.actor.Future objects
    so that an implementation can do some asynchronous work to discover where to connect.


    private_key is a tls private key file loaded as bytes used to establish secure connections.
    The workers must trust this private_key in their certificate authority file.

    ca is a certificate authority key file. This client will only trust workers with private_keys signed
    by the ca file.

    """

    if private_key is not None or ca != "trust_all_connections":
        raise NotImplementedError("TLS security plumbing")

    workers_tasks = [_as_python_task(w) for w in workers]
    host_mesh: PythonTask[HyHostMesh] = _attach_to_workers(workers_tasks, name=name)
    extent = Extent(["hosts"], [len(workers)])
    hm = HostMesh(
        host_mesh.spawn(),
        extent.region,
        stream_logs=False,
        is_fake_in_process=False,
        _code_sync_proc_mesh=None,
    )
    hm._code_sync_proc_mesh = _Lazy(lambda: hm.spawn_procs())
    return hm
