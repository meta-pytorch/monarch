# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from pathlib import Path
from typing import List, Literal, Optional, Union

PrivateKey = Union[bytes, Path, None]
CA = Union[bytes, Path, Literal["trust_all_connections"]]

from monarch._rust_bindings.monarch_hyperactor.context import Instance
from monarch._rust_bindings.monarch_hyperactor.handle import Handle
from monarch._rust_bindings.monarch_hyperactor.host_mesh import HostMesh

def bootstrap_main() -> int:
    """Bootstrap a proc. Returns an exit code that should be used to exit the process
    with sys.exit()."""
    ...

def start_worker_loop_forever(address: str) -> Handle[None]: ...
def start_worker_loop_until_shutdown(address: str) -> Handle[None]: ...
def attach_to_workers(
    instance: Instance,
    workers: List[str | Handle[str]],
    name: Optional[str] = None,
) -> Handle[HostMesh]:
    """Create a host mesh by connecting to existing Monarch worker servers.

    Worker addresses may be strings or Handles that supply strings later.
    The returned Handle supplies the native HostMesh once attachment finishes.
    This does not start the worker servers or spawn user procs.
    """
    ...
