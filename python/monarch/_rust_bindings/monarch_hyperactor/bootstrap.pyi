# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from pathlib import Path
from typing import List, Literal, Optional, Union

PrivateKey = Union[bytes, Path, None]
CA = Union[bytes, Path, Literal["trust_all_connections"]]

from monarch._rust_bindings.monarch_hyperactor.host_mesh import HostMesh
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask

def bootstrap_main() -> None: ...
def run_worker_loop_forever(address: str) -> PythonTask[None]: ...
def attach_to_workers(
    workers: List[PythonTask[str]], name: Optional[str] = None
) -> PythonTask[HostMesh]: ...
