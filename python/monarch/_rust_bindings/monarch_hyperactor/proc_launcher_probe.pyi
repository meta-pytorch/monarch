# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import final

from monarch._rust_bindings.monarch_hyperactor.actor_mesh import PythonActorMesh
from monarch._rust_bindings.monarch_hyperactor.context import Instance
<<<<<<< dest:   864f4f1b28a4 - generatedunixname89002005337844: [Codemod][Rem...
||||||| base:   22e0cac56b6a - generatedunixname89002005279527: [Codemod][Ent...
from monarch._rust_bindings.monarch_hyperactor.mailbox import Mailbox
=======
from monarch._rust_bindings.monarch_hyperactor.mailbox import Mailbox
from monarch._rust_bindings.monarch_hyperactor.pickle import PicklingState
>>>>>>> source: f7dd4d49c103 pickle_stack - zdevito: [monarch] Refactor pickl...
from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask

@final
class ProbeReport:
    """Report describing what Rust received on the port."""

    @property
    def received_type(self) -> str:
        """High-level classification: 'PythonMessage' or 'Error'."""
        ...

    @property
    def kind(self) -> str | None:
        """If PythonMessage, the kind: 'Result', 'Exception', etc."""
        ...

    @property
    def rank(self) -> int | None:
        """If PythonMessage, the rank field from Result/Exception."""
        ...

    @property
    def payload_len(self) -> int:
        """Length of the message payload bytes."""
        ...

    @property
    def payload_bytes(self) -> list[int]:
        """Raw payload bytes."""
        ...

    @property
    def error(self) -> str | None:
        """Error message if something went wrong."""
        ...

def probe_exit_port_via_mesh(
    actor_mesh_inner: PythonActorMesh,
    instance: Instance,
    method_name: str,
    pickling_state: PicklingState,
) -> PythonTask[ProbeReport]:
    """Probe the wire format by calling an endpoint and receiving on a
    port."""
    ...
