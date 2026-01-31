# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import final

from monarch._rust_bindings.monarch_hyperactor.actor_mesh import PythonActorMesh
from monarch._rust_bindings.monarch_hyperactor.context import Instance
from monarch._rust_bindings.monarch_hyperactor.mailbox import Mailbox
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
    def pending_pickle_state_present(self) -> bool | None:
        """If PythonMessage, whether pending_pickle_state was present."""
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
    mailbox: Mailbox,
    method_name: str,
    pickled_args: bytes,
) -> PythonTask[ProbeReport]:
    """Probe the wire format by calling an endpoint and receiving on a
    port."""
    ...
