# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import logging
import sys

from monarch._rust_bindings.monarch_hyperactor.supervision import MeshFailure

_logger: logging.Logger = logging.getLogger(__name__)


def unhandled_fault_hook(failure: MeshFailure) -> None:
    """When a supervision event is unhandled and is propagated back to the client,
    this hook is called.
    The default implementation is to exit the process after logging
    the event.
    Assign to this function to change the behavior.
    Single argument is the SupervisionEvent
    """

    _logger.error(f"Unhandled monarch error on the root actor: {failure.report()}")
    sys.exit(1)
