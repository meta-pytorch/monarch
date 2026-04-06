# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Optional

from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask

# This class is stubbed out in monarch._src.actor.actor_mesh, but
# it's useful to have this here for type-checking in other rust
# bindings.
class Instance:
    def stop_and_wait(
        self, reason: Optional[str] = None
    ) -> PythonTask[None]: ...
