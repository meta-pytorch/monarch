# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Optional

# This class is stubbed out in monarch._src.actor.actor_mesh, but
# it's useful to have this here for type-checking in other rust
# bindings.
class Instance:
    def _inject_failure(self, reason: Optional[str] = None) -> None: ...
