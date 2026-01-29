# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask

# This class is stubbed out in monarch._src.actor.actor_mesh, but
# it's useful to have this here for type-checking in other rust
# bindings.
class Instance: ...

def shutdown_client() -> PythonTask[None]:
    """
    Provides a future that when completed will stop the client actor. After this
    point, any usage of the client actor will re-initialize it.
    """
    ...
