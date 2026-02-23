# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
from typing import Optional

from monarch._rust_bindings.monarch_extension.tensor_worker import Ref
from monarch._rust_bindings.monarch_hyperactor.pickle import (
    pop_tensor_engine_reference,
    push_tensor_engine_reference_if_active,
)


class Referenceable:
    def __init__(self):
        self.ref: Optional[int] = None

    def delete_ref(self, ref):
        raise NotImplementedError("no delete_ref method")

    def __reduce_ex__(self, protocol):
        assert self.ref is not None, (
            f"{self} is being sent but does not have a reference"
        )
        if push_tensor_engine_reference_if_active(self):
            return pop_tensor_engine_reference, ()
        return Ref, (self.ref,)

    # Used by rust backend to get the ref for this object
    def __monarch_ref__(self) -> int:
        assert self.ref is not None
        return self.ref

    def __del__(self):
        if self.ref is not None:
            self.delete_ref(self.ref)
