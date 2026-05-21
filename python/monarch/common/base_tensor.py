# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import torch


# All of the tensor examples in this zoo inherit from BaseTensor.  Ideally,
# however, they would inherit directly from Tensor.  This is just our staging
# ground for applying behavior that hasn't yet made it into core but that
# we would like to apply by default.
class BaseTensor(torch.Tensor):
    # See https://github.com/pytorch/pytorch/pull/73727 ; this is necessary
    # to ensure that super().__new__ can cooperate with each other
    @staticmethod
    def __new__(cls, elem, *, requires_grad=None):
        if requires_grad is None:
            return super().__new__(cls, elem)
        else:
            return cls._make_subclass(cls, elem, requires_grad)

    # If __torch_dispatch__ is defined (which it will be for all our examples)
    # the default torch function implementation (which preserves subclasses)
    # typically must be disabled
    # pyrefly: ignore [bad-override]
    __torch_function__ = torch._C._disabled_torch_function_impl
