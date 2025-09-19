# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import itertools
from typing import Any, Dict, Hashable, List, NamedTuple, Optional, Sequence, Tuple

import torch
from torch import autograd
from torch.utils._pytree import tree_flatten, TreeSpec


class AliasOf(NamedTuple):
    group: int  # 0 -this group, -1 - the parent, -2 - parent's parent, etc.
    offset: int


class Storage(NamedTuple):
    numel: int


# Hashable pattern for recreating tensors
# Each tensor either creates its own Storage
# or is an AliasOf another tensor either earlier in this list,
# or in one of the parent lists.
# parent lists are used to represent other collections of tensors
# for instance if this pattern is for outputs of a function
# parents might contains lists of inputs to the function and captured
# globals as two separate lists.
class TensorGroupPattern(NamedTuple):
    entries: Tuple["PatternEntry", ...]

    def empty(self, parents: List[List[torch.Tensor]]) -> List[torch.Tensor]:
        tensors = []
        for entry in self.entries:
            match entry.storage:
                case AliasOf(group=group, offset=offset):
                    base = tensors[offset] if group == 0 else parents[group][offset]
                case Storage(numel=numel):
                    base = torch.empty(
                        (numel,),
                        dtype=entry.dtype,
                        layout=entry.layout,
                        device=entry.device,
                    )
                case _:
                    raise ValueError("unexpected storage")
            t = torch.as_strided(base, entry.size, entry.stride, entry.storage_offset)
            tensors.append(t)
        return tensors


class PatternEntry(NamedTuple):
    size: Tuple[int, ...]
    stride: Tuple[int, ...]
    storage_offset: int
    dtype: torch.dtype
    layout: torch.layout
    device: torch.device
    storage: AliasOf | Storage


# Takes a list of tensors and computes the pattern of aliasing that
# would reconstruct the group. If `parent` is specified aliases
# are also computed with respect to that group and its parents.
# new storage is only specified is a tensor's storage was not
# seen in any parent or previously in a group.
class TensorGroup:
    def __init__(
        self,
        tensors: Sequence[torch.Tensor],
        parent: Optional["TensorGroup"] = None,
    ):
        self.parent = parent
        self.tensors = tensors
        self.storage_dict: Dict[torch.UntypedStorage, int] = {}

        def create_entry(i: int, t: torch.Tensor):
            storage = t.untyped_storage()
            numel = t.untyped_storage().size() // t.element_size()
            alias = self._find_alias(storage)
            if alias is None:
                self.storage_dict[storage] = i
                alias = Storage(numel)

            return PatternEntry(
                tuple(t.size()),
                tuple(t.stride()),
                int(t.storage_offset()),
                t.dtype,
                t.layout,
                t.device,
                alias,
            )

        self.pattern = TensorGroupPattern(
            tuple(create_entry(i, t) for i, t in enumerate(tensors))
        )

    def _find_alias(self, storage: torch.UntypedStorage) -> Optional[AliasOf]:
        grp = self
        for i in itertools.count():
            if storage in grp.storage_dict:
                return AliasOf(-i, grp.storage_dict[storage])
            if grp.parent is None:
                return None
            grp = grp.parent


class TensorPlaceholder:
    pass


# singleton to represent where tensors go in a pytree
tensor_placeholder = TensorPlaceholder()


def _to_placeholder(x):
    if isinstance(x, torch.Tensor):
        return tensor_placeholder
    return x


def _remove_ctx(x):
    if isinstance(x, autograd.function.FunctionCtx):
        return None
    return x


# customizable set of filters to handle data types that appear
# in functions that one wants to support in cached functions
key_filters = [_to_placeholder, _remove_ctx]


def _filter_key(v: Any):
    for filter in key_filters:
        v = filter(v)
    return v


class HashableTreeSpec(NamedTuple):
    type: Any
    context: Any
    children_specs: Tuple["HashableTreeSpec", ...]

    @staticmethod
    def from_treespec(t: "TreeSpec"):
        return HashableTreeSpec(
            t.type,
            tuple(t.context) if isinstance(t.context, list) else t.context,
            tuple(HashableTreeSpec.from_treespec(child) for child in t.children_specs),
        )


def hashable_tensor_flatten(args, kwargs) -> Tuple[List[torch.Tensor], Hashable]:
    values, spec = tree_flatten((args, kwargs))
    tensors = [t for t in values if isinstance(t, torch.Tensor)]
    key: Hashable = (
        tuple(_filter_key(v) for v in values),
        HashableTreeSpec.from_treespec(spec),
    )
    return tensors, key
