# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
from torch.utils._pytree import (
    _get_node_type,
    SUPPORTED_NODES,
    tree_flatten,
    tree_map,
    tree_unflatten,
)


def flatten(tree, cond):
    r, spec = tree_flatten(tree)

    # be careful to not capture values we return in
    # 'trues'. We do not need them to reconstruct and do not want to
    # extend their lifetime.
    trues = []
    falses = []
    conds = []
    for e in r:
        c = cond(e)
        (trues if c else falses).append(e)
        conds.append(c)

    def unflatten(n):
        n_it = iter(n)
        falses_it = iter(falses)
        return tree_unflatten([next(n_it if c else falses_it) for c in conds], spec)

    return trues, unflatten


def flattener(tree, cond=None):
    """
    Produce a _traceable_ flattener routine from tree. That is, it produces code that can
    flatten another object shaped the same as tree, but whose structure cannot
    be introspected because it might be (e.g.) an fx proxy value.
    """
    if isinstance(tree, (tuple, list)):
        flattens = [flattener(t, cond) for t in tree]
        return lambda obj: [
            f for i, flatten in enumerate(flattens) for f in flatten(obj[i])
        ]
    elif isinstance(tree, dict):
        keys = tuple(tree.keys())
        flattens = [flattener(t, cond) for t in tree.values()]
        return lambda obj: [
            f for k, flatten in zip(keys, flattens) for f in flatten(obj[k])
        ]
    elif _get_node_type(tree) in SUPPORTED_NODES:
        flatten_fn = SUPPORTED_NODES[_get_node_type(tree)].flatten_fn
        trees, _ = flatten_fn(tree)
        flattens = [flattener(t, cond) for t in trees]

        def the_flattener(obj):
            trees, _ = flatten_fn(obj)
            return [f for i, flatten in enumerate(flattens) for f in flatten(trees[i])]

        return the_flattener
    elif cond is None or cond(tree):
        return lambda obj: [obj]
    else:
        return lambda obj: []
