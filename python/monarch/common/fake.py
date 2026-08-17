# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
from concurrent.futures import ThreadPoolExecutor
from functools import cache

from torch._subclasses.fake_tensor import FakeTensorMode


@cache
def _fake_mode_worker():
    return ThreadPoolExecutor(max_workers=1)


@cache
def _fake_mode():
    return FakeTensorMode()


def fake_call(fn, *args, **kwargs):
    """Execute on work on a ThreadPool worker

    First call (ThreadPoolExecutor init) will take the GIL and may block for long time!
    TODO: this will be replaced with something more performant
    """
    global _fake_mode_worker, fake_mode

    # # Calls FakeTensorMode while re-enabling version counter tracking
    # # todo(chilli): I'm not totally sure why I need to disable python dispatch
    # # key. Perhaps there's some unwrapping that should have happened further up.
    # include_to_set = torch._C._dispatch_tls_local_include_set()
    # exclude_to_set = (
    #     torch._C._dispatch_tls_local_exclude_set()
    #     | torch._C.DispatchKeySet(torch._C.DispatchKey.Python)
    # ) - torch._C.DispatchKeySet(torch._C.DispatchKey.ADInplaceOrView)

    # def work():
    #     with torch._C._ForceDispatchKeyGuard(include_to_set, exclude_to_set):
    #         with fake_mode:
    #             return fn(*args, **kwargs)

    # return work()

    def work():
        # fake mode must be initialized in the worker thread
        # otherwise a monarch dispatch mode may be active, causing
        # FakeTensorMode to initialize wrong.
        with _fake_mode():
            return fn(*args, **kwargs)

    return _fake_mode_worker().submit(work).result()
