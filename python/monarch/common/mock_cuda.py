# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import logging
from contextlib import contextmanager
from typing import Generator, Optional

import monarch.common._C  # @manual=//monarch/python/monarch/common:_C
import torch

monarch.common._C.patch_cuda()

_mock_cuda_stream: Optional[torch.cuda.Stream] = None

logger: logging.Logger = logging.getLogger(__name__)


def _mock_init_test() -> None:
    global _mock_cuda_stream
    base_mock_address = 1 << 48
    with torch.cuda.stream(_mock_cuda_stream):
        monarch.common._C.mock_cuda()
        x = torch.rand(4, dtype=torch.float32, device="cuda")
        monarch.common._C.unmock_cuda()
    # x will result in a small pool (2MB) caching allocator allocation
    segment_size = 2 * 1024 * 1024
    # therefore we expect the address of x's allocation to be...
    expected_address = base_mock_address - segment_size
    assert (
        x.untyped_storage().data_ptr() == expected_address
    ), "monarch mock initialization failed. please import mock_cuda at the top of your imports"
    logger.info("monarch mock initialization succeeded")


def get_mock_cuda_stream() -> torch.cuda.Stream:
    global _mock_cuda_stream
    if _mock_cuda_stream is None:
        _mock_cuda_stream = torch.cuda.Stream()
        _mock_init_test()
    assert _mock_cuda_stream is not None
    return _mock_cuda_stream


@contextmanager
def mock_cuda_guard() -> Generator[None, None, None]:
    try:
        with torch.cuda.stream(get_mock_cuda_stream()):
            monarch.common._C.mock_cuda()
            yield
    finally:
        monarch.common._C.unmock_cuda()


def mock_cuda() -> None:
    monarch.common._C.mock_cuda()


def unmock_cuda() -> None:
    monarch.common._C.unmock_cuda()
