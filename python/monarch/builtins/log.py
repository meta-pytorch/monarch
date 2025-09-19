# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import logging

from monarch.common.remote import remote


logger = logging.getLogger(__name__)


@remote(propagate="inspect")
def log_remote(*args, level: int = logging.WARNING, **kwargs) -> None:
    logger.log(level, *args, **kwargs)


@remote(propagate="inspect")
def set_logging_level_remote(level: int) -> None:
    logger.setLevel(level)
