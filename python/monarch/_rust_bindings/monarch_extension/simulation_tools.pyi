# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

async def sleep(seconds: float) -> None:
    """
    Asyncio friendly sleep that waits for the simulator event loop to wake up
    """
    ...

async def start_event_loop() -> None:
    """
    Starts the simulator event loop
    """
    ...

def is_simulator_active() -> bool:
    """
    Check if the simulation network is currently active.

    Returns True if SimNet has been started (typically after calling
    start_event_loop()), False otherwise.
    """
    ...
