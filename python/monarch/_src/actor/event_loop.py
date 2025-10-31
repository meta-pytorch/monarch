# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""
Module for managing the event loop used by Monarch Python actors.
This provides a way to create a Python-aware thread from Rust that runs the worker event loop.
"""

import asyncio
import logging
import threading
from typing import Optional

from pyre_extensions import none_throws

logger: logging.Logger = logging.getLogger(__name__)

_event_loop: Optional[asyncio.AbstractEventLoop] = None
_lock = threading.Lock()
_ready = threading.Event()


def _initialize_event_loop() -> None:
    """
    Internal function to initialize the event loop.
    This creates a new thread with an event loop that runs forever.
    """
    global _event_loop, _ready
    if _event_loop is not None:
        return

    # Create a new thread that will host our event loop
    def event_loop_thread() -> None:
        """Target function for the event loop thread."""
        global _event_loop, _ready
        try:
            # Create a new event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            _event_loop = loop
            _ready.set()

            logger.debug(
                f"Python worker event loop thread started: {threading.current_thread().name}"
            )
            try:
                # Run the event loop forever
                loop.run_forever()
            finally:
                # Clean up when the loop stops
                logger.debug("Python worker event loop stopped, closing...")
                loop.close()
        except Exception as e:
            logger.error(f"Error in event loop thread: {e}")
            _ready.set()
            raise

    # Create and start the thread
    threading.Thread(
        target=event_loop_thread,
        name="asyncio-event-loop",
        daemon=True,  # Make it a daemon thread so it doesn't block program exit
    ).start()

    _ready.wait()  # Wait for the event loop to be ready

    if _event_loop is None:
        raise RuntimeError("Failed to initialize event loop")


def get_event_loop() -> asyncio.AbstractEventLoop:
    """
    Get the Python worker event loop.
    If no event loop is currently running, this will start a new one.

    Expected to be called from rust code.
    """
    global _event_loop
    if _event_loop is None:
        with _lock:
            _initialize_event_loop()
    return none_throws(_event_loop)


def stop_event_loop() -> None:
    """
    Stop the event loop gracefully.
    """
    global _event_loop
    if _event_loop is not None:
        logger.debug("Stopping event loop...")
        event_loop = none_throws(_event_loop)
        event_loop.call_soon_threadsafe(event_loop.stop)
