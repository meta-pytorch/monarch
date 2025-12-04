# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
"""Python equivalents of CUDA event and stream management APIs."""

import torch


def create_cuda_event(
    enable_timing: bool = False,
    blocking: bool = False,
    interprocess: bool = False,
) -> torch.cuda.Event:
    """Create a new CUDA event.

    Args:
        enable_timing: If True, the event will measure time. If False, timing is disabled
                      for better performance.
        blocking: If True, the event will use blocking synchronization (cudaEventBlockingSync).
        interprocess: If True, the event can be shared between processes (cudaEventInterprocess).

    Returns:
        A new CUDA event with the specified configuration.
    """
    return torch.cuda.Event(
        enable_timing=enable_timing,
        blocking=blocking,
        interprocess=interprocess,
    )


def get_current_stream(device: int) -> torch.cuda.Stream:
    """Get the current CUDA stream for the specified device.

    Args:
        device: The device index.

    Returns:
        The current CUDA stream for the device.
    """
    return torch.cuda.current_stream(device)


def create_stream(device: int, priority: int = 0) -> torch.cuda.Stream:
    """Create a new CUDA stream for the specified device.

    Args:
        device: The device index.
        priority: Stream priority. Higher values indicate higher priority.
                 PyTorch uses priority values in the range [-1, 0], where
                 -1 is high priority and 0 is normal priority.

    Returns:
        A new CUDA stream for the device.
    """
    return torch.cuda.Stream(device=device, priority=priority)


def set_current_stream(stream: torch.cuda.Stream) -> None:
    """Set the current CUDA stream.

    This function will switch devices if the stream is on a different device
    than the current device.

    Args:
        stream: The stream to set as current.
    """
    current_device = torch.cuda.current_device()
    stream_device = stream.device_index

    if current_device != stream_device:
        torch.cuda.set_device(stream_device)

    torch.cuda.set_stream(stream)
