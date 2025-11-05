# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from pathlib import Path
from typing import List, Literal, Optional, Union

PrivateKey = Union[bytes, Path, None]
CA = Union[bytes, Path, Literal["trust_all_connections"]]

from monarch._rust_bindings.monarch_hyperactor.pytokio import PythonTask
from monarch._rust_bindings.monarch_hyperactor.v1.host_mesh import HostMesh

def bootstrap_main() -> None: ...
def run_worker_loop_forever(address: str) -> PythonTask[None]: ...
def attach_to_workers(
    workers: List[PythonTask[str]], name: Optional[str] = None
) -> PythonTask[HostMesh]: ...

class ChannelTx:
    """
    A channel transmitter for sending bytes messages over network connections.

    Supports TCP, Unix domain sockets, and in-process channels using ZMQ-style
    addressing (e.g., "tcp://hostname:port", "unix:/path", "inproc://port").
    """

    def send(self, message: bytes) -> PythonTask[None]:
        """
        Send a message on the channel and wait for delivery confirmation.

        Args:
            message: The bytes message to send.

        Returns:
            A PythonTask that completes when the message has been delivered to the
            remote end of the channel.

        Raises:
            RuntimeError: If the message fails to send.
        """
        ...

class ChannelRx:
    """
    A channel receiver for receiving bytes messages over network connections.

    Receives messages from multiple concurrent connections on a single address.
    """

    def recv(self) -> PythonTask[bytes]:
        """
        Receive the next message from the channel.

        Returns:
            A PythonTask that completes with the received message bytes.

        Raises:
            RuntimeError: If the channel is closed or an error occurs.
        """
        ...

def dial(address: str) -> ChannelTx:
    """
    Dial a channel address and return a transmitter for sending bytes.

    Establishes a connection to the specified address and returns a ChannelTx
    that can be used to send messages. The connection uses reliable delivery
    with automatic reconnection and retransmission.

    Args:
        address: The channel address in ZMQ-style URL format:
            - "tcp://hostname:port" - TCP connection to hostname:port
            - "unix:/path/to/socket" - Unix domain socket
            - "inproc://name" - In-process channel (local only)
            - "ipc://path" - IPC socket (equivalent to unix)
            - "metatls://hostname:port" - TLS connection (Meta internal)

    Returns:
        A ChannelTx instance for sending messages.

    Raises:
        RuntimeError: If the address format is invalid or connection fails.

    Example:
        >>> tx = dial("tcp://localhost:12345")
        >>> await tx.send(b"Hello, world!")
    """
    ...

def serve(address: str) -> ChannelRx:
    """
    Serve on a channel address and return a receiver for accepting connections.

    Starts listening on the specified address and returns a ChannelRx for
    receiving messages from multiple concurrent connections.

    Args:
        address: The channel address in ZMQ-style URL format:
            - "tcp://*:0" - Listen on any available port on all interfaces
            - "tcp://hostname:port" - Listen on specific hostname and port
            - "unix:/path/to/socket" - Unix domain socket
            - "inproc://name" - In-process channel (local only)
            - "ipc://path" - IPC socket (equivalent to unix)
            - "metatls://*:port" - TLS listener (Meta internal)

    Returns:
        A ChannelRx for receiving messages.

    Raises:
        RuntimeError: If the address format is invalid or binding fails.

    Example:
        >>> rx = serve("tcp://*:8080")
        >>> message = await rx.recv()
    """
    ...
