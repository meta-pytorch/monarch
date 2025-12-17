# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import final

class FrozenBuffer:
    """
    An immutable buffer for reading bytes data.

    The `FrozenBuffer` struct provides a read-only interface to byte data. Once created,
    the buffer's content cannot be modified, but it supports various reading operations
    including line-by-line reading and copying data to external buffers. It implements
    Python's buffer protocol for zero-copy access from Python code.

    Examples:
        ```python
        from monarch._rust_bindings.monarch_hyperactor.buffers import Buffer

        # Create and populate a buffer
        buffer = Buffer()
        buffer.write(b"Hello\\nWorld\\n")

        # Freeze it for reading
        frozen = buffer.freeze()

        # Read all content
        content = frozen.read()
        print(content)  # b"Hello\\nWorld\\n"

        # Read line by line (create a new frozen buffer)
        buffer.write(b"Line 1\\nLine 2\\n")
        frozen = buffer.freeze()
        line1 = frozen.readline()
        line2 = frozen.readline()
        ```
    """

    def read(self, size: int = -1) -> bytes:
        """
        Read bytes from the buffer.

        Advances the internal read position by the number of bytes read.
        This is a consuming operation - once bytes are read, they cannot be read again.

        Arguments:
        - `size`: Number of bytes to read. If -1 or not provided, reads all remaining bytes

        Returns:
        A PyBytes object containing the bytes read from the buffer
        """
        ...

    def __len__(self) -> int:
        """
        Return the number of bytes remaining in the buffer.

        Returns:
        The number of bytes that can still be read from the buffer
        """
        ...

    def __str__(self) -> str:
        """
        Return a string representation of the buffer content.

        This method provides a debug representation of the remaining bytes in the buffer.

        Returns:
        A string showing the bytes remaining in the buffer
        """
        ...

    def readline(self, size: int = -1) -> bytes:
        """
        Read a line from the buffer up to a newline character.

        Searches for the first newline character ('\\n') within the specified size limit
        and returns all bytes up to and including that character. If no newline is found
        within the limit, returns up to `size` bytes. Advances the read position by the
        number of bytes read.

        Arguments:
        - `size`: Maximum number of bytes to read. If -1 or not provided, searches through all remaining bytes

        Returns:
        A PyBytes object containing the line data (including the newline character if found)
        """
        ...

    def readinto(self, b: bytearray) -> int:
        """
        Read bytes from the buffer into an existing buffer-like object.

        This method implements efficient copying of data from the FrozenBuffer into
        any Python object that supports the buffer protocol (like bytearray, memoryview, etc.).
        The number of bytes copied is limited by either the remaining bytes in this buffer
        or the capacity of the destination buffer, whichever is smaller.

        Arguments:
        - `b`: Any Python object that supports the buffer protocol for writing

        Returns:
        The number of bytes actually copied into the destination buffer
        """
        ...

    def __buffer__(self, flags: int, /) -> memoryview[bytes]:
        """
        Return a memoryview exposing the buffer's contents according to the buffer protocol.

        This method allows zero-copy access to the underlying bytes data from Python code
        and enables interoperability with objects and APIs that support the buffer protocol.
        The returned memoryview is read-only and reflects the current unread contents of the buffer.

        Arguments:
        - `flags`: Flags passed by the buffer protocol (typically ignored in Python implementations)

        Returns:
        A read-only memoryview of the buffer's bytes
        """
        ...

@final
class Buffer:
    """
    A mutable buffer for reading and writing bytes data.

    The `Buffer` struct provides an interface for accumulating byte data from Python `bytes` objects
    that can be converted into a `Part` for zero-copy multipart message serialization.
    It accumulates references to Python bytes objects without copying.

    Examples:
        ```python
        from monarch._rust_bindings.monarch_hyperactor.buffers import Buffer

        # Create a new buffer
        buffer = Buffer()

        # Write some data
        data = b"Hello, World!"
        bytes_written = buffer.write(data)

        # Use in multipart serialization
        # The buffer accumulates multiple writes as separate fragments
        ```
    """

    def __init__(self) -> None:
        """
        Create a new empty buffer.
        """
        ...

    def write(self, buff: bytes) -> int:
        """
        Write bytes data to the buffer.

        This keeps a reference to the Python bytes object without copying.

        Arguments:
        - `buff`: The bytes object to write to the buffer

        Returns:
        The number of bytes written (always equal to the length of input bytes)
        """
        ...

    def __len__(self) -> int:
        """
        Return the total number of bytes in the buffer.

        This iterates over all accumulated PyBytes fragments and sums their lengths.

        Returns:
        The total number of bytes stored in the buffer
        """
        ...

    def freeze(self) -> FrozenBuffer:
        """
        Freeze this buffer into an immutable `FrozenBuffer`.

        This operation consumes the mutable buffer's contents, transferring ownership
        to a new `FrozenBuffer` that can only be read from. The original buffer
        becomes empty after this operation.

        This operation should generally be avoided in hot paths as it creates copies in order to concatenate
        bytes that are potentially fragmented in memory into a single contiguous series of bytes

        Returns:
        A new `FrozenBuffer` containing all the data that was in this buffer
        """
        ...
