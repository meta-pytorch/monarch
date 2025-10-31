# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import asyncio
import sys
from abc import abstractmethod


class DebugIO:
    @abstractmethod
    async def input(self, prompt: str = "") -> str: ...

    @abstractmethod
    async def output(self, msg: str) -> None: ...

    @abstractmethod
    async def quit(self) -> None: ...


class DebugStdIO(DebugIO):
    async def input(self, prompt: str = "") -> str:
        return await asyncio.to_thread(input, prompt)

    async def output(self, msg: str) -> None:
        sys.stdout.write(msg)
        sys.stdout.flush()

    async def quit(self) -> None:
        pass


class DebugIOError(RuntimeError):
    def __init__(self) -> None:
        super().__init__("Error encountered during debugger I/O operation.")


class DebugCliIO(DebugIO):
    def __init__(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self._reader = reader
        self._writer = writer

    async def input(self, prompt: str = "") -> str:
        try:
            await self.output(prompt)
            msg = (await self._reader.readline()).decode()
            # Incomplete read due to EOF
            if not msg.endswith("\n"):
                raise RuntimeError("Unexpected end of input.")
            # Strip the newline to be consistent with the behavior of input()
            return msg.strip("\n")
        except Exception as e:
            raise DebugIOError() from e

    async def output(self, msg: str) -> None:
        try:
            self._writer.write(msg.encode())
            await self._writer.drain()
        except Exception as e:
            raise DebugIOError() from e

    async def quit(self) -> None:
        await self.output("Quitting debug session...\n")
        self._writer.close()
        await self._writer.wait_closed()
