# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import traceback
import warnings
from typing import List, Optional, TYPE_CHECKING
from weakref import ref, WeakSet

from . import messages

if TYPE_CHECKING:
    from .device_mesh import DeviceMesh
    from .tensor import Tensor


# all the aliases for the same storage on a particular stream
# borrows of a storage to another stream are not considered aliases
# but instead copies and will have a different set of storage aliases.
# conceptually, think of borrows as copies that have been guarded
# so that we do not actually perform the data movement.
class StorageAliases:
    def __init__(self):
        # how are we allowed to access this storage
        # string containing 0 or more of:
        # r - can read
        # w - can write
        self.access = "rw"
        # what Tensor aliases exist for this storage
        self.aliases = WeakSet()

        # was this set of storages originally a borrow
        # from another stream?
        self._borrow: Optional[ref[Borrow]] = None
        self.borrowed_from: "Optional[StorageAliases]" = None
        # how many times has this storage been borrowed?
        self.live_borrows = WeakSet()

    @property
    def borrow(self) -> "Borrow":
        assert self._borrow is not None
        borrow = self._borrow()
        assert borrow is not None
        return borrow

    def register(self, tensor: "Tensor"):
        self.aliases.add(tensor)
        if self.borrowed_from is not None:
            self.borrow._live_tensors += 1

    def unregister(self, tensor: "Tensor"):
        borrowed_from = self.borrowed_from
        if borrowed_from is not None:
            borrow = self.borrow
            borrow._live_tensors -= 1
            if borrow._live_tensors == 0:
                borrow._use()
                if self.access == "rw":
                    # returning a mutable borrow needs to propagate errors
                    # from the stream (which may have mutated the value) back to the values
                    # on the origin stream. This does not happen automatically because
                    # borrows are not tracked as tensor aliases, but are instead treated
                    # as a kind of optimized copy or move.
                    tensor.mesh.client.new_node(borrowed_from.aliases, (tensor,))
                tensor.mesh._send(messages.BorrowLastUse(borrow._id))

    def borrow_from(
        self, id: int, mesh: "DeviceMesh", f: "StorageAliases", mutable: bool
    ):
        assert self.borrowed_from is None, (
            "we should have created a new storage with no borrows"
        )
        if mutable:
            if "w" not in f.access:
                raise RuntimeError(
                    "Cannot borrow this tensor mutably because it (or a view) is already being borrowed non-mutably."
                )
            f.access = ""
            self.access = "rw"
        else:
            f.access = self.access = "r"
        self.borrowed_from = f
        borrow = Borrow(id, self, mesh)
        f.live_borrows.add(borrow)
        self._borrow = ref(borrow)
        return borrow


class Borrow:
    def __init__(self, id: int, aliases: StorageAliases, mesh: "DeviceMesh"):
        self._storage_aliases = aliases
        self._mesh = mesh
        self._id = id
        self._live_tensors = 1
        self._dropped = False
        self._used = False
        self._frames: List[traceback.FrameSummary] = traceback.extract_stack()

    @property
    def traceback_string(self):
        return "".join(traceback.format_list(self._frames))

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        self.drop()

    def _use(self):
        if self._used:
            return
        self._used = True
        self._mesh._send(messages.BorrowFirstUse(self._id))

    def drop(self) -> None:
        if self._dropped:
            return
        self._dropped = True

        for alias in self._storage_aliases.aliases:
            alias._drop_ref()

        self._mesh.client.drop_borrow(self)
        self._mesh._send(messages.BorrowDrop(self._id))
        f = self._storage_aliases.borrowed_from
        assert f is not None
        f.live_borrows.remove(self)
        if len(f.live_borrows) == 0:
            f.access = "rw" if f.borrowed_from is None else self._storage_aliases.access

    def __del__(self):
        if not self._dropped:
            current = "".join(traceback.format_stack())
            warnings.warn(
                "borrow.drop() must be called before a borrowed tensor is freed to specify when the borrowed tensor should return to its origin stream, but borrow is being deleted before drop."
                "borrow.drop() is being called automatically here to ensure correctness, but this will force a synchronization back to the original stream at this point which might not be intended."
                f"\nTraceback of __del__(most recent call last):\n{current}\nTraceback of original borrow (most recent call last):{self.traceback_string}",
                stacklevel=2,
            )
            self.drop()
