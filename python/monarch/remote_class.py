# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import importlib
import itertools
from typing import Any, Dict

from monarch.common import device_mesh
from monarch.common.remote import remote


class ControllerRemoteClass:
    """
    This class simplifies the creation and management of remote classes.  It serves as
    the controller side of a remote class architecture.  Classes that are intended to be
    controlled remotely should inherit from this class. The constructor of the inheriting
    class must invoke `super().__init__()` with the path to the remote class that will be
    used on the worker nodes.  Methods that are intended for remote execution must be
    decorated with `ControllerRemoteClass.remote_method`.

    Note: This class is designed for use by the controller developer only and should
    not be directly used in model code.

    Example usage:

    class ControllerMyClass(ControllerRemoteClass):
        def __init__(self, *args, **kwargs) -> None:
            super().__init__("my_package.my_class", *args, **kwargs)

        @ControllerRemoteClass.remote_method
        def some_method(self, *args, **kwargs) -> None:
            # This method is intended for remote execution and does nothing locally.
            pass
    """

    _counter = itertools.count()

    def __init__(self, cls_path: str, *args, **kwargs) -> None:
        self.ident = next(ControllerRemoteClass._counter)
        self.cls_path = cls_path
        self.mesh = device_mesh._active
        _controller_remote_class_method(
            cls_path, "remote_init", self.ident, *args, **kwargs
        )

    def __del__(self) -> None:
        mesh = getattr(self, "mesh", None)
        if mesh is not None and not mesh.client._shutdown:
            with self.mesh.activate():
                _controller_remote_class_method(
                    self.cls_path,
                    "remote_del",
                    self.ident,
                )

    @staticmethod
    def remote_method(fn):
        def wrapper(self, *args, **kwargs) -> None:
            _controller_remote_class_method(
                self.cls_path, "remote_method", self.ident, fn.__name__, *args, **kwargs
            )

        return wrapper


# Add the logic as a separate private function instead of adding ita to
# ResolvableFunctionFromPath. This avoids users to using this directly.
_controller_remote_class_method = remote(
    "monarch.remote_class._remote_class_method", propagate="inspect"
)


def _remote_class_method(cls_path: str, method_name: str, *args, **kwargs) -> None:
    modulename, classname = cls_path.rsplit(".", 1)
    module = importlib.import_module(modulename)
    cls = getattr(module, classname)
    method = getattr(cls, method_name)
    method(*args, **kwargs)


class WorkerRemoteClass:
    """
    This class is designed to be used alongside ``ControllerRemoteClass`` and represents
    the worker-side of a remote class architecture. Instances of this class should just
    mimic standard Python classes, with the notable exception that all methods must
    return None -- the current RemoteClass architecture does not support methods that
    return values.

    The `ident` attribute is used for tracking object instances created via `remote_init`.
    This tracking is necessary because the remote function would otherwise lose the
    reference to the object.
    """

    _objects: Dict[int, Any] = {}

    @classmethod
    def remote_init(cls, ident: int, *args, **kwargs) -> None:
        WorkerRemoteClass._objects[ident] = cls(*args, **kwargs)

    @classmethod
    def remote_del(cls, ident) -> None:
        WorkerRemoteClass._objects.pop(ident)

    @classmethod
    def remote_method(cls, ident: int, method_name, *args, **kwargs) -> None:
        instance = WorkerRemoteClass._objects[ident]
        assert cls == instance.__class__, (
            "Mismatched class type {cls} {instance.__class__}"
        )
        getattr(instance, method_name)(*args, **kwargs)
