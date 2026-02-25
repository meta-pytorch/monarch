# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import abc
import importlib
from typing import cast, Generic, Type, TypeVar


T = TypeVar("T")

_SKIP_BASES: frozenset[Type[object]] = frozenset({object, abc.ABC, Generic})


class PatchRustClass:
    def __init__(self, rust_class: Type[T]) -> None:
        self.rust_class = rust_class

    def __call__(self, python_class: Type[T]) -> Type[T]:
        rust_name = f"{self.rust_class.__module__}.{self.rust_class.__name__}"
        python_name = f"{python_class.__module__}.{python_class.__name__}"
        if rust_name != python_name:
            raise ValueError(f"mismatched type names {rust_name} != {python_name}")
        for name, implementation in python_class.__dict__.items():
            if self._should_patch(name, implementation):
                setattr(self.rust_class, name, implementation)

        # Patch in methods from mixins. We enforce that mixins must be ABC
        for base in python_class.__bases__:
            if base in _SKIP_BASES:
                continue
            if not isinstance(base, abc.ABCMeta):
                raise TypeError(
                    f"Mixin {base.__name__} must inherit from ABC to be used "
                    f"with @rust_struct (isinstance() checks won't work otherwise)."
                )
            for name, implementation in base.__dict__.items():
                if self._should_patch(name, implementation):
                    setattr(self.rust_class, name, implementation)
            base.register(self.rust_class)

        # If the Python class inherited from Generic, make the Rust class
        # subscriptable so that ValueMesh[T] works at runtime.
        if hasattr(python_class, "__class_getitem__") and not hasattr(
            self.rust_class, "__class_getitem__"
        ):
            self.rust_class.__class_getitem__ = classmethod(lambda cls, params: cls)

        return cast(Type[T], self.rust_class)

    def _should_patch(self, name: str, implementation: object) -> bool:
        if getattr(implementation, "__isabstractmethod__", False):
            return False
        if hasattr(self.rust_class, name):
            the_attr = getattr(self.rust_class, name)
            is_object_default = name.startswith("__") and getattr(
                the_attr, "__qualname__", ""
            ).startswith("object.")
            if not is_object_default:
                # do not patch in the stub methods that
                # are already defined by the rust implementation
                return False
        if not callable(implementation) and not isinstance(implementation, property):
            return False
        return True


def rust_struct(name: str) -> PatchRustClass:
    """
    When we bind a rust struct into Python, it is sometimes faster to implement
    parts of the desired Python API in Python. It is also easier to understand
    what the class does in terms of these methods.

    We also want to avoid having to wrap rust objects in another layer of python objects
    because:
    * wrappers double the python overhead
    * it is easy to confuse which level of wrappers and API takes, especially
      along the python<->rust boundary.


    To avoid wrappers we first define the class in pyo3. Lets say we add a class
    monarch_hyperactor::actor_mesh::TestClass which we will want to extend with python methods in
    the monarch/actor/_src/actor_mesh.py. In rust we will define the class as

         #[pyclass(name = "TestClass", module = "monarch._src.actor_mesh")]
         struct TestClass {}
         #[pymethods]
         impl TestClass {
            fn hello(&self) {
                println!("hello");
            }
         }

    Then rather than writing typing stubs in a pyi file we write the stub code directly in
    monarch/actor/_src/actor_mesh.py along with any helper methods:

        @rust_struct("monarch_hyperactor::actor_mesh::TestClass")
        class TestClass:
            def hello(self) -> None:
                ...
            def hello_world(self) -> None:
                self.hello()
                print("world")

    This class annotation then merges the python extension methods with the rust
    class implementation. Any rust code that returns the TestClass will have the `hello_world`
    extension method attached. Python typechecking always things TestClass is the python code,
    so typing works.

    It is ok to have the pyclass module not match where it is defined because (1) we patch it into the right place
    to make sure pickling works, and (2) the rust_struct annotation points directly to where to find the rust code,
    and will be discovered by goto line in the IDE.

    Mixins via inheritance:
        Base classes listed in the Python class definition are automatically
        treated as mixins.  Their concrete (non-abstract) methods and properties
        are patched onto the Rust class after the Python class body is applied.
        ABC bases are also registered so that ``isinstance()`` checks work.

        This is useful when a Rust struct should implement a Python
        trait/interface (e.g. MeshTrait) whose concrete methods depend on a
        small abstract interface that the ``@rust_struct`` class provides::

            @rust_struct("monarch_hyperactor::value_mesh::ValueMesh")
            class ValueMesh(MeshTrait):
                # provide the abstract interface MeshTrait needs
                @property
                def _ndslice(self) -> NDSlice: ...
                @property
                def _labels(self) -> ...: ...
                def _new_with_shape(self, shape): ...
    """

    *modules, name = name.split("::")
    module_name = ".".join(modules)
    module = importlib.import_module(f"monarch._rust_bindings.{module_name}")

    rust_class = getattr(module, name)

    return PatchRustClass(rust_class)
