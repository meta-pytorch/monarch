# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import importlib
from typing import cast, Type, TypeVar


T = TypeVar("T")


class PatchRustClass:
    def __init__(self, rust_class: Type[T]) -> None:
        self.rust_class = rust_class

    def __call__(self, python_class: Type[T]) -> Type[T]:
        rust_name = f"{self.rust_class.__module__}.{self.rust_class.__name__}"
        python_name = f"{python_class.__module__}.{python_class.__name__}"
        if rust_name != python_name:
            raise ValueError(f"mismatched type names {rust_name} != {python_name}")
        for name, implementation in python_class.__dict__.items():
            if hasattr(self.rust_class, name):
                the_attr = getattr(self.rust_class, name)
                is_object_default = name.startswith("__") and getattr(
                    the_attr, "__qualname__", ""
                ).startswith("object.")
                if not is_object_default:
                    # do not patch in the stub methods that
                    # are already defined by the rust implementation
                    continue
            if not callable(implementation) and not isinstance(
                implementation, property
            ):
                continue
            setattr(self.rust_class, name, implementation)
        return cast(Type[T], self.rust_class)


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
    """

    *modules, name = name.split("::")
    module_name = ".".join(modules)
    module = importlib.import_module(f"monarch._rust_bindings.{module_name}")

    rust_class = getattr(module, name)

    return PatchRustClass(rust_class)
