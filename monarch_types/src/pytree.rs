/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use pyo3::Bound;
use pyo3::FromPyObject;
use pyo3::IntoPyObject;
use pyo3::IntoPyObjectExt;
use pyo3::PyAny;
use pyo3::PyErr;
use pyo3::Python;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::PyAnyMethods;
use pyo3::prelude::PyResult;
use pyo3::prelude::PyTupleMethods;
use pyo3::types::PyBool;
use pyo3::types::PyBoolMethods;
use pyo3::types::PyList;
use pyo3::types::PyModule;
use pyo3::types::PyTuple;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::PickledPyObject;
use crate::python::TryIntoPyObjectUnsafe;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TreeSpec {
    Leaf,
    Tree(PickledPyObject),
}

/// A Rust wrapper around A PyTorch pytree, which can be serialized and sent
/// across the wire.
/// https://github.com/pytorch/pytorch/blob/main/torch/utils/_pytree.py
// NOTE: We have runtime deps on torch's pytree module and `pickle`, which the
// user must ensure is available.
#[derive(Clone, Debug, Serialize, Deserialize, Named)]
pub struct PyTree<T> {
    /// A wrapper around the tree spec.
    // NOTE: This is currently just the pickled bytes of the tree spec.  We
    // could also just deserialize to a `PyObject`, but this would mean
    // acquiring the GIL when deserializing the message.
    treespec: TreeSpec,
    /// The deserialized leaves of the pytree.
    leaves: Vec<T>,
}

impl<T> PyTree<T> {
    pub fn is_leaf(&self) -> bool {
        matches!(self.treespec, TreeSpec::Leaf)
    }

    pub fn into_leaf(mut self) -> Option<T> {
        if self.is_leaf() {
            self.leaves.pop()
        } else {
            None
        }
    }

    pub fn leaves(&self) -> &[T] {
        &self.leaves
    }

    pub fn into_leaves(self) -> Vec<T> {
        self.leaves
    }

    pub fn for_each<F>(&self, func: F)
    where
        F: FnMut(&T),
    {
        self.leaves.iter().for_each(func)
    }

    pub fn iter<'a>(&'a self) -> std::slice::Iter<'a, T> {
        self.leaves.iter()
    }

    pub fn iter_mut<'a>(&'a mut self) -> std::slice::IterMut<'a, T> {
        self.leaves.iter_mut()
    }

    /// Map leaf values with the given func.
    pub fn into_map<U, F>(self, func: F) -> PyTree<U>
    where
        F: FnMut(T) -> U,
    {
        PyTree {
            treespec: self.treespec,
            leaves: self.leaves.into_iter().map(func).collect(),
        }
    }

    /// Map the leaves of the pytree with the given fallible callback.
    // NOTE: This ends up copying the serialized treespec.
    pub fn try_map<U, F, E>(&self, mut func: F) -> Result<PyTree<U>, E>
    where
        F: FnMut(&T) -> Result<U, E>,
    {
        let mut leaves = vec![];
        for leaf in self.leaves.iter() {
            leaves.push(func(leaf)?);
        }
        Ok(PyTree {
            treespec: self.treespec.clone(),
            leaves,
        })
    }

    /// Map the leaves of the pytree with the given fallible callback.
    pub fn try_into_map<U, F, E>(self, mut func: F) -> Result<PyTree<U>, E>
    where
        F: FnMut(T) -> Result<U, E>,
    {
        let mut leaves = vec![];
        for leaf in self.leaves.into_iter() {
            leaves.push(func(leaf)?);
        }
        Ok(PyTree {
            treespec: self.treespec,
            leaves,
        })
    }

    fn unflatten_impl<'a>(
        py: Python<'a>,
        treespec: &TreeSpec,
        mut leaves: impl Iterator<Item = PyResult<Bound<'a, PyAny>>>,
    ) -> PyResult<Bound<'a, PyAny>> {
        if let TreeSpec::Tree(tree) = treespec {
            // Call into pytorch's unflatten.
            let module = PyModule::import(py, "torch.utils._pytree")?;
            let function = module.getattr("tree_unflatten")?;
            let leaves = leaves.collect::<Result<Vec<_>, _>>()?;
            let leaves = PyList::new(py, &leaves)?;
            let args = PyTuple::new(py, vec![leaves.as_any(), &tree.unpickle(py)?])?;
            let result = function.call(args, None)?;
            Ok(result)
        } else {
            leaves.next().ok_or(PyRuntimeError::new_err(
                "Pytree leaf unexpectedly had no value",
            ))?
        }
    }
}

impl<T> From<T> for PyTree<T> {
    fn from(value: T) -> Self {
        PyTree {
            treespec: TreeSpec::Leaf,
            leaves: vec![value],
        }
    }
}

impl<'py, T> IntoPyObject<'py> for PyTree<T>
where
    T: IntoPyObject<'py>,
{
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyTree::<T>::unflatten_impl(
            py,
            &self.treespec,
            self.leaves.into_iter().map(|l| l.into_bound_py_any(py)),
        )
    }
}

impl<'a, 'py, T> IntoPyObject<'py> for &'a PyTree<T>
where
    &'a T: IntoPyObject<'py>,
    T: 'a,
{
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyTree::<T>::unflatten_impl(
            py,
            &self.treespec,
            self.leaves.iter().map(|l| l.into_bound_py_any(py)),
        )
    }
}

/// Serialize into a `PyObject`.
impl<'a, 'py, T> TryIntoPyObjectUnsafe<'py, PyAny> for &'a PyTree<T>
where
    &'a T: TryIntoPyObjectUnsafe<'py, PyAny>,
    T: 'a,
{
    unsafe fn try_to_object_unsafe(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        PyTree::<T>::unflatten_impl(
            py,
            &self.treespec,
            self.leaves
                .iter()
                // SAFETY: Safety requirements are propagated via the `unsafe`
                // tag on this method.
                .map(|l| unsafe { l.try_to_object_unsafe(py) }),
        )
    }
}

impl<'a, T: FromPyObject<'a>> PyTree<T> {
    pub fn flatten(tree: &Bound<'a, PyAny>) -> PyResult<Self> {
        let py = tree.py();

        // Call into pytorch's flatten.
        let pytree_module = PyModule::import(py, "torch.utils._pytree")?;
        let tree_flatten = pytree_module.getattr("tree_flatten")?;
        let res = tree_flatten.call1((tree,))?;

        // Convert leaves to Rust objects.
        let (leaves, treespec) = match res.downcast::<PyTuple>()?.as_slice() {
            [leaves, treespec] => {
                let mut out = vec![];
                for leaf in leaves.try_iter()? {
                    out.push(T::extract_bound(&leaf?)?);
                }
                (out, treespec)
            }
            _ => return Err(PyTypeError::new_err("unexpected result from tree_flatten")),
        };

        if treespec
            .call_method0("is_leaf")?
            .downcast::<PyBool>()?
            .is_true()
        {
            Ok(Self {
                treespec: TreeSpec::Leaf,
                leaves,
            })
        } else {
            Ok(Self {
                treespec: TreeSpec::Tree(PickledPyObject::pickle(treespec)?),
                leaves,
            })
        }
    }
}

/// Deserialize from a `PyObject`.
impl<'a, T: FromPyObject<'a>> FromPyObject<'a> for PyTree<T> {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        Self::flatten(ob)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use pyo3::IntoPyObject;
    use pyo3::Python;
    use pyo3::ffi::c_str;
    use pyo3::py_run;

    use super::PyTree;

    #[test]
    fn flatten_unflatten() -> Result<()> {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let tree = py.eval(c_str!("[1, 2]"), None, None)?;
            let tree: PyTree<u64> = PyTree::flatten(&tree)?;
            assert_eq!(tree.leaves, vec![1u64, 2u64]);
            let list = tree.into_pyobject(py)?;
            py_run!(py, list, "assert list == [1, 2]");
            anyhow::Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn try_map() -> Result<()> {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let tree = py.eval(c_str!("[1, 2]"), None, None)?;
            let tree: PyTree<u64> = PyTree::flatten(&tree)?;
            let tree = tree.try_map(|v| anyhow::Ok(v + 1))?;
            assert_eq!(tree.leaves, vec![2u64, 3u64]);
            anyhow::Ok(())
        })?;
        Ok(())
    }
}
