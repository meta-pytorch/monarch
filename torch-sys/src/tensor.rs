/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fmt;

use cxx::ExternType;
use cxx::type_id;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use serde::de::Visitor;

use crate::DeviceType;
use crate::bridge::const_data_ptr;
use crate::bridge::cpp_incref;
use crate::bridge::ffi;
use crate::bridge::ffi::copy_;
use crate::bridge::ffi::load_tensor;
use crate::bridge::ffi::repr;
use crate::bridge::ffi::save_tensor;
use crate::bridge::ffi::sizes;
use crate::bridge::mut_data_ptr;
use crate::cell::CloneUnsafe;

/// Rust binding for the C++ type `at::Tensor`.
///
/// # Safety
/// `Tensor` will properly manage the refcount of the underling `TensorImpl`.
///
/// `Tensor` is [`Send`]: it is safe to send across thread boundaries because
/// the underlying C++ type is atomically refcounted.
///
/// `Tensor` is [`Sync`]: it can be shared across threads. The underlying C++
/// type has interior mutability, (i.e. a `const Tensor&` can be used to mutate
/// the tensor) but we are careful to expose Rust bindings that require
/// exclusive access (ownership or mutable reference) for any C++ code that can
/// mutate a tensor.
#[repr(C)]
pub struct Tensor {
    /// This corresponds to impl_ in the C++ Tensor class.
    repr: *mut std::ffi::c_void,
}

impl Drop for Tensor {
    fn drop(&mut self) {
        // Undefined tensors do not have their refcounts changed.
        if self.defined() {
            // SAFETY: decrement this tensor's refcount. This ptr is guaranteed to
            // be non-null by the C++ side.
            unsafe { crate::bridge::cpp_decref(self.repr) };
        }
    }
}

impl fmt::Debug for Tensor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tensor").field("data", &"<...>").finish()
    }
}

impl Tensor {
    /// This is *unsafe* as it directly accesses the underlying data pointer.
    /// Additionally, this should only be used when the user is sure the tensor
    /// is defined.
    pub unsafe fn data_ptr(&self) -> *const std::ffi::c_void {
        // SAFETY: self.repr is guaranteed to be a non-null TensorImpl*
        unsafe { const_data_ptr(self.repr) }
    }

    /// This is *unsafe* as it directly accesses the underlying data pointer.
    /// Additionally, this should only be used when the user is sure the tensor
    /// is defined.
    pub unsafe fn mut_data_ptr(&self) -> *mut std::ffi::c_void {
        // SAFETY: self.repr is guaranteed to be a non-null TensorImpl*
        unsafe { mut_data_ptr(self.repr) }
    }

    /// Self-modify this tensor by copying data from another tensor. The other
    /// tensor must be the same shape as this one.
    pub fn copy_(&mut self, src: &Tensor) {
        copy_(self, src);
    }

    /// Return the size of each dimension in this tensor.
    pub fn sizes(&self) -> Vec<i32> {
        sizes(self)
    }

    /// Alias of sizes.
    pub fn shape(&self) -> Vec<i32> {
        self.sizes()
    }
}

impl CloneUnsafe for Tensor {
    /// This is *unsafe*, it creates an alias of the underlying Tensor that is
    /// not tracked by Rust. We use this to interface with C++ functions that
    /// expect an `at::Tensor`.
    ///
    /// The contract for calling this function is that the clone is local and
    /// ephemeral. More precisely:
    /// 1. The clone must not be sent to another thread (local).
    /// 2. You must guarantee that clone is dropped before the originating
    ///    mutable reference is dropped (ephemeral).
    unsafe fn clone_unsafe(&self) -> Self {
        // Undefined tensors do not have their refcounts changed.
        if self.defined() {
            // SAFETY: increment this tensor's refcount. This ptr is guaranteed to
            // be non-null by the C++ side.
            unsafe { cpp_incref(self.repr) };
        }
        Tensor { repr: self.repr }
    }
}

impl fmt::Display for Tensor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", repr(self))
    }
}

impl PartialEq for Tensor {
    fn eq(&self, other: &Self) -> bool {
        self.equal(other)
    }
}

// SAFETY: See safety discussion in [`Tensor`]
unsafe impl Send for Tensor {}
// SAFETY: See safety discussion in [`Tensor`]
unsafe impl Sync for Tensor {}

// SAFETY: Register our custom type implementation with cxx.
// It is okay to mark as trivial, as Tensor is relocatable, see the discussion
// in `bridge.h`.
unsafe impl ExternType for Tensor {
    type Id = type_id!("torch::Tensor");
    type Kind = cxx::kind::Trivial;
}

// Simple serialize/desrialize impls for `Tensor` to support sending them over
// the wire for e.g. `SendValue`.  Right now we just defer to C++'s `torch::save`
// and `torch::load`, but there might be more efficient ways to do this.
impl Serialize for Tensor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // We see deadlocks in CUDA libs that appear to happen when attempting
        // to save tensors from a thread that doesn't have the corresponding
        // device active.  So, try to detect this and fail.
        if self.device().device_type() != DeviceType::CPU {
            return Err(serde::ser::Error::custom(format!(
                "can only save CPU tensors (found {:?})",
                self.device(),
            )));
        }

        let bytes = save_tensor(self).map_err(serde::ser::Error::custom)?;
        serializer.serialize_bytes(&bytes)
    }
}

impl<'de> Deserialize<'de> for Tensor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct TensorVisitor;

        impl<'de> Visitor<'de> for TensorVisitor {
            type Value = Tensor;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("raw tensor bytes")
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                load_tensor(v).map_err(E::custom)
            }
        }

        deserializer.deserialize_bytes(TensorVisitor)
    }
}

impl FromPyObject<'_> for Tensor {
    fn extract_bound(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        ffi::tensor_from_py_object(obj.into()).map_err(|e| {
            PyValueError::new_err(format!(
                "Failed extracting {} from py as Tensor: {}",
                obj, e
            ))
        })
    }
}

impl<'py> IntoPyObject<'py> for Tensor {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        ffi::tensor_to_py_object(self).into_pyobject(py)
    }
}

pub type TensorCell = crate::cell::AliasTrackingRefCell<Tensor>;

impl TensorCell {
    /// Return cell with the backing tensor on the CPU.  If the backing tensor
    /// is on a GPU, it'll create a new Tensor/TensorCell with a copy of the
    /// backing tensor.  If the tensor is already on the CPU, it'll just return
    /// a this cell.
    pub fn try_cpu(self) -> Result<TensorCell, atomic_refcell::BorrowError> {
        {
            let borrow = self.try_borrow()?;
            if borrow.device().device_type() != DeviceType::CPU {
                return Ok(TensorCell::new(borrow.cpu()));
            }
        }
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use pyo3::prelude::*;

    use crate::Tensor;
    use crate::bridge::ffi::deep_clone;
    use crate::bridge::ffi::test_make_tensor;

    #[test]
    fn partial_eq() {
        let t1 = test_make_tensor();
        let t2 = deep_clone(&t1);
        assert_eq!(t1, t2);
    }

    #[test]
    fn bincode_serialize() {
        let t1 = test_make_tensor();
        let buf = bincode::serialize(&t1).unwrap();
        let t2: Tensor = bincode::deserialize(&buf).unwrap();
        assert_eq!(t1, t2);
    }

    #[test]
    fn multipart_serialize() {
        let t1 = test_make_tensor();
        let buf = serde_multipart::serialize_bincode(&t1).unwrap();
        let t2: Tensor = serde_multipart::deserialize_bincode(buf).unwrap();
        assert_eq!(t1, t2);
    }

    #[test]
    fn convert_to_py_and_back() {
        pyo3::prepare_freethreaded_python();
        let tensor = test_make_tensor();
        let converted = Python::with_gil(|py| {
            // import torch to ensure torch.layout types are registered
            py.import("torch").unwrap();
            let obj = deep_clone(&tensor).into_pyobject(py).unwrap();
            obj.extract::<Tensor>().unwrap()
        });
        assert_eq!(converted, tensor);
    }
}
