/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::ffi::c_void;
use std::fmt;
use std::mem::MaybeUninit;

use cxx::ExternType;
use cxx::type_id;
use monarch_types::TryIntoPyObjectUnsafe;
use paste::paste;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use serde::Deserialize;
use serde::Serialize;

use crate::CloneUnsafe;
use crate::Device;
use crate::Tensor;
use crate::bridge::clone_iv;
use crate::bridge::ffi;
use crate::cell::AliasTrackingRefCell;

/// Rust binding for the C++ type `c10::IValue`.
///
/// `IValue` is a tagged union type that can hold any input to a PyTorch
/// operator. See [`IValueKind`] for the list of supported types.
///
/// # Safety
///
/// `IValue` either contains [`Copy`]-able data or a Tensor-like object, so it
/// inherits the safety properties of [`Tensor`]. See the safety discussion in
/// [`Tensor#safety`] for more info.
#[repr(C)]
pub struct IValue {
    /// #[doc(hidden)]
    /// Internal representation of IValue in C++. An IValue is 16 bytes, with 8
    /// bytes for a payload and 8 bytes for a type tag. We assert in `bridge.h`
    /// that the size and alignment are what we expect.
    repr: [*mut c_void; 2],
}

// SAFETY: Register our custom bindings with cxx. IValue is trivial, see the
// discussion in `bridge.h`.
unsafe impl ExternType for IValue {
    type Id = type_id!("c10::IValue");
    type Kind = cxx::kind::Trivial;
}

impl Drop for IValue {
    fn drop(&mut self) {
        // SAFETY: calls the C++ destructor for IValue, potentially
        // decrementing a tensor refcount.
        unsafe { crate::bridge::drop(self) };
    }
}

impl PartialEq for IValue {
    fn eq(&self, other: &Self) -> bool {
        ffi::ivalues_equal_operator(self, other)
    }
}

/// SAFETY: IValue is [`Send`], it is either a copyable type or atomically
/// refcounted via `c10::intrusive_ptr`.
unsafe impl Send for IValue {}

/// SAFETY: IValue is [`Sync`], due to safety in exposing any of the interior
/// mutability of the payload it holds. The value is converted to native types
/// like [`Tensor`] for use in rust or left opaque.
/// See [`OpaqueIValue`] for more details.
unsafe impl Sync for IValue {}

impl Serialize for IValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(
            ffi::serialize_ivalue(self)
                .map_err(serde::ser::Error::custom)?
                .as_slice(),
        )
    }
}

impl<'de> Deserialize<'de> for IValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let buf: &[u8] = Deserialize::deserialize(deserializer)?;
        ffi::deserialize_ivalue(buf).map_err(serde::de::Error::custom)
    }
}

impl CloneUnsafe for IValue {
    /// This is *unsafe*, it creates an alias of the underlying data that is
    /// not tracked by Rust. We use this to interface with C++ functions that
    /// expect an owned IValue.
    ///
    /// The contract for calling this function is that the clone is local and
    /// ephemeral. More precisely:
    /// 1. The clone must not be sent to another thread (local).
    /// 2. You must guarantee that clone is dropped before the originating
    ///    mutable reference is dropped (ephemeral).
    unsafe fn clone_unsafe(&self) -> Self {
        let mut ivalue = MaybeUninit::<IValue>::uninit();
        let new = ivalue.as_mut_ptr().cast();
        // SAFETY: `ivalue` will be correctly initialized by the call to `clone_iv`.
        unsafe {
            clone_iv(self, new);
            ivalue.assume_init()
        }
    }
}

/// An opaque container for an [`IValue`]. This is used to restrict safe direct access
/// to the underlying [`IValue`].
#[derive(Debug, Serialize, Deserialize)]
pub struct OpaqueIValue(IValue);

impl OpaqueIValue {
    /// This is *unsafe*, it creates an alias of the underlying data that is
    /// not tracked by Rust. We need this to interface with C++ functions that
    /// expect an owned IValue. The caller is responsible for ensuring that
    /// this is done in a safe way.
    pub(crate) unsafe fn ivalue(&self) -> IValue {
        // SAFETY: See above
        unsafe { self.0.clone_unsafe() }
    }

    pub fn from_py_object_with_type(
        obj: Bound<'_, PyAny>,
        type_: &crate::call_op::TypePtr,
        num_elements: i32,
        allow_nums_as_tensors: bool,
    ) -> PyResult<OpaqueIValue> {
        IValue::from_py_object_with_type(obj, type_, num_elements, allow_nums_as_tensors)
            .map(OpaqueIValue)
    }
}

impl Clone for OpaqueIValue {
    /// This creates a deep copy of the underlying data and can be expensive.
    /// It might also panic if the `IValue` is not cloneable.
    fn clone(&self) -> Self {
        let serialized = bincode::serialize(&self.0).unwrap();
        bincode::deserialize(&serialized).unwrap()
    }
}

impl CloneUnsafe for OpaqueIValue {
    unsafe fn clone_unsafe(&self) -> Self {
        // SAFETY: See discussion for `IValue::clone_unsafe`.
        Self(unsafe { self.0.clone_unsafe() })
    }
}

impl<'py> TryIntoPyObjectUnsafe<'py, PyAny> for OpaqueIValue {
    unsafe fn try_to_object_unsafe(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        // SAFETY: See discussion for `IValue::clone_unsafe`.
        unsafe { self.ivalue() }.into_pyobject(py)
    }
}

pub type OpaqueIValueCell = AliasTrackingRefCell<OpaqueIValue>;

impl From<OpaqueIValue> for OpaqueIValueCell {
    #[inline]
    fn from(value: OpaqueIValue) -> Self {
        Self::new(value)
    }
}

macro_rules! gen_is_impl {
    ($enum:ty, [$($kind:ident),* $(,)? ]) => {
        paste! {
            $(
                pub fn [<is_ $kind:snake:lower>](&self) -> bool {
                    self.[<is $kind>]()
                }
            )*

            pub fn is_other(&self) -> bool {
                self.kind() == $enum::Other
            }

            fn __exhaustive_checker(foo: $enum) {
                match foo {
                    $($enum::$kind => (),)*
                    IValueKind::Other => (),
                }
            }

            pub fn kind(&self) -> $enum {
                if false {
                    unreachable!();
                } $(else if self.[<is_ $kind:snake:lower>]() {
                    $enum::$kind
                })*
                else {
                    $enum::Other
                }
            }
        }
    }
}

macro_rules! gen_from_impl {
    ($enum:ty, $($kind:ident, $from_type:ty);* $(;)?) => {
        paste! {
            $(
                impl From<$from_type> for IValue {
                    fn from(value: $from_type) -> Self {
                        ffi::[<ivalue_from_ $kind:snake:lower>](value)
                    }
                }

            )*

            fn __exhaustive_checker(foo: $enum) {
                match foo {
                    $($enum::$kind => (),)*
                    IValueKind::None => (),
                    IValueKind::Other => (),
                }
            }
        }

    }
}

impl From<()> for IValue {
    fn from(_value: ()) -> Self {
        ffi::ivalue_from_none()
    }
}

impl IValue {
    pub fn to_tensor(self) -> Option<Tensor> {
        ffi::toTensor(self).ok()
    }
    pub fn to_string(&self) -> Option<String> {
        ffi::toString(self).ok()
    }
    pub fn to_int_list(&self) -> Option<Vec<i64>> {
        ffi::toIntList(self).ok()
    }
    pub fn to_int(&self) -> Option<i64> {
        self.toInt().ok()
    }
    pub fn to_double(&self) -> Option<f64> {
        self.toDouble().ok()
    }
    pub fn to_bool(&self) -> Option<bool> {
        self.toBool().ok()
    }
    pub fn to_tensor_list(self) -> Option<Vec<Tensor>> {
        ffi::toTensorList(self).ok()
    }
    pub fn to_device(&self) -> Option<Device> {
        self.toDevice().ok()
    }
    pub fn to_none(&self) -> Option<()> {
        if self.is_none() { Some(()) } else { None }
    }
    pub fn to_opaque(self) -> Option<OpaqueIValue> {
        if self.is_other() {
            Some(OpaqueIValue(self))
        } else {
            None
        }
    }
    // Generate is_ methods for all IValue kinds.
    // If you get a compile error here, make sure:
    //   - Your new kind is registered on IValueKind
    //   - You added a field here.
    gen_is_impl! {
        IValueKind, [
            Tensor,
            String,
            IntList,
            Int,
            Double,
            Bool,
            TensorList,
            Device,
            None,
        ]
    }

    pub fn from_py_object_with_type(
        obj: Bound<'_, PyAny>,
        type_: &crate::call_op::TypePtr,
        num_elements: i32,
        allow_nums_as_tensors: bool,
    ) -> PyResult<IValue> {
        ffi::ivalue_from_py_object_with_type(obj.into(), type_, num_elements, allow_nums_as_tensors)
            .map_err(|err| {
                PyValueError::new_err(format!(
                    "Failed to extract IValue from python object: {:?}",
                    err
                ))
            })
    }

    pub(crate) fn from_py_object_or_none(obj: &Bound<'_, PyAny>) -> Option<IValue> {
        ffi::py_object_is_ivalue(obj.clone().into())
            .then(|| ffi::ivalue_from_arbitrary_py_object(obj.into()).unwrap())
    }
}

// impl `From` for all IValue kinds.
// If you get a compile error here, make sure:
//   - Your new kind is registered on IValueKind
//   - You added a field here.
gen_from_impl! {
    IValueKind,
    Tensor, Tensor;
    String, &String;
    IntList, &[i64];
    Int, i64;
    Double, f64;
    Bool, bool;
    TensorList, Vec<Tensor>;
    Device, Device;
}

/// Enum representing the different internal types an [`IValue`] can hold.
///
/// Check each variant docs to see what the internal storage in C++ is, and what
/// the cost of moving across the Rust<>C++ boundary is.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum IValueKind {
    /// - C++ type: `at::Tensor`
    /// - Rust type: [`Tensor`]
    Tensor,
    /// - C++ type: `bool`
    /// - Rust type: [`bool`]
    Bool,
    /// - C++ type: `int64_t`
    /// - Rust type: [`i64`]
    Int,
    /// - C++ type: `c10::List<int64_t>`
    /// - Rust type: [`Vec<i64>`]
    ///
    /// <div class="warning">
    /// Passing across the C++-Rust boundary will copy the vector.
    /// </div>
    IntList,
    /// - C++ type: `double`
    /// - Rust type: [`f64`]
    Double,
    /// - C++ type: `c10::intrusive_ptr<ConstantString>`
    /// - Rust type: [`String`]
    ///
    /// <div class="warning">
    /// Passing across the C++-Rust boundary will copy the string.
    /// </div>
    String,
    /// - C++ type: `c10::List<at::Tensor>`
    /// - Rust type: [`Vec<Tensor>`]
    ///
    /// <div class="warning">
    /// Passing across the C++-Rust boundary will copy the vector.
    /// </div>
    TensorList,
    /// - C++ type: `c10::Device`
    /// - Rust type: [`Device`]
    Device,
    None,

    /// Catch-all for all other types. This is used for types that are not
    /// natively supported in rust and can remain as opaque IValues for
    /// interacting with torch apis. There is an overhead associated with
    /// tracking alias and borrows for any trivial IValues being converted
    /// to this type so they should be natively supported. Most of them are
    /// already supported.
    Other,
}

impl fmt::Debug for IValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", ffi::debug_print(self).map_err(|_| fmt::Error)?)
    }
}

impl<'py> IntoPyObject<'py> for IValue {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        ffi::arbitrary_ivalue_to_py_object(self)
            .map_err(|e| PyValueError::new_err(format!("Failed converting to py: {}", e)))?
            .into_pyobject(py)
    }
}

impl FromPyObject<'_> for IValue {
    fn extract_bound(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        ffi::ivalue_from_arbitrary_py_object(obj.into()).map_err(|e| {
            PyValueError::new_err(format!("Failed extracting from py: {}: {}", e, obj))
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::useless_vec)]

    use pyo3::types::PyFloat;

    use super::*;
    use crate::DeviceType;
    use crate::bridge::ffi::test_make_opaque_ivalue;
    use crate::bridge::ffi::test_make_tensor;
    use crate::bridge::ffi::test_make_undefined_tensor_ivalue;

    impl From<String> for IValue {
        fn from(value: String) -> Self {
            ffi::ivalue_from_string(&value)
        }
    }

    // Check for the equality of two IValues using tensor.equal to compare
    // tensors.
    fn ivalues_equal_with_tensor_equal(a: IValue, b: IValue) -> bool {
        if a.isTensor() {
            return b.isTensor() && a.to_tensor().unwrap().equal(&b.to_tensor().unwrap());
        }

        if a.isTensorList() {
            if !b.isTensorList() {
                return false;
            }
            let a_list = a.to_tensor_list().unwrap();
            let b_list = b.to_tensor_list().unwrap();
            if a_list.len() != b_list.len() {
                return false;
            }
            for (a_tensor, b_tensor) in a_list.iter().zip(b_list.iter()) {
                if !a_tensor.equal(b_tensor) {
                    return false;
                }
            }
            return true;
        }

        a == b
    }

    #[test]
    fn bincode_serialize() {
        let tensor = test_make_tensor();
        let i1 = IValue::from(tensor);
        let buf = bincode::serialize(&i1).unwrap();
        let i2: IValue = bincode::deserialize(&buf).unwrap();
        assert!(ivalues_equal_with_tensor_equal(i1, i2));
    }

    #[test]
    fn multipart_serialize() {
        let tensor = test_make_tensor();
        let i1 = IValue::from(tensor);
        let buf = serde_multipart::serialize_bincode(&i1).unwrap();
        let i2_result = serde_multipart::deserialize_bincode::<IValue>(buf);
        assert!(i2_result.is_err());
        assert_eq!(
            format!("{}", i2_result.unwrap_err()),
            "invalid type: byte array, expected a borrowed byte array",
        );
    }

    #[test]
    fn test_ivalue_from_py_object_with_type() {
        pyo3::prepare_freethreaded_python();

        let args_info =
            crate::call_op::get_schema_args_info("aten::_foreach_add_", "Tensor").unwrap();
        let (list, tensor, tensor_1, tensor_1_err) = Python::with_gil(|py| {
            let list = pyo3::types::PyList::empty(py).into_any();
            let none = py.None().into_bound(py);
            let one = PyFloat::new(py, 1.0).into_any();
            (
                IValue::from_py_object_with_type(
                    list,
                    args_info[0].type_,
                    args_info[0].num_elements,
                    false,
                )
                .unwrap(),
                IValue::from_py_object_with_type(
                    none,
                    args_info[1].type_,
                    args_info[1].num_elements,
                    false,
                )
                .unwrap(),
                IValue::from_py_object_with_type(
                    one.clone(),
                    args_info[1].type_,
                    args_info[1].num_elements,
                    true,
                )
                .unwrap(),
                IValue::from_py_object_with_type(
                    one,
                    args_info[1].type_,
                    args_info[1].num_elements,
                    false,
                ),
            )
        });
        assert!(list.is_tensor_list());
        assert!(tensor.is_tensor());
        assert!(tensor_1.is_tensor());
        assert!(tensor_1_err.is_err());
    }

    macro_rules! generate_py_object_roundtrip_tests {
        ($($kind:ident, $input:expr);* $(;)?) => {
            paste! {
                $(
                    #[test]
                    fn [<test_py_object_roundtrip_ $kind:snake:lower>]() {
                            pyo3::prepare_freethreaded_python();
                            // We need to load torch to initialize some internal
                            // structures used by the FFI funcs we use to convert
                            // ivalues to/from py objects.
                            Python::with_gil(|py| py.run(pyo3::ffi::c_str!("import torch"), None, None)).unwrap();
                            let original = IValue::from($input);
                            // SAFETY: `TryIntoPyObject` consumes the value, so
                            // we clone here to use for the `assert_eq` at end.
                            let converted = unsafe { original.clone_unsafe() };
                            let converted = Python::with_gil(|py| {
                                let py_object = converted.into_pyobject(py).unwrap();
                                anyhow::Ok(IValue::extract_bound(&py_object).unwrap())
                            }).unwrap();
                            assert!(ivalues_equal_with_tensor_equal(original, converted));
                    }
                )*

                #[test]
                fn test_py_object_roundtrip_was_exhaustive() {
                    match IValueKind::Int {
                        $(IValueKind::$kind => (),)*
                    }
                }
            }
        }
    }

    // Generate exhaustive roundtrip tests for all IValue kind.
    // If you got a "non-exhaustive patterns" error here, you need to add a new
    // test entry for your IValue kind!
    generate_py_object_roundtrip_tests! {
        Int, 123;
        Double, 1.23;
        String, "foobar".to_owned();
        IntList, [1, 2, 3].as_slice();
        Bool, false;
        Tensor, test_make_tensor();
        TensorList, vec![test_make_tensor()];
        Device, Device::new(DeviceType::CPU);
        None, ();
        Other, test_make_opaque_ivalue();
    }

    macro_rules! generate_serde_roundtrip_tests {
        ($($kind:ident, $input:expr);* $(;)?) => {
            paste! {
                $(
                    #[test]
                    fn [<test_serde_roundtrip_ $kind:snake:lower>]() {
                            pyo3::prepare_freethreaded_python();
                            // We need to load torch to initialize some internal
                            // structures used by the FFI funcs we use to convert
                            // ivalues to/from py objects.
                            Python::with_gil(|py| py.run(pyo3::ffi::c_str!("import torch"), None, None)).unwrap();
                            let original = IValue::from($input);
                            let converted: IValue = bincode::deserialize(&bincode::serialize(&original).unwrap()).unwrap();
                            assert!(ivalues_equal_with_tensor_equal(original, converted));
                    }
                )*

                #[test]
                fn test_serde_roundtrip_was_exhaustive() {
                    match IValueKind::Int {
                        $(IValueKind::$kind => (),)*
                    }
                }
            }
        }
    }

    // Generate exhaustive serde roundtrip tests for all IValue kind.
    // If you got a "non-exhaustive patterns" error here, you need to add a new
    // test entry for your IValue kind!
    generate_serde_roundtrip_tests! {
        Int, 123;
        Double, 1.23;
        String, "foobar".to_owned();
        IntList, [1, 2, 3].as_slice();
        Bool, false;
        Tensor, test_make_tensor();
        TensorList, vec![test_make_tensor()];
        Device, Device::new(DeviceType::CPU);
        None, ();
        Other, test_make_opaque_ivalue();
    }

    #[test]
    fn test_serde_roundtrip_undefined_tensor() {
        let original = test_make_undefined_tensor_ivalue();
        assert!(original.is_tensor());
        assert!(
            // SAFETY: Since it is an undefined tensor that we dont mutate,
            // it is safe to clone in this test.
            !unsafe { original.clone_unsafe() }
                .to_tensor()
                .unwrap()
                .defined()
        );
        let converted: IValue =
            bincode::deserialize(&bincode::serialize(&original).unwrap()).unwrap();
        assert!(converted.is_tensor());
        assert!(!converted.to_tensor().unwrap().defined());
    }
}
