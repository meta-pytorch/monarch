/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![allow(non_camel_case_types)]

//! Simplified Rust bindings for libtorch C++ APIs.
//!
//! This is a streamlined version that only includes the functionality
//! actually used by the monarch codebase.

pub mod testing;

use monarch_types::py_global;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

// Cached imports for torch APIs
py_global!(torch_device, "torch", "device");
py_global!(torch_strided, "torch", "strided");
py_global!(torch_sparse_coo, "torch", "sparse_coo");
py_global!(torch_contiguous_format, "torch", "contiguous_format");
py_global!(torch_preserve_format, "torch", "preserve_format");
py_global!(torch_channels_last, "torch", "channels_last");
py_global!(torch_channels_last_3d, "torch", "channels_last_3d");
py_global!(torch_uint8, "torch", "uint8");
py_global!(torch_int8, "torch", "int8");
py_global!(torch_int16, "torch", "int16");
py_global!(torch_int32, "torch", "int32");
py_global!(torch_int64, "torch", "int64");
py_global!(torch_float16, "torch", "float16");
py_global!(torch_float32, "torch", "float32");
py_global!(torch_float64, "torch", "float64");
py_global!(torch_complex32, "torch", "complex32");
py_global!(torch_complex64, "torch", "complex64");
py_global!(torch_complex128, "torch", "complex128");
py_global!(torch_bool, "torch", "bool");
py_global!(torch_bfloat16, "torch", "bfloat16");
py_global!(torch_float8_e5m2, "torch", "float8_e5m2");
py_global!(torch_float8_e4m3fn, "torch", "float8_e4m3fn");
py_global!(torch_zeros, "torch", "zeros");
py_global!(torch_empty, "torch", "empty");
py_global!(torch_tensor, "torch", "tensor");
py_global!(torch_allclose, "torch", "allclose");
py_global!(torch_full, "torch", "full");
py_global!(torch_stack, "torch", "stack");

// ============================================================================
// Device Types
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DeviceType {
    CPU,
    CUDA,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeviceIndex(pub i8);

impl From<DeviceIndex> for i8 {
    fn from(idx: DeviceIndex) -> i8 {
        idx.0
    }
}

impl From<i8> for DeviceIndex {
    fn from(idx: i8) -> DeviceIndex {
        DeviceIndex(idx)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Device {
    device_type: DeviceType,
    index: Option<DeviceIndex>,
}

impl Device {
    pub fn device_type(&self) -> DeviceType {
        self.device_type
    }
}

impl FromPyObject<'_> for Device {
    fn extract_bound(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        let device_str: String = obj.str()?.extract()?;
        // Parse the device string
        device_str.parse().map_err(|e: DeviceParseError| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string())
        })
    }
}

impl<'py> IntoPyObject<'py> for Device {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let device_str = self.to_string();
        let device = torch_device(py).call1((device_str,))?;
        Ok(device)
    }
}

impl std::fmt::Display for Device {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.device_type {
            DeviceType::CPU => write!(f, "cpu"),
            DeviceType::CUDA => {
                if let Some(index) = self.index {
                    write!(f, "cuda:{}", index.0)
                } else {
                    write!(f, "cuda")
                }
            }
        }
    }
}

impl std::str::FromStr for Device {
    type Err = DeviceParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "cpu" {
            Ok(Device {
                device_type: DeviceType::CPU,
                index: None,
            })
        } else if s == "cuda" {
            Ok(Device {
                device_type: DeviceType::CUDA,
                index: None,
            })
        } else if let Some(cuda_idx) = s.strip_prefix("cuda:") {
            let index = cuda_idx
                .parse::<i8>()
                .map_err(|_| DeviceParseError::InvalidDevice)?;
            Ok(Device {
                device_type: DeviceType::CUDA,
                index: Some(DeviceIndex(index)),
            })
        } else {
            Err(DeviceParseError::InvalidDevice)
        }
    }
}

impl From<CudaDevice> for Device {
    fn from(cuda_device: CudaDevice) -> Self {
        Device {
            device_type: DeviceType::CUDA,
            index: Some(cuda_device.index),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CudaDevice {
    index: DeviceIndex,
}

impl CudaDevice {
    pub fn new(index: DeviceIndex) -> Self {
        CudaDevice { index }
    }

    pub fn index(&self) -> DeviceIndex {
        self.index
    }
}

#[derive(Debug, Error)]
pub enum DeviceParseError {
    #[error("invalid device string")]
    InvalidDevice,
}

// ============================================================================
// Layout and Memory Format
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(i32)]
pub enum Layout {
    Strided = 0,
    Sparse = 1,
    Mkldnn = 2,
}

/// Remote serde implementation for Layout
#[derive(Serialize, Deserialize)]
#[serde(remote = "Layout")]
pub enum LayoutDef {
    Strided,
    Sparse,
    Mkldnn,
}

impl FromPyObject<'_> for Layout {
    fn extract_bound(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        Python::attach(|py| {
            let strided = torch_strided(py);
            let sparse_coo = torch_sparse_coo(py);

            if obj.eq(strided)? {
                Ok(Layout::Strided)
            } else if obj.eq(sparse_coo)? {
                Ok(Layout::Sparse)
            } else {
                // Try to match by string representation
                let obj_str: String = obj.str()?.extract()?;
                match obj_str.as_str() {
                    "torch.strided" => Ok(Layout::Strided),
                    "torch.sparse_coo" => Ok(Layout::Sparse),
                    _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "Unknown layout type",
                    )),
                }
            }
        })
    }
}

impl<'py> IntoPyObject<'py> for Layout {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            Layout::Strided => Ok(torch_strided(py)),
            Layout::Sparse => Ok(torch_sparse_coo(py)),
            Layout::Mkldnn => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "MKLDNN layout not supported in PyTorch",
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(i32)]
pub enum MemoryFormat {
    Contiguous = 0,
    Preserve = 1,
    ChannelsLast = 2,
    ChannelsLast3d = 3,
}

/// Remote serde implementation for MemoryFormat
#[derive(Serialize, Deserialize)]
#[serde(remote = "MemoryFormat")]
pub enum MemoryFormatDef {
    Contiguous,
    Preserve,
    ChannelsLast,
    ChannelsLast3d,
}

impl<'py> IntoPyObject<'py> for MemoryFormat {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            MemoryFormat::Contiguous => Ok(torch_contiguous_format(py)),
            MemoryFormat::Preserve => Ok(torch_preserve_format(py)),
            MemoryFormat::ChannelsLast => Ok(torch_channels_last(py)),
            MemoryFormat::ChannelsLast3d => Ok(torch_channels_last_3d(py)),
        }
    }
}

// ============================================================================
// ScalarType
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(i32)]
pub enum ScalarType {
    Byte = 0,
    Char = 1,
    Short = 2,
    Int = 3,
    Long = 4,
    Half = 5,
    Float = 6,
    Double = 7,
    ComplexHalf = 8,
    ComplexFloat = 9,
    ComplexDouble = 10,
    Bool = 11,
    QInt8 = 12,
    QUInt8 = 13,
    QInt32 = 14,
    BFloat16 = 15,
    QUInt4x2 = 16,
    QUInt2x4 = 17,
    Bits1x8 = 18,
    Bits2x4 = 19,
    Bits4x2 = 20,
    Bits8 = 21,
    Bits16 = 22,
    Float8_e5m2 = 23,
    Float8_e4m3fn = 24,
    Float8_e5m2fnuz = 25,
    Float8_e4m3fnuz = 26,
}

/// Remote serde implementation for ScalarType
#[derive(Serialize, Deserialize)]
#[serde(remote = "ScalarType")]
pub enum ScalarTypeDef {
    Byte,
    Char,
    Short,
    Int,
    Long,
    Half,
    Float,
    Double,
    ComplexHalf,
    ComplexFloat,
    ComplexDouble,
    Bool,
    QInt8,
    QUInt8,
    QInt32,
    BFloat16,
    QUInt4x2,
    QUInt2x4,
    Bits1x8,
    Bits2x4,
    Bits4x2,
    Bits8,
    Bits16,
    Float8_e5m2,
    Float8_e4m3fn,
    Float8_e5m2fnuz,
    Float8_e4m3fnuz,
}

impl FromPyObject<'_> for ScalarType {
    fn extract_bound(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        Python::attach(|py| {
            // Map of PyTorch dtype getters to ScalarType
            let dtype_map = [
                (torch_uint8(py), ScalarType::Byte),
                (torch_int8(py), ScalarType::Char),
                (torch_int16(py), ScalarType::Short),
                (torch_int32(py), ScalarType::Int),
                (torch_int64(py), ScalarType::Long),
                (torch_float16(py), ScalarType::Half),
                (torch_float32(py), ScalarType::Float),
                (torch_float64(py), ScalarType::Double),
                (torch_complex32(py), ScalarType::ComplexHalf),
                (torch_complex64(py), ScalarType::ComplexFloat),
                (torch_complex128(py), ScalarType::ComplexDouble),
                (torch_bool(py), ScalarType::Bool),
                (torch_bfloat16(py), ScalarType::BFloat16),
                (torch_float8_e5m2(py), ScalarType::Float8_e5m2),
                (torch_float8_e4m3fn(py), ScalarType::Float8_e4m3fn),
            ];

            // Try matching by equality with torch dtypes
            for (dtype, scalar_type) in &dtype_map {
                if obj.eq(dtype)? {
                    return Ok(*scalar_type);
                }
            }

            // Try matching by string representation
            let obj_str: String = obj.str()?.extract()?;
            let str_map = [
                ("uint8", ScalarType::Byte),
                ("int8", ScalarType::Char),
                ("int16", ScalarType::Short),
                ("int32", ScalarType::Int),
                ("int64", ScalarType::Long),
                ("float16", ScalarType::Half),
                ("float32", ScalarType::Float),
                ("float64", ScalarType::Double),
                ("complex32", ScalarType::ComplexHalf),
                ("complex64", ScalarType::ComplexFloat),
                ("complex128", ScalarType::ComplexDouble),
                ("bool", ScalarType::Bool),
                ("bfloat16", ScalarType::BFloat16),
                ("float8_e5m2", ScalarType::Float8_e5m2),
                ("float8_e4m3fn", ScalarType::Float8_e4m3fn),
            ];

            for (name, scalar_type) in &str_map {
                if obj_str.contains(name) {
                    return Ok(*scalar_type);
                }
            }

            Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Unknown scalar type: {}",
                obj_str
            )))
        })
    }
}

impl<'py> IntoPyObject<'py> for ScalarType {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            ScalarType::Byte => Ok(torch_uint8(py)),
            ScalarType::Char => Ok(torch_int8(py)),
            ScalarType::Short => Ok(torch_int16(py)),
            ScalarType::Int => Ok(torch_int32(py)),
            ScalarType::Long => Ok(torch_int64(py)),
            ScalarType::Half => Ok(torch_float16(py)),
            ScalarType::Float => Ok(torch_float32(py)),
            ScalarType::Double => Ok(torch_float64(py)),
            ScalarType::ComplexHalf => Ok(torch_complex32(py)),
            ScalarType::ComplexFloat => Ok(torch_complex64(py)),
            ScalarType::ComplexDouble => Ok(torch_complex128(py)),
            ScalarType::Bool => Ok(torch_bool(py)),
            ScalarType::BFloat16 => Ok(torch_bfloat16(py)),
            ScalarType::Float8_e5m2 => Ok(torch_float8_e5m2(py)),
            ScalarType::Float8_e4m3fn => Ok(torch_float8_e4m3fn(py)),
            _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Unsupported scalar type: {:?}",
                self
            ))),
        }
    }
}

// ============================================================================
// Tensor and TensorCell
// ============================================================================

#[derive(Debug)]
pub struct Tensor {
    inner: Py<PyAny>,
}

impl Clone for Tensor {
    fn clone(&self) -> Self {
        Python::attach(|py| Tensor {
            inner: self.inner.clone_ref(py),
        })
    }
}

impl Tensor {
    pub fn scalar_type(&self) -> ScalarType {
        Python::attach(|py| {
            let tensor = self.inner.bind(py);
            let dtype = tensor.getattr("dtype").unwrap();
            ScalarType::extract_bound(&dtype).unwrap()
        })
    }

    pub fn device(&self) -> Device {
        Python::attach(|py| {
            let tensor = self.inner.bind(py);
            let device = tensor.getattr("device").unwrap();
            Device::extract_bound(&device).unwrap()
        })
    }

    pub fn numel(&self) -> i64 {
        Python::attach(|py| {
            let tensor = self.inner.bind(py);
            tensor.call_method0("numel").unwrap().extract().unwrap()
        })
    }

    pub fn data_ptr(&self) -> *const std::ffi::c_void {
        Python::attach(|py| {
            let tensor = self.inner.bind(py);
            let ptr: usize = tensor.call_method0("data_ptr").unwrap().extract().unwrap();
            ptr as *const std::ffi::c_void
        })
    }

    pub fn mut_data_ptr(&self) -> *mut std::ffi::c_void {
        self.data_ptr() as *mut std::ffi::c_void
    }

    pub fn defined(&self) -> bool {
        Python::attach(|py| {
            let tensor = self.inner.bind(py);
            // A tensor is defined if it's not None and has storage
            !tensor.is_none()
        })
    }

    pub fn is_cuda(&self) -> bool {
        Python::attach(|py| {
            let tensor = self.inner.bind(py);
            tensor.getattr("is_cuda").unwrap().extract().unwrap()
        })
    }

    pub fn is_sparse(&self) -> bool {
        Python::attach(|py| {
            let tensor = self.inner.bind(py);
            tensor.getattr("is_sparse").unwrap().extract().unwrap()
        })
    }

    pub fn is_contiguous(&self) -> bool {
        Python::attach(|py| {
            let tensor = self.inner.bind(py);
            tensor
                .call_method0("is_contiguous")
                .unwrap()
                .extract()
                .unwrap()
        })
    }

    pub fn nbytes(&self) -> i64 {
        Python::attach(|py| {
            let tensor = self.inner.bind(py);
            tensor.getattr("nbytes").unwrap().extract().unwrap()
        })
    }

    pub fn sizes(&self) -> Vec<i64> {
        Python::attach(|py| {
            let tensor = self.inner.bind(py);
            let size = tensor.call_method0("size").unwrap();
            size.try_iter()
                .unwrap()
                .map(|x| x.unwrap().extract().unwrap())
                .collect()
        })
    }
}

impl pyo3::FromPyObject<'_> for Tensor {
    fn extract_bound(ob: &pyo3::Bound<'_, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        Ok(Tensor {
            inner: ob.clone().unbind(),
        })
    }
}

impl<'py> IntoPyObject<'py> for Tensor {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(self.inner.bind(py).clone())
    }
}

#[derive(Debug, Clone)]
pub struct TensorCell {
    tensor: Tensor,
}

impl TensorCell {
    pub fn new(tensor: Tensor) -> Self {
        TensorCell { tensor }
    }

    pub fn borrow(&self) -> BorrowGuard {
        BorrowGuard {
            // SAFETY: TensorCell owns the tensor and the returned BorrowGuard
            // maintains proper ownership semantics by holding a clone.
            tensor: unsafe { self.tensor.clone_unsafe() },
        }
    }

    pub fn borrow_mut(&self) -> BorrowGuardMut {
        BorrowGuardMut {
            // SAFETY: TensorCell owns the tensor and the returned BorrowGuardMut
            // maintains proper ownership semantics by holding a clone.
            tensor: unsafe { self.tensor.clone_unsafe() },
        }
    }

    pub fn aliases(&self, other: &TensorCell) -> bool {
        // Check if two tensors share the same underlying storage
        Python::attach(|_py| {
            let self_ptr = self.tensor.data_ptr();
            let other_ptr = other.tensor.data_ptr();
            self_ptr == other_ptr && !self_ptr.is_null()
        })
    }

    /// # Safety
    /// Caller must ensure that the TensorCell is borrowed appropriately
    pub unsafe fn get_unchecked(&self) -> &Tensor {
        &self.tensor
    }

    pub fn try_borrow(&self) -> Result<BorrowGuard, BorrowError> {
        Ok(self.borrow())
    }

    pub fn try_borrow_mut(&self) -> Result<BorrowGuardMut, BorrowError> {
        Ok(self.borrow_mut())
    }

    pub fn try_cpu(&self) -> Result<TensorCell, BorrowError> {
        Python::attach(|py| {
            let tensor = self.tensor.inner.bind(py);
            let cpu_tensor = tensor
                .call_method0("cpu")
                .map_err(|_| BorrowError::BorrowError)?;
            Ok(TensorCell::new(Tensor {
                inner: cpu_tensor.clone().unbind(),
            }))
        })
    }
}

#[derive(Debug, Clone)]
pub struct BorrowGuard {
    tensor: Tensor,
}

impl std::ops::Deref for BorrowGuard {
    type Target = Tensor;

    fn deref(&self) -> &Self::Target {
        &self.tensor
    }
}

#[derive(Debug, Clone)]
pub struct BorrowGuardMut {
    tensor: Tensor,
}

impl std::ops::Deref for BorrowGuardMut {
    type Target = Tensor;

    fn deref(&self) -> &Self::Target {
        &self.tensor
    }
}

impl std::ops::DerefMut for BorrowGuardMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tensor
    }
}

impl BorrowGuardMut {
    pub fn copy_(&mut self, src: &Tensor) {
        Python::attach(|py| {
            let dst_tensor = self.tensor.inner.bind(py);
            let src_tensor = src.inner.bind(py);
            dst_tensor.call_method1("copy_", (src_tensor,)).unwrap();
        })
    }
}

// ============================================================================
// CloneUnsafe trait
// ============================================================================

pub trait CloneUnsafe {
    /// # Safety
    /// Caller must ensure proper ownership semantics
    unsafe fn clone_unsafe(&self) -> Self;
}

impl CloneUnsafe for Tensor {
    unsafe fn clone_unsafe(&self) -> Self {
        self.clone()
    }
}

// ============================================================================
// Borrow errors
// ============================================================================

#[derive(Debug, Error)]
pub enum BorrowError {
    #[error("borrow error")]
    BorrowError,
}

#[derive(Debug, Clone, Copy)]
pub enum BorrowType {
    Shared,
    Exclusive,
}

#[derive(Debug)]
pub struct Borrow {
    _private: (),
}

#[derive(Debug)]
pub struct MultiBorrow {
    _private: (),
}

// ============================================================================
// Factory functions
// ============================================================================

pub fn factory_zeros(size: &[i64], dtype: ScalarType, layout: Layout, device: Device) -> Tensor {
    Python::attach(|py| {
        let size_tuple = pyo3::types::PyTuple::new(py, size).unwrap();
        let dtype_obj = dtype.into_pyobject(py).unwrap();
        let device_obj = device.into_pyobject(py).unwrap();
        let layout_obj = layout.into_pyobject(py).unwrap();

        let kwargs = pyo3::types::PyDict::new(py);
        kwargs.set_item("dtype", dtype_obj).unwrap();
        kwargs.set_item("device", device_obj).unwrap();
        kwargs.set_item("layout", layout_obj).unwrap();

        let result = torch_zeros(py).call((size_tuple,), Some(&kwargs)).unwrap();

        Tensor {
            inner: result.clone().unbind(),
        }
    })
}

pub fn factory_empty(size: &[i64], dtype: ScalarType, layout: Layout, device: Device) -> Tensor {
    Python::attach(|py| {
        let size_tuple = pyo3::types::PyTuple::new(py, size).unwrap();
        let dtype_obj = dtype.into_pyobject(py).unwrap();
        let device_obj = device.into_pyobject(py).unwrap();
        let layout_obj = layout.into_pyobject(py).unwrap();

        let kwargs = pyo3::types::PyDict::new(py);
        kwargs.set_item("dtype", dtype_obj).unwrap();
        kwargs.set_item("device", device_obj).unwrap();
        kwargs.set_item("layout", layout_obj).unwrap();

        let result = torch_empty(py).call((size_tuple,), Some(&kwargs)).unwrap();

        Tensor {
            inner: result.clone().unbind(),
        }
    })
}

pub fn factory_float_tensor(data: &[f32], device: Device) -> Tensor {
    Python::attach(|py| {
        let data_list = pyo3::types::PyList::new(py, data).unwrap();
        let device_obj = device.into_pyobject(py).unwrap();

        let kwargs = pyo3::types::PyDict::new(py);
        kwargs.set_item("device", device_obj).unwrap();
        kwargs.set_item("dtype", torch_float32(py)).unwrap();

        let result = torch_tensor(py).call((data_list,), Some(&kwargs)).unwrap();

        Tensor {
            inner: result.clone().unbind(),
        }
    })
}

pub fn deep_clone(tensor: &Tensor) -> Tensor {
    Python::attach(|py| {
        let tensor_obj = tensor.inner.bind(py);
        let cloned = tensor_obj.call_method0("clone").unwrap();
        Tensor {
            inner: cloned.clone().unbind(),
        }
    })
}

pub fn is_float8_type(scalar_type: ScalarType) -> bool {
    matches!(
        scalar_type,
        ScalarType::Float8_e5m2
            | ScalarType::Float8_e4m3fn
            | ScalarType::Float8_e5m2fnuz
            | ScalarType::Float8_e4m3fnuz
    )
}

pub fn suggest_memory_format(tensor: &Tensor) -> MemoryFormat {
    Python::attach(|py| {
        let tensor_obj = tensor.inner.bind(py);

        // Call suggest_memory_format method on the tensor
        let result = tensor_obj.call_method0("suggest_memory_format").unwrap();

        // Convert the result back to our enum
        let result_str: String = result.str().unwrap().extract().unwrap();

        if result_str.contains("channels_last_3d") {
            MemoryFormat::ChannelsLast3d
        } else if result_str.contains("channels_last") {
            MemoryFormat::ChannelsLast
        } else if result_str.contains("preserve") {
            MemoryFormat::Preserve
        } else {
            MemoryFormat::Contiguous
        }
    })
}
