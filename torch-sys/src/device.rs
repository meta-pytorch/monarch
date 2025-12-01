/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Bindings for `c10::Device` and friends.

use std::sync::LazyLock;

use cxx::ExternType;
use cxx::type_id;
use derive_more::From;
use derive_more::Into;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use regex::Regex;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

use crate::bridge::ffi;

/// Errors that can be returned from constructing a device from a string.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum DeviceParseError {
    #[error("invalid device type specified: {0}")]
    InvalidDeviceType(String),

    #[error("invalid device index specified: {0}")]
    InvalidDeviceIndex(#[from] std::num::ParseIntError),

    #[error("invalid device string: {0}")]
    ParserFailure(String),
}

/// Binding for `c10::DeviceType`.
///
/// This is an `int8_t` enum class in C++. The reason it looks ridiculous here
/// is because C++ allows the value of an enum to any `int8_t` value, even if
/// there is no discriminant specified. This is UB in Rust, so in order to
/// control for that case, we follow the `cxx` strategy of defining a struct
/// that looks more or less like an enum.
///
/// This is a little pedantic but better safe than sorry :)
#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct DeviceType {
    pub repr: i8,
}

#[allow(dead_code)]
#[allow(non_upper_case_globals)]
impl DeviceType {
    pub const CPU: Self = DeviceType { repr: 0 };
    pub const CUDA: Self = DeviceType { repr: 1 };
    pub const MKLDNN: Self = DeviceType { repr: 2 };
    pub const OPENGL: Self = DeviceType { repr: 3 };
    pub const OPENCL: Self = DeviceType { repr: 4 };
    pub const IDEEP: Self = DeviceType { repr: 5 };
    pub const HIP: Self = DeviceType { repr: 6 };
    pub const FPGA: Self = DeviceType { repr: 7 };
    pub const MAIA: Self = DeviceType { repr: 8 };
    pub const XLA: Self = DeviceType { repr: 9 };
    pub const Vulkan: Self = DeviceType { repr: 10 };
    pub const Metal: Self = DeviceType { repr: 11 };
    pub const XPU: Self = DeviceType { repr: 12 };
    pub const MPS: Self = DeviceType { repr: 13 };
    pub const Meta: Self = DeviceType { repr: 14 };
    pub const HPU: Self = DeviceType { repr: 15 };
    pub const VE: Self = DeviceType { repr: 16 };
    pub const Lazy: Self = DeviceType { repr: 17 };
    pub const IPU: Self = DeviceType { repr: 18 };
    pub const MTIA: Self = DeviceType { repr: 19 };
    pub const PrivateUse1: Self = DeviceType { repr: 20 };
    pub const CompileTimeMaxDeviceTypes: Self = DeviceType { repr: 21 };
}

impl TryFrom<&str> for DeviceType {
    type Error = DeviceParseError;
    fn try_from(val: &str) -> Result<DeviceType, Self::Error> {
        Ok(match val {
            "cpu" => DeviceType::CPU,
            "cuda" => DeviceType::CUDA,
            "ipu" => DeviceType::IPU,
            "xpu" => DeviceType::XPU,
            "mkldnn" => DeviceType::MKLDNN,
            "opengl" => DeviceType::OPENGL,
            "opencl" => DeviceType::OPENCL,
            "ideep" => DeviceType::IDEEP,
            "hip" => DeviceType::HIP,
            "ve" => DeviceType::VE,
            "fpga" => DeviceType::FPGA,
            "maia" => DeviceType::MAIA,
            "xla" => DeviceType::XLA,
            "lazy" => DeviceType::Lazy,
            "vulkan" => DeviceType::Vulkan,
            "mps" => DeviceType::MPS,
            "meta" => DeviceType::Meta,
            "hpu" => DeviceType::HPU,
            "mtia" => DeviceType::MTIA,
            "privateuseone" => DeviceType::PrivateUse1,
            _ => return Err(DeviceParseError::InvalidDeviceType(val.to_string())),
        })
    }
}

impl std::fmt::Display for DeviceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            DeviceType::CPU => write!(f, "cpu"),
            DeviceType::CUDA => write!(f, "cuda"),
            DeviceType::IPU => write!(f, "ipu"),
            DeviceType::XPU => write!(f, "xpu"),
            DeviceType::MKLDNN => write!(f, "mkldnn"),
            DeviceType::OPENGL => write!(f, "opengl"),
            DeviceType::OPENCL => write!(f, "opencl"),
            DeviceType::IDEEP => write!(f, "ideep"),
            DeviceType::HIP => write!(f, "hip"),
            DeviceType::VE => write!(f, "ve"),
            DeviceType::FPGA => write!(f, "fpga"),
            DeviceType::MAIA => write!(f, "maia"),
            DeviceType::XLA => write!(f, "xla"),
            DeviceType::Lazy => write!(f, "lazy"),
            DeviceType::Vulkan => write!(f, "vulkan"),
            DeviceType::MPS => write!(f, "mps"),
            DeviceType::Meta => write!(f, "meta"),
            DeviceType::HPU => write!(f, "hpu"),
            DeviceType::MTIA => write!(f, "mtia"),
            DeviceType::PrivateUse1 => write!(f, "privateuseone"),
            _ => write!(f, "unknown"),
        }
    }
}

// SAFETY: Register our custom type implementation with cxx.
unsafe impl ExternType for DeviceType {
    type Id = type_id!("c10::DeviceType");
    // Yes, it's trivial, it's just an i8.
    type Kind = cxx::kind::Trivial;
}

impl FromPyObject<'_> for DeviceType {
    fn extract_bound(obj: &pyo3::Bound<'_, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        obj.extract::<String>()?
            .as_str()
            .try_into()
            .map_err(|e| PyErr::new::<PyValueError, _>(format!("Failed extracting from py: {}", e)))
    }
}

impl<'py> IntoPyObject<'py> for DeviceType {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        format!("{}", self).into_bound_py_any(py)
    }
}

/// Binding for `c10::DeviceIndex`.
///
/// An index representing a specific device; e.g., the 1 in GPU 1.  A
/// DeviceIndex is not independently meaningful without knowing the DeviceType
/// it is associated; try to use Device rather than DeviceIndex directly.
///
/// Marked `repr(transparent)` because `c10::DeviceType` is really just a type
/// alias for `int8_t`, so we want to guarantee that the representation is
/// identical to that.
#[derive(
    Debug,
    Copy,
    Clone,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    Hash,
    Into,
    From
)]
#[repr(transparent)]
pub struct DeviceIndex(pub i8);

// SAFETY: Register our custom type implementation with cxx.
unsafe impl ExternType for DeviceIndex {
    type Id = type_id!("c10::DeviceIndex");
    // Yes, it's trivial, it's just an i8.
    type Kind = cxx::kind::Trivial;
}

impl FromPyObject<'_> for DeviceIndex {
    fn extract_bound(obj: &pyo3::Bound<'_, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        let v = obj.extract::<Option<i8>>()?;
        Ok(DeviceIndex(v.unwrap_or(-1)))
    }
}

impl<'py> IntoPyObject<'py> for DeviceIndex {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.0.into_bound_py_any(py)
    }
}

/// Binding for `c10::Device`.
///
/// Represents a compute device on which a tensor is located. A device is
/// uniquely identified by a type, which specifies the type of machine it is
/// (e.g. CPU or CUDA GPU), and a device index or ordinal, which identifies the
/// specific compute device when there is more than one of a certain type. The
/// device index is optional, and in its defaulted state represents (abstractly)
/// "the current device". Further, there are two constraints on the value of the
/// device index, if one is explicitly stored:
/// 1. A -1 represents the current device, a non-negative index
///    represents a specific, concrete device,
/// 2. When the device type is CPU, the device index must be -1 or zero.
#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct Device {
    device_type: DeviceType,
    index: DeviceIndex,
}

impl Device {
    /// Create a new device of the specified type on the "current" device index.
    pub fn new(device_type: DeviceType) -> Self {
        Self {
            device_type,
            index: DeviceIndex(-1),
        }
    }

    pub fn new_with_index(device_type: DeviceType, index: DeviceIndex) -> Self {
        debug_assert!(
            index.0 >= -1,
            "Device index must be -1 or non-negative, got: {}",
            index.0
        );
        debug_assert!(
            !matches!(device_type, DeviceType::CPU) || index.0 <= 0,
            "Device index for CPU must be -1 or 0, got: {}",
            index.0
        );
        Self { device_type, index }
    }

    pub fn device_type(&self) -> DeviceType {
        self.device_type
    }

    pub fn index(&self) -> DeviceIndex {
        self.index
    }
}

impl TryFrom<Device> for CudaDevice {
    type Error = &'static str;
    fn try_from(value: Device) -> Result<Self, Self::Error> {
        if value.device_type() == DeviceType::CUDA {
            Ok(CudaDevice { index: value.index })
        } else {
            Err("Device is not a CUDA device")
        }
    }
}

/// A device that is statically guaranteed to be a CUDA device.
#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct CudaDevice {
    index: DeviceIndex,
}

impl CudaDevice {
    pub fn new(index: DeviceIndex) -> Self {
        Self { index }
    }

    pub fn index(&self) -> DeviceIndex {
        self.index
    }
}

impl From<CudaDevice> for Device {
    fn from(device: CudaDevice) -> Self {
        Device::new_with_index(DeviceType::CUDA, device.index)
    }
}

static DEVICE_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new("([a-zA-Z_]+)(?::([1-9]\\d*|0))?").unwrap());

impl TryFrom<&str> for Device {
    type Error = DeviceParseError;
    fn try_from(val: &str) -> Result<Device, Self::Error> {
        let captures = DEVICE_REGEX
            .captures(val)
            .ok_or_else(|| DeviceParseError::ParserFailure(val.to_string()))?;

        if captures.get_match().len() != val.len() {
            return Err(DeviceParseError::ParserFailure(val.to_string()));
        }

        let device_type: DeviceType = captures
            .get(1)
            .ok_or_else(|| DeviceParseError::ParserFailure(val.to_string()))?
            .as_str()
            .try_into()?;

        let index = captures.get(2);
        match index {
            Some(match_) => Ok(Device::new_with_index(
                device_type,
                DeviceIndex(
                    match_
                        .as_str()
                        .parse::<i8>()
                        .map_err(DeviceParseError::from)?,
                ),
            )),
            None => Ok(Device::new(device_type)),
        }
    }
}

impl TryFrom<String> for Device {
    type Error = DeviceParseError;
    fn try_from(val: String) -> Result<Device, Self::Error> {
        Device::try_from(val.as_ref())
    }
}

impl std::fmt::Display for Device {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.index.0 == -1 {
            write!(f, "{}", self.device_type)
        } else {
            write!(f, "{}:{}", self.device_type, self.index.0)
        }
    }
}

// SAFETY: Register our custom type implementation with cxx.
unsafe impl ExternType for Device {
    type Id = type_id!("c10::Device");
    // Yes, it's trivial, it's just two i8s.
    type Kind = cxx::kind::Trivial;
}

impl FromPyObject<'_> for Device {
    fn extract_bound(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        ffi::device_from_py_object(obj.into()).map_err(|e| {
            PyValueError::new_err(format!(
                "Failed extracting {} from py as Device: {}",
                obj, e
            ))
        })
    }
}

impl<'py> IntoPyObject<'py> for Device {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        ffi::device_to_py_object(self).into_pyobject(py)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::*;

    #[test]
    fn from_str_bad() {
        let result: Result<Device, DeviceParseError> = "a@#fij".try_into();
        assert!(matches!(result, Err(DeviceParseError::ParserFailure(_))));

        let result: Result<Device, DeviceParseError> = "asdf:4".try_into();
        assert!(matches!(
            result,
            Err(DeviceParseError::InvalidDeviceType(_))
        ));
    }

    #[test]
    fn from_str_good() {
        let device: Device = "cuda".try_into().unwrap();
        assert!(matches!(device.device_type(), DeviceType::CUDA));
        assert_eq!(device.index(), DeviceIndex(-1));
    }

    #[test]
    fn from_str_index() {
        let device: Device = "cuda:5".try_into().unwrap();
        assert!(matches!(device.device_type(), DeviceType::CUDA));
        assert_eq!(device.index(), DeviceIndex(5));
    }

    #[test]
    fn device_type_convert_to_py_and_back() -> Result<()> {
        pyo3::prepare_freethreaded_python();
        let expected: DeviceType = DeviceType::CUDA;
        let actual = Python::with_gil(|py| {
            // import torch to ensure torch.dtype types are registered
            py.import("torch")?;
            let obj = expected.clone().into_pyobject(py)?;
            obj.extract::<DeviceType>()
        })?;
        assert_eq!(actual, expected);
        Ok(())
    }

    #[test]
    fn device_index_convert_to_py_and_back() -> Result<()> {
        pyo3::prepare_freethreaded_python();
        let expected: DeviceIndex = 3.into();
        let actual = Python::with_gil(|py| {
            // import torch to ensure torch.dtype types are registered
            py.import("torch")?;
            let obj = expected.clone().into_pyobject(py)?;
            obj.extract::<DeviceIndex>()
        })?;
        assert_eq!(actual, expected);
        Ok(())
    }

    #[test]
    fn device_convert_to_py_and_back() -> Result<()> {
        pyo3::prepare_freethreaded_python();
        let expected: Device = "cuda:2".try_into()?;
        let actual = Python::with_gil(|py| {
            // import torch to ensure torch.dtype types are registered
            py.import("torch")?;
            let obj = expected.clone().into_pyobject(py)?;
            obj.extract::<Device>()
        })?;
        assert_eq!(actual, expected);
        Ok(())
    }

    #[test]
    fn device_from_py() -> Result<()> {
        pyo3::prepare_freethreaded_python();
        let expected: Device = "cuda:2".try_into()?;
        let actual = Python::with_gil(|py| {
            let obj = py.import("torch")?.getattr("device")?.call1(("cuda:2",))?;
            obj.extract::<Device>()
        })?;
        assert_eq!(actual, expected);
        Ok(())
    }
}
