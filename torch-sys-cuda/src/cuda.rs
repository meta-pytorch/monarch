/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Bindings for torch's wrappers around CUDA-related functionality.
use std::time::Duration;

use derive_more::Into;
use monarch_types::py_global;
use nccl_sys::cudaError_t;
use nccl_sys::cudaSetDevice;
use nccl_sys::cudaStream_t;
use pyo3::Py;
use pyo3::PyAny;
use pyo3::prelude::*;
use thiserror::Error;
use torch_sys2::CudaDevice;

// Cached imports for torch.cuda APIs
py_global!(cuda_stream_class, "torch.cuda", "Stream");
py_global!(cuda_event_class, "torch.cuda", "Event");
py_global!(cuda_current_stream, "torch.cuda", "current_stream");
py_global!(cuda_set_stream, "torch.cuda", "set_stream");
py_global!(cuda_current_device, "torch.cuda", "current_device");
py_global!(cuda_set_device, "torch.cuda", "set_device");

/// Wrapper around a CUDA stream.
///
/// A CUDA stream is a linear sequence of execution that belongs to a specific
/// device, independent from other streams.  See the documentation for
/// `torch.cuda.Stream` for more details.
#[derive(Debug, Into)]
#[into(ref)]
pub struct Stream {
    inner: Py<PyAny>,
}

impl Clone for Stream {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            inner: self.inner.clone_ref(py),
        })
    }
}

impl Stream {
    /// Create a new stream on the current device, at priority 0.
    pub fn new() -> Self {
        Python::attach(|py| {
            let stream = cuda_stream_class(py).call0().unwrap();
            Self {
                inner: stream.into(),
            }
        })
    }

    /// Clone this stream reference. Requires holding the GIL.
    pub fn clone_ref(&self, py: Python<'_>) -> Self {
        Self {
            inner: self.inner.clone_ref(py),
        }
    }

    /// Create a new stream on the specified device, at priority 0.
    pub fn new_with_device(device: CudaDevice) -> Self {
        Python::attach(|py| {
            let device_idx: i8 = device.index().into();
            let stream = cuda_stream_class(py).call1((device_idx,)).unwrap();
            Self {
                inner: stream.into(),
            }
        })
    }

    /// Get the current stream on the current device.
    pub fn get_current_stream() -> Self {
        Python::attach(|py| {
            let stream = cuda_current_stream(py).call0().unwrap();
            Self {
                inner: stream.into(),
            }
        })
    }

    /// Get the current stream on the specified device.
    pub fn get_current_stream_on_device(device: CudaDevice) -> Self {
        Python::attach(|py| {
            let device_idx: i8 = device.index().into();
            let stream = cuda_current_stream(py).call1((device_idx,)).unwrap();
            Self {
                inner: stream.into(),
            }
        })
    }

    /// Set the provided stream as the current stream. Also sets the current
    /// device to be the same as the stream's device.
    pub fn set_current_stream(stream: &Stream) {
        Python::attach(|py| {
            let stream_obj = stream.inner.bind(py);

            // Get current device and stream device
            let current_device = cuda_current_device(py)
                .call0()
                .unwrap()
                .extract::<i64>()
                .unwrap();

            let stream_device = stream_obj
                .getattr("device_index")
                .unwrap()
                .extract::<i64>()
                .unwrap();

            // Set device if different
            if current_device != stream_device {
                cuda_set_device(py).call1((stream_device,)).unwrap();
            }

            // Set the stream
            cuda_set_stream(py).call1((stream_obj,)).unwrap();
        })
    }

    /// Make all future work submitted to this stream wait for an event.
    pub fn wait_event(&self, event: &mut Event) {
        event.wait(Some(self))
    }

    /// Synchronize with another stream.
    ///
    /// All future work submitted to this stream will wait until all kernels
    /// submitted to a given stream at the time of call entry complete.
    pub fn wait_stream(&self, stream: &Stream) {
        self.wait_event(&mut stream.record_event(None))
    }

    /// Record an event on this stream. If no event is provided one will be
    /// created.
    pub fn record_event(&self, event: Option<Event>) -> Event {
        let mut event = event.unwrap_or(Event::new());
        event.record(Some(self));
        event
    }

    /// Check if all work submitted to this stream has completed.
    pub fn query(&self) -> bool {
        Python::attach(|py| {
            let stream_obj = self.inner.bind(py);
            stream_obj
                .call_method0("query")
                .unwrap()
                .extract::<bool>()
                .unwrap()
        })
    }

    /// Wait for all kernels in this stream to complete.
    pub fn synchronize(&self) {
        Python::attach(|py| {
            let stream_obj = self.inner.bind(py);
            stream_obj.call_method0("synchronize").unwrap();
        })
    }

    pub fn stream(&self) -> cudaStream_t {
        Python::attach(|py| {
            let stream_obj = self.inner.bind(py);
            let cuda_stream = stream_obj.getattr("cuda_stream").unwrap();

            // Extract the raw pointer
            let ptr = cuda_stream.extract::<usize>().unwrap();

            ptr as cudaStream_t
        })
    }
}

impl PartialEq for Stream {
    fn eq(&self, other: &Self) -> bool {
        self.stream() == other.stream()
    }
}

/// Wrapper around a CUDA event.
///
/// CUDA events are synchronization markers that can be used to monitor the
/// device's progress, to accurately measure timing, and to synchronize CUDA
/// streams.
///
/// The underlying CUDA events are lazily initialized when the event is first
/// recorded or exported to another process. After creation, only streams on the
/// same device may record the event. However, streams on any device can wait on
/// the event.
///
/// See the docs of `torch.cuda.Event` for more details.
#[derive(Debug)]
pub struct Event {
    inner: Py<PyAny>,
}

impl Clone for Event {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            inner: self.inner.clone_ref(py),
        })
    }
}

impl Event {
    /// Create a new event.
    // TODO: add support for flags.
    pub fn new() -> Self {
        Python::attach(|py| {
            let event = cuda_event_class(py).call0().unwrap();
            Self {
                inner: event.into(),
            }
        })
    }

    /// Record the event on the current stream.
    ///
    /// Uses the current stream if no stream is provided.
    pub fn record(&mut self, stream: Option<&Stream>) {
        Python::attach(|py| {
            let event_obj = self.inner.bind(py);

            match stream {
                Some(stream) => {
                    let stream_obj = stream.inner.bind(py);
                    event_obj.call_method1("record", (stream_obj,)).unwrap();
                }
                None => {
                    event_obj.call_method0("record").unwrap();
                }
            }
        })
    }

    /// Make all future work submitted to the given stream wait for this event.
    ///
    /// Uses the current stream if no stream is specified.
    pub fn wait(&mut self, stream: Option<&Stream>) {
        Python::attach(|py| {
            let event_obj = self.inner.bind(py);

            match stream {
                Some(stream) => {
                    let stream_obj = stream.inner.bind(py);
                    event_obj.call_method1("wait", (stream_obj,)).unwrap();
                }
                None => {
                    event_obj.call_method0("wait").unwrap();
                }
            }
        })
    }

    /// Check if all work currently captured by event has completed.
    pub fn query(&self) -> bool {
        Python::attach(|py| {
            let event_obj = self.inner.bind(py);
            event_obj
                .call_method0("query")
                .unwrap()
                .extract::<bool>()
                .unwrap()
        })
    }

    /// Return the time elapsed.
    ///
    /// Time reported in after the event was recorded and before the end_event
    /// was recorded.
    pub fn elapsed_time(&self, end_event: &Event) -> Duration {
        Python::attach(|py| {
            let event_obj = self.inner.bind(py);
            let end_event_obj = end_event.inner.bind(py);

            let elapsed_ms = event_obj
                .call_method1("elapsed_time", (end_event_obj,))
                .unwrap()
                .extract::<f64>()
                .unwrap();

            Duration::from_millis(elapsed_ms as u64)
        })
    }

    /// Wait for the event to complete.
    /// Waits until the completion of all work currently captured in this event.
    /// This prevents the CPU thread from proceeding until the event completes.
    pub fn synchronize(&self) {
        Python::attach(|py| {
            let event_obj = self.inner.bind(py);
            event_obj.call_method0("synchronize").unwrap();
        })
    }
}

/// Corresponds to the CUDA error codes.
#[derive(Debug, Error)]
pub enum CudaError {
    #[error(
        "one or more parameters passed to the API call is not within an acceptable range of values"
    )]
    InvalidValue,
    #[error("the API call failed due to insufficient memory or resources")]
    MemoryAllocation,
    #[error("failed to initialize the CUDA driver and runtime")]
    InitializationError,
    #[error("CUDA Runtime API call was executed after the CUDA driver has been unloaded")]
    CudartUnloading,
    #[error("profiler is not initialized for this run, possibly due to an external profiling tool")]
    ProfilerDisabled,
    #[error("deprecated. Attempted to enable/disable profiling without initialization")]
    ProfilerNotInitialized,
    #[error("deprecated. Profiling is already started")]
    ProfilerAlreadyStarted,
    #[error("deprecated. Profiling is already stopped")]
    ProfilerAlreadyStopped,
    #[error("kernel launch requested resources that cannot be satisfied by the current device")]
    InvalidConfiguration,
    #[error("one or more of the pitch-related parameters passed to the API call is out of range")]
    InvalidPitchValue,
    #[error("the symbol name/identifier passed to the API call is invalid")]
    InvalidSymbol,
    #[error("the host pointer passed to the API call is invalid")]
    InvalidHostPointer,
    #[error("the device pointer passed to the API call is invalid")]
    InvalidDevicePointer,
    #[error("the texture passed to the API call is invalid")]
    InvalidTexture,
    #[error("the texture binding is invalid")]
    InvalidTextureBinding,
    #[error("the channel descriptor passed to the API call is invalid")]
    InvalidChannelDescriptor,
    #[error("the direction of the memcpy operation is invalid")]
    InvalidMemcpyDirection,
    #[error(
        "attempted to take the address of a constant variable, which is forbidden before CUDA 3.1"
    )]
    AddressOfConstant,
    #[error("deprecated. A texture fetch operation failed")]
    TextureFetchFailed,
    #[error("deprecated. The texture is not bound for access")]
    TextureNotBound,
    #[error("a synchronization operation failed")]
    SynchronizationError,
    #[error(
        "a non-float texture was accessed with linear filtering, which is not supported by CUDA"
    )]
    InvalidFilterSetting,
    #[error(
        "attempted to read a non-float texture as a normalized float, which is not supported by CUDA"
    )]
    InvalidNormSetting,
    #[error("the API call is not yet implemented")]
    NotYetImplemented,
    #[error("an emulated device pointer exceeded the 32-bit address range")]
    MemoryValueTooLarge,
    #[error("the CUDA driver is a stub library")]
    StubLibrary,
    #[error("the installed NVIDIA CUDA driver is older than the CUDA runtime library")]
    InsufficientDriver,
    #[error("the API call requires a newer CUDA driver")]
    CallRequiresNewerDriver,
    #[error("the surface passed to the API call is invalid")]
    InvalidSurface,
    #[error("multiple global or constant variables share the same string name")]
    DuplicateVariableName,
    #[error("multiple textures share the same string name")]
    DuplicateTextureName,
    #[error("multiple surfaces share the same string name")]
    DuplicateSurfaceName,
    #[error("all CUDA devices are currently busy or unavailable")]
    DevicesUnavailable,
    #[error("the current CUDA context is not compatible with the runtime")]
    IncompatibleDriverContext,
    #[error("the device function being invoked was not previously configured")]
    MissingConfiguration,
    #[error("a previous kernel launch failed")]
    PriorLaunchFailure,
    #[error(
        "the depth of the child grid exceeded the maximum supported number of nested grid launches"
    )]
    LaunchMaxDepthExceeded,
    #[error("a grid launch did not occur because file-scoped textures are unsupported")]
    LaunchFileScopedTex,
    #[error("a grid launch did not occur because file-scoped surfaces are unsupported")]
    LaunchFileScopedSurf,
    #[error("a call to cudaDeviceSynchronize failed due to exceeding the sync depth")]
    SyncDepthExceeded,
    #[error(
        "a grid launch failed because the launch exceeded the limit of pending device runtime launches"
    )]
    LaunchPendingCountExceeded,
    #[error(
        "the requested device function does not exist or is not compiled for the proper device architecture"
    )]
    InvalidDeviceFunction,
    #[error("no CUDA-capable devices were detected")]
    NoDevice,
    #[error("the device ordinal supplied does not correspond to a valid CUDA device")]
    InvalidDevice,
    #[error("the device does not have a valid Grid License")]
    DeviceNotLicensed,
    #[error("an internal startup failure occurred in the CUDA runtime")]
    StartupFailure,
    #[error("the device kernel image is invalid")]
    InvalidKernelImage,
    #[error("the device is not initialized")]
    DeviceUninitialized,
    #[error("the buffer object could not be mapped")]
    MapBufferObjectFailed,
    #[error("the buffer object could not be unmapped")]
    UnmapBufferObjectFailed,
    #[error("the specified array is currently mapped and cannot be destroyed")]
    ArrayIsMapped,
    #[error("the resource is already mapped")]
    AlreadyMapped,
    #[error("there is no kernel image available that is suitable for the device")]
    NoKernelImageForDevice,
    #[error("the resource has already been acquired")]
    AlreadyAcquired,
    #[error("the resource is not mapped")]
    NotMapped,
    #[error("the mapped resource is not available for access as an array")]
    NotMappedAsArray,
    #[error("the mapped resource is not available for access as a pointer")]
    NotMappedAsPointer,
    #[error("an uncorrectable ECC error was detected")]
    ECCUncorrectable,
    #[error("the specified cudaLimit is not supported by the device")]
    UnsupportedLimit,
    #[error("a call tried to access an exclusive-thread device that is already in use")]
    DeviceAlreadyInUse,
    #[error("P2P access is not supported across the given devices")]
    PeerAccessUnsupported,
    #[error("a PTX compilation failed")]
    InvalidPtx,
    #[error("an error occurred with the OpenGL or DirectX context")]
    InvalidGraphicsContext,
    #[error("an uncorrectable NVLink error was detected during execution")]
    NvlinkUncorrectable,
    #[error("the PTX JIT compiler library was not found")]
    JitCompilerNotFound,
    #[error("the provided PTX was compiled with an unsupported toolchain")]
    UnsupportedPtxVersion,
    #[error("JIT compilation was disabled")]
    JitCompilationDisabled,
    #[error("the provided execution affinity is not supported by the device")]
    UnsupportedExecAffinity,
    #[error("the operation is not permitted when the stream is capturing")]
    StreamCaptureUnsupported,
    #[error(
        "the current capture sequence on the stream has been invalidated due to a previous error"
    )]
    StreamCaptureInvalidated,
    #[error("a merge of two independent capture sequences was not allowed")]
    StreamCaptureMerge,
    #[error("the capture was not initiated in this stream")]
    StreamCaptureUnmatched,
    #[error("a stream capture sequence was passed to cudaStreamEndCapture in a different thread")]
    StreamCaptureWrongThread,
    #[error("the wait operation has timed out")]
    Timeout,
    #[error("an unknown internal error occurred")]
    Unknown,
    #[error("the API call returned a failure")]
    ApiFailureBase,
}

pub fn cuda_check(result: cudaError_t) -> Result<(), CudaError> {
    match result.0 {
        0 => Ok(()),
        1 => Err(CudaError::InvalidValue),
        2 => Err(CudaError::MemoryAllocation),
        3 => Err(CudaError::InitializationError),
        4 => Err(CudaError::CudartUnloading),
        5 => Err(CudaError::ProfilerDisabled),
        6 => Err(CudaError::ProfilerNotInitialized),
        7 => Err(CudaError::ProfilerAlreadyStarted),
        8 => Err(CudaError::ProfilerAlreadyStopped),
        9 => Err(CudaError::InvalidConfiguration),
        12 => Err(CudaError::InvalidPitchValue),
        13 => Err(CudaError::InvalidSymbol),
        16 => Err(CudaError::InvalidHostPointer),
        17 => Err(CudaError::InvalidDevicePointer),
        18 => Err(CudaError::InvalidTexture),
        19 => Err(CudaError::InvalidTextureBinding),
        20 => Err(CudaError::InvalidChannelDescriptor),
        21 => Err(CudaError::InvalidMemcpyDirection),
        22 => Err(CudaError::AddressOfConstant),
        23 => Err(CudaError::TextureFetchFailed),
        24 => Err(CudaError::TextureNotBound),
        25 => Err(CudaError::SynchronizationError),
        26 => Err(CudaError::InvalidFilterSetting),
        27 => Err(CudaError::InvalidNormSetting),
        31 => Err(CudaError::NotYetImplemented),
        32 => Err(CudaError::MemoryValueTooLarge),
        34 => Err(CudaError::StubLibrary),
        35 => Err(CudaError::InsufficientDriver),
        36 => Err(CudaError::CallRequiresNewerDriver),
        37 => Err(CudaError::InvalidSurface),
        43 => Err(CudaError::DuplicateVariableName),
        44 => Err(CudaError::DuplicateTextureName),
        45 => Err(CudaError::DuplicateSurfaceName),
        46 => Err(CudaError::DevicesUnavailable),
        49 => Err(CudaError::IncompatibleDriverContext),
        52 => Err(CudaError::MissingConfiguration),
        53 => Err(CudaError::PriorLaunchFailure),
        65 => Err(CudaError::LaunchMaxDepthExceeded),
        66 => Err(CudaError::LaunchFileScopedTex),
        67 => Err(CudaError::LaunchFileScopedSurf),
        68 => Err(CudaError::SyncDepthExceeded),
        69 => Err(CudaError::LaunchPendingCountExceeded),
        98 => Err(CudaError::InvalidDeviceFunction),
        100 => Err(CudaError::NoDevice),
        101 => Err(CudaError::InvalidDevice),
        102 => Err(CudaError::DeviceNotLicensed),
        127 => Err(CudaError::StartupFailure),
        200 => Err(CudaError::InvalidKernelImage),
        201 => Err(CudaError::DeviceUninitialized),
        205 => Err(CudaError::MapBufferObjectFailed),
        206 => Err(CudaError::UnmapBufferObjectFailed),
        207 => Err(CudaError::ArrayIsMapped),
        208 => Err(CudaError::AlreadyMapped),
        209 => Err(CudaError::NoKernelImageForDevice),
        210 => Err(CudaError::AlreadyAcquired),
        211 => Err(CudaError::NotMapped),
        212 => Err(CudaError::NotMappedAsArray),
        213 => Err(CudaError::NotMappedAsPointer),
        214 => Err(CudaError::ECCUncorrectable),
        215 => Err(CudaError::UnsupportedLimit),
        216 => Err(CudaError::DeviceAlreadyInUse),
        217 => Err(CudaError::PeerAccessUnsupported),
        218 => Err(CudaError::InvalidPtx),
        219 => Err(CudaError::InvalidGraphicsContext),
        220 => Err(CudaError::NvlinkUncorrectable),
        221 => Err(CudaError::JitCompilerNotFound),
        222 => Err(CudaError::UnsupportedPtxVersion),
        223 => Err(CudaError::JitCompilationDisabled),
        224 => Err(CudaError::UnsupportedExecAffinity),
        900 => Err(CudaError::StreamCaptureUnsupported),
        901 => Err(CudaError::StreamCaptureInvalidated),
        902 => Err(CudaError::StreamCaptureMerge),
        903 => Err(CudaError::StreamCaptureUnmatched),
        904 => Err(CudaError::StreamCaptureWrongThread),
        909 => Err(CudaError::Timeout),
        999 => Err(CudaError::Unknown),
        _ => panic!("Unknown cudaError_t: {:?}", result.0),
    }
}

pub fn set_device(device: CudaDevice) -> Result<(), CudaError> {
    let index: i8 = device.index().into();
    // SAFETY: intended usage of this function
    unsafe { cuda_check(cudaSetDevice(index.into())) }
}
