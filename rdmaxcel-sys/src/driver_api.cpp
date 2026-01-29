/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "driver_api.h"
#include <cuda_runtime.h>
#include <dlfcn.h>
#include <iostream>
#include <stdexcept>

// List of GPU driver functions needed by rdmaxcel
// hipify_torch does not convert these Driver API function names automatically
#ifdef USE_ROCM
  #define RDMAXCEL_CUDA_DRIVER_API(_)      \
    _(hipMemGetHandleForAddressRange)      \
    _(hipMemGetAllocationGranularity)      \
    _(hipMemCreate)                        \
    _(hipMemAddressReserve)                \
    _(hipMemMap)                           \
    _(hipMemSetAccess)                     \
    _(hipMemUnmap)                         \
    _(hipMemAddressFree)                   \
    _(hipMemRelease)                       \
    _(hipMemcpyHtoD)                       \
    _(hipMemcpyDtoH)                       \
    _(hipMemsetD8)                         \
    _(hipPointerGetAttribute)              \
    _(hipInit)                             \
    _(hipDeviceGet)                        \
    _(hipGetDeviceCount)                   \
    _(hipDeviceGetAttribute)               \
    _(hipCtxCreate)                        \
    _(hipCtxSetCurrent)                    \
    _(hipCtxSynchronize)                   \
    _(hipDrvGetErrorString)
#else
  #define RDMAXCEL_CUDA_DRIVER_API(_)      \
    _(cuMemGetHandleForAddressRange)       \
    _(cuMemGetAllocationGranularity)       \
    _(cuMemCreate)                         \
    _(cuMemAddressReserve)                 \
    _(cuMemMap)                            \
    _(cuMemSetAccess)                      \
    _(cuMemUnmap)                          \
    _(cuMemAddressFree)                    \
    _(cuMemRelease)                        \
    _(cuMemcpyHtoD_v2)                     \
    _(cuMemcpyDtoH_v2)                     \
    _(cuMemsetD8_v2)                       \
    _(cuPointerGetAttribute)               \
    _(cuInit)                              \
    _(cuDeviceGet)                         \
    _(cuDeviceGetCount)                    \
    _(cuDeviceGetAttribute)                \
    _(cuCtxCreate_v2)                      \
    _(cuCtxSetCurrent)                     \
    _(cuCtxSynchronize)                    \
    _(cuGetErrorString)
#endif

namespace rdmaxcel {

struct DriverAPI {
#define CREATE_MEMBER(name) decltype(&name) name##_;
  RDMAXCEL_CUDA_DRIVER_API(CREATE_MEMBER)
#undef CREATE_MEMBER
  static DriverAPI* get();
};

namespace {

DriverAPI create_driver_api() {
#ifdef USE_ROCM
  // Try to open libamdhip64.so - RTLD_NOLOAD means only succeed if already loaded
  void* handle = dlopen("libamdhip64.so", RTLD_LAZY | RTLD_NOLOAD);
  if (!handle) {
    std::cerr
        << "[RdmaXcel] Warning: libamdhip64.so not loaded, trying to load it now"
        << std::endl;
    handle = dlopen("libamdhip64.so", RTLD_LAZY);
  }

  if (!handle) {
    throw std::runtime_error(
        std::string("[RdmaXcel] Can't open libamdhip64.so: ") + dlerror());
  }
#else
  // Try to open libcuda.so.1 - RTLD_NOLOAD means only succeed if already loaded
  void* handle = dlopen("libcuda.so.1", RTLD_LAZY | RTLD_NOLOAD);
  if (!handle) {
    std::cerr
        << "[RdmaXcel] Warning: libcuda.so.1 not loaded, trying to load it now"
        << std::endl;
    handle = dlopen("libcuda.so.1", RTLD_LAZY);
  }

  if (!handle) {
    throw std::runtime_error(
        std::string("[RdmaXcel] Can't open libcuda.so.1: ") + dlerror());
  }
#endif

  DriverAPI r{};

#define LOOKUP_CUDA_ENTRY(name)                                            \
  r.name##_ = reinterpret_cast<decltype(&name)>(dlsym(handle, #name));     \
  if (!r.name##_) {                                                        \
    throw std::runtime_error(                                              \
        std::string("[RdmaXcel] Can't find ") + #name + ": " + dlerror()); \
  }

  RDMAXCEL_CUDA_DRIVER_API(LOOKUP_CUDA_ENTRY)
#undef LOOKUP_CUDA_ENTRY

  return r;
}

} // namespace

DriverAPI* DriverAPI::get() {
  // Ensure we have a valid CUDA context for this thread
  cudaFree(0);
  static DriverAPI singleton = create_driver_api();
  return &singleton;
}

} // namespace rdmaxcel

// C API wrapper implementations
extern "C" {

// Memory management
CUresult rdmaxcel_cuMemGetHandleForAddressRange(
    int* handle,
    CUdeviceptr dptr,
    size_t size,
    CUmemRangeHandleType handleType,
    unsigned long long flags) {
#ifdef USE_ROCM
  return rdmaxcel::DriverAPI::get()->hipMemGetHandleForAddressRange_(
      handle, dptr, size, handleType, flags);
#else
  return rdmaxcel::DriverAPI::get()->cuMemGetHandleForAddressRange_(
      handle, dptr, size, handleType, flags);
#endif
}

CUresult rdmaxcel_cuMemGetAllocationGranularity(
    size_t* granularity,
    const CUmemAllocationProp* prop,
    CUmemAllocationGranularity_flags option) {
#ifdef USE_ROCM
  return rdmaxcel::DriverAPI::get()->hipMemGetAllocationGranularity_(
      granularity, prop, option);
#else
  return rdmaxcel::DriverAPI::get()->cuMemGetAllocationGranularity_(
      granularity, prop, option);
#endif
}

CUresult rdmaxcel_cuMemCreate(
    CUmemGenericAllocationHandle* handle,
    size_t size,
    const CUmemAllocationProp* prop,
    unsigned long long flags) {
#ifdef USE_ROCM
  return rdmaxcel::DriverAPI::get()->hipMemCreate_(handle, size, prop, flags);
#else
  return rdmaxcel::DriverAPI::get()->cuMemCreate_(handle, size, prop, flags);
#endif
}

CUresult rdmaxcel_cuMemAddressReserve(
    CUdeviceptr* ptr,
    size_t size,
    size_t alignment,
    CUdeviceptr addr,
    unsigned long long flags) {
#ifdef USE_ROCM
  return rdmaxcel::DriverAPI::get()->hipMemAddressReserve_(
      ptr, size, alignment, addr, flags);
#else
  return rdmaxcel::DriverAPI::get()->cuMemAddressReserve_(
      ptr, size, alignment, addr, flags);
#endif
}

CUresult rdmaxcel_cuMemMap(
    CUdeviceptr ptr,
    size_t size,
    size_t offset,
    CUmemGenericAllocationHandle handle,
    unsigned long long flags) {
#ifdef USE_ROCM
  return rdmaxcel::DriverAPI::get()->hipMemMap_(
      ptr, size, offset, handle, flags);
#else
  return rdmaxcel::DriverAPI::get()->cuMemMap_(
      ptr, size, offset, handle, flags);
#endif
}

CUresult rdmaxcel_cuMemSetAccess(
    CUdeviceptr ptr,
    size_t size,
    const CUmemAccessDesc* desc,
    size_t count) {
#ifdef USE_ROCM
  return rdmaxcel::DriverAPI::get()->hipMemSetAccess_(ptr, size, desc, count);
#else
  return rdmaxcel::DriverAPI::get()->cuMemSetAccess_(ptr, size, desc, count);
#endif
}

CUresult rdmaxcel_cuMemUnmap(CUdeviceptr ptr, size_t size) {
#ifdef USE_ROCM
  return rdmaxcel::DriverAPI::get()->hipMemUnmap_(ptr, size);
#else
  return rdmaxcel::DriverAPI::get()->cuMemUnmap_(ptr, size);
#endif
}

CUresult rdmaxcel_cuMemAddressFree(CUdeviceptr ptr, size_t size) {
#ifdef USE_ROCM
  return rdmaxcel::DriverAPI::get()->hipMemAddressFree_(ptr, size);
#else
  return rdmaxcel::DriverAPI::get()->cuMemAddressFree_(ptr, size);
#endif
}

CUresult rdmaxcel_cuMemRelease(CUmemGenericAllocationHandle handle) {
#ifdef USE_ROCM
  return rdmaxcel::DriverAPI::get()->hipMemRelease_(handle);
#else
  return rdmaxcel::DriverAPI::get()->cuMemRelease_(handle);
#endif
}

CUresult rdmaxcel_cuMemcpyHtoD_v2(
    CUdeviceptr dstDevice,
    const void* srcHost,
    size_t ByteCount) {
#ifdef USE_ROCM
  return rdmaxcel::DriverAPI::get()->hipMemcpyHtoD_(
      dstDevice, srcHost, ByteCount);
#else
  return rdmaxcel::DriverAPI::get()->cuMemcpyHtoD_v2_(
      dstDevice, srcHost, ByteCount);
#endif
}

CUresult rdmaxcel_cuMemcpyDtoH_v2(
    void* dstHost,
    CUdeviceptr srcDevice,
    size_t ByteCount) {
#ifdef USE_ROCM
  return rdmaxcel::DriverAPI::get()->hipMemcpyDtoH_(
      dstHost, srcDevice, ByteCount);
#else
  return rdmaxcel::DriverAPI::get()->cuMemcpyDtoH_v2_(
      dstHost, srcDevice, ByteCount);
#endif
}

CUresult
rdmaxcel_cuMemsetD8_v2(CUdeviceptr dstDevice, unsigned char uc, size_t N) {
#ifdef USE_ROCM
  return rdmaxcel::DriverAPI::get()->hipMemsetD8_(dstDevice, uc, N);
#else
  return rdmaxcel::DriverAPI::get()->cuMemsetD8_v2_(dstDevice, uc, N);
#endif
}

// Pointer queries
CUresult rdmaxcel_cuPointerGetAttribute(
    void* data,
    CUpointer_attribute attribute,
    CUdeviceptr ptr) {
#ifdef USE_ROCM
  return rdmaxcel::DriverAPI::get()->hipPointerGetAttribute_(
      data, attribute, ptr);
#else
  return rdmaxcel::DriverAPI::get()->cuPointerGetAttribute_(
      data, attribute, ptr);
#endif
}

// Device management
CUresult rdmaxcel_cuInit(unsigned int Flags) {
#ifdef USE_ROCM
  return rdmaxcel::DriverAPI::get()->hipInit_(Flags);
#else
  return rdmaxcel::DriverAPI::get()->cuInit_(Flags);
#endif
}

CUresult rdmaxcel_cuDeviceGet(CUdevice* device, int ordinal) {
#ifdef USE_ROCM
  return rdmaxcel::DriverAPI::get()->hipDeviceGet_(device, ordinal);
#else
  return rdmaxcel::DriverAPI::get()->cuDeviceGet_(device, ordinal);
#endif
}

CUresult rdmaxcel_cuDeviceGetCount(int* count) {
#ifdef USE_ROCM
  return rdmaxcel::DriverAPI::get()->hipGetDeviceCount_(count);
#else
  return rdmaxcel::DriverAPI::get()->cuDeviceGetCount_(count);
#endif
}

CUresult rdmaxcel_cuDeviceGetAttribute(
    int* pi,
    CUdevice_attribute attrib,
    CUdevice dev) {
#ifdef USE_ROCM
  return rdmaxcel::DriverAPI::get()->hipDeviceGetAttribute_(pi, attrib, dev);
#else
  return rdmaxcel::DriverAPI::get()->cuDeviceGetAttribute_(pi, attrib, dev);
#endif
}

// Context management
CUresult
rdmaxcel_cuCtxCreate_v2(CUcontext* pctx, unsigned int flags, CUdevice dev) {
#ifdef USE_ROCM
  return rdmaxcel::DriverAPI::get()->hipCtxCreate_(pctx, flags, dev);
#else
  return rdmaxcel::DriverAPI::get()->cuCtxCreate_v2_(pctx, flags, dev);
#endif
}

CUresult rdmaxcel_cuCtxSetCurrent(CUcontext ctx) {
#ifdef USE_ROCM
  return rdmaxcel::DriverAPI::get()->hipCtxSetCurrent_(ctx);
#else
  return rdmaxcel::DriverAPI::get()->cuCtxSetCurrent_(ctx);
#endif
}

CUresult rdmaxcel_cuCtxSynchronize(void) {
#ifdef USE_ROCM
  return rdmaxcel::DriverAPI::get()->hipCtxSynchronize_();
#else
  return rdmaxcel::DriverAPI::get()->cuCtxSynchronize_();
#endif
}

// Error handling
CUresult rdmaxcel_cuGetErrorString(CUresult error, const char** pStr) {
#ifdef USE_ROCM
  return rdmaxcel::DriverAPI::get()->hipDrvGetErrorString_(error, pStr);
#else
  return rdmaxcel::DriverAPI::get()->cuGetErrorString_(error, pStr);
#endif
}

} // extern "C"
