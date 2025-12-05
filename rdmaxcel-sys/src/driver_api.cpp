/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "driver_api.h"
#include <dlfcn.h>
#include <iostream>
#include <stdexcept>

// List of CUDA driver functions needed by rdmaxcel
#define RDMAXCEL_CUDA_DRIVER_API(_) \
  _(cuMemGetHandleForAddressRange)  \
  _(cuMemGetAllocationGranularity)  \
  _(cuMemCreate)                    \
  _(cuMemAddressReserve)            \
  _(cuMemMap)                       \
  _(cuMemSetAccess)                 \
  _(cuMemUnmap)                     \
  _(cuMemAddressFree)               \
  _(cuMemRelease)                   \
  _(cuMemcpyHtoD_v2)                \
  _(cuMemcpyDtoH_v2)                \
  _(cuMemsetD8_v2)                  \
  _(cuPointerGetAttribute)          \
  _(cuInit)                         \
  _(cuDeviceGet)                    \
  _(cuDeviceGetCount)               \
  _(cuDeviceGetAttribute)           \
  _(cuCtxCreate_v2)                 \
  _(cuCtxSetCurrent)                \
  _(cuCtxSynchronize)               \
  _(cuGetErrorString)

namespace rdmaxcel {

struct DriverAPI {
#define CREATE_MEMBER(name) decltype(&name) name##_;
  RDMAXCEL_CUDA_DRIVER_API(CREATE_MEMBER)
#undef CREATE_MEMBER
  static DriverAPI* get();
};

namespace {

DriverAPI create_driver_api() {
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
  return rdmaxcel::DriverAPI::get()->cuMemGetHandleForAddressRange_(
      handle, dptr, size, handleType, flags);
}

CUresult rdmaxcel_cuMemGetAllocationGranularity(
    size_t* granularity,
    const CUmemAllocationProp* prop,
    CUmemAllocationGranularity_flags option) {
  return rdmaxcel::DriverAPI::get()->cuMemGetAllocationGranularity_(
      granularity, prop, option);
}

CUresult rdmaxcel_cuMemCreate(
    CUmemGenericAllocationHandle* handle,
    size_t size,
    const CUmemAllocationProp* prop,
    unsigned long long flags) {
  return rdmaxcel::DriverAPI::get()->cuMemCreate_(handle, size, prop, flags);
}

CUresult rdmaxcel_cuMemAddressReserve(
    CUdeviceptr* ptr,
    size_t size,
    size_t alignment,
    CUdeviceptr addr,
    unsigned long long flags) {
  return rdmaxcel::DriverAPI::get()->cuMemAddressReserve_(
      ptr, size, alignment, addr, flags);
}

CUresult rdmaxcel_cuMemMap(
    CUdeviceptr ptr,
    size_t size,
    size_t offset,
    CUmemGenericAllocationHandle handle,
    unsigned long long flags) {
  return rdmaxcel::DriverAPI::get()->cuMemMap_(
      ptr, size, offset, handle, flags);
}

CUresult rdmaxcel_cuMemSetAccess(
    CUdeviceptr ptr,
    size_t size,
    const CUmemAccessDesc* desc,
    size_t count) {
  return rdmaxcel::DriverAPI::get()->cuMemSetAccess_(ptr, size, desc, count);
}

CUresult rdmaxcel_cuMemUnmap(CUdeviceptr ptr, size_t size) {
  return rdmaxcel::DriverAPI::get()->cuMemUnmap_(ptr, size);
}

CUresult rdmaxcel_cuMemAddressFree(CUdeviceptr ptr, size_t size) {
  return rdmaxcel::DriverAPI::get()->cuMemAddressFree_(ptr, size);
}

CUresult rdmaxcel_cuMemRelease(CUmemGenericAllocationHandle handle) {
  return rdmaxcel::DriverAPI::get()->cuMemRelease_(handle);
}

CUresult rdmaxcel_cuMemcpyHtoD_v2(
    CUdeviceptr dstDevice,
    const void* srcHost,
    size_t ByteCount) {
  return rdmaxcel::DriverAPI::get()->cuMemcpyHtoD_v2_(
      dstDevice, srcHost, ByteCount);
}

CUresult rdmaxcel_cuMemcpyDtoH_v2(
    void* dstHost,
    CUdeviceptr srcDevice,
    size_t ByteCount) {
  return rdmaxcel::DriverAPI::get()->cuMemcpyDtoH_v2_(
      dstHost, srcDevice, ByteCount);
}

CUresult
rdmaxcel_cuMemsetD8_v2(CUdeviceptr dstDevice, unsigned char uc, size_t N) {
  return rdmaxcel::DriverAPI::get()->cuMemsetD8_v2_(dstDevice, uc, N);
}

// Pointer queries
CUresult rdmaxcel_cuPointerGetAttribute(
    void* data,
    CUpointer_attribute attribute,
    CUdeviceptr ptr) {
  return rdmaxcel::DriverAPI::get()->cuPointerGetAttribute_(
      data, attribute, ptr);
}

// Device management
CUresult rdmaxcel_cuInit(unsigned int Flags) {
  return rdmaxcel::DriverAPI::get()->cuInit_(Flags);
}

CUresult rdmaxcel_cuDeviceGet(CUdevice* device, int ordinal) {
  return rdmaxcel::DriverAPI::get()->cuDeviceGet_(device, ordinal);
}

CUresult rdmaxcel_cuDeviceGetCount(int* count) {
  return rdmaxcel::DriverAPI::get()->cuDeviceGetCount_(count);
}

CUresult rdmaxcel_cuDeviceGetAttribute(
    int* pi,
    CUdevice_attribute attrib,
    CUdevice dev) {
  return rdmaxcel::DriverAPI::get()->cuDeviceGetAttribute_(pi, attrib, dev);
}

// Context management
CUresult
rdmaxcel_cuCtxCreate_v2(CUcontext* pctx, unsigned int flags, CUdevice dev) {
  return rdmaxcel::DriverAPI::get()->cuCtxCreate_v2_(pctx, flags, dev);
}

CUresult rdmaxcel_cuCtxSetCurrent(CUcontext ctx) {
  return rdmaxcel::DriverAPI::get()->cuCtxSetCurrent_(ctx);
}

CUresult rdmaxcel_cuCtxSynchronize(void) {
  return rdmaxcel::DriverAPI::get()->cuCtxSynchronize_();
}

// Error handling
CUresult rdmaxcel_cuGetErrorString(CUresult error, const char** pStr) {
  return rdmaxcel::DriverAPI::get()->cuGetErrorString_(error, pStr);
}

} // extern "C"
