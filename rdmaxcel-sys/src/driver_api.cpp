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

// Symbol name macros - platform-specific function names for dlsym lookup
#ifdef USE_ROCM
#define SYM_MEM_GET_HANDLE_FOR_ADDRESS_RANGE hipMemGetHandleForAddressRange
#define SYM_MEM_GET_ALLOCATION_GRANULARITY hipMemGetAllocationGranularity
#define SYM_MEM_CREATE hipMemCreate
#define SYM_MEM_ADDRESS_RESERVE hipMemAddressReserve
#define SYM_MEM_MAP hipMemMap
#define SYM_MEM_SET_ACCESS hipMemSetAccess
#define SYM_MEM_UNMAP hipMemUnmap
#define SYM_MEM_ADDRESS_FREE hipMemAddressFree
#define SYM_MEM_RELEASE hipMemRelease
#define SYM_MEMCPY_HTOD hipMemcpyHtoD
#define SYM_MEMCPY_DTOH hipMemcpyDtoH
#define SYM_MEMSET_D8 hipMemsetD8
#define SYM_POINTER_GET_ATTRIBUTE hipPointerGetAttribute
#define SYM_INIT hipInit
#define SYM_DEVICE_GET hipDeviceGet
#define SYM_DEVICE_GET_COUNT hipGetDeviceCount
#define SYM_DEVICE_GET_ATTRIBUTE hipDeviceGetAttribute
#define SYM_CTX_CREATE hipCtxCreate
#define SYM_CTX_SET_CURRENT hipCtxSetCurrent
#define SYM_CTX_SYNCHRONIZE hipCtxSynchronize
#define SYM_GET_ERROR_STRING hipDrvGetErrorString
#else
#define SYM_MEM_GET_HANDLE_FOR_ADDRESS_RANGE cuMemGetHandleForAddressRange
#define SYM_MEM_GET_ALLOCATION_GRANULARITY cuMemGetAllocationGranularity
#define SYM_MEM_CREATE cuMemCreate
#define SYM_MEM_ADDRESS_RESERVE cuMemAddressReserve
#define SYM_MEM_MAP cuMemMap
#define SYM_MEM_SET_ACCESS cuMemSetAccess
#define SYM_MEM_UNMAP cuMemUnmap
#define SYM_MEM_ADDRESS_FREE cuMemAddressFree
#define SYM_MEM_RELEASE cuMemRelease
#define SYM_MEMCPY_HTOD cuMemcpyHtoD_v2
#define SYM_MEMCPY_DTOH cuMemcpyDtoH_v2
#define SYM_MEMSET_D8 cuMemsetD8_v2
#define SYM_POINTER_GET_ATTRIBUTE cuPointerGetAttribute
#define SYM_INIT cuInit
#define SYM_DEVICE_GET cuDeviceGet
#define SYM_DEVICE_GET_COUNT cuDeviceGetCount
#define SYM_DEVICE_GET_ATTRIBUTE cuDeviceGetAttribute
#define SYM_CTX_CREATE cuCtxCreate_v2
#define SYM_CTX_SET_CURRENT cuCtxSetCurrent
#define SYM_CTX_SYNCHRONIZE cuCtxSynchronize
#define SYM_GET_ERROR_STRING cuGetErrorString
#endif

// List of GPU driver functions needed by rdmaxcel
// Format: _(methodName, symbolName)
// The methodName is used for the DriverAPI struct member
// The symbolName is the platform-specific function looked up via dlsym
#define RDMAXCEL_CUDA_DRIVER_API(_)                                    \
  _(memGetHandleForAddressRange, SYM_MEM_GET_HANDLE_FOR_ADDRESS_RANGE) \
  _(memGetAllocationGranularity, SYM_MEM_GET_ALLOCATION_GRANULARITY)   \
  _(memCreate, SYM_MEM_CREATE)                                         \
  _(memAddressReserve, SYM_MEM_ADDRESS_RESERVE)                        \
  _(memMap, SYM_MEM_MAP)                                               \
  _(memSetAccess, SYM_MEM_SET_ACCESS)                                  \
  _(memUnmap, SYM_MEM_UNMAP)                                           \
  _(memAddressFree, SYM_MEM_ADDRESS_FREE)                              \
  _(memRelease, SYM_MEM_RELEASE)                                       \
  _(memcpyHtoD, SYM_MEMCPY_HTOD)                                       \
  _(memcpyDtoH, SYM_MEMCPY_DTOH)                                       \
  _(memsetD8, SYM_MEMSET_D8)                                           \
  _(pointerGetAttribute, SYM_POINTER_GET_ATTRIBUTE)                    \
  _(init, SYM_INIT)                                                    \
  _(deviceGet, SYM_DEVICE_GET)                                         \
  _(deviceGetCount, SYM_DEVICE_GET_COUNT)                              \
  _(deviceGetAttribute, SYM_DEVICE_GET_ATTRIBUTE)                      \
  _(ctxCreate, SYM_CTX_CREATE)                                         \
  _(ctxSetCurrent, SYM_CTX_SET_CURRENT)                                \
  _(ctxSynchronize, SYM_CTX_SYNCHRONIZE)                               \
  _(getErrorString, SYM_GET_ERROR_STRING)

namespace rdmaxcel {

struct DriverAPI {
#define CREATE_MEMBER(name, sym) decltype(&sym) name##_;
  RDMAXCEL_CUDA_DRIVER_API(CREATE_MEMBER)
#undef CREATE_MEMBER
  static DriverAPI* get();
};

namespace {

DriverAPI create_driver_api() {
#ifdef USE_ROCM
  // Try to open libamdhip64.so - RTLD_NOLOAD means only succeed if already
  // loaded
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

#define LOOKUP_CUDA_ENTRY(name, sym)                                      \
  r.name##_ = reinterpret_cast<decltype(&sym)>(dlsym(handle, #sym));      \
  if (!r.name##_) {                                                       \
    throw std::runtime_error(                                             \
        std::string("[RdmaXcel] Can't find ") + #sym + ": " + dlerror()); \
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
  return rdmaxcel::DriverAPI::get()->memGetHandleForAddressRange_(
      handle, dptr, size, handleType, flags);
}

CUresult rdmaxcel_cuMemGetAllocationGranularity(
    size_t* granularity,
    const CUmemAllocationProp* prop,
    CUmemAllocationGranularity_flags option) {
  return rdmaxcel::DriverAPI::get()->memGetAllocationGranularity_(
      granularity, prop, option);
}

CUresult rdmaxcel_cuMemCreate(
    CUmemGenericAllocationHandle* handle,
    size_t size,
    const CUmemAllocationProp* prop,
    unsigned long long flags) {
  return rdmaxcel::DriverAPI::get()->memCreate_(handle, size, prop, flags);
}

CUresult rdmaxcel_cuMemAddressReserve(
    CUdeviceptr* ptr,
    size_t size,
    size_t alignment,
    CUdeviceptr addr,
    unsigned long long flags) {
  return rdmaxcel::DriverAPI::get()->memAddressReserve_(
      ptr, size, alignment, addr, flags);
}

CUresult rdmaxcel_cuMemMap(
    CUdeviceptr ptr,
    size_t size,
    size_t offset,
    CUmemGenericAllocationHandle handle,
    unsigned long long flags) {
  return rdmaxcel::DriverAPI::get()->memMap_(ptr, size, offset, handle, flags);
}

CUresult rdmaxcel_cuMemSetAccess(
    CUdeviceptr ptr,
    size_t size,
    const CUmemAccessDesc* desc,
    size_t count) {
  return rdmaxcel::DriverAPI::get()->memSetAccess_(ptr, size, desc, count);
}

CUresult rdmaxcel_cuMemUnmap(CUdeviceptr ptr, size_t size) {
  return rdmaxcel::DriverAPI::get()->memUnmap_(ptr, size);
}

CUresult rdmaxcel_cuMemAddressFree(CUdeviceptr ptr, size_t size) {
  return rdmaxcel::DriverAPI::get()->memAddressFree_(ptr, size);
}

CUresult rdmaxcel_cuMemRelease(CUmemGenericAllocationHandle handle) {
  return rdmaxcel::DriverAPI::get()->memRelease_(handle);
}

CUresult rdmaxcel_cuMemcpyHtoD_v2(
    CUdeviceptr dstDevice,
    const void* srcHost,
    size_t ByteCount) {
  return rdmaxcel::DriverAPI::get()->memcpyHtoD_(dstDevice, srcHost, ByteCount);
}

CUresult rdmaxcel_cuMemcpyDtoH_v2(
    void* dstHost,
    CUdeviceptr srcDevice,
    size_t ByteCount) {
  return rdmaxcel::DriverAPI::get()->memcpyDtoH_(dstHost, srcDevice, ByteCount);
}

CUresult
rdmaxcel_cuMemsetD8_v2(CUdeviceptr dstDevice, unsigned char uc, size_t N) {
  return rdmaxcel::DriverAPI::get()->memsetD8_(dstDevice, uc, N);
}

// Pointer queries
CUresult rdmaxcel_cuPointerGetAttribute(
    void* data,
    CUpointer_attribute attribute,
    CUdeviceptr ptr) {
  return rdmaxcel::DriverAPI::get()->pointerGetAttribute_(data, attribute, ptr);
}

// Device management
CUresult rdmaxcel_cuInit(unsigned int Flags) {
  return rdmaxcel::DriverAPI::get()->init_(Flags);
}

CUresult rdmaxcel_cuDeviceGet(CUdevice* device, int ordinal) {
  return rdmaxcel::DriverAPI::get()->deviceGet_(device, ordinal);
}

CUresult rdmaxcel_cuDeviceGetCount(int* count) {
  return rdmaxcel::DriverAPI::get()->deviceGetCount_(count);
}

CUresult rdmaxcel_cuDeviceGetAttribute(
    int* pi,
    CUdevice_attribute attrib,
    CUdevice dev) {
  return rdmaxcel::DriverAPI::get()->deviceGetAttribute_(pi, attrib, dev);
}

// Context management
CUresult
rdmaxcel_cuCtxCreate_v2(CUcontext* pctx, unsigned int flags, CUdevice dev) {
  return rdmaxcel::DriverAPI::get()->ctxCreate_(pctx, flags, dev);
}

CUresult rdmaxcel_cuCtxSetCurrent(CUcontext ctx) {
  return rdmaxcel::DriverAPI::get()->ctxSetCurrent_(ctx);
}

CUresult rdmaxcel_cuCtxSynchronize(void) {
  return rdmaxcel::DriverAPI::get()->ctxSynchronize_();
}

// Error handling
CUresult rdmaxcel_cuGetErrorString(CUresult error, const char** pStr) {
  return rdmaxcel::DriverAPI::get()->getErrorString_(error, pStr);
}

} // extern "C"
