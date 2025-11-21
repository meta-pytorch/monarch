/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once
#include <cuda.h>

#ifdef __cplusplus
extern "C" {
#endif

// C API wrapper functions for CUDA driver functions
// These are loaded dynamically via dlopen and exported to Rust via bindgen

// Memory management
CUresult rdmaxcel_cuMemGetHandleForAddressRange(
    int* handle,
    CUdeviceptr dptr,
    size_t size,
    CUmemRangeHandleType handleType,
    unsigned long long flags);

CUresult rdmaxcel_cuMemGetAllocationGranularity(
    size_t* granularity,
    const CUmemAllocationProp* prop,
    CUmemAllocationGranularity_flags option);

CUresult rdmaxcel_cuMemCreate(
    CUmemGenericAllocationHandle* handle,
    size_t size,
    const CUmemAllocationProp* prop,
    unsigned long long flags);

CUresult rdmaxcel_cuMemAddressReserve(
    CUdeviceptr* ptr,
    size_t size,
    size_t alignment,
    CUdeviceptr addr,
    unsigned long long flags);

CUresult rdmaxcel_cuMemMap(
    CUdeviceptr ptr,
    size_t size,
    size_t offset,
    CUmemGenericAllocationHandle handle,
    unsigned long long flags);

CUresult rdmaxcel_cuMemSetAccess(
    CUdeviceptr ptr,
    size_t size,
    const CUmemAccessDesc* desc,
    size_t count);

CUresult rdmaxcel_cuMemUnmap(CUdeviceptr ptr, size_t size);

CUresult rdmaxcel_cuMemAddressFree(CUdeviceptr ptr, size_t size);

CUresult rdmaxcel_cuMemRelease(CUmemGenericAllocationHandle handle);

CUresult rdmaxcel_cuMemcpyHtoD_v2(
    CUdeviceptr dstDevice,
    const void* srcHost,
    size_t ByteCount);

CUresult rdmaxcel_cuMemcpyDtoH_v2(
    void* dstHost,
    CUdeviceptr srcDevice,
    size_t ByteCount);

CUresult
rdmaxcel_cuMemsetD8_v2(CUdeviceptr dstDevice, unsigned char uc, size_t N);

// Pointer queries
CUresult rdmaxcel_cuPointerGetAttribute(
    void* data,
    CUpointer_attribute attribute,
    CUdeviceptr ptr);

// Device management
CUresult rdmaxcel_cuInit(unsigned int Flags);

CUresult rdmaxcel_cuDeviceGet(CUdevice* device, int ordinal);

CUresult rdmaxcel_cuDeviceGetCount(int* count);

CUresult
rdmaxcel_cuDeviceGetAttribute(int* pi, CUdevice_attribute attrib, CUdevice dev);

// Context management
CUresult
rdmaxcel_cuCtxCreate_v2(CUcontext* pctx, unsigned int flags, CUdevice dev);

CUresult rdmaxcel_cuCtxSetCurrent(CUcontext ctx);

// Error handling
CUresult rdmaxcel_cuGetErrorString(CUresult error, const char** pStr);

#ifdef __cplusplus
} // extern "C"
#endif
