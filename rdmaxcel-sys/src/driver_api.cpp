/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "driver_api.h"
#include <dlfcn.h>
#include <atomic>
#include <exception>
#include <iostream>
#include <utility>

// Two-level stringify macro to ensure macro arguments are expanded before
// stringification
#define STRINGIFY_HELPER(x) #x
#define STRINGIFY(x) STRINGIFY_HELPER(x)

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
#define SYM_DEVICE_PRIMARY_CTX_RETAIN hipDevicePrimaryCtxRetain
#define SYM_DEVICE_PRIMARY_CTX_RELEASE hipDevicePrimaryCtxRelease
#define SYM_DEVICE_PRIMARY_CTX_GET_STATE hipDevicePrimaryCtxGetState
#define SYM_CTX_GET_CURRENT hipCtxGetCurrent
#define SYM_CTX_SET_CURRENT hipCtxSetCurrent
#define SYM_CTX_SYNCHRONIZE hipCtxSynchronize
#define SYM_GET_ERROR_STRING hipDrvGetErrorString
#define RDMAXCEL_DRIVER_LIB "libamdhip64.so"
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
// CUDA 13.x removed cuCtxCreate_v2 from headers, but libcuda.so still
// exports it for backward compatibility. Provide our own declaration so
// decltype and STRINGIFY resolve correctly.
#if CUDA_VERSION >= 13000
CUresult CUDAAPI
cuCtxCreate_v2(CUcontext* pctx, unsigned int flags, CUdevice dev);
#endif
#define SYM_CTX_CREATE cuCtxCreate_v2
#define SYM_DEVICE_PRIMARY_CTX_RETAIN cuDevicePrimaryCtxRetain
#define SYM_DEVICE_PRIMARY_CTX_RELEASE cuDevicePrimaryCtxRelease
#define SYM_DEVICE_PRIMARY_CTX_GET_STATE cuDevicePrimaryCtxGetState
#define SYM_CTX_GET_CURRENT cuCtxGetCurrent
#define SYM_CTX_SET_CURRENT cuCtxSetCurrent
#define SYM_CTX_SYNCHRONIZE cuCtxSynchronize
#define SYM_GET_ERROR_STRING cuGetErrorString
#define RDMAXCEL_DRIVER_LIB "libcuda.so.1"
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
  _(devicePrimaryCtxRetain, SYM_DEVICE_PRIMARY_CTX_RETAIN)             \
  _(devicePrimaryCtxRelease, SYM_DEVICE_PRIMARY_CTX_RELEASE)           \
  _(devicePrimaryCtxGetState, SYM_DEVICE_PRIMARY_CTX_GET_STATE)        \
  _(ctxGetCurrent, SYM_CTX_GET_CURRENT)                                \
  _(ctxSetCurrent, SYM_CTX_SET_CURRENT)                                \
  _(ctxSynchronize, SYM_CTX_SYNCHRONIZE)                               \
  _(getErrorString, SYM_GET_ERROR_STRING)

namespace rdmaxcel {
namespace {

// On ROCm, a failed HIP driver call leaves a sticky error that subsequent
// calls return, unlike the CUDA driver. Clearing it with hipGetLastError
// after each failure makes ROCm match CUDA's behavior. On CUDA this is a
// no-op pass-through.
CUresult clear_sticky_error(CUresult rc) {
#ifdef USE_ROCM
  if (rc != CUDA_SUCCESS) {
    hipGetLastError();
  }
#endif
  return rc;
}

// Wraps a driver function pointer so every call routes its result through
// clear_sticky_error. Call sites invoke it exactly as they would the raw
// pointer.
template <typename Fn>
struct Wrapped {
  Fn fn;

  template <typename... Args>
  CUresult operator()(Args&&... args) const {
    return clear_sticky_error(fn(std::forward<Args>(args)...));
  }
};

struct DriverAPI {
#define CREATE_MEMBER(name, sym) Wrapped<decltype(&sym)> name##_;
  RDMAXCEL_CUDA_DRIVER_API(CREATE_MEMBER)
#undef CREATE_MEMBER
  static DriverAPI* get();
};

// dlerror() returns nullptr when there is no pending error -- notably after a
// RTLD_NOLOAD dlopen that simply finds the library not loaded. Passing that
// nullptr to std::string concatenation would crash, so always route dlerror()
// through this fallback when building messages.
const char* dlerror_or(const char* fallback) {
  const char* err = dlerror();
  return err != nullptr ? err : fallback;
}

// Signals a driver-load failure that has already been logged at the point it
// occurred. create_driver_api_or_null catches it and returns nullptr without
// logging again; any other exception is unexpected and logged there.
struct DriverLoadError : std::exception {};

// dlcloses a handle on scope exit unless dismissed. Used to drop the handle on
// the error paths while retaining it on success (held for process lifetime).
struct HandleGuard {
  void* handle;

  explicit HandleGuard(void* h) : handle(h) {}
  HandleGuard(const HandleGuard&) = delete;
  HandleGuard& operator=(const HandleGuard&) = delete;
  HandleGuard(HandleGuard&&) = delete;
  HandleGuard& operator=(HandleGuard&&) = delete;
  ~HandleGuard() {
    if (handle != nullptr) {
      dlclose(handle);
    }
  }

  void dismiss() {
    handle = nullptr;
  }
};

// Build the driver-API table. The driver library is adopted only if it is
// already loaded into the process (RTLD_NOLOAD); rdmaxcel never loads CUDA or
// HIP itself. Each failure logs once at the failure site -- throttled per site,
// so distinct causes are each reported -- and throws DriverLoadError.
DriverAPI create_driver_api() {
  void* handle = dlopen(RDMAXCEL_DRIVER_LIB, RTLD_LAZY | RTLD_NOLOAD);
  if (!handle) {
    static std::atomic<bool> logged{false};
    if (!logged.exchange(true, std::memory_order_relaxed)) {
      std::cerr << "[RdmaXcel] Can't open " RDMAXCEL_DRIVER_LIB ": "
                << dlerror_or("library not loaded") << std::endl;
    }
    throw DriverLoadError{};
  }

  // Close the handle if we bail out before the table is built. On success we
  // dismiss the guard and intentionally retain this one reference for the
  // lifetime of the process: the driver must stay resident for the cached
  // function pointers to remain valid.
  HandleGuard guard{handle};

  DriverAPI r{};

#define LOOKUP_CUDA_ENTRY(name, sym)                                   \
  r.name##_.fn =                                                       \
      reinterpret_cast<decltype(&sym)>(dlsym(handle, STRINGIFY(sym))); \
  if (!r.name##_.fn) {                                                 \
    static std::atomic<bool> logged{false};                            \
    if (!logged.exchange(true, std::memory_order_relaxed)) {           \
      std::cerr << "[RdmaXcel] Can't find " STRINGIFY(sym) ": "        \
                << dlerror_or("symbol not found") << std::endl;        \
    }                                                                  \
    throw DriverLoadError{};                                           \
  }

  RDMAXCEL_CUDA_DRIVER_API(LOOKUP_CUDA_ENTRY)
#undef LOOKUP_CUDA_ENTRY

  guard.dismiss();
  return r;
}

// Ensure a CUDA context is current on the calling thread before a
// context-sensitive driver call. If one is already current, do nothing.
// Otherwise adopt the primary context of the first device that already has
// one active -- never creating a context, so we don't initialize one before
// the owning framework has. The adopted primary context is retained and
// intentionally never released, mirroring the CUDA runtime's primary-context
// lifecycle (held for the lifetime of the process). On success, sets
// `*out_api` to the driver table and returns CUDA_SUCCESS once a context is
// current; otherwise returns an error to propagate to the caller.
CUresult ensure_active_context(DriverAPI** out_api) {
  DriverAPI* api = DriverAPI::get();
  if (api == nullptr) {
    return CUDA_ERROR_NOT_INITIALIZED;
  }
  *out_api = api;

  CUcontext ctx = nullptr;
  CUresult rc = api->ctxGetCurrent_(&ctx);
  if (rc != CUDA_SUCCESS) {
    std::cerr << "[RdmaXcel] Failed to get current CUDA context." << std::endl;
    return rc;
  }
  if (ctx != nullptr) {
    return CUDA_SUCCESS;
  }

  int count = 0;
  rc = api->deviceGetCount_(&count);
  if (rc != CUDA_SUCCESS) {
    std::cerr << "[RdmaXcel] Failed to get device count." << std::endl;
    return rc;
  }

  CUresult last_error = CUDA_ERROR_INVALID_CONTEXT;
  for (int ordinal = 0; ordinal < count; ++ordinal) {
    CUdevice device = 0;
    if (api->deviceGet_(&device, ordinal) != CUDA_SUCCESS) {
      std::cerr << "[RdmaXcel] Failed to get device " << ordinal << std::endl;
      continue;
    }
    // `flags` is required by the API but unused here; only `active` matters.
    unsigned int flags = 0;
    int active = 0;
    if (api->devicePrimaryCtxGetState_(device, &flags, &active) !=
        CUDA_SUCCESS) {
      std::cerr << "[RdmaXcel] Failed to get device " << ordinal
                << " primary context state" << std::endl;
      continue;
    }
    if (!active) {
      continue;
    }

    CUcontext primary = nullptr;
    rc = api->devicePrimaryCtxRetain_(&primary, device);
    if (rc != CUDA_SUCCESS) {
      std::cerr << "[RdmaXcel] Failed to retain primary context for device "
                << ordinal << std::endl;
      last_error = rc;
      continue;
    }
    rc = api->ctxSetCurrent_(primary);
    if (rc != CUDA_SUCCESS) {
      std::cerr << "[RdmaXcel] Failed to set primary context for device "
                << ordinal << std::endl;
      CUresult rc_release = api->devicePrimaryCtxRelease_(device);
      if (rc_release != CUDA_SUCCESS) {
        std::cerr << "[RdmaXcel] Failed to release primary context for device "
                  << ordinal << std::endl;
      }
      last_error = rc;
      continue;
    }
    return CUDA_SUCCESS;
  }

  std::cerr
      << "[RdmaXcel] No CUDA context is active on the calling thread and no "
         "device primary context could be adopted. Initialize CUDA and ensure "
         "either that a CUDA context is active on the calling thread or that a "
         "device primary context exists."
      << std::endl;
  return last_error;
}

// Build the driver-API table, caching it on first success. create_driver_api()
// throws on dlopen/dlsym failure; catch it here so no exception crosses the
// extern "C" boundary into Rust (which would be undefined behavior), returning
// nullptr instead for callers to translate into a CUresult error. A throwing
// static initializer leaves the static uninitialized and is retried on the
// next call, so a failed no-load attempt can still succeed later once the
// driver library is loaded externally. Expected failures (DriverLoadError) are
// already logged at the site and swallowed silently here; any other exception
// is unexpected and logged every time.
DriverAPI* create_driver_api_or_null() noexcept {
  try {
    static DriverAPI instance = create_driver_api();
    return &instance;
  } catch (const DriverLoadError&) {
    return nullptr;
  } catch (const std::exception& e) {
    std::cerr << "[RdmaXcel] Failed to load CUDA driver API: " << e.what()
              << std::endl;
    return nullptr;
  } catch (...) {
    std::cerr << "[RdmaXcel] Failed to load CUDA driver API: unknown error"
              << std::endl;
    return nullptr;
  }
}

#define ENSURE_ACTIVE_DRIVER(api)                              \
  rdmaxcel::DriverAPI* api = nullptr;                          \
  if (CUresult ctx_rc = rdmaxcel::ensure_active_context(&api); \
      ctx_rc != CUDA_SUCCESS) {                                \
    return ctx_rc;                                             \
  }

DriverAPI* DriverAPI::get() {
  // Never cache a failure: the driver library may be loaded externally (e.g.
  // by PyTorch) between calls, so every call re-attempts adoption and can
  // start succeeding once the library appears. A successful table is cached
  // by the function-local static inside create_driver_api_or_null().
  return create_driver_api_or_null();
}

} // namespace
} // namespace rdmaxcel

// C API wrapper implementations
extern "C" {

// Memory management
CUresult rdmaxcel_cuMemGetHandleForAddressRange(
    int* handle,
    CUdeviceptr dptr,
    size_t size,
    CUmemRangeHandleType handleType,
    unsigned long long flags) noexcept {
  ENSURE_ACTIVE_DRIVER(api);
  return api->memGetHandleForAddressRange_(
      handle, dptr, size, handleType, flags);
}

CUresult rdmaxcel_cuMemGetAllocationGranularity(
    size_t* granularity,
    const CUmemAllocationProp* prop,
    CUmemAllocationGranularity_flags option) noexcept {
  ENSURE_ACTIVE_DRIVER(api);
  return api->memGetAllocationGranularity_(granularity, prop, option);
}

CUresult rdmaxcel_cuMemCreate(
    CUmemGenericAllocationHandle* handle,
    size_t size,
    const CUmemAllocationProp* prop,
    unsigned long long flags) noexcept {
  ENSURE_ACTIVE_DRIVER(api);
  return api->memCreate_(handle, size, prop, flags);
}

CUresult rdmaxcel_cuMemAddressReserve(
    CUdeviceptr* ptr,
    size_t size,
    size_t alignment,
    CUdeviceptr addr,
    unsigned long long flags) noexcept {
  ENSURE_ACTIVE_DRIVER(api);
  return api->memAddressReserve_(ptr, size, alignment, addr, flags);
}

CUresult rdmaxcel_cuMemMap(
    CUdeviceptr ptr,
    size_t size,
    size_t offset,
    CUmemGenericAllocationHandle handle,
    unsigned long long flags) noexcept {
  ENSURE_ACTIVE_DRIVER(api);
  return api->memMap_(ptr, size, offset, handle, flags);
}

CUresult rdmaxcel_cuMemSetAccess(
    CUdeviceptr ptr,
    size_t size,
    const CUmemAccessDesc* desc,
    size_t count) noexcept {
  ENSURE_ACTIVE_DRIVER(api);
  return api->memSetAccess_(ptr, size, desc, count);
}

CUresult rdmaxcel_cuMemUnmap(CUdeviceptr ptr, size_t size) noexcept {
  ENSURE_ACTIVE_DRIVER(api);
  return api->memUnmap_(ptr, size);
}

CUresult rdmaxcel_cuMemAddressFree(CUdeviceptr ptr, size_t size) noexcept {
  ENSURE_ACTIVE_DRIVER(api);
  return api->memAddressFree_(ptr, size);
}

CUresult rdmaxcel_cuMemRelease(CUmemGenericAllocationHandle handle) noexcept {
  ENSURE_ACTIVE_DRIVER(api);
  return api->memRelease_(handle);
}

CUresult rdmaxcel_cuMemcpyHtoD_v2(
    CUdeviceptr dstDevice,
    const void* srcHost,
    size_t ByteCount) noexcept {
  ENSURE_ACTIVE_DRIVER(api);
  return api->memcpyHtoD_(dstDevice, srcHost, ByteCount);
}

CUresult rdmaxcel_cuMemcpyDtoH_v2(
    void* dstHost,
    CUdeviceptr srcDevice,
    size_t ByteCount) noexcept {
  ENSURE_ACTIVE_DRIVER(api);
  return api->memcpyDtoH_(dstHost, srcDevice, ByteCount);
}

CUresult rdmaxcel_cuMemsetD8_v2(
    CUdeviceptr dstDevice,
    unsigned char uc,
    size_t N) noexcept {
  ENSURE_ACTIVE_DRIVER(api);
  return api->memsetD8_(dstDevice, uc, N);
}

// Pointer queries
CUresult rdmaxcel_cuPointerGetAttribute(
    void* data,
    CUpointer_attribute attribute,
    CUdeviceptr ptr) noexcept {
  ENSURE_ACTIVE_DRIVER(api);
  return api->pointerGetAttribute_(data, attribute, ptr);
}

// Driver library loading
int ensure_cuda_driver_loaded(void) noexcept {
  // Load the driver, or adopt it if already resident -- a plain dlopen does
  // both in one step (it bumps the refcount on an existing mapping rather than
  // reloading). The handle is cached in a function-local static and held for
  // the lifetime of the process -- never dlclosed -- so the driver stays
  // resident for later adoption by the wrappers; exactly one reference is
  // taken, so nothing accumulates across calls. This is the only entry point
  // that loads the driver; the wrappers never do. A load failure is logged
  // once, during the static's initialization.
  //
  // noexcept: this is an extern "C" entry point called from Rust, so no
  // exception may cross the boundary (that would be undefined behavior); catch
  // anything (e.g. from std::cerr) and report failure instead.
  try {
    static void* handle = []() -> void* {
      void* h = dlopen(RDMAXCEL_DRIVER_LIB, RTLD_LAZY);
      if (h == nullptr) {
        std::cerr << "[RdmaXcel] Failed to load " RDMAXCEL_DRIVER_LIB ": "
                  << rdmaxcel::dlerror_or("unknown error") << std::endl;
      }
      return h;
    }();
    return handle != nullptr ? 0 : -1;
  } catch (...) {
    return -1;
  }
}

// Device management
CUresult rdmaxcel_cuInit(unsigned int Flags) noexcept {
  // Adopts an already-loaded driver only; it does not load CUDA/HIP itself.
  // Callers that need the driver loaded must dlopen it before calling this
  // (e.g. via ensure_cuda_driver_loaded).
  rdmaxcel::DriverAPI* api = rdmaxcel::DriverAPI::get();
  if (api == nullptr) {
    return CUDA_ERROR_NOT_INITIALIZED;
  }
  return api->init_(Flags);
}

CUresult rdmaxcel_cuDeviceGet(CUdevice* device, int ordinal) noexcept {
  rdmaxcel::DriverAPI* api = rdmaxcel::DriverAPI::get();
  if (api == nullptr) {
    return CUDA_ERROR_NOT_INITIALIZED;
  }
  return api->deviceGet_(device, ordinal);
}

CUresult rdmaxcel_cuDeviceGetCount(int* count) noexcept {
  rdmaxcel::DriverAPI* api = rdmaxcel::DriverAPI::get();
  if (api == nullptr) {
    return CUDA_ERROR_NOT_INITIALIZED;
  }
  return api->deviceGetCount_(count);
}

CUresult rdmaxcel_cuDeviceGetAttribute(
    int* pi,
    CUdevice_attribute attrib,
    CUdevice dev) noexcept {
  rdmaxcel::DriverAPI* api = rdmaxcel::DriverAPI::get();
  if (api == nullptr) {
    return CUDA_ERROR_NOT_INITIALIZED;
  }
  return api->deviceGetAttribute_(pi, attrib, dev);
}

// Context management
CUresult rdmaxcel_cuCtxCreate_v2(
    CUcontext* pctx,
    unsigned int flags,
    CUdevice dev) noexcept {
  rdmaxcel::DriverAPI* api = rdmaxcel::DriverAPI::get();
  if (api == nullptr) {
    return CUDA_ERROR_NOT_INITIALIZED;
  }
  return api->ctxCreate_(pctx, flags, dev);
}

CUresult rdmaxcel_cuDevicePrimaryCtxRetain(
    CUcontext* pctx,
    CUdevice dev) noexcept {
  rdmaxcel::DriverAPI* api = rdmaxcel::DriverAPI::get();
  if (api == nullptr) {
    return CUDA_ERROR_NOT_INITIALIZED;
  }
  return api->devicePrimaryCtxRetain_(pctx, dev);
}

CUresult rdmaxcel_cuDevicePrimaryCtxRelease(CUdevice dev) noexcept {
  rdmaxcel::DriverAPI* api = rdmaxcel::DriverAPI::get();
  if (api == nullptr) {
    return CUDA_ERROR_NOT_INITIALIZED;
  }
  return api->devicePrimaryCtxRelease_(dev);
}

CUresult rdmaxcel_cuCtxGetCurrent(CUcontext* pctx) noexcept {
  rdmaxcel::DriverAPI* api = rdmaxcel::DriverAPI::get();
  if (api == nullptr) {
    return CUDA_ERROR_NOT_INITIALIZED;
  }
  return api->ctxGetCurrent_(pctx);
}

CUresult rdmaxcel_cuCtxSetCurrent(CUcontext ctx) noexcept {
  rdmaxcel::DriverAPI* api = rdmaxcel::DriverAPI::get();
  if (api == nullptr) {
    return CUDA_ERROR_NOT_INITIALIZED;
  }
  return api->ctxSetCurrent_(ctx);
}

CUresult rdmaxcel_cuCtxSynchronize(void) noexcept {
  ENSURE_ACTIVE_DRIVER(api);
  return api->ctxSynchronize_();
}

// Error handling
CUresult rdmaxcel_cuGetErrorString(CUresult error, const char** pStr) noexcept {
  rdmaxcel::DriverAPI* api = rdmaxcel::DriverAPI::get();
  if (api == nullptr) {
    return CUDA_ERROR_NOT_INITIALIZED;
  }
  return api->getErrorString_(error, pStr);
}

} // extern "C"
