/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

// @lint-ignore-every CLANGSECURITY facebook-security-vulnerable-memcpy
// @lint-ignore-every CLANGTIDY clang-diagnostic-unused-parameter
#include <Python.h>
#include <optional>
#include <unordered_set>
#ifdef MONARCH_CUDA_INSPECT
#include <asmjit/x86.h> // @manual=fbsource//third-party/asmjit:asmjit
#include <ostream>
#endif
#include <assert.h>
#include <dlfcn.h>
#include <link.h>
#include <stdint.h>
#include <sys/mman.h>
#include <unistd.h>
#include <atomic>
#include <cstddef>
#include <cstring>
#include <mutex>
#include <random>
#include <stdexcept>
#include <string>
#include <unordered_map>

namespace {

// CUDA API functions all start with the same pattern where they check
// if CUDA is de-initialize and return 4 if it is. Otherwise, they
// do an indirect jump to a function table in the data section of libcuda:
//
// 0x00007ffff64ebb40 <+0>: cmpl $0x321cba00,0x191afde(%rip) # 0x7ffff7e06b28
// 0x00007ffff64ebb4a <+10>: je 0x7ffff64ebb58 <cuPointerGetAttribute+24>
// 0x00007ffff64ebb4c <+12>: jmpq *0x18235fe(%rip) # 0x7ffff7d0f150
// 0x00007ffff64ebb52 <+18>: nopw 0x0(%rax,%rax,1)
// 0x00007ffff64ebb58 <+24>: mov $0x4,%eax
// 0x00007ffff64ebb5d <+29>: retq

// We can can swizzle our own function into this table to intercept commands

// This function extracts the address of the table entry for a function (e.g.
// 0x7ffff7d0f150 above)
std::optional<void*> extractJumpTarget(const uint8_t* functionBytes) {
  const uint8_t expectedOpcode[] = {0x81, 0x3D};
  const uint8_t expectedImmediateValue[] = {0x00, 0xBA, 0x1C, 0x32};
  const uint8_t jmpq[] = {0xFF, 0x25};
  const uint8_t movzbl_sil_esi[] = {0x40, 0x0f, 0xb6, 0xf6};
  if (std::memcmp(functionBytes, expectedOpcode, sizeof(expectedOpcode)) != 0) {
    return std::nullopt;
  }
  if (std::memcmp(
          functionBytes + 6,
          expectedImmediateValue,
          sizeof(expectedImmediateValue)) != 0) {
    return std::nullopt;
  }

  size_t jmpqOffset = 12;
  if (std::memcmp(functionBytes + 12, jmpq, sizeof(jmpq)) != 0) {
    // The only exception to the pattern is cuMemsetD8Async which does a
    // ubyte -> uint promotion of its second argument. We just
    // skip that here. Our own function will redo the promotion,
    // but the operation is idempotent.
    if (std::memcmp(
            functionBytes + 12, movzbl_sil_esi, sizeof(movzbl_sil_esi)) != 0) {
      return std::nullopt;
    }
    jmpqOffset += sizeof(movzbl_sil_esi);
  }

  int32_t ripRelativeOffset;
  std::memcpy(
      &ripRelativeOffset,
      functionBytes + jmpqOffset + 2,
      sizeof(ripRelativeOffset));
  uintptr_t jmpqInstructionAddress =
      reinterpret_cast<uintptr_t>(functionBytes) + jmpqOffset;
  uintptr_t targetAddress = jmpqInstructionAddress + 6 + ripRelativeOffset;
  return reinterpret_cast<void*>(targetAddress);
}

// This function swaps the jump target with our own replacement,
// Returning the (real) original function.
std::optional<void*> swapJumpTarget(void* functionAddr, void* newTarget) {
  uint8_t* functionBytes = (uint8_t*)functionAddr;
  auto targetAddressOpt = extractJumpTarget(functionBytes);
  if (!targetAddressOpt) {
    return std::nullopt;
  }
  std::atomic<void*>* atomicTargetAddress =
      reinterpret_cast<std::atomic<void*>*>(*targetAddressOpt);
  return atomicTargetAddress->exchange(newTarget);
}

void inspect(const char* name_) {
#ifdef MONARCH_CUDA_INSPECT
  std::cout << "called: " << name_ << "\n";
#endif
}

#define FORALL_FUNCTIONS(_)      \
  _(cuGetProcAddress)            \
  _(cuLaunchKernel)              \
  _(cuMemcpyDtoHAsync)           \
  _(cuMemcpyHtoDAsync)           \
  _(cuMemsetD8Async)             \
  _(cuLaunchKernelEx)            \
  _(cuMemAlloc)                  \
  _(cuMemFree)                   \
  _(cuMemcpyDtoDAsync)           \
  _(cuPointerGetAttribute)       \
  _(cuGetProcAddress_v2)         \
  _(cuMemCreate)                 \
  _(cuMemAddressReserve)         \
  _(cuMemMap)                    \
  _(cuMemSetAccess)              \
  _(cuMemcpyAsync)               \
  _(cuMemRelease)                \
  _(cuMemUnmap)                  \
  _(cuMemAddressFree)            \
  _(cuMemRetainAllocationHandle) \
  _(cuMemGetAddressRange)

constexpr int MAX_VERSIONS = 4;
#define CREATE_REALS(fn)                     \
  void* real_##fn[MAX_VERSIONS] = {nullptr}; \
  extern void* ps_##fn[];

FORALL_FUNCTIONS(CREATE_REALS)
#undef CREATE_REALS

using CUresult = int;
using CUdevice = int;
using CUmemGenericAllocationHandle = unsigned long long;
using CUcontext = struct CUctx_st*; /**< CUDA context */
using CUfunction = struct CUfunc_st*; /**< CUDA function */
using CUstream = struct CUstream_st*; /**< CUDA stream */
struct CUlaunchConfig;
struct CUmemAllocationProp;
struct CUmemAccessDesc;

enum CUpointer_attribute {
  CU_POINTER_ATTRIBUTE_CONTEXT = 1,
  CU_POINTER_ATTRIBUTE_MEMORY_TYPE = 2,
  CU_POINTER_ATTRIBUTE_DEVICE_POINTER = 3,
  CU_POINTER_ATTRIBUTE_HOST_POINTER = 4,
  CU_POINTER_ATTRIBUTE_P2P_TOKENS = 5,
  CU_POINTER_ATTRIBUTE_SYNC_MEMOPS = 6,
  CU_POINTER_ATTRIBUTE_BUFFER_ID = 7,
  CU_POINTER_ATTRIBUTE_IS_MANAGED = 8,
  CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL = 9,
  CU_POINTER_ATTRIBUTE_IS_LEGACY_CUDA_IPC_CAPABLE = 10,
  CU_POINTER_ATTRIBUTE_RANGE_START_ADDR = 11,
  CU_POINTER_ATTRIBUTE_RANGE_SIZE = 12,
  CU_POINTER_ATTRIBUTE_MAPPED = 13,
  CU_POINTER_ATTRIBUTE_ALLOWED_HANDLE_TYPES = 14,
  CU_POINTER_ATTRIBUTE_IS_GPU_DIRECT_RDMA_CAPABLE = 15,
  CU_POINTER_ATTRIBUTE_ACCESS_FLAGS = 16,
  CU_POINTER_ATTRIBUTE_MEMPOOL_HANDLE = 17,
  CU_POINTER_ATTRIBUTE_MAPPING_SIZE = 18,
  CU_POINTER_ATTRIBUTE_MAPPING_BASE_ADDR = 19,
  CU_POINTER_ATTRIBUTE_MEMORY_BLOCK_ID = 20
};

thread_local std::atomic<bool> mockCudaEnabled = false;

#define RETURN_REAL_IF_UNMOCKED(fn, ...)                      \
  if (!mockCudaEnabled.load()) {                              \
    return ((decltype(&p_##fn<N>))real_##fn[N])(__VA_ARGS__); \
  }

template <int N>
CUresult p_cuLaunchKernel(
    CUfunction f,
    unsigned int gridDimX,
    unsigned int gridDimY,
    unsigned int gridDimZ,
    unsigned int blockDimX,
    unsigned int blockDimY,
    unsigned int blockDimZ,
    unsigned int sharedMemBytes,
    CUstream hStream,
    void** kernelParams,
    void** extra) {
  inspect("cuLaunchKernel");
  RETURN_REAL_IF_UNMOCKED(
      cuLaunchKernel,
      f,
      gridDimX,
      gridDimY,
      gridDimZ,
      blockDimX,
      blockDimY,
      blockDimZ,
      sharedMemBytes,
      hStream,
      kernelParams,
      extra);
  return 0;
}

std::mutex lockMemAddr;
size_t memAddr = static_cast<size_t>(1UL << 48);

template <int N>
CUresult p_cuMemAlloc(CUdevice** dptr, size_t bytesize) {
  inspect("cuMemAlloc");
  RETURN_REAL_IF_UNMOCKED(cuMemAlloc, dptr, bytesize);
  std::lock_guard<std::mutex> guard(lockMemAddr);
  memAddr -= bytesize;
  memAddr -= memAddr % 8;
  *dptr = (CUdevice*)memAddr;
  return 0;
}

template <int N>
CUresult p_cuMemFree(CUdevice* dptr) {
  inspect("cuMemFree");
  RETURN_REAL_IF_UNMOCKED(cuMemFree, dptr);
  return 0;
}

template <int N>
CUresult p_cuMemcpyDtoDAsync(
    CUdevice* dstDevice,
    CUdevice* srcDevice,
    size_t ByteCount,
    CUstream hStream) {
  inspect("cuMemcpyDtoDAsync");
  RETURN_REAL_IF_UNMOCKED(
      cuMemcpyDtoDAsync, dstDevice, srcDevice, ByteCount, hStream);
  return 0;
}

template <int N>
CUresult p_cuLaunchKernelEx(
    const CUlaunchConfig* config,
    CUfunction f,
    void** kernelParams,
    void** extra) {
  inspect("cuLaunchKernelEx");
  RETURN_REAL_IF_UNMOCKED(cuLaunchKernelEx, config, f, kernelParams, extra);
  return 0;
}

template <int N>
CUresult p_cuMemcpyDtoHAsync(
    void* dstHost,
    CUdevice* srcDevice,
    size_t ByteCount,
    CUstream hStream) {
  inspect("cuMemcpyDtoHAsync");
  RETURN_REAL_IF_UNMOCKED(
      cuMemcpyDtoHAsync, dstHost, srcDevice, ByteCount, hStream);
  return 0;
}

template <int N>
CUresult p_cuMemcpyHtoDAsync(
    CUdevice* dstDevice,
    const void* srcHost,
    size_t ByteCount,
    CUstream hStream) {
  inspect("cuMemcpyHtoDAsync");
  RETURN_REAL_IF_UNMOCKED(
      cuMemcpyHtoDAsync, dstDevice, srcHost, ByteCount, hStream);
  return 0;
}

template <int N>
CUresult p_cuMemsetD8Async(
    CUdevice* dstDevice,
    unsigned char uc,
    size_t M,
    CUstream hStream) {
  inspect("cuMemsetD8Async");
  RETURN_REAL_IF_UNMOCKED(cuMemsetD8Async, dstDevice, uc, M, hStream);
  return 0;
}

template <int N>
CUresult p_cuPointerGetAttribute(
    void* data,
    CUpointer_attribute attribute,
    CUdevice* ptr) {
  inspect("cuPointerGetAttribute");
  RETURN_REAL_IF_UNMOCKED(cuPointerGetAttribute, data, attribute, ptr);
  return 0;
}

namespace {
std::random_device _rd;
std::mt19937_64 _gen(_rd());
std::uniform_int_distribution<uint64_t> _dis;

uint64_t randUint64_t() {
  return _dis(_gen);
}
} // namespace

template <int N>
CUresult p_cuMemCreate(
    CUmemGenericAllocationHandle* handle,
    size_t size,
    const CUmemAllocationProp* prop,
    unsigned long long flags) {
  inspect("cuMemCreate");
  RETURN_REAL_IF_UNMOCKED(cuMemCreate, handle, size, prop, flags);
  *handle = randUint64_t();
  return 0;
}

template <int N>
CUresult p_cuMemRelease(CUmemGenericAllocationHandle handle) {
  inspect("cuMemRelease");
  RETURN_REAL_IF_UNMOCKED(cuMemRelease, handle);
  return 0;
}

static std::unordered_map<CUdevice*, size_t> ptrToSize;

template <int N>
CUresult p_cuMemAddressReserve(
    CUdevice** ptr,
    size_t size,
    size_t alignment,
    CUdevice* addr,
    unsigned long long flags) {
  inspect("cuMemAddressReserve");
  RETURN_REAL_IF_UNMOCKED(
      cuMemAddressReserve, ptr, size, alignment, addr, flags);
  std::lock_guard<std::mutex> guard(lockMemAddr);
  memAddr -= size;
  size_t offset = memAddr % (alignment ? alignment : 8);
  memAddr -= offset;
  *ptr = (CUdevice*)memAddr;
  ptrToSize[*ptr] = size + offset;
  return 0;
}

template <int N>
CUresult p_cuMemMap(
    CUdevice* ptr,
    size_t size,
    size_t offset,
    CUmemGenericAllocationHandle handle,
    unsigned long long flags) {
  inspect("cuMemMap");
  RETURN_REAL_IF_UNMOCKED(cuMemMap, ptr, size, offset, handle, flags);
  return 0;
}

template <int N>
CUresult p_cuMemUnmap(CUdevice* ptr, size_t size) {
  inspect("cuMemUnmap");
  RETURN_REAL_IF_UNMOCKED(cuMemUnmap, ptr, size);
  return 0;
}

template <int N>
CUresult p_cuMemSetAccess(
    CUdevice* ptr,
    size_t size,
    const CUmemAccessDesc* desc,
    size_t count) {
  inspect("cuMemSetAccess");
  RETURN_REAL_IF_UNMOCKED(cuMemSetAccess, ptr, size, desc, count);
  return 0;
}

template <int N>
CUresult p_cuMemcpyAsync(
    CUdevice* dst,
    CUdevice* src,
    size_t ByteCount,
    CUstream hStream) {
  inspect("cuMemcpyAsync");
  RETURN_REAL_IF_UNMOCKED(cuMemcpyAsync, dst, src, ByteCount, hStream);
  return 0;
}

template <int N>
CUresult p_cuMemAddressFree(CUdevice* ptr, size_t size) {
  inspect("cuMemAddressFree");
  RETURN_REAL_IF_UNMOCKED(cuMemAddressFree, ptr, size);
  return 0;
}

template <int N>
CUresult p_cuMemRetainAllocationHandle(
    CUmemGenericAllocationHandle* handle,
    void* addr) {
  inspect("cuMemRetainAllocationHandle");
  RETURN_REAL_IF_UNMOCKED(cuMemRetainAllocationHandle, handle, addr);
  return 0;
}

template <int N>
CUresult
p_cuMemGetAddressRange(CUdevice** pbase, size_t* psize, CUdevice* dptr) {
  inspect("cuMemGetAddressRange");
  RETURN_REAL_IF_UNMOCKED(cuMemGetAddressRange, pbase, psize, dptr);
  auto it = ptrToSize.find(dptr);
  if (it != ptrToSize.end()) {
    if (pbase) {
      *pbase = dptr;
    }
    if (psize) {
      *psize = it->second;
    }
    return 0;
  } else {
    for (const auto& entry : ptrToSize) {
      CUdevice* base = entry.first;
      size_t size = entry.second;
      if (dptr >= base && dptr < (CUdevice*)((char*)base + size)) {
        if (pbase) {
          *pbase = base;
        }
        if (psize) {
          *psize = size;
        }
        return 0;
      }
    }
    return 1; // CUDA_ERROR_INVALID_VALUE
  }
}

std::unordered_set<void*> patched;
std::mutex patchedMutex;

void doPatch(const char* name, void** realFns, void* toPatch, void** ourFns) {
  std::lock_guard<std::mutex> guard(patchedMutex);
  if (patched.contains(toPatch)) {
    return;
  }
  patched.emplace(toPatch);
  for (size_t i = 0; i < MAX_VERSIONS; ++i) {
    if (realFns[i] == nullptr) {
      realFns[i] = swapJumpTarget(toPatch, ourFns[i]).value();
      return;
    }
  }
  throw std::runtime_error("increase MAX_VERSIONS!");
}

#define CREATE_PATCH(fn)                    \
  if (symbol == #fn) {                      \
    doPatch(#fn, real_##fn, *pfn, ps_##fn); \
    return r;                               \
  }

template <int N>
CUresult p_cuGetProcAddress(
    const char* symbol_,
    void** pfn,
    int cudaVersion,
    uint64_t flags,
    void* symbolStatus);

template <int N>
CUresult p_cuGetProcAddress_v2(
    const char* symbol_,
    void** pfn,
    int cudaVersion,
    uint64_t flags,
    void* symbolStatus) {
  inspect("cuGetProcAddress_v2");
  auto r = ((decltype(&p_cuGetProcAddress_v2<N>))real_cuGetProcAddress_v2[N])(
      symbol_, pfn, cudaVersion, flags, symbolStatus);
  std::string symbol = symbol_;
  FORALL_FUNCTIONS(CREATE_PATCH)
  return r;
}

#ifdef MONARCH_CUDA_INSPECT
asmjit::JitRuntime* rt = new asmjit::JitRuntime();
#endif

template <int N>
CUresult p_cuGetProcAddress(
    const char* symbol_,
    void** pfn,
    int cudaVersion,
    uint64_t flags,
    void* symbolStatus) {
  inspect("cuGetProcAddress");

  auto r = ((decltype(&p_cuGetProcAddress<N>))real_cuGetProcAddress[N])(
      symbol_, pfn, cudaVersion, flags, symbolStatus);

  std::string symbol = symbol_;
  FORALL_FUNCTIONS(CREATE_PATCH)

#ifdef MONARCH_CUDA_INSPECT
  asmjit::CodeHolder code;

  code.init(rt->environment(), rt->cpuFeatures());

  asmjit::x86::Assembler a(&code);
  // Make space on the stack for 16 registers by subtracting 16 * 8 bytes from
  // the current stack pointer.
  a.lea(asmjit::x86::rsp, asmjit::x86::Mem(asmjit::x86::rsp, -16 * 8));
  // Push all register values onto the stack.
  for (int i = 0; i < 16; ++i) {
    a.mov(asmjit::x86::Mem(asmjit::x86::rsp, i * 8), asmjit::x86::Gpq(i));
  }
  // Move the name of the function to be called into the rdi register, which
  // is used as the first argument to the inspect function.
  a.mov(asmjit::x86::rdi, symbol_);
  a.call(inspect);
  // Load all of the old register values back from the stack.
  for (int i = 0; i < 16; ++i) {
    a.mov(asmjit::x86::Gpq(i), asmjit::x86::Mem(asmjit::x86::rsp, i * 8));
  }
  // Reset the stack pointer to where it was before we called inspect
  a.lea(asmjit::x86::rsp, asmjit::x86::Mem(asmjit::x86::rsp, 16 * 8));
  // Call the real cuda function.
  a.jmp(*pfn);
  // Set pfn to point to our new custom code instead of the real cuda
  // function.
  asmjit::Error err = rt->add(pfn, &code);
  if (err) {
    std::cout << "AsmJit failed: " << asmjit::DebugUtils::errorAsString(err);
  }
#endif

  return r;
}

#define DEFINE_PATCHES(fn) \
  void* ps_##fn[] = {      \
      (void*)p_##fn<0>, (void*)p_##fn<1>, (void*)p_##fn<2>, (void*)p_##fn<3>};

FORALL_FUNCTIONS(DEFINE_PATCHES)

void install() {
  void* dl = dlopen("libcuda.so.1", RTLD_NOW);
  assert(dl != 0);

#define REDIRECT_FUNCTION(fn)              \
  {                                        \
    void* sym = dlsym(dl, #fn);            \
    assert(sym);                           \
    doPatch(#fn, real_##fn, sym, ps_##fn); \
  }

  FORALL_FUNCTIONS(REDIRECT_FUNCTION)
}

} // namespace

PyObject* mock_cuda(PyObject*, PyObject*) {
  mockCudaEnabled = true;
  Py_RETURN_NONE;
}

PyObject* unmock_cuda(PyObject*, PyObject*) {
  mockCudaEnabled = false;
  Py_RETURN_NONE;
}

PyObject* patch_cuda(PyObject*, PyObject*) {
  try {
    // Call a function that might throw a C++ exception
    install();
    // If no exception, return Py_None
    Py_RETURN_NONE;
  } catch (const std::runtime_error& e) {
    // Catch specific C++ exceptions and set a Python exception
    PyErr_SetString(PyExc_RuntimeError, e.what());
    return nullptr;
  } catch (const std::exception& e) {
    // Catch other standard C++ exceptions
    PyErr_SetString(PyExc_Exception, e.what());
    return nullptr;
  } catch (...) {
    // Catch any other exceptions
    PyErr_SetString(PyExc_Exception, "An unknown error occurred");
    return nullptr;
  }
}
