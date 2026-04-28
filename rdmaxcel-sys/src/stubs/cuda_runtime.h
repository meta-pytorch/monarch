/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Stub replacement for <cuda_runtime.h> used in no-GPU builds. Declares
// only the handful of CUDA runtime types that are named in `rdmaxcel.h`
// and stubs out the CUDA function-space keywords so that headers
// sprinkled with `__host__`/`__device__`/`__global__` still parse.

#ifndef RDMAXCEL_STUB_CUDA_RUNTIME_H
#define RDMAXCEL_STUB_CUDA_RUNTIME_H

#include <stddef.h>

#ifndef __host__
#define __host__
#endif
#ifndef __device__
#define __device__
#endif
#ifndef __global__
#define __global__
#endif
#ifndef __forceinline__
#define __forceinline__ inline
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef int cudaError_t;

#define cudaSuccess ((cudaError_t)0)
#define cudaErrorNotInitialized ((cudaError_t)3)

#ifdef __cplusplus
} // extern "C"
#endif

#endif // RDMAXCEL_STUB_CUDA_RUNTIME_H
