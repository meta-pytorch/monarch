/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/*
 * Host build of the GPU-runtime-free rdmaxcel cores (db_ring / send_wqe /
 * recv_wqe / cqe_poll + byte swaps). Compiled by the host C compiler ONLY when
 * kernel-launched RDMA is disabled (the default); in that configuration these
 * are the CPU-initiated RDMA primitives and pull in no CUDA/HIP runtime, so
 * nothing links libamdhip64.
 *
 * When kernel-launched RDMA is enabled, rdmaxcel.cu includes the same header and
 * provides these symbols instead -- build.rs compiles exactly one of the two.
 */
#include "rdmaxcel_core_impl.h"
