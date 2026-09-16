/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/*
 * Shared, GPU-runtime-free core of rdmaxcel: doorbell / WQE / CQE helpers.
 *
 * These functions manipulate only host-visible WQE/CQE/doorbell memory (memcpy,
 * byte swaps, MMIO writes) and call NO CUDA/HIP runtime API. They are marked
 * `RDMAXCEL_HD` so that:
 *   - when compiled by the GPU compiler (feature `cuda`/kernel-launched RDMA on),
 *     they are `__host__ __device__` and can be invoked from both the CPU and
 *     from the `cu_*` kernels in rdmaxcel.cu;
 *   - when compiled by a plain host compiler (kernel-launched RDMA OFF, the
 *     default), they are ordinary host functions, so CPU-initiated RDMA works
 *     with no libamdhip64 link.
 *
 * Exactly ONE translation unit must include this file (rdmaxcel.cu when kernels
 * are built, rdmaxcel_core.c otherwise) so the extern symbols are defined once.
 */
#ifndef RDMAXCEL_CORE_IMPL_H
#define RDMAXCEL_CORE_IMPL_H

#include <assert.h>
#include <stdint.h>
#include <string.h>

#include "rdmaxcel.h"

#if defined(__CUDACC__) || defined(__HIPCC__) || defined(__HIP__)
#define RDMAXCEL_HD __host__ __device__
#else
#define RDMAXCEL_HD
#endif

/* ---- endianness helpers (InfiniBand hardware is big-endian) ---- */

RDMAXCEL_HD static inline uint16_t byte_swap16(uint16_t val) {
  return ((val & 0xFF00) >> 8) | ((val & 0x00FF) << 8);
}

RDMAXCEL_HD static inline uint32_t byte_swap32(uint32_t val) {
  return ((val & 0xFF000000) >> 24) | ((val & 0x00FF0000) >> 8) |
      ((val & 0x0000FF00) << 8) | ((val & 0x000000FF) << 24);
}

RDMAXCEL_HD static inline uint64_t byte_swap64(uint64_t val) {
  return ((val & 0xFF00000000000000ULL) >> 56) |
      ((val & 0x00FF000000000000ULL) >> 40) |
      ((val & 0x0000FF0000000000ULL) >> 24) |
      ((val & 0x000000FF00000000ULL) >> 8) |
      ((val & 0x00000000FF000000ULL) << 8) |
      ((val & 0x0000000000FF0000ULL) << 24) |
      ((val & 0x000000000000FF00ULL) << 40) |
      ((val & 0x00000000000000FFULL) << 56);
}

/* ---- doorbell ---- */

RDMAXCEL_HD void db_ring(void* dst, void* src) {
  volatile uint64_t* dst_v = (uint64_t*)dst;
  volatile uint64_t* src_v = (uint64_t*)src;
  dst_v[0] = src_v[0];
  dst_v[1] = src_v[1];
  dst_v[2] = src_v[2];
  dst_v[3] = src_v[3];
  dst_v[4] = src_v[4];
  dst_v[5] = src_v[5];
  dst_v[6] = src_v[6];
  dst_v[7] = src_v[7];
}

/* ---- Work Queue Element (WQE) operations ---- */

RDMAXCEL_HD void recv_wqe(wqe_params_t params) {
  // For MLX5 receive WQEs, we need to create a proper structure with:
  // 1. A next segment (mlx5_wqe_srq_next_seg)
  // 2. A data segment (mlx5_wqe_data_seg)

  // Declare individual segments instead of using the combined struct
  struct mlx5_wqe_data_seg data_seg;

  // Initialize the data segment
  data_seg.byte_count = byte_swap32(params.length);
  data_seg.lkey = byte_swap32(params.lkey);
  data_seg.addr = byte_swap64(params.laddr);

  // Calculate pointers for segments
  uintptr_t data_seg_ptr = (uintptr_t)params.buf;

  // Copy segments to WQE buffer
  memcpy((void*)data_seg_ptr, &data_seg, sizeof(data_seg));

  volatile uint32_t* dbrec = params.dbrec; // Declare a volatile pointer
  dbrec[MLX5_RCV_DBR] = byte_swap32(params.wr_id + 1);
}

RDMAXCEL_HD void send_wqe(wqe_params_t params) {
  struct mlx5_wqe_ctrl_seg ctrl_seg = {0};
  struct mlx5_wqe_data_seg data_seg = {0};
  struct mlx5_wqe_raddr_seg raddr_seg = {0};

  uint32_t idx = params.wr_id;
  uint32_t buffer_idx = idx & (params.wqe_cnt - 1);

  // Set control segment
  ctrl_seg.fm_ce_se =
      params.signaled ? MLX5_WQE_CTRL_CQ_UPDATE | MLX5_WQE_CTRL_SOLICITED : 0;

  // Set opcode based on operation type
  ctrl_seg.opmod_idx_opcode = ((idx << 8) | params.op_type);

  // Convert to big endian
  ctrl_seg.opmod_idx_opcode = byte_swap32(ctrl_seg.opmod_idx_opcode);

  // Set QP number and data size (48 bytes / 16 = 3 DS)
  ctrl_seg.qpn_ds = (params.qp_num << 8 | (48 / 16));
  ctrl_seg.qpn_ds = byte_swap32(ctrl_seg.qpn_ds);

  // Set remote address segment
  raddr_seg.raddr = byte_swap64(params.raddr);
  raddr_seg.rkey = byte_swap32(params.rkey);

  // Set data segment
  data_seg.addr = byte_swap64(params.laddr);
  data_seg.byte_count = byte_swap32(params.length);
  data_seg.lkey = byte_swap32(params.lkey);

  // Calculate pointers for segments
  uintptr_t ctrl_seg_ptr =
      (uintptr_t)(params.buf) + (buffer_idx << MLX5_SEND_WQE_SHIFT);
  uintptr_t raddr_seg_ptr = ctrl_seg_ptr + sizeof(ctrl_seg);
  uintptr_t data_seg_ptr = raddr_seg_ptr + sizeof(raddr_seg);

  // Copy segments to WQE buffer
  memcpy((void*)ctrl_seg_ptr, &ctrl_seg, sizeof(ctrl_seg));
  memcpy((void*)raddr_seg_ptr, &raddr_seg, sizeof(raddr_seg));
  memcpy((void*)data_seg_ptr, &data_seg, sizeof(data_seg));

  volatile uint32_t* dbrec = params.dbrec;
  dbrec[MLX5_SND_DBR] = byte_swap32((idx + 1) & 0xFFFFFF);
}

/* ---- Completion Queue Element (CQE) operations ---- */

RDMAXCEL_HD void cqe_poll(int32_t* byte_cnt, cqe_poll_params_t params) {
  assert(*byte_cnt == -1); // byte_cnt should be initialized to -1

  // Calculate the index in the CQ buffer
  uint32_t idx = params.consumer_index;
  uint32_t buffer_idx = idx & (params.cqe_cnt - 1);

  // Get the CQE at that index
  uint8_t* cqe = params.cqe_buf + (buffer_idx * params.cqe_size);

  // The op_own byte is the last byte of the CQE
  uint8_t op_own = cqe[params.cqe_size - 1];

  // Extract the opcode (upper 4 bits)
  uint8_t actual_opcode = op_own >> 4;

  // to check if the CQE is owned by SW (but opcode at 0xF implies also not
  // owned!)
  bool is_sw_owned = ((op_own & 0x1) == ((idx / params.cqe_cnt) & 0x1));
  is_sw_owned = is_sw_owned && (actual_opcode != 0xF);

  // this only checks for valid opcode, in some case should generate error
  const uint8_t FIRST_TWO_BITS_MASK = 0xC; // Binary: 1100
  bool is_valid_opcode = (actual_opcode & FIRST_TWO_BITS_MASK) == 0;

  if (is_sw_owned && is_valid_opcode) {
    *byte_cnt = byte_swap32(*(uint32_t*)(cqe + 44));
    volatile uint32_t* dbrec = (uint32_t*)params.dbrec;
    *dbrec = byte_swap32((idx + 1) & 0xFFFFFF);
  } else if (is_sw_owned && !is_valid_opcode) {
    *byte_cnt = -2; // signal error
    volatile uint32_t* dbrec = (uint32_t*)params.dbrec;
    *dbrec = byte_swap32((idx + 1) & 0xFFFFFF);
  }
}

#endif /* RDMAXCEL_CORE_IMPL_H */
