/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#ifndef RDMAXCEL_MLX5_IFC_SUBSET_H
#define RDMAXCEL_MLX5_IFC_SUBSET_H

#include <endian.h>
#include <stddef.h>
#include <stdint.h>

// Minimal mlx5 IFC schema for the DevX commands used by rdmaxcel. These types
// describe bits: array sizes and field offsets are bit counts, so the types
// must only be used through the DEVX_* helpers below.

enum {
  MLX5_CMD_OP_QUERY_HCA_CAP = 0x100,
  MLX5_CMD_OP_CREATE_MKEY = 0x200,
};

enum mlx5_cap_mode {
  HCA_CAP_OPMOD_GET_MAX = 0,
  HCA_CAP_OPMOD_GET_CUR = 1,
};

enum {
  MLX5_MKC_ACCESS_MODE_KLMS = 0x2,
};

struct mlx5_ifc_cmd_hca_cap_bits {
  uint8_t reserved_at_0[0x11a];
  uint8_t log_max_klm_list_size[0x6];
};

union mlx5_ifc_hca_cap_union_bits {
  struct mlx5_ifc_cmd_hca_cap_bits cmd_hca_cap;
  uint8_t reserved_at_0[0x8000];
};

struct mlx5_ifc_query_hca_cap_out_bits {
  uint8_t status[0x8];
  uint8_t reserved_at_8[0x18];
  uint8_t syndrome[0x20];
  uint8_t reserved_at_40[0x40];
  union mlx5_ifc_hca_cap_union_bits capability;
};

struct mlx5_ifc_query_hca_cap_in_bits {
  uint8_t opcode[0x10];
  uint8_t reserved_at_10[0x20];
  uint8_t op_mod[0x10];
  uint8_t reserved_at_40[0x40];
};

struct mlx5_ifc_mkc_bits {
  uint8_t reserved_at_0[0x11];
  uint8_t a[0x1];
  uint8_t rw[0x1];
  uint8_t rr[0x1];
  uint8_t lw[0x1];
  uint8_t lr[0x1];
  uint8_t access_mode_1_0[0x2];
  uint8_t reserved_at_18[0x8];
  uint8_t qpn[0x18];
  uint8_t mkey_7_0[0x8];
  uint8_t reserved_at_40[0x28];
  uint8_t pd[0x18];
  uint8_t start_addr[0x40];
  uint8_t len[0x40];
  uint8_t reserved_at_100[0xa0];
  uint8_t translations_octword_size[0x20];
  uint8_t reserved_at_1c0[0x40];
};

struct mlx5_ifc_create_mkey_out_bits {
  uint8_t status[0x8];
  uint8_t reserved_at_8[0x18];
  uint8_t syndrome[0x20];
  uint8_t reserved_at_40[0x8];
  uint8_t mkey_index[0x18];
  uint8_t reserved_at_60[0x20];
};

struct mlx5_ifc_create_mkey_in_bits {
  uint8_t opcode[0x10];
  uint8_t reserved_at_10[0x70];
  struct mlx5_ifc_mkc_bits memory_key_mkey_entry;
  uint8_t reserved_at_280[0x80];
  uint8_t translations_octword_actual_size[0x20];
  uint8_t reserved_at_320[0x560];
  uint8_t klm_pas_mtt[0][0x20];
};

#define RDMAXCEL_DEVX_NULLP(type) ((struct mlx5_ifc_##type##_bits*)NULL)
#define RDMAXCEL_DEVX_BIT_SIZE(type, field) \
  sizeof(RDMAXCEL_DEVX_NULLP(type)->field)
#define RDMAXCEL_DEVX_BIT_OFFSET(type, field) \
  offsetof(struct mlx5_ifc_##type##_bits, field)
#define RDMAXCEL_DEVX_DWORD_OFFSET(bit_offset) ((bit_offset) / 32)
#define RDMAXCEL_DEVX_QWORD_OFFSET(bit_offset) ((bit_offset) / 64)
#define RDMAXCEL_DEVX_DWORD_BIT_OFFSET(bit_size, bit_offset) \
  (32 - (bit_size) - ((bit_offset) & 0x1f))
#define RDMAXCEL_DEVX_MASK(bit_size) \
  ((uint32_t)((UINT64_C(1) << (bit_size)) - 1))

#ifndef DEVX_ST_SZ_BYTES
#define DEVX_ST_SZ_BYTES(type) (sizeof(struct mlx5_ifc_##type##_bits) / 8)
#endif

#ifndef DEVX_ST_SZ_DW
#define DEVX_ST_SZ_DW(type) (sizeof(struct mlx5_ifc_##type##_bits) / 32)
#endif

#ifndef DEVX_ADDR_OF
#define DEVX_ADDR_OF(type, pointer, field) \
  ((unsigned char*)(pointer) + RDMAXCEL_DEVX_BIT_OFFSET(type, field) / 8)
#endif

#ifndef DEVX_SET
static inline void rdmaxcel_devx_set(
    void* pointer,
    uint32_t value,
    size_t bit_offset,
    size_t bit_size) {
  uint32_t* field = (uint32_t*)pointer + RDMAXCEL_DEVX_DWORD_OFFSET(bit_offset);
  const uint32_t value_mask = RDMAXCEL_DEVX_MASK(bit_size);
  const uint32_t field_mask = value_mask
      << RDMAXCEL_DEVX_DWORD_BIT_OFFSET(bit_size, bit_offset);
  *field = htobe32(
      (be32toh(*field) & ~field_mask) |
      ((value & value_mask)
       << RDMAXCEL_DEVX_DWORD_BIT_OFFSET(bit_size, bit_offset)));
}

#define DEVX_SET(type, pointer, field, value) \
  rdmaxcel_devx_set(                          \
      pointer,                                \
      value,                                  \
      RDMAXCEL_DEVX_BIT_OFFSET(type, field),  \
      RDMAXCEL_DEVX_BIT_SIZE(type, field))
#endif

#ifndef DEVX_GET
static inline uint32_t
rdmaxcel_devx_get(const void* pointer, size_t bit_offset, size_t bit_size) {
  const uint32_t* field =
      (const uint32_t*)pointer + RDMAXCEL_DEVX_DWORD_OFFSET(bit_offset);
  return (be32toh(*field) >>
          RDMAXCEL_DEVX_DWORD_BIT_OFFSET(bit_size, bit_offset)) &
      RDMAXCEL_DEVX_MASK(bit_size);
}

#define DEVX_GET(type, pointer, field)       \
  rdmaxcel_devx_get(                         \
      pointer,                               \
      RDMAXCEL_DEVX_BIT_OFFSET(type, field), \
      RDMAXCEL_DEVX_BIT_SIZE(type, field))
#endif

#ifndef DEVX_SET64
static inline void
rdmaxcel_devx_set64(void* pointer, uint64_t value, size_t bit_offset) {
  *((uint64_t*)pointer + RDMAXCEL_DEVX_QWORD_OFFSET(bit_offset)) =
      htobe64(value);
}

#define DEVX_SET64(type, pointer, field, value) \
  rdmaxcel_devx_set64(pointer, value, RDMAXCEL_DEVX_BIT_OFFSET(type, field))
#endif

#endif
