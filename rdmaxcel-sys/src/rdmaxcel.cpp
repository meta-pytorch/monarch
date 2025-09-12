/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "rdmaxcel.h"
#include <c10/cuda/CUDAAllocatorConfig.h>
#include <c10/cuda/CUDACachingAllocator.h>
#include <cuda.h>
#include <unistd.h>
#include <iostream>
#include <mutex>
#include <unordered_map>

// Structure to hold segment information
struct SegmentInfo {
  size_t phys_address;
  size_t phys_size;
  int32_t device;
  bool is_expandable;
  std::vector<struct ibv_mr*> mrs;
  uint32_t lkey;
  uint32_t rkey;
  size_t mr_size;
  uintptr_t mr_addr;

  // Default constructor - initialize mr as null, keys as 0
  SegmentInfo()
      : phys_address(0),
        phys_size(0),
        device(-1),
        is_expandable(false),
        mrs(),
        lkey(0),
        rkey(0),
        mr_size(0),
        mr_addr(0) {}

  // Parameterized constructor
  SegmentInfo(size_t addr, size_t sz, int32_t dev, bool expandable)
      : phys_address(addr),
        phys_size(sz),
        device(dev),
        is_expandable(expandable),
        mrs(),
        lkey(0),
        rkey(0),
        mr_size(0),
        mr_addr(0) {}
};

// Global map to track active CUDA segments by address
static std::unordered_map<size_t, SegmentInfo> activeSegments;
static std::mutex segmentsMutex;

// Helper function to scan existing segments from allocator snapshot
void scan_existing_segments() {
  std::lock_guard<std::mutex> lock(segmentsMutex);

  // Get current snapshot from the allocator
  auto snapshot = c10::cuda::CUDACachingAllocator::snapshot();

  // Clear current segments
  activeSegments.clear();

  // Process snapshot segments directly
  for (const auto& segment : snapshot.segments) {
    size_t segment_address = reinterpret_cast<size_t>(segment.address);

    // Create segment info
    SegmentInfo segInfo(
        segment_address,
        segment.total_size,
        segment.device,
        segment.is_expandable);

    activeSegments[segment_address] = segInfo;
  }
}

extern "C" {

// Simple check for PyTorch CUDA allocator compatibility
bool pt_cuda_allocator_compatibility() {
  return (
      c10::cuda::CUDACachingAllocator::isEnabled() &&
      c10::cuda::CUDACachingAllocator::CUDAAllocatorConfig::
          expandable_segments());
}

// Get count of active segments
int rdma_get_active_segment_count() {
  std::lock_guard<std::mutex> lock(segmentsMutex);
  return static_cast<int>(activeSegments.size());
}

// Get all segment info into an array
int rdma_get_all_segment_info(rdma_segment_info_t* info_array, int max_count) {
  if (!info_array || max_count <= 0) {
    return -1; // Invalid parameters
  }

  std::lock_guard<std::mutex> lock(segmentsMutex);

  int count = 0;
  for (const auto& pair : activeSegments) {
    if (count >= max_count) {
      break; // Avoid buffer overflow
    }

    const SegmentInfo& seg = pair.second;
    info_array[count].phys_address = seg.phys_address;
    info_array[count].phys_size = seg.phys_size;
    info_array[count].device = seg.device;
    info_array[count].is_expandable = seg.is_expandable ? 1 : 0;
    info_array[count].lkey = seg.lkey;
    info_array[count].rkey = seg.rkey;
    info_array[count].mr_size = seg.mr_size;
    info_array[count].mr_addr = seg.mr_addr;
    count++;
  }

  return count; // Return number of segments copied
}

void check(void* ptr, const char* errorMessage) {
  if (!ptr) {
    std::cerr << errorMessage << ": " << strerror(errno) << std::endl;
    exit(2);
  }
}

struct mlx5dv_mkey* bind_mrs(
    struct ibv_pd* pd,
    struct ibv_qp* qp,
    int access_flags,
    std::vector<ibv_mr*> mrs) {
  ibv_qp_ex* qpx = ibv_qp_to_qp_ex(qp);
  check(qpx, "qpx");
  mlx5dv_qp_ex* mqpx = mlx5dv_qp_ex_from_ibv_qp_ex(qpx);
  check(mqpx, "mqpx");
  auto mrs_cnt = mrs.size();
  struct mlx5dv_mkey_init_attr mkey_attr = {};
  mkey_attr.pd = pd;
  mkey_attr.create_flags = MLX5DV_MKEY_INIT_ATTR_FLAGS_INDIRECT;
  mkey_attr.max_entries = mrs_cnt;
  struct mlx5dv_mkey* mkey = mlx5dv_create_mkey(&mkey_attr);
  check(mkey, "mkey");

  std::vector<ibv_sge> sgl(mrs_cnt);
  for (size_t i = 0; i < mrs_cnt; i++) {
    sgl[i].addr = reinterpret_cast<uintptr_t>(mrs[i]->addr);
    sgl[i].length = mrs[i]->length;
    sgl[i].lkey = mrs[i]->lkey;
  }

  qpx->wr_flags = IBV_SEND_INLINE | IBV_SEND_SIGNALED;
  ibv_wr_start(qpx);
  struct mlx5dv_mkey_conf_attr mkey_cattr = {};
  mlx5dv_wr_mkey_configure(mqpx, mkey, 2, &mkey_cattr);
  mlx5dv_wr_set_mkey_access_flags(mqpx, access_flags);
  mlx5dv_wr_set_mkey_layout_list(mqpx, mrs_cnt, sgl.data());
  int ret = ibv_wr_complete(qpx);

  if (ret != 0) {
    std::cout << "ibv_wr_complete failed " << strerror(ret) << "\n";
    exit(1);
  }

  // Poll for completion and verify it matches the expected wr_id
  struct ibv_wc wc {};
  while (ibv_poll_cq(qp->send_cq, 1, &wc) == 0) {
    // Continue polling until completion arrives
  }

  if (wc.status != IBV_WC_SUCCESS) {
    std::cout << "memory registration wc failed: " << wc.status << "\n";
    exit(1);
  }

  // Now increment wr_id for the next work request
  qpx->wr_id += 1;
  qpx->wr_flags = 0;

  return mkey;
}

// Register memory region for a specific segment address, assume cuda
int register_segments(struct ibv_pd* pd, struct ibv_qp* qp) {
  if (!pd) {
    return -1; // Invalid parameter
  }
  scan_existing_segments();
  std::lock_guard<std::mutex> lock(segmentsMutex);

  // Conservative access flags - remove ATOMIC for CUDA DMA-BUF compatibility
  int access_flags =
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;

  for (auto& pair : activeSegments) {
    SegmentInfo& seg = pair.second;
    if (seg.mr_size != seg.phys_size) {
      auto mr_start = seg.phys_address + seg.mr_size;
      auto mr_end = seg.phys_address + seg.phys_size;
      auto mr_size = mr_end - mr_start;
      int fd = -1;
      CUresult cu_result = cuMemGetHandleForAddressRange(
          &fd,
          static_cast<CUdeviceptr>(mr_start),
          mr_size,
          CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD,
          0);

      if (cu_result != CUDA_SUCCESS || fd < 0) {
        return -3; // Failed to get dmabuf handle
      }

      // Register the dmabuf with fd, address is 0.
      auto mr = ibv_reg_dmabuf_mr(pd, 0, mr_size, 0, fd, access_flags);
      close(fd); // Close fd after registration

      if (!mr) {
        return -4; // MR registration failed
      }

      seg.mrs.push_back(mr);
      seg.mr_size = mr_size;
      // Create vector of GPU addresses for bind_mrs
      auto keys = bind_mrs(pd, qp, access_flags, seg.mrs);
      seg.lkey = keys->lkey;
      seg.rkey = keys->rkey;
    }
  }
  return 0; // Success
}

} // extern "C"
