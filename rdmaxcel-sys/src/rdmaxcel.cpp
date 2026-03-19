/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "rdmaxcel.h"
#include <cuda.h>
#include <unistd.h>
#include <memory>
#include <mutex>
#include <set>
#include <unordered_map>
#include <vector>
#include "driver_api.h"

// Platform-specific cast for device pointers
// In CUDA: CUdeviceptr is unsigned long long, use static_cast from size_t
// In ROCm: hipDeviceptr_t is void*, use reinterpret_cast from size_t
CUdeviceptr deviceptr_cast(size_t addr) {
#ifdef USE_ROCM
  return reinterpret_cast<CUdeviceptr>(addr);
#else
  return static_cast<CUdeviceptr>(addr);
#endif
}

// MR size must be a multiple of 2MB
const size_t MR_ALIGNMENT = 2ULL * 1024 * 1024;

// Maximum size for a single MR: 4GB max,  need to be one page under.
const size_t MAX_MR_SIZE = 4ULL * 1024 * 1024 * 1024 - MR_ALIGNMENT;

// Registration state for a segment that has been registered with RDMA.
// Only populated once registration succeeds.
struct SegmentRegistrationInfo {
  std::vector<struct ibv_mr*> mrs;
  size_t mr_size;
  uintptr_t mr_addr;
  struct mlx5dv_mkey* mkey;
  void* pd; // which PD this segment's MR was created under

  SegmentRegistrationInfo()
      : mrs(), mr_size(0), mr_addr(0), mkey(nullptr), pd(nullptr) {}
};

// Structure to hold segment information
struct SegmentInfo {
  size_t phys_address;
  size_t phys_size;
  int32_t device;
  bool is_expandable;
  std::unique_ptr<SegmentRegistrationInfo> registration;

  SegmentInfo()
      : phys_address(0), phys_size(0), device(-1), is_expandable(false) {}

  SegmentInfo(size_t addr, size_t sz, int32_t dev, bool expandable)
      : phys_address(addr),
        phys_size(sz),
        device(dev),
        is_expandable(expandable) {}
};

// Global map to track active CUDA segments: address -> SegmentInfo.
// Each segment is registered to exactly one PD (the one for its device's NIC),
// tracked by SegmentRegistrationInfo::pd.
static std::unordered_map<size_t, SegmentInfo> activeSegments;
static std::mutex segmentsMutex;

// Segment scanner callback - set via rdmaxcel_register_segment_scanner()
static rdmaxcel_segment_scanner_fn g_segment_scanner = nullptr;

// Initial buffer size for segment scanning (will grow if needed)
static size_t g_segment_buffer_size = 64;

// Helper function to scan existing segments from allocator snapshot
void scan_existing_segments() {
  // If no scanner is registered, nothing to do
  if (!g_segment_scanner) {
    return;
  }

  std::lock_guard<std::mutex> lock(segmentsMutex);

  // Allocate a buffer for the scanner to fill
  std::vector<rdmaxcel_scanned_segment_t> scanned_segments(
      g_segment_buffer_size);

  // Call the scanner
  size_t segment_count =
      g_segment_scanner(scanned_segments.data(), g_segment_buffer_size);

  // If we need more space, grow the buffer and retry
  if (segment_count > g_segment_buffer_size) {
    // Round up to next power of 2 for efficiency
    size_t new_size = g_segment_buffer_size;
    while (new_size < segment_count) {
      new_size *= 2;
    }
    g_segment_buffer_size = new_size;
    scanned_segments.resize(g_segment_buffer_size);

    // Retry with larger buffer
    segment_count =
        g_segment_scanner(scanned_segments.data(), g_segment_buffer_size);
  }

  // Create a set to track scanned segments
  std::set<std::pair<size_t, int32_t>> snapshotSegments;

  // Process scanned segments
  for (size_t i = 0; i < segment_count; i++) {
    const auto& scanned = scanned_segments[i];
    size_t segment_address = scanned.address;
    int32_t device = scanned.device;

    snapshotSegments.insert({segment_address, device});

    // Look for existing segment by address and device
    auto it = activeSegments.find(segment_address);
    if (it != activeSegments.end() && it->second.device == device) {
      // Existing segment found - update total_size if needed
      if (it->second.phys_size != scanned.size) {
        it->second.phys_size = scanned.size;
      }
    } else {
      // New segment - add it
      SegmentInfo segInfo(
          segment_address, scanned.size, device, scanned.is_expandable != 0);

      activeSegments[segment_address] = std::move(segInfo);
    }
  }

  // Remove segments that are no longer in the snapshot
  for (auto it = activeSegments.begin(); it != activeSegments.end();) {
    std::pair<size_t, int32_t> key = {it->first, it->second.device};
    if (snapshotSegments.find(key) == snapshotSegments.end()) {
      if (it->second.registration) {
        // Deregister all MRs before removing segment
        for (auto* mr : it->second.registration->mrs) {
          if (mr) {
            ibv_dereg_mr(mr);
          }
        }
        if (it->second.registration->mkey) {
          mlx5dv_destroy_mkey(it->second.registration->mkey);
        }
      }
      it = activeSegments.erase(it);
    } else {
      ++it;
    }
  }
}

extern "C" {

// Register a segment scanner callback
void rdmaxcel_register_segment_scanner(rdmaxcel_segment_scanner_fn scanner) {
  g_segment_scanner = scanner;
}

// Get count of active segments registered under the given PD
int rdma_get_active_segment_count(struct ibv_pd* pd) {
  std::lock_guard<std::mutex> lock(segmentsMutex);
  int count = 0;
  for (const auto& [addr, seg] : activeSegments) {
    if (seg.registration && seg.registration->pd == static_cast<void*>(pd)) {
      count++;
    }
  }
  return count;
}

// Get all segment info for segments registered on the given PD.
// Only segments that have a completed registration are returned.
int rdma_get_all_registered_segment_info(
    struct ibv_pd* pd,
    rdma_segment_info_t* info_array,
    int max_count) {
  if (!info_array || max_count <= 0) {
    return RDMAXCEL_INVALID_PARAMS;
  }

  std::lock_guard<std::mutex> lock(segmentsMutex);

  int count = 0;
  for (const auto& [addr, seg] : activeSegments) {
    if (!seg.registration || seg.registration->pd != static_cast<void*>(pd)) {
      continue;
    }
    if (count >= max_count) {
      break;
    }

    const auto& reg = *seg.registration;
    info_array[count].phys_address = seg.phys_address;
    info_array[count].phys_size = seg.phys_size;
    info_array[count].device = seg.device;
    info_array[count].is_expandable = seg.is_expandable ? 1 : 0;
    info_array[count].lkey = reg.mkey ? reg.mkey->lkey : 0;
    info_array[count].rkey = reg.mkey ? reg.mkey->rkey : 0;
    info_array[count].mr_size = reg.mr_size;
    info_array[count].mr_addr = reg.mr_addr;
    count++;
  }

  return count;
}

int bind_mrs(
    struct ibv_pd* pd,
    struct ibv_qp* qp,
    int access_flags,
    const std::vector<ibv_mr*>& mrs,
    struct mlx5dv_mkey** mkey) {
  auto mrs_cnt = mrs.size();
  ibv_qp_ex* qpx = ibv_qp_to_qp_ex(qp);
  if (!qpx) {
    return RDMAXCEL_QP_EX_FAILED;
  }
  mlx5dv_qp_ex* mqpx = mlx5dv_qp_ex_from_ibv_qp_ex(qpx);
  if (!mqpx) {
    return RDMAXCEL_MLX5DV_QP_EX_FAILED;
  }

  if (!*mkey) {
    struct mlx5dv_mkey_init_attr mkey_attr = {};
    mkey_attr.pd = pd;
    mkey_attr.create_flags = MLX5DV_MKEY_INIT_ATTR_FLAGS_INDIRECT;
    mkey_attr.max_entries = 32;
    auto new_mkey = mlx5dv_create_mkey(&mkey_attr);
    if (!new_mkey) {
      return RDMAXCEL_MKEY_CREATE_FAILED;
    }
    *mkey = new_mkey;
  }

  std::vector<ibv_sge> sgl(mrs_cnt);
  for (size_t i = 0; i < mrs_cnt; i++) {
    sgl[i].addr = reinterpret_cast<uintptr_t>(mrs[i]->addr);
    sgl[i].length = mrs[i]->length;
    sgl[i].lkey = mrs[i]->lkey;
  }

  qpx->wr_flags = IBV_SEND_INLINE | IBV_SEND_SIGNALED;
  ibv_wr_start(qpx);
  mlx5dv_wr_mr_list(mqpx, *mkey, access_flags, mrs_cnt, sgl.data());
  int ret = ibv_wr_complete(qpx);

  if (ret != 0) {
    return RDMAXCEL_WR_COMPLETE_FAILED;
  }

  struct ibv_wc wc{};
  while (ibv_poll_cq(qp->send_cq, 1, &wc) == 0) {
    // Continue polling until completion arrives
  }

  if (wc.status != IBV_WC_SUCCESS) {
    return RDMAXCEL_WC_STATUS_FAILED;
  }

  qpx->wr_id += 1;
  qpx->wr_flags = 0;

  return 0;
}

// Clean up newly registered MRs and a newly created mkey on failure.
static void cleanup_new_resources(
    std::vector<ibv_mr*>& new_mrs,
    struct mlx5dv_mkey* new_mkey) {
  for (auto* mr : new_mrs) {
    if (mr) {
      ibv_dereg_mr(mr);
    }
  }
  if (new_mkey) {
    mlx5dv_destroy_mkey(new_mkey);
  }
}

// Compact multiple MRs into a single MR for a segment if SGE_MAX hit
// TODO: setup a global lock, may be needed to safely do this
int compact_mrs(struct ibv_pd* pd, SegmentInfo& seg, int access_flags) {
  if (!seg.registration || seg.registration->mrs.empty()) {
    return 0; // Nothing to compact
  }
  auto& reg = *seg.registration;

  size_t total_size = seg.phys_size;
  size_t start_addr = seg.phys_address;

  // Deregister all existing MRs
  for (auto* mr : reg.mrs) {
    if (mr) {
      ibv_dereg_mr(mr);
    }
  }
  reg.mrs.clear();

  // Get dmabuf handle for the entire segment
  int fd = -1;
  CUresult cu_result = rdmaxcel_cuMemGetHandleForAddressRange(
      &fd,
      deviceptr_cast(start_addr),
      total_size,
      CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD,
      0);

  if (cu_result != CUDA_SUCCESS || fd < 0) {
    return RDMAXCEL_DMABUF_HANDLE_FAILED;
  }

  // Register the consolidated dmabuf
  auto mr = ibv_reg_dmabuf_mr(pd, 0, total_size, 0, fd, access_flags);
  close(fd); // Close fd after registration

  if (!mr) {
    return RDMAXCEL_MR_REGISTRATION_FAILED;
  }
  reg.mrs.push_back(mr);

  return 0;
}

// Register CUDA segments to the PD of each device's NIC.
// pds and qps are parallel arrays indexed by CUDA device ordinal.
// num_devices is the length of both arrays.
// A null entry means that device has no mapped NIC; its segments are skipped.
int register_segments(
    struct ibv_pd** pds,
    rdmaxcel_qp_t** qps,
    int num_devices) {
  if (!pds || !qps || num_devices <= 0) {
    return RDMAXCEL_INVALID_PARAMS;
  }
  scan_existing_segments();
  std::lock_guard<std::mutex> lock(segmentsMutex);

  int access_flags =
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;

  // We cache max_sge per PD context to avoid repeated queries
  std::unordered_map<void*, uint32_t> max_sge_cache;

  // For each segment, register any newly-allocated physical memory as MRs.
  // New MRs are accumulated separately and only committed to the segment
  // registration on success, so a failure never corrupts existing state.
  for (auto& [addr, seg] : activeSegments) {
    size_t current_mr_size = seg.registration ? seg.registration->mr_size : 0;
    if (current_mr_size == seg.phys_size) {
      continue; // already fully registered
    }

    // Look up the PD for this segment's device
    if (seg.device < 0 || seg.device >= num_devices) {
      continue; // device ordinal out of range
    }
    auto* pd = pds[seg.device];
    auto* qp = qps[seg.device];
    if (!pd || !qp) {
      continue; // no NIC for this device
    }

    // Get max_sge for this PD's device
    uint32_t max_sge;
    auto cache_it = max_sge_cache.find(static_cast<void*>(pd));
    if (cache_it != max_sge_cache.end()) {
      max_sge = cache_it->second;
    } else {
      struct ibv_device_attr dev_attr{};
      if (ibv_query_device(pd->context, &dev_attr)) {
        return RDMAXCEL_QUERY_DEVICE_FAILED;
      }
      max_sge = dev_attr.max_sge;
      max_sge_cache[static_cast<void*>(pd)] = max_sge;
    }

    std::vector<ibv_mr*> new_mrs;

    auto mr_start = seg.phys_address + current_mr_size;
    auto mr_end = seg.phys_address + seg.phys_size;
    auto remaining_size = mr_end - mr_start;

    size_t existing_mrs_count =
        seg.registration ? seg.registration->mrs.size() : 0;

    // Register in chunks of MAX_MR_SIZE
    size_t current_offset = 0;
    while (current_offset < remaining_size) {
      size_t chunk_size =
          std::min(remaining_size - current_offset, MAX_MR_SIZE);
      auto chunk_start = mr_start + current_offset;

      // Validate that chunk_size is a multiple of 2MB
      if (chunk_size % MR_ALIGNMENT != 0) {
        cleanup_new_resources(new_mrs, nullptr);
        return RDMAXCEL_MR_REGISTRATION_FAILED;
      }

      int fd = -1;
      CUresult cu_result = rdmaxcel_cuMemGetHandleForAddressRange(
          &fd,
          deviceptr_cast(chunk_start),
          chunk_size,
          CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD,
          0);

      if (cu_result != CUDA_SUCCESS || fd < 0) {
        cleanup_new_resources(new_mrs, nullptr);
        return RDMAXCEL_DMABUF_HANDLE_FAILED;
      }

      // Register the dmabuf with fd, address is always 0.
      auto mr = ibv_reg_dmabuf_mr(pd, 0, chunk_size, 0, fd, access_flags);
      close(fd);

      if (!mr) {
        cleanup_new_resources(new_mrs, nullptr);
        return RDMAXCEL_MR_REG_FAILED;
      }

      new_mrs.push_back(mr);
      current_offset += chunk_size;

      if (existing_mrs_count + new_mrs.size() > max_sge) {
        // TODO: find a safe way to compact with low performance cost.
        // return MAX_SGE error auto err = compact_mrs(pd, seg, access_flags);
        // if (err != 0) {
        //   return err;
        // }
        cleanup_new_resources(new_mrs, nullptr);
        return RDMAXCEL_MKEY_REG_LIMIT;
      }
    }

    // Build combined MR list for binding
    std::vector<ibv_mr*> all_mrs;
    if (seg.registration) {
      all_mrs = seg.registration->mrs;
    }
    all_mrs.insert(all_mrs.end(), new_mrs.begin(), new_mrs.end());

    // bind_mrs creates the mkey if null
    struct mlx5dv_mkey* mkey =
        seg.registration ? seg.registration->mkey : nullptr;
    bool had_mkey = mkey != nullptr;

    auto err = bind_mrs(pd, qp->ibv_qp, access_flags, all_mrs, &mkey);
    if (err != 0) {
      cleanup_new_resources(new_mrs, had_mkey ? nullptr : mkey);
      return err;
    }

    // Everything succeeded: commit to the segment registration
    if (!seg.registration) {
      seg.registration.reset(new SegmentRegistrationInfo());
    }
    seg.registration->mrs = std::move(all_mrs);
    seg.registration->mr_size = seg.phys_size;
    seg.registration->mr_addr = seg.phys_address;
    seg.registration->mkey = mkey;
    seg.registration->pd = static_cast<void*>(pd);
  }
  return 0; // Success
}

// Get PCI address from CUDA pointer
int get_cuda_pci_address_from_ptr(
    CUdeviceptr cuda_ptr,
    char* pci_addr_out,
    size_t pci_addr_size) {
  if (!pci_addr_out || pci_addr_size < 16) {
    return RDMAXCEL_INVALID_PARAMS;
  }

  int device_ordinal = -1;
  CUresult err = rdmaxcel_cuPointerGetAttribute(
      &device_ordinal, CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL, cuda_ptr);

  if (err != CUDA_SUCCESS) {
    return RDMAXCEL_CUDA_GET_ATTRIBUTE_FAILED;
  }

  CUdevice device;
  err = rdmaxcel_cuDeviceGet(&device, device_ordinal);
  if (err != CUDA_SUCCESS) {
    return RDMAXCEL_CUDA_GET_DEVICE_FAILED;
  }

  int pci_bus_id = -1;
  int pci_device_id = -1;
  int pci_domain_id = -1;

  // Get PCI bus ID
  err = rdmaxcel_cuDeviceGetAttribute(
      &pci_bus_id, CU_DEVICE_ATTRIBUTE_PCI_BUS_ID, device);
  if (err != CUDA_SUCCESS) {
    return RDMAXCEL_CUDA_GET_ATTRIBUTE_FAILED;
  }

  // Get PCI device ID
  err = rdmaxcel_cuDeviceGetAttribute(
      &pci_device_id, CU_DEVICE_ATTRIBUTE_PCI_DEVICE_ID, device);
  if (err != CUDA_SUCCESS) {
    return RDMAXCEL_CUDA_GET_ATTRIBUTE_FAILED;
  }

  // Get PCI domain ID
  err = rdmaxcel_cuDeviceGetAttribute(
      &pci_domain_id, CU_DEVICE_ATTRIBUTE_PCI_DOMAIN_ID, device);
  if (err != CUDA_SUCCESS) {
    return RDMAXCEL_CUDA_GET_ATTRIBUTE_FAILED;
  }

  // Format PCI address as "domain:bus:device.0"
  int written = snprintf(
      pci_addr_out,
      pci_addr_size,
      "%04x:%02x:%02x.0",
      pci_domain_id,
      pci_bus_id,
      pci_device_id);

  if (written < 0 || written >= (int)pci_addr_size) {
    return RDMAXCEL_BUFFER_TOO_SMALL;
  }

  return 0; // Success
}

// Deregister all segments and clean up
int deregister_segments() {
  std::lock_guard<std::mutex> lock(segmentsMutex);

  for (auto& pair : activeSegments) {
    SegmentInfo& seg = pair.second;

    if (seg.registration) {
      for (auto* mr : seg.registration->mrs) {
        if (mr) {
          ibv_dereg_mr(mr);
        }
      }

      if (seg.registration->mkey) {
        mlx5dv_destroy_mkey(seg.registration->mkey);
      }

      seg.registration.reset();
    }
  }

  // Clear all segments
  activeSegments.clear();

  return 0; // Success
}

// Debug: Print comprehensive device attributes
void rdmaxcel_print_device_info(struct ibv_context* context) {
  if (!context) {
    fprintf(stderr, "[RdmaXcel] Error: NULL context provided\n");
    return;
  }

  struct ibv_device_attr dev_attr;
  if (ibv_query_device(context, &dev_attr) != 0) {
    fprintf(stderr, "[RdmaXcel] Error: Failed to query device attributes\n");
    return;
  }

  fprintf(
      stderr,
      "\n[RdmaXcel] ==================== Device Attributes ====================\n");
  fprintf(
      stderr,
      "[RdmaXcel] Firmware: %s, Vendor: 0x%x (Part ID: %u)\n",
      dev_attr.fw_ver,
      dev_attr.vendor_id,
      dev_attr.vendor_part_id);
  fprintf(
      stderr,
      "[RdmaXcel] Max MR size: %.2f GB, Page size cap: 0x%llx\n",
      (double)dev_attr.max_mr_size / (1024.0 * 1024.0 * 1024.0),
      (unsigned long long)dev_attr.page_size_cap);
  fprintf(
      stderr,
      "[RdmaXcel] Queue Pairs: Max QP=%d, Max QP WR=%d, Max SGE=%d\n",
      dev_attr.max_qp,
      dev_attr.max_qp_wr,
      dev_attr.max_sge);
  fprintf(
      stderr,
      "[RdmaXcel] Completion Queues: Max CQ=%d, Max CQE=%d\n",
      dev_attr.max_cq,
      dev_attr.max_cqe);
  fprintf(
      stderr,
      "[RdmaXcel] Memory: Max MR=%d, Max PD=%d\n",
      dev_attr.max_mr,
      dev_attr.max_pd);
  fprintf(
      stderr,
      "[RdmaXcel] RDMA Ops: Max QP RD atom=%d, Max QP init RD atom=%d\n",
      dev_attr.max_qp_rd_atom,
      dev_attr.max_qp_init_rd_atom);
  fprintf(
      stderr,
      "[RdmaXcel] Shared Receive: Max SRQ=%d, Max SRQ WR=%d, Max SRQ SGE=%d\n",
      dev_attr.max_srq,
      dev_attr.max_srq_wr,
      dev_attr.max_srq_sge);
  fprintf(
      stderr,
      "[RdmaXcel] Physical ports: %u, Max pkeys: %u\n",
      dev_attr.phys_port_cnt,
      dev_attr.max_pkeys);
  fprintf(
      stderr,
      "[RdmaXcel] ==================================================================\n\n");
}

const char* rdmaxcel_error_string(int error_code) {
  switch (error_code) {
    case RDMAXCEL_SUCCESS:
      return "[RdmaXcel] Success";
    case RDMAXCEL_INVALID_PARAMS:
      return "[RdmaXcel] Invalid parameters provided";
    case RDMAXCEL_MR_REGISTRATION_FAILED:
      return "[RdmaXcel] Memory region registration failed during compaction";
    case RDMAXCEL_DMABUF_HANDLE_FAILED:
      return "[RdmaXcel] Failed to get dmabuf handle for CUDA memory region";
    case RDMAXCEL_MR_REG_FAILED:
      return "[RdmaXcel] Memory region registration failed in register_segments";
    case RDMAXCEL_MEMORY_BINDING_FAILED:
      return "[RdmaXcel] Memory binding failed - hardware limit exceeded or MLX5 constraint";
    case RDMAXCEL_QP_EX_FAILED:
      return "[RdmaXcel] Failed to get extended queue pair (ibv_qp_to_qp_ex)";
    case RDMAXCEL_MLX5DV_QP_EX_FAILED:
      return "[RdmaXcel] Failed to get MLX5DV extended queue pair (mlx5dv_qp_ex_from_ibv_qp_ex)";
    case RDMAXCEL_MKEY_CREATE_FAILED:
      return "[RdmaXcel] Failed to create MLX5 memory key (mlx5dv_create_mkey)";
    case RDMAXCEL_WR_COMPLETE_FAILED:
      return "[RdmaXcel] Work request completion failed (ibv_wr_complete)";
    case RDMAXCEL_WC_STATUS_FAILED:
      return "[RdmaXcel] Work completion status failed - memory registration unsuccessful";
    case RDMAXCEL_MKEY_REG_LIMIT:
      return "[RdmaXcel] mkey registration failed - segment size > 4 GiB or SGL max exceeded";
    case RDMAXCEL_CUDA_GET_ATTRIBUTE_FAILED:
      return "[RdmaXcel] Failed to get CUDA device attribute";
    case RDMAXCEL_CUDA_GET_DEVICE_FAILED:
      return "[RdmaXcel] Failed to get CUDA device handle";
    case RDMAXCEL_BUFFER_TOO_SMALL:
      return "[RdmaXcel] Output buffer too small";
    case RDMAXCEL_QUERY_DEVICE_FAILED:
      return "[RdmaXcel] Failed to query device attributes";
    case RDMAXCEL_CQ_POLL_FAILED:
      return "[RdmaXcel] CQ polling failed";
    case RDMAXCEL_COMPLETION_FAILED:
      return "[RdmaXcel] Completion status not successful";
    case RDMAXCEL_QP_MODIFY_FAILED:
      return "[RdmaXcel] QP state transition failed (ibv_modify_qp)";
    case RDMAXCEL_AH_CREATE_FAILED:
      return "[RdmaXcel] Address handle creation failed (ibv_create_ah)";
    case RDMAXCEL_UNSUPPORTED_OP:
      return "[RdmaXcel] Unsupported operation type";
    default:
      return "[RdmaXcel] Unknown error code";
  }
}

} // extern "C"
