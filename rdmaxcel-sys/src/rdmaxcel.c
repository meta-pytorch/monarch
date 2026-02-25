/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "rdmaxcel.h"
#include <errno.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

// ============================================================================
// RDMAXCEL QP Wrapper Implementation
// ============================================================================

rdmaxcel_qp_t* rdmaxcel_qp_create(
    struct ibv_context* context,
    struct ibv_pd* pd,
    int cq_entries,
    int max_send_wr,
    int max_recv_wr,
    int max_send_sge,
    int max_recv_sge,
    rdma_qp_type_t qp_type) {
  // Allocate wrapper structure
  rdmaxcel_qp_t* qp = (rdmaxcel_qp_t*)calloc(1, sizeof(rdmaxcel_qp_t));
  if (!qp) {
    fprintf(stderr, "ERROR: Failed to allocate rdmaxcel_qp_t\n");
    return NULL;
  }

  // Create underlying ibverbs QP
  qp->ibv_qp = create_qp(
      context,
      pd,
      cq_entries,
      max_send_wr,
      max_recv_wr,
      max_send_sge,
      max_recv_sge,
      qp_type);
  if (!qp->ibv_qp) {
    free(qp);
    return NULL;
  }

  // Store CQ pointers
  qp->send_cq = qp->ibv_qp->send_cq;
  qp->recv_cq = qp->ibv_qp->recv_cq;
  qp->is_efa = (qp_type == RDMA_QP_TYPE_EFA) ? 1 : 0;

  // Get extended QP handle for EFA
  if (qp->is_efa) {
    qp->qpex = ibv_qp_to_qp_ex(qp->ibv_qp);
    if (!qp->qpex) {
      fprintf(stderr, "ERROR: Failed to get ibv_qp_ex for EFA QP\n");
      ibv_destroy_qp(qp->ibv_qp);
      free(qp);
      return NULL;
    }
  } else {
    qp->qpex = NULL;
  }

  // Initialize atomic counters
  atomic_init(&qp->send_wqe_idx, 0);
  atomic_init(&qp->send_db_idx, 0);
  atomic_init(&qp->send_cq_idx, 0);
  atomic_init(&qp->recv_wqe_idx, 0);
  atomic_init(&qp->recv_db_idx, 0);
  atomic_init(&qp->recv_cq_idx, 0);
  atomic_init(&qp->rts_timestamp, UINT64_MAX);

  // Initialize completion caches
  qp->send_completion_cache =
      (completion_cache_t*)calloc(1, sizeof(completion_cache_t));
  qp->recv_completion_cache =
      (completion_cache_t*)calloc(1, sizeof(completion_cache_t));

  if (!qp->send_completion_cache || !qp->recv_completion_cache) {
    if (qp->send_completion_cache)
      free(qp->send_completion_cache);
    if (qp->recv_completion_cache)
      free(qp->recv_completion_cache);
    ibv_destroy_qp(qp->ibv_qp);
    free(qp);
    fprintf(stderr, "ERROR: Failed to allocate completion caches\n");
    return NULL;
  }

  completion_cache_init(qp->send_completion_cache);
  completion_cache_init(qp->recv_completion_cache);

  return qp;
}

void rdmaxcel_qp_destroy(rdmaxcel_qp_t* qp) {
  if (!qp) {
    return;
  }

  // Clean up completion caches
  if (qp->send_completion_cache) {
    completion_cache_destroy(qp->send_completion_cache);
    free(qp->send_completion_cache);
  }
  if (qp->recv_completion_cache) {
    completion_cache_destroy(qp->recv_completion_cache);
    free(qp->recv_completion_cache);
  }

  // Destroy the underlying ibv_qp and its CQs
  if (qp->ibv_qp) {
    ibv_destroy_qp(qp->ibv_qp);
  }
  if (qp->send_cq) {
    ibv_destroy_cq(qp->send_cq);
  }
  if (qp->recv_cq) {
    ibv_destroy_cq(qp->recv_cq);
  }

  free(qp);
}

struct ibv_qp* rdmaxcel_qp_get_ibv_qp(rdmaxcel_qp_t* qp) {
  return qp ? qp->ibv_qp : NULL;
}

// Atomic fetch_add operations
uint64_t rdmaxcel_qp_fetch_add_send_wqe_idx(rdmaxcel_qp_t* qp) {
  return qp ? atomic_fetch_add(&qp->send_wqe_idx, 1) : 0;
}

uint64_t rdmaxcel_qp_fetch_add_send_db_idx(rdmaxcel_qp_t* qp) {
  return qp ? atomic_fetch_add(&qp->send_db_idx, 1) : 0;
}

uint64_t rdmaxcel_qp_fetch_add_send_cq_idx(rdmaxcel_qp_t* qp) {
  return qp ? atomic_fetch_add(&qp->send_cq_idx, 1) : 0;
}

uint64_t rdmaxcel_qp_fetch_add_recv_wqe_idx(rdmaxcel_qp_t* qp) {
  return qp ? atomic_fetch_add(&qp->recv_wqe_idx, 1) : 0;
}

uint64_t rdmaxcel_qp_fetch_add_recv_db_idx(rdmaxcel_qp_t* qp) {
  return qp ? atomic_fetch_add(&qp->recv_db_idx, 1) : 0;
}

uint64_t rdmaxcel_qp_fetch_add_recv_cq_idx(rdmaxcel_qp_t* qp) {
  return qp ? atomic_fetch_add(&qp->recv_cq_idx, 1) : 0;
}

// Atomic load operations
uint64_t rdmaxcel_qp_load_send_wqe_idx(rdmaxcel_qp_t* qp) {
  return qp ? atomic_load(&qp->send_wqe_idx) : 0;
}

uint64_t rdmaxcel_qp_load_send_db_idx(rdmaxcel_qp_t* qp) {
  return qp ? atomic_load(&qp->send_db_idx) : 0;
}

uint64_t rdmaxcel_qp_load_send_cq_idx(rdmaxcel_qp_t* qp) {
  return qp ? atomic_load(&qp->send_cq_idx) : 0;
}

uint64_t rdmaxcel_qp_load_recv_wqe_idx(rdmaxcel_qp_t* qp) {
  return qp ? atomic_load(&qp->recv_wqe_idx) : 0;
}

uint64_t rdmaxcel_qp_load_recv_cq_idx(rdmaxcel_qp_t* qp) {
  return qp ? atomic_load(&qp->recv_cq_idx) : 0;
}

uint64_t rdmaxcel_qp_load_rts_timestamp(rdmaxcel_qp_t* qp) {
  return qp ? atomic_load(&qp->rts_timestamp) : UINT64_MAX;
}

// Atomic store operations
void rdmaxcel_qp_store_send_db_idx(rdmaxcel_qp_t* qp, uint64_t value) {
  if (qp) {
    atomic_store(&qp->send_db_idx, value);
  }
}

void rdmaxcel_qp_store_rts_timestamp(rdmaxcel_qp_t* qp, uint64_t value) {
  if (qp) {
    atomic_store(&qp->rts_timestamp, value);
  }
}

// Get completion caches
completion_cache_t* rdmaxcel_qp_get_send_cache(rdmaxcel_qp_t* qp) {
  return qp ? qp->send_completion_cache : NULL;
}

completion_cache_t* rdmaxcel_qp_get_recv_cache(rdmaxcel_qp_t* qp) {
  return qp ? qp->recv_completion_cache : NULL;
}

// ============================================================================
// Completion Cache Implementation
// ============================================================================

void completion_cache_init(completion_cache_t* cache) {
  if (!cache) {
    return;
  }
  cache->head = -1;
  cache->tail = -1;
  cache->count = 0;

  // Initialize free list
  cache->free_head = 0;
  for (int i = 0; i < MAX_CACHED_COMPLETIONS - 1; i++) {
    cache->nodes[i].next = i + 1;
  }
  cache->nodes[MAX_CACHED_COMPLETIONS - 1].next = -1;

  pthread_mutex_init(&cache->lock, NULL);
}

void completion_cache_destroy(completion_cache_t* cache) {
  if (!cache) {
    return;
  }

  // Warn if cache still has entries when being destroyed
  if (cache->count > 0) {
    fprintf(
        stderr,
        "WARNING: Destroying completion cache with %zu unretrieved entries! "
        "Possible missing poll operations or leaked work requests.\n",
        cache->count);
    int curr = cache->head;
    fprintf(stderr, "  Cached wr_ids:");
    while (curr != -1 && cache->count > 0) {
      fprintf(stderr, " %lu", cache->nodes[curr].wc.wr_id);
      curr = cache->nodes[curr].next;
    }
    fprintf(stderr, "\n");
  }

  pthread_mutex_destroy(&cache->lock);
  cache->count = 0;
}

int completion_cache_add(completion_cache_t* cache, struct ibv_wc* wc) {
  if (!cache || !wc) {
    return 0;
  }

  pthread_mutex_lock(&cache->lock);

  if (cache->free_head == -1) {
    pthread_mutex_unlock(&cache->lock);
    fprintf(
        stderr,
        "WARNING: Completion cache full (%zu entries)! Dropping completion "
        "for wr_id=%lu, qp=%u\n",
        cache->count,
        wc->wr_id,
        wc->qp_num);
    return 0;
  }

  // Pop from free list
  int idx = cache->free_head;
  cache->free_head = cache->nodes[idx].next;

  // Store completion
  cache->nodes[idx].wc = *wc;
  cache->nodes[idx].next = -1;

  // Append to tail of used list
  if (cache->head == -1) {
    cache->head = idx;
    cache->tail = idx;
  } else {
    cache->nodes[cache->tail].next = idx;
    cache->tail = idx;
  }

  cache->count++;
  pthread_mutex_unlock(&cache->lock);
  return 1;
}

int completion_cache_find(
    completion_cache_t* cache,
    uint64_t wr_id,
    uint32_t qp_num,
    struct ibv_wc* out_wc) {
  if (!cache || !out_wc) {
    return 0;
  }

  pthread_mutex_lock(&cache->lock);

  int prev = -1;
  int curr = cache->head;

  while (curr != -1) {
    if (cache->nodes[curr].wc.wr_id == wr_id &&
        cache->nodes[curr].wc.qp_num == qp_num) {
      // Found it! Copy out
      *out_wc = cache->nodes[curr].wc;

      // Remove from used list
      if (prev == -1) {
        // Removing head (O(1) for typical case!)
        cache->head = cache->nodes[curr].next;
        if (cache->head == -1) {
          cache->tail = -1;
        }
      } else {
        // Removing from middle/tail
        cache->nodes[prev].next = cache->nodes[curr].next;
        if (cache->nodes[curr].next == -1) {
          cache->tail = prev;
        }
      }

      // Add to free list
      cache->nodes[curr].next = cache->free_head;
      cache->free_head = curr;

      cache->count--;
      pthread_mutex_unlock(&cache->lock);
      return 1;
    }

    prev = curr;
    curr = cache->nodes[curr].next;
  }

  pthread_mutex_unlock(&cache->lock);
  return 0;
}

int poll_cq_with_cache(poll_context_t* ctx, struct ibv_wc* out_wc) {
  if (!ctx || !out_wc) {
    return RDMAXCEL_INVALID_PARAMS;
  }

  if (completion_cache_find(
          ctx->cache, ctx->expected_wr_id, ctx->expected_qp_num, out_wc)) {
    if (out_wc->status != IBV_WC_SUCCESS) {
      return RDMAXCEL_COMPLETION_FAILED;
    }
    return 1;
  }

  struct ibv_wc wc;
  int ret = ibv_poll_cq(ctx->cq, 1, &wc);

  if (ret < 0) {
    return RDMAXCEL_CQ_POLL_FAILED;
  }

  if (ret == 0) {
    return 0;
  }

  if (wc.status != IBV_WC_SUCCESS) {
    if (wc.wr_id == ctx->expected_wr_id && wc.qp_num == ctx->expected_qp_num) {
      *out_wc = wc;
      return RDMAXCEL_COMPLETION_FAILED;
    }
    completion_cache_add(ctx->cache, &wc);
    return 0;
  }

  if (wc.wr_id == ctx->expected_wr_id && wc.qp_num == ctx->expected_qp_num) {
    *out_wc = wc;
    return 1;
  }

  completion_cache_add(ctx->cache, &wc);
  return 0;
}

// ============================================================================
// End of Completion Cache Implementation
// ============================================================================

cudaError_t register_mmio_to_cuda(void* bf, size_t size) {
  cudaError_t result = cudaHostRegister(
      bf,
      size,
      cudaHostRegisterMapped | cudaHostRegisterPortable |
          cudaHostRegisterIoMemory);
  return result;
}

struct ibv_qp* create_qp(
    struct ibv_context* context,
    struct ibv_pd* pd,
    int cq_entries,
    int max_send_wr,
    int max_recv_wr,
    int max_send_sge,
    int max_recv_sge,
    rdma_qp_type_t qp_type) {
  // Create separate completion queues for send and receive operations
  struct ibv_cq* send_cq = ibv_create_cq(context, cq_entries, NULL, NULL, 0);
  if (!send_cq) {
    perror("failed to create send completion queue (CQ)");
    return NULL;
  }

  struct ibv_cq* recv_cq = ibv_create_cq(context, cq_entries, NULL, NULL, 0);
  if (!recv_cq) {
    perror("failed to create receive completion queue (CQ)");
    ibv_destroy_cq(send_cq);
    return NULL;
  }

  switch (qp_type) {
    case RDMA_QP_TYPE_MLX5DV: {
      // Initialize extended queue pair attributes
      struct ibv_qp_init_attr_ex qp_init_attr_ex = {
          .qp_context = NULL,
          .send_cq = send_cq,
          .recv_cq = recv_cq,
          .srq = NULL,
          .cap =
              {
                  .max_send_wr = max_send_wr,
                  .max_recv_wr = max_recv_wr,
                  .max_send_sge = max_send_sge,
                  .max_recv_sge = max_recv_sge,
                  .max_inline_data = 0,
              },
          .qp_type = IBV_QPT_RC,
          .sq_sig_all = 0,
          .pd = pd,
          .comp_mask = IBV_QP_INIT_ATTR_PD | IBV_QP_INIT_ATTR_SEND_OPS_FLAGS,
          .send_ops_flags = IBV_QP_EX_WITH_RDMA_WRITE |
              IBV_QP_EX_WITH_RDMA_READ | IBV_QP_EX_WITH_SEND,
          .create_flags = 0,
      };

      struct mlx5dv_qp_init_attr mlx5dv_attr = {};
      mlx5dv_attr.comp_mask |= MLX5DV_QP_INIT_ATTR_MASK_SEND_OPS_FLAGS;
      mlx5dv_attr.send_ops_flags =
          MLX5DV_QP_EX_WITH_MKEY_CONFIGURE | MLX5DV_QP_EX_WITH_MR_LIST;

      // Create extended queue pair
      struct ibv_qp* qp =
          mlx5dv_create_qp(context, &qp_init_attr_ex, &mlx5dv_attr);
      if (!qp) {
        perror("failed to create extended queue pair (QP)");
        ibv_destroy_cq(send_cq);
        ibv_destroy_cq(recv_cq);
        return NULL;
      }

      return qp;
    }

    case RDMA_QP_TYPE_STANDARD: {
      // Initialize queue pair attributes
      struct ibv_qp_init_attr qp_init_attr = {
          .qp_context = NULL,
          .send_cq = send_cq,
          .recv_cq = recv_cq,
          .srq = NULL,
          .cap =
              {
                  .max_send_wr = max_send_wr,
                  .max_recv_wr = max_recv_wr,
                  .max_send_sge = max_send_sge,
                  .max_recv_sge = max_recv_sge,
                  .max_inline_data = 0,
              },
          .qp_type = IBV_QPT_RC,
          .sq_sig_all = 0,
      };

      // Create queue pair
      struct ibv_qp* qp = ibv_create_qp(pd, &qp_init_attr);
      if (!qp) {
        perror("failed to create queue pair (QP)");
        ibv_destroy_cq(send_cq);
        ibv_destroy_cq(recv_cq);
        return NULL;
      }

      return qp;
    }

    case RDMA_QP_TYPE_EFA: {
      struct ibv_qp* qp = create_efa_qp(
          context,
          pd,
          send_cq,
          recv_cq,
          max_send_wr,
          max_recv_wr,
          max_send_sge,
          max_recv_sge);
      if (!qp) {
        ibv_destroy_cq(send_cq);
        ibv_destroy_cq(recv_cq);
        return NULL;
      }

      return qp;
    }

    default: {
      perror("Invalid QP type");
      return NULL;
    }
  }
}

struct mlx5dv_qp* create_mlx5dv_qp(struct ibv_qp* qp) {
  struct mlx5dv_qp* dv_qp = malloc(sizeof(struct mlx5dv_qp));
  struct mlx5dv_obj dv_obj;
  memset(&dv_obj, 0, sizeof(dv_obj));
  memset(dv_qp, 0, sizeof(*dv_qp));

  dv_obj.qp.in = qp;
  dv_obj.qp.out = dv_qp;
  int ret = mlx5dv_init_obj(&dv_obj, MLX5DV_OBJ_QP);
  if (ret != 0) {
    perror("failed to init mlx5dv_qp");
    free(dv_qp);
    return NULL;
  }

  return dv_qp;
}

struct mlx5dv_cq* create_mlx5dv_cq(struct ibv_qp* qp) {
  // We'll use the receive CQ for now, but in the future this will be updated
  // to handle both send and receive CQs separately
  struct mlx5dv_cq* dv_cq = malloc(sizeof(struct mlx5dv_cq));
  struct mlx5dv_obj dv_obj;
  memset(&dv_obj, 0, sizeof(dv_obj));
  memset(dv_cq, 0, sizeof(*dv_cq));

  dv_obj.cq.in = qp->recv_cq;
  dv_obj.cq.out = dv_cq;
  int ret = mlx5dv_init_obj(&dv_obj, MLX5DV_OBJ_CQ);
  if (ret != 0) {
    perror("failed to init mlx5dv_cq");
    free(dv_cq);
    return NULL;
  }
  return dv_cq;
}

struct mlx5dv_cq* create_mlx5dv_send_cq(struct ibv_qp* qp) {
  struct mlx5dv_cq* dv_cq = malloc(sizeof(struct mlx5dv_cq));
  struct mlx5dv_obj dv_obj;
  memset(&dv_obj, 0, sizeof(dv_obj));
  memset(dv_cq, 0, sizeof(*dv_cq));

  dv_obj.cq.in = qp->send_cq;
  dv_obj.cq.out = dv_cq;
  int ret = mlx5dv_init_obj(&dv_obj, MLX5DV_OBJ_CQ);
  if (ret != 0) {
    perror("failed to init mlx5dv_send_cq");
    free(dv_cq);
    return NULL;
  }
  return dv_cq;
}

struct mlx5dv_cq* create_mlx5dv_recv_cq(struct ibv_qp* qp) {
  struct mlx5dv_cq* dv_cq = malloc(sizeof(struct mlx5dv_cq));
  struct mlx5dv_obj dv_obj;
  memset(&dv_obj, 0, sizeof(dv_obj));
  memset(dv_cq, 0, sizeof(*dv_cq));

  dv_obj.cq.in = qp->recv_cq;
  dv_obj.cq.out = dv_cq;
  int ret = mlx5dv_init_obj(&dv_obj, MLX5DV_OBJ_CQ);
  if (ret != 0) {
    perror("failed to init mlx5dv_recv_cq");
    free(dv_cq);
    return NULL;
  }
  return dv_cq;
}

cudaError_t register_cuda_memory(
    struct mlx5dv_qp* dv_qp,
    struct mlx5dv_cq* dv_recv_cq,
    struct mlx5dv_cq* dv_send_cq) {
  cudaError_t ret;

  ret = cudaHostRegister(
      dv_qp->sq.buf,
      dv_qp->sq.stride * dv_qp->sq.wqe_cnt,
      cudaHostRegisterMapped | cudaHostRegisterPortable);
  if (ret != cudaSuccess) {
    return ret;
  }

  ret = cudaHostRegister(
      dv_qp->bf.reg,
      dv_qp->bf.size,
      cudaHostRegisterMapped | cudaHostRegisterPortable |
          cudaHostRegisterIoMemory);
  if (ret != cudaSuccess) {
    return ret;
  }

  ret = cudaHostRegister(
      dv_qp->dbrec, 8, cudaHostRegisterMapped | cudaHostRegisterPortable);
  if (ret != cudaSuccess) {
    return ret;
  }

  // Register receive completion queue
  ret = cudaHostRegister(
      dv_recv_cq->buf,
      dv_recv_cq->cqe_size * dv_recv_cq->cqe_cnt,
      cudaHostRegisterMapped | cudaHostRegisterPortable);
  if (ret != cudaSuccess) {
    return ret;
  }

  ret = cudaHostRegister(
      dv_recv_cq->dbrec, 4, cudaHostRegisterMapped | cudaHostRegisterPortable);
  if (ret != cudaSuccess) {
    return ret;
  }

  // Register send completion queue
  ret = cudaHostRegister(
      dv_send_cq->buf,
      dv_send_cq->cqe_size * dv_send_cq->cqe_cnt,
      cudaHostRegisterMapped | cudaHostRegisterPortable);
  if (ret != cudaSuccess) {
    return ret;
  }

  ret = cudaHostRegister(
      dv_send_cq->dbrec, 4, cudaHostRegisterMapped | cudaHostRegisterPortable);
  if (ret != cudaSuccess) {
    return ret;
  }

  return cudaSuccess;
}

// ============================================================================
// EFA device detection
// ============================================================================

int rdmaxcel_is_efa_dev(struct ibv_context* ctx) {
  if (!ctx || !ctx->device) {
    return 0;
  }
  struct efadv_device_attr efa_attr = {};
  int ret = efadv_query_device(ctx, &efa_attr, sizeof(efa_attr));
  return (ret == 0) ? 1 : 0;
}

// ============================================================================
// EFA QP Creation
// ============================================================================

struct ibv_qp* create_efa_qp(
    struct ibv_context* context,
    struct ibv_pd* pd,
    struct ibv_cq* send_cq,
    struct ibv_cq* recv_cq,
    int max_send_wr,
    int max_recv_wr,
    int max_send_sge,
    int max_recv_sge) {
  // EFA SRD queue pair via efadv_create_qp_ex
  struct ibv_qp_init_attr_ex initAttributes = {
      .qp_context = NULL,
      .send_cq = send_cq,
      .recv_cq = recv_cq,
      .srq = NULL,
      .cap =
          {
              .max_send_wr = max_send_wr,
              .max_recv_wr = max_recv_wr,
              .max_send_sge = max_send_sge,
              .max_recv_sge = max_recv_sge,
              .max_inline_data = 0,
          },
      .qp_type = IBV_QPT_DRIVER,
      .sq_sig_all = 0,
      .pd = pd,
      .comp_mask = IBV_QP_INIT_ATTR_PD | IBV_QP_INIT_ATTR_SEND_OPS_FLAGS,
      .send_ops_flags = IBV_QP_EX_WITH_RDMA_WRITE | IBV_QP_EX_WITH_RDMA_READ |
          IBV_QP_EX_WITH_SEND,
      .create_flags = 0,
  };

  struct efadv_qp_init_attr efaAttr = {
      .driver_qp_type = EFADV_QP_DRIVER_TYPE_SRD,
  };

  struct ibv_qp* qp =
      efadv_create_qp_ex(context, &initAttributes, &efaAttr, sizeof(efaAttr));
  if (!qp) {
    perror("failed to create EFA SRD queue pair (QP)");
    return NULL;
  }

  return qp;
}

// ============================================================================
// EFA Extended-Verbs Operation Posting
// ============================================================================

int rdmaxcel_efa_post_write(
    rdmaxcel_qp_t* qp,
    struct ibv_ah* ah,
    uint32_t remote_qpn,
    uint32_t qkey,
    void* local_addr,
    uint32_t lkey,
    size_t length,
    void* remote_addr,
    uint32_t rkey,
    uint64_t wr_id) {
  if (!qp || !qp->qpex || !ah) {
    return RDMAXCEL_INVALID_PARAMS;
  }

  ibv_wr_start(qp->qpex);
  qp->qpex->wr_id = wr_id;
  qp->qpex->wr_flags = IBV_SEND_SIGNALED;

  ibv_wr_rdma_write(qp->qpex, rkey, (uintptr_t)remote_addr);
  ibv_wr_set_sge(qp->qpex, lkey, (uintptr_t)local_addr, (uint32_t)length);
  ibv_wr_set_ud_addr(qp->qpex, ah, remote_qpn, qkey);

  int ret = ibv_wr_complete(qp->qpex);
  if (ret != 0) {
    fprintf(
        stderr,
        "ERROR: EFA ibv_wr_complete (write) failed: %d (%s)\n",
        ret,
        strerror(ret));
    return RDMAXCEL_WR_COMPLETE_FAILED;
  }
  return RDMAXCEL_SUCCESS;
}

int rdmaxcel_efa_post_read(
    rdmaxcel_qp_t* qp,
    struct ibv_ah* ah,
    uint32_t remote_qpn,
    uint32_t qkey,
    void* local_addr,
    uint32_t lkey,
    size_t length,
    void* remote_addr,
    uint32_t rkey,
    uint64_t wr_id) {
  if (!qp || !qp->qpex || !ah) {
    return RDMAXCEL_INVALID_PARAMS;
  }

  ibv_wr_start(qp->qpex);
  qp->qpex->wr_id = wr_id;
  qp->qpex->wr_flags = IBV_SEND_SIGNALED;

  ibv_wr_rdma_read(qp->qpex, rkey, (uintptr_t)remote_addr);
  ibv_wr_set_sge(qp->qpex, lkey, (uintptr_t)local_addr, (uint32_t)length);
  ibv_wr_set_ud_addr(qp->qpex, ah, remote_qpn, qkey);

  int ret = ibv_wr_complete(qp->qpex);
  if (ret != 0) {
    fprintf(
        stderr,
        "ERROR: EFA ibv_wr_complete (read) failed: %d (%s)\n",
        ret,
        strerror(ret));
    return RDMAXCEL_WR_COMPLETE_FAILED;
  }
  return RDMAXCEL_SUCCESS;
}

// ============================================================================
// EFA Connect (INIT->RTR->RTS + Address Handle)
// ============================================================================

int rdmaxcel_efa_connect(
    rdmaxcel_qp_t* qp,
    uint8_t port_num,
    uint16_t pkey_index,
    uint32_t qkey,
    uint32_t psn,
    uint8_t gid_index,
    const uint8_t* remote_gid,
    uint32_t remote_qpn) {
  if (!qp || !qp->ibv_qp) {
    return RDMAXCEL_INVALID_PARAMS;
  }

  // Transition to INIT - EFA uses QKEY instead of ACCESS_FLAGS
  struct ibv_qp_attr qp_attr;
  memset(&qp_attr, 0, sizeof(qp_attr));
  qp_attr.qp_state = IBV_QPS_INIT;
  qp_attr.qkey = qkey;
  qp_attr.port_num = port_num;
  qp_attr.pkey_index = pkey_index;

  int mask = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_QKEY;
  int ret = ibv_modify_qp(qp->ibv_qp, &qp_attr, mask);
  if (ret != 0) {
    fprintf(
        stderr,
        "ERROR: EFA failed to transition QP to INIT: %d (%s)\n",
        ret,
        strerror(ret));
    return RDMAXCEL_QP_MODIFY_FAILED;
  }

  // Transition to RTR - minimal attributes for EFA
  memset(&qp_attr, 0, sizeof(qp_attr));
  qp_attr.qp_state = IBV_QPS_RTR;

  ret = ibv_modify_qp(qp->ibv_qp, &qp_attr, IBV_QP_STATE);
  if (ret != 0) {
    fprintf(
        stderr,
        "ERROR: EFA failed to transition QP to RTR: %d (%s)\n",
        ret,
        strerror(ret));
    return RDMAXCEL_QP_MODIFY_FAILED;
  }

  // Transition to RTS
  memset(&qp_attr, 0, sizeof(qp_attr));
  qp_attr.qp_state = IBV_QPS_RTS;
  qp_attr.sq_psn = psn;

  ret = ibv_modify_qp(qp->ibv_qp, &qp_attr, IBV_QP_STATE | IBV_QP_SQ_PSN);
  if (ret != 0) {
    fprintf(
        stderr,
        "ERROR: EFA failed to transition QP to RTS: %d (%s)\n",
        ret,
        strerror(ret));
    return RDMAXCEL_QP_MODIFY_FAILED;
  }

  // Create address handle for the remote peer
  struct ibv_ah_attr ah_attr;
  memset(&ah_attr, 0, sizeof(ah_attr));
  ah_attr.port_num = port_num;

  if (remote_gid) {
    ah_attr.is_global = 1;
    memcpy(&ah_attr.grh.dgid, remote_gid, 16);
    ah_attr.grh.sgid_index = gid_index;
  }

  struct ibv_ah* ah = ibv_create_ah(qp->ibv_qp->pd, &ah_attr);
  if (!ah) {
    fprintf(
        stderr,
        "ERROR: EFA failed to create address handle: %s\n",
        strerror(errno));
    return RDMAXCEL_AH_CREATE_FAILED;
  }

  // Store EFA routing info in the QP struct
  qp->efa_ah = ah;
  qp->efa_remote_qpn = remote_qpn;
  qp->efa_qkey = qkey;

  // Record RTS timestamp
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  uint64_t rts_nanos =
      (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
  rdmaxcel_qp_store_rts_timestamp(qp, rts_nanos);

  return RDMAXCEL_SUCCESS;
}

// ============================================================================
// EFA Unified Post Operation
// ============================================================================

int rdmaxcel_efa_post_op(
    rdmaxcel_qp_t* qp,
    struct ibv_ah* ah,
    uint32_t remote_qpn,
    uint32_t qkey,
    void* local_addr,
    uint32_t lkey,
    size_t length,
    void* remote_addr,
    uint32_t rkey,
    uint64_t wr_id,
    int op_type) {
  if (!ah) {
    return RDMAXCEL_INVALID_PARAMS;
  }
  if (op_type == 0) {
    return rdmaxcel_efa_post_write(
        qp,
        ah,
        remote_qpn,
        qkey,
        local_addr,
        lkey,
        length,
        remote_addr,
        rkey,
        wr_id);
  } else if (op_type == 1) {
    return rdmaxcel_efa_post_read(
        qp,
        ah,
        remote_qpn,
        qkey,
        local_addr,
        lkey,
        length,
        remote_addr,
        rkey,
        wr_id);
  }
  return RDMAXCEL_UNSUPPORTED_OP;
}

// ============================================================================
// EFA Post Operation with ibv_post_recv fallback
// ============================================================================
// Only called for EFA QPs; RC send/write/read is handled directly in Rust.
// op_type: 0 = write, 1 = read, 2 = recv, 3 = write_with_imm
int rdmaxcel_qp_post_op(
    rdmaxcel_qp_t* qp,
    void* local_addr,
    uint32_t lkey,
    size_t length,
    void* remote_addr,
    uint32_t rkey,
    uint64_t wr_id,
    int signaled,
    int op_type) {
  if (!qp || !qp->ibv_qp) {
    return RDMAXCEL_INVALID_PARAMS;
  }

  // Recv uses standard ibv_post_recv (works for all QP types)
  if (op_type == 2) {
    struct ibv_sge sge = {
        .addr = (uintptr_t)local_addr,
        .length = (uint32_t)length,
        .lkey = lkey,
    };
    struct ibv_recv_wr wr = {
        .wr_id = wr_id,
        .sg_list = &sge,
        .num_sge = 1,
        .next = NULL,
    };
    struct ibv_recv_wr* bad_wr = NULL;
    int ret = ibv_post_recv(qp->ibv_qp, &wr, &bad_wr);
    if (ret != 0) {
      fprintf(
          stderr, "ERROR: ibv_post_recv failed: %d (%s)\n", ret, strerror(ret));
      return RDMAXCEL_WR_COMPLETE_FAILED;
    }
    return RDMAXCEL_SUCCESS;
  }

  // EFA send/write/read: dispatch via extended verbs
  // Map op_type 3 (write_with_imm) to 0 (write) for EFA
  int efa_op = (op_type == 3) ? 0 : op_type;
  return rdmaxcel_efa_post_op(
      qp,
      qp->efa_ah,
      qp->efa_remote_qpn,
      qp->efa_qkey,
      local_addr,
      lkey,
      length,
      remote_addr,
      rkey,
      wr_id,
      efa_op);
}
