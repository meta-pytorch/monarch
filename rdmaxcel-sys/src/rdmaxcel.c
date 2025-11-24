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
