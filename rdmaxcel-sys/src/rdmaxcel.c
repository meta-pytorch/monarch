/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "rdmaxcel.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
