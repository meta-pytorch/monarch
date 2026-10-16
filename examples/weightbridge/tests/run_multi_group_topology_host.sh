#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Run one rank of multi_group_topology_integration.py inside an EFA-capable image.
# Launch 24 ranks over three 8-GPU nodes; Slurm block rank order makes node 0 the eight trainers and nodes
# 1-2 the sixteen rollout workers:
#
#   WBRIDGE_TEST_SIF=/path/to/runtime.sif \
#   srun --jobid="$JOBID" --overlap --mpi=none -N3 --ntasks-per-node=1 --gres=gpu:8 \
#     --export=ALL,MASTER_ADDR="$HEAD_IP",WBRIDGE_TEST_TORCHRUN=1,WBRIDGE_TEST_SIF \
#     bash tests/run_multi_group_topology_host.sh
#
# Site-specific inputs are environment variables: WBRIDGE_TEST_SIF is required;
# WBRIDGE_NETWORK_INTERFACE, APPTAINER_CACHEDIR, WBRIDGE_EFA_LIBRARY_PATH, and
# WBRIDGE_TEST_TMP_ROOT are optional. If WBRIDGE_TEST_PATCH_LIBFABRIC=1, also
# provide the in-container WBRIDGE_MOONCAKE_LIB_DIR, WBRIDGE_LIBFABRIC_SO, and
# optional WBRIDGE_LIBEFA_SO paths.
set -euo pipefail

: "${MASTER_ADDR:?MASTER_ADDR must identify the rank-0 node}"
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
LIB_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd)
: "${WBRIDGE_TEST_SIF:?set WBRIDGE_TEST_SIF to an EFA-capable Apptainer image}"
if [ ! -f "$WBRIDGE_TEST_SIF" ]; then
  echo "WBRIDGE_TEST_SIF does not exist: $WBRIDGE_TEST_SIF" >&2
  exit 2
fi

CONTAINER_LIB=${WBRIDGE_TEST_CONTAINER_LIB:-/wbridge-lib}
case "$CONTAINER_LIB" in
  /*) ;;
  *) echo "WBRIDGE_TEST_CONTAINER_LIB must be absolute inside the container" >&2; exit 2 ;;
esac

export APPTAINER_CACHEDIR=${APPTAINER_CACHEDIR:-${XDG_CACHE_HOME:-${TMPDIR:-/tmp}}/apptainer}
mkdir -p "$APPTAINER_CACHEDIR"
export APPTAINERENV_WBRIDGE_TEST_NNODES=${SLURM_NNODES:-${SLURM_JOB_NUM_NODES:-1}}
export APPTAINERENV_WBRIDGE_TEST_NODE_RANK=${SLURM_NODEID:-0}
export APPTAINERENV_MASTER_ADDR=$MASTER_ADDR
export APPTAINERENV_WBRIDGE_TEST_CONTAINER_LIB=$CONTAINER_LIB
export APPTAINERENV_WBRIDGE_NETWORK_INTERFACE=${WBRIDGE_NETWORK_INTERFACE:-}
export APPTAINERENV_WBRIDGE_EFA_LIBRARY_PATH=${WBRIDGE_EFA_LIBRARY_PATH:-}
export APPTAINERENV_WBRIDGE_TEST_TMP_ROOT=${WBRIDGE_TEST_TMP_ROOT:-}
export APPTAINERENV_WBRIDGE_TEST_PATCH_LIBFABRIC=${WBRIDGE_TEST_PATCH_LIBFABRIC:-0}
export APPTAINERENV_WBRIDGE_MOONCAKE_LIB_DIR=${WBRIDGE_MOONCAKE_LIB_DIR:-}
export APPTAINERENV_WBRIDGE_LIBFABRIC_SO=${WBRIDGE_LIBFABRIC_SO:-}
export APPTAINERENV_WBRIDGE_LIBEFA_SO=${WBRIDGE_LIBEFA_SO:-}
export APPTAINERENV_MC_PATH_ROUNDROBIN=${MC_PATH_ROUNDROBIN:-1}
export APPTAINERENV_MC_NUM_QP_PER_EP=${MC_NUM_QP_PER_EP:-4}
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY no_proxy NO_PROXY

exec apptainer exec --nv --writable-tmpfs --no-mount home \
  --bind "$LIB_ROOT:$CONTAINER_LIB" \
  "$WBRIDGE_TEST_SIF" bash -lc '
set -euo pipefail
if [ -n "${WBRIDGE_EFA_LIBRARY_PATH:-}" ]; then
  export LD_LIBRARY_PATH="${WBRIDGE_EFA_LIBRARY_PATH}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
fi
export FI_PROVIDER=efa FI_EFA_USE_DEVICE_RDMA=1 FI_EFA_ENABLE_SHM_TRANSFER=0
export MC_PATH_ROUNDROBIN=${MC_PATH_ROUNDROBIN:-1}
export MC_NUM_QP_PER_EP=${MC_NUM_QP_PER_EP:-4}
IFACE=${WBRIDGE_NETWORK_INTERFACE:-}
if [ -z "$IFACE" ]; then
  IFACE=$(ip route show default 2>/dev/null | awk "{print \$5; exit}" || true)
fi
if [ -z "$IFACE" ]; then
  echo "could not determine a network interface; set WBRIDGE_NETWORK_INTERFACE" >&2
  exit 2
fi
export GLOO_SOCKET_IFNAME=$IFACE
TEST_TMP_ROOT=${WBRIDGE_TEST_TMP_ROOT:-${TMPDIR:-/tmp}/wbridge-multigroup-${SLURM_JOB_ID:-0}-${SLURM_PROCID:-0}}
mkdir -p "$TEST_TMP_ROOT/tmp" "$TEST_TMP_ROOT/cache" "$TEST_TMP_ROOT/triton"
export TMPDIR=$TEST_TMP_ROOT/tmp
export XDG_CACHE_HOME=$TEST_TMP_ROOT/cache
export TRITON_CACHE_DIR=$TEST_TMP_ROOT/triton
export PYTHONPATH="${WBRIDGE_TEST_CONTAINER_LIB:?}${PYTHONPATH:+:$PYTHONPATH}"
export PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

# Avoid loading Mooncake auditwheel libfabric beside the system EFA libfabric.
if [ "${WBRIDGE_TEST_PATCH_LIBFABRIC:-0}" = "1" ]; then
  : "${WBRIDGE_MOONCAKE_LIB_DIR:?set to mooncake_transfer_engine.libs inside the container}"
  : "${WBRIDGE_LIBFABRIC_SO:?set to the system libfabric.so.1 inside the container}"
  if [ ! -d "$WBRIDGE_MOONCAKE_LIB_DIR" ] || [ ! -f "$WBRIDGE_LIBFABRIC_SO" ]; then
    echo "invalid Mooncake or libfabric patch path" >&2
    exit 2
  fi
  for f in "$WBRIDGE_MOONCAKE_LIB_DIR"/libfabric-*.so.*; do
    [ -e "$f" ] && ln -sf "$WBRIDGE_LIBFABRIC_SO" "$f"
  done
  if [ -n "${WBRIDGE_LIBEFA_SO:-}" ]; then
    if [ ! -f "$WBRIDGE_LIBEFA_SO" ]; then
      echo "WBRIDGE_LIBEFA_SO does not exist: $WBRIDGE_LIBEFA_SO" >&2
      exit 2
    fi
    for f in "$WBRIDGE_MOONCAKE_LIB_DIR"/libefa-*.so.*; do
      [ -e "$f" ] && ln -sf "$WBRIDGE_LIBEFA_SO" "$f"
    done
  fi
fi

export WBRIDGE_ROUND_CAP_BYTES=${WBRIDGE_ROUND_CAP_BYTES:-40}
export WBRIDGE_DEDUP_PAIR_BYTES=${WBRIDGE_DEDUP_PAIR_BYTES:-0}
export WBRIDGE_RECV_PIPELINE=${WBRIDGE_RECV_PIPELINE:-1}
export WBRIDGE_RECV_3STAGE=${WBRIDGE_RECV_3STAGE:-1}
export WBRIDGE_TOPO_EXCHANGE=${WBRIDGE_TOPO_EXCHANGE:-1}
export WBRIDGE_TEST_ITERS=${WBRIDGE_TEST_ITERS:-2}
if [ "${WBRIDGE_TEST_TORCHRUN:-0}" = "1" ]; then
  exec torchrun \
    --nnodes="${WBRIDGE_TEST_NNODES:?}" \
    --nproc-per-node=8 \
    --node-rank="${WBRIDGE_TEST_NODE_RANK:?}" \
    --master-addr="${MASTER_ADDR:?}" \
    --master-port="${WBRIDGE_TORCHRUN_PORT:-63300}" \
    "$WBRIDGE_TEST_CONTAINER_LIB/tests/multi_group_topology_integration.py"
fi
exec python3 "$WBRIDGE_TEST_CONTAINER_LIB/tests/multi_group_topology_integration.py"
'
