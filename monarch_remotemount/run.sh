#!/usr/bin/bash
set -ex

FUSE_LIBRARY_PATH=/home/cpuhrsch/download/temp_fuse/lib/x86_64-linux-gnu/libfuse.so.2 LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/cpuhrsch/miniconda3/envs/monarch20251201py310/lib/python3.10/site-packages/torch/lib:/home/cpuhrsch/miniconda3/envs/monarch20251201py310/lib/python3.10/site-packages/nvidia/nccl/lib:/home/cpuhrsch/miniconda3/envs/monarch20251201py310/lib python run.py "$@"
