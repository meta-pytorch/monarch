#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# The two-node UDP check: IPv6 first, then IPv4 if needed, one step per family.
#
#   sbatch [-p PARTITION] [-A ACCOUNT] [-q QOS] [-w NODE1,NODE2] \
#       run_udp.sh /absolute/path/to/slurm
#
# The argument is the directory holding `check_udp.py` and `slurm_network.py`:
# sbatch runs a spooled copy of this script, so its own location says nothing.
# Requires a writable shared home mounted at the same path on both nodes.
# Both Python files must be readable on both nodes at the supplied path.
# Each run gets a fresh attempt directory under ~/chr-slurm-checks/<job id>/,
# which holds the workers' records and logs and is kept.

#SBATCH --job-name=chr-udp-check
#SBATCH --nodes=2
#SBATCH --ntasks=2
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --time=20:00

set -euo pipefail

if [ "$#" -ne 1 ] || [ "${1:0:1}" != / ]; then
  echo "usage: run_udp.sh /absolute/path/to/slurm" >&2
  exit 2
fi
tools=$1
for f in check_udp.py slurm_network.py; do
  if [ ! -r "$tools/$f" ]; then
    echo "missing $tools/$f" >&2
    exit 2
  fi
done
: "${SLURM_JOB_ID:?run_udp.sh runs under sbatch}"

umask 077
mkdir -p "$HOME/chr-slurm-checks/$SLURM_JOB_ID"
attempt=$(mktemp -d "$HOME/chr-slurm-checks/$SLURM_JOB_ID/attempt.XXXXXX")
echo "logs: $attempt"

exec python3 -u "$tools/check_udp.py" --launch "$tools" --rendezvous "$attempt"
