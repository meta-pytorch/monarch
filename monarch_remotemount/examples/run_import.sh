#!/bin/bash

set -ex

source /scratch/cpuhrsch/venv_torch/bin/activate
cd /scratch/cpuhrsch/venv
python -c "import torch; print(torch.randn(123).cuda().mean())"
