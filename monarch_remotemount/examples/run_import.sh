#!/bin/bash

set -ex

source /tmp/myenv/bin/activate
cd /tmp/myenv
python -c "import torch; print(torch.randn(123).cuda().mean())"
