#!/bin/bash

source /scratch/cpuhrsch/venv/bin/activate
( time python -c "import torch" ) 2>&1
( time python -c "import torch" ) 2>&1
