#!/bin/bash

set -ex

source /scratch/cpuhrsch/venv/bin/activate
cd /scratch/cpuhrsch/venv
HF_HOME=/scratch/cpuhrsch/venv/hf_cache python hf_example.py
