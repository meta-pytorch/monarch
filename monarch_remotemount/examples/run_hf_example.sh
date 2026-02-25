#!/bin/bash

MYENV_PATH="/tmp/myenv"

source "${MYENV_PATH}/bin/activate"
unset PYTHONPATH
HF_HUB_DISABLE_PROGRESS_BARS=1 HF_HUB_OFFLINE=1 HF_HOME="${MYENV_PATH}/hf_cache" "${MYENV_PATH}/bin/python" "${MYENV_PATH}/hf_example.py"
