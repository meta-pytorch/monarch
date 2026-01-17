#!/bin/bash

set -ex

python -m venv /tmp/myvenv
source /tmp/myvenv/bin/activate
pip install --no-index --find-links /tmp/flat_wheels torch transformers sentencepiece
