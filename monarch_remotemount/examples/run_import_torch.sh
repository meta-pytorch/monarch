#!/bin/bash

source /tmp/myenv/bin/activate
( time python -c "import torch" ) 2>&1
( time python -c "import torch" ) 2>&1
