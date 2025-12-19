#!/bin/bash

find /scratch/cpuhrsch/venv -type f > /tmp/all_files
python -c "import random; random.seed(123); import sys; lines = sys.stdin.read().split('\n')[:-1]; random.shuffle(lines); print('\n'.join(lines))" < /tmp/all_files > /tmp/all_files_shuf

( time xargs -d '\n' cat < /tmp/all_files_shuf > /tmp/bigfile  ) 2> /tmp/total_time

cat /tmp/total_time
