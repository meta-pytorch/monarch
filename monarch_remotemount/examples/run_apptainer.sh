#!/bin/bash

rm -f /tmp/overlay.img
apptainer overlay create --size 2048 /tmp/overlay.img

rm -rf /tmp/apptainer-work
mkdir /tmp/apptainer-work

apptainer exec --containall --network none --workdir /tmp/apptainer-work --overlay /tmp/overlay.img /tmp/myapp/img.sif uv pip install requests numpy pandas
apptainer exec --containall --network none --workdir /tmp/apptainer-work --overlay /tmp/overlay.img /tmp/myapp/img.sif python -c "import pandas; print(pandas.__version__)"
