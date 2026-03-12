"""
Minimal repro for the "timeout waiting for message from proc mesh agent" error.

Run with:
    source ~/oss.sh && python repro_timeout.py
"""

from monarch._src.job.process import ProcessJob
from monarch.mesh_controller import spawn_tensor_engine

job = ProcessJob({"hosts": 1})
proc_mesh = job.state(cached_path=None).hosts.spawn_procs(per_host={"gpus": 1})

for i in range(1000):
    dm = spawn_tensor_engine(proc_mesh)
    dm.exit()
    print(f"iter {i}", flush=True)
