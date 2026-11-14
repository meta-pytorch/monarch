# monarch.spmd

The `monarch.spmd` module provides primitives for running torchrun-style SPMD
(Single Program Multiple Data) distributed training scripts over Monarch actor meshes.

It bridges PyTorch distributed training with Monarch by automatically configuring
torch elastic environment variables (`RANK`, `LOCAL_RANK`, `WORLD_SIZE`,
`MASTER_ADDR`, `MASTER_PORT`, etc.) across the mesh.

## Environment Setup

monarch.spmd.setup_torch_elastic_env(*proc_mesh*, *master_addr=None*, *master_port=None*, *use_ipaddr=None*)[[source]](../_modules/monarch/_src/spmd.html#setup_torch_elastic_env)

Sets up environment variables for pytorch torchelastic.
It sets enviornment variables like RANK, LOCAL_RANK, WORLD_SIZE, etc.
If master_addr and master_port are None, it will automatically select a master node and port.
It selects the first proc in the proc_mesh to be the master node.

*async*monarch.spmd.setup_torch_elastic_env_async(*proc_mesh*, *master_addr=None*, *master_port=None*, *use_ipaddr=None*)[[source]](../_modules/monarch/_src/spmd.html#setup_torch_elastic_env_async)

Sets up environment variables for pytorch torchelastic.
It sets enviornment variables like RANK, LOCAL_RANK, WORLD_SIZE, etc.
If master_addr and master_port are None, it will automatically select a master node and port.
It selects the first proc in the proc_mesh to be the master node.

## SPMD Actor

*class*monarch.spmd.SPMDActor[[source]](../_modules/monarch/_src/spmd/actor.html#SPMDActor)

Bases: [`Actor`](monarch.actor.html#monarch.actor.Actor)

Actor that sets up PyTorch distibuted.run training environment variables and
executes SPMD training scripts.

This actor replicates torchrun's behavior by configuring environment variables (see [https://docs.pytorch.org/docs/stable/elastic/run.html#environment-variables](https://docs.pytorch.org/docs/stable/elastic/run.html#environment-variables))
for PyTorch distributed training including RANK, WORLD_SIZE, LOCAL_RANK,
LOCAL_WORLD_SIZE, GROUP_RANK, GROUP_WORLD_SIZE, ROLE_RANK, ROLE_WORLD_SIZE,
ROLE_NAME, MASTER_ADDR, and MASTER_PORT before launching the training script.
All rank and mesh information is automatically derived from current_rank().

Example:

```
from monarch.spmd import SPMDActor

# Spawn SPMDActor on the process mesh
spmd_actors = proc_mesh.spawn("_SPMDActor", SPMDActor)

# Get master address/port from first actor
first_values = dict.fromkeys(proc_mesh._labels, 0)
master_addr, master_port = spmd_actors.slice(**first_values).get_host_port.call_one(None).get()

# Execute training script across the mesh
spmd_actors.main.call(master_addr, master_port, ["-m", "train", "--lr", "0.001"]).get()
```

For custom client code that uses torch.distributed directly:

```
from monarch.actor import this_host
from monarch.spmd import setup_torch_elastic_env

# Create a process mesh with 2 GPUs per host
proc_mesh = this_host().spawn_procs(per_host={"gpus": 2})

# Set up torch elastic environment (spawns SPMDActor internally)
setup_torch_elastic_env(proc_mesh)

# ... rest of custom client code
```

get_host_port

setup_env

main