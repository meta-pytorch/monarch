Note

Go to the end
to download the full example code

# Sharing GPUs Between Actors

Monarch's actor API does not assign GPUs to procs. A proc sees the devices that
its `CUDA_VISIBLE_DEVICES` exposes, and nothing in Monarch reserves a device for
one proc mesh. Placement is a choice of each proc's environment, made with
`bootstrap_command`, so two roles can share a GPU as easily as they can split a
host. This example covers:

- Pinning proc meshes to GPUs
- Two roles sharing GPUs, concurrently or taking turns
- Co-locating actors in one proc

The example runs one proc per GPU in `DEVICE_IDS`, which is `[0, 1]` by default.
List any GPUs on your host, even just one.

## Pinning procs to GPUs

`bootstrap_command` accepts a callable that builds each proc's launch command from
its coordinate. `on_gpus(device_ids)` maps coordinate `gpus=i` to GPU
`device_ids[i]`, so each proc sees exactly one device as `cuda`. The callable
runs where `spawn_procs` is called, not on the host, so it takes GPUs from the list
it is given rather than from its own environment.

Two meshes with different lists, such as `on_gpus([0, 1])` and `on_gpus([4, 5])`,
run on disjoint GPUs. Two meshes with the same list run on the same GPUs.

```
import os
from functools import partial

import torch
from monarch.actor import Actor, default_bootstrap_cmd, endpoint, this_host

DEVICE_IDS = [0, 1]
HIDDEN = 1024
BATCH = 64

if max(DEVICE_IDS) >= torch.cuda.device_count():
 raise SystemExit(f"this host has no GPU {max(DEVICE_IDS)}; edit DEVICE_IDS")

def on_gpus(device_ids):
 base = default_bootstrap_cmd()

 def command(point):
 return base.with_env({"CUDA_VISIBLE_DEVICES": str(device_ids[point["gpus"]])})

 return command
```

## Roles

A policy that generates and a trainer that updates its weights. Both report their
device and memory use; PyTorch's memory counters are per process.

```
class GpuActor(Actor):
 @endpoint
 def memory(self):
 return {
 "gpu": os.environ["CUDA_VISIBLE_DEVICES"],
 "allocated": torch.cuda.memory_allocated(),
 "reserved": torch.cuda.memory_reserved(),
 }

class Policy(GpuActor):
 def __init__(self):
 self.model = torch.nn.Linear(HIDDEN, HIDDEN, device="cuda")

 @endpoint
 def generate(self):
 with torch.no_grad():
 return self.model(torch.randn(BATCH, HIDDEN, device="cuda")).norm().item()

class Trainer(GpuActor):
 def __init__(self):
 self.model = torch.nn.Linear(HIDDEN, HIDDEN, device="cuda")
 self.optim = torch.optim.SGD(self.model.parameters(), lr=1e-3, foreach=True)

 @endpoint
 def train_step(self):
 loss = self.model(torch.randn(BATCH, HIDDEN, device="cuda")).pow(2).mean()
 loss.backward()
 self.optim.step()
 self.optim.zero_grad()
 return loss.item()
```

## Concurrent sharing

Both meshes get the same list, so policy proc `i` and trainer proc `i` share
GPU `DEVICE_IDS[i]`. Each proc has its own CUDA context, and the driver
time-slices between them. This requires the GPUs' compute mode to be `Default`
rather than `Exclusive_Process`.

`bootstrap` runs in each proc before any actor is spawned. Here it caps each
process's PyTorch allocator at a fraction of GPU memory, so a role that outgrows its
share fails with an out-of-memory error in its own process instead of starving the
other.

```
policy_procs = this_host().spawn_procs(
 per_host={"gpus": len(DEVICE_IDS)},
 bootstrap_command=on_gpus(DEVICE_IDS),
 bootstrap=partial(torch.cuda.set_per_process_memory_fraction, 0.3),
)
trainer_procs = this_host().spawn_procs(
 per_host={"gpus": len(DEVICE_IDS)},
 bootstrap_command=on_gpus(DEVICE_IDS),
 bootstrap=partial(torch.cuda.set_per_process_memory_fraction, 0.6),
)
policy = policy_procs.spawn("policy", Policy)
trainer = trainer_procs.spawn("trainer", Trainer)

generated = policy.generate.call()
losses = trainer.train_step.call()
print("generated:", generated.get().item(gpus=0), "loss:", losses.get().item(gpus=0))
```

## Taking turns

When generation and training alternate within a step, the controller orders them by
waiting for each call before issuing the next, so only one role computes at a time.
Both models stay resident. Taking turns avoids contention for compute, not memory:
the caps above still apply, and each process's PyTorch allocator keeps its cached
blocks between turns.

```
for step in range(3):
 policy.generate.call().get()
 loss = trainer.train_step.call().get().item(gpus=0)
 print(f"step {step}: loss={loss:.4f}")
```

## Co-locating actors in one proc

A proc mesh can host several actors. A frozen reference model spawned on the
trainer's procs shares the trainer's process, CUDA context, and memory cap, so it
adds no context of its own. Actors in one proc share its fate: if the proc dies,
both go with it.

Because PyTorch's memory counters are per process, the reference's allocations
appear in the trainer's report rather than in one of their own.

```
def report(label, role):
 for usage in role.memory.call().get().values():
 print(label, usage)

report("policy", policy)
report("trainer", trainer)

reference = trainer_procs.spawn("reference", Policy)
reference.generate.call().get()
report("trainer + reference", trainer)

policy_procs.stop().get()
trainer_procs.stop().get()
print("Example completed successfully!")
```

**Total running time of the script:** (0 minutes 0.000 seconds)

[`Download Python source code: gpu_sharing.py`](../../_downloads/ccb22b19282b0e3def66b5ccf46894e3/gpu_sharing.py)

[`Download Jupyter notebook: gpu_sharing.ipynb`](../../_downloads/9da5e1f98c97b0ea20d285e3c2bc597c/gpu_sharing.ipynb)

[Gallery generated by Sphinx-Gallery](https://sphinx-gallery.github.io)