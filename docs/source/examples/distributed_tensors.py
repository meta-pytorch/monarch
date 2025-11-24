#!/usr/bin/env -S grimaldi --kernel monarch_demo_local
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# FILE_UID: f083db77-6303-4fbf-a9ec-8be986f0926a
# NOTEBOOK_NUMBER: N6185324 (8543474375772761)

"""
Distributed Tensors in Monarch
------------------------------
"""

import monarch
import torch

import torch.nn as nn
from monarch.actor import this_host

torch.set_default_device("cuda")


# %%
# Meshes
# ------
# All computation is done on a 'mesh' of devices.
# Here we create a mesh composed of the machine running the notebook:

mesh = this_host().spawn_procs({"gpu": 8})
print(mesh.to_table())

# %%
# Without a mesh active, torch runs locally.

torch.rand(3, 4)

# %%
# Once active, torch runs on every device in the mesh.

with mesh.activate():
    t = torch.rand(3, 4, device="cuda")

t

# %%
# Inspect moves rank0's copy of t to the notebook for debugging.

monarch.inspect(t)

monarch.show(t)

# %%
# Providing coordinates lets us inspect other ranks copies.

monarch.show(t, gpu=1)

# %%
# Tensor Commands
# ---------------
#
# Any command done on the controller, such as multiplying these tensors,
# performs that action to all of the tensors in the collection.

with mesh.activate():
    obj = t @ t.T
    monarch.show(obj)

# %%
# If a command fails, the workers stay alive and can execute future
# commands:

try:
    with mesh.activate():
        # too big
        big_w = torch.rand(4, 1024 * 1024 * 1024 * 1024 * 8, device="cuda")
        v = t @ big_w
        monarch.show(v)

except Exception:
    import traceback

    traceback.print_exc()

del big_w
print("RECOVERED!")

# %%
# Since monarch recovers from errors, you can search for
# what works:

N = 1
while True:
    try:
        with mesh.activate():
            batch = torch.rand(N, 1024 * 1024 * 1024, device="cuda")
        monarch.inspect(batch.sum())
        N = 2 * N
        print(f"at least 2**{N} elements work")
    except Exception:
        print(f"max is 2**{N} elements")
        break

# %%
# Collectives
# -----------
# Each machine has its own copy of the tensor, similar to torch.distributed.
#
# To compute across tensors in the mesh, we use special communication operators, analogous to collectives.

with mesh.activate():
    a = torch.rand(3, 4, device="cuda")
    r = a.reduce("gpu", "sum")

monarch.show(a, gpu=0)  # try
monarch.show(a, gpu=1)  # try

monarch.show(r, gpu=0)  # try
monarch.show(r, gpu=1)  # try

# %%
# Remote GPUs
# ---------
#
# We can also connect to remote GPUs reserved from some scheduler

# NYI: schedule public API based on config, just fake it locally

remote_mesh = this_host().spawn_procs({"host": 4, "gpu": 4})

print(remote_mesh.to_table())
with remote_mesh.activate():
    eg = torch.rand(3, 4, device="cuda")
    rgpu = eg.reduce("gpu", "sum")
    rhost = eg.reduce("host", "sum")

# %%
# Device Mesh Dimensions
# ----------------------
#
# Meshes can be renamed and reshaped to fit the parallelism desired.

mesh_2d_parallel = remote_mesh.rename(host="dp", gpu="tp")
print(mesh_2d_parallel.to_table())

mesh_3d_parallel = remote_mesh.split(host=("dp", "pp"), gpu=("tp",), pp=2)
print(mesh_3d_parallel.to_table())

# %%
# Pipelining
# ----------
#
# Pipelining is accomplished by slicing the mesh, and copying tensors from
# one mesh to another.

pipeline_mesh = remote_mesh.rename(host="pp")
meshes = [pipeline_mesh.slice(pp=i) for i in range(pipeline_mesh.size("pp"))]

print(meshes[0].to_table())

# %%
# Initialize a model across multiple meshes

layers_per_stage = 2
stages = []
for stage_mesh in meshes:
    with stage_mesh.activate():
        layers = []
        for _ in range(layers_per_stage):
            layers.extend([nn.Linear(4, 4), nn.ReLU()])
        stages.append(nn.Sequential(*layers))


def forward_pipeline(x):
    with torch.no_grad():
        for stage_mesh, stage in zip(meshes, stages):
            x = x.to_mesh(stage_mesh)
            with stage_mesh.activate():
                x = stage(x)
        return x


with meshes[0].activate():
    input = torch.rand(3, 4, device="cuda")

output = forward_pipeline(input)
monarch.show(output)
print(output.mesh.to_table())

# %%
# DDP Example
# -----------
#
# The next sections will use an example of writing DDP to illustrate a
# typical way to develop code in monarch.
#
# Let's interleave the backward pass with the gradient reductions and
# parameter updates.
#
# We use monarch.grad_generator to incrementally run the backward pass.
# It returns an iterator that computes the grad parameters one at a time.


def train(model, input, target):
    loss = model(input, target)
    rparameters = list(reversed(list(model.parameters())))
    grads = monarch.grad_generator(loss, rparameters)
    with torch.no_grad():
        it = iter(zip(rparameters, grads))
        todo = next(it, None)
        while todo is not None:
            param, grad = todo
            grad.reduce_("dp", "sum")
            todo = next(it, None)
            param += 0.01 * grad


# %%
# Simulation of DDP
# -----------------
#
# We can use a simulator to check for expected behavior of code before running it
# for real.
#
# It is another kind of mesh, which simulates rather than computes results for real.


class Net(nn.Module):
    def __init__(self):
        super().__init__()
        layers = []
        for _ in range(8):
            layers.append(nn.Linear(4, 4))
            layers.append(nn.ReLU())
        self.layers = nn.Sequential(*layers)

    def forward(self, input, target):
        output = self.layers(input)
        return torch.nn.functional.cross_entropy(output, target)


def simulate():
    simulator = monarch.Simulator(hosts=1, gpus=4, trace_mode="stream_only")
    mesh = simulator.mesh.rename(gpu="dp")
    with mesh.activate():
        model = Net()

        train(model, torch.rand(3, 4), torch.full((3,), 1, dtype=torch.int64))

        simulator.display()


# Make sure pop-ups are enabled in your browser for internalfb.com
simulate()

# %%
# Overlapping Comms/Compute
# -------------------------
# Commands on different devices run in parallel,
# but by default commands on a single device run sequentially.
#
# We introduce parallelism on a device via stream objects.

main = monarch.get_active_stream()
comms = monarch.Stream("comms")

# %%
# <img src="https://lookaside.internalfb.com/intern/pixelcloudnew/asset/?id=468246752388474" width=500>

# %%
# To use a tensor from one stream on another we borrow it. The borrow API ensures determinstic memory usage,
# and eliminates the race conditions in the torch.cuda.stream API.
#
# <img src="https://lookaside.internalfb.com/intern/pixelcloudnew/asset/?id=556746733733298" width=500>

# %%
# The DDP example again, but using multiple streams.


def train(model, input, target):
    loss = model(input, target)
    rparameters = list(reversed(list(model.parameters())))
    grads = monarch.grad_generator(loss, rparameters)
    with torch.no_grad():
        # NEW: iter also produces the tensor borrowed
        # to the comm stream
        it = iter(
            (param, grad, *comms.borrow(grad, mutable=True))
            for param, grad in zip(rparameters, grads)
        )

        todo = next(it, None)
        while todo is not None:
            param, grad, comm_grad, borrow = todo
            # NEW: compute the reduce on the comm stream
            with comms.activate():
                comm_grad.reduce_("dp", "sum")
            borrow.drop()
            todo = next(it, None)
            param += 0.01 * grad


simulate()

# %%
# The simulation result showed the results did not overlap much
# due to wherethe borrow.drop was placed.
#
# <img src="https://lookaside.internalfb.com/intern/pixelcloudnew/asset/?id=1282606659410255" width=500>

# %%
# The goal is to get overlap like so:
#
# <img src="https://lookaside.internalfb.com/intern/pixelcloudnew/asset/?id=1110575440645591" width=500>
#
# We can achieve this by ending the borrow after the grad step but before
# we update the param.


def train(model, input, target):
    loss = model(input, target)
    rparameters = list(reversed(list(model.parameters())))
    grads = monarch.grad_generator(loss, rparameters)
    with torch.no_grad():
        it = iter(
            (param, grad, *comms.borrow(grad, mutable=True))
            for param, grad in zip(rparameters, grads)
        )

        todo = next(it, None)
        while todo is not None:
            param, grad, comm_grad, borrow = todo
            with comms.activate():
                comm_grad.reduce_("dp", "sum")
            todo = next(it, None)
            # NEW: delay the borrow as late as possible
            borrow.drop()
            param += 0.01 * grad


simulate()
