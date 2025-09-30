# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
RDMA support for Monarch.
==========================

This guide provides a brief overview of RDMA support in Monarch.
This is meant to be merged with the getting_started guide.

"""

import torch
from monarch.actor import Actor, endpoint, this_host
from monarch.rdma import RDMABuffer

# %%
# Point-to-Point RDMA
# ====================
# Monarch provides first-class RDMA support through the RDMABuffer API. This lets
# you separate data references from data movement - instead of sending large tensors
# through your messaging patterns, you pass lightweight buffer references anywhere
# in your system, then explicitly transfer the actual data only when and where you need it.
# This approach lets you design your service's coordination patterns based on your application
# logic rather than being constrained by when the framework forces data transfers.


class ParameterServer(Actor):
    def __init__(self):
        self.weights = torch.rand(1000, 1000)  # Large model weights
        self.weight_buffer = RDMABuffer(self.weights.view(torch.uint8).flatten())

    @endpoint
    def get_weights(self) -> RDMABuffer:
        return self.weight_buffer  # Small message: just a reference!


class Worker(Actor):
    def __init__(self):
        self.local_weights = torch.zeros(1000, 1000)

    @endpoint
    def sync_weights(self, server: ParameterServer):
        # Control plane: get lightweight reference
        weight_ref = server.get_weights.call_one().get()

        # Data plane: explicit bulk transfer when needed
        weight_ref.read_into(self.local_weights.view(torch.uint8).flatten()).get()


server_proc = this_host().spawn_procs(per_host={"gpus": 1})
worker_proc = this_host().spawn_procs(per_host={"gpus": 1})

server = server_proc.spawn("server", ParameterServer)
worker = worker_proc.spawn("worker", Worker)

worker.sync_weights.call_one(server).get()
