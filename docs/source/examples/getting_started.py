# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

"""
Getting Started
===============

This guide introduces the core concepts of Monarch, a framework for building
multi-machine training programs using actors. We'll cover:

- Defining actors with endpoint functions
- Spawning actors locally and across multiple hosts and processes
- Sending messages and organizing actors into meshes
- The supervision tree for fault tolerance
- Distributed tensors and RDMA capabilities
"""

# %%
# Defining an Actor
# -----------------
# At its core, Monarch uses actors as a way to create multi-machine training programs.
# Actors are Python objects that expose a number of endpoint functions. These functions
# can be called by other actors in the system and their responses gathered asynchronously.
#
# Let's start by defining a simple actor:

from monarch.actor import Actor, endpoint, this_proc


class Counter(Actor):
    def __init__(self, initial_value: int):
        self.value = initial_value

    @endpoint
    def increment(self) -> None:
        self.value += 1

    @endpoint
    def get_value(self) -> int:
        return self.value


# %%
# The decorator `endpoint` specifies functions of the Actor that can be called remotely
# from other actors. We do it this way to ensure that IDE can offer autocompletions of
# actor calls with correct types.

# %%
# Spawning An Actor In The Local Process
# ======================================
# We can then spawn an actor in the currently running process like so:

counter: Counter = this_proc().spawn("counter", Counter, initial_value=0)

# %%
# ``this_proc()`` is a handle to a process and lets us directly control where an actor runs.
# Monarch is very literal about where things run so that code can be written in the most
# efficient way. For instance, two actors in the same proc can rely on the fact that they
# have the same memory space. Two actors on the same host can communicate through /dev/shm, etc.
# Note that passing messages between two actors (even if they are on the same proc) currently
# always implies serialization.

# %%
# Sending A Simple Message
# ========================
# Once spawned we can send messages to the actor:

from monarch.actor import Future

fut: Future[int] = counter.get_value.call_one()
value: int = fut.get()

print(f"Counter value: {value}")

# %%
# Here we invoked the get_value message, returning 0, the current value of the Counter.
# We refer to the ``call_one`` method as an 'adverb' because it modifies how results of the
# endpoint are handled. ``call_one`` just invokes a single actor and gets its value.
#
# Notice the return value is a Future[int] -- the message is sent asynchronously, letting
# the sender do other things before it needs the reply. ``get()`` waits on the result.
# Futures can also be awaited if you are in an asyncio context.

# %%
# Multiple Actors at Once
# =======================
# Monarch scales to thousands of machines because of its ability to broadcast a single
# message to many actors at once rather than send many point-to-point messages. By
# organizing communication into trees, monarch can multicast messages to thousands of
# machines in a scalable way with good latency.
#
# We express broadcasted communication by organizing actors into a **Mesh** -- a
# multidimensional container with named dimensions. For instance a cluster might have
# dimensions {"hosts": 32, "gpus": 8}. Dimension names are normally things like "hosts",
# indexing across the hosts in a cluster, or "gpus", indexing across things in a machine.

from monarch.actor import ProcMesh, this_host

# %%
# To create a mesh of actors, we first create a mesh of processes, and spawn an actor on them:
procs: ProcMesh = this_host().spawn_procs(per_host={"gpus": 8})
counters: Counter = procs.spawn("counters", Counter, 0)

# %%
# Broadcasting Messages
# ---------------------
# Now messages can be sent to all the actors in the mesh:

counters.increment.broadcast()


# %%
# The `broadcast` adverb means that we are sending a message to all members of the mesh.
# `broadcast` is inherently asynchronous: it does not return a future that you can wait on.
# Note however that all messages (including broadcasts) are delivered in the order sent by
# the client (about which more later).

# %%
# Logging
# ---------------------
# Since we're talking about having multiple actors now, it's worth briefly covering how Monarch handles distributed logging.
# Monarch streams logs from all the distributed processes back to the Monarch client and applies
# a log aggregator while doing so.  The log aggregator helps reduce the verbosity of the logs from the various processes by aggregating
# similar lines and providing a summary.  This follows the larger theme of making the distributed job feel like a local one.
#
# If you wish to look at the raw log files rather than the streamed ones (as streaming is best-effort), you can generally find
# them at ``/tmp/$USER/monarch*`` on the server running the client and the other Monarch processes.
#
# You can override the log levels by setting ``MONARCH_FILE_LOG`` (stdout), and ``MONARCH_STDERR_LOG`` (stderr).  Valid values
# include ``["TRACE", "DEBUG", "INFO", "WARN", "ERROR"]``.

# %%
# Slicing Meshes
# --------------
# We can also slice up the mesh and send to only some of it (gpus 0 through 4):

counters.slice(gpus=slice(0, 4)).increment.broadcast()


# %%
# The ``call`` adverb lets us broadcast a message to a group of actors, and then aggregate
# their responses back:
print(counters.get_value.call().get())


# %%
# ``broadcast`` and ``call`` are the most commonly used adverbs. The ``call_one`` adverb we used
# earlier is actually just a special case of ``call``, asserting that you know there is only
# a single actor being messaged.

# %%
# Multiple Hosts
# ==============
# When we created our processes before, we spawned them on `this_host()` -- the machine
# running the top-level script. For larger jobs, monarch controls many machines. How these
# machines are obtained depends on the scheduling system (slurm, kubernetes, etc), but these
# schedulers are typically encapsulated in a config file.

from monarch.actor import context, HostMesh, hosts_from_config


# %%
# We obtain the mesh of hosts for the job by loading that config:

hosts: HostMesh = hosts_from_config("MONARCH_HOSTS")  # NYI: hosts_from_config
print(hosts.extent)

# An extent is the logical shape of a mesh. It is an ordered map, specifying the size of
# each dimension in the mesh.


# %%
# Let's imagine we are writing a trainer across multiple hosts:
class Trainer(Actor):
    @endpoint
    def step(self):
        my_point = context().message_rank
        return f"Trainer {my_point} taking a step."


trainer_procs: ProcMesh = hosts.spawn_procs(per_host={"gpus": 4})
print(f"Process mesh extent: {trainer_procs.extent}")


# %%
# Spawn the trainers

trainers = trainer_procs.spawn("trainer", Trainer)

# %%
# Do one training step and wait for all to finish it

print(trainers.step.call().get())

# %%
# Actor and Process References
# ============================
# Actors, processes, and hosts can be sent between actors as arguments to messages.
# This lets programs establish more complicated messaging patterns or establish point-to-point
# connectivity between actors. For instance, we can launch DataLoader actors and Trainer actors
# then pass the dataloaders to the trainers. The nice part of doing it this way instead of having
# the trainers launch the data loaders is that we can relocate the data loader to live on another host
# without the trainer needing to be modified.


class DataLoader(Actor):
    def __init__(self):
        self.i = 0

    @endpoint
    def example(self):
        self.i += 1
        return f"<fake data {self.i}>"


class Trainer(Actor):
    def __init__(self, dataloader: DataLoader):
        my_rank = context().actor_instance.rank
        # select or single corresponding data loader to work with.
        self.dataloader = dataloader.slice(**my_rank)

    @endpoint
    def step(self, input=None):
        data = self.dataloader.example.call_one().get()
        return f"Step with data {data} and input = {input}"


dataloaders = trainer_procs.spawn("dataloader", DataLoader)
dtrainers = trainer_procs.spawn("dtrainers", Trainer, dataloaders)
print(dtrainers.step.call().get())

# %%
# The Supervision Tree
# ====================
# Large scale training will run into issues with faulty hardware, flaky networks, and
# software bugs. Monarch is designed to provide good behaviors for errors and faults by
# default with the option to define more sophisticated behavior for faster recovery or
# fault tolerance.
#
# We borrow from Erlang the idea of a tree of supervision. Each process, and each actor
# has a parent that launched it and is responsible for its health.


class Errorful(Actor):
    @endpoint
    def say_hello(self):
        raise Exception("I don't want to")


# %%
# If we are calling the endpoint and expecting a response, the error will get routed to the caller:

e = this_proc().spawn("errorful", Errorful)
try:
    e.say_hello.call_one().get()
except Exception:
    print("It didn't say hello")

# %%
# Broadcasting Without Response
# ----------------------------
# We also might call something and provide it no way to respond:

if False:
    e.say_hello.broadcast()  # do not expect a response NYI: supervision is buggy here and doesn't kill the process!


# %%
# The default behavior of the supervision tree means that an error at any level of process
# or actor creation will not go unreported. This can prevent a lot of accidental deadlocks
# compared to the standard unix-style defaults where threads and processes exiting do not
# stop the spawning processes.

# %%
# Fine-grained Supervision
# ========================
# Sometimes fine-grained recovery is possible. For instance, if a data loader failed to
# read a URL, perhaps it would work to just restart it. In these cases, we also offer a
# different API. If an actor defines a `__supervise__` special method, then it will get
# called to handle supervision events for meshes owned by the actor.


class SupervisorActor(Actor):
    def __supervise__(self, event):
        # NYI: specific supervise protocol is not spec'd out or implemented.
        print(f"Supervision event received: {event}")
        # Logic to handle supervision events and potentially restart failed actors


# %%
# Point-to-Point RDMA
# ====================
# Monarch provides first-class RDMA support through the RDMABuffer API. This lets
# you separate data references from data movement - instead of sending large tensors
# through your messaging patterns, you pass lightweight buffer references anywhere
# in your system, then explicitly transfer the actual data only when and where you need it.
# This approach lets you design your service's coordination patterns based on your application
# logic rather than being constrained by when the framework forces data transfers.
#
# RDMA transfers use libibverbs to transfer data over infiniband or RoCE.
# Unlike traditional NCCL collectives where a send must be matched to a receive,
# once an actor has a handle to a buffer, it can read or write to the buffer without the owner of the buffer.

import torch
from monarch.rdma import RDMABuffer


class ParameterServer(Actor):
    def __init__(self):
        self.weights = torch.rand(1000, 1000)  # Large model weights

        # RDMABuffer does not copy the data. It just
        # creates a view of the data that can be passed to
        # other processes.
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


# %%
# RDMABuffers are intentionally low-level: they do just the data movement when requested. It is up
# to the caller to ensure that the data is not modified until the movement is complete. We intend to
# build higher-level libraries for transferring tensors or implementing collective operations
# on top of these primitives.


# %%
# Distributed Tensors
# ===================
# Monarch also comes with a 'tensor engine' that provides distributed torch tensors.
# This lets a single actor directly compute with tensors distributed across a mesh of processes.
#
# We can use distributed features by 'activating' a ProcMesh:

with trainer_procs.activate():
    t = torch.rand(3, 4)
print(t)


# %%
# The tensor `t` is now a distributed tensor with a unique value across each process in the mesh.
# we can examine the values on each proc using built in functions:

from monarch import fetch_shard

print("one", fetch_shard(t, hosts=0, gpus=0).get())
print("two", fetch_shard(t, hosts=0, gpus=1).get())


# %%
# and compute with the tensors using any torch function:

with trainer_procs.activate():
    dist_compute = t @ t.T

print("dist", fetch_shard(dist_compute, hosts=0, gpus=0).get())


# %%
# A distributed tensor can be passed to any mesh of actors that is located on the same mesh
# of processes as the tensors. Each actor will receive the shard of the distributed tensor
# that is on the same process as the actor:

print(dtrainers.step.call(dist_compute[0]).get())


# %%
# And actors themselves can define their own distributed tensors with the `rref` adverb:


class LinearActor(Actor):
    def __init__(self):
        self.w = torch.rand(3, 3)

    # The propagation function is like a type signature: it computes the
    # shape of the output, given an input of a particular shape. It is used
    # by the tensor engine to correctly create the (local) returned distributed
    # tensor references; this way local references can be created and used
    # immediately, without needing to synchronize the calls with the actors.
    @endpoint(propagate=lambda x: x)
    def forward(self, input):
        return input @ self.w


linear = trainer_procs.spawn("linear", LinearActor)

o = torch.relu(linear.forward.rref(t))


# %%
# The ``rref`` adverb invokes the endpoint and then makes the output of the endpoint into a distributed tensor that can be used in
# further distributed computation.
#
# Distributed tensors also include ways of doing reductions and gathers across shards and moving tensors between processes.
# See :doc:`distributed_tensors` for more information including reductions across shards and moving tensors between processes.
# We eventually want it to be possible for an entirely training framework to be written in terms of distributed tensors.
# However, currently the performance of messaging for distributed tensors is not optimized enough to make this practice. They are
# still useful for interactive debugging.


# %%
# Summary
# =======
# We have now seen all four major features of Monarch:
#
# 1. **Scalable messaging** using multidimensional meshes of actors
# 2. **Fault tolerance** through supervision trees and __supervise__
# 3. **Point-to-point low-level RDMA**
# 4. **Built-in distributed tensors**
#
# This foundation enables building sophisticated multi-machine training programs with
# clear semantics for distribution, fault tolerance, and communication patterns.
#
# The remaining sections fill in more details about how to accomplish common patterns
# with the above features.

# %%
# Actor Context
# =============
# At any point, code might need to know 'what' it is and 'where' it is. For instance,
# when initializing torch.distributed, it will need a RANK/WORLD_SIZE. In monarch, the
# 'what' is always what actor is currently running the code. The 'where' can be
# looked up using the `context()` API:


class ContextAwareActor(Actor):
    @endpoint
    def get_context_info(self):
        actor_instance = context().actor_instance
        # `context().message_rank` returns the coordinates in the message. This isn't always the
        # same as the actor's rank because messages can be sent to a slice of actors rather than
        # the whole thing.
        message_rank = context().message_rank
        return f"Actor rank: {actor_instance.rank}, message rank: {message_rank}"


c = trainer_procs.spawn("context_actor", ContextAwareActor)
print(c.get_context_info.call().get())


# %%
# ranks are always multidimension and reported as dictionaries of the dimension names
# and the point within that dimension.


# %%
# Free Remote Functions
# ====================
# It is possible to run functions on a process mesh that are not associated with any actor.
# These can be useful to set up state on a process, or perform pre-post processing of
# data before sending it to an actor:

from monarch import remote


@remote
def check_memory():
    import psutil

    return psutil.Process().memory_info()


with trainer_procs.activate():
    print(check_memory.call().get())

# %%
# Channels and Low-level messaging
# ================================
# It is sometimes useful to establish direct channels between two points,
# or forward the handling of some messages from one actor to another.
# To enable this, all messaging in monarch is build out of port objects.

# %%
# An actor can create a new channel, which provides a Port for sending and
# a PortReceiver for receiving messages. The Port object can then be send
# to any endpoint.

from monarch.actor import Channel, Port

port, recv = Channel.open()

port.send(3)
print(recv.recv().get())


# %%
# Ports can be passed as arguments to actors and sent a response
# remotely. We can also directly ask an endpoint to send its response to a port using
# the  ``send`` messaging primitive.

from monarch.actor import send

with trainer_procs.activate():
    send(check_memory, args=(), kwargs={}, port=port)


# %%
# The port will receive a response from each actor send the message:

for _ in range(4):
    print(recv.recv().get())


# %%
# The other adverbs like ``call``, ``stream``, and ``broadcast`` are just
# implemented in terms of ports and send.


# %%
# Message Ordering
# ======================
# Messages from an actor are delivered to the destination actor in order in which they are sent.
# In particular, if actor A sends a message M0 to actor B, and then
# later A sends another message M1 to B, then actor B will receive M0 before M1.
# Messages in monarch are sent to a mesh of multiple actor instances at once. For
# the purpose of message ordering, this bulk send behaves as if it sent each message
# individually to each destination.
#
# Each actor handles its messages sequentially. It must finish the handling of a message
# before the next message on the actor is called.
#
# Different actors in the same process handle messages concurrently.

# %%
# Responding Out of Order
# =============================
# Messages to actors are delivered in order, but sometimes an actor might want to
# respond to later messages first. The normal way of defining an endpoint does not
# allow for this since it must return the response before future messages are delivered.
#
# Instead, an endpoint can request an explicit port object on which to deliver a response.
#
# Here is an example of an inference engine
# where we use an explicit reponse port for the `infer` endpoint
# so that we can make sure that we always use the most recent version of the weights
# even if update_weights was called after infer:

import threading
import time
from queue import Queue


class InferenceEngine(Actor):
    def __init__(self):
        self.q = Queue()
        self.new_weights = None
        self.worker = threading.Thread(target=self.run)
        self.worker.start()

    @endpoint
    def update_weights(self, new_weights):
        self.new_weights = new_weights

    @endpoint(explicit_response_port=True)
    def infer(self, port: Port[int]):
        self.q.put(port)

    def run(self):
        while True:
            request = self.q.get()
            # always use the most recent weight version we have:
            result = f"inferred with: {self.new_weights}"
            # pretend to take time
            time.sleep(1)
            # process request, send response
            request.send(result)


engine = this_proc().spawn("engine", InferenceEngine)

engine.update_weights.broadcast(0)

first = engine.infer.call_one()
second = engine.infer.call_one()
engine.update_weights.broadcast(1)

print(first.get())
print(second.get())  # still uses newest weights
