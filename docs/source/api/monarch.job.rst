monarch.job
===========

.. currentmodule:: monarch.job

The ``monarch.job`` module provides a declarative interface for managing
distributed job resources. Jobs abstract away the details of different
schedulers (SLURM, local execution, etc.) and provide a unified way to
allocate hosts and create HostMesh objects.

Overview
========

The Job API is responsible for preparing the execution environment for a
Monarch program. A Job specifies *where* a distributed program will run
(locally, on a cluster such as SLURM, or on Kubernetes) and *what resources*
are required before any actors or training code is executed.

Jobs are declarative: users describe the desired resources (for example, the
number of processes or hosts), and Monarch handles interacting with the
underlying scheduler or runtime to allocate those resources.

Job Model
=========

A job object comprises a declarative specification and optionally the job's
*state*. The ``apply()`` operation applies the job's specification to the
scheduler, creating or updating the job as required. Once applied, you can
query the job's ``state()`` to get the allocated HostMesh objects.

Example::

    from monarch.job import SlurmJob

    # Create a job specification
    job = SlurmJob(
        meshes={"trainers": 4, "dataloaders": 2},
        partition="gpu",
        time_limit="01:00:00",
    )

    # Get the state (applies the job if needed)
    state = job.state()

    # Access host meshes by name
    trainer_hosts = state.trainers
    dataloader_hosts = state.dataloaders
    
Simple Bootstrap Example
========================

The following example demonstrates a minimal local Job setup. It allocates
resources on the local machine and runs a simple actor, illustrating the full
flow from Job creation to execution.

.. code-block:: python

    from monarch.job import LocalJob
    from monarch.actor import Actor, endpoint

    class HelloActor(Actor):
        @endpoint
        def hello(self):
            print("Hello from Monarch")

    # Define a local job with two worker processes
    job = LocalJob(meshes={"workers": 2})

    # Apply the job and retrieve allocated resources
    state = job.state()

    # Access the allocated host mesh
    workers = state.workers

    # Spawn actors on the workers and invoke the endpoint
    actors = workers.spawn("hello", HelloActor)
    actors.hello.call()


Job State
=========

.. autoclass:: JobState
   :members:
   :undoc-members:
   :show-inheritance:


Job Base Class
==============

All job implementations inherit from ``JobTrait``, which defines the core
interface for job lifecycle management.

.. autoclass:: JobTrait
   :members:
   :undoc-members:
   :show-inheritance:
   :exclude-members: __init__


Job Implementations
===================

LocalJob
--------

.. autoclass:: LocalJob
   :members:
   :undoc-members:
   :show-inheritance:
   :exclude-members: __init__

SlurmJob
--------

.. autoclass:: SlurmJob
   :members:
   :undoc-members:
   :show-inheritance:
   :exclude-members: __init__

KubernetesJob
-------------

.. autoclass:: monarch.job.kubernetes.KubernetesJob
   :members:
   :undoc-members:
   :show-inheritance:
   :exclude-members: __init__


Serialization
=============

Jobs can be serialized and deserialized for persistence and caching.

.. autofunction:: job_load

.. autofunction:: job_loads


SPMD Jobs
=========

The ``monarch.job.spmd`` submodule provides job primitives for launching
torchrun-style SPMD training over Monarch. It parses torchrun arguments from
an AppDef and executes the training script across the mesh.

.. currentmodule:: monarch.job.spmd

.. autofunction:: serve

.. autoclass:: SPMDJob
   :members:
   :undoc-members:
   :show-inheritance:
   :exclude-members: __init__
