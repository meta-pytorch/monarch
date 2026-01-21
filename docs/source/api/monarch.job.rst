monarch.job
===========

.. currentmodule:: monarch.job

The ``monarch.job`` module provides a declarative interface for managing
distributed job resources. Jobs abstract away the details of different
schedulers (SLURM, local execution, etc.) and provide a unified way to
allocate hosts and create HostMesh objects.

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


Serialization
=============

Jobs can be serialized and deserialized for persistence and caching.

.. autofunction:: job_load

.. autofunction:: job_loads
