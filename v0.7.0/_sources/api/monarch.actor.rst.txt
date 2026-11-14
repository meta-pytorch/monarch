monarch.actor
=============

.. currentmodule:: monarch.actor

The ``monarch.actor`` module provides the actor-based programming model for distributed computation. See :doc:`../generated/examples/getting_started` for an overview.


Creating Actors
===============

Actors are created on multidimensional meshes of processes that
are launched across hosts. HostMesh represents a mesh of hosts. ProcMesh is a mesh of processes.

.. autoclass:: HostMesh
   :members:
   :inherited-members:
   :show-inheritance:
   :exclude-members: __init__

.. autoclass:: ProcMesh
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:
   :exclude-members: __init__, monitor, _from_alloc, sync_workspace, logging_option, get

.. autofunction:: get_or_spawn_controller

.. autofunction:: this_host

.. autofunction:: this_proc

.. autofunction:: default_bootstrap_cmd

.. autofunction:: hosts_from_config

.. autofunction:: enable_transport


Defining Actors
===============

All actor classes subclass the Actor base object, which provides them mesh slicing API.
Each publicly exposed function of the actor is annotated with `@endpoint`:

.. autoclass:: Actor
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members:
   :exclude-members: __init__, get

.. autofunction:: endpoint

.. autofunction:: concurrent_endpoint



Messaging Actor
===============

Messaging is done through the "adverbs" defined for each endpoint

.. autoclass:: Endpoint
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:
   :exclude-members: __init__

.. autoclass:: Future
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:
   :exclude-members: __init__

.. autoclass:: ValueMesh
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:
   :exclude-members: __init__


.. autoclass:: ActorError
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:

.. autoclass:: Accumulator
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:





.. autofunction:: send


.. autoclass:: Channel
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:

.. autoclass:: Port
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:
   :exclude-members: __init__


.. autoclass:: PortReceiver
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:
   :exclude-members: __init__


.. autofunction:: as_endpoint


Context API
===========
Use these functions to identify the actor associated with the currently executing code. Inside an endpoint, this is the worker actor. In ordinary controller code, it is the controller's root client actor.

.. autofunction:: current_actor_name

.. autofunction:: current_rank

.. autofunction:: current_size

.. autofunction:: context

.. autoclass:: Context
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:

.. autoclass:: Point
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:
   :exclude-members: from_bytes

.. autoclass:: Extent
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:
   :exclude-members: from_bytes, labels, sizes


Client Shutdown
===============

Call ``shutdown_context()`` from the user's Python controller program when it has finished using Monarch. This ends the lifetime of that process's client; the client cannot be restarted in the same process. ``context()`` is not used to select an actor to stop: the target is the process's client. It is not a shutdown callback to run once per host in a HostMesh. Use a mesh's own stop or shutdown method when you want to stop only that mesh and continue using the client.

Monarch registers client shutdown automatically for normal Python interpreter exit. Call it explicitly when you need to wait for shutdown before continuing or exiting:

.. code-block:: python

    from monarch.actor import shutdown_context

    # After the controller has finished all Monarch work:
    shutdown = shutdown_context()
    shutdown.get(timeout=30)

An async controller can await the returned Future instead. Keep the Future from the call that starts shutdown: later calls return already-completed Futures and do not wait for an earlier shutdown still in progress. Dropping the first Future does not cancel native shutdown.

This is not enforced by a controller-only caller check. In a worker process without an initialized client, the call does nothing and does not create a client. A worker's actor context is not a client. If a worker separately initializes its own client, ``shutdown_context()`` applies to that client.

.. autofunction:: shutdown_context


Supervision
===========
Types used for error handling and supervision in actor meshes.

.. autoclass:: MeshFailure
   :members:
   :undoc-members:
   :show-inheritance:

.. autofunction:: unhandled_fault_hook


Telemetry
=========
Utilities for tracing actor execution.

.. autofunction:: traced
