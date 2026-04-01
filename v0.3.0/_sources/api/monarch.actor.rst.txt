monarch.actor
=============

.. currentmodule:: monarch.actor

The ``monarch.actor`` module provides the actor-based programming model for distributed computation. See :doc:`../generated/examples/getting_started` for an overview.


Creating Actors
===============

Actors are created on multidmensional meshes of processes that
are launch across hosts. HostMesh represents a mesh of hosts. ProcMesh is a mesh of processes.

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
   :exclude-members: __init__, monitor, from_alloc, sync_workspace, logging_option, get

.. autofunction:: get_or_spawn_controller

.. autofunction:: this_host


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



Messaging Actor
===============

Messaging is done through the "adverbs" defined for each endpoint

.. autoclass:: Endpoint
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:
   :exclude-members: __init__, choose

.. autoclass:: Future
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:

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
Use these functions to look up what actor is running the currently executing code.

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
