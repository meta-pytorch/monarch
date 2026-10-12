monarch
=======

These API functions define monarch's distributed tensor computation API. See :doc:`../generated/examples/distributed_tensors` for an overview.

.. currentmodule:: monarch

.. autoclass:: Tensor
   :members:
   :show-inheritance:
   :exclude-members: __init__, get

.. autoclass:: Stream
   :members:
   :undoc-members:
   :show-inheritance:

.. autofunction:: remote

.. autofunction:: coalescing

.. autofunction:: get_active_mesh

.. autofunction:: no_mesh

.. autofunction:: to_mesh

.. autofunction:: slice_mesh

.. autofunction:: get_active_stream

.. autofunction:: reduce

.. autofunction:: reduce_

.. autofunction:: call_on_shard_and_fetch

.. autofunction:: fetch_shard

.. autofunction:: inspect

.. autofunction:: show

.. autofunction:: grad_function

.. autofunction:: grad_generator

.. autofunction:: timer

.. autofunction:: world_mesh

.. autofunction:: function_resolvers


Types
=====

.. autoclass:: Extent
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: Shape
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: Selection
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: NDSlice
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: OpaqueRef
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: Future
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: ActorFuture
   :members:
   :undoc-members:
   :show-inheritance:


Distributed Computing
=====================

.. autoclass:: RemoteProcessGroup
   :members:
   :undoc-members:
   :show-inheritance:


Simulation
==========

.. autoclass:: Simulator
   :members:
   :undoc-members:
   :show-inheritance:

.. autofunction:: set_meta


Builtins
========

.. automodule:: monarch.builtins
   :members:
   :undoc-members:
   :show-inheritance:
