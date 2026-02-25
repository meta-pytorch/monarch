monarch.spmd
============

.. currentmodule:: monarch.spmd

The ``monarch.spmd`` module provides primitives for running torchrun-style SPMD
(Single Program Multiple Data) distributed training scripts over Monarch actor meshes.

It bridges PyTorch distributed training with Monarch by automatically configuring
torch elastic environment variables (``RANK``, ``LOCAL_RANK``, ``WORLD_SIZE``,
``MASTER_ADDR``, ``MASTER_PORT``, etc.) across the mesh.


Environment Setup
-----------------

.. autofunction:: setup_torch_elastic_env

.. autofunction:: setup_torch_elastic_env_async


SPMD Actor
----------

.. autoclass:: SPMDActor
   :members:
   :undoc-members:
   :show-inheritance:
   :exclude-members: __init__
