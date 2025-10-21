monarch.rdma
============

.. currentmodule:: monarch.rdma

The ``monarch.rdma`` module provides Remote Direct Memory Access (RDMA) support for high-performance networking and zero-copy data transfers between processes. See the `Point-to-Point RDMA guide <https://meta-pytorch.org/monarch/generated/examples/getting_started.html#point-to-point-rdma>`_ for an overview.

RDMA Buffer
===========

.. autoclass:: RDMABuffer
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:

RDMA Actions
============

.. autoclass:: RDMAAction
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:

Utility Functions
=================

.. autofunction:: is_rdma_available
