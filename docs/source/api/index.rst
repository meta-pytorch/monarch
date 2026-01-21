Python API
==========

.. note::
   This documents Monarch's **public APIs** - the stable, supported interfaces.

The actor API for monarch is accessed through the :doc:`monarch.actor` package.
The :doc:`monarch` package contains APIs related to computing with distributed tensors.
The :doc:`monarch.job` package provides a declarative interface for managing distributed job resources.
The :doc:`monarch.rdma` package provides RDMA support for high-performance networking.

.. toctree::
   :maxdepth: 2

   monarch.actor
   monarch.config
   monarch.job
   monarch
   monarch.rdma
