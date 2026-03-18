monarch.remotemount
===================

.. currentmodule:: monarch.remotemount

The ``monarch.remotemount`` module mounts a local directory as a read-only
FUSE filesystem on every host in a Monarch mesh. Files are packed into a
contiguous buffer, transferred via Monarch messaging (optionally over RDMA),
and served from RAM on each remote host. See the
`remotemount example <https://meta-pytorch.org/monarch/generated/examples/remotemount.html>`_
for usage.

remotemount
===========

.. autofunction:: remotemount

MountHandler
============

.. autoclass:: MountHandler
   :members:
   :undoc-members:
   :show-inheritance:
