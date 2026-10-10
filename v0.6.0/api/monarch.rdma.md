# monarch.rdma

The `monarch.rdma` module provides Remote Direct Memory Access (RDMA) support for high-performance networking and zero-copy data transfers between processes. See the [Point-to-Point RDMA guide](https://meta-pytorch.org/monarch/generated/examples/getting_started.html#point-to-point-rdma) for an overview.

# RDMA Buffer

*class*monarch.rdma.RDMABuffer(*data*)[[source]](../_modules/monarch/_src/rdma/rdma.html#RDMABuffer)

Bases: [`object`](https://docs.python.org/3/library/functions.html#object)

__init__(*data*)[[source]](../_modules/monarch/_src/rdma/rdma.html#RDMABuffer.__init__)

RDMABuffer supports 1d contiguous tensors (including tensor views/slices) or 1d c-contiguous memoryviews.

Parameters:

**data** ([*torch.Tensor*](https://docs.pytorch.org/docs/stable/tensors.html#torch.Tensor)*|*[*memoryview*](https://docs.python.org/3/library/stdtypes.html#memoryview)) - torch.Tensor or memoryview to create the buffer from. Must be 1d and contiguous.
If provided, addr and size must not be specified.

Raises:

- [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) - If data is not 1d contiguous, if size is 0, or if data is a GPU tensor.
- [**RuntimeError**](https://docs.python.org/3/library/exceptions.html#RuntimeError) - If no RDMA backend is available on this platform.

Note

Currently only CPU tensors are supported. GPU tensor support will be added in the future.

TODO: Create TensorBuffer, which will be main user API supporting non-contiguous tensors

*property*backend*: [str](https://docs.python.org/3/library/stdtypes.html#str)*

Return the RDMA backend in use ('ibverbs').

size()[[source]](../_modules/monarch/_src/rdma/rdma.html#RDMABuffer.size)

read_into(*dst*, ***, *timeout=60*)[[source]](../_modules/monarch/_src/rdma/rdma.html#RDMABuffer.read_into)

Read data from this RDMABuffer into `dst`.

`dst` must be a 1D contiguous tensor or c-contiguous memoryview
whose byte-size is at least `self.size()`.

Parameters:

**dst** ([*torch.Tensor*](https://docs.pytorch.org/docs/stable/tensors.html#torch.Tensor)*|*[*memoryview*](https://docs.python.org/3/library/stdtypes.html#memoryview)) - Destination tensor or memoryview to read into.

Keyword Arguments:

**timeout** ([*int*](https://docs.python.org/3/library/functions.html#int)*,**optional*) - Timeout in seconds. Defaults to 60s.

Returns:

A Monarch Future that resolves to `None` when

the read completes.

Return type:

[Future](monarch.html#monarch.Future)[None]

Raises:

[**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) - If `dst` is smaller than the RDMA buffer.

write_from(*src*, ***, *timeout=60*)[[source]](../_modules/monarch/_src/rdma/rdma.html#RDMABuffer.write_from)

Write data from `src` into this RDMABuffer.

`src` must be a 1D contiguous tensor or c-contiguous memoryview
whose byte-size is at most `self.size()`.

Parameters:

**src** ([*torch.Tensor*](https://docs.pytorch.org/docs/stable/tensors.html#torch.Tensor)*|*[*memoryview*](https://docs.python.org/3/library/stdtypes.html#memoryview)) - Source tensor or memoryview containing the bytes to
write to the RDMA buffer.

Keyword Arguments:

**timeout** ([*int*](https://docs.python.org/3/library/functions.html#int)*,**optional*) - Timeout in seconds. Defaults to 60s.

Returns:

A Monarch Future that resolves to `None` when

the write completes.

Return type:

[Future](monarch.html#monarch.Future)[None]

Raises:

[**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) - If `src` exceeds the RDMA buffer size.

drop()[[source]](../_modules/monarch/_src/rdma/rdma.html#RDMABuffer.drop)

Release the handle on the memory that the src holds to this memory.

*property*owner*: [str](https://docs.python.org/3/library/stdtypes.html#str)*

The owner reference (str)

# RDMA Actions

*class*monarch.rdma.RDMAAction[[source]](../_modules/monarch/_src/rdma/rdma.html#RDMAAction)

Bases: [`object`](https://docs.python.org/3/library/functions.html#object)

Schedule a batch of RDMA operations and submit them as one unit.

All bookkeeping (per-op validation, intra-batch local-memory race
detection, backend grouping, parallel dispatch) lives in the Rust
_RdmaAction; this class is a thin wrapper around it.

__init__()[[source]](../_modules/monarch/_src/rdma/rdma.html#RDMAAction.__init__)

read_remote(*dst*, *src*)[[source]](../_modules/monarch/_src/rdma/rdma.html#RDMAAction.read_remote)

Queue a read from RDMA buffer `src` into local memory `dst`.

write_remote(*dst*, *src*)[[source]](../_modules/monarch/_src/rdma/rdma.html#RDMAAction.write_remote)

Queue a write from local memory `src` into RDMA buffer `dst`.

fetch_add(*src*, *dst*, *add*)[[source]](../_modules/monarch/_src/rdma/rdma.html#RDMAAction.fetch_add)

compare_and_swap(*src*, *dst*, *compare*, *swap*)[[source]](../_modules/monarch/_src/rdma/rdma.html#RDMAAction.compare_and_swap)

submit(***, *timeout=60*)[[source]](../_modules/monarch/_src/rdma/rdma.html#RDMAAction.submit)

Schedule the queued ops. Safe to call multiple times.

The returned Future does not resolve until every op in the batch
completes, or until the timeout is reached. If any op fails, the
Future resolves with an exception.

# Utility Functions

monarch.rdma.is_ibverbs_available()[[source]](../_modules/monarch/_src/rdma/rdma.html#is_ibverbs_available)

Whether ibverbs RDMA hardware is available on this system.

monarch.rdma.is_rdma_available()[[source]](../_modules/monarch/_src/rdma/rdma.html#is_rdma_available)

Whether RDMA over ibverbs is available on this system.

Deprecated since version Monarch: now supports multiple RDMA backends, so is_rdma_available
is ambiguous and will be removed in a future release. Use
`is_ibverbs_available()` or `get_rdma_backend()` instead.

monarch.rdma.get_rdma_backend()[[source]](../_modules/monarch/_src/rdma/rdma.html#get_rdma_backend)

Return available RDMA backend.

Returns:

One of 'ibverbs', 'tcp', or 'none' indicating the available backend.

Both Mellanox and EFA hardware are accessed through ibverbs.
'tcp' indicates the TCP fallback transport is enabled.

Return type:

[str](https://docs.python.org/3/library/stdtypes.html#str)