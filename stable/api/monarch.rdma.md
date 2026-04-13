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

read_into(*dst*, ***, *timeout=3*)[[source]](../_modules/monarch/_src/rdma/rdma.html#RDMABuffer.read_into)

Read data from the RDMABuffer into a destination tensor.

The destination tensor must be contiguous (including tensor views/slices).
:param dst: Destination tensor or memoryview to read into.

Keyword Arguments:

**timeout** ([*int*](https://docs.python.org/3/library/functions.html#int)*,**optional*) - Timeout in seconds for the operation. Defaults to 3s.

Returns:

A Monarch Future that can be awaited or called with .get() for blocking operation.

Return type:

[Future](monarch.html#monarch.Future)[Optional[[int](https://docs.python.org/3/library/functions.html#int)]]

Raises:

[**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) - If the destination tensor size is smaller than the RDMA buffer size.

Note

Currently only CPU tensors are fully supported. GPU tensors will be temporarily
copied to CPU, which may impact performance.

write_from(*src*, ***, *timeout=3*)[[source]](../_modules/monarch/_src/rdma/rdma.html#RDMABuffer.write_from)

Write data from a source tensor into the RDMABuffer.

Parameters:

**src** ([*torch.Tensor*](https://docs.pytorch.org/docs/stable/tensors.html#torch.Tensor)*|*[*memoryview*](https://docs.python.org/3/library/stdtypes.html#memoryview)) - Source tensor containing data to be written to the RDMA buffer.
Must be a contiguous tensor (including tensor views/slices).
Either src or addr/size must be provided.

Keyword Arguments:

**timeout** ([*int*](https://docs.python.org/3/library/functions.html#int)*,**optional*) - Timeout in seconds for the operation. Defaults to 3s.

Returns:

A Monarch Future object that can be awaited or called with .get()

for blocking operation. Returns None when completed successfully.

Return type:

[Future](monarch.html#monarch.Future)[None]

Raises:

[**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) - If the source tensor size exceeds the RDMA buffer size.

Note

Currently only CPU tensors are fully supported. GPU tensors will be temporarily
copied to CPU, which may impact performance.

drop()[[source]](../_modules/monarch/_src/rdma/rdma.html#RDMABuffer.drop)

Release the handle on the memory that the src holds to this memory.

*property*owner*: [str](https://docs.python.org/3/library/stdtypes.html#str)*

The owner reference (str)

# RDMA Actions

*class*monarch.rdma.RDMAAction[[source]](../_modules/monarch/_src/rdma/rdma.html#RDMAAction)

Bases: [`object`](https://docs.python.org/3/library/functions.html#object)

Schedule a bunch of actions at once. This provides an opportunity to
optimize bulk RDMA transactions without exposing complexity to users.

*class*RDMAOp(*value*, *names=<not given>*, **values*, *module=None*, *qualname=None*, *type=None*, *start=1*, *boundary=None*)[[source]](../_modules/monarch/_src/rdma/rdma.html#RDMAAction.RDMAOp)

Bases: [`Enum`](https://docs.python.org/3/library/enum.html#enum.Enum)

Enumeration of RDMA operation types.

READ_INTO*= 'read_into'*

WRITE_FROM*= 'write_from'*

FETCH_ADD*= 'fetch_add'*

COMPARE_AND_SWAP*= 'compare_and_swap'*

__init__()[[source]](../_modules/monarch/_src/rdma/rdma.html#RDMAAction.__init__)

read_into(*src*, *dst*)[[source]](../_modules/monarch/_src/rdma/rdma.html#RDMAAction.read_into)

Read from src RDMA buffer into dst memory.

Parameters:

- **src** (*RDMABuffer*) - Source RDMA buffer to read from
- **dst** (*LocalMemory**|**List**[**LocalMemory**]*) - Destination local memory to read into
If dst is a list, it is the concatenation of the data in the list

write_from(*src*, *dst*)[[source]](../_modules/monarch/_src/rdma/rdma.html#RDMAAction.write_from)

Write from dst memory to src RDMA buffer.

Parameters:

- **src** (*RDMABuffer*) - Destination RDMA buffer to write to
- **dst** (*LocalMemory**|**List**[**LocalMemory**]*) - Source local memory to write from
If local is a list, it is the concatenation of the data in the list

fetch_add(*src*, *dst*, *add*)[[source]](../_modules/monarch/_src/rdma/rdma.html#RDMAAction.fetch_add)

Perform atomic fetch-and-add operation on src RDMA buffer.

Parameters:

- **src** (*RDMABuffer*) - src RDMA buffer to perform operation on
- **dst** (*LocalMemory*) - Local memory to store the original value
- **add** ([*int*](https://docs.python.org/3/library/functions.html#int)) - Value to add to the src buffer

Atomically:

*dst = *src
*src = *src + add

Note: src/dst are 8 bytes

compare_and_swap(*src*, *dst*, *compare*, *swap*)[[source]](../_modules/monarch/_src/rdma/rdma.html#RDMAAction.compare_and_swap)

Perform atomic compare-and-swap operation on src RDMA buffer.

Parameters:

- **src** (*RDMABuffer*) - src RDMA buffer to perform operation on
- **dst** (*LocalMemory*) - Local memory to store the original value
- **compare** ([*int*](https://docs.python.org/3/library/functions.html#int)) - Value to compare against
- **swap** ([*int*](https://docs.python.org/3/library/functions.html#int)) - Value to swap in if comparison succeeds

Atomically:

*dst = *src;
if (*src == compare) {

> *src = swap

}

Note: src/dst are 8 bytes

submit()[[source]](../_modules/monarch/_src/rdma/rdma.html#RDMAAction.submit)

Schedules the work (can be called multiple times to schedule the same work more than once).
Future completes when all the work is done.

Executes futures for each src actor independently and concurrently for optimal performance.

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