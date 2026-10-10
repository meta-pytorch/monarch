# Internal APIs

Internally monarch is implemented using a Rust library for actors called hyperactor.

[This book](books/hyperactor-book/src/introduction) provides more details about its design.

This page provides access to the Rust API documentation for Monarch.

The Monarch project consists of several Rust crates, each with specialized functionality:

### Core Framework
- <a id="link-hyperactor" href="rust-api/hyperactor/index.html">**hyperactor**</a><span id="desc-hyperactor"> - Core actor framework for distributed computing</span>
- <a id="link-hyperactor_macros" href="rust-api/hyperactor_macros/index.html">**hyperactor_macros**</a><span id="desc-hyperactor_macros"> - Procedural macros for the hyperactor framework</span>
- <a id="link-hyperactor_mesh" href="rust-api/hyperactor_mesh/index.html">**hyperactor_mesh**</a><span id="desc-hyperactor_mesh"> - Mesh networking for hyperactor clusters</span>
- <a id="link-hyperactor_mesh_macros" href="rust-api/hyperactor_mesh_macros/index.html">**hyperactor_mesh_macros**</a><span id="desc-hyperactor_mesh_macros"> - Macros for hyperactor mesh functionality</span>
- <a id="link-hyperactor_config" href="rust-api/hyperactor_config/index.html">**hyperactor_config**</a><span id="desc-hyperactor_config"> - Configuration framework for hyperactor</span>
- <a id="link-hyperactor_telemetry" href="rust-api/hyperactor_telemetry/index.html">**hyperactor_telemetry**</a><span id="desc-hyperactor_telemetry"> - Telemetry and monitoring for hyperactor</span>

### CUDA and GPU Computing
- <a id="link-nccl-sys" href="rust-api/nccl_sys/index.html">**nccl-sys**</a><span id="desc-nccl-sys"> - NCCL (NVIDIA Collective Communications Library) bindings</span>
- <a id="link-torch-sys2" href="rust-api/torch_sys2/index.html">**torch-sys2**</a><span id="desc-torch-sys2"> - Simplified PyTorch Python API bindings for Rust</span>
- <a id="link-torch-sys-cuda" href="rust-api/torch_sys_cuda/index.html">**torch-sys-cuda**</a><span id="desc-torch-sys-cuda"> - CUDA-specific PyTorch FFI bindings</span>
- <a id="link-monarch_tensor_worker" href="rust-api/monarch_tensor_worker/index.html">**monarch_tensor_worker**</a><span id="desc-monarch_tensor_worker"> - High-performance tensor processing worker</span>

### RDMA and High-Performance Networking
- <a id="link-monarch_rdma" href="rust-api/monarch_rdma/index.html">**monarch_rdma**</a><span id="desc-monarch_rdma"> - Remote Direct Memory Access (RDMA) support for high-speed networking</span>
- <a id="link-rdmaxcel-sys" href="rust-api/rdmaxcel_sys/index.html">**rdmaxcel-sys**</a><span id="desc-rdmaxcel-sys"> - Low-level RDMA acceleration bindings</span>

### Monarch Python Integration
- <a id="link-monarch_hyperactor" href="rust-api/monarch_hyperactor/index.html">**monarch_hyperactor**</a><span id="desc-monarch_hyperactor"> - Python bindings bridging hyperactor to Monarch's Python API</span>
- <a id="link-monarch_extension" href="rust-api/monarch_extension/index.html">**monarch_extension**</a><span id="desc-monarch_extension"> - Python extension module for Monarch functionality</span>
- <a id="link-monarch_messages" href="rust-api/monarch_messages/index.html">**monarch_messages**</a><span id="desc-monarch_messages"> - Message types for Monarch actor communication</span>

### System and Utilities
- <a id="link-hyper" href="rust-api/hyper/index.html">**hyper**</a><span id="desc-hyper"> - Mesh admin CLI and HTTP utilities</span>
- <a id="link-ndslice" href="rust-api/ndslice/index.html">**ndslice**</a><span id="desc-ndslice"> - N-dimensional array slicing and manipulation</span>
- <a id="link-typeuri" href="rust-api/typeuri/index.html">**typeuri**</a><span id="desc-typeuri"> - Type URI system for message serialization</span>
- <a id="link-wirevalue" href="rust-api/wirevalue/index.html">**wirevalue**</a><span id="desc-wirevalue"> - Wire-level value serialization for actor messages</span>
- <a id="link-serde_multipart" href="rust-api/serde_multipart/index.html">**serde_multipart**</a><span id="desc-serde_multipart"> - Zero-copy multipart serialization</span>

<!-- Static links are shown by default since documentation exists -->

## Architecture Overview

The Rust implementation provides a comprehensive framework for distributed computing with GPU acceleration:

- **Actor Model**: Built on the hyperactor framework for concurrent, distributed processing
- **GPU Integration**: Native CUDA support for high-performance computing workloads
- **Mesh Networking**: Efficient communication between distributed nodes
- **Tensor Operations**: Optimized tensor processing with PyTorch integration
- **Multi-dimensional Arrays**: Advanced slicing and manipulation of n-dimensional data

For complete technical details, API references, and usage examples, explore the individual crate documentation above.
