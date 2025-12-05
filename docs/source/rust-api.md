# Internal APIs

Internally monarch is implemented using a Rust library for actors called hyperactor.

[This book](books/hyperactor-book/src/introduction) provides more details about its design.

This page provides access to the Rust API documentation for Monarch.

The Monarch project consists of several Rust crates, each with specialized functionality:

### Core Framework
- <a id="link-hyperactor" href="rust-api/hyperactor/index.html">**hyperactor**</a><span id="desc-hyperactor"> - Core actor framework for distributed computing</span>
- <a id="link-hyperactor_macros" href="rust-api/hyperactor_macros/index.html">**hyperactor_macros**</a><span id="desc-hyperactor_macros"> - Procedural macros for the hyperactor framework</span>
- <a id="link-hyperactor_multiprocess" href="rust-api/hyperactor_multiprocess/index.html">**hyperactor_multiprocess**</a><span id="desc-hyperactor_multiprocess"> - Multi-process support for hyperactor</span>
- <a id="link-hyperactor_mesh" href="rust-api/hyperactor_mesh/index.html">**hyperactor_mesh**</a><span id="desc-hyperactor_mesh"> - Mesh networking for hyperactor clusters</span>
- <a id="link-hyperactor_mesh_macros" href="rust-api/hyperactor_mesh_macros/index.html">**hyperactor_mesh_macros**</a><span id="desc-hyperactor_mesh_macros"> - Macros for hyperactor mesh functionality</span>

### CUDA and GPU Computing
- <a id="link-cuda-sys" href="rust-api/cuda_sys/index.html">**cuda-sys**</a><span id="desc-cuda-sys"> - Low-level CUDA FFI bindings</span>
- <a id="link-nccl-sys" href="rust-api/nccl_sys/index.html">**nccl-sys**</a><span id="desc-nccl-sys"> - NCCL (NVIDIA Collective Communications Library) bindings</span>
- <a id="link-torch-sys2" href="rust-api/torch_sys2/index.html">**torch-sys2**</a><span id="desc-torch-sys2"> - Simplified PyTorch Python API bindings for Rust</span>
- <a id="link-monarch_tensor_worker" href="rust-api/monarch_tensor_worker/index.html">**monarch_tensor_worker**</a><span id="desc-monarch_tensor_worker"> - High-performance tensor processing worker</span>

### RDMA and High-Performance Networking
- <a id="link-monarch_rdma" href="rust-api/monarch_rdma/index.html">**monarch_rdma**</a><span id="desc-monarch_rdma"> - Remote Direct Memory Access (RDMA) support for high-speed networking</span>
- <a id="link-rdmaxcel-sys" href="rust-api/rdmaxcel_sys/index.html">**rdmaxcel-sys**</a><span id="desc-rdmaxcel-sys"> - Low-level RDMA acceleration bindings</span>

### System and Utilities
- <a id="link-controller" href="rust-api/controller/index.html">**controller**</a><span id="desc-controller"> - System controller and orchestration</span>
- <a id="link-hyper" href="rust-api/hyper/index.html">**hyper**</a><span id="desc-hyper"> - HTTP utilities and web service support</span>
- <a id="link-ndslice" href="rust-api/ndslice/index.html">**ndslice**</a><span id="desc-ndslice"> - N-dimensional array slicing and manipulation</span>
- <a id="link-monarch_extension" href="rust-api/monarch_extension/index.html">**monarch_extension**</a><span id="desc-monarch_extension"> - Python extension module for Monarch functionality</span>

<!-- Static links are shown by default since documentation exists -->

## Architecture Overview

The Rust implementation provides a comprehensive framework for distributed computing with GPU acceleration:

- **Actor Model**: Built on the hyperactor framework for concurrent, distributed processing
- **GPU Integration**: Native CUDA support for high-performance computing workloads
- **Mesh Networking**: Efficient communication between distributed nodes
- **Tensor Operations**: Optimized tensor processing with PyTorch integration
- **Multi-dimensional Arrays**: Advanced slicing and manipulation of n-dimensional data

For complete technical details, API references, and usage examples, explore the individual crate documentation above.
