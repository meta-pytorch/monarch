# Internal APIs

Internally monarch is implemented using a Rust library for actors called hyperactor.

[This book](books/hyperactor-book/src/introduction.html) provides more details about its design.

This page provides access to the Rust API documentation for Monarch.

The Monarch project consists of several Rust crates, each with specialized functionality:

## Core Framework

- [**hyperactor**](rust-api/hyperactor/index.html) - Core actor framework for distributed computing
- [**hyperactor_macros**](rust-api/hyperactor_macros/index.html) - Procedural macros for the hyperactor framework
- [**hyperactor_mesh**](rust-api/hyperactor_mesh/index.html) - Mesh networking for hyperactor clusters
- [**hyperactor_mesh_macros**](rust-api/hyperactor_mesh_macros/index.html) - Macros for hyperactor mesh functionality
- [**hyperactor_config**](rust-api/hyperactor_config/index.html) - Configuration framework for hyperactor
- [**hyperactor_telemetry**](rust-api/hyperactor_telemetry/index.html) - Telemetry and monitoring for hyperactor

## CUDA and GPU Computing

- [**nccl-sys**](rust-api/nccl_sys/index.html) - NCCL (NVIDIA Collective Communications Library) bindings
- [**torch-sys2**](rust-api/torch_sys2/index.html) - Simplified PyTorch Python API bindings for Rust
- [**torch-sys-cuda**](rust-api/torch_sys_cuda/index.html) - CUDA-specific PyTorch FFI bindings
- [**monarch_tensor_worker**](rust-api/monarch_tensor_worker/index.html) - High-performance tensor processing worker

## RDMA and High-Performance Networking

- [**monarch_rdma**](rust-api/monarch_rdma/index.html) - Remote Direct Memory Access (RDMA) support for high-speed networking
- [**rdmaxcel-sys**](rust-api/rdmaxcel_sys/index.html) - Low-level RDMA acceleration bindings

## Monarch Python Integration

- [**monarch_hyperactor**](rust-api/monarch_hyperactor/index.html) - Python bindings bridging hyperactor to Monarch's Python API
- [**monarch_extension**](rust-api/monarch_extension/index.html) - Python extension module for Monarch functionality
- [**monarch_messages**](rust-api/monarch_messages/index.html) - Message types for Monarch actor communication

## System and Utilities

- [**hyper**](rust-api/hyper/index.html) - Mesh admin CLI and HTTP utilities
- [**ndslice**](rust-api/ndslice/index.html) - N-dimensional array slicing and manipulation
- [**typeuri**](rust-api/typeuri/index.html) - Type URI system for message serialization
- [**wirevalue**](rust-api/wirevalue/index.html) - Wire-level value serialization for actor messages
- [**serde_multipart**](rust-api/serde_multipart/index.html) - Zero-copy multipart serialization

 Static links are shown by default since documentation exists 

## Architecture Overview

The Rust implementation provides a comprehensive framework for distributed computing with GPU acceleration:

- **Actor Model**: Built on the hyperactor framework for concurrent, distributed processing
- **GPU Integration**: Native CUDA support for high-performance computing workloads
- **Mesh Networking**: Efficient communication between distributed nodes
- **Tensor Operations**: Optimized tensor processing with PyTorch integration
- **Multi-dimensional Arrays**: Advanced slicing and manipulation of n-dimensional data

For complete technical details, API references, and usage examples, explore the individual crate documentation above.