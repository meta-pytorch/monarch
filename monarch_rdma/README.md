# Monarch RDMA

## Overview

Monarch RDMA is a Rust library that provides high-performance Remote Direct Memory Access (RDMA) capabilities for the Monarch framework. It enables direct memory access from the memory of one computer into the memory of another without involving either computer's operating system, resulting in high-throughput, low-latency networking with minimal CPU overhead.

## Features

- **High-performance RDMA communication**: Enables direct memory-to-memory transfers between machines
- **CUDA integration**: Support for GPU memory transfers via RDMA with GPUDirect RDMA
- **Actor-based API**: Clean, actor-based interface for managing RDMA resources and connections
- **Comprehensive error handling**: Robust error handling for RDMA operations
- **Memory region management**: Efficient registration and management of memory regions for RDMA operations

## System Requirements

### Hardware Requirements

- RDMA-capable network interface card (NIC), such as Mellanox ConnectX series
- For GPU integration: NVIDIA GPU with CUDA support

On AWS Elastic Fabric Adapter, RDMA runs over SRD rather than a reliable
connection, and several assumptions that hold on Mellanox hardware fail there —
some silently. See [EFA_NOTES.md](EFA_NOTES.md) before debugging an EFA setup.

### Software Requirements

#### Required Libraries

- **libibverbs**: RDMA verbs library for interacting with RDMA hardware
- **CUDA headers**: Required for GPU memory integration (if using CUDA features)
- **GPUDirect RDMA**: Required for direct GPU memory access via RDMA (see installation instructions below)

#### Installing GPUDirect RDMA

For GPU integration, you need to install the GPUDirect RDMA library. Follow the installation guide at:
https://docs.nvidia.com/networking/display/gpudirectrdmav18/installing+gpudirect+rdma

After installation, you can verify that GPUDirect RDMA is properly installed by checking if the nvidia_peermem kernel module is loaded:

```bash
lsmod | grep nvidia_peermem
```

If the module is loaded, you should see output similar to:
```
nvidia_peermem         16384  0
```

#### Configuration for GPUDirect RDMA

If you're using CUDA this library assumes GPUDirect is enabled, you need to enable peer memory mapping by adding the following to your `/etc/modprobe.d/nvidia.conf` file:

```
options nvidia NVreg_RegistryDwords="PeerMappingOverride=1;"
```

After adding this configuration, you'll need to reload the NVIDIA kernel module:

```bash
sudo rmmod nvidia
sudo modprobe nvidia
```

#### Verifying Installation with validate_execution_context

After configuring GPUDirect RDMA, you can use the `validate_execution_context` function to verify that your environment is properly configured for RDMA operations:

```rust
// In your Rust code
use monarch_rdma::rdma_components::validate_execution_context;

async fn check_environment() -> Result<(), anyhow::Error> {
    validate_execution_context().await
}
```

This function checks for the presence of required kernel modules, device files, and proper permissions. A successful result indicates that your system is correctly configured for RDMA operations with GPUDirect support.

## Usage

The library provides several core components:

- `RdmaDomain`: Manages RDMA resources including context, protection domain, and memory region
- `RdmaQueuePair`: Handles communication between endpoints via queue pairs and completion queues
- `RdmaBuffer`: Represents a memory buffer that can be used for RDMA operations
- `RdmaManagerActor`: Actor that manages RDMA resources and connections

### Basic Example

See the `examples` directory for more detailed usage examples, and tests within the library. Users should generally leverage rdma_manager_actor to manage the RDMA resources and connections.

## Architecture

The library is organized into several key components:

- **ibverbs_primitives.rs**: Low-level primitives for interacting with the RDMA hardware
- **rdma_components.rs**: Core RDMA components like domains, queue pairs, and buffers
- **rdma_manager_actor.rs**: Actor-based API for managing RDMA resources and connections

## License

This source code is licensed under the BSD-style license found in the LICENSE file in the root directory of this source tree.
