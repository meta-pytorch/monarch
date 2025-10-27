# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import os

# To force Monarch to use V0 for this Notebook (This will be remove in the future)
os.environ["MONARCH_V0_WORKAROUND_DO_NOT_USE"] = "1"

from monarch._rust_bindings.monarch_hyperactor.alloc import AllocConstraints, AllocSpec
from monarch._src.actor.allocator import RemoteAllocator, StaticRemoteAllocInitializer
from monarch.actor import ProcMesh
from utils.ip_utils import check_ips_available, create_tcp_addresses, get_master_ips

# Monarch default port configuration
MONARCH_DEFAULT_PORT = 26600

# Client allowed port range for Monarch communication
CLIENT_ALLOWED_PORT_RANGE = "26600..26610"
os.environ["MONARCH_FILE_LOG"] = "debug"


def setup_allocator(tcp_addresses, public_master_host_ip_address, num_nodes, num_gpus):
    """
    Set up the RemoteAllocator and allocate resources.

    Args:
        tcp_addresses: List of TCP addresses for remote allocation
        public_master_host_ip_address: Public IP address of the master host
        num_nodes: Number of nodes to allocate
        num_gpus: Number of GPUs per node

    Returns:
        tuple: (allocator, alloc)
    """
    os.environ["HYPERACTOR_REMOTE_ALLOC_ALLOWED_PORT_RANGE"] = CLIENT_ALLOWED_PORT_RANGE
    os.environ["HYPERACTOR_REMOTE_ALLOC_BOOTSTRAP_ADDR"] = (
        f"tcp!{public_master_host_ip_address}:0"
    )
    os.environ["HYPERACTOR_REMOTE_ALLOC_BIND_TO_INADDR_ANY"] = "true"
    os.environ["MONARCH_FILE_LOG"] = "debug"

    allocator = RemoteAllocator(
        world_id="foo",
        initializer=StaticRemoteAllocInitializer(*tcp_addresses),
    )

    alloc = allocator.allocate(
        AllocSpec(AllocConstraints(), hosts=num_nodes, gpus=num_gpus)
    )

    print(alloc)
    return allocator, alloc


def create_proc_mesh(alloc):
    """
    Create a ProcMesh from an allocation.

    Args:
        alloc: Allocation object from RemoteAllocator

    Returns:
        ProcMesh: Process mesh created from the allocation
    """
    proc_mesh = ProcMesh.from_alloc(alloc)
    return proc_mesh


def setup_proc_mesh_from_job(job=None, num_nodes=2, num_gpus=8, port=MONARCH_DEFAULT_PORT, ip_addresses_set={}):
    """
    High-level function to set up ProcMesh from an MMT job.

    This function handles all the low-level details of:
    - Getting master node IPs
    - Checking IP availability
    - Creating TCP addresses
    - Setting up allocator
    - Creating proc_mesh

    Args:
        job: MMT job object with machines attribute
        num_nodes: Number of nodes to allocate
        num_gpus: Number of GPUs per node
        port: Port number to use for TCP connections (default: 26600 - Monarch default port)
        ip_set: Set of remote IP addresses in case that user provides them

    Returns:
        ProcMesh: Process mesh ready to use for distributed training
    """
    if not ip_addresses_set:
        # Check IP availability and get IP addresses
        ips_available, ip_addresses_set = check_ips_available(job, num_nodes)

        if not ips_available:
            raise RuntimeError(
                f"IPs are not available. Expected {num_nodes} nodes, got {len(ip_addresses_set)}"
            )

    # Get master IPs (internal use only)
    _, public_master_host_ip_address = get_master_ips()

    # Create TCP addresses
    tcp_addresses = create_tcp_addresses(ip_addresses_set, port)

    # Setup allocator and get allocation
    allocator, alloc = setup_allocator(
        tcp_addresses, public_master_host_ip_address, num_nodes, num_gpus
    )

    # Create and return proc_mesh
    proc_mesh = create_proc_mesh(alloc)

    return proc_mesh
