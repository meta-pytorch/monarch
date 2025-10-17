# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from utils.master_node import MasterNodeServer


def get_master_ips():
    """
    Get private and public IP addresses of the master node.
    
    Returns:
        tuple: (private_master_host_ip_address, public_master_host_ip_address)
    """
    private_master_host_ip_address = MasterNodeServer.get_master_ip()
    public_master_host_ip_address = MasterNodeServer.get_master_public_ip_curl()
    return private_master_host_ip_address, public_master_host_ip_address


def extract_ips_simple(file_path):
    """
    Simple extraction assuming each line contains an IP address.
    """
    ip_set = set()

    try:
        with open(file_path, "r") as file:
            for line in file:
                ip = line.strip()
                if ip:  # Skip empty lines
                    ip_set.add(ip)
    except FileNotFoundError:
        print(f"Error: File {file_path} not found")
    except Exception as e:
        print(f"Error reading file: {e}")

    return ip_set


def check_ips_available(job, num_nodes):
    """
    Extract IP addresses from job machines and check if they are available.
    
    Args:
        job: MMT job object with machines attribute
        num_nodes: Expected number of nodes
        
    Returns:
        tuple: (ips_available flag, ip_addresses_set)
    """
    ip_addresses_list = [machine.public_ip for machine in job.machines]
    ip_addresses_set = set(ip_addresses_list)
    print(f"{ip_addresses_list=}")
    print(f"{ip_addresses_set=}")
    ips_available = not ip_addresses_set == {''} and len(ip_addresses_set) == num_nodes
    print(f"IP addresses are available: {ips_available}")
    return ips_available, ip_addresses_set


def create_tcp_addresses(ip_addresses_set, port):
    """
    Create TCP addresses from a set of IP addresses and a port.
    
    Args:
        ip_addresses_set: Set of IP addresses
        port: Port number to use
        
    Returns:
        list: List of TCP addresses in the format "tcp!{ip}:{port}"
    """
    tcp_addresses = [f"tcp!{ip}:{port}" for ip in ip_addresses_set]
    print(*tcp_addresses)
    return tcp_addresses
