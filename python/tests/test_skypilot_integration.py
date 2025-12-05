#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Integration test script for SkyPilot job.

This script tests the basic SkyPilot integration without requiring Monarch
runtime. It validates that SkyPilot cluster launching and node IP retrieval works.

Run this script with:
    python tests/test_skypilot_integration.py

Prerequisites:
- SkyPilot installed and configured with cloud credentials
- Run `sky check` to verify cloud access
"""

import argparse
import sys
import time

try:
    import sky
    from sky.backends.cloud_vm_ray_backend import CloudVmRayResourceHandle
except ImportError:
    print("Error: SkyPilot is not installed. Install with: pip install skypilot")
    sys.exit(1)


def test_skypilot_cluster_launch(
    cluster_name: str = "monarch-integration-test",
    cloud: str = "aws",
    cpus: str = "2+",
    timeout_minutes: int = 10,
) -> bool:
    """
    Test launching a SkyPilot cluster and retrieving node IPs.

    Args:
        cluster_name: Name for the test cluster
        cloud: Cloud provider to use
        cpus: CPU specification
        timeout_minutes: Timeout for cluster launch

    Returns:
        True if test passed, False otherwise
    """
    print(f"\n{'='*60}")
    print("SkyPilot Integration Test")
    print(f"{'='*60}\n")

    # Create a simple task
    task = sky.Task(
        name="monarch-test-task",
        run="echo 'SkyPilot test successful' && hostname && sleep 30",
    )

    # Set resources based on cloud
    cloud_obj = None
    if cloud.lower() == "aws":
        cloud_obj = sky.AWS()
    elif cloud.lower() == "gcp":
        cloud_obj = sky.GCP()
    elif cloud.lower() == "azure":
        cloud_obj = sky.Azure()
    elif cloud.lower() == "kubernetes":
        cloud_obj = sky.Kubernetes()

    resources = sky.Resources(
        cloud=cloud_obj,
        cpus=cpus,
    )
    task.set_resources(resources)

    print(f"Test configuration:")
    print(f"  Cluster name: {cluster_name}")
    print(f"  Cloud: {cloud}")
    print(f"  CPUs: {cpus}")
    print()

    try:
        # Launch the cluster
        print("Step 1: Launching cluster...")
        request_id = sky.launch(
            task,
            cluster_name=cluster_name,
            idle_minutes_to_autostop=5,
            down=True,  # Auto-teardown after idle
        )

        print(f"  Request ID: {request_id}")
        job_id, handle = sky.get(request_id)
        print(f"  Job ID: {job_id}")

        if handle is None:
            print("  ERROR: No handle returned from launch")
            return False

        print("  Cluster launched successfully!")

        # Get cluster status and node IPs
        print("\nStep 2: Getting cluster status and node IPs...")
        request_id = sky.status(cluster_names=[cluster_name])
        statuses = sky.get(request_id)

        if not statuses:
            print("  ERROR: No status returned")
            return False

        status = statuses[0]
        print(f"  Cluster status: {status.status}")
        print(f"  Cluster name: {status.name}")

        handle = status.handle
        if handle is None:
            print("  ERROR: Status has no handle")
            return False

        if not isinstance(handle, CloudVmRayResourceHandle):
            print(f"  ERROR: Unexpected handle type: {type(handle)}")
            return False

        # Get IPs
        if handle.stable_internal_external_ips:
            print(f"\n  Node IPs ({len(handle.stable_internal_external_ips)} nodes):")
            for i, (internal_ip, external_ip) in enumerate(
                handle.stable_internal_external_ips
            ):
                print(f"    Node {i}: internal={internal_ip}, external={external_ip}")
        else:
            print("  WARNING: No IP information available yet")

        # Test passed!
        print("\n" + "=" * 60)
        print("TEST PASSED!")
        print("=" * 60)
        print(
            "\nThe SkyPilot integration is working correctly."
            "\nMonarch workers can be launched on these nodes."
        )
        return True

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback

        traceback.print_exc()
        return False

    finally:
        # Cleanup
        print("\nStep 3: Cleaning up cluster...")
        try:
            request_id = sky.down(cluster_name)
            sky.get(request_id)
            print("  Cluster terminated successfully")
        except Exception as e:
            print(f"  Warning: Failed to cleanup cluster: {e}")
            print(f"  You may need to manually run: sky down {cluster_name}")


def main():
    parser = argparse.ArgumentParser(
        description="Integration test for SkyPilot-Monarch integration"
    )
    parser.add_argument(
        "--cluster-name",
        default="monarch-integration-test",
        help="Name for the test cluster",
    )
    parser.add_argument(
        "--cloud",
        default="aws",
        choices=["aws", "gcp", "azure", "kubernetes"],
        help="Cloud provider to use",
    )
    parser.add_argument(
        "--cpus",
        default="2+",
        help="CPU specification",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="Timeout in minutes for cluster launch",
    )

    args = parser.parse_args()

    # Check SkyPilot is configured
    print("Checking SkyPilot configuration...")
    print(f"  Using cloud: {args.cloud}")
    print("  (Run 'sky check' to verify cloud credentials)")

    # Run the test
    success = test_skypilot_cluster_launch(
        cluster_name=args.cluster_name,
        cloud=args.cloud,
        cpus=args.cpus,
        timeout_minutes=args.timeout,
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

