"""
Monarch SkyPilot Integration Package.

This package provides SkyPilotJob - a way to run Monarch workloads on
Kubernetes and cloud VMs via SkyPilot.

Usage:
    from monarch_skypilot import SkyPilotJob
    
    job = SkyPilotJob(
        meshes={"workers": 2},
        resources=sky.Resources(cloud=sky.Kubernetes(), accelerators="H100:1"),
    )
    state = job.state()
"""

from .skypilot_job import SkyPilotJob

__all__ = ["SkyPilotJob"]

