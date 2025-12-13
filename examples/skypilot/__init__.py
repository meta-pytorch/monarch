"""
SkyPilot integration for Monarch.

This is a standalone package that provides SkyPilotJob - a way to run Monarch
workloads on Kubernetes and cloud VMs via SkyPilot.

This package is separate from the main Monarch codebase to allow independent
iteration and to avoid chicken-and-egg problems with releases.

Usage:
    from skypilot_job import SkyPilotJob
    
    job = SkyPilotJob(
        meshes={"workers": 2},
        resources=sky.Resources(cloud=sky.Kubernetes(), accelerators="H100:1"),
    )
    state = job.state()
"""

from .skypilot_job import SkyPilotJob

__all__ = ["SkyPilotJob"]

