# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

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
