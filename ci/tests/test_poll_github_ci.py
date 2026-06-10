# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import unittest

import monarch.ci.poll_github_ci as poll_github_ci


def _check_run(
    name: str,
    status: str = "COMPLETED",
    conclusion: str | None = "SUCCESS",
) -> dict[str, str | None]:
    return {"name": name, "status": status, "conclusion": conclusion}


class PollGithubCITest(unittest.TestCase):
    def test_latest_version_link_selects_newest_matching_link(self) -> None:
        prs = [
            {
                "number": 4153,
                "phabricator_version_links": {
                    "nodes": [
                        {
                            "creation_time": 10,
                            "head_sha": "old",
                            "phabricator_version": {"id": "123"},
                        },
                        {
                            "creation_time": 20,
                            "head_sha": "new",
                            "phabricator_version": {"id": "123"},
                        },
                    ]
                },
            }
        ]

        match = poll_github_ci._latest_version_link(prs, "123")

        self.assertIsNotNone(match)
        _, link = match
        self.assertEqual("new", link["head_sha"])

    def test_matching_check_runs_filters_to_github_actions_head_sha(self) -> None:
        pr = {
            "test_check_runs": [
                {
                    "name": "Build CPU / Build CPU - No Tensor Engine / linux-job",
                    "app_name": "GitHub Actions",
                    "sha": "head",
                },
                {
                    "name": "old Build CPU",
                    "app_name": "GitHub Actions",
                    "sha": "old",
                },
                {
                    "name": "internal",
                    "app_name": "Sandcastle",
                    "sha": "head",
                },
            ]
        }

        check_runs = poll_github_ci._matching_check_runs(pr, "head")

        self.assertEqual(
            ["Build CPU / Build CPU - No Tensor Engine / linux-job"],
            [check_run["name"] for check_run in check_runs],
        )

    def test_real_check_runs_ignore_non_blocking_checks(self) -> None:
        check_runs = [
            _check_run("Status Check", conclusion="FAILURE"),
            _check_run("deploy", conclusion="FAILURE"),
            _check_run("Build CPU (macOS)", conclusion="FAILURE"),
            _check_run(
                "Build Docker image / Build docker container", conclusion="FAILURE"
            ),
            _check_run("Test CPU Python (macOS)", conclusion="FAILURE"),
            _check_run("Test CPU Rust (macOS)", conclusion="FAILURE"),
            _check_run("Build GPU / set-matrix / set"),
            _check_run("Build GPU / Build cuda (py3.10) / linux-job"),
        ]

        self.assertEqual(
            ["Build GPU / Build cuda (py3.10) / linux-job"],
            [c["name"] for c in poll_github_ci._real_check_runs(check_runs)],
        )

    def test_check_run_classification_uses_real_required_jobs(self) -> None:
        check_runs = [
            _check_run("Build CPU / Build CPU - No Tensor Engine / linux-job"),
            _check_run("Build Documentation / linux-job"),
            _check_run("Build GPU / Build cuda (py3.10) / linux-job"),
            _check_run(
                "Test CPU Python / Test CPU Python - No Tensor Engine / linux-job"
            ),
            _check_run("Test CPU Rust / Test CPU Rust (x86_64) / linux-job"),
            _check_run("Test GPU Python / Test GPU Python (cuda-py3.10) / linux-job"),
            _check_run("Test GPU Rust / Test GPU Rust (cuda-py3.10) / linux-job"),
            _check_run("Type Check Python / Type Check Python (pyright) / linux-job"),
            _check_run("Status Check", conclusion="FAILURE"),
            _check_run("extra pending job", status="IN_PROGRESS", conclusion=None),
            _check_run("extra failed job", conclusion="FAILURE"),
        ]
        real_check_runs = poll_github_ci._real_check_runs(check_runs)

        self.assertEqual(
            [], poll_github_ci._missing_required_check_runs(real_check_runs)
        )
        self.assertEqual(
            ["extra pending job"],
            [c["name"] for c in poll_github_ci._pending_check_runs(real_check_runs)],
        )
        self.assertEqual(
            ["extra failed job"],
            [c["name"] for c in poll_github_ci._failed_check_runs(real_check_runs)],
        )
