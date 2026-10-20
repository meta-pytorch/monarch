# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from prepare_release import main, prepare_release


class PrepareReleaseTest(unittest.TestCase):
    def test_automated_builds_are_always_nightly(self) -> None:
        for event in ("schedule", "pull_request", "push"):
            with self.subTest(event=event):
                self.assertEqual(
                    prepare_release(event, "Official release", "0.8.0"),
                    ("nightly", ""),
                )

    def test_nightly_requires_an_empty_version(self) -> None:
        self.assertEqual(
            prepare_release("workflow_dispatch", "Nightly", ""), ("nightly", "")
        )
        with self.assertRaises(ValueError):
            prepare_release("workflow_dispatch", "Nightly", "0.8.0")

    def test_official_release_versions(self) -> None:
        for version in ("0.8.0", "0.8.0.post1"):
            with self.subTest(version=version):
                self.assertEqual(
                    prepare_release("workflow_dispatch", "Official release", version),
                    ("release", version),
                )

    def test_official_prerelease_versions(self) -> None:
        for version in ("0.8.0a1", "0.8.0b1", "0.8.0rc1"):
            with self.subTest(version=version):
                self.assertEqual(
                    prepare_release(
                        "workflow_dispatch", "Official prerelease", version
                    ),
                    ("prerelease", version),
                )

    def test_mismatched_versions_are_rejected(self) -> None:
        for release_type, version in (
            ("Official release", "0.8.0rc1"),
            ("Official prerelease", "0.8.0"),
            ("Official release", ""),
            ("Official prerelease", ""),
            ("Official release", "0.8.0.dev1"),
            ("Official prerelease", "0.8.0rc1.dev1"),
            ("Official release", "0.8.0+local"),
            ("Official release", "1!0.8.0"),
            ("Official release", '0.8.0"; exit 0'),
            ("Official release", "0.8.0\nrelease_type=nightly"),
            ("unknown", "0.8.0"),
        ):
            with self.subTest(release_type=release_type, version=version):
                with self.assertRaises(ValueError):
                    prepare_release("workflow_dispatch", release_type, version)

    def test_version_is_normalized(self) -> None:
        self.assertEqual(
            prepare_release("workflow_dispatch", "Official prerelease", " v0.8.0-RC1 "),
            ("prerelease", "0.8.0rc1"),
        )

    def test_outputs_are_written_only_after_validation(self) -> None:
        with TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            env = {
                "GITHUB_EVENT_NAME": "workflow_dispatch",
                "RELEASE_TYPE": "Official release",
                "RELEASE_VERSION": "0.8.0rc1",
                "GITHUB_OUTPUT": str(output),
            }
            with patch.dict(os.environ, env):
                with self.assertRaises(SystemExit):
                    main()
                self.assertFalse(output.exists())
                os.environ["RELEASE_VERSION"] = "0.8.0"
                main()
                self.assertEqual(
                    output.read_text(), "release_type=release\nversion=0.8.0\n"
                )


if __name__ == "__main__":
    unittest.main()
