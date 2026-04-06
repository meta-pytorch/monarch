#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Common setup functions for Monarch macOS CI workflows

set -ex

install_macos_base_dependencies() {
    export HOMEBREW_NO_AUTO_UPDATE=1
    brew install protobuf
    python -m pip install --upgrade pip
}

install_macos_python_test_dependencies() {
    python -m pip install -r python/tests/requirements.txt
    python -m pip install --pre torch --extra-index-url https://download.pytorch.org/whl/nightly/cpu
}

install_macos_rust_test_dependencies() {
    cargo install cargo-nextest --locked
}
