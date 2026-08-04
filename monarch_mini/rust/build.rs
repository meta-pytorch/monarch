/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Links minimonarch as an opaque native library over its C ABI.
//!
//! minimonarch is declared as a `staticlib` artifact dependency (see
//! Cargo.toml), so cargo builds its self-contained `libmonarch_mini-*.a` — whose
//! only exported symbols are the `mm_*` C ABI, with its std and tokio bundled
//! and internalized — and hands us its path via an env var. We link that archive
//! directly. The outer program cannot tell minimonarch is implemented in Rust
//! and shares no runtime state with it (own allocator, panic runtime, and
//! thread-locals). This is the same archive the Python and C consumers link.

use std::path::PathBuf;

fn main() {
    // cargo (with -Z bindeps) sets this to the built `.a` for the staticlib
    // artifact dependency `monarch_mini`.
    let archive = PathBuf::from(
        std::env::var("CARGO_STATICLIB_FILE_MONARCH_MINI_monarch_mini")
            .or_else(|_| std::env::var("CARGO_STATICLIB_FILE_MONARCH_MINI"))
            .expect("cargo should provide the monarch_mini staticlib artifact path"),
    );
    let dir = archive
        .parent()
        .expect("artifact path should have a parent directory");
    // The artifact is named `libmonarch_mini-<hash>.a`, so link it by its exact
    // file name via the `verbatim` modifier rather than the `-lmonarch_mini`
    // naming convention.
    let file_name = archive
        .file_name()
        .expect("artifact path should have a file name")
        .to_str()
        .expect("artifact file name should be valid UTF-8");

    println!("cargo:rustc-link-search=native={}", dir.display());
    println!("cargo:rustc-link-lib=static:+verbatim={file_name}");
    println!("cargo:rerun-if-changed=build.rs");
}
