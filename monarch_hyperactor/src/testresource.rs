/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![cfg(test)]

use std::path::PathBuf;

/// Fetch the named (BUCK) named resource, heuristically falling back on
/// the cargo-built path when possible. Beware! This is not actually a
/// true cargo dependency, so the binaries have to be built independently.
///
/// We should convert these tests to integration tests, so that cargo can
/// also manage the binaries.
pub fn get<S>(name: S) -> PathBuf
where
    S: AsRef<str>,
{
    let name = name.as_ref().to_owned();
    // TODO: actually check if we're running in Buck context or not.
    if let Ok(path) = buck_resources::get(name.clone()) {
        return path;
    }

    assert!(
        name.starts_with("monarch/monarch_hyperactor/"),
        "invalid resource {}: must start with \"monarch/monarch_hyperactor/\"",
        name
    );

    let path: PathBuf = name
        .replace(
            "monarch/monarch_hyperactor/",
            "../target/debug/monarch_hyperactor_test_",
        )
        .into();

    assert!(
        path.exists(),
        "no cargo-built resource at {}",
        path.display()
    );

    path
}
