/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/// Result of handling a key event.
#[derive(Debug)]
pub(crate) enum KeyResult {
    /// Nothing changed.
    None,
    /// Selection or expand/collapse changed; update detail from cache.
    DetailChanged,
    /// A filter/view setting changed; full tree refresh needed.
    NeedsRefresh,
    /// Lazily expand the node at the given (reference, depth).
    ExpandNode(String, usize),
    /// Start the self-diagnostic suite against the attached mesh.
    RunDiagnostics,
    /// Fetch a py-spy stack dump for the given proc reference.
    RunPySpy(String),
    /// Fetch the config dump for the given proc reference.
    RunConfig(String),
}
