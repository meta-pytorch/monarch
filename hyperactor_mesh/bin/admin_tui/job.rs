/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use ratatui::text::Line;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::diagnostics::DiagResult;

/// An in-progress overlay-producing async job.
///
/// Exactly one variant is live at a time. `App` holds
/// `active_job: Option<ActiveJob>` which is always `Some` iff
/// `overlay` is `Some` (TUI-21).
pub(crate) enum ActiveJob {
    /// A streaming diagnostic run. `running` flips to `false` and
    /// `rx` to `None` when the mpsc sender closes.
    Diagnostics {
        results: Vec<DiagResult>,
        running: bool,
        rx: Option<mpsc::Receiver<DiagResult>>,
        completed_at: Option<String>,
    },
    /// A single py-spy HTTP fetch. `rx` is `None` once the oneshot
    /// has fired (results are in `overlay`).
    PySpy {
        rx: Option<oneshot::Receiver<Vec<Line<'static>>>>,
        short: String,
    },
}
