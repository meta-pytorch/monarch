/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/// Signals that a queue may contain work.
///
/// Notifications are coalesced, may be spurious, carry no payload, and must return promptly.
/// Implementations must not reenter Chrysalis.
pub trait Notifier: Send + Sync + 'static {
    /// Signals a queue state transition.
    fn notify(&self);
}

/// A notifier for callers that continuously poll a queue.
#[derive(Debug, Default)]
pub struct NoopNotifier;

impl Notifier for NoopNotifier {
    fn notify(&self) {}
}
