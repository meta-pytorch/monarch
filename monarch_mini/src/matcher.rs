/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::VecDeque;

use crate::connection::ConnectionRef;

pub(crate) struct Matcher<Left, Right> {
    left: VecDeque<Left>,
    right: VecDeque<Right>,
}

impl<Left, Right> Matcher<Left, Right> {
    pub(crate) fn new() -> Self {
        Self {
            left: VecDeque::new(),
            right: VecDeque::new(),
        }
    }

    pub(crate) fn push_left<Output>(
        &mut self,
        left: Left,
        on_match: impl FnOnce(Left, Right) -> Output,
    ) -> Option<Output> {
        let Some(right) = self.right.pop_front() else {
            self.left.push_back(left);
            return None;
        };
        Some(on_match(left, right))
    }

    pub(crate) fn push_right<Output>(
        &mut self,
        right: Right,
        on_match: impl FnOnce(Left, Right) -> Output,
    ) -> Option<Output> {
        let Some(left) = self.left.pop_front() else {
            self.right.push_back(right);
            return None;
        };
        Some(on_match(left, right))
    }
}

pub(crate) type InprocMatcher = Matcher<ConnectionRef, ConnectionRef>;
