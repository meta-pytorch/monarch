/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Reusable scrollable overlay for the detail pane.
//!
//! An `Overlay` takes over the detail pane with scrollable text
//! content, dismissed with Esc, scrolled with j/k. Used by
//! diagnostics, py-spy, and future overlays (e.g. config display).

use std::cell::Cell;

use ratatui::text::Line;

/// A scrollable content overlay that takes over the detail pane.
pub(crate) struct Overlay {
    /// Title shown in the overlay border.
    pub title: Line<'static>,
    /// Optional pinned status line above the scrollable content.
    pub status_line: Option<Line<'static>>,
    /// Scrollable content lines.
    pub lines: Vec<Line<'static>>,
    /// True while content is still loading.
    pub loading: bool,
    /// Vertical scroll offset.
    pub scroll: Cell<u16>,
    /// Max scroll (computed during render).
    pub max_scroll: Cell<u16>,
}

impl Overlay {
    /// Scroll up by one line.
    pub fn scroll_up(&self) {
        self.scroll.set(self.scroll.get().saturating_sub(1));
    }

    /// Scroll down by one line, bounded by max_scroll.
    pub fn scroll_down(&self) {
        self.scroll.set(
            self.scroll
                .get()
                .saturating_add(1)
                .min(self.max_scroll.get()),
        );
    }
}
