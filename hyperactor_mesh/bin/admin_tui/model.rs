/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use hyperactor::introspect::NodePayload;
use hyperactor::introspect::NodeProperties;

use crate::filter::is_failed_node;
use crate::filter::is_stopped_node;
use crate::filter::is_system_node;
use crate::format::derive_label;
use crate::format::derive_label_from_ref;

/// Maximum recursion depth when walking references.
/// Root(skipped) → Host(0) → Proc(1) → Actor(2) → ChildActor(3).
pub(crate) const MAX_TREE_DEPTH: usize = 4;

/// Navigation cursor over a bounded list.
///
/// Invariant: `pos < len` (or `pos == 0` when `len == 0`).
/// Movement methods return `true` when the position changes.
#[derive(Debug, Clone)]
pub(crate) struct Cursor {
    /// Current position within the list.
    pos: usize,
    /// Current length of the list.
    len: usize,
}

impl Cursor {
    /// Create a new cursor for a list of the given length.
    ///
    /// Position starts at 0. If `len == 0`, position is 0 (no valid
    /// selection).
    pub(crate) fn new(len: usize) -> Self {
        Self { pos: 0, len }
    }

    /// Move up (decrement). Returns true if position changed.
    pub(crate) fn move_up(&mut self) -> bool {
        if self.pos > 0 {
            self.pos -= 1;
            true
        } else {
            false
        }
    }

    /// Move down (increment). Returns true if position changed.
    pub(crate) fn move_down(&mut self) -> bool {
        if self.pos + 1 < self.len {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    /// Jump to start. Returns true if position changed.
    pub(crate) fn home(&mut self) -> bool {
        if self.pos != 0 {
            self.pos = 0;
            true
        } else {
            false
        }
    }

    /// Jump to end. Returns true if position changed.
    pub(crate) fn end(&mut self) -> bool {
        let new_pos = self.len.saturating_sub(1);
        if self.pos != new_pos {
            self.pos = new_pos;
            true
        } else {
            false
        }
    }

    /// Page down by `amount`. Returns true if position changed.
    pub(crate) fn page_down(&mut self, amount: usize) -> bool {
        let new_pos = (self.pos + amount).min(self.len.saturating_sub(1));
        if self.pos != new_pos {
            self.pos = new_pos;
            true
        } else {
            false
        }
    }

    /// Page up by `amount`. Returns true if position changed.
    pub(crate) fn page_up(&mut self, amount: usize) -> bool {
        let new_pos = self.pos.saturating_sub(amount);
        if self.pos != new_pos {
            self.pos = new_pos;
            true
        } else {
            false
        }
    }

    /// Update length and clamp position to remain valid.
    ///
    /// Used after tree mutations (refresh, collapse) to maintain the
    /// cursor invariant.
    pub(crate) fn update_len(&mut self, new_len: usize) {
        self.len = new_len;
        if new_len == 0 {
            self.pos = 0;
        } else {
            self.pos = self.pos.min(new_len - 1);
        }
    }

    /// Set position directly (for restoring saved selection).
    ///
    /// Clamps to valid range.
    pub(crate) fn set_pos(&mut self, new_pos: usize) {
        if self.len == 0 {
            self.pos = 0;
        } else {
            self.pos = new_pos.min(self.len - 1);
        }
    }

    /// Get current position.
    pub(crate) fn pos(&self) -> usize {
        self.pos
    }

    /// Get current length.
    #[allow(dead_code)] // used by tests
    pub(crate) fn len(&self) -> usize {
        self.len
    }
}

/// Lightweight classification for a topology node, used for UI
/// concerns (primarily color-coding and a few display heuristics).
///
/// This is derived from the node's [`NodeProperties`] variant rather
/// than being persisted in the payload.
#[derive(Debug, Clone, Copy)]
pub(crate) enum NodeType {
    /// Synthetic root of the admin tree (not rendered as a row; hosts
    /// appear at depth 0).
    Root,
    /// A host in the mesh, identified by its admin-reported address.
    Host,
    /// A proc running on a host (system or user).
    Proc,
    /// An actor instance within a proc.
    Actor,
}

impl NodeType {
    /// Classify a node for UI purposes by mapping from its
    /// [`NodeProperties`] variant.
    ///
    /// This is a lossy projection: it preserves only the high-level
    /// kind (root/host/proc/actor), not the detailed fields (e.g.,
    /// `is_system`, counts, status).
    pub(crate) fn from_properties(props: &NodeProperties) -> Self {
        match props {
            NodeProperties::Root { .. } => NodeType::Root,
            NodeProperties::Host { .. } => NodeType::Host,
            NodeProperties::Proc { .. } => NodeType::Proc,
            NodeProperties::Actor { .. } => NodeType::Actor,
            NodeProperties::Error { .. } => NodeType::Actor,
        }
    }

    /// Short human-readable label for display.
    pub(crate) fn label(&self) -> &'static str {
        match self {
            NodeType::Root => "root",
            NodeType::Host => "host",
            NodeType::Proc => "proc",
            NodeType::Actor => "actor",
        }
    }
}

/// A node in the topology tree.
///
/// Represents the actual tree structure (not a flattened view).
/// The tree is materialized from the admin API by walking references
/// recursively, respecting `expanded_keys` and depth limits.
#[derive(Debug, Clone)]
pub(crate) struct TreeNode {
    /// Opaque reference string for this node (identity in the admin
    /// API).
    pub(crate) reference: String,
    /// Human-friendly label shown in the tree (derived from
    /// [`NodePayload`]).
    pub(crate) label: String,
    /// Node type for color coding.
    pub(crate) node_type: NodeType,
    /// Whether this node is currently expanded in the UI.
    pub(crate) expanded: bool,
    /// Whether this node's own payload has been fetched (as opposed to
    /// being a placeholder derived from a parent's children list).
    pub(crate) fetched: bool,
    /// Whether the backing payload reports any children (controls
    /// fold arrow rendering).
    pub(crate) has_children: bool,
    /// Whether this actor is stopped/failed (terminal state).
    pub(crate) stopped: bool,
    /// Whether this actor failed (as opposed to stopping cleanly).
    /// When true, renders in red instead of gray.
    pub(crate) failed: bool,
    /// Whether this is a system/infrastructure actor or proc.
    pub(crate) is_system: bool,
    /// Direct children of this node in the tree.
    pub(crate) children: Vec<TreeNode>,
}

impl TreeNode {
    /// Create a placeholder node (not yet fetched).
    ///
    /// Placeholders are created from parent children lists without
    /// fetching payload. They have `fetched: false`, `has_children:
    /// true`, and empty `children` vector.
    pub(crate) fn placeholder(reference: String) -> Self {
        Self {
            label: derive_label_from_ref(&reference),
            reference,
            node_type: NodeType::Actor,
            expanded: false,
            fetched: false,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: Vec::new(),
        }
    }

    /// Create a placeholder for a known-stopped actor.
    ///
    /// Like `placeholder` but with `stopped: true` so the node
    /// renders gray and can be filtered without a child fetch.
    pub(crate) fn placeholder_stopped(reference: String) -> Self {
        Self {
            label: derive_label_from_ref(&reference),
            reference,
            node_type: NodeType::Actor,
            expanded: false,
            fetched: false,
            has_children: true,
            stopped: true,
            failed: false,
            is_system: false,
            children: Vec::new(),
        }
    }

    /// Create a collapsed, fetched node from a payload.
    ///
    /// Used when building child lists from cached or freshly fetched
    /// payloads. The node starts collapsed with no children.
    pub(crate) fn from_payload(reference: String, payload: &NodePayload) -> Self {
        Self {
            label: derive_label(payload),
            node_type: NodeType::from_properties(&payload.properties),
            expanded: false,
            fetched: true,
            has_children: !payload.children.is_empty(),
            stopped: is_stopped_node(&payload.properties),
            failed: is_failed_node(&payload.properties),
            is_system: is_system_node(&payload.properties),
            children: Vec::new(),
            reference,
        }
    }
}

/// A single row in the flattened UI view.
///
/// Ephemeral structure computed by `flatten_visible` for rendering.
#[derive(Debug, Clone)]
pub(crate) struct FlatRow<'a> {
    /// Reference to the tree node backing this row.
    pub(crate) node: &'a TreeNode,
    /// Visual indentation level for this row.
    pub(crate) depth: usize,
}

/// Wrapper for flattened visible rows with cursor helpers.
///
/// Makes the "ephemeral view" concept explicit and provides safe
/// cursor-based access.
#[derive(Debug)]
pub(crate) struct VisibleRows<'a> {
    rows: Vec<FlatRow<'a>>,
}

impl<'a> VisibleRows<'a> {
    pub(crate) fn new(rows: Vec<FlatRow<'a>>) -> Self {
        Self { rows }
    }

    pub(crate) fn get(&self, cursor: &Cursor) -> Option<&FlatRow<'a>> {
        self.rows.get(cursor.pos())
    }

    pub(crate) fn len(&self) -> usize {
        self.rows.len()
    }

    pub(crate) fn as_slice(&self) -> &[FlatRow<'a>] {
        &self.rows
    }

    /// Check whether a later row at the same depth exists (for tree
    /// connector rendering: `├─` vs `└─`).
    pub(crate) fn has_sibling_after(&self, idx: usize, depth: usize) -> bool {
        for row in &self.rows[idx + 1..] {
            if row.depth < depth {
                return false;
            }
            if row.depth == depth {
                return true;
            }
        }
        false
    }
}
