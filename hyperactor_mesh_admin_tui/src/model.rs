/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use hyperactor_mesh::introspect::NodePayload;
use hyperactor_mesh::introspect::NodeProperties;
use hyperactor_mesh::introspect::NodeRef;

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
/// See TUI-2 (cursor-bounds) in main.rs module doc.
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
    /// Typed node reference (identity in the admin API).
    pub(crate) reference: NodeRef,
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
    pub(crate) fn placeholder(reference: NodeRef) -> Self {
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
    pub(crate) fn placeholder_stopped(reference: NodeRef) -> Self {
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
    pub(crate) fn from_payload(reference: NodeRef, payload: &NodePayload) -> Self {
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

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use hyperactor_mesh::introspect::FailureInfo;
    use hyperactor_mesh::introspect::NodePayload;
    use hyperactor_mesh::introspect::NodeProperties;
    use hyperactor_mesh::introspect::NodeRef;

    use super::*;

    fn mock_actor_ref(name: &str) -> NodeRef {
        let proc_id = hyperactor::reference::ProcId::with_name(
            "unix:@test"
                .parse::<hyperactor::channel::ChannelAddr>()
                .unwrap(),
            "world",
        );
        NodeRef::Actor(proc_id.actor_id(name))
    }

    fn mock_proc_ref(name: &str) -> NodeRef {
        let proc_id = hyperactor::reference::ProcId::with_name(
            "unix:@test"
                .parse::<hyperactor::channel::ChannelAddr>()
                .unwrap(),
            name,
        );
        NodeRef::Proc(proc_id)
    }

    // Helper to create test payloads
    fn mock_payload(identity: NodeRef) -> NodePayload {
        NodePayload {
            identity,
            properties: NodeProperties::Actor {
                actor_status: "Running".to_string(),
                actor_type: "test".to_string(),
                messages_processed: 0,
                created_at: Some(SystemTime::UNIX_EPOCH),
                last_message_handler: None,
                total_processing_time_us: 0,
                flight_recorder: None,
                is_system: false,
                failure_info: None,
            },
            children: vec![],
            parent: None,
            as_of: SystemTime::now(),
        }
    }

    #[test]
    fn cursor_new_creates_valid_cursor() {
        let cursor = Cursor::new(10);
        assert_eq!(cursor.pos(), 0);
        assert_eq!(cursor.len(), 10);
    }

    #[test]
    fn cursor_new_empty_creates_zero_cursor() {
        let cursor = Cursor::new(0);
        assert_eq!(cursor.pos(), 0);
        assert_eq!(cursor.len(), 0);
    }

    #[test]
    fn cursor_move_up_at_start_returns_false() {
        let mut cursor = Cursor::new(5);
        assert!(!cursor.move_up());
        assert_eq!(cursor.pos(), 0);
    }

    #[test]
    fn cursor_move_up_from_middle_decrements() {
        let mut cursor = Cursor::new(5);
        cursor.set_pos(2);
        assert!(cursor.move_up());
        assert_eq!(cursor.pos(), 1);
    }

    #[test]
    fn cursor_move_down_at_end_returns_false() {
        let mut cursor = Cursor::new(5);
        cursor.set_pos(4);
        assert!(!cursor.move_down());
        assert_eq!(cursor.pos(), 4);
    }

    #[test]
    fn cursor_move_down_from_start_increments() {
        let mut cursor = Cursor::new(5);
        assert!(cursor.move_down());
        assert_eq!(cursor.pos(), 1);
    }

    #[test]
    fn cursor_home_at_start_returns_false() {
        let mut cursor = Cursor::new(5);
        assert!(!cursor.home());
        assert_eq!(cursor.pos(), 0);
    }

    #[test]
    fn cursor_home_from_middle_jumps_to_start() {
        let mut cursor = Cursor::new(5);
        cursor.set_pos(3);
        assert!(cursor.home());
        assert_eq!(cursor.pos(), 0);
    }

    #[test]
    fn cursor_end_at_end_returns_false() {
        let mut cursor = Cursor::new(5);
        cursor.set_pos(4);
        assert!(!cursor.end());
        assert_eq!(cursor.pos(), 4);
    }

    #[test]
    fn cursor_end_from_start_jumps_to_end() {
        let mut cursor = Cursor::new(5);
        assert!(cursor.end());
        assert_eq!(cursor.pos(), 4);
    }

    #[test]
    fn cursor_empty_all_movements_return_false() {
        let mut cursor = Cursor::new(0);
        assert!(!cursor.move_up());
        assert!(!cursor.move_down());
        assert!(!cursor.home());
        assert!(!cursor.end());
        assert_eq!(cursor.pos(), 0);
    }

    #[test]
    fn cursor_single_item_movements() {
        let mut cursor = Cursor::new(1);
        assert_eq!(cursor.pos(), 0);
        assert!(!cursor.move_up());
        assert!(!cursor.move_down());
        assert!(!cursor.home());
        assert!(!cursor.end());
        assert_eq!(cursor.pos(), 0);
    }

    #[test]
    fn cursor_update_len_expands_preserves_position() {
        let mut cursor = Cursor::new(5);
        cursor.set_pos(2);
        cursor.update_len(10);
        assert_eq!(cursor.pos(), 2);
        assert_eq!(cursor.len(), 10);
    }

    #[test]
    fn cursor_update_len_shrinks_clamps_position() {
        let mut cursor = Cursor::new(10);
        cursor.set_pos(8);
        cursor.update_len(5);
        assert_eq!(cursor.pos(), 4);
        assert_eq!(cursor.len(), 5);
    }

    #[test]
    fn cursor_update_len_to_zero_resets_position() {
        let mut cursor = Cursor::new(10);
        cursor.set_pos(5);
        cursor.update_len(0);
        assert_eq!(cursor.pos(), 0);
        assert_eq!(cursor.len(), 0);
    }

    #[test]
    fn cursor_set_pos_within_bounds_works() {
        let mut cursor = Cursor::new(10);
        cursor.set_pos(7);
        assert_eq!(cursor.pos(), 7);
    }

    #[test]
    fn cursor_set_pos_beyond_bounds_clamps() {
        let mut cursor = Cursor::new(5);
        cursor.set_pos(10);
        assert_eq!(cursor.pos(), 4);
    }

    #[test]
    fn cursor_set_pos_on_empty_stays_zero() {
        let mut cursor = Cursor::new(0);
        cursor.set_pos(5);
        assert_eq!(cursor.pos(), 0);
    }

    #[test]
    fn cursor_maintains_invariant_after_operations() {
        let mut cursor = Cursor::new(5);
        cursor.move_down();
        cursor.move_down();
        assert!(cursor.pos() < cursor.len());
        cursor.update_len(2);
        assert!(cursor.pos() < cursor.len());
        cursor.set_pos(100);
        assert!(cursor.pos() < cursor.len());
        cursor.update_len(0);
        assert_eq!(cursor.pos(), 0);
    }

    #[test]
    fn cursor_update_after_empty_tree() {
        let mut cursor = Cursor::new(5);
        cursor.set_pos(2);
        cursor.update_len(0);
        assert_eq!(cursor.len(), 0);
        assert_eq!(cursor.pos(), 0);
        assert!(!cursor.move_down());
        assert!(!cursor.move_up());
        assert!(!cursor.home());
        assert!(!cursor.end());
    }

    #[test]
    fn cursor_navigation_large_list() {
        let mut cursor = Cursor::new(100000);
        cursor.end();
        assert_eq!(cursor.pos(), 99999);
        cursor.home();
        assert_eq!(cursor.pos(), 0);
        for i in 0..1000 {
            cursor.set_pos(i * 50);
        }
        assert!(cursor.pos() < cursor.len());
    }

    #[test]
    fn cursor_update_when_rows_shrink_to_zero() {
        let mut cursor = Cursor::new(1);
        cursor.set_pos(0);
        cursor.update_len(0);
        assert_eq!(cursor.pos(), 0);
        assert_eq!(cursor.len(), 0);
    }

    #[test]
    fn placeholder_stopped_differs_only_in_stopped_field() {
        let r = mock_actor_ref("actor1");
        let normal = TreeNode::placeholder(r.clone());
        let stopped = TreeNode::placeholder_stopped(r);
        assert_eq!(normal.reference, stopped.reference);
        assert_eq!(normal.label, stopped.label);
        assert!(matches!(normal.node_type, NodeType::Actor));
        assert!(matches!(stopped.node_type, NodeType::Actor));
        assert_eq!(normal.expanded, stopped.expanded);
        assert_eq!(normal.fetched, stopped.fetched);
        assert_eq!(normal.has_children, stopped.has_children);
        assert_eq!(normal.children.len(), stopped.children.len());
        assert!(!normal.stopped);
        assert!(stopped.stopped);
        assert!(!stopped.fetched);
        assert!(stopped.has_children);
    }

    #[test]
    fn from_payload_sets_stopped_for_stopped_actor() {
        let r = mock_actor_ref("actor1");
        let payload = NodePayload {
            identity: r.clone(),
            properties: NodeProperties::Actor {
                actor_status: "stopped:done".to_string(),
                actor_type: "test".to_string(),
                messages_processed: 5,
                created_at: Some(SystemTime::UNIX_EPOCH),
                last_message_handler: None,
                total_processing_time_us: 0,
                flight_recorder: None,
                is_system: false,
                failure_info: None,
            },
            children: vec![],
            parent: None,
            as_of: SystemTime::now(),
        };
        let node = TreeNode::from_payload(r, &payload);
        assert!(node.stopped);
    }

    #[test]
    fn from_payload_not_stopped_for_running_actor() {
        let r = mock_actor_ref("actor1");
        let payload = mock_payload(r.clone());
        let node = TreeNode::from_payload(r, &payload);
        assert!(!node.stopped);
    }

    #[test]
    fn from_payload_sets_is_system_for_system_actor() {
        let r = mock_actor_ref("host_agent");
        let payload = NodePayload {
            identity: r.clone(),
            properties: NodeProperties::Actor {
                actor_status: "idle".to_string(),
                actor_type: "hyperactor_mesh::proc_agent::ProcAgent".to_string(),
                messages_processed: 10,
                created_at: Some(SystemTime::UNIX_EPOCH),
                last_message_handler: None,
                total_processing_time_us: 0,
                flight_recorder: None,
                is_system: true,
                failure_info: None,
            },
            children: vec![],
            parent: None,
            as_of: SystemTime::now(),
        };
        let node = TreeNode::from_payload(r, &payload);
        assert!(node.is_system);
        assert!(!node.stopped);
    }

    #[test]
    fn from_payload_not_system_for_user_actor() {
        let r = mock_actor_ref("worker");
        let payload = mock_payload(r.clone());
        let node = TreeNode::from_payload(r, &payload);
        assert!(!node.is_system);
    }

    #[test]
    fn from_payload_proc_is_never_stopped() {
        let r = mock_proc_ref("proc1");
        let payload = NodePayload {
            identity: r.clone(),
            properties: NodeProperties::Proc {
                proc_name: "proc1".to_string(),
                num_actors: 0,
                system_children: vec![],
                stopped_children: vec![mock_actor_ref("x")],
                stopped_retention_cap: 10,
                is_poisoned: false,
                failed_actor_count: 0,
                debug: Default::default(),
            },
            children: vec![],
            parent: None,
            as_of: SystemTime::now(),
        };
        let node = TreeNode::from_payload(r, &payload);
        assert!(!node.stopped);
    }

    #[test]
    fn from_payload_sets_failed_for_actor_with_failure_info() {
        let r = mock_actor_ref("actor1");
        let worker_id = {
            hyperactor::reference::ProcId::with_name(
                "unix:@test"
                    .parse::<hyperactor::channel::ChannelAddr>()
                    .unwrap(),
                "world",
            )
            .actor_id("worker")
        };
        let payload = NodePayload {
            identity: r.clone(),
            properties: NodeProperties::Actor {
                actor_status: "failed:panic".to_string(),
                actor_type: "test".to_string(),
                messages_processed: 0,
                created_at: Some(SystemTime::UNIX_EPOCH),
                last_message_handler: None,
                total_processing_time_us: 0,
                flight_recorder: None,
                is_system: false,
                failure_info: Some(FailureInfo {
                    error_message: "GPU memory corruption".to_string(),
                    root_cause_actor: worker_id,
                    root_cause_name: Some("worker".to_string()),
                    occurred_at: SystemTime::UNIX_EPOCH,
                    is_propagated: false,
                }),
            },
            children: vec![],
            parent: None,
            as_of: SystemTime::now(),
        };
        let node = TreeNode::from_payload(r, &payload);
        assert!(node.failed);
        assert!(node.stopped);
    }

    #[test]
    fn from_payload_not_failed_without_failure_info() {
        let r = mock_actor_ref("actor1");
        let payload = mock_payload(r.clone());
        let node = TreeNode::from_payload(r, &payload);
        assert!(!node.failed);
    }

    #[test]
    fn from_payload_or_logic_both_sources_agree() {
        let r = mock_actor_ref("actor1");
        let payload = NodePayload {
            identity: r.clone(),
            properties: NodeProperties::Actor {
                actor_status: "stopped:done".to_string(),
                actor_type: "test".to_string(),
                messages_processed: 0,
                created_at: Some(SystemTime::UNIX_EPOCH),
                last_message_handler: None,
                total_processing_time_us: 0,
                flight_recorder: None,
                is_system: false,
                failure_info: None,
            },
            children: vec![],
            parent: None,
            as_of: SystemTime::now(),
        };
        let mut node = TreeNode::from_payload(r, &payload);
        let child_is_stopped = true;
        node.stopped = node.stopped || child_is_stopped;
        assert!(node.stopped);
    }

    #[test]
    fn from_payload_or_logic_only_proc_list() {
        let r = mock_actor_ref("actor1");
        let payload = mock_payload(r.clone());
        let mut node = TreeNode::from_payload(r, &payload);
        assert!(!node.stopped);
        let child_is_stopped = true;
        node.stopped = node.stopped || child_is_stopped;
        assert!(node.stopped);
    }

    #[test]
    fn from_payload_or_logic_only_cached_payload() {
        let r = mock_actor_ref("actor1");
        let payload = NodePayload {
            identity: r.clone(),
            properties: NodeProperties::Actor {
                actor_status: "failed:panic".to_string(),
                actor_type: "test".to_string(),
                messages_processed: 0,
                created_at: Some(SystemTime::UNIX_EPOCH),
                last_message_handler: None,
                total_processing_time_us: 0,
                flight_recorder: None,
                is_system: false,
                failure_info: None,
            },
            children: vec![],
            parent: None,
            as_of: SystemTime::now(),
        };
        let mut node = TreeNode::from_payload(r, &payload);
        assert!(node.stopped);
        let child_is_stopped = false;
        node.stopped = node.stopped || child_is_stopped;
        assert!(node.stopped);
    }
}
