/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Invariants:
//!
//! - **TR-1 (fold-result-safety):** In `find_node_mut` and
//!   `find_node_at_depth_mut`, `result` is only set when the
//!   callback returns `Break`. This is verified by a
//!   `debug_assert_eq!(result.is_some(), flow.is_break())` after
//!   the fold completes. The raw pointer is valid for the input
//!   lifetime because the fold visits each node exactly once and
//!   we break immediately after capturing the pointer.

use std::collections::HashSet;

use hyperactor_mesh::introspect::NodeRef;

use crate::model::FlatRow;
use crate::model::TreeNode;

/// Flatten a tree into visible rows using algebraic fold.
///
/// Only expanded nodes contribute their children. This replaces
/// the old `visible_indices()` logic.
pub(crate) fn flatten_tree(root: &TreeNode) -> Vec<FlatRow<'_>> {
    root.children
        .iter()
        .flat_map(|child| flatten_visible(child, 0))
        .collect()
}

/// Flatten visible nodes using fold_tree_with_depth.
///
/// Includes current node and recursively includes children only if
/// current node is expanded.
pub(crate) fn flatten_visible<'a>(node: &'a TreeNode, depth: usize) -> Vec<FlatRow<'a>> {
    fold_tree_with_depth(node, depth, &|n, d, child_results| {
        let mut rows = vec![FlatRow { node: n, depth: d }];
        if n.expanded {
            for child_rows in child_results {
                rows.extend(child_rows);
            }
        }
        rows
    })
}

/// Generic tree fold - unified traversal abstraction.
///
/// Applies `f` to each node in pre-order DFS, accumulating a result.
/// The function receives the current node and the accumulated results
/// from children.
pub(crate) fn fold_tree<'a, B, F>(node: &'a TreeNode, f: &F) -> B
where
    F: Fn(&'a TreeNode, Vec<B>) -> B,
{
    let child_results: Vec<B> = node
        .children
        .iter()
        .map(|child| fold_tree(child, f))
        .collect();
    f(node, child_results)
}

/// Immutable tree fold with depth tracking.
///
/// Like fold_tree, but passes the current depth to the fold function.
/// Applies `f` to each (node, depth) in pre-order DFS, accumulating results.
pub(crate) fn fold_tree_with_depth<'a, B, F>(node: &'a TreeNode, depth: usize, f: &F) -> B
where
    F: Fn(&'a TreeNode, usize, Vec<B>) -> B,
{
    let child_results: Vec<B> = node
        .children
        .iter()
        .map(|child| fold_tree_with_depth(child, depth + 1, f))
        .collect();
    f(node, depth, child_results)
}

/// Mutable tree fold with early-exit via ControlFlow.
///
/// Applies `f` to each node in pre-order DFS with mutable access.
/// Returns ControlFlow::Break to stop traversal early, or Continue
/// to proceed. This enables algebraic mutable traversals with short-circuiting.
pub(crate) fn fold_tree_mut<B, F>(node: &mut TreeNode, f: &mut F) -> std::ops::ControlFlow<B>
where
    F: for<'a> FnMut(&'a mut TreeNode) -> std::ops::ControlFlow<B>,
{
    // Check current node first
    f(node)?;

    // Then traverse children
    for child in &mut node.children {
        fold_tree_mut(child, f)?;
    }

    std::ops::ControlFlow::Continue(())
}

/// Mutable tree fold with depth tracking and early-exit via ControlFlow.
///
/// Like fold_tree_mut, but passes the current depth to the closure.
/// Applies `f` to each (node, depth) in pre-order DFS with mutable access.
pub(crate) fn fold_tree_mut_with_depth<B, F>(
    node: &mut TreeNode,
    depth: usize,
    f: &mut F,
) -> std::ops::ControlFlow<B>
where
    F: for<'a> FnMut(&'a mut TreeNode, usize) -> std::ops::ControlFlow<B>,
{
    // Check current node first
    f(node, depth)?;

    // Then traverse children at depth + 1
    for child in &mut node.children {
        fold_tree_mut_with_depth(child, depth + 1, f)?;
    }

    std::ops::ControlFlow::Continue(())
}

// Mutable tree traversals using algebraic fold.

/// Find a node by reference using fold_tree_mut (mutable).
///
/// Uses raw pointer to escape closure lifetime, which is safe because:
/// 1. fold_tree_mut visits each node exactly once
/// 2. We Break immediately after finding the match
/// 3. The pointer is valid for the input lifetime 'a
#[allow(dead_code)] // used by tests
pub(crate) fn find_node_mut<'a>(
    node: &'a mut TreeNode,
    reference: &NodeRef,
) -> Option<&'a mut TreeNode> {
    use std::ops::ControlFlow;
    let mut result: Option<*mut TreeNode> = None;
    let flow = fold_tree_mut(node, &mut |n| {
        if &n.reference == reference {
            result = Some(n as *mut TreeNode);
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    });
    // TR-1 (fold-result-safety): result is only set when we Break
    debug_assert_eq!(result.is_some(), flow.is_break());
    // SAFETY: The pointer came from a live `&mut TreeNode` obtained during
    // fold_tree_mut, which visits each node exactly once and we
    // Break immediately after capturing the pointer, so it remains valid
    // for lifetime 'a and no aliasing occurs.
    result.map(|ptr| unsafe { &mut *ptr })
}

/// Find a node by matching both reference and depth using fold_tree_mut_with_depth.
///
/// This correctly handles dual appearances: when the same reference
/// appears multiple times in the tree, we match the instance at the
/// specific depth the user is viewing.
///
/// Uses raw pointer to escape closure lifetime, which is safe because:
/// 1. fold_tree_mut_with_depth visits each node exactly once
/// 2. We Break immediately after finding the match
/// 3. The pointer is valid for the input lifetime 'a
#[allow(dead_code)] // used by tests
pub(crate) fn find_node_at_depth_mut<'a>(
    node: &'a mut TreeNode,
    reference: &NodeRef,
    target_depth: usize,
    current_depth: usize,
    found_count: &mut usize,
) -> Option<&'a mut TreeNode> {
    use std::ops::ControlFlow;
    let mut result: Option<*mut TreeNode> = None;
    let flow = fold_tree_mut_with_depth(node, current_depth, &mut |n, d| {
        if &n.reference == reference && d == target_depth {
            if *found_count == 0 {
                result = Some(n as *mut TreeNode);
                return ControlFlow::Break(());
            }
            *found_count -= 1;
        }
        ControlFlow::Continue(())
    });
    // TR-1 (fold-result-safety): result is only set when we Break
    debug_assert_eq!(result.is_some(), flow.is_break());
    // SAFETY: The pointer came from a live `&mut TreeNode` obtained during
    // fold_tree_mut_with_depth, which visits each node exactly once and we
    // Break immediately after capturing the pointer, so it remains valid
    // for lifetime 'a and no aliasing occurs.
    result.map(|ptr| unsafe { &mut *ptr })
}

/// Find a node by (reference, depth) starting from root's children.
///
/// The root node itself is synthetic and not rendered, so this
/// iterates over `root.children` and delegates to
/// `find_node_at_depth_mut`. Encapsulates the repeated
/// root-children search pattern used by expand and collapse.
pub(crate) fn find_at_depth_from_root_mut<'a>(
    root: &'a mut TreeNode,
    reference: &NodeRef,
    depth: usize,
) -> Option<&'a mut TreeNode> {
    let mut count = 0;
    for child in &mut root.children {
        if let Some(node) = find_node_at_depth_mut(child, reference, depth, 0, &mut count) {
            return Some(node);
        }
    }
    None
}

/// Collect all references in tree (recursive, visits all nodes).
///
/// Traverses ALL nodes regardless of expanded state, used for cache
/// pruning.
/// Collect all references using algebraic fold.
pub(crate) fn collect_refs<'a>(node: &'a TreeNode, out: &mut HashSet<&'a NodeRef>) {
    let all_refs = fold_tree(node, &|n, child_results: Vec<HashSet<&'a NodeRef>>| {
        let mut refs = HashSet::new();
        refs.insert(&n.reference);
        for child_set in child_results {
            refs.extend(child_set);
        }
        refs
    });
    out.extend(all_refs);
}

/// Collect (reference, depth) pairs of all expanded nodes using algebraic fold.
///
/// Tracks expansion state per tree position, not just per reference.
/// This correctly handles dual appearances where the same reference
/// appears at multiple depths with different expansion states.
pub(crate) fn collect_expanded_refs(
    node: &TreeNode,
    depth: usize,
    out: &mut HashSet<(NodeRef, usize)>,
) {
    let refs = fold_tree_with_depth(node, depth, &|n, d, child_results| {
        let mut result: HashSet<(NodeRef, usize)> = child_results.into_iter().flatten().collect();
        if n.expanded {
            result.insert((n.reference.clone(), d));
        }
        result
    });
    out.extend(refs);
}

/// Collect (reference, depth) pairs of all failed nodes.
///
/// Mirrors `collect_expanded_refs`. Used to carry forward failure
/// state across tree rebuilds for collapsed nodes whose children
/// are not recomputed.
pub(crate) fn collect_failed_refs(
    node: &TreeNode,
    depth: usize,
    out: &mut HashSet<(NodeRef, usize)>,
) {
    let refs = fold_tree_with_depth(node, depth, &|n, d, child_results| {
        let mut result: HashSet<(NodeRef, usize)> = child_results.into_iter().flatten().collect();
        if n.failed {
            result.insert((n.reference.clone(), d));
        }
        result
    });
    out.extend(refs);
}

/// Collapse all nodes using fold-based traversal.
pub(crate) fn collapse_all(node: &mut TreeNode) {
    use std::ops::ControlFlow;
    let _ = fold_tree_mut(node, &mut |n| {
        n.expanded = false;
        ControlFlow::<()>::Continue(())
    });
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::model::NodeType;

    fn root() -> NodeRef {
        NodeRef::Root
    }

    fn host(name: &str) -> NodeRef {
        let proc_id = hyperactor::reference::ProcId::with_name(
            "unix:@test"
                .parse::<hyperactor::channel::ChannelAddr>()
                .unwrap(),
            "world",
        );
        NodeRef::Host(proc_id.actor_id(name))
    }

    fn proc_ref(name: &str) -> NodeRef {
        let proc_id = hyperactor::reference::ProcId::with_name(
            "unix:@test"
                .parse::<hyperactor::channel::ChannelAddr>()
                .unwrap(),
            name,
        );
        NodeRef::Proc(proc_id)
    }

    fn actor(name: &str) -> NodeRef {
        let proc_id = hyperactor::reference::ProcId::with_name(
            "unix:@test"
                .parse::<hyperactor::channel::ChannelAddr>()
                .unwrap(),
            "world",
        );
        NodeRef::Actor(proc_id.actor_id(name))
    }

    // Helper to find a node by reference using algebraic fold.
    fn find_node_by_ref<'a>(node: &'a TreeNode, reference: &NodeRef) -> Option<&'a TreeNode> {
        fold_tree(node, &|n, child_results| {
            if &n.reference == reference {
                Some(n)
            } else {
                child_results.into_iter().find_map(|x| x)
            }
        })
    }

    #[test]
    fn flatten_collapsed_node_hides_children() {
        let tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![TreeNode {
                reference: host("host1"),
                label: "Host 1".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: true,
                has_children: true,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![TreeNode {
                    reference: proc_ref("proc1"),
                    label: "Proc 1".into(),
                    node_type: NodeType::Proc,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![],
                }],
            }],
        };
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].node.reference, host("host1"));
        assert_eq!(rows[0].depth, 0);
    }

    #[test]
    fn flatten_expanded_node_shows_children() {
        let tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![TreeNode {
                reference: host("host1"),
                label: "Host 1".into(),
                node_type: NodeType::Host,
                expanded: true,
                fetched: true,
                has_children: true,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![TreeNode {
                    reference: proc_ref("proc1"),
                    label: "Proc 1".into(),
                    node_type: NodeType::Proc,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![],
                }],
            }],
        };
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].node.reference, host("host1"));
        assert_eq!(rows[0].depth, 0);
        assert_eq!(rows[1].node.reference, proc_ref("proc1"));
        assert_eq!(rows[1].depth, 1);
    }

    #[test]
    fn find_node_by_reference_works() {
        let tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![TreeNode {
                reference: actor("child1"),
                label: "Child 1".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: true,
                has_children: false,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            }],
        };
        let found = find_node_by_ref(&tree, &actor("child1"));
        assert!(found.is_some());
        assert_eq!(found.unwrap().reference, actor("child1"));
    }

    #[test]
    fn find_node_mut_works() {
        let mut tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![TreeNode {
                reference: actor("child1"),
                label: "Child 1".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: true,
                has_children: false,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            }],
        };
        let found = find_node_mut(&mut tree, &actor("child1"));
        assert!(found.is_some());
        found.unwrap().expanded = true;
        assert!(tree.children[0].expanded);
    }

    #[test]
    fn collect_refs_visits_all_nodes() {
        let tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![TreeNode {
                reference: host("host1"),
                label: "Host 1".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: true,
                has_children: true,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![TreeNode {
                    reference: proc_ref("proc1"),
                    label: "Proc 1".into(),
                    node_type: NodeType::Proc,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![],
                }],
            }],
        };
        let mut refs = HashSet::new();
        collect_refs(&tree, &mut refs);
        assert_eq!(refs.len(), 3);
        assert!(refs.contains(&root()));
        assert!(refs.contains(&host("host1")));
        assert!(refs.contains(&proc_ref("proc1")));
    }

    #[test]
    fn dual_appearances_flatten_correctly() {
        let tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![
                TreeNode {
                    reference: proc_ref("proc1"),
                    label: "Proc 1".into(),
                    node_type: NodeType::Proc,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![TreeNode {
                        reference: actor("actor1"),
                        label: "Actor 1".into(),
                        node_type: NodeType::Actor,
                        expanded: false,
                        fetched: true,
                        has_children: false,
                        stopped: false,
                        failed: false,
                        is_system: false,
                        children: vec![],
                    }],
                },
                TreeNode {
                    reference: actor("actor1"),
                    label: "Actor 1".into(),
                    node_type: NodeType::Actor,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![],
                },
            ],
        };
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].node.reference, proc_ref("proc1"));
        assert_eq!(rows[0].depth, 0);
        assert_eq!(rows[1].node.reference, actor("actor1"));
        assert_eq!(rows[1].depth, 1);
        assert_eq!(rows[2].node.reference, actor("actor1"));
        assert_eq!(rows[2].depth, 0);
    }

    #[test]
    fn expansion_tracking_uses_depth_pairs() {
        let tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![
                TreeNode {
                    reference: proc_ref("proc1"),
                    label: "Proc 1".into(),
                    node_type: NodeType::Proc,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![TreeNode {
                        reference: actor("actor1"),
                        label: "Actor 1".into(),
                        node_type: NodeType::Actor,
                        expanded: true,
                        fetched: true,
                        has_children: false,
                        stopped: false,
                        failed: false,
                        is_system: false,
                        children: vec![],
                    }],
                },
                TreeNode {
                    reference: actor("actor1"),
                    label: "Actor 1".into(),
                    node_type: NodeType::Actor,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![],
                },
            ],
        };
        let mut expanded_keys = HashSet::new();
        for child in &tree.children {
            collect_expanded_refs(child, 0, &mut expanded_keys);
        }
        assert!(expanded_keys.contains(&(proc_ref("proc1"), 0)));
        assert!(expanded_keys.contains(&(actor("actor1"), 1)));
        assert!(!expanded_keys.contains(&(actor("actor1"), 0)));
    }

    #[test]
    fn find_node_at_depth_distinguishes_instances() {
        let mut tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![
                TreeNode {
                    reference: proc_ref("proc1"),
                    label: "Proc 1".into(),
                    node_type: NodeType::Proc,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![TreeNode {
                        reference: actor("actor1"),
                        label: "Actor 1 in supervision".into(),
                        node_type: NodeType::Actor,
                        expanded: true,
                        fetched: true,
                        has_children: false,
                        stopped: false,
                        failed: false,
                        is_system: false,
                        children: vec![],
                    }],
                },
                TreeNode {
                    reference: actor("actor1"),
                    label: "Actor 1 in flat list".into(),
                    node_type: NodeType::Actor,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![],
                },
            ],
        };
        let mut count = 0;
        let found_depth_1 = tree
            .children
            .iter_mut()
            .find_map(|child| find_node_at_depth_mut(child, &actor("actor1"), 1, 0, &mut count));
        assert!(found_depth_1.is_some());
        assert_eq!(found_depth_1.unwrap().label, "Actor 1 in supervision");
        let mut count = 0;
        let found_depth_0 = tree
            .children
            .iter_mut()
            .find_map(|child| find_node_at_depth_mut(child, &actor("actor1"), 0, 0, &mut count));
        assert!(found_depth_0.is_some());
        assert_eq!(found_depth_0.unwrap().label, "Actor 1 in flat list");
    }

    #[test]
    fn collapsed_nodes_stay_collapsed_after_refresh() {
        let tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![TreeNode {
                reference: proc_ref("proc1"),
                label: "Proc 1".into(),
                node_type: NodeType::Proc,
                expanded: false,
                fetched: true,
                has_children: true,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![TreeNode {
                    reference: actor("actor1"),
                    label: "Actor 1".into(),
                    node_type: NodeType::Actor,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![],
                }],
            }],
        };
        let mut expanded_keys = HashSet::new();
        for child in &tree.children {
            collect_expanded_refs(child, 0, &mut expanded_keys);
        }
        assert!(!expanded_keys.contains(&(proc_ref("proc1"), 0)));
        assert!(!expanded_keys.contains(&(actor("actor1"), 1)));
    }

    #[test]
    fn fold_equivalence_flatten_tree() {
        let tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![
                TreeNode {
                    reference: host("host1"),
                    label: "Host 1".into(),
                    node_type: NodeType::Host,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![TreeNode {
                        reference: proc_ref("proc1"),
                        label: "Proc 1".into(),
                        node_type: NodeType::Proc,
                        expanded: false,
                        fetched: true,
                        has_children: true,
                        stopped: false,
                        failed: false,
                        is_system: false,
                        children: vec![TreeNode {
                            reference: actor("actor1"),
                            label: "Actor 1".into(),
                            node_type: NodeType::Actor,
                            expanded: false,
                            fetched: true,
                            has_children: false,
                            stopped: false,
                            failed: false,
                            is_system: false,
                            children: vec![],
                        }],
                    }],
                },
                TreeNode {
                    reference: host("host2"),
                    label: "Host 2".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![],
                },
            ],
        };
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].node.reference, host("host1"));
        assert_eq!(rows[0].depth, 0);
        assert_eq!(rows[1].node.reference, proc_ref("proc1"));
        assert_eq!(rows[1].depth, 1);
        assert_eq!(rows[2].node.reference, host("host2"));
        assert_eq!(rows[2].depth, 0);
    }

    #[test]
    fn fold_tree_mut_early_exit_stops_traversal() {
        let mut tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![
                TreeNode {
                    reference: actor("child1"),
                    label: "Child 1".into(),
                    node_type: NodeType::Host,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![TreeNode {
                        reference: actor("target"),
                        label: "Target".into(),
                        node_type: NodeType::Proc,
                        expanded: true,
                        fetched: true,
                        has_children: false,
                        stopped: false,
                        failed: false,
                        is_system: false,
                        children: vec![],
                    }],
                },
                TreeNode {
                    reference: actor("child2"),
                    label: "Child 2".into(),
                    node_type: NodeType::Host,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![TreeNode {
                        reference: actor("should_not_visit"),
                        label: "Should Not Visit".into(),
                        node_type: NodeType::Proc,
                        expanded: true,
                        fetched: true,
                        has_children: false,
                        stopped: false,
                        failed: false,
                        is_system: false,
                        children: vec![],
                    }],
                },
            ],
        };
        use std::ops::ControlFlow;
        let mut visited = Vec::new();
        let result = fold_tree_mut_with_depth(&mut tree, 0, &mut |n, _d| {
            visited.push(n.reference.clone());
            if n.reference == actor("target") {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        });
        assert!(result.is_break());
        assert_eq!(visited, vec![root(), actor("child1"), actor("target")]);
        assert!(!visited.contains(&actor("should_not_visit")));
    }

    #[test]
    fn selection_restore_prefers_depth_match() {
        let mut tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![TreeNode {
                reference: actor("duplicate"),
                label: "Duplicate at depth 0".into(),
                node_type: NodeType::Host,
                expanded: true,
                fetched: true,
                has_children: true,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![TreeNode {
                    reference: actor("duplicate"),
                    label: "Duplicate at depth 1".into(),
                    node_type: NodeType::Proc,
                    expanded: true,
                    fetched: true,
                    has_children: false,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![],
                }],
            }],
        };
        let mut count = 0;
        let found_d0 = tree
            .children
            .iter_mut()
            .find_map(|child| find_node_at_depth_mut(child, &actor("duplicate"), 0, 0, &mut count));
        assert!(found_d0.is_some());
        assert_eq!(found_d0.unwrap().label, "Duplicate at depth 0");
        let mut count = 0;
        let found_d1 = tree
            .children
            .iter_mut()
            .find_map(|child| find_node_at_depth_mut(child, &actor("duplicate"), 1, 0, &mut count));
        assert!(found_d1.is_some());
        assert_eq!(found_d1.unwrap().label, "Duplicate at depth 1");
    }

    #[test]
    fn fold_vs_traversal_law_node_count() {
        let tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![
                TreeNode {
                    reference: host("host1"),
                    label: "Host 1".into(),
                    node_type: NodeType::Host,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![TreeNode {
                        reference: proc_ref("proc1"),
                        label: "Proc 1".into(),
                        node_type: NodeType::Proc,
                        expanded: true,
                        fetched: true,
                        has_children: false,
                        stopped: false,
                        failed: false,
                        is_system: false,
                        children: vec![],
                    }],
                },
                TreeNode {
                    reference: host("host2"),
                    label: "Host 2".into(),
                    node_type: NodeType::Host,
                    expanded: true,
                    fetched: true,
                    has_children: false,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![],
                },
            ],
        };
        let node_count = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
            1 + child_counts.iter().sum::<usize>()
        });
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), node_count - 1);
    }

    #[test]
    fn collapse_idempotence() {
        let mut tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![TreeNode {
                reference: actor("child"),
                label: "Child".into(),
                node_type: NodeType::Host,
                expanded: true,
                fetched: true,
                has_children: false,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            }],
        };
        collapse_all(&mut tree);
        assert!(!tree.expanded);
        assert!(!tree.children[0].expanded);
        collapse_all(&mut tree);
        assert!(!tree.expanded);
        assert!(!tree.children[0].expanded);
        let snapshot_after_first = tree.expanded;
        collapse_all(&mut tree);
        assert_eq!(tree.expanded, snapshot_after_first);
    }

    #[test]
    fn placeholder_refinement_transitions_fetched_state() {
        let mut tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![TreeNode {
                reference: actor("placeholder"),
                label: "Loading...".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: false,
                has_children: true,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            }],
        };
        use std::ops::ControlFlow;
        let _ = fold_tree_mut(&mut tree, &mut |n| {
            if n.reference == actor("placeholder") && !n.fetched {
                n.fetched = true;
                n.has_children = true;
                n.children = vec![TreeNode {
                    reference: actor("child"),
                    label: "Child".into(),
                    node_type: NodeType::Proc,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![],
                }];
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        });
        let placeholder = find_node_by_ref(&tree, &actor("placeholder"));
        assert!(placeholder.is_some());
        let placeholder = placeholder.unwrap();
        assert!(placeholder.fetched);
        assert_eq!(placeholder.children.len(), 1);
        let initial_children = placeholder.children.len();
        let _ = find_node_by_ref(&tree, &actor("placeholder"));
        assert_eq!(initial_children, 1);
    }

    #[test]
    fn cycle_guard_prevents_infinite_recursion() {
        let mut tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![TreeNode {
                reference: root(),
                label: "Self-reference".into(),
                node_type: NodeType::Host,
                expanded: true,
                fetched: true,
                has_children: false,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            }],
        };
        let count = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
            1 + child_counts.iter().sum::<usize>()
        });
        assert_eq!(count, 2);
        use std::ops::ControlFlow;
        let mut visited = 0;
        let _ = fold_tree_mut(&mut tree, &mut |_n| {
            visited += 1;
            if visited > 100 {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        });
        assert_eq!(visited, 2);
    }

    #[test]
    fn fold_tree_mut_visits_in_preorder() {
        let mut tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![
                TreeNode {
                    reference: actor("child1"),
                    label: "Child 1".into(),
                    node_type: NodeType::Host,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![TreeNode {
                        reference: actor("grandchild1"),
                        label: "Grandchild 1".into(),
                        node_type: NodeType::Proc,
                        expanded: false,
                        fetched: true,
                        has_children: false,
                        stopped: false,
                        failed: false,
                        is_system: false,
                        children: vec![],
                    }],
                },
                TreeNode {
                    reference: actor("child2"),
                    label: "Child 2".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![],
                },
            ],
        };
        use std::ops::ControlFlow;
        let mut visit_order = Vec::new();
        let _ = fold_tree_mut_with_depth(&mut tree, 0, &mut |n, d| {
            visit_order.push((n.reference.clone(), d));
            ControlFlow::<()>::Continue(())
        });
        assert_eq!(
            visit_order,
            vec![
                (root(), 0),
                (actor("child1"), 1),
                (actor("grandchild1"), 2),
                (actor("child2"), 1),
            ]
        );
    }

    #[test]
    fn fold_tree_with_depth_deterministic_preorder() {
        let tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![
                TreeNode {
                    reference: actor("a"),
                    label: "A".into(),
                    node_type: NodeType::Host,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![TreeNode {
                        reference: actor("a1"),
                        label: "A1".into(),
                        node_type: NodeType::Proc,
                        expanded: false,
                        fetched: true,
                        has_children: false,
                        stopped: false,
                        failed: false,
                        is_system: false,
                        children: vec![],
                    }],
                },
                TreeNode {
                    reference: actor("b"),
                    label: "B".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![],
                },
            ],
        };
        let visit_order = fold_tree_with_depth(&tree, 0, &|n,
                                                           d,
                                                           child_orders: Vec<
            Vec<(NodeRef, usize)>,
        >| {
            let mut order = vec![(n.reference.clone(), d)];
            for child_order in child_orders {
                order.extend(child_order);
            }
            order
        });
        assert_eq!(
            visit_order,
            vec![
                (root(), 0),
                (actor("a"), 1),
                (actor("a1"), 2),
                (actor("b"), 1),
            ]
        );
    }

    #[test]
    fn cycle_vs_duplicate_reference_allowed() {
        let tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![
                TreeNode {
                    reference: actor("branch_a"),
                    label: "Branch A".into(),
                    node_type: NodeType::Host,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![TreeNode {
                        reference: actor("duplicate"),
                        label: "Duplicate in A".into(),
                        node_type: NodeType::Proc,
                        expanded: false,
                        fetched: true,
                        has_children: false,
                        stopped: false,
                        failed: false,
                        is_system: false,
                        children: vec![],
                    }],
                },
                TreeNode {
                    reference: actor("branch_b"),
                    label: "Branch B".into(),
                    node_type: NodeType::Host,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![TreeNode {
                        reference: actor("duplicate"),
                        label: "Duplicate in B".into(),
                        node_type: NodeType::Proc,
                        expanded: false,
                        fetched: true,
                        has_children: false,
                        stopped: false,
                        failed: false,
                        is_system: false,
                        children: vec![],
                    }],
                },
            ],
        };
        let count = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
            1 + child_counts.iter().sum::<usize>()
        });
        assert_eq!(count, 5);
        let rows = flatten_tree(&tree);
        let duplicate_count = rows
            .iter()
            .filter(|r| r.node.reference == actor("duplicate"))
            .count();
        assert_eq!(duplicate_count, 2);
    }

    #[test]
    fn single_node_tree_expand_collapse() {
        let mut tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![TreeNode {
                reference: actor("only_child"),
                label: "Only Child".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: true,
                has_children: false,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            }],
        };
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].node.reference, actor("only_child"));
        let child = find_node_mut(&mut tree, &actor("only_child"));
        assert!(child.is_some());
        let child = child.unwrap();
        assert!(!child.has_children);
        assert!(!child.expanded);
        collapse_all(&mut tree);
        assert!(!tree.expanded);
        assert!(!tree.children[0].expanded);
    }

    #[test]
    fn placeholder_with_has_children_true_awaits_fetch() {
        let mut tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![TreeNode {
                reference: actor("placeholder"),
                label: "Loading...".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: false,
                has_children: true,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            }],
        };
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].node.reference, actor("placeholder"));
        let placeholder = find_node_mut(&mut tree, &actor("placeholder"));
        assert!(placeholder.is_some());
        let placeholder = placeholder.unwrap();
        assert!(placeholder.has_children);
        assert!(!placeholder.fetched);
        assert_eq!(placeholder.children.len(), 0);
        use std::ops::ControlFlow;
        let _ = fold_tree_mut(&mut tree, &mut |n| {
            if n.reference == actor("placeholder") && !n.expanded {
                assert!(!n.expanded);
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        });
    }

    #[test]
    fn placeholder_noop_when_has_children_false() {
        let mut tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![TreeNode {
                reference: actor("leaf"),
                label: "Leaf Node".into(),
                node_type: NodeType::Actor,
                expanded: false,
                fetched: true,
                has_children: false,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            }],
        };
        use std::ops::ControlFlow;
        let _ = fold_tree_mut(&mut tree, &mut |n| {
            if n.reference == actor("leaf") && !n.has_children {
                assert_eq!(n.children.len(), 0);
                assert!(!n.expanded);
            }
            ControlFlow::<()>::Continue(())
        });
        let leaf = find_node_by_ref(&tree, &actor("leaf"));
        assert!(leaf.is_some());
        let leaf = leaf.unwrap();
        assert!(!leaf.expanded);
        assert_eq!(leaf.children.len(), 0);
        assert!(!leaf.has_children);
    }

    #[test]
    fn expanded_node_with_empty_children_renders_safely() {
        let tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![TreeNode {
                reference: actor("empty_parent"),
                label: "Empty Parent".into(),
                node_type: NodeType::Host,
                expanded: true,
                fetched: true,
                has_children: false,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            }],
        };
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].node.reference, actor("empty_parent"));
        let count = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
            1 + child_counts.iter().sum::<usize>()
        });
        assert_eq!(count, 2);
    }

    #[test]
    fn duplicate_references_expansion_targets_specific_instance() {
        let mut tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![
                TreeNode {
                    reference: actor("duplicate"),
                    label: "Duplicate at 0".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: true,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![TreeNode {
                        reference: actor("child_of_first"),
                        label: "Child of First".into(),
                        node_type: NodeType::Proc,
                        expanded: false,
                        fetched: true,
                        has_children: false,
                        stopped: false,
                        failed: false,
                        is_system: false,
                        children: vec![],
                    }],
                },
                TreeNode {
                    reference: actor("duplicate"),
                    label: "Duplicate at 0 (second)".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: true,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![TreeNode {
                        reference: actor("child_of_second"),
                        label: "Child of Second".into(),
                        node_type: NodeType::Proc,
                        expanded: false,
                        fetched: true,
                        has_children: false,
                        stopped: false,
                        failed: false,
                        is_system: false,
                        children: vec![],
                    }],
                },
            ],
        };
        let mut count = 0;
        let first = tree
            .children
            .iter_mut()
            .find_map(|child| find_node_at_depth_mut(child, &actor("duplicate"), 0, 0, &mut count));
        assert!(first.is_some());
        first.unwrap().expanded = true;
        assert!(tree.children[0].expanded);
        assert!(!tree.children[1].expanded);
        let rows = flatten_tree(&tree);
        let refs: Vec<_> = rows.iter().map(|r| &r.node.reference).collect();
        assert!(refs.contains(&&actor("child_of_first")));
        assert!(!refs.contains(&&actor("child_of_second")));
    }

    // Stopped nodes visible in flatten
    #[test]
    fn stopped_nodes_visible_in_flatten() {
        let tree = make_proc_with_stopped_children();
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 4);
        assert!(rows.iter().any(|r| r.node.reference == actor("dead_actor")));
    }

    #[test]
    fn stopped_node_stopped_field_survives_flatten() {
        let tree = make_proc_with_stopped_children();
        let rows = flatten_tree(&tree);
        let dead = rows
            .iter()
            .find(|r| r.node.reference == actor("dead_actor"))
            .unwrap();
        assert!(dead.node.stopped);
        let live = rows
            .iter()
            .find(|r| r.node.reference == actor("live_actor_1"))
            .unwrap();
        assert!(!live.node.stopped);
    }

    #[test]
    fn placeholder_stopped_visible_in_flatten() {
        let tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![TreeNode::placeholder_stopped(actor("dead1"))],
        };
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 1);
        assert!(rows[0].node.stopped);
        assert!(!rows[0].node.fetched);
    }

    // Helper for stopped-children tests
    fn make_proc_with_stopped_children() -> TreeNode {
        TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![TreeNode {
                reference: proc_ref("proc1"),
                label: "proc1".into(),
                node_type: NodeType::Proc,
                expanded: true,
                fetched: true,
                has_children: true,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![
                    TreeNode {
                        reference: actor("live_actor_1"),
                        label: "live_actor_1".into(),
                        node_type: NodeType::Actor,
                        expanded: false,
                        fetched: true,
                        has_children: false,
                        stopped: false,
                        failed: false,
                        is_system: false,
                        children: vec![],
                    },
                    TreeNode {
                        reference: actor("live_actor_2"),
                        label: "live_actor_2".into(),
                        node_type: NodeType::Actor,
                        expanded: false,
                        fetched: true,
                        has_children: false,
                        stopped: false,
                        failed: false,
                        is_system: false,
                        children: vec![],
                    },
                    TreeNode {
                        reference: actor("dead_actor"),
                        label: "dead_actor".into(),
                        node_type: NodeType::Actor,
                        expanded: false,
                        fetched: true,
                        has_children: false,
                        stopped: true,
                        failed: false,
                        is_system: false,
                        children: vec![],
                    },
                ],
            }],
        }
    }

    // Performance / scale tests

    #[test]
    fn flatten_scales_linearly_with_visible_nodes() {
        for scale in [100, 500, 1000] {
            let mut children = Vec::new();
            for i in 0..scale {
                children.push(TreeNode {
                    reference: actor(&format!("node_{}", i)),
                    label: format!("Node {}", i),
                    node_type: NodeType::Actor,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![],
                });
            }
            let tree = TreeNode {
                reference: root(),
                label: "Root".into(),
                node_type: NodeType::Root,
                expanded: true,
                fetched: true,
                has_children: true,
                stopped: false,
                failed: false,
                is_system: false,
                children,
            };
            let rows = flatten_tree(&tree);
            assert_eq!(rows.len(), scale);
            let count = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
                1 + child_counts.iter().sum::<usize>()
            });
            assert_eq!(count, scale + 1);
        }
    }

    #[test]
    fn deep_chain_vs_wide_fanout_performance() {
        let mut deep = TreeNode {
            reference: actor("deep_root"),
            label: "Deep Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![],
        };
        let mut current = &mut deep;
        for i in 0..499 {
            current.children.push(TreeNode {
                reference: actor(&format!("deep_{}", i)),
                label: format!("Deep {}", i),
                node_type: NodeType::Host,
                expanded: true,
                fetched: true,
                has_children: true,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            });
            current = &mut current.children[0];
        }
        let mut wide_children = Vec::new();
        for i in 0..500 {
            wide_children.push(TreeNode {
                reference: actor(&format!("wide_{}", i)),
                label: format!("Wide {}", i),
                node_type: NodeType::Actor,
                expanded: false,
                fetched: true,
                has_children: false,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            });
        }
        let wide = TreeNode {
            reference: actor("wide_root"),
            label: "Wide Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: wide_children,
        };
        assert_eq!(flatten_tree(&deep).len(), 499);
        assert_eq!(flatten_tree(&wide).len(), 500);
        let deep_count = fold_tree(&deep, &|_n, cs: Vec<usize>| 1 + cs.iter().sum::<usize>());
        let wide_count = fold_tree(&wide, &|_n, cs: Vec<usize>| 1 + cs.iter().sum::<usize>());
        assert_eq!(deep_count, 500);
        assert_eq!(wide_count, 501);
    }

    #[test]
    fn early_exit_avoids_full_traversal() {
        let mut children = Vec::new();
        for i in 0..1000 {
            children.push(TreeNode {
                reference: actor(&format!("node_{}", i)),
                label: format!("Node {}", i),
                node_type: NodeType::Actor,
                expanded: false,
                fetched: true,
                has_children: false,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            });
        }
        let mut tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children,
        };
        let mut visited = 0;
        let _ = fold_tree_mut(&mut tree, &mut |_n| {
            use std::ops::ControlFlow;
            visited += 1;
            if visited == 10 {
                ControlFlow::Break(())
            } else {
                ControlFlow::<()>::Continue(())
            }
        });
        assert!(visited < 50, "Early exit failed, visited {}", visited);
        assert_eq!(visited, 10);
    }

    #[test]
    fn flatten_ignores_collapsed_subtrees() {
        let mut children = Vec::new();
        for i in 0..100 {
            let mut grandchildren = Vec::new();
            for j in 0..100 {
                grandchildren.push(TreeNode {
                    reference: actor(&format!("child_{}_{}", i, j)),
                    label: format!("Child {} {}", i, j),
                    node_type: NodeType::Actor,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![],
                });
            }
            children.push(TreeNode {
                reference: actor(&format!("parent_{}", i)),
                label: format!("Parent {}", i),
                node_type: NodeType::Proc,
                expanded: false,
                fetched: true,
                has_children: true,
                stopped: false,
                failed: false,
                is_system: false,
                children: grandchildren,
            });
        }
        let tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children,
        };
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 100);
    }

    #[test]
    fn many_nodes_same_reference_depth_tracking() {
        let mut children = Vec::new();
        for depth in 0..100 {
            let mut node = TreeNode {
                reference: actor("dup"),
                label: format!("Dup at depth {}", depth),
                node_type: NodeType::Actor,
                expanded: false,
                fetched: true,
                has_children: false,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            };
            for i in (0..depth).rev() {
                node = TreeNode {
                    reference: actor(&format!("wrapper_{}", i)),
                    label: format!("Wrapper {}", i),
                    node_type: NodeType::Host,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![node],
                };
            }
            children.push(node);
        }
        let tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children,
        };
        let rows = flatten_tree(&tree);
        let expected: usize = (0..100).map(|d| d + 1).sum();
        assert_eq!(rows.len(), expected);
    }

    #[test]
    fn memory_stable_across_repeated_operations() {
        let mut tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![TreeNode {
                reference: actor("child"),
                label: "Child".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: true,
                has_children: false,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            }],
        };
        for _ in 0..5000 {
            tree.children[0].expanded = !tree.children[0].expanded;
            let _ = flatten_tree(&tree);
        }
        let node_count = fold_tree(&tree, &|_n, cs: Vec<usize>| 1 + cs.iter().sum::<usize>());
        assert_eq!(node_count, 2);
    }

    #[test]
    fn fold_performance_parity_with_manual_recursion() {
        let mut children = Vec::new();
        for i in 0..1000 {
            children.push(TreeNode {
                reference: actor(&format!("node_{}", i)),
                label: format!("Node {}", i),
                node_type: NodeType::Actor,
                expanded: false,
                fetched: true,
                has_children: false,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            });
        }
        let tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children,
        };
        let fold_result = fold_tree(&tree, &|_n, cs: Vec<usize>| 1 + cs.iter().sum::<usize>());
        fn manual_count(n: &TreeNode) -> usize {
            1 + n.children.iter().map(manual_count).sum::<usize>()
        }
        let manual_result = manual_count(&tree);
        assert_eq!(fold_result, manual_result);
        assert_eq!(fold_result, 1001);
    }

    #[test]
    fn failed_tracking_uses_depth_pairs() {
        // Mirrors expansion_tracking_uses_depth_pairs but for failed state.
        let tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![
                TreeNode {
                    reference: proc_ref("proc1"),
                    label: "Proc 1".into(),
                    node_type: NodeType::Proc,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    stopped: false,
                    failed: true, // failed proc
                    is_system: false,
                    children: vec![TreeNode {
                        reference: actor("actor1"),
                        label: "Actor 1".into(),
                        node_type: NodeType::Actor,
                        expanded: false,
                        fetched: true,
                        has_children: false,
                        stopped: false,
                        failed: true, // failed actor
                        is_system: false,
                        children: vec![],
                    }],
                },
                TreeNode {
                    reference: actor("actor1"),
                    label: "Actor 1 (flat)".into(),
                    node_type: NodeType::Actor,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    stopped: false,
                    failed: false, // same ref, not failed at this depth
                    is_system: false,
                    children: vec![],
                },
            ],
        };
        let mut failed_keys = HashSet::new();
        for child in &tree.children {
            collect_failed_refs(child, 0, &mut failed_keys);
        }
        // proc1 at depth 0 is failed.
        assert!(failed_keys.contains(&(proc_ref("proc1"), 0)));
        // actor1 at depth 1 (under proc1) is failed.
        assert!(failed_keys.contains(&(actor("actor1"), 1)));
        // actor1 at depth 0 (flat list) is NOT failed.
        assert!(!failed_keys.contains(&(actor("actor1"), 0)));
    }

    #[test]
    fn failed_propagates_from_child_to_parent() {
        // A host with a failed child proc should appear in failed_keys
        // only if the host itself is marked failed (propagation happens
        // at build time, not in collect_failed_refs).
        let tree = TreeNode {
            reference: root(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![TreeNode {
                reference: host("host1"),
                label: "Host 1".into(),
                node_type: NodeType::Host,
                expanded: true,
                fetched: true,
                has_children: true,
                stopped: false,
                failed: true, // propagated from child at build time
                is_system: false,
                children: vec![TreeNode {
                    reference: proc_ref("proc1"),
                    label: "Proc 1".into(),
                    node_type: NodeType::Proc,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    stopped: false,
                    failed: true,
                    is_system: false,
                    children: vec![],
                }],
            }],
        };
        let mut failed_keys = HashSet::new();
        for child in &tree.children {
            collect_failed_refs(child, 0, &mut failed_keys);
        }
        assert!(failed_keys.contains(&(host("host1"), 0)));
        assert!(failed_keys.contains(&(proc_ref("proc1"), 1)));
    }
}
