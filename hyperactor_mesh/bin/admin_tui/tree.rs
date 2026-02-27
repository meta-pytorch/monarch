/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashSet;

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
    reference: &str,
) -> Option<&'a mut TreeNode> {
    use std::ops::ControlFlow;
    let mut result: Option<*mut TreeNode> = None;
    let flow = fold_tree_mut(node, &mut |n| {
        if n.reference == reference {
            result = Some(n as *mut TreeNode);
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    });
    // Safety invariant: result is only set when we Break
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
    reference: &str,
    target_depth: usize,
    current_depth: usize,
    found_count: &mut usize,
) -> Option<&'a mut TreeNode> {
    use std::ops::ControlFlow;
    let mut result: Option<*mut TreeNode> = None;
    let flow = fold_tree_mut_with_depth(node, current_depth, &mut |n, d| {
        if n.reference == reference && d == target_depth {
            if *found_count == 0 {
                result = Some(n as *mut TreeNode);
                return ControlFlow::Break(());
            }
            *found_count -= 1;
        }
        ControlFlow::Continue(())
    });
    // Safety invariant: result is only set when we Break
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
    reference: &str,
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
pub(crate) fn collect_refs<'a>(node: &'a TreeNode, out: &mut HashSet<&'a str>) {
    let all_refs = fold_tree(node, &|n, child_results: Vec<HashSet<&'a str>>| {
        let mut refs = HashSet::new();
        refs.insert(n.reference.as_str());
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
    out: &mut HashSet<(String, usize)>,
) {
    let refs = fold_tree_with_depth(node, depth, &|n, d, child_results| {
        let mut result: HashSet<(String, usize)> = child_results.into_iter().flatten().collect();
        if n.expanded {
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
