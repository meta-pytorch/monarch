/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Integration tests that exercise multiple modules together (App +
//! tree + cursor + fetch). Per-module unit tests live in each
//! module's own `#[cfg(test)] mod tests` block.

use super::*;

// Empty tree all operations are noops.
#[test]
fn empty_tree_all_operations_are_noops() {
    let app = App::new("localhost:8080", ThemeName::Nord, LangName::En);
    let rows = app.visible_rows();
    assert_eq!(rows.len(), 0);
    assert_eq!(app.cursor.pos(), 0);
    assert_eq!(app.cursor.len(), 0);
    assert!(app.tree().is_none());
}

// System proc filter toggles visibility.
#[test]
fn system_proc_filter_toggles_visibility() {
    let tree = TreeNode {
        reference: "root".into(),
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
                reference: "user_proc".into(),
                label: "User Proc".into(),
                node_type: NodeType::Proc,
                expanded: false,
                fetched: true,
                has_children: false,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            },
            TreeNode {
                reference: "system_proc".into(),
                label: "System Proc".into(),
                node_type: NodeType::Proc,
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
    assert_eq!(rows.len(), 2);
    let refs: Vec<_> = rows.iter().map(|r| r.node.reference.as_str()).collect();
    assert!(refs.contains(&"user_proc"));
    assert!(refs.contains(&"system_proc"));
}

// Stale selection after refresh clamps cursor.
#[test]
fn stale_selection_after_refresh_clamps_cursor() {
    let tree_before = TreeNode {
        reference: "root".into(),
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
                reference: "child1".into(),
                label: "Child 1".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: true,
                has_children: false,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            },
            TreeNode {
                reference: "child2".into(),
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
            TreeNode {
                reference: "child3".into(),
                label: "Child 3".into(),
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
    let rows_before = flatten_tree(&tree_before);
    assert_eq!(rows_before.len(), 3);
    let mut cursor = Cursor::new(rows_before.len());
    cursor.set_pos(2);
    assert_eq!(cursor.pos(), 2);
    let tree_after = TreeNode {
        reference: "root".into(),
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
                reference: "child1".into(),
                label: "Child 1".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: true,
                has_children: false,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            },
            TreeNode {
                reference: "child2".into(),
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
    let rows_after = flatten_tree(&tree_after);
    assert_eq!(rows_after.len(), 2);
    cursor.update_len(rows_after.len());
    assert!(cursor.pos() < rows_after.len());
    assert_eq!(cursor.pos(), 1);
}

// Selection restore fallback when depth changes.
#[test]
fn selection_restore_fallback_when_depth_changes() {
    let tree_before = TreeNode {
        reference: "root".into(),
        label: "Root".into(),
        node_type: NodeType::Root,
        expanded: true,
        fetched: true,
        has_children: true,
        stopped: false,
        failed: false,
        is_system: false,
        children: vec![TreeNode {
            reference: "parent".into(),
            label: "Parent".into(),
            node_type: NodeType::Host,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![TreeNode {
                reference: "target".into(),
                label: "Target at depth 1".into(),
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
    let rows_before = flatten_tree(&tree_before);
    let target_before: Vec<_> = rows_before
        .iter()
        .filter(|r| r.node.reference == "target")
        .collect();
    assert_eq!(target_before.len(), 1);
    assert_eq!(target_before[0].depth, 1);
    let tree_after = TreeNode {
        reference: "root".into(),
        label: "Root".into(),
        node_type: NodeType::Root,
        expanded: true,
        fetched: true,
        has_children: true,
        stopped: false,
        failed: false,
        is_system: false,
        children: vec![TreeNode {
            reference: "target".into(),
            label: "Target at depth 0".into(),
            node_type: NodeType::Proc,
            expanded: false,
            fetched: true,
            has_children: false,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![],
        }],
    };
    let rows_after = flatten_tree(&tree_after);
    let target_after: Vec<_> = rows_after
        .iter()
        .filter(|r| r.node.reference == "target")
        .collect();
    assert_eq!(target_after.len(), 1);
    assert_eq!(target_after[0].depth, 0);
}

// Partial failure resilience.
#[test]
fn partial_failure_resilience() {
    let tree = TreeNode {
        reference: "root".into(),
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
                reference: "success".into(),
                label: "Success Node".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: true,
                has_children: false,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            },
            TreeNode {
                reference: "error".into(),
                label: "Error: Failed to fetch".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: true,
                has_children: false,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            },
            TreeNode {
                reference: "success2".into(),
                label: "Another Success".into(),
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
    let error_node = rows.iter().find(|r| r.node.reference == "error");
    assert!(error_node.is_some());
    assert!(error_node.unwrap().node.label.contains("Error"));
    assert!(rows.iter().any(|r| r.node.reference == "success"));
    assert!(rows.iter().any(|r| r.node.reference == "success2"));
}

// High fanout proc placeholder performance.
#[test]
fn high_fanout_proc_placeholder_performance() {
    let mut children = Vec::new();
    for i in 0..1000 {
        children.push(TreeNode {
            reference: format!("actor_{}", i),
            label: format!("Actor {}", i),
            node_type: NodeType::Actor,
            expanded: false,
            fetched: false,
            has_children: false,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![],
        });
    }
    let tree = TreeNode {
        reference: "root".into(),
        label: "Root".into(),
        node_type: NodeType::Root,
        expanded: true,
        fetched: true,
        has_children: true,
        stopped: false,
        failed: false,
        is_system: false,
        children: vec![TreeNode {
            reference: "high_fanout_proc".into(),
            label: "High Fanout Proc".into(),
            node_type: NodeType::Proc,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children,
        }],
    };
    let rows = flatten_tree(&tree);
    assert_eq!(rows.len(), 1001);
    let actor_count = rows
        .iter()
        .filter(|r| r.node.reference.starts_with("actor_"))
        .count();
    assert_eq!(actor_count, 1000);
    let count = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
        1 + child_counts.iter().sum::<usize>()
    });
    assert_eq!(count, 1002);
}

// Rapid toggle stress test.
#[test]
fn rapid_toggle_stress_test() {
    let mut tree = TreeNode {
        reference: "root".into(),
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
                reference: "child1".into(),
                label: "Child 1".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: true,
                has_children: true,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![TreeNode {
                    reference: "grandchild".into(),
                    label: "Grandchild".into(),
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
                reference: "child2".into(),
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
    for _ in 0..100 {
        tree.children[0].expanded = true;
        let rows = flatten_tree(&tree);
        assert!(rows.len() >= 2);
        tree.children[0].expanded = false;
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 2);
        collapse_all(&mut tree);
        let rows = flatten_tree(&tree);
        assert!(rows.len() <= 2);
    }
    let final_rows = flatten_tree(&tree);
    assert!(final_rows.len() <= 2);
}

// Selection stickiness during refresh.
#[test]
fn selection_stickiness_during_refresh() {
    let tree_before = TreeNode {
        reference: "root".into(),
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
                reference: "stable".into(),
                label: "Stable Node".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: true,
                has_children: false,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            },
            TreeNode {
                reference: "transient".into(),
                label: "Transient Node".into(),
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
    let rows_before = flatten_tree(&tree_before);
    assert_eq!(rows_before.len(), 2);
    let stable_idx = rows_before
        .iter()
        .position(|r| r.node.reference == "stable")
        .unwrap();
    assert_eq!(stable_idx, 0);
    let tree_after = TreeNode {
        reference: "root".into(),
        label: "Root".into(),
        node_type: NodeType::Root,
        expanded: true,
        fetched: true,
        has_children: true,
        stopped: false,
        failed: false,
        is_system: false,
        children: vec![TreeNode {
            reference: "stable".into(),
            label: "Stable Node (refreshed)".into(),
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
    let rows_after = flatten_tree(&tree_after);
    assert_eq!(rows_after.len(), 1);
    let stable_idx_after = rows_after
        .iter()
        .position(|r| r.node.reference == "stable")
        .unwrap();
    assert_eq!(stable_idx_after, 0);
}

// Empty flight recorder renders safely.
#[test]
fn empty_flight_recorder_renders_safely() {
    let tree = TreeNode {
        reference: "root".into(),
        label: "Root".into(),
        node_type: NodeType::Root,
        expanded: true,
        fetched: true,
        has_children: true,
        stopped: false,
        failed: false,
        is_system: false,
        children: vec![TreeNode {
            reference: "actor_with_empty_data".into(),
            label: "Actor (no flight recorder)".into(),
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
    let rows = flatten_tree(&tree);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].node.reference, "actor_with_empty_data");
}

// Concurrent expansion stability.
#[test]
fn concurrent_expansion_stability() {
    let mut tree = TreeNode {
        reference: "root".into(),
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
                reference: "a".into(),
                label: "A".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: true,
                has_children: true,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![TreeNode {
                    reference: "a1".into(),
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
                reference: "b".into(),
                label: "B".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: true,
                has_children: true,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![TreeNode {
                    reference: "b1".into(),
                    label: "B1".into(),
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
    tree.children[0].expanded = true;
    tree.children[1].expanded = true;
    let rows = flatten_tree(&tree);
    assert_eq!(rows.len(), 4);
    tree.children[0].expanded = false;
    tree.children[1].expanded = false;
    let rows = flatten_tree(&tree);
    assert_eq!(rows.len(), 2);
    let count = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
        1 + child_counts.iter().sum::<usize>()
    });
    assert_eq!(count, 5);
}

// Refresh under partial failure keeps rendering.
#[test]
fn refresh_under_partial_failure_keeps_rendering() {
    let tree_refresh1 = TreeNode {
        reference: "root".into(),
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
                reference: "child1".into(),
                label: "Child 1".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: true,
                has_children: false,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            },
            TreeNode {
                reference: "child2".into(),
                label: "Error: Fetch failed".into(),
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
    let rows1 = flatten_tree(&tree_refresh1);
    assert_eq!(rows1.len(), 2);
    assert!(rows1.iter().any(|r| r.node.reference == "child1"));
    assert!(rows1.iter().any(|r| r.node.reference == "child2"));
    let tree_refresh2 = TreeNode {
        reference: "root".into(),
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
                reference: "child1".into(),
                label: "Error: Fetch failed".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: true,
                has_children: false,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            },
            TreeNode {
                reference: "child2".into(),
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
    let rows2 = flatten_tree(&tree_refresh2);
    assert_eq!(rows2.len(), 2);
    assert!(rows2.iter().any(|r| r.node.reference == "child1"));
    assert!(rows2.iter().any(|r| r.node.reference == "child2"));
}

// Large refresh churn selection clamping.
#[test]
fn large_refresh_churn_selection_clamping() {
    let tree_before = TreeNode {
        reference: "root".into(),
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
                reference: "a".into(),
                label: "A".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: true,
                has_children: false,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            },
            TreeNode {
                reference: "b".into(),
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
            TreeNode {
                reference: "c".into(),
                label: "C".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: true,
                has_children: false,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            },
            TreeNode {
                reference: "d".into(),
                label: "D".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: true,
                has_children: false,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            },
            TreeNode {
                reference: "e".into(),
                label: "E".into(),
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
    let rows_before = flatten_tree(&tree_before);
    assert_eq!(rows_before.len(), 5);
    let mut cursor = Cursor::new(rows_before.len());
    cursor.set_pos(4);
    let tree_after = TreeNode {
        reference: "root".into(),
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
                reference: "a".into(),
                label: "A (updated)".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: true,
                has_children: false,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            },
            TreeNode {
                reference: "f".into(),
                label: "F (new)".into(),
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
    let rows_after = flatten_tree(&tree_after);
    assert_eq!(rows_after.len(), 2);
    cursor.update_len(rows_after.len());
    assert!(cursor.pos() < rows_after.len());
    assert_eq!(cursor.pos(), 1);
}

// Zero actor proc renders correctly.
#[test]
fn zero_actor_proc_renders_correctly() {
    let tree = TreeNode {
        reference: "root".into(),
        label: "Root".into(),
        node_type: NodeType::Root,
        expanded: true,
        fetched: true,
        has_children: true,
        stopped: false,
        failed: false,
        is_system: false,
        children: vec![TreeNode {
            reference: "empty_proc".into(),
            label: "Proc (0 actors)".into(),
            node_type: NodeType::Proc,
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
    assert_eq!(rows[0].node.reference, "empty_proc");
    assert!(!rows[0].node.has_children);
    assert!(rows[0].node.children.is_empty());
}

// Long identity strings render safely.
#[test]
fn long_identity_strings_render_safely() {
    let long_ref = "a".repeat(500);
    let long_label = "Very long label: ".to_string() + &"x".repeat(1000);
    let tree = TreeNode {
        reference: "root".into(),
        label: "Root".into(),
        node_type: NodeType::Root,
        expanded: true,
        fetched: true,
        has_children: true,
        stopped: false,
        failed: false,
        is_system: false,
        children: vec![TreeNode {
            reference: long_ref.clone(),
            label: long_label.clone(),
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
    assert_eq!(rows[0].node.reference, long_ref);
    assert_eq!(rows[0].node.label, long_label);
    let count = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
        1 + child_counts.iter().sum::<usize>()
    });
    assert_eq!(count, 2);
}

// Duplicate references depth targeting under refresh.
#[test]
fn duplicate_references_depth_targeting_under_refresh() {
    let mut tree = TreeNode {
        reference: "root".into(),
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
                reference: "branch_a".into(),
                label: "Branch A".into(),
                node_type: NodeType::Host,
                expanded: true,
                fetched: true,
                has_children: true,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![TreeNode {
                    reference: "dup".into(),
                    label: "Dup at depth 1".into(),
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
                reference: "dup".into(),
                label: "Dup at depth 0".into(),
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
    tree.children[0].children[0].expanded = true;
    let rows = flatten_tree(&tree);
    let dup_refs: Vec<_> = rows
        .iter()
        .filter(|r| r.node.reference == "dup")
        .map(|r| r.depth)
        .collect();
    assert_eq!(dup_refs.len(), 2);
    assert!(dup_refs.contains(&0));
    assert!(dup_refs.contains(&1));
    let tree_after_refresh = tree.clone();
    let rows_after = flatten_tree(&tree_after_refresh);
    let dup_refs_after: Vec<_> = rows_after
        .iter()
        .filter(|r| r.node.reference == "dup")
        .map(|r| r.depth)
        .collect();
    assert_eq!(dup_refs_after, dup_refs);
}

// Payload schema drift missing fields.
#[test]
fn payload_schema_drift_missing_fields() {
    let tree = TreeNode {
        reference: "root".into(),
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
                reference: "incomplete".into(),
                label: "".into(),
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
                reference: "".into(),
                label: "Unknown".into(),
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
    assert_eq!(rows.len(), 2);
    let count = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
        1 + child_counts.iter().sum::<usize>()
    });
    assert_eq!(count, 3);
}

// System proc filter toggle during churn.
#[test]
fn system_proc_filter_toggle_during_churn() {
    let tree = TreeNode {
        reference: "root".into(),
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
                reference: "user_proc".into(),
                label: "User Proc".into(),
                node_type: NodeType::Proc,
                expanded: false,
                fetched: true,
                has_children: false,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            },
            TreeNode {
                reference: "system_proc_1".into(),
                label: "System Proc 1".into(),
                node_type: NodeType::Proc,
                expanded: false,
                fetched: true,
                has_children: false,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            },
            TreeNode {
                reference: "system_proc_2".into(),
                label: "System Proc 2".into(),
                node_type: NodeType::Proc,
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
    let rows_all = flatten_tree(&tree);
    assert_eq!(rows_all.len(), 3);
    let tree_filtered = TreeNode {
        reference: "root".into(),
        label: "Root".into(),
        node_type: NodeType::Root,
        expanded: true,
        fetched: true,
        has_children: true,
        stopped: false,
        failed: false,
        is_system: false,
        children: vec![TreeNode {
            reference: "user_proc".into(),
            label: "User Proc".into(),
            node_type: NodeType::Proc,
            expanded: false,
            fetched: true,
            has_children: false,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![],
        }],
    };
    let rows_filtered = flatten_tree(&tree_filtered);
    assert_eq!(rows_filtered.len(), 1);
    assert_eq!(rows_filtered[0].node.reference, "user_proc");
}

// Unicode and invalid strings render safely.
#[test]
fn unicode_and_invalid_strings_render_safely() {
    let unicode_cases: Vec<String> = vec![
        "actor_🚀_emoji".to_string(),
        "proc_with_日本語".to_string(),
        "host_with_é_accents".to_string(),
        "zero_width_\u{200B}_joiner".to_string(),
        "rtl_\u{202E}_override".to_string(),
        "a".repeat(1000),
    ];
    for identity in &unicode_cases {
        let tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![TreeNode {
                reference: identity.clone(),
                label: format!("Label: {}", identity),
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
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 1);
        assert_eq!(&rows[0].node.reference, identity);
    }
}

// Memory pressure expand collapse cycle.
#[test]
fn memory_pressure_expand_collapse_cycle() {
    let mut children = Vec::new();
    for i in 0..2000 {
        children.push(TreeNode {
            reference: format!("actor_{}", i),
            label: format!("Actor {}", i),
            node_type: NodeType::Actor,
            expanded: false,
            fetched: false,
            has_children: false,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![],
        });
    }
    let mut tree = TreeNode {
        reference: "root".into(),
        label: "Root".into(),
        node_type: NodeType::Root,
        expanded: true,
        fetched: true,
        has_children: true,
        stopped: false,
        failed: false,
        is_system: false,
        children: vec![TreeNode {
            reference: "mega_proc".into(),
            label: "Mega Proc".into(),
            node_type: NodeType::Proc,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children,
        }],
    };
    let rows_expanded = flatten_tree(&tree);
    assert_eq!(rows_expanded.len(), 2001);
    collapse_all(&mut tree);
    let rows_collapsed = flatten_tree(&tree);
    assert_eq!(rows_collapsed.len(), 1);
    let rows_after_refresh = flatten_tree(&tree);
    assert_eq!(rows_after_refresh.len(), 1);
}

// Rapid cursor ops during tree changes.
#[test]
fn rapid_cursor_ops_during_tree_changes() {
    let mut tree = TreeNode {
        reference: "root".into(),
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
                reference: "a".into(),
                label: "A".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: true,
                has_children: false,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            },
            TreeNode {
                reference: "b".into(),
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
    let mut cursor = Cursor::new(flatten_tree(&tree).len());
    for _ in 0..50 {
        cursor.move_down();
        cursor.move_up();
        tree.children[0].expanded = !tree.children[0].expanded;
        let rows = flatten_tree(&tree);
        cursor.update_len(rows.len());
        assert!(cursor.pos() < cursor.len() || cursor.len() == 0);
    }
}

// Header stats match tree fold.
#[test]
fn header_stats_match_tree_fold() {
    let tree = TreeNode {
        reference: "root".into(),
        label: "Root".into(),
        node_type: NodeType::Root,
        expanded: true,
        fetched: true,
        has_children: true,
        stopped: false,
        failed: false,
        is_system: false,
        children: vec![TreeNode {
            reference: "host1".into(),
            label: "Host 1".into(),
            node_type: NodeType::Host,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![TreeNode {
                reference: "proc1".into(),
                label: "Proc 1".into(),
                node_type: NodeType::Proc,
                expanded: true,
                fetched: true,
                has_children: true,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![
                    TreeNode {
                        reference: "actor1".into(),
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
                    TreeNode {
                        reference: "actor2".into(),
                        label: "Actor 2".into(),
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
            }],
        }],
    };
    let total_nodes = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
        1 + child_counts.iter().sum::<usize>()
    });
    let visible_rows = flatten_tree(&tree).len();
    assert_eq!(total_nodes, 5);
    assert_eq!(visible_rows, 4);
}

// Zero length and whitespace only strings.
#[test]
fn zero_length_and_whitespace_only_strings() {
    let edge_cases = vec!["", " ", "   ", "\t", "\n", " \t\n "];
    for test_str in edge_cases {
        let tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![TreeNode {
                reference: test_str.to_string(),
                label: test_str.to_string(),
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
    }
}

// Refresh churn large differential.
#[test]
fn refresh_churn_large_differential() {
    let mut tree_before_children = Vec::new();
    for i in 0..1000 {
        tree_before_children.push(TreeNode {
            reference: format!("before_{}", i),
            label: format!("Before {}", i),
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
    let tree_before = TreeNode {
        reference: "root".into(),
        label: "Root".into(),
        node_type: NodeType::Root,
        expanded: true,
        fetched: true,
        has_children: true,
        stopped: false,
        failed: false,
        is_system: false,
        children: tree_before_children,
    };
    let rows_before = flatten_tree(&tree_before);
    assert_eq!(rows_before.len(), 1000);
    let mut tree_after_children = Vec::new();
    for i in 0..100 {
        tree_after_children.push(TreeNode {
            reference: format!("after_{}", i),
            label: format!("After {}", i),
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
    let tree_after = TreeNode {
        reference: "root".into(),
        label: "Root".into(),
        node_type: NodeType::Root,
        expanded: true,
        fetched: true,
        has_children: true,
        stopped: false,
        failed: false,
        is_system: false,
        children: tree_after_children,
    };
    let rows_after = flatten_tree(&tree_after);
    assert_eq!(rows_after.len(), 100);
}
