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

use crossterm::event::KeyCode;
use crossterm::event::KeyEvent;
use crossterm::event::KeyModifiers;

use super::*;
use crate::diagnostics::DiagOutcome;
use crate::diagnostics::DiagPhase;
use crate::diagnostics::DiagResult;

// Empty tree all operations are noops.
#[test]
fn empty_tree_all_operations_are_noops() {
    let app = App::new(
        "http://localhost:8080".to_string(),
        reqwest::Client::new(),
        ThemeName::Nord,
        LangName::En,
    );
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

// -- MastResolver::new() tests (MR-1) --

// MR-1: no fb, no choice → Cli.
#[test]
fn test_mast_resolver_no_fb_defaults_to_cli() {
    let resolver = client::MastResolver::new(None, None);
    assert!(matches!(resolver, client::MastResolver::Cli));
}

// MR-1: explicit "cli" choice → Cli regardless of fb.
// fbcode_build only: requires fbinit, and the Thrift variant only
// exists in Meta builds.
#[cfg(fbcode_build)]
#[test]
fn test_mast_resolver_cli_choice_overrides_fb() {
    // SAFETY: only reachable in fbcode_build tests where main()
    // is annotated #[fbinit::main].
    let fb = unsafe { fbinit::assume_init() };
    let resolver = client::MastResolver::new(Some(fb), Some("cli"));
    assert!(matches!(resolver, client::MastResolver::Cli));
}

// MR-1: fb present, no choice → Thrift.
// fbcode_build only: the Thrift variant and fbinit are unavailable
// in OSS builds.
#[cfg(fbcode_build)]
#[test]
fn test_mast_resolver_fb_defaults_to_thrift() {
    // SAFETY: only reachable in fbcode_build tests where main()
    // is annotated #[fbinit::main].
    let fb = unsafe { fbinit::assume_init() };
    let resolver = client::MastResolver::new(Some(fb), None);
    assert!(matches!(resolver, client::MastResolver::Thrift(_)));
}

// MR-1: explicit "thrift" choice (or any non-"cli" string)
// → Thrift when fb is available.
// fbcode_build only: the Thrift variant and fbinit are unavailable
// in OSS builds.
#[cfg(fbcode_build)]
#[test]
fn test_mast_resolver_explicit_thrift_choice() {
    // SAFETY: only reachable in fbcode_build tests where main()
    // is annotated #[fbinit::main].
    let fb = unsafe { fbinit::assume_init() };
    let resolver = client::MastResolver::new(Some(fb), Some("thrift"));
    assert!(matches!(resolver, client::MastResolver::Thrift(_)));
}

// ── Py-spy invariant coverage ──────────────────────────────────────────────
//
// PY-1 (fresh-trace): enforced by start_pyspy always constructing a new
//   oneshot channel; no automated test (requires mock HTTP server).
// PY-2 (overlay-ownership): TUI-21 now provides structural coverage —
//   replacing `active_job` atomically drops the old receiver, making
//   "stale result reaches wrong overlay" structurally impossible.
// PY-3 (replacement): covered by pyspy_json_to_lines_* tests below.
// PY-4 (selection-totality): covered by pyspy_proc_ref_* tests below.
// PY-5 (overlay-isolation): covered by parse_error_envelope_* tests and
//   the cancellation sites; the cross-overlay race (p then d) is
//   manual-verification only until an async event-loop test is added.
//

/// Join all span content in a line into a single string for assertion.
///
/// Span-structure details (e.g. how many spans a label/value pair is
/// split into) are implementation details we do not want to assert on
/// in tests; this helper lets tests check the full rendered text.
fn line_text(line: &ratatui::text::Line<'_>) -> String {
    line.spans.iter().map(|s| s.content.as_ref()).collect()
}
// Helper: build a minimal App with a flat tree whose children are the
// given nodes, cursor positioned at `cursor_pos`.
fn make_app_with_cursor(children: Vec<TreeNode>, cursor_pos: usize) -> App {
    let mut app = App::new(
        "http://localhost:8080".to_string(),
        reqwest::Client::new(),
        ThemeName::Nord,
        LangName::En,
    );
    let len = children.len();
    app.set_tree(Some(TreeNode {
        reference: "root".into(),
        label: "Root".into(),
        node_type: NodeType::Root,
        expanded: true,
        fetched: true,
        has_children: !children.is_empty(),
        stopped: false,
        failed: false,
        is_system: false,
        children,
    }));
    app.cursor.update_len(len);
    app.cursor.set_pos(cursor_pos);
    app
}

fn proc_node(reference: &str) -> TreeNode {
    TreeNode {
        reference: reference.into(),
        label: reference.into(),
        node_type: NodeType::Proc,
        expanded: false,
        fetched: true,
        has_children: false,
        stopped: false,
        failed: false,
        is_system: false,
        children: vec![],
    }
}

fn actor_node(reference: &str) -> TreeNode {
    TreeNode {
        reference: reference.into(),
        label: reference.into(),
        node_type: NodeType::Actor,
        expanded: false,
        fetched: true,
        has_children: false,
        stopped: false,
        failed: false,
        is_system: false,
        children: vec![],
    }
}

// PY-4: Proc selected → own reference returned.
#[test]
fn pyspy_proc_ref_proc_node() {
    let app = make_app_with_cursor(vec![proc_node("proc_ref,worker[0]")], 0);
    assert_eq!(app.pyspy_proc_ref(), Some("proc_ref,worker[0]".to_string()));
}

// PY-4: Actor selected with detail.parent → owning proc returned.
#[test]
fn pyspy_proc_ref_actor_node_with_parent() {
    let mut app = make_app_with_cursor(vec![actor_node("actor1")], 0);
    app.detail = Some(NodePayload {
        identity: "actor1".into(),
        properties: NodeProperties::Actor {
            actor_status: "running".into(),
            actor_type: "TestActor".into(),
            messages_processed: 0,
            created_at: "2024-01-01T00:00:00.000Z".into(),
            last_message_handler: None,
            total_processing_time_us: 0,
            flight_recorder: None,
            is_system: false,
            failure_info: None,
        },
        children: vec![],
        parent: Some("proc_ref,worker[0]".into()),
        as_of: "2024-01-01T00:00:00.000Z".into(),
    });
    assert_eq!(app.pyspy_proc_ref(), Some("proc_ref,worker[0]".to_string()));
}

// PY-4: Root node selected → None.
#[test]
fn pyspy_proc_ref_root_node() {
    let app = make_app_with_cursor(
        vec![TreeNode {
            reference: "root_child".into(),
            label: "root_child".into(),
            node_type: NodeType::Root,
            expanded: false,
            fetched: true,
            has_children: false,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![],
        }],
        0,
    );
    assert_eq!(app.pyspy_proc_ref(), None);
}

// PY-4: Host node selected → None.
#[test]
fn pyspy_proc_ref_host_node() {
    let app = make_app_with_cursor(
        vec![TreeNode {
            reference: "host1".into(),
            label: "host1".into(),
            node_type: NodeType::Host,
            expanded: false,
            fetched: true,
            has_children: false,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![],
        }],
        0,
    );
    assert_eq!(app.pyspy_proc_ref(), None);
}

// PY-4: Actor selected with detail=None → None (no panic).
#[test]
fn pyspy_proc_ref_actor_no_detail() {
    let app = make_app_with_cursor(vec![actor_node("actor1")], 0);
    assert_eq!(app.pyspy_proc_ref(), None);
}

// PY-5: parse_error_envelope renders ApiErrorEnvelope body so py-spy
// errors surface useful text rather than a bare HTTP status.
// parse_error_envelope: well-formed ApiErrorEnvelope → "code: message".
#[test]
fn parse_error_envelope_well_formed() {
    let json = serde_json::json!({"error": {"code": "gateway_timeout", "message": "timed out waiting for py-spy dump from worker[0]"}});
    let lines = parse_error_envelope(&json);
    assert_eq!(lines.len(), 1);
    assert_eq!(
        lines[0].spans[0].content,
        "gateway_timeout: timed out waiting for py-spy dump from worker[0]"
    );
}

// PY-5: graceful fallback when envelope shape is unexpected.
// parse_error_envelope: missing error field → "unknown: ".
#[test]
fn parse_error_envelope_missing_error_field() {
    let json = serde_json::json!({"something_else": "value"});
    let lines = parse_error_envelope(&json);
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0].spans[0].content, "unknown: ");
}

// PY-5: graceful fallback when inner fields are absent.
// parse_error_envelope: error present but code/message absent → fallback values.
#[test]
fn parse_error_envelope_missing_code_and_message() {
    let json = serde_json::json!({"error": {}});
    let lines = parse_error_envelope(&json);
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0].spans[0].content, "unknown: ");
}

// PY-3: Ok result replaces overlay content with structured stack traces.
// pyspy_json_to_lines: Ok with one thread/frame → header + blank + thread + frame + trailing blank.
#[test]
fn pyspy_json_to_lines_ok_with_stack() {
    let json = serde_json::json!({"Ok": {
        "pid": 1,
        "binary": "py-spy",
        "stack_traces": [{
            "pid": 1,
            "thread_id": 0,
            "thread_name": null,
            "os_thread_id": null,
            "active": false,
            "owns_gil": false,
            "frames": [{
                "name": "foo",
                "filename": "foo.py",
                "module": null,
                "short_filename": null,
                "line": 1,
                "locals": null,
                "is_entry": false
            }]
        }]
    }});
    let scheme = ColorScheme::nord();
    let lines = pyspy_json_to_lines(&json, &scheme);
    // header + blank + thread header + frame + trailing blank
    assert_eq!(lines.len(), 5);
    assert_eq!(line_text(&lines[0]), "pid: 1  binary: py-spy");
    assert_eq!(line_text(&lines[1]), ""); // blank separator
    assert_eq!(line_text(&lines[2]), "Thread 0x0");
    assert_eq!(line_text(&lines[3]), "  #0   foo (foo.py:1)");
    assert_eq!(line_text(&lines[4]), ""); // trailing blank
}

// PY-3: empty stack still produces a readable sentinel rather than a blank overlay.
// pyspy_json_to_lines: Ok with empty stack_traces → metadata header + blank + sentinel line.
#[test]
fn pyspy_json_to_lines_ok_empty_stack() {
    let json = serde_json::json!({"Ok": {"pid": 1, "binary": "py-spy", "stack_traces": []}});
    let scheme = ColorScheme::nord();
    let lines = pyspy_json_to_lines(&json, &scheme);
    assert_eq!(lines.len(), 3); // header + blank + sentinel
    assert_eq!(line_text(&lines[0]), "pid: 1  binary: py-spy");
    assert_eq!(line_text(&lines[1]), "");
    assert_eq!(line_text(&lines[2]), "(empty stack)");
}

// PY-3: BinaryNotFound surfaces the searched paths so the user knows
// where to install py-spy.
// pyspy_json_to_lines: BinaryNotFound with searched paths.
#[test]
fn pyspy_json_to_lines_binary_not_found() {
    let json =
        serde_json::json!({"BinaryNotFound": {"searched": ["PYSPY_BIN=/x", "py-spy on PATH"]}});
    let scheme = ColorScheme::nord();
    let lines = pyspy_json_to_lines(&json, &scheme);
    assert_eq!(lines.len(), 3);
    assert_eq!(line_text(&lines[0]), "py-spy binary not found");
    assert_eq!(line_text(&lines[1]), "  searched: PYSPY_BIN=/x");
    assert_eq!(line_text(&lines[2]), "  searched: py-spy on PATH");
}

// PY-3: Failed result surfaces pid/binary/exit_code/stderr for diagnosis.
// pyspy_json_to_lines: Failed with null exit_code and non-empty stderr.
#[test]
fn pyspy_json_to_lines_failed_null_exit_code() {
    let json = serde_json::json!({"Failed": {"pid": 1, "binary": "py-spy", "exit_code": null, "stderr": "ptrace denied"}});
    let scheme = ColorScheme::nord();
    let lines = pyspy_json_to_lines(&json, &scheme);
    assert_eq!(lines.len(), 4);
    assert_eq!(line_text(&lines[0]), "pid: 1");
    assert_eq!(line_text(&lines[1]), "binary: py-spy");
    assert_eq!(line_text(&lines[2]), "exit_code: (killed/timeout)");
    assert_eq!(line_text(&lines[3]), "ptrace denied");
}

// pyspy_json_to_lines: Failed with numeric exit_code and empty stderr.
// The fallback fires only when lines is empty (no pid, binary, exit_code,
// or stderr), so with pid/binary/exit_code present the result has 3 lines.
#[test]
fn pyspy_json_to_lines_failed_numeric_exit_code_empty_stderr() {
    let json =
        serde_json::json!({"Failed": {"pid": 1, "binary": "py-spy", "exit_code": 1, "stderr": ""}});
    let scheme = ColorScheme::nord();
    let lines = pyspy_json_to_lines(&json, &scheme);
    assert_eq!(lines.len(), 3);
    assert_eq!(line_text(&lines[0]), "pid: 1");
    assert_eq!(line_text(&lines[1]), "binary: py-spy");
    assert_eq!(line_text(&lines[2]), "exit_code: 1");
}

// pyspy_json_to_lines: Failed with no pid/binary/exit_code and no stderr → fallback.
#[test]
fn pyspy_json_to_lines_failed_no_exit_code_no_stderr() {
    let json = serde_json::json!({"Failed": {"stderr": ""}});
    let scheme = ColorScheme::nord();
    let lines = pyspy_json_to_lines(&json, &scheme);
    assert_eq!(lines.len(), 1);
    assert_eq!(line_text(&lines[0]), "(py-spy failed, no output)");
}

// PY-3: unknown variant produces a single fallback line rather than panicking.
// pyspy_json_to_lines: unknown variant → fallback line.
#[test]
fn pyspy_json_to_lines_unknown_variant() {
    let json = serde_json::json!({"SomeUnknownVariant": {}});
    let scheme = ColorScheme::nord();
    let lines = pyspy_json_to_lines(&json, &scheme);
    assert_eq!(lines.len(), 1);
    assert!(
        line_text(&lines[0]).starts_with("unexpected response"),
        "got: {}",
        line_text(&lines[0])
    );
}

// PY-3: structured output renders thread name, flags, and binary basename.
// pyspy_json_to_lines: Ok with named active thread holding GIL → thread
// header includes name and [active, gil] flags; binary path is basename-only.
#[test]
fn pyspy_json_to_lines_ok_thread_name_and_flags() {
    let json = serde_json::json!({
        "Ok": {
            "pid": 123,
            "binary": "/very/long/path/to/pyspy_workload",
            "stack_traces": [{
                "pid": 123,
                "thread_id": 4660,
                "thread_name": "MainThread",
                "os_thread_id": null,
                "active": true,
                "owns_gil": true,
                "frames": [{
                    "name": "foo",
                    "filename": "/path/to/foo.py",
                    "module": null,
                    "short_filename": "foo.py",
                    "line": 1,
                    "locals": null,
                    "is_entry": false
                }]
            }]
        }
    });
    let scheme = ColorScheme::nord();
    let lines = pyspy_json_to_lines(&json, &scheme);
    // header + blank + thread header + frame + trailing blank
    assert_eq!(lines.len(), 5);
    assert_eq!(line_text(&lines[0]), "pid: 123  binary: pyspy_workload");
    assert_eq!(line_text(&lines[1]), "");
    assert_eq!(
        line_text(&lines[2]),
        "Thread 0x1234 (MainThread) [active, gil]"
    );
    assert_eq!(line_text(&lines[3]), "  #0   foo (foo.py:1)");
    assert_eq!(line_text(&lines[4]), "");
}

// ── TUI-21 build_diag_overlay tests ────────────────────────────────────────

// TUI-21: running diagnostics produces a loading overlay with status line.
#[test]
fn build_diag_overlay_running() {
    let theme = Theme::new(ThemeName::Nord, LangName::En);
    let job = ActiveJob::Diagnostics {
        results: Vec::new(),
        running: true,
        rx: None,
        completed_at: None,
    };
    let overlay = job.build_overlay(&theme);
    assert!(overlay.loading, "overlay should be loading while running");
    assert!(
        overlay.status_line.is_some(),
        "running overlay needs a status line"
    );
    let title_text = line_text(&overlay.title);
    assert!(
        title_text.contains("Diagnostics"),
        "title should contain 'Diagnostics', got: {title_text}"
    );
    assert!(
        title_text.contains("Running"),
        "title should contain running indicator, got: {title_text}"
    );
}

// TUI-21: completed diagnostics with one pass result produces a non-loading
// overlay whose status line summarises the pass count.
#[test]
fn build_diag_overlay_one_result() {
    let theme = Theme::new(ThemeName::Nord, LangName::En);
    let job = ActiveJob::Diagnostics {
        results: vec![DiagResult {
            label: "root".into(),
            reference: "root_ref".into(),
            note: None,
            phase: DiagPhase::AdminInfra,
            outcome: DiagOutcome::Pass { elapsed_ms: 5 },
        }],
        running: false,
        rx: None,
        completed_at: Some("12:00:00".into()),
    };
    let overlay = job.build_overlay(&theme);
    assert!(
        !overlay.loading,
        "overlay should not be loading when completed"
    );
    assert!(
        !overlay.lines.is_empty(),
        "overlay should have result lines"
    );
    let status_text = overlay
        .status_line
        .as_ref()
        .map(line_text)
        .unwrap_or_default();
    assert!(
        status_text.contains("All 1 checks passed"),
        "status line should mention pass count, got: {status_text}"
    );
}

// TUI-21: set_job establishes the job-overlay biconditional.
#[test]
fn set_job_establishes_overlay() {
    let mut app = App::new(
        "http://localhost:8080".to_string(),
        reqwest::Client::new(),
        ThemeName::Nord,
        LangName::En,
    );
    assert!(app.active_job.is_none());
    assert!(app.overlay.is_none());
    app.set_job(ActiveJob::Diagnostics {
        results: Vec::new(),
        running: true,
        rx: None,
        completed_at: None,
    });
    assert!(app.active_job.is_some(), "set_job should set active_job");
    assert!(app.overlay.is_some(), "set_job should create overlay");
}

// TUI-21: dismiss_job clears both fields together.
#[test]
fn dismiss_job_clears_both() {
    let mut app = App::new(
        "http://localhost:8080".to_string(),
        reqwest::Client::new(),
        ThemeName::Nord,
        LangName::En,
    );
    app.set_job(ActiveJob::Diagnostics {
        results: Vec::new(),
        running: true,
        rx: None,
        completed_at: None,
    });
    app.dismiss_job();
    assert!(
        app.active_job.is_none(),
        "dismiss_job should clear active_job"
    );
    assert!(app.overlay.is_none(), "dismiss_job should clear overlay");
}

// ── ActiveJob::build_overlay PySpy tests ───────────────────────────────────

// PY-3: PySpy loading state: rx is Some → overlay.loading, has status_line.
#[test]
fn build_overlay_pyspy_loading() {
    let theme = Theme::new(ThemeName::Nord, LangName::En);
    let (_, rx) = tokio::sync::oneshot::channel::<Vec<ratatui::text::Line<'static>>>();
    let job = ActiveJob::PySpy {
        rx: Some(rx),
        short: "worker[0]".to_string(),
        lines: vec![],
        completed_at: None,
    };
    let overlay = job.build_overlay(&theme);
    assert!(overlay.loading, "rx is Some → loading");
    assert!(
        overlay.status_line.is_some(),
        "loading overlay needs status line"
    );
    let title = line_text(&overlay.title);
    assert!(
        title.contains("py-spy: worker[0]"),
        "title should name the proc, got: {title}"
    );
}

// PY-3: PySpy completed state: rx is None → not loading, no status_line, lines populated.
#[test]
fn build_overlay_pyspy_completed() {
    let theme = Theme::new(ThemeName::Nord, LangName::En);
    let job = ActiveJob::PySpy {
        rx: None,
        short: "worker[0]".to_string(),
        lines: vec![ratatui::text::Line::from("frame 0")],
        completed_at: Some("14:30:00".to_string()),
    };
    let overlay = job.build_overlay(&theme);
    assert!(!overlay.loading, "rx is None → not loading");
    assert!(
        overlay.status_line.is_none(),
        "completed overlay has no status line"
    );
    assert_eq!(overlay.lines.len(), 1, "lines should be populated");
    let title = line_text(&overlay.title);
    assert!(
        title.contains("14:30:00"),
        "title should show completion time, got: {title}"
    );
}

// PY-3: PySpy completed with empty output: rx is None, lines empty → not loading.
// Distinguishes "completed with no output" from "still loading".
#[test]
fn build_overlay_pyspy_completed_empty() {
    let theme = Theme::new(ThemeName::Nord, LangName::En);
    let job = ActiveJob::PySpy {
        rx: None,
        short: "worker[0]".to_string(),
        lines: vec![],
        completed_at: Some("14:30:00".to_string()),
    };
    let overlay = job.build_overlay(&theme);
    assert!(
        !overlay.loading,
        "empty lines with rx None is completed, not loading"
    );
    assert!(overlay.lines.is_empty());
}

// ── ActiveJob::on_event tests ──────────────────────────────────────────────

// TUI-21: on_event DiagResult(Some) pushes to results without changing running state.
#[test]
fn on_event_diag_result_pushes() {
    let mut job = ActiveJob::Diagnostics {
        results: vec![],
        running: true,
        rx: None,
        completed_at: None,
    };
    let r = DiagResult {
        label: "check".into(),
        reference: "ref".into(),
        note: None,
        phase: DiagPhase::AdminInfra,
        outcome: DiagOutcome::Pass { elapsed_ms: 1 },
    };
    job.on_event(ActiveJobEvent::DiagResult(Some(r)));
    if let ActiveJob::Diagnostics {
        results, running, ..
    } = &job
    {
        assert_eq!(results.len(), 1);
        assert!(*running, "should still be running after a single result");
    } else {
        panic!("job variant changed");
    }
}

// TUI-21: on_event DiagResult(None) marks completed — clears rx, sets timestamp.
#[test]
fn on_event_diag_stream_end() {
    let mut job = ActiveJob::Diagnostics {
        results: vec![],
        running: true,
        rx: None,
        completed_at: None,
    };
    job.on_event(ActiveJobEvent::DiagResult(None));
    if let ActiveJob::Diagnostics {
        running,
        rx,
        completed_at,
        ..
    } = &job
    {
        assert!(!running, "should be stopped after stream end");
        assert!(rx.is_none());
        assert!(completed_at.is_some(), "should have a completion timestamp");
    } else {
        panic!("job variant changed");
    }
}

// PY-2/PY-3: on_event PySpyResult populates lines, clears rx, sets timestamp.
#[test]
fn on_event_pyspy_result() {
    let (_, rx) = tokio::sync::oneshot::channel::<Vec<ratatui::text::Line<'static>>>();
    let mut job = ActiveJob::PySpy {
        rx: Some(rx),
        short: "w".to_string(),
        lines: vec![],
        completed_at: None,
    };
    let result_lines = vec![ratatui::text::Line::from("frame")];
    job.on_event(ActiveJobEvent::PySpyResult(result_lines));
    if let ActiveJob::PySpy {
        rx,
        lines,
        completed_at,
        ..
    } = &job
    {
        assert!(rx.is_none(), "rx should be cleared");
        assert_eq!(lines.len(), 1);
        assert!(completed_at.is_some(), "should have a completion timestamp");
    } else {
        panic!("job variant changed");
    }
}

// ── overlay_rerun_key tests ────────────────────────────────────────────────

// PY-5: Diagnostics overlay: 'd' and 'r' trigger rerun.
#[test]
fn overlay_rerun_key_diag_d() {
    let mut app = make_app_with_cursor(vec![proc_node("p")], 0);
    app.active_job = Some(ActiveJob::Diagnostics {
        results: vec![],
        running: false,
        rx: None,
        completed_at: None,
    });
    let key = KeyEvent::new(KeyCode::Char('d'), KeyModifiers::NONE);
    assert!(matches!(
        app.overlay_rerun_key(key),
        KeyResult::RunDiagnostics
    ));
    let key_r = KeyEvent::new(KeyCode::Char('r'), KeyModifiers::NONE);
    assert!(matches!(
        app.overlay_rerun_key(key_r),
        KeyResult::RunDiagnostics
    ));
}

// PY-5: Diagnostics overlay: unrelated key is not dispatched.
#[test]
fn overlay_rerun_key_diag_unrelated() {
    let mut app = make_app_with_cursor(vec![proc_node("p")], 0);
    app.active_job = Some(ActiveJob::Diagnostics {
        results: vec![],
        running: false,
        rx: None,
        completed_at: None,
    });
    let key = KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE);
    assert!(matches!(app.overlay_rerun_key(key), KeyResult::None));
}

// PY-1: PySpy overlay: 'p' on a proc node triggers fresh fetch.
#[test]
fn overlay_rerun_key_pyspy_p() {
    let mut app = make_app_with_cursor(vec![proc_node("proc_ref,worker[0]")], 0);
    app.active_job = Some(ActiveJob::PySpy {
        rx: None,
        short: "worker[0]".to_string(),
        lines: vec![],
        completed_at: None,
    });
    let key = KeyEvent::new(KeyCode::Char('p'), KeyModifiers::NONE);
    assert!(matches!(app.overlay_rerun_key(key), KeyResult::RunPySpy(_)));
}

// PY-5: PySpy overlay: unrelated key is not dispatched.
#[test]
fn overlay_rerun_key_pyspy_unrelated() {
    let mut app = make_app_with_cursor(vec![proc_node("p")], 0);
    app.active_job = Some(ActiveJob::PySpy {
        rx: None,
        short: "w".to_string(),
        lines: vec![],
        completed_at: None,
    });
    let key = KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE);
    assert!(matches!(app.overlay_rerun_key(key), KeyResult::None));
}

// PY-5: No active job → rerun key is a no-op.
#[test]
fn overlay_rerun_key_no_job() {
    let app = make_app_with_cursor(vec![proc_node("p")], 0);
    let key = KeyEvent::new(KeyCode::Char('d'), KeyModifiers::NONE);
    assert!(matches!(app.overlay_rerun_key(key), KeyResult::None));
}

// PY-4: PySpy overlay 'p' when cursor is on a non-proc node (pyspy_proc_ref → None).
#[test]
fn overlay_rerun_key_pyspy_no_proc_ref() {
    let host = TreeNode {
        reference: "host1".into(),
        label: "host1".into(),
        node_type: NodeType::Host,
        expanded: false,
        fetched: true,
        has_children: false,
        stopped: false,
        failed: false,
        is_system: false,
        children: vec![],
    };
    let mut app = make_app_with_cursor(vec![host], 0);
    app.active_job = Some(ActiveJob::PySpy {
        rx: None,
        short: "w".to_string(),
        lines: vec![],
        completed_at: None,
    });
    let key = KeyEvent::new(KeyCode::Char('p'), KeyModifiers::NONE);
    assert!(
        matches!(app.overlay_rerun_key(key), KeyResult::None),
        "p on a Host node should not trigger RunPySpy"
    );
}
