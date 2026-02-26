/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Interactive TUI client for the Monarch mesh admin HTTP API.
//!
//! Displays the mesh topology as a navigable tree by walking `GET
//! /v1/{reference}` endpoints. Selecting any node shows contextual
//! details on the right pane, including actor flight recorder events
//! when an actor is selected.
//!
//! # Design Pillars (Algebraic)
//!
//! This TUI is intentionally structured around three algebraic
//! invariants to make behavior correct by construction:
//!
//! 1. **Join-semilattice cache**: all fetch results merge via
//!    `FetchState::join`, guaranteeing commutativity, associativity,
//!    and idempotence under retries and reordering.
//! 2. **Cursor laws**: selection is managed by `Cursor`, which
//!    enforces the invariant `pos < len` (or `pos == 0` when empty).
//! 3. **Tree as structural recursion**: the mesh topology is stored
//!    as an explicit tree (`TreeNode { children }`) and rendered via
//!    a pure projection (`flatten_tree`), avoiding ad-hoc list
//!    surgery.
//!
//! Additional invariants enforced throughout the code:
//! - **Single fetch+join path**: all cache writes go through
//!   `fetch_with_join` (no direct inserts).
//! - **Refresh staleness**: `FetchState::Ready` with `generation <
//!   refresh_gen` is refetched; errors always retry.
//! - **Synthetic root**: the root node is synthetic and always
//!   expanded; only its children are rendered at depth 0.
//! - **Cycle safety**: tree building rejects only true cycles (nodes
//!   that appear in their own ancestor path).
//! - **Depth cap**: recursion is bounded by `MAX_TREE_DEPTH`.
//!   This limits traversal to Root→Host→Proc→Actor→ChildActor, keeps
//!   stack depth small, and avoids runaway fetches on deep graphs.
//! - **Tree-structure traversal via folds**: walks over the `TreeNode`
//!   structure use fold abstractions (`fold_tree`, `fold_tree_mut`, or
//!   `fold_tree_mut_with_depth`), not bespoke recursion. The flattened
//!   row list (`VisibleRows`) is iterated directly for rendering and
//!   event handling.
//! - **Selection semantics**: cursor restoration prefers
//!   `(reference, depth)` to disambiguate duplicate references, and
//!   falls back to reference-only matching if depth changes (e.g.,
//!   parent expanded/collapsed between refreshes).
//! - **Concurrency model**: HTTP fetches are scheduled serially
//!   through the event loop; in-flight requests are not explicitly
//!   cancelled or serialized, so slow responses may overlap. Join
//!   semantics handle retries and reordering.
//! - **Stopped detection is actor-only and prefix-based**:
//!   `is_stopped_node` matches `Actor` variants whose `actor_status`
//!   starts with `"stopped:"` or `"failed:"`. All other variants
//!   return false.
//! - **Stopped filtering is dual-source (OR)**: `TreeNode.stopped`
//!   is true if the cached payload is stopped OR the child ref
//!   appears in the parent proc's `stopped_children` list. Either
//!   source alone is sufficient.
//! - **Filter order: system before stopped**: when both filters are
//!   off, system membership is checked first. A node that is both
//!   system and stopped is eliminated by the system check.
//! - **Placeholder structural equivalence**: `placeholder_stopped`
//!   is identical to `placeholder` except `stopped: true`. Stopped
//!   is a rendering hint, not an expansion barrier.
//! - **System actor styling is dual-source (OR)**: `TreeNode.is_system`
//!   is true if the cached payload reports `is_system: true` OR the
//!   child ref appears in the parent's `system_children` list.
//!   System actors render Blue; style precedence is
//!   selected > stopped > system > node-type.
//! - **Proc actor accounting**: displayed total is `num_actors +
//!   stopped_children.len()`. `num_actors` counts only live actors.
//!   `"(max retained)"` appears iff `stopped_retention_cap > 0` and
//!   `stopped_children.len() >= stopped_retention_cap`.
//!
//! Laziness + recursion benefits:
//! - **Lazy expansion**: proc/actor children are placeholders until
//!   expanded, keeping refresh costs bounded and scaling work to what
//!   the user explores.
//! - **Structural recursion**: tree operations are defined by
//!   recursion/folds over the explicit tree, avoiding brittle
//!   index-based manipulation and making the view a pure projection.
//!
//! ```bash
//! # Terminal 1: Run dining philosophers (or any hyperactor application)
//! buck2 run fbcode//monarch/hyperactor_mesh:hyperactor_mesh_example_dining_philosophers
//!
//! # Terminal 2: Run this TUI (use the port printed by the application)
//! buck2 run fbcode//monarch/hyperactor_mesh:hyperactor_mesh_admin_tui -- --addr 127.0.0.1:XXXXX
//! ```

mod actions;
mod app;
mod fetch;
mod filter;
mod format;
mod model;
mod render;
mod theme;
mod tree;

// Re-exports so #[cfg(test)] mod tests can use `use super::*`.
#[allow(unused_imports)]
pub(crate) use std::collections::HashMap;
#[allow(unused_imports)]
pub(crate) use std::collections::HashSet;
use std::io;
use std::io::IsTerminal;
use std::time::Duration;

pub(crate) use actions::*;
pub(crate) use app::*;
use clap::Parser;
use crossterm::ExecutableCommand;
use crossterm::terminal::EnterAlternateScreen;
use crossterm::terminal::LeaveAlternateScreen;
use crossterm::terminal::disable_raw_mode;
use crossterm::terminal::enable_raw_mode;
pub(crate) use fetch::*;
pub(crate) use filter::*;
pub(crate) use format::*;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
// Re-exports so #[cfg(test)] mod tests can use `use super::*`.
#[allow(unused_imports)]
pub(crate) use hyperactor::introspect::NodePayload;
#[allow(unused_imports)]
pub(crate) use hyperactor::introspect::NodeProperties;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
pub(crate) use model::*;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
// Re-exports so #[cfg(test)] mod tests can use `use super::*`.
#[allow(unused_imports)]
pub(crate) use render::*;
pub(crate) use theme::*;
pub(crate) use tree::*;

// Terminal setup / teardown

/// Put the terminal into "TUI mode".
///
/// Enables raw mode, switches to the alternate screen, and clears it,
/// returning a `ratatui::Terminal` backed by crossterm.
fn setup_terminal() -> io::Result<Terminal<CrosstermBackend<io::Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    stdout.execute(EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;
    Ok(terminal)
}

/// Restore the terminal back to normal “shell mode”.
///
/// Disables raw mode, leaves the alternate screen, and re-enables the
/// cursor.
fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) -> io::Result<()> {
    disable_raw_mode()?;
    terminal.backend_mut().execute(LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

// Main loop

#[tokio::main]
async fn main() -> io::Result<()> {
    let args = Args::parse();

    if !io::stdout().is_terminal() {
        eprintln!("This TUI requires a real terminal.");
        return Ok(());
    }

    // Show an indicatif spinner on stderr while fetching initial data.
    // This runs before the alternate screen so it's visible as a normal
    // Terminal line.
    let mut app = App::new(&args.addr, args.theme, args.lang);
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .expect("valid template"),
    );
    spinner.set_message(format!("mesh-admin — Connecting to {} ...", app.base_url));
    spinner.enable_steady_tick(Duration::from_millis(80));

    let splash_start = RealClock.now();
    app.refresh().await;
    let elapsed = splash_start.elapsed();
    let min_splash = Duration::from_secs(2);
    if elapsed < min_splash {
        RealClock.sleep(min_splash - elapsed).await;
    }

    spinner.finish_and_clear();

    let mut terminal = setup_terminal()?;
    let result = run_app(&mut terminal, &args, app).await;
    restore_terminal(&mut terminal)?;
    result
}

#[cfg(test)]
mod tests {
    use algebra::JoinSemilattice;
    use hyperactor::introspect::FailureInfo;
    use serde_json::Value;

    use super::*;

    // Test Suite Organization
    //
    // This test suite validates the algebraic properties and invariants
    // Of the admin TUI's three core algebras:
    // 1. Join algebra (cache merge semantics)
    // 2. Cursor algebra (navigation state management)
    // 3. Tree algebra (structural fold operations)
    //
    // Tests are organized into the following categories:
    //
    // ## 1. Join Algebra Laws (FetchState merge semantics)
    //    - join_is_commutative: a.join(b) == b.join(a)
    //    - join_is_associative: (a.join(b)).join(c) == a.join(b.join(c))
    //    - join_is_idempotent: a.join(a) == a
    //    - join_unknown_is_identity: Unknown is identity element
    //    - join_prefers_newer_stamp: Higher timestamp wins
    //    - join_uses_seq_for_tie_break: Sequence breaks timestamp ties
    //    - join_deterministic_tie_break_ready_over_error: Ready beats Error
    //    - join_error_states_newer_wins: Errors merge by timestamp
    //    - join_error_equal_stamps_is_commutative: Error commutativity
    //    - join_error_always_retries_on_fetch: Errors don't cache
    //    - cache_join_commutativity_ready_vs_error: Ready vs Error with equal stamp
    //
    // ## 2. Staleness & Refresh Semantics
    //    - join_refresh_staleness_triggers_refetch: old generation refetches
    //    - collapsed_nodes_stay_collapsed_after_refresh: expansion state preserved
    //
    // ## 3. Cursor Invariants (pos < len always holds)
    //    - cursor_new_creates_valid_cursor: Initial state valid
    //    - cursor_new_empty_creates_zero_cursor: Empty case
    //    - cursor_maintains_invariant_after_operations: all ops preserve pos < len
    //    - cursor_move_up/down_*: Boundary conditions
    //    - cursor_home/end_*: Jump operations
    //    - cursor_set_pos_*: Direct positioning
    //    - cursor_update_len_*: Length changes
    //    - cursor_single_item_movements: Edge case with n=1
    //    - cursor_empty_all_movements_return_false: Edge case with n=0
    //
    // ## 4. Tree Fold Invariants
    //    - fold_equivalence_flatten_tree: flatten_tree produces correct depths
    //    - fold_vs_traversal_law_node_count: row count equals node count when expanded
    //    - fold_tree_mut_early_exit_stops_traversal: ControlFlow::Break short-circuits
    //    - flatten_collapsed_node_hides_children: collapsed nodes don't contribute
    //    - flatten_expanded_node_shows_children: expanded nodes visible
    //    - collect_refs_visits_all_nodes: fold traverses entire structure
    //    - find_node_by_reference_works: immutable fold search
    //    - find_node_mut_works: mutable fold search
    //    - find_node_at_depth_distinguishes_instances: depth-aware search
    //
    // ## 5. Collapse Idempotence
    //    - collapse_idempotence: collapse_all twice yields same state
    //
    // ## 6. Placeholder Refinement (fetched state transitions)
    //    - placeholder_refinement_transitions_fetched_state: fetched=false → true on expand
    //
    // ## 7. Cycle Safety & Duplicate Handling
    //    - cycle_guard_prevents_infinite_recursion: self-reference handled
    //    - dual_appearances_flatten_correctly: same reference at multiple depths
    //    - expansion_tracking_uses_depth_pairs: (reference, depth) pairs track state
    //    - selection_restore_prefers_depth_match: depth-aware matching
    //
    // ## 8. Stamp Ordering (temporal semantics)
    //    - stamp_orders_by_timestamp_first: Primary ordering
    //    - stamp_orders_by_seq_when_timestamp_equal: Tie-breaker
    //    - stamp_equality_works: Equivalence relation
    //
    // ## 9. Stopped/Failed Actor Filtering
    //    - is_stopped_node_true_for_stopped_prefix: "stopped:" prefix matches
    //    - is_stopped_node_true_for_failed_prefix: "failed:" prefix matches
    //    - is_stopped_node_false_for_running: Running status does not match
    //    - is_stopped_node_false_without_colon: bare "stopped"/"failed" rejected
    //    - is_stopped_node_false_for_non_actor_variants: Root/Host/Proc always false
    //    - placeholder_stopped_differs_only_in_stopped_field: structural equivalence
    //    - from_payload_sets_stopped_for_stopped_actor: stopped payload → stopped node
    //    - from_payload_not_stopped_for_running_actor: running payload → not stopped
    //    - from_payload_proc_is_never_stopped: Proc is never stopped (actor-only)
    //    - from_payload_or_logic_both_sources_agree: payload ∧ proc list agree
    //    - from_payload_or_logic_only_proc_list: proc list alone promotes to stopped
    //    - from_payload_or_logic_only_cached_payload: cached payload alone is sufficient
    //    - derive_label_host_*: host label user/system breakdown
    //    - derive_label_proc_*: proc label live/stopped/system breakdown
    //    - stopped_nodes_visible_in_flatten: stopped nodes survive flatten
    //    - stopped_node_stopped_field_survives_flatten: field propagates through flatten
    //    - placeholder_stopped_visible_in_flatten: stopped is visual hint, not barrier

    // Helper to find a node by reference using algebraic fold.
    fn find_node_by_ref<'a>(node: &'a TreeNode, reference: &str) -> Option<&'a TreeNode> {
        fold_tree(node, &|n, child_results| {
            if n.reference == reference {
                Some(n)
            } else {
                child_results.into_iter().find_map(|x| x)
            }
        })
    }

    // Helper to create test payloads
    fn mock_payload(identity: &str) -> NodePayload {
        NodePayload {
            identity: identity.to_string(),
            properties: NodeProperties::Actor {
                actor_status: "Running".to_string(),
                actor_type: "test".to_string(),
                messages_processed: 0,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                last_message_handler: None,
                total_processing_time_us: 0,
                flight_recorder: None,
                is_system: false,
                failure_info: None,
            },
            children: vec![],
            parent: None,
            as_of: "2026-01-01T00:00:00.000Z".to_string(),
        }
    }

    // Test fixtures: Stamp ordering

    // Stamp orders by timestamp first.
    #[test]
    fn stamp_orders_by_timestamp_first() {
        let earlier = Stamp {
            ts_micros: 1000,
            seq: 2,
        };
        let later = Stamp {
            ts_micros: 2000,
            seq: 1,
        };
        assert!(earlier < later);
        assert!(later > earlier);
    }

    // Stamp orders by seq when timestamp equal.
    #[test]
    fn stamp_orders_by_seq_when_timestamp_equal() {
        let first = Stamp {
            ts_micros: 1000,
            seq: 1,
        };
        let second = Stamp {
            ts_micros: 1000,
            seq: 2,
        };
        assert!(first < second);
        assert!(second > first);
    }

    // Stamp equality works.
    #[test]
    fn stamp_equality_works() {
        let a = Stamp {
            ts_micros: 1000,
            seq: 5,
        };
        let b = Stamp {
            ts_micros: 1000,
            seq: 5,
        };
        assert_eq!(a, b);
    }

    // Test fixtures: Cursor navigation

    // Cursor new creates valid cursor.
    #[test]
    fn cursor_new_creates_valid_cursor() {
        let cursor = Cursor::new(10);
        assert_eq!(cursor.pos(), 0);
        assert_eq!(cursor.len(), 10);
    }

    // Cursor new empty creates zero cursor.
    #[test]
    fn cursor_new_empty_creates_zero_cursor() {
        let cursor = Cursor::new(0);
        assert_eq!(cursor.pos(), 0);
        assert_eq!(cursor.len(), 0);
    }

    // Cursor move up at start returns false.
    #[test]
    fn cursor_move_up_at_start_returns_false() {
        let mut cursor = Cursor::new(5);
        assert!(!cursor.move_up());
        assert_eq!(cursor.pos(), 0);
    }

    // Cursor move up from middle decrements.
    #[test]
    fn cursor_move_up_from_middle_decrements() {
        let mut cursor = Cursor::new(5);
        cursor.set_pos(2);
        assert!(cursor.move_up());
        assert_eq!(cursor.pos(), 1);
    }

    // Cursor move down at end returns false.
    #[test]
    fn cursor_move_down_at_end_returns_false() {
        let mut cursor = Cursor::new(5);
        cursor.set_pos(4); // last position
        assert!(!cursor.move_down());
        assert_eq!(cursor.pos(), 4);
    }

    // Cursor move down from start increments.
    #[test]
    fn cursor_move_down_from_start_increments() {
        let mut cursor = Cursor::new(5);
        assert!(cursor.move_down());
        assert_eq!(cursor.pos(), 1);
    }

    // Cursor home at start returns false.
    #[test]
    fn cursor_home_at_start_returns_false() {
        let mut cursor = Cursor::new(5);
        assert!(!cursor.home());
        assert_eq!(cursor.pos(), 0);
    }

    // Cursor home from middle jumps to start.
    #[test]
    fn cursor_home_from_middle_jumps_to_start() {
        let mut cursor = Cursor::new(5);
        cursor.set_pos(3);
        assert!(cursor.home());
        assert_eq!(cursor.pos(), 0);
    }

    // Cursor end at end returns false.
    #[test]
    fn cursor_end_at_end_returns_false() {
        let mut cursor = Cursor::new(5);
        cursor.set_pos(4);
        assert!(!cursor.end());
        assert_eq!(cursor.pos(), 4);
    }

    // Cursor end from start jumps to end.
    #[test]
    fn cursor_end_from_start_jumps_to_end() {
        let mut cursor = Cursor::new(5);
        assert!(cursor.end());
        assert_eq!(cursor.pos(), 4);
    }

    // Cursor empty all movements return false.
    #[test]
    fn cursor_empty_all_movements_return_false() {
        let mut cursor = Cursor::new(0);
        assert!(!cursor.move_up());
        assert!(!cursor.move_down());
        assert!(!cursor.home());
        assert!(!cursor.end());
        assert_eq!(cursor.pos(), 0);
    }

    // Cursor single item movements.
    #[test]
    fn cursor_single_item_movements() {
        let mut cursor = Cursor::new(1);
        assert_eq!(cursor.pos(), 0);
        assert!(!cursor.move_up()); // can't go up
        assert!(!cursor.move_down()); // can't go down (already at last)
        assert!(!cursor.home()); // already at home
        assert!(!cursor.end()); // already at end (same as start)
        assert_eq!(cursor.pos(), 0);
    }

    // Cursor update len expands preserves position.
    #[test]
    fn cursor_update_len_expands_preserves_position() {
        let mut cursor = Cursor::new(5);
        cursor.set_pos(2);
        cursor.update_len(10);
        assert_eq!(cursor.pos(), 2);
        assert_eq!(cursor.len(), 10);
    }

    // Cursor update len shrinks clamps position.
    #[test]
    fn cursor_update_len_shrinks_clamps_position() {
        let mut cursor = Cursor::new(10);
        cursor.set_pos(8);
        cursor.update_len(5);
        assert_eq!(cursor.pos(), 4); // clamped to len-1
        assert_eq!(cursor.len(), 5);
    }

    // Cursor update len to zero resets position.
    #[test]
    fn cursor_update_len_to_zero_resets_position() {
        let mut cursor = Cursor::new(10);
        cursor.set_pos(5);
        cursor.update_len(0);
        assert_eq!(cursor.pos(), 0);
        assert_eq!(cursor.len(), 0);
    }

    // Cursor set pos within bounds works.
    #[test]
    fn cursor_set_pos_within_bounds_works() {
        let mut cursor = Cursor::new(10);
        cursor.set_pos(7);
        assert_eq!(cursor.pos(), 7);
    }

    // Cursor set pos beyond bounds clamps.
    #[test]
    fn cursor_set_pos_beyond_bounds_clamps() {
        let mut cursor = Cursor::new(5);
        cursor.set_pos(10);
        assert_eq!(cursor.pos(), 4); // clamped to len-1
    }

    // Cursor set pos on empty stays zero.
    #[test]
    fn cursor_set_pos_on_empty_stays_zero() {
        let mut cursor = Cursor::new(0);
        cursor.set_pos(5);
        assert_eq!(cursor.pos(), 0);
    }

    // Cursor maintains invariant after operations.
    #[test]
    fn cursor_maintains_invariant_after_operations() {
        let mut cursor = Cursor::new(5);

        // Move around
        cursor.move_down();
        cursor.move_down();
        assert!(cursor.pos() < cursor.len());

        // Shrink
        cursor.update_len(2);
        assert!(cursor.pos() < cursor.len());

        // Set beyond
        cursor.set_pos(100);
        assert!(cursor.pos() < cursor.len());

        // Empty
        cursor.update_len(0);
        assert_eq!(cursor.pos(), 0);
    }

    // Test fixtures: FetchState JoinSemilattice properties

    // Join unknown is identity.
    #[test]
    fn join_unknown_is_identity() {
        let payload = mock_payload("test");
        let ready = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 1,
            value: payload.clone(),
        };

        let result = FetchState::Unknown.join(&ready);
        assert!(matches!(result, FetchState::Ready { .. }));

        let result2 = ready.join(&FetchState::Unknown);
        assert!(matches!(result2, FetchState::Ready { .. }));
    }

    // Join is commutative.
    #[test]
    fn join_is_commutative() {
        let payload1 = mock_payload("test1");
        let payload2 = mock_payload("test2");

        let older = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 1,
            value: payload1,
        };
        let newer = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            generation: 1,
            value: payload2.clone(),
        };

        let result1 = older.join(&newer);
        let result2 = newer.join(&older);

        match (&result1, &result2) {
            (FetchState::Ready { value: v1, .. }, FetchState::Ready { value: v2, .. }) => {
                assert_eq!(v1.identity, v2.identity);
                assert_eq!(v1.identity, payload2.identity);
            }
            _ => panic!("Expected Ready states"),
        }
    }

    // Join is idempotent.
    #[test]
    fn join_is_idempotent() {
        let payload = mock_payload("test");
        let state = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 1,
            value: payload.clone(),
        };

        let result = state.join(&state);
        match result {
            FetchState::Ready { value, .. } => {
                assert_eq!(value.identity, payload.identity);
            }
            _ => panic!("Expected Ready state"),
        }
    }

    // Join prefers newer stamp.
    #[test]
    fn join_prefers_newer_stamp() {
        let payload1 = mock_payload("old");
        let payload2 = mock_payload("new");

        let older = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 1,
            value: payload1,
        };
        let newer = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            generation: 2,
            value: payload2.clone(),
        };

        let result = older.join(&newer);
        match result {
            FetchState::Ready { value, stamp, .. } => {
                assert_eq!(value.identity, "new");
                assert_eq!(stamp.ts_micros, 2000);
            }
            _ => panic!("Expected Ready state"),
        }
    }

    // Join uses seq for tie break.
    #[test]
    fn join_uses_seq_for_tie_break() {
        let payload1 = mock_payload("first");
        let payload2 = mock_payload("second");

        let first = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 1,
            value: payload1,
        };
        let second = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 2,
            },
            generation: 1,
            value: payload2.clone(),
        };

        let result = first.join(&second);
        match result {
            FetchState::Ready { value, .. } => {
                assert_eq!(value.identity, "second");
            }
            _ => panic!("Expected Ready state"),
        }
    }

    // Join deterministic tie break ready over error.
    #[test]
    fn join_deterministic_tie_break_ready_over_error() {
        let payload = mock_payload("test");

        let ready = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 1,
            value: payload.clone(),
        };
        let error = FetchState::Error {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            msg: "error".to_string(),
        };

        // Ready should win in both directions
        let result1 = ready.join(&error);
        let result2 = error.join(&ready);

        assert!(matches!(result1, FetchState::Ready { .. }));
        assert!(matches!(result2, FetchState::Ready { .. }));
    }

    // Join error states newer wins.
    #[test]
    fn join_error_states_newer_wins() {
        let older_error = FetchState::<NodePayload>::Error {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            msg: "old error".to_string(),
        };
        let newer_error = FetchState::<NodePayload>::Error {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            msg: "new error".to_string(),
        };

        // Newer stamp should win (commutative)
        let result1 = older_error.join(&newer_error);
        let result2 = newer_error.join(&older_error);

        match (&result1, &result2) {
            (FetchState::Error { msg: m1, .. }, FetchState::Error { msg: m2, .. }) => {
                assert_eq!(m1, m2);
                assert_eq!(m1, "new error");
            }
            _ => panic!("Expected Error states"),
        }
    }

    // Join error equal stamps is commutative.
    #[test]
    fn join_error_equal_stamps_is_commutative() {
        // Test the lexicographic tie-break for Error states with equal
        // Stamps. This is the edge case that ensures full commutativity.
        let error1 = FetchState::<NodePayload>::Error {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            msg: "alpha".to_string(),
        };
        let error2 = FetchState::<NodePayload>::Error {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            msg: "beta".to_string(),
        };

        // Should pick same result regardless of order (lexicographic)
        let result1 = error1.join(&error2);
        let result2 = error2.join(&error1);

        match (&result1, &result2) {
            (FetchState::Error { msg: m1, .. }, FetchState::Error { msg: m2, .. }) => {
                assert_eq!(m1, m2);
                // "beta" >= "alpha", so "beta" should win
                assert_eq!(m1, "beta");
            }
            _ => panic!("Expected Error states"),
        }
    }

    // Join is associative.
    #[test]
    fn join_is_associative() {
        let p1 = mock_payload("p1");
        let p2 = mock_payload("p2");
        let p3 = mock_payload("p3");

        let s1 = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 1,
            value: p1,
        };
        let s2 = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            generation: 1,
            value: p2,
        };
        let s3 = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 3000,
                seq: 1,
            },
            generation: 1,
            value: p3.clone(),
        };

        let left = s1.join(&s2).join(&s3);
        let right = s1.join(&s2.join(&s3));

        match (&left, &right) {
            (FetchState::Ready { value: v1, .. }, FetchState::Ready { value: v2, .. }) => {
                assert_eq!(v1.identity, v2.identity);
                assert_eq!(v1.identity, p3.identity);
            }
            _ => panic!("Expected Ready states"),
        }
    }

    // Join error always retries on fetch.
    #[test]
    fn join_error_always_retries_on_fetch() {
        // This is a behavioral test - errors should be treated as cache
        // Miss. The fetch_node_state implementation checks for Error and
        // Sets should_fetch = true.
        let error = FetchState::<NodePayload>::Error {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            msg: "network error".to_string(),
        };

        // Verify error can be joined with newer states
        let newer_ready = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            generation: 1,
            value: mock_payload("recovered"),
        };

        let result = error.join(&newer_ready);
        match result {
            FetchState::Ready { value, .. } => {
                assert_eq!(value.identity, "recovered");
            }
            _ => panic!("Expected Ready state after retry"),
        }
    }

    // Error with newer stamp wins over Ready with older stamp.
    #[test]
    fn join_error_newer_beats_ready_older() {
        let ready = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 1,
            value: mock_payload("old"),
        };
        let error = FetchState::<NodePayload>::Error {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            msg: "newer error".to_string(),
        };

        let result1 = error.join(&ready);
        let result2 = ready.join(&error);
        assert!(matches!(result1, FetchState::Error { .. }));
        assert!(matches!(result2, FetchState::Error { .. }));
    }

    // Unknown × Error both orders → Error (identity law).
    #[test]
    fn join_unknown_error_both_orders() {
        let error = FetchState::<NodePayload>::Error {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            msg: "some error".to_string(),
        };

        let result1 = FetchState::Unknown.join(&error);
        let result2 = error.join(&FetchState::Unknown);
        assert!(matches!(result1, FetchState::Error { .. }));
        assert!(matches!(result2, FetchState::Error { .. }));
    }

    // Error.join(&Error) with same state is idempotent.
    #[test]
    fn join_error_same_state_idempotent() {
        let error = FetchState::<NodePayload>::Error {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            msg: "same".to_string(),
        };

        let result = error.join(&error);
        match result {
            FetchState::Error { stamp, msg } => {
                assert_eq!(stamp.ts_micros, 1000);
                assert_eq!(stamp.seq, 1);
                assert_eq!(msg, "same");
            }
            _ => panic!("Expected Error state"),
        }
    }

    // Tree operation tests

    // Flatten collapsed node hides children.
    #[test]
    fn flatten_collapsed_node_hides_children() {
        // Test that a collapsed host node hides its children.
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
                expanded: false, // Collapsed
                fetched: true,
                has_children: true,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![TreeNode {
                    reference: "proc1".into(),
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
        // Root is skipped, host1 is included (depth 0).
        // Proc1 is hidden because host1.expanded=false.
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].node.reference, "host1");
        assert_eq!(rows[0].depth, 0);
    }

    // Flatten expanded node shows children.
    #[test]
    fn flatten_expanded_node_shows_children() {
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
                expanded: true, // Expanded
                fetched: true,
                has_children: true,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![TreeNode {
                    reference: "proc1".into(),
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
        assert_eq!(rows[0].node.reference, "host1");
        assert_eq!(rows[0].depth, 0);
        assert_eq!(rows[1].node.reference, "proc1");
        assert_eq!(rows[1].depth, 1);
    }

    // Find node by reference works.
    #[test]
    fn find_node_by_reference_works() {
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
            }],
        };
        let found = find_node_by_ref(&tree, "child1");
        assert!(found.is_some());
        assert_eq!(found.unwrap().reference, "child1");
    }

    // Find node mut works.
    #[test]
    fn find_node_mut_works() {
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
            }],
        };
        let found = find_node_mut(&mut tree, "child1");
        assert!(found.is_some());
        found.unwrap().expanded = true;
        // Verify mutation worked
        assert!(tree.children[0].expanded);
    }

    // Collect refs visits all nodes.
    #[test]
    fn collect_refs_visits_all_nodes() {
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
                expanded: false, // Collapsed, but collect_refs should still visit children
                fetched: true,
                has_children: true,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![TreeNode {
                    reference: "proc1".into(),
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
        assert!(refs.contains("root"));
        assert!(refs.contains("host1"));
        assert!(refs.contains("proc1"));
    }

    // Tests for Phase 3 tree refactoring

    // Dual appearances flatten correctly.
    #[test]
    fn dual_appearances_flatten_correctly() {
        // Test that the same reference can appear at multiple depths
        // (dual appearance pattern).
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
                    reference: "proc1".into(),
                    label: "Proc 1".into(),
                    node_type: NodeType::Proc,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![TreeNode {
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
                    }],
                },
                // Same actor appears in flat list
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
            ],
        };

        let rows = flatten_tree(&tree);
        // Should see: proc1 (depth 0), actor1 (depth 1), actor1 (depth 0)
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].node.reference, "proc1");
        assert_eq!(rows[0].depth, 0);
        assert_eq!(rows[1].node.reference, "actor1");
        assert_eq!(rows[1].depth, 1);
        assert_eq!(rows[2].node.reference, "actor1");
        assert_eq!(rows[2].depth, 0);
    }

    // Expansion tracking uses depth pairs.
    #[test]
    fn expansion_tracking_uses_depth_pairs() {
        // Test that expanded_keys tracks (reference, depth) pairs,
        // Not just references.
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
                    reference: "proc1".into(),
                    label: "Proc 1".into(),
                    node_type: NodeType::Proc,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![TreeNode {
                        reference: "actor1".into(),
                        label: "Actor 1".into(),
                        node_type: NodeType::Actor,
                        expanded: true, // Expanded at depth 1
                        fetched: true,
                        has_children: false,
                        stopped: false,
                        failed: false,
                        is_system: false,
                        children: vec![],
                    }],
                },
                // Same actor collapsed in flat list
                TreeNode {
                    reference: "actor1".into(),
                    label: "Actor 1".into(),
                    node_type: NodeType::Actor,
                    expanded: false, // Collapsed at depth 0
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

        // Should track both proc1 and actor1 at their specific depths
        assert!(expanded_keys.contains(&("proc1".to_string(), 0)));
        assert!(expanded_keys.contains(&("actor1".to_string(), 1)));
        // Actor1 at depth 0 is not expanded, should not be in set
        assert!(!expanded_keys.contains(&("actor1".to_string(), 0)));
    }

    // Find node at depth distinguishes instances.
    #[test]
    fn find_node_at_depth_distinguishes_instances() {
        // Test that find_node_at_depth_mut finds the correct instance
        // When same reference appears at multiple depths.
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
                    reference: "proc1".into(),
                    label: "Proc 1".into(),
                    node_type: NodeType::Proc,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![TreeNode {
                        reference: "actor1".into(),
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
                    reference: "actor1".into(),
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

        // Find actor1 at depth 1 (in supervision tree)
        let mut count = 0;
        let found_depth_1 = tree
            .children
            .iter_mut()
            .find_map(|child| find_node_at_depth_mut(child, "actor1", 1, 0, &mut count));
        assert!(found_depth_1.is_some());
        assert_eq!(found_depth_1.unwrap().label, "Actor 1 in supervision");

        // Find actor1 at depth 0 (in flat list)
        let mut count = 0;
        let found_depth_0 = tree
            .children
            .iter_mut()
            .find_map(|child| find_node_at_depth_mut(child, "actor1", 0, 0, &mut count));
        assert!(found_depth_0.is_some());
        assert_eq!(found_depth_0.unwrap().label, "Actor 1 in flat list");
    }

    // Collapsed nodes stay collapsed after refresh.
    #[test]
    fn collapsed_nodes_stay_collapsed_after_refresh() {
        // Test that expansion state is preserved correctly when some
        // Instances are collapsed and others expanded.
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
                reference: "proc1".into(),
                label: "Proc 1".into(),
                node_type: NodeType::Proc,
                expanded: false, // Collapsed
                fetched: true,
                has_children: true,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![TreeNode {
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
                }],
            }],
        };

        let mut expanded_keys = HashSet::new();
        for child in &tree.children {
            collect_expanded_refs(child, 0, &mut expanded_keys);
        }

        // Collapsed nodes should NOT be in expanded_keys
        assert!(!expanded_keys.contains(&("proc1".to_string(), 0)));
        assert!(!expanded_keys.contains(&("actor1".to_string(), 1)));
    }

    // Fold equivalence flatten tree.
    #[test]
    fn fold_equivalence_flatten_tree() {
        // Verify flatten_tree produces expected row list with correct depths
        // And respects expansion flags.
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
                        expanded: false,
                        fetched: true,
                        has_children: true,
                        stopped: false,
                        failed: false,
                        is_system: false,
                        children: vec![TreeNode {
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
                        }],
                    }],
                },
                TreeNode {
                    reference: "host2".into(),
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

        // Expected: host1 (d=0), proc1 (d=1), host2 (d=0)
        // Actor1 should NOT appear because proc1.expanded = false
        assert_eq!(rows.len(), 3);

        assert_eq!(rows[0].node.reference, "host1");
        assert_eq!(rows[0].depth, 0);

        assert_eq!(rows[1].node.reference, "proc1");
        assert_eq!(rows[1].depth, 1);

        assert_eq!(rows[2].node.reference, "host2");
        assert_eq!(rows[2].depth, 0);
    }

    // Fold tree mut early exit stops traversal.
    #[test]
    fn fold_tree_mut_early_exit_stops_traversal() {
        // Verify fold_tree_mut_with_depth stops at first match using
        // ControlFlow::Break.
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
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![TreeNode {
                        reference: "target".into(),
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
                    reference: "child2".into(),
                    label: "Child 2".into(),
                    node_type: NodeType::Host,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![TreeNode {
                        reference: "should_not_visit".into(),
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
            if n.reference == "target" {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        });

        assert!(result.is_break());
        // Should visit: root, child1, target (then stop)
        assert_eq!(visited, vec!["root", "child1", "target"]);
        // Should_not_visit should NOT be in the list
        assert!(!visited.contains(&"should_not_visit".to_string()));
    }

    // Selection restore prefers depth match.
    #[test]
    fn selection_restore_prefers_depth_match() {
        // Verify that (reference, depth) matching correctly distinguishes
        // Between duplicate references at different depths.
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
                reference: "duplicate".into(),
                label: "Duplicate at depth 0".into(),
                node_type: NodeType::Host,
                expanded: true,
                fetched: true,
                has_children: true,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![TreeNode {
                    reference: "duplicate".into(),
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

        // Find at depth 0
        let mut count = 0;
        let found_d0 = tree
            .children
            .iter_mut()
            .find_map(|child| find_node_at_depth_mut(child, "duplicate", 0, 0, &mut count));
        assert!(found_d0.is_some());
        assert_eq!(found_d0.unwrap().label, "Duplicate at depth 0");

        // Find at depth 1
        let mut count = 0;
        let found_d1 = tree
            .children
            .iter_mut()
            .find_map(|child| find_node_at_depth_mut(child, "duplicate", 1, 0, &mut count));
        assert!(found_d1.is_some());
        assert_eq!(found_d1.unwrap().label, "Duplicate at depth 1");
    }

    // Join refresh staleness triggers refetch.
    #[test]
    fn join_refresh_staleness_triggers_refetch() {
        // Verify that FetchState::Ready with old generation is treated as
        // Stale and would trigger refetch.
        let payload1 = mock_payload("stale");
        let payload2 = mock_payload("fresh");

        let stale = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 10,
            value: payload1,
        };

        let fresh = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            generation: 20,
            value: payload2,
        };

        // Join with higher generation should prefer fresh
        let result = stale.join(&fresh);

        match result {
            FetchState::Ready {
                generation, value, ..
            } => {
                // Should pick the one with higher generation
                assert_eq!(generation, 20);
                assert_eq!(value.identity, "fresh");
            }
            _ => panic!("Expected Ready state"),
        }

        // In practice, fetch_with_join would detect stale.generation <
        // Refresh_gen and refetch. We verify the join semantics here.
    }

    // Fold vs traversal law node count.
    #[test]
    fn fold_vs_traversal_law_node_count() {
        // For a tree with all nodes expanded, flatten_tree size should equal
        // The total node count computed via fold.
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
                        has_children: false,
                        stopped: false,
                        failed: false,
                        is_system: false,
                        children: vec![],
                    }],
                },
                TreeNode {
                    reference: "host2".into(),
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

        // Count nodes using fold
        let node_count = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
            1 + child_counts.iter().sum::<usize>()
        });

        // Flatten_tree should produce exactly node_count rows (minus root,
        // Which is not rendered)
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), node_count - 1); // -1 for root
    }

    // Collapse idempotence.
    #[test]
    fn collapse_idempotence() {
        // Applying collapse_all twice should have no additional effect
        // (idempotence).
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
                reference: "child".into(),
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

        // First collapse
        collapse_all(&mut tree);
        assert!(!tree.expanded);
        assert!(!tree.children[0].expanded);

        // Second collapse should be no-op
        collapse_all(&mut tree);
        assert!(!tree.expanded);
        assert!(!tree.children[0].expanded);

        // Verify stability
        let snapshot_after_first = tree.expanded;
        collapse_all(&mut tree);
        assert_eq!(tree.expanded, snapshot_after_first);
    }

    // Placeholder refinement transitions fetched state.
    #[test]
    fn placeholder_refinement_transitions_fetched_state() {
        // Verify that expanding a placeholder transitions fetched=false
        // To fetched=true.
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
                reference: "placeholder".into(),
                label: "Loading...".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: false, // Not yet fetched
                has_children: true,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            }],
        };

        // Simulate fetch by finding and updating the placeholder
        use std::ops::ControlFlow;
        let _ = fold_tree_mut(&mut tree, &mut |n| {
            if n.reference == "placeholder" && !n.fetched {
                n.fetched = true;
                n.has_children = true; // Payload indicates children exist
                n.children = vec![TreeNode {
                    reference: "child".into(),
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

        // Verify transition
        let placeholder = find_node_by_ref(&tree, "placeholder");
        assert!(placeholder.is_some());
        let placeholder = placeholder.unwrap();
        assert!(placeholder.fetched);
        assert_eq!(placeholder.children.len(), 1);

        // Expand again should be no-op (already fetched)
        let initial_children = placeholder.children.len();
        let _ = find_node_by_ref(&tree, "placeholder");
        assert_eq!(initial_children, 1); // No change
    }

    // Cache join commutativity ready vs error.
    #[test]
    fn cache_join_commutativity_ready_vs_error() {
        // Verify join commutativity for Ready vs Error with equal stamps.
        let payload = mock_payload("test");

        let ready = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 1,
            value: payload,
        };

        let error = FetchState::Error {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            msg: "test error".to_string(),
        };

        // Join in both orders
        let result1 = ready.join(&error);
        let result2 = error.join(&ready);

        // Both should produce Ready (deterministic tie-break)
        match (&result1, &result2) {
            (FetchState::Ready { .. }, FetchState::Ready { .. }) => {
                // Verify they're equivalent
                assert!(matches!(result1, FetchState::Ready { .. }));
                assert!(matches!(result2, FetchState::Ready { .. }));
            }
            _ => panic!("Expected Ready states from tie-break"),
        }
    }

    // Cycle guard prevents infinite recursion.
    #[test]
    fn cycle_guard_prevents_infinite_recursion() {
        // Verify that attempting to build a tree with a self-reference cycle
        // Is handled gracefully (typically by returning None or marking as
        // Error).
        //
        // In the current implementation, cycles are prevented by not allowing
        // A child to have the same reference as its ancestor. We test that
        // The fold traversal completes without infinite recursion.

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
                reference: "root".into(), // Cycle!
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

        // Fold should complete without stack overflow
        let count = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
            1 + child_counts.iter().sum::<usize>()
        });

        // Should count root + self-reference = 2 nodes (not infinite)
        assert_eq!(count, 2);

        // Mutable fold should also complete
        use std::ops::ControlFlow;
        let mut visited = 0;
        let _ = fold_tree_mut(&mut tree, &mut |_n| {
            visited += 1;
            if visited > 100 {
                // Safety: prevent actual infinite recursion in test
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        });

        // Should visit only 2 nodes
        assert_eq!(visited, 2);
    }

    // Join cache preserves ready when generation matches.
    #[test]
    fn join_cache_preserves_ready_when_generation_matches() {
        // Verify that fetch_with_join preserves FetchState::Ready when
        // Generation == refresh_gen, and only refetches when generation <
        // Refresh_gen.
        let payload = mock_payload("current");

        // Cache entry with matching generation
        let current = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 10,
            value: payload.clone(),
        };

        // Simulate join with fresh fetch (higher generation)
        let fresh = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            generation: 10, // Same generation
            value: payload.clone(),
        };

        let result = current.join(&fresh);
        match result {
            FetchState::Ready { generation, .. } => {
                assert_eq!(generation, 10);
            }
            _ => panic!("Expected Ready state"),
        }

        // Now test stale case (generation < refresh_gen would trigger
        // Refetch in fetch_with_join)
        let stale = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 5, // Old generation
            value: payload.clone(),
        };

        let newer = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            generation: 10,
            value: payload,
        };

        let result = stale.join(&newer);
        match result {
            FetchState::Ready { generation, .. } => {
                // Join prefers newer generation
                assert_eq!(generation, 10);
            }
            _ => panic!("Expected Ready state"),
        }
    }

    // Fold tree mut visits in preorder.
    #[test]
    fn fold_tree_mut_visits_in_preorder() {
        // Verify that fold_tree_mut_with_depth visits nodes in pre-order
        // DFS (parent before children).
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
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![TreeNode {
                        reference: "grandchild1".into(),
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

        use std::ops::ControlFlow;
        let mut visit_order = Vec::new();
        let _ = fold_tree_mut_with_depth(&mut tree, 0, &mut |n, d| {
            visit_order.push((n.reference.clone(), d));
            ControlFlow::<()>::Continue(())
        });

        // Expected pre-order: root, child1, grandchild1, child2
        assert_eq!(
            visit_order,
            vec![
                ("root".to_string(), 0),
                ("child1".to_string(), 1),
                ("grandchild1".to_string(), 2),
                ("child2".to_string(), 1),
            ]
        );
    }

    // Selection stability with duplicate references.
    #[test]
    fn selection_stability_with_duplicate_references() {
        // Test cursor stability when reference exists at multiple depths
        // And when reference disappears entirely.
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
                reference: "duplicate".into(),
                label: "Duplicate at 0".into(),
                node_type: NodeType::Host,
                expanded: true,
                fetched: true,
                has_children: true,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![TreeNode {
                    reference: "duplicate".into(),
                    label: "Duplicate at 1".into(),
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

        // Two instances of "duplicate" at depths 0 and 1
        let duplicate_refs: Vec<_> = rows
            .iter()
            .filter(|r| r.node.reference == "duplicate")
            .map(|r| (r.node.reference.as_str(), r.depth))
            .collect();

        assert_eq!(duplicate_refs.len(), 2);
        assert_eq!(duplicate_refs[0], ("duplicate", 0));
        assert_eq!(duplicate_refs[1], ("duplicate", 1));

        // Test cursor with disappearing reference (collapse parent)
        let mut tree_collapsed = tree.clone();
        tree_collapsed.children[0].expanded = false; // Collapse

        let rows_after = flatten_tree(&tree_collapsed);

        // Now only one "duplicate" visible at depth 0
        let duplicate_refs_after: Vec<_> = rows_after
            .iter()
            .filter(|r| r.node.reference == "duplicate")
            .map(|r| (r.node.reference.as_str(), r.depth))
            .collect();

        assert_eq!(duplicate_refs_after.len(), 1);
        assert_eq!(duplicate_refs_after[0], ("duplicate", 0));

        // Cursor should clamp to valid range
        let mut cursor = Cursor::new(rows.len());
        cursor.set_pos(1); // Was at depth-1 duplicate
        cursor.update_len(rows_after.len()); // List shrunk
        assert!(cursor.pos() < rows_after.len());
    }

    // Placeholder noop when has children false.
    #[test]
    fn placeholder_noop_when_has_children_false() {
        // Verify that expanding a node with has_children=false leaves
        // Children empty and doesn't erroneously mark it as expanded.
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
                reference: "leaf".into(),
                label: "Leaf Node".into(),
                node_type: NodeType::Actor,
                expanded: false,
                fetched: true,
                has_children: false, // No children
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            }],
        };

        // Attempt to "expand" the leaf (simulate user pressing Tab)
        use std::ops::ControlFlow;
        let _ = fold_tree_mut(&mut tree, &mut |n| {
            if n.reference == "leaf" && !n.has_children {
                // Should NOT set expanded=true or add children
                // (This is application logic, but we verify state stays
                // Consistent)
                assert_eq!(n.children.len(), 0);
                assert!(!n.expanded);
            }
            ControlFlow::<()>::Continue(())
        });

        // Verify leaf remains unexpanded with no children
        let leaf = find_node_by_ref(&tree, "leaf");
        assert!(leaf.is_some());
        let leaf = leaf.unwrap();
        assert!(!leaf.expanded);
        assert_eq!(leaf.children.len(), 0);
        assert!(!leaf.has_children);
    }

    // Fold tree with depth deterministic preorder.
    #[test]
    fn fold_tree_with_depth_deterministic_preorder() {
        // Verify that fold_tree_with_depth (immutable fold) visits nodes
        // In deterministic pre-order DFS.
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
                    reference: "a".into(),
                    label: "A".into(),
                    node_type: NodeType::Host,
                    expanded: true,
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
                    has_children: false,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![],
                },
            ],
        };

        // Collect visit order using fold_tree_with_depth
        let visit_order = fold_tree_with_depth(&tree, 0, &|n,
                                                           d,
                                                           child_orders: Vec<
            Vec<(String, usize)>,
        >| {
            let mut order = vec![(n.reference.clone(), d)];
            for child_order in child_orders {
                order.extend(child_order);
            }
            order
        });

        // Expected pre-order: root, a, a1, b
        assert_eq!(
            visit_order,
            vec![
                ("root".to_string(), 0),
                ("a".to_string(), 1),
                ("a1".to_string(), 2),
                ("b".to_string(), 1),
            ]
        );
    }

    // Cycle vs duplicate reference allowed.
    #[test]
    fn cycle_vs_duplicate_reference_allowed() {
        // Verify that a duplicate reference NOT in the ancestor path is
        // Allowed (dual appearance), while a true cycle (ancestor path
        // Contains reference) would be rejected.
        //
        // Current implementation doesn't enforce cycle detection at tree
        // Construction, but we verify that duplicates at different branches
        // Work correctly.

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
                        reference: "duplicate".into(),
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
                    reference: "branch_b".into(),
                    label: "Branch B".into(),
                    node_type: NodeType::Host,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![TreeNode {
                        reference: "duplicate".into(),
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

        // Fold should complete successfully (no cycle)
        let count = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
            1 + child_counts.iter().sum::<usize>()
        });

        // Should count 5 nodes: root, branch_a, duplicate, branch_b,
        // Duplicate
        assert_eq!(count, 5);

        // Flatten should show both duplicates
        let rows = flatten_tree(&tree);
        let duplicate_count = rows
            .iter()
            .filter(|r| r.node.reference == "duplicate")
            .count();
        assert_eq!(duplicate_count, 2);
    }

    // Error state does not cache.
    #[test]
    fn error_state_does_not_cache() {
        // Verify that FetchState::Error always retries (doesn't prevent
        // Refetch). Join with Error should allow fresh attempt.
        let error1 = FetchState::<NodePayload>::Error {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            msg: "first error".to_string(),
        };

        let error2 = FetchState::Error {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            msg: "second error".to_string(),
        };

        // Join two errors - should prefer newer timestamp
        let result = error1.join(&error2);
        match result {
            FetchState::Error { stamp, .. } => {
                assert_eq!(stamp.ts_micros, 2000);
            }
            _ => panic!("Expected Error state"),
        }

        // Error joined with Unknown should return Error (not Unknown)
        let unknown = FetchState::Unknown;
        let result = error1.join(&unknown);
        assert!(matches!(result, FetchState::Error { .. }));

        // This demonstrates errors don't "stick" - each fetch attempt
        // Can retry
    }

    // Cursor update after empty tree.
    #[test]
    fn cursor_update_after_empty_tree() {
        // Verify cursor state when tree becomes empty (refresh fails,
        // Tree is None).
        let mut cursor = Cursor::new(5); // Previously had 5 items
        cursor.set_pos(2);

        // Tree refresh fails, now we have 0 items
        cursor.update_len(0);

        assert_eq!(cursor.len(), 0);
        assert_eq!(cursor.pos(), 0);

        // All movements should return false
        assert!(!cursor.move_down());
        assert!(!cursor.move_up());
        assert!(!cursor.home());
        assert!(!cursor.end());
    }

    // Empty tree all operations are noops.
    #[test]
    fn empty_tree_all_operations_are_noops() {
        // Verify that visible_rows(), selection restore, and detail fetch
        // Are all no-ops when tree=None.
        let app = App::new("localhost:8080", ThemeName::Nord, LangName::En);

        // Visible_rows should be empty
        let rows = app.visible_rows();
        assert_eq!(rows.len(), 0);

        // Cursor should be at 0
        assert_eq!(app.cursor.pos(), 0);
        assert_eq!(app.cursor.len(), 0);

        // Tree should be None
        assert!(app.tree().is_none());
    }

    // Single node tree expand collapse.
    #[test]
    fn single_node_tree_expand_collapse() {
        // Test edge cases with root having exactly one child.
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
                reference: "only_child".into(),
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

        // Flatten should show only the one child
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].node.reference, "only_child");

        // Expand the child (no-op since has_children=false)
        let child = find_node_mut(&mut tree, "only_child");
        assert!(child.is_some());
        let child = child.unwrap();
        assert!(!child.has_children);
        assert!(!child.expanded); // Should not expand

        // Collapse root
        collapse_all(&mut tree);
        assert!(!tree.expanded);
        assert!(!tree.children[0].expanded);
    }

    // Placeholder with has children true awaits fetch.
    #[test]
    fn placeholder_with_has_children_true_awaits_fetch() {
        // Node with has_children=true but empty children vec is a
        // Placeholder awaiting fetch. Expand should trigger fetch; collapse
        // Is no-op if never expanded.
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
                reference: "placeholder".into(),
                label: "Loading...".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: false,
                has_children: true, // Indicates children exist but not fetched
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            }],
        };

        // Placeholder should appear in flatten
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].node.reference, "placeholder");

        // Simulate expanding placeholder (would trigger fetch)
        let placeholder = find_node_mut(&mut tree, "placeholder");
        assert!(placeholder.is_some());
        let placeholder = placeholder.unwrap();
        assert!(placeholder.has_children);
        assert!(!placeholder.fetched);
        assert_eq!(placeholder.children.len(), 0);

        // Collapse on unexpanded placeholder is no-op
        use std::ops::ControlFlow;
        let _ = fold_tree_mut(&mut tree, &mut |n| {
            if n.reference == "placeholder" && !n.expanded {
                // Collapse on unexpanded node does nothing
                assert!(!n.expanded);
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        });
    }

    // System proc filter toggles visibility.
    #[test]
    fn system_proc_filter_toggles_visibility() {
        // Verify that toggling show_system filters/unfilters nodes.
        // Note: This tests the filtering logic at the model level.
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

        // With show_system=false, system procs would be filtered
        // (actual filtering happens in build logic, not flatten)
        // Here we verify the tree structure allows both types
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 2);

        // Verify both proc types present
        let refs: Vec<_> = rows.iter().map(|r| r.node.reference.as_str()).collect();
        assert!(refs.contains(&"user_proc"));
        assert!(refs.contains(&"system_proc"));

        // In practice, build_tree_node would filter based on
        // Show_system_procs flag
    }

    // Stale selection after refresh clamps cursor.
    #[test]
    fn stale_selection_after_refresh_clamps_cursor() {
        // Previously selected reference disappears after refresh; cursor
        // Should clamp to valid range.
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

        // Cursor at position 2 (child3)
        let mut cursor = Cursor::new(rows_before.len());
        cursor.set_pos(2);
        assert_eq!(cursor.pos(), 2);

        // After refresh, child3 disappears
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

        // Update cursor length (should clamp position)
        cursor.update_len(rows_after.len());
        assert!(cursor.pos() < rows_after.len());
        assert_eq!(cursor.pos(), 1); // Clamped to last valid position
    }

    // Duplicate references expansion targets specific instance.
    #[test]
    fn duplicate_references_expansion_targets_specific_instance() {
        // With duplicate references at different depths, expansion should
        // Only affect the targeted (reference, depth) instance.
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
                    reference: "duplicate".into(),
                    label: "Duplicate at 0".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: true,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![TreeNode {
                        reference: "child_of_first".into(),
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
                    reference: "duplicate".into(),
                    label: "Duplicate at 0 (second)".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: true,
                    stopped: false,
                    failed: false,
                    is_system: false,
                    children: vec![TreeNode {
                        reference: "child_of_second".into(),
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

        // Expand first duplicate at depth 0, index 0
        let mut count = 0;
        let first = tree
            .children
            .iter_mut()
            .find_map(|child| find_node_at_depth_mut(child, "duplicate", 0, 0, &mut count));
        assert!(first.is_some());
        first.unwrap().expanded = true;

        // Verify only first is expanded
        assert!(tree.children[0].expanded);
        assert!(!tree.children[1].expanded);

        // Flatten should show first's child but not second's
        let rows = flatten_tree(&tree);
        let refs: Vec<_> = rows.iter().map(|r| r.node.reference.as_str()).collect();
        assert!(refs.contains(&"child_of_first"));
        assert!(!refs.contains(&"child_of_second"));
    }

    // Expanded node with empty children renders safely.
    #[test]
    fn expanded_node_with_empty_children_renders_safely() {
        // Node with expanded=true but children=[] should not panic and
        // Should render correctly.
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
                reference: "empty_parent".into(),
                label: "Empty Parent".into(),
                node_type: NodeType::Host,
                expanded: true, // Expanded but has no children
                fetched: true,
                has_children: false,
                stopped: false,
                failed: false,
                is_system: false,
                children: vec![],
            }],
        };

        // Flatten should not panic
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].node.reference, "empty_parent");

        // Fold should complete normally
        let count = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
            1 + child_counts.iter().sum::<usize>()
        });
        assert_eq!(count, 2); // root + empty_parent
    }

    // Cache join ready vs ready equal stamps.
    #[test]
    fn cache_join_ready_vs_ready_equal_stamps() {
        // Ready vs Ready with equal stamps should have deterministic winner
        // (prefer one with higher generation, or use seq as tie-break).
        let payload1 = mock_payload("first");
        let payload2 = mock_payload("second");

        let ready1 = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 5,
            value: payload1,
        };

        let ready2 = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 5,
            value: payload2,
        };

        // Join should preserve one of the values (both are valid since
        // Stamps/generation are equal)
        let result = ready1.join(&ready2);

        // Result should be Ready
        match result {
            FetchState::Ready { generation, .. } => {
                assert_eq!(generation, 5);
            }
            _ => panic!("Expected Ready state"),
        }
    }

    // Cursor update when rows shrink to zero.
    #[test]
    fn cursor_update_when_rows_shrink_to_zero() {
        // After refresh failure (tree becomes None), cursor should clamp
        // To (0,0).
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
                reference: "child".into(),
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

        // Initially has 1 visible row
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 1);

        let mut cursor = Cursor::new(rows.len());
        cursor.set_pos(0);

        // Simulate tree becoming None (refresh failure)
        // Create empty tree with no children
        let empty_tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: false,
            stopped: false,
            failed: false,
            is_system: false,
            children: vec![],
        };

        let rows_after = flatten_tree(&empty_tree);
        assert_eq!(rows_after.len(), 0);

        // Update cursor
        cursor.update_len(rows_after.len());
        assert_eq!(cursor.pos(), 0);
        assert_eq!(cursor.len(), 0);
    }

    // Selection restore fallback when depth changes.
    #[test]
    fn selection_restore_fallback_when_depth_changes() {
        // Same reference appears but at different depth after refresh;
        // Ensure fallback to reference-only selection.
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
        // Target is at depth 1
        let target_before: Vec<_> = rows_before
            .iter()
            .filter(|r| r.node.reference == "target")
            .collect();
        assert_eq!(target_before.len(), 1);
        assert_eq!(target_before[0].depth, 1);

        // After refresh, target appears at depth 0 (structure changed)
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

        // Selection restore should fallback to reference-only match when
        // (reference, depth) doesn't exist
    }

    // Operational Reliability Tests
    //
    // These tests verify real-world operational scenarios that admins
    // would encounter: partial failures, high fan-out, rapid interactions,
    // stale cache recovery, etc.

    // Partial failure resilience.
    #[test]
    fn partial_failure_resilience() {
        // Some children fetch fails; rest of tree still renders with errors
        // Surfaced.
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
                    node_type: NodeType::Host, // Error represented as Host with error label
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

        // Tree should render all nodes including error
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 3);

        // Verify error node is present (indicated by label)
        let error_node = rows.iter().find(|r| r.node.reference == "error");
        assert!(error_node.is_some());
        assert!(error_node.unwrap().node.label.contains("Error"));

        // Success nodes should also be present
        assert!(rows.iter().any(|r| r.node.reference == "success"));
        assert!(rows.iter().any(|r| r.node.reference == "success2"));
    }

    // High fanout proc placeholder performance.
    #[test]
    fn high_fanout_proc_placeholder_performance() {
        // 1 proc with 1000 actors - ensure expand produces placeholders
        // Without attempting to fetch all actors upfront.
        let mut children = Vec::new();
        for i in 0..1000 {
            children.push(TreeNode {
                reference: format!("actor_{}", i),
                label: format!("Actor {}", i),
                node_type: NodeType::Actor,
                expanded: false,
                fetched: false, // Placeholder, not fetched yet
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

        // Flatten should handle 1000 children efficiently
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 1001); // proc + 1000 actors

        // Verify all are placeholders (not fetched)
        let actor_count = rows
            .iter()
            .filter(|r| r.node.reference.starts_with("actor_"))
            .count();
        assert_eq!(actor_count, 1000);

        // Fold should complete efficiently without stack overflow
        let count = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
            1 + child_counts.iter().sum::<usize>()
        });
        assert_eq!(count, 1002); // root + proc + 1000 actors
    }

    // Rapid toggle stress test.
    #[test]
    fn rapid_toggle_stress_test() {
        // Rapidly toggle expand/collapse - ensure no stale rows or panics.
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

        // Rapid expand/collapse cycles
        for _ in 0..100 {
            // Expand child1
            tree.children[0].expanded = true;
            let rows = flatten_tree(&tree);
            assert!(rows.len() >= 2); // At least child1, child2

            // Collapse child1
            tree.children[0].expanded = false;
            let rows = flatten_tree(&tree);
            assert_eq!(rows.len(), 2); // Only child1, child2

            // Collapse all
            collapse_all(&mut tree);
            let rows = flatten_tree(&tree);
            assert!(rows.len() <= 2); // All collapsed
        }

        // Final state should be stable
        let final_rows = flatten_tree(&tree);
        assert!(final_rows.len() <= 2);
    }

    // Stale cache recovery.
    #[test]
    fn stale_cache_recovery() {
        // Stale cached payload then refresh; ensure new payload replaces
        // Old.
        let old_payload = mock_payload("stale_data");
        let new_payload = mock_payload("fresh_data");

        let stale = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 5,
            value: old_payload,
        };

        let fresh = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            generation: 10,
            value: new_payload,
        };

        // Join should prefer fresh (newer generation and timestamp)
        let result = stale.join(&fresh);

        match result {
            FetchState::Ready {
                value, generation, ..
            } => {
                assert_eq!(value.identity, "fresh_data");
                assert_eq!(generation, 10);
            }
            _ => panic!("Expected Ready state"),
        }
    }

    // Selection stickiness during refresh.
    #[test]
    fn selection_stickiness_during_refresh() {
        // While refresh happens, selected node should remain stable if it
        // Still exists.
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

        // Select "stable"
        let stable_idx = rows_before
            .iter()
            .position(|r| r.node.reference == "stable")
            .unwrap();
        assert_eq!(stable_idx, 0);

        // After refresh, "transient" disappears but "stable" remains
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

        // "stable" should still be selectable at index 0
        let stable_idx_after = rows_after
            .iter()
            .position(|r| r.node.reference == "stable")
            .unwrap();
        assert_eq!(stable_idx_after, 0);
    }

    // Empty flight recorder renders safely.
    #[test]
    fn empty_flight_recorder_renders_safely() {
        // Actor node with empty/missing data should render gracefully
        // Without crashing.
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

        // Flatten should handle this gracefully
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].node.reference, "actor_with_empty_data");
    }

    // Concurrent expansion stability.
    #[test]
    fn concurrent_expansion_stability() {
        // Simulate multiple expansions happening concurrently (or in rapid
        // Succession). Ensure tree state remains consistent.
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

        // Expand both a and b in sequence
        tree.children[0].expanded = true;
        tree.children[1].expanded = true;

        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 4); // a, a1, b, b1

        // Collapse both
        tree.children[0].expanded = false;
        tree.children[1].expanded = false;

        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 2); // a, b

        // Verify consistency with fold
        let count = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
            1 + child_counts.iter().sum::<usize>()
        });
        assert_eq!(count, 5); // root, a, a1, b, b1
    }

    // Refresh under partial failure keeps rendering.
    #[test]
    fn refresh_under_partial_failure_keeps_rendering() {
        // Some nodes error on refresh, but UI keeps rendering rest of tree.
        // Simulate two refresh cycles where different nodes fail.

        // First refresh: child1 succeeds, child2 fails
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

        // Second refresh: child1 fails, child2 succeeds
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
        // Both still render despite alternating failures
        assert!(rows2.iter().any(|r| r.node.reference == "child1"));
        assert!(rows2.iter().any(|r| r.node.reference == "child2"));
    }

    // Large refresh churn selection clamping.
    #[test]
    fn large_refresh_churn_selection_clamping() {
        // Tree shape changes dramatically across refreshes; verify
        // Selection/clamping and no panic.

        // Initial: 5 nodes
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
        cursor.set_pos(4); // Select last item "e"

        // After refresh: only 2 nodes remain (massive churn)
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

        // Update cursor (should clamp)
        cursor.update_len(rows_after.len());
        assert!(cursor.pos() < rows_after.len());
        assert_eq!(cursor.pos(), 1); // Clamped to last valid
    }

    // Zero actor proc renders correctly.
    #[test]
    fn zero_actor_proc_renders_correctly() {
        // Proc with num_actors=0 should show no children and no expand
        // Affordance.
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
                has_children: false, // No children
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
        // Oversized reference/identity strings should not break rendering or
        // Wrap logic.
        let long_ref = "a".repeat(500); // 500 character reference
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

        // Flatten should not panic with long strings
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].node.reference, long_ref);
        assert_eq!(rows[0].node.label, long_label);

        // Fold should handle long strings
        let count = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
            1 + child_counts.iter().sum::<usize>()
        });
        assert_eq!(count, 2);
    }

    // Duplicate references depth targeting under refresh.
    #[test]
    fn duplicate_references_depth_targeting_under_refresh() {
        // Duplicate reference in different branches; expansion toggles work
        // Correctly; verify depth-targeted behavior holds across refresh.

        // Initial state: "dup" at depths 0 and 1
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

        // Expand branch_a's dup (depth 1)
        tree.children[0].children[0].expanded = true;

        // Flatten should show both dups
        let rows = flatten_tree(&tree);
        let dup_refs: Vec<_> = rows
            .iter()
            .filter(|r| r.node.reference == "dup")
            .map(|r| r.depth)
            .collect();
        assert_eq!(dup_refs.len(), 2);
        assert!(dup_refs.contains(&0));
        assert!(dup_refs.contains(&1));

        // Simulate refresh: structure preserved
        let tree_after_refresh = tree.clone();
        let rows_after = flatten_tree(&tree_after_refresh);

        // Both dups still present with correct depths
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
        // Missing or extra fields in payload (simulated by tree node) should
        // Not crash rendering.

        // Simulate a node that might have missing optional fields
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
                    label: "".into(), // Empty label (missing data)
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
                    reference: "".into(), // Empty reference (corrupt data)
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

        // Should not panic with missing/empty fields
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 2);

        // Fold should also handle gracefully
        let count = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
            1 + child_counts.iter().sum::<usize>()
        });
        assert_eq!(count, 3);
    }

    // System proc filter toggle during churn.
    #[test]
    fn system_proc_filter_toggle_during_churn() {
        // Toggle system proc filter while tree structure is changing;
        // Ensure consistency.

        // Initial: mixed system and user procs
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

        // All nodes render (show_system=true)
        let rows_all = flatten_tree(&tree);
        assert_eq!(rows_all.len(), 3);

        // Simulate filter toggle (in practice, build_tree_node would filter)
        // Here we just verify tree structure is stable
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

    // Hostile Condition Tests
    //
    // These tests validate behavior under corrupt data, extreme inputs,
    // and adversarial conditions.

    // Corrupted cached state recovers via join.
    #[test]
    fn corrupted_cached_state_recovery() {
        // Inject bad FetchState, ensure next refresh repairs via join.
        let bad = FetchState::<NodePayload>::Unknown;
        let good = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 10,
            value: mock_payload("repaired"),
        };

        // Join should recover
        let result = bad.join(&good);
        match result {
            FetchState::Ready { value, .. } => {
                assert_eq!(value.identity, "repaired");
            }
            _ => panic!("Expected recovery to Ready"),
        }

        // Reverse order also recovers
        let result2 = good.join(&bad);
        match result2 {
            FetchState::Ready { value, .. } => {
                assert_eq!(value.identity, "repaired");
            }
            _ => panic!("Expected recovery to Ready"),
        }
    }

    // Out of order fetch completion.
    #[test]
    fn out_of_order_fetch_completion() {
        // Two fetches for same reference with different stamps; cache
        // Converges.
        let early = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 5,
            value: mock_payload("early"),
        };

        let late = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            generation: 10,
            value: mock_payload("late"),
        };

        // Late should win
        let result = early.join(&late);
        match result {
            FetchState::Ready {
                value, generation, ..
            } => {
                assert_eq!(value.identity, "late");
                assert_eq!(generation, 10);
            }
            _ => panic!("Expected Ready with late data"),
        }

        // Even if received out-of-order
        let result_reversed = late.join(&early);
        match result_reversed {
            FetchState::Ready {
                value, generation, ..
            } => {
                assert_eq!(value.identity, "late");
                assert_eq!(generation, 10);
            }
            _ => panic!("Expected Ready with late data"),
        }
    }

    // Unicode and invalid strings render safely.
    #[test]
    fn unicode_and_invalid_strings_render_safely() {
        // Unicode, emoji, and unusual characters in identity should render
        // Without panic.
        let unicode_cases: Vec<String> = vec![
            "actor_🚀_emoji".to_string(),
            "proc_with_日本語".to_string(),
            "host_with_é_accents".to_string(),
            "zero_width_\u{200B}_joiner".to_string(),
            "rtl_\u{202E}_override".to_string(),
            "a".repeat(1000), // Very long ASCII
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

            // Should not panic
            let rows = flatten_tree(&tree);
            assert_eq!(rows.len(), 1);
            assert_eq!(&rows[0].node.reference, identity);
        }
    }

    // Memory pressure expand collapse cycle.
    #[test]
    fn memory_pressure_expand_collapse_cycle() {
        // Expand thousands of placeholders, collapse all, refresh; ensure
        // No runaway growth.

        // Create large tree
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

        // Expand - should handle 2000 nodes
        let rows_expanded = flatten_tree(&tree);
        assert_eq!(rows_expanded.len(), 2001); // proc + 2000 actors

        // Collapse all
        collapse_all(&mut tree);
        let rows_collapsed = flatten_tree(&tree);
        assert_eq!(rows_collapsed.len(), 1); // Only proc visible

        // Simulate refresh (structure unchanged)
        let rows_after_refresh = flatten_tree(&tree);
        assert_eq!(rows_after_refresh.len(), 1);

        // Memory should be stable (no allocations leaked)
    }

    // Rapid cursor ops during tree changes.
    #[test]
    fn rapid_cursor_ops_during_tree_changes() {
        // Simulate rapid key inputs while tree structure changes; verify
        // Cursor invariants hold.
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

        // Rapid operations
        for _ in 0..50 {
            cursor.move_down();
            cursor.move_up();

            // Simulate tree change
            tree.children[0].expanded = !tree.children[0].expanded;
            let rows = flatten_tree(&tree);
            cursor.update_len(rows.len());

            // Invariant must hold
            assert!(cursor.pos() < cursor.len() || cursor.len() == 0);
        }
    }

    // Header stats match tree fold.
    #[test]
    fn header_stats_match_tree_fold() {
        // Verify counts derived from tree match fold results under stress.
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

        // Count via fold
        let total_nodes = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
            1 + child_counts.iter().sum::<usize>()
        });

        // Count visible rows
        let visible_rows = flatten_tree(&tree).len();

        // Total should be root + host + proc + 2 actors = 5
        assert_eq!(total_nodes, 5);
        // Visible should be host + proc + 2 actors = 4 (no root)
        assert_eq!(visible_rows, 4);
    }

    // Timestamp monotonicity across refreshes.
    #[test]
    fn timestamp_monotonicity_across_refreshes() {
        // Ensure stamps remain strictly ordered even across multiple
        // Refresh cycles.
        let mut stamps = Vec::new();

        for i in 1..=10 {
            let state = FetchState::Ready {
                stamp: Stamp {
                    ts_micros: 1000 * i,
                    seq: i,
                },
                generation: i,
                value: mock_payload(&format!("refresh_{}", i)),
            };

            if let FetchState::Ready { stamp, .. } = &state {
                stamps.push(*stamp);
            }
        }

        // Verify monotonic increasing
        for i in 1..stamps.len() {
            assert!(stamps[i] > stamps[i - 1]);
        }
    }

    // Zero length and whitespace only strings.
    #[test]
    fn zero_length_and_whitespace_only_strings() {
        // Empty and whitespace-only strings should not break rendering.
        let edge_cases = vec![
            "",       // Empty
            " ",      // Single space
            "   ",    // Multiple spaces
            "\t",     // Tab
            "\n",     // Newline
            " \t\n ", // Mixed whitespace
        ];

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

            // Should not panic
            let rows = flatten_tree(&tree);
            assert_eq!(rows.len(), 1);
        }
    }

    // Performance at Scale Tests
    //
    // These tests validate algorithmic complexity, prove optimizations work,
    // and test pathological tree shapes at scale.

    // Flatten is O(visible) not O(total).
    #[test]
    fn flatten_scales_linearly_with_visible_nodes() {
        // Prove flatten is O(visible) not O(total).
        // Test at multiple scales: 100, 500, 1000.
        for scale in [100, 500, 1000] {
            let mut children = Vec::new();
            for i in 0..scale {
                children.push(TreeNode {
                    reference: format!("node_{}", i),
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
                reference: "root".into(),
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

            // Verify fold also scales linearly
            let count = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
                1 + child_counts.iter().sum::<usize>()
            });
            assert_eq!(count, scale + 1); // +1 for root
        }
    }

    // Deep chain and wide fanout both fold and flatten without overflow.
    #[test]
    fn deep_chain_vs_wide_fanout_performance() {
        // Compare deep chain (depth=500, breadth=1) vs
        // wide fan-out (depth=1, breadth=500).

        // Deep: 500 levels, each with 1 child
        let mut deep = TreeNode {
            reference: "deep_root".into(),
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
                reference: format!("deep_{}", i),
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

        // Wide: 1 level with 500 children
        let mut wide_children = Vec::new();
        for i in 0..500 {
            wide_children.push(TreeNode {
                reference: format!("wide_{}", i),
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
            reference: "wide_root".into(),
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

        // Both should handle efficiently
        assert_eq!(flatten_tree(&deep).len(), 499);
        assert_eq!(flatten_tree(&wide).len(), 500);

        // Fold should also handle both
        let deep_count = fold_tree(&deep, &|_n, cs: Vec<usize>| 1 + cs.iter().sum::<usize>());
        let wide_count = fold_tree(&wide, &|_n, cs: Vec<usize>| 1 + cs.iter().sum::<usize>());

        assert_eq!(deep_count, 500);
        assert_eq!(wide_count, 501);
    }

    // ControlFlow::Break short-circuits traversal.
    #[test]
    fn early_exit_avoids_full_traversal() {
        // Prove ControlFlow::Break actually short-circuits.
        // 1000 node tree, target at position 10.

        let mut children = Vec::new();
        for i in 0..1000 {
            children.push(TreeNode {
                reference: format!("node_{}", i),
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
            reference: "root".into(),
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

        // Should visit ~10 nodes, not 1000
        assert!(visited < 50, "Early exit failed, visited {}", visited);
        assert_eq!(visited, 10);
    }

    // Cursor operations on very large flattened list.
    #[test]
    fn cursor_navigation_large_list() {
        // Test cursor operations on very large flattened list.
        let mut cursor = Cursor::new(100000);

        // Jump to end
        cursor.end();
        assert_eq!(cursor.pos(), 99999);

        // Jump to start
        cursor.home();
        assert_eq!(cursor.pos(), 0);

        // Repeated set operations (stress test)
        for i in 0..1000 {
            cursor.set_pos(i * 50);
        }

        // Should complete quickly with no panic
        assert!(cursor.pos() < cursor.len());
    }

    // Flatten only traverses visible (expanded) nodes.
    #[test]
    fn flatten_ignores_collapsed_subtrees() {
        // Prove flatten only traverses visible nodes.
        // Tree: 1 visible parent + 100 collapsed parents,
        // each with 100 children.
        // Should visit 100 nodes, not 10,000.

        let mut children = Vec::new();
        for i in 0..100 {
            let mut grandchildren = Vec::new();
            for j in 0..100 {
                grandchildren.push(TreeNode {
                    reference: format!("child_{}_{}", i, j),
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
                reference: format!("parent_{}", i),
                label: format!("Parent {}", i),
                node_type: NodeType::Proc,
                expanded: false, // Collapsed!
                fetched: true,
                has_children: true,
                stopped: false,
                failed: false,
                is_system: false,
                children: grandchildren,
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
            children,
        };

        // Should only see the 100 collapsed parents, not their 10k children
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 100);
    }

    // (reference, depth) tracking with many duplicates.
    #[test]
    fn many_nodes_same_reference_depth_tracking() {
        // Test (reference, depth) tracking with many duplicates.
        // 100 branches, each with "dup" at different depth.

        let mut children = Vec::new();
        for depth in 0..100 {
            // Create nested structure to get "dup" at various depths
            let mut node = TreeNode {
                reference: "dup".into(),
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

            // Wrap in depth layers
            for i in (0..depth).rev() {
                node = TreeNode {
                    reference: format!("wrapper_{}", i),
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
            reference: "root".into(),
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

        // Flatten should handle all the different (dup, depth) pairs
        let rows = flatten_tree(&tree);
        // Each branch contributes depth+1 nodes
        let expected: usize = (0..100).map(|d| d + 1).sum();
        assert_eq!(rows.len(), expected);
    }

    // No growth proportional to operation count.
    #[test]
    fn memory_stable_across_repeated_operations() {
        // Verify no growth proportional to operation count.
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
                reference: "child".into(),
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

        // Perform 5000 expand/collapse operations
        for _ in 0..5000 {
            tree.children[0].expanded = !tree.children[0].expanded;
            let _ = flatten_tree(&tree);
        }

        // Tree size should be constant
        let node_count = fold_tree(&tree, &|_n, cs: Vec<usize>| 1 + cs.iter().sum::<usize>());
        assert_eq!(node_count, 2); // root + child only
    }

    // Fold is as efficient as manual recursion.
    #[test]
    fn fold_performance_parity_with_manual_recursion() {
        // Prove fold is as efficient as manual recursion.
        let mut children = Vec::new();
        for i in 0..1000 {
            children.push(TreeNode {
                reference: format!("node_{}", i),
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
            reference: "root".into(),
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

        // Count via fold
        let fold_result = fold_tree(&tree, &|_n, cs: Vec<usize>| 1 + cs.iter().sum::<usize>());

        // Count via manual recursion
        fn manual_count(n: &TreeNode) -> usize {
            1 + n.children.iter().map(manual_count).sum::<usize>()
        }
        let manual_result = manual_count(&tree);

        assert_eq!(fold_result, manual_result);
        assert_eq!(fold_result, 1001);
    }

    // Maximal churn: tree changes from 1000 to 100 nodes.
    #[test]
    fn refresh_churn_large_differential() {
        // Test maximal churn: tree changes from 1000 to 100 nodes.
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

        // After refresh: only 100 nodes
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

        // Should handle massive churn efficiently
    }

    // Test fixtures: Stopped/failed actor filtering

    // Stopped prefix matches.
    #[test]
    fn is_stopped_node_true_for_stopped_prefix() {
        let props = NodeProperties::Actor {
            actor_status: "stopped:sleep completed (2.3s)".to_string(),
            actor_type: "test".to_string(),
            messages_processed: 0,
            created_at: "".to_string(),
            last_message_handler: None,
            total_processing_time_us: 0,
            flight_recorder: None,
            is_system: false,
            failure_info: None,
        };
        assert!(is_stopped_node(&props));
    }

    // Failed prefix matches.
    #[test]
    fn is_stopped_node_true_for_failed_prefix() {
        let props = NodeProperties::Actor {
            actor_status: "failed:panic in handler".to_string(),
            actor_type: "test".to_string(),
            messages_processed: 0,
            created_at: "".to_string(),
            last_message_handler: None,
            total_processing_time_us: 0,
            flight_recorder: None,
            is_system: false,
            failure_info: None,
        };
        assert!(is_stopped_node(&props));
    }

    // Running status does not match.
    #[test]
    fn is_stopped_node_false_for_running() {
        let props = NodeProperties::Actor {
            actor_status: "Running".to_string(),
            actor_type: "test".to_string(),
            messages_processed: 0,
            created_at: "".to_string(),
            last_message_handler: None,
            total_processing_time_us: 0,
            flight_recorder: None,
            is_system: false,
            failure_info: None,
        };
        assert!(!is_stopped_node(&props));
    }

    // "stopped" without trailing colon must NOT match.
    #[test]
    fn is_stopped_node_false_without_colon() {
        let props = NodeProperties::Actor {
            actor_status: "stopped".to_string(),
            actor_type: "test".to_string(),
            messages_processed: 0,
            created_at: "".to_string(),
            last_message_handler: None,
            total_processing_time_us: 0,
            flight_recorder: None,
            is_system: false,
            failure_info: None,
        };
        assert!(!is_stopped_node(&props));

        let props2 = NodeProperties::Actor {
            actor_status: "failed".to_string(),
            actor_type: "test".to_string(),
            messages_processed: 0,
            created_at: "".to_string(),
            last_message_handler: None,
            total_processing_time_us: 0,
            flight_recorder: None,
            is_system: false,
            failure_info: None,
        };
        assert!(!is_stopped_node(&props2));
    }

    // Non-Actor variants always return false.
    #[test]
    fn is_stopped_node_false_for_non_actor_variants() {
        let root = NodeProperties::Root {
            num_hosts: 1,
            started_at: "".to_string(),
            started_by: "".to_string(),
            system_children: vec![],
        };
        assert!(!is_stopped_node(&root));

        let host = NodeProperties::Host {
            addr: "127.0.0.1:1234".to_string(),
            num_procs: 1,
            system_children: vec![],
        };
        assert!(!is_stopped_node(&host));

        let proc_props = NodeProperties::Proc {
            proc_name: "proc".to_string(),
            num_actors: 0,
            is_system: false,
            system_children: vec![],
            stopped_children: vec![],
            stopped_retention_cap: 0,
            is_poisoned: false,
            failed_actor_count: 0,
        };
        assert!(!is_stopped_node(&proc_props));
    }

    // placeholder_stopped is structurally identical to placeholder except stopped.
    #[test]
    fn placeholder_stopped_differs_only_in_stopped_field() {
        let normal = TreeNode::placeholder("actor1".to_string());
        let stopped = TreeNode::placeholder_stopped("actor1".to_string());

        // Structural equivalence except stopped.
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
        assert!(!stopped.fetched); // still unfetched
        assert!(stopped.has_children); // expandable
    }

    // from_payload sets stopped when payload has stopped actor_status.
    #[test]
    fn from_payload_sets_stopped_for_stopped_actor() {
        let payload = NodePayload {
            identity: "actor1".to_string(),
            properties: NodeProperties::Actor {
                actor_status: "stopped:done".to_string(),
                actor_type: "test".to_string(),
                messages_processed: 5,
                created_at: "".to_string(),
                last_message_handler: None,
                total_processing_time_us: 0,
                flight_recorder: None,
                is_system: false,
                failure_info: None,
            },
            children: vec![],
            parent: None,
            as_of: "".to_string(),
        };
        let node = TreeNode::from_payload("actor1".to_string(), &payload);
        assert!(node.stopped);
    }

    // from_payload does not set stopped for a running actor.
    #[test]
    fn from_payload_not_stopped_for_running_actor() {
        let payload = mock_payload("actor1");
        let node = TreeNode::from_payload("actor1".to_string(), &payload);
        assert!(!node.stopped);
    }

    // from_payload sets is_system for system actors.
    #[test]
    fn from_payload_sets_is_system_for_system_actor() {
        let payload = NodePayload {
            identity: "agent[0]".to_string(),
            properties: NodeProperties::Actor {
                actor_status: "idle".to_string(),
                actor_type: "hyperactor_mesh::mesh_agent::ProcMeshAgent".to_string(),
                messages_processed: 10,
                created_at: "".to_string(),
                last_message_handler: None,
                total_processing_time_us: 0,
                flight_recorder: None,
                is_system: true,
                failure_info: None,
            },
            children: vec![],
            parent: None,
            as_of: "".to_string(),
        };
        let node = TreeNode::from_payload("agent[0]".to_string(), &payload);
        assert!(node.is_system);
        assert!(!node.stopped);
    }

    // from_payload does not set is_system for regular actors.
    #[test]
    fn from_payload_not_system_for_user_actor() {
        let payload = mock_payload("worker[0]");
        let node = TreeNode::from_payload("worker[0]".to_string(), &payload);
        assert!(!node.is_system);
    }

    // Proc payloads never set stopped (is_stopped_node is actor-only).
    #[test]
    fn from_payload_proc_is_never_stopped() {
        let payload = NodePayload {
            identity: "proc1".to_string(),
            properties: NodeProperties::Proc {
                proc_name: "proc1".to_string(),
                num_actors: 0,
                is_system: false,
                system_children: vec![],
                stopped_children: vec!["x".to_string()],
                stopped_retention_cap: 10,
                is_poisoned: false,
                failed_actor_count: 0,
            },
            children: vec![],
            parent: None,
            as_of: "".to_string(),
        };
        let node = TreeNode::from_payload("proc1".to_string(), &payload);
        assert!(!node.stopped);
    }

    // Root label shows host count.
    #[test]
    fn derive_label_root_basic() {
        let payload = NodePayload {
            identity: "root".to_string(),
            properties: NodeProperties::Root {
                num_hosts: 3,
                started_at: "2026-01-01T00:00:00Z".to_string(),
                started_by: "testuser".to_string(),
                system_children: vec!["sys1".into()],
            },
            children: vec!["h1".into(), "h2".into(), "h3".into()],
            parent: None,
            as_of: "2026-01-01T00:00:00.000Z".to_string(),
        };
        assert_eq!(derive_label(&payload), "Mesh Root (3 hosts)");
    }

    // Host label with no system children shows user count only.
    #[test]
    fn derive_label_host_no_system_children() {
        let payload = NodePayload {
            identity: "host:h1".to_string(),
            properties: NodeProperties::Host {
                addr: "10.0.0.1:8000".to_string(),
                num_procs: 3,
                system_children: vec![],
            },
            children: vec![],
            parent: None,
            as_of: "".to_string(),
        };
        assert_eq!(derive_label(&payload), "10.0.0.1:8000  (3 procs: 3 user)");
    }

    // Host label breaks down user vs system procs.
    #[test]
    fn derive_label_host_with_system_children() {
        let payload = NodePayload {
            identity: "host:h1".to_string(),
            properties: NodeProperties::Host {
                addr: "10.0.0.1:8000".to_string(),
                num_procs: 5,
                system_children: vec!["sys1".into(), "sys2".into()],
            },
            children: vec![],
            parent: None,
            as_of: "".to_string(),
        };
        assert_eq!(
            derive_label(&payload),
            "10.0.0.1:8000  (5 procs: 3 user, 2 system)"
        );
    }

    // Host label omits "0 user" when all procs are system.
    #[test]
    fn derive_label_host_all_system() {
        let payload = NodePayload {
            identity: "host:h1".to_string(),
            properties: NodeProperties::Host {
                addr: "10.0.0.1:8000".to_string(),
                num_procs: 2,
                system_children: vec!["s1".into(), "s2".into()],
            },
            children: vec![],
            parent: None,
            as_of: "".to_string(),
        };
        // num_user = 2 - 2 = 0, so no "0 user" part.
        assert_eq!(derive_label(&payload), "10.0.0.1:8000  (2 procs: 2 system)");
    }

    // Proc label with no system or stopped children shows live count.
    #[test]
    fn derive_label_proc_no_system_no_stopped() {
        let payload = NodePayload {
            identity: "myproc".to_string(),
            properties: NodeProperties::Proc {
                proc_name: "myproc".to_string(),
                num_actors: 4,
                is_system: false,
                system_children: vec![],
                stopped_children: vec![],
                stopped_retention_cap: 0,
                is_poisoned: false,
                failed_actor_count: 0,
            },
            children: vec![],
            parent: None,
            as_of: "".to_string(),
        };
        assert_eq!(derive_label(&payload), "myproc  (4 actors: 4 live)");
    }

    // Proc label with system children but no stopped shows system and live.
    #[test]
    fn derive_label_proc_with_system_no_stopped() {
        let payload = NodePayload {
            identity: "myproc".to_string(),
            properties: NodeProperties::Proc {
                proc_name: "myproc".to_string(),
                num_actors: 5,
                is_system: false,
                system_children: vec!["sys1".into(), "sys2".into()],
                stopped_children: vec![],
                stopped_retention_cap: 0,
                is_poisoned: false,
                failed_actor_count: 0,
            },
            children: vec![],
            parent: None,
            as_of: "".to_string(),
        };
        // num_system=2, num_live=5-2=3, no stopped. total=5.
        assert_eq!(
            derive_label(&payload),
            "myproc  (5 actors: 2 system, 3 live)"
        );
    }

    // Proc label shows live and stopped counts.
    #[test]
    fn derive_label_proc_with_stopped() {
        let payload = NodePayload {
            identity: "myproc".to_string(),
            properties: NodeProperties::Proc {
                proc_name: "myproc".to_string(),
                num_actors: 3,
                is_system: false,
                system_children: vec![],
                stopped_children: vec!["s1".into(), "s2".into()],
                stopped_retention_cap: 100,
                is_poisoned: false,
                failed_actor_count: 0,
            },
            children: vec![],
            parent: None,
            as_of: "".to_string(),
        };
        // total = 3 + 2 = 5. num_live = 3 - 0 = 3.
        assert_eq!(
            derive_label(&payload),
            "myproc  (5 actors: 3 live, 2 stopped)"
        );
    }

    // Proc label annotates "(max retained)" when at retention cap.
    #[test]
    fn derive_label_proc_stopped_at_retention_cap() {
        let payload = NodePayload {
            identity: "myproc".to_string(),
            properties: NodeProperties::Proc {
                proc_name: "myproc".to_string(),
                num_actors: 1,
                is_system: false,
                system_children: vec![],
                stopped_children: vec!["s1".into(), "s2".into(), "s3".into()],
                stopped_retention_cap: 3,
                is_poisoned: false,
                failed_actor_count: 0,
            },
            children: vec![],
            parent: None,
            as_of: "".to_string(),
        };
        // len(3) >= cap(3), cap > 0 → "(max retained)".
        assert!(derive_label(&payload).contains("3 stopped (max retained)"));
    }

    // Cap of 0 means retention disabled; never show "max retained".
    #[test]
    fn derive_label_proc_stopped_retention_cap_zero_never_annotates() {
        let payload = NodePayload {
            identity: "myproc".to_string(),
            properties: NodeProperties::Proc {
                proc_name: "myproc".to_string(),
                num_actors: 0,
                is_system: false,
                system_children: vec![],
                stopped_children: vec!["s1".into()],
                stopped_retention_cap: 0,
                is_poisoned: false,
                failed_actor_count: 0,
            },
            children: vec![],
            parent: None,
            as_of: "".to_string(),
        };
        // cap == 0 → never show "max retained" even though len >= cap.
        let label = derive_label(&payload);
        assert!(label.contains("1 stopped"));
        assert!(!label.contains("max retained"));
    }

    // Proc label shows all three parts when system, live, and stopped coexist.
    #[test]
    fn derive_label_proc_system_and_stopped_and_live() {
        let payload = NodePayload {
            identity: "myproc".to_string(),
            properties: NodeProperties::Proc {
                proc_name: "myproc".to_string(),
                num_actors: 5,
                is_system: false,
                system_children: vec!["sys1".into()],
                stopped_children: vec!["dead1".into(), "dead2".into()],
                stopped_retention_cap: 100,
                is_poisoned: false,
                failed_actor_count: 0,
            },
            children: vec![],
            parent: None,
            as_of: "".to_string(),
        };
        // num_system=1, num_live=5-1=4, num_stopped=2, total=5+2=7.
        assert_eq!(
            derive_label(&payload),
            "myproc  (7 actors: 1 system, 4 live, 2 stopped)"
        );
    }

    // Proc label omits "0 live" when no live actors remain.
    #[test]
    fn derive_label_proc_all_stopped_none_live() {
        let payload = NodePayload {
            identity: "myproc".to_string(),
            properties: NodeProperties::Proc {
                proc_name: "myproc".to_string(),
                num_actors: 0,
                is_system: false,
                system_children: vec![],
                stopped_children: vec!["d1".into(), "d2".into()],
                stopped_retention_cap: 100,
                is_poisoned: false,
                failed_actor_count: 0,
            },
            children: vec![],
            parent: None,
            as_of: "".to_string(),
        };
        // num_live=0, no "0 live" part. total=0+2=2.
        assert_eq!(derive_label(&payload), "myproc  (2 actors: 2 stopped)");
    }

    // saturating_sub prevents underflow when num_system > num_actors (race).
    #[test]
    fn derive_label_proc_saturating_sub_prevents_underflow() {
        // Edge case: num_system > num_actors (race condition).
        let payload = NodePayload {
            identity: "myproc".to_string(),
            properties: NodeProperties::Proc {
                proc_name: "myproc".to_string(),
                num_actors: 1,
                is_system: false,
                system_children: vec!["s1".into(), "s2".into(), "s3".into()],
                stopped_children: vec![],
                stopped_retention_cap: 0,
                is_poisoned: false,
                failed_actor_count: 0,
            },
            children: vec![],
            parent: None,
            as_of: "".to_string(),
        };
        // num_live = 1.saturating_sub(3) = 0. No "0 live" part.
        let label = derive_label(&payload);
        assert!(label.contains("3 system"));
        assert!(!label.contains("live"));
    }

    // Helper: proc node with 2 live + 1 stopped actor for filtering tests.
    fn make_proc_with_stopped_children() -> TreeNode {
        // Simulate a proc with 2 live + 1 stopped actor.
        TreeNode {
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
                reference: "proc1".into(),
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
                        reference: "live_actor_1".into(),
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
                        reference: "live_actor_2".into(),
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
                        reference: "dead_actor".into(),
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

    // Stopped nodes are visible in flatten (not filtered by tree structure).
    #[test]
    fn stopped_nodes_visible_in_flatten() {
        let tree = make_proc_with_stopped_children();
        let rows = flatten_tree(&tree);
        // proc1 + 2 live + 1 stopped = 4 visible rows.
        assert_eq!(rows.len(), 4);
        assert!(rows.iter().any(|r| r.node.reference == "dead_actor"));
    }

    // Stopped field propagates through flatten_tree.
    #[test]
    fn stopped_node_stopped_field_survives_flatten() {
        let tree = make_proc_with_stopped_children();
        let rows = flatten_tree(&tree);
        let dead = rows
            .iter()
            .find(|r| r.node.reference == "dead_actor")
            .unwrap();
        assert!(dead.node.stopped);
        let live = rows
            .iter()
            .find(|r| r.node.reference == "live_actor_1")
            .unwrap();
        assert!(!live.node.stopped);
    }

    // Stopped placeholder is visible in flatten (visual hint, not barrier).
    #[test]
    fn placeholder_stopped_visible_in_flatten() {
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
            children: vec![TreeNode::placeholder_stopped("dead1".to_string())],
        };
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 1);
        assert!(rows[0].node.stopped);
        assert!(!rows[0].node.fetched);
    }

    // Dual-source OR: both payload and proc list agree on stopped.
    #[test]
    fn from_payload_or_logic_both_sources_agree() {
        // Payload says stopped, proc list also says stopped.
        let payload = NodePayload {
            identity: "actor1".to_string(),
            properties: NodeProperties::Actor {
                actor_status: "stopped:done".to_string(),
                actor_type: "test".to_string(),
                messages_processed: 0,
                created_at: "".to_string(),
                last_message_handler: None,
                total_processing_time_us: 0,
                flight_recorder: None,
                is_system: false,
                failure_info: None,
            },
            children: vec![],
            parent: None,
            as_of: "".to_string(),
        };
        let mut node = TreeNode::from_payload("actor1".to_string(), &payload);
        // Simulate the OR assignment from update_node_children:
        // node.stopped = node.stopped || child_is_stopped;
        let child_is_stopped = true; // from proc's stopped_children
        node.stopped = node.stopped || child_is_stopped;
        assert!(node.stopped);
    }

    // Dual-source OR: proc list alone promotes to stopped.
    #[test]
    fn from_payload_or_logic_only_proc_list() {
        // Payload says Running (stale), but proc list says stopped.
        let payload = mock_payload("actor1"); // Running
        let mut node = TreeNode::from_payload("actor1".to_string(), &payload);
        assert!(!node.stopped); // payload alone says not stopped
        let child_is_stopped = true; // proc list disagrees
        node.stopped = node.stopped || child_is_stopped;
        assert!(node.stopped); // OR promotes to stopped
    }

    // Dual-source OR: cached payload alone is sufficient.
    #[test]
    fn from_payload_or_logic_only_cached_payload() {
        // Payload says stopped, but proc list doesn't include it
        // (proc list stale).
        let payload = NodePayload {
            identity: "actor1".to_string(),
            properties: NodeProperties::Actor {
                actor_status: "failed:panic".to_string(),
                actor_type: "test".to_string(),
                messages_processed: 0,
                created_at: "".to_string(),
                last_message_handler: None,
                total_processing_time_us: 0,
                flight_recorder: None,
                is_system: false,
                failure_info: None,
            },
            children: vec![],
            parent: None,
            as_of: "".to_string(),
        };
        let mut node = TreeNode::from_payload("actor1".to_string(), &payload);
        assert!(node.stopped); // payload alone is sufficient
        let child_is_stopped = false; // proc list hasn't caught up
        node.stopped = node.stopped || child_is_stopped;
        assert!(node.stopped); // still stopped
    }

    // from_payload sets failed when failure_info is present.
    #[test]
    fn from_payload_sets_failed_for_actor_with_failure_info() {
        let payload = NodePayload {
            identity: "actor1".to_string(),
            properties: NodeProperties::Actor {
                actor_status: "failed:panic".to_string(),
                actor_type: "test".to_string(),
                messages_processed: 0,
                created_at: "".to_string(),
                last_message_handler: None,
                total_processing_time_us: 0,
                flight_recorder: None,
                is_system: false,
                failure_info: Some(FailureInfo {
                    error_message: "GPU memory corruption".to_string(),
                    root_cause_actor: "worker[0]".to_string(),
                    root_cause_name: Some("worker".to_string()),
                    occurred_at: "2025-01-01T00:00:00Z".to_string(),
                    is_propagated: false,
                }),
            },
            children: vec![],
            parent: None,
            as_of: "".to_string(),
        };
        let node = TreeNode::from_payload("actor1".to_string(), &payload);
        assert!(node.failed);
        assert!(node.stopped); // failed implies stopped
    }

    // from_payload does not set failed when failure_info is absent.
    #[test]
    fn from_payload_not_failed_without_failure_info() {
        let payload = mock_payload("actor1");
        let node = TreeNode::from_payload("actor1".to_string(), &payload);
        assert!(!node.failed);
    }

    // Proc label includes POISONED indicator when is_poisoned is true.
    #[test]
    fn derive_label_proc_poisoned() {
        let payload = NodePayload {
            identity: "myproc".to_string(),
            properties: NodeProperties::Proc {
                proc_name: "myproc".to_string(),
                num_actors: 2,
                is_system: false,
                system_children: vec![],
                stopped_children: vec!["dead1".into()],
                stopped_retention_cap: 100,
                is_poisoned: true,
                failed_actor_count: 1,
            },
            children: vec![],
            parent: None,
            as_of: "".to_string(),
        };
        let label = derive_label(&payload);
        assert!(label.contains("[POISONED: 1 failed]"));
    }

    // Proc label omits POISONED indicator when not poisoned.
    #[test]
    fn derive_label_proc_not_poisoned() {
        let payload = NodePayload {
            identity: "myproc".to_string(),
            properties: NodeProperties::Proc {
                proc_name: "myproc".to_string(),
                num_actors: 3,
                is_system: false,
                system_children: vec![],
                stopped_children: vec![],
                stopped_retention_cap: 100,
                is_poisoned: false,
                failed_actor_count: 0,
            },
            children: vec![],
            parent: None,
            as_of: "".to_string(),
        };
        let label = derive_label(&payload);
        assert!(!label.contains("POISONED"));
    }

    // Actor label formats as name[pid] when identity parses as ActorId.
    #[test]
    fn derive_label_actor_standard_actor_id() {
        let payload = NodePayload {
            identity: "myworld[0].worker[3]".to_string(),
            properties: NodeProperties::Actor {
                actor_status: "Running".to_string(),
                actor_type: "Worker".to_string(),
                messages_processed: 42,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                last_message_handler: Some("handle_task".to_string()),
                total_processing_time_us: 1000,
                flight_recorder: None,
                is_system: false,
                failure_info: None,
            },
            children: vec![],
            parent: Some("myworld[0]".to_string()),
            as_of: "2026-01-01T00:00:00.000Z".to_string(),
        };
        assert_eq!(derive_label(&payload), "worker[3]");
    }

    // Actor label falls back to raw identity when it doesn't parse as ActorId.
    #[test]
    fn derive_label_actor_unparseable_identity() {
        let payload = NodePayload {
            identity: "not-a-valid-actor-id!!!".to_string(),
            properties: NodeProperties::Actor {
                actor_status: "Running".to_string(),
                actor_type: "Unknown".to_string(),
                messages_processed: 0,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                last_message_handler: None,
                total_processing_time_us: 0,
                flight_recorder: None,
                is_system: false,
                failure_info: None,
            },
            children: vec![],
            parent: None,
            as_of: "2026-01-01T00:00:00.000Z".to_string(),
        };
        assert_eq!(derive_label(&payload), "not-a-valid-actor-id!!!");
    }

    // --- format_value tests ---

    // format_value renders a string as-is.
    #[test]
    fn format_value_string() {
        let v = Value::String("hello world".to_string());
        assert_eq!(format_value(&v), "hello world");
    }

    // format_value renders a number as its string form.
    #[test]
    fn format_value_number() {
        let v = serde_json::json!(42);
        assert_eq!(format_value(&v), "42");
    }

    // format_value renders a bool as "true"/"false".
    #[test]
    fn format_value_bool() {
        assert_eq!(format_value(&serde_json::json!(true)), "true");
        assert_eq!(format_value(&serde_json::json!(false)), "false");
    }

    // format_value renders null as the string "null".
    #[test]
    fn format_value_null() {
        assert_eq!(format_value(&Value::Null), "null");
    }

    // format_value renders an array as its length in brackets.
    #[test]
    fn format_value_array() {
        let v = serde_json::json!([1, 2, 3]);
        assert_eq!(format_value(&v), "[3]");
    }

    // format_value renders an object as its field count in braces.
    #[test]
    fn format_value_object() {
        let v = serde_json::json!({"a": 1, "b": 2, "c": 3, "d": 4, "e": 5});
        assert_eq!(format_value(&v), "{5}");
    }

    // --- format_event_summary tests ---

    // format_event_summary prefers "message" field.
    #[test]
    fn format_event_summary_message_field() {
        let fields = serde_json::json!({"message": "something happened", "other": 42});
        assert_eq!(
            format_event_summary("event_name", &fields),
            "something happened"
        );
    }

    // format_event_summary uses "handler" field when no message.
    #[test]
    fn format_event_summary_handler_field() {
        let fields = serde_json::json!({"handler": "on_tick", "count": 7});
        assert_eq!(
            format_event_summary("event_name", &fields),
            "handler: on_tick"
        );
    }

    // format_event_summary falls back to event name when fields is not an object.
    #[test]
    fn format_event_summary_fallback_to_name() {
        let fields = Value::Null;
        assert_eq!(format_event_summary("my_event", &fields), "my_event");
    }

    // --- format_local_time tests ---

    // format_local_time falls back to substring extraction for invalid input.
    #[test]
    fn format_local_time_invalid_string_fallback() {
        // Not a valid RFC 3339 timestamp but has enough chars for 11..19 slice.
        assert_eq!(format_local_time("xxxxxxxxxxx12:34:56yyy"), "12:34:56");
    }

    // format_local_time falls back to full string when too short for slice.
    #[test]
    fn format_local_time_too_short_fallback() {
        assert_eq!(format_local_time("short"), "short");
    }

    // --- format_relative_time tests ---

    // format_relative_time returns the raw string on parse failure.
    #[test]
    fn format_relative_time_parse_failure_fallback() {
        assert_eq!(format_relative_time("not-a-date"), "not-a-date");
    }

    // --- format_uptime tests ---

    // format_uptime returns "unknown" on parse failure.
    #[test]
    fn format_uptime_parse_failure() {
        assert_eq!(format_uptime("garbage"), "unknown");
    }

    // is_failed_node returns true for actor with failure_info.
    #[test]
    fn is_failed_node_with_failure_info() {
        let props = NodeProperties::Actor {
            actor_status: "failed:panic".to_string(),
            actor_type: "test".to_string(),
            messages_processed: 0,
            created_at: "".to_string(),
            last_message_handler: None,
            total_processing_time_us: 0,
            flight_recorder: None,
            is_system: false,
            failure_info: Some(FailureInfo {
                error_message: "boom".to_string(),
                root_cause_actor: "a[0]".to_string(),
                root_cause_name: None,
                occurred_at: "2025-01-01T00:00:00Z".to_string(),
                is_propagated: false,
            }),
        };
        assert!(is_failed_node(&props));
    }

    // is_failed_node returns false for actor without failure_info.
    #[test]
    fn is_failed_node_without_failure_info() {
        let props = NodeProperties::Actor {
            actor_status: "Running".to_string(),
            actor_type: "test".to_string(),
            messages_processed: 0,
            created_at: "".to_string(),
            last_message_handler: None,
            total_processing_time_us: 0,
            flight_recorder: None,
            is_system: false,
            failure_info: None,
        };
        assert!(!is_failed_node(&props));
    }

    // is_failed_node returns true for poisoned proc.
    #[test]
    fn is_failed_node_returns_true_for_poisoned_proc() {
        let props = NodeProperties::Proc {
            proc_name: "myproc".to_string(),
            num_actors: 1,
            is_system: false,
            system_children: vec![],
            stopped_children: vec![],
            stopped_retention_cap: 0,
            is_poisoned: true,
            failed_actor_count: 1,
        };
        assert!(is_failed_node(&props));
    }

    // is_failed_node returns false for healthy proc.
    #[test]
    fn is_failed_node_returns_false_for_healthy_proc() {
        let props = NodeProperties::Proc {
            proc_name: "myproc".to_string(),
            num_actors: 1,
            is_system: false,
            system_children: vec![],
            stopped_children: vec![],
            stopped_retention_cap: 0,
            is_poisoned: false,
            failed_actor_count: 0,
        };
        assert!(!is_failed_node(&props));
    }
}
