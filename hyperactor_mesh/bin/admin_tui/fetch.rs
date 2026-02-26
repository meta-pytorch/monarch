/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;

use algebra::JoinSemilattice;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor::introspect::NodePayload;
use hyperactor::introspect::NodeProperties;

use crate::filter::is_failed_node;
use crate::filter::is_stopped_node;
use crate::filter::is_system_node;
use crate::format::derive_label;
use crate::model::MAX_TREE_DEPTH;
use crate::model::NodeType;
use crate::model::TreeNode;

/// Monotonic ordering key for fetch results.
///
/// `ts_micros` comes from wall-clock time (RealClock) and `seq`
/// breaks ties to ensure a total order within this process.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Stamp {
    /// Wall-clock timestamp in microseconds since UNIX epoch.
    pub(crate) ts_micros: u64,
    /// Monotonic tie-breaker for identical timestamps in this
    /// process.
    pub(crate) seq: u64,
}

/// Cached result of fetching a node, with ordering metadata.
///
/// `generation` tracks the refresh cycle that produced the entry.
/// `stamp` provides a total order among fetches for join semantics.
#[derive(Clone, Debug)]
pub(crate) enum FetchState<T> {
    /// Not yet fetched or explicitly invalidated.
    Unknown,
    /// Successful fetch result.
    Ready {
        /// Ordering key for merge semantics.
        stamp: Stamp,
        /// Refresh generation when this value was fetched.
        generation: u64,
        /// The fetched payload.
        value: T,
    },
    /// Failed fetch result (always retries - no generation tracking).
    Error {
        /// Ordering key for merge semantics.
        stamp: Stamp,
        /// Human-readable error message.
        msg: String,
    },
}

/// Join prefers the entry with the newer `Stamp` to ensure total
/// ordering.
///
/// `Unknown` acts as the identity. When stamps are equal, a
/// deterministic tie-break keeps the operation commutative and
/// idempotent (Ready > Error, and Error uses lexicographic `msg`
/// ordering).
impl<T: Clone> JoinSemilattice for FetchState<T> {
    fn join(&self, other: &Self) -> Self {
        use FetchState::*;
        match (self, other) {
            (Unknown, x) | (x, Unknown) => x.clone(),
            (Ready { stamp: a, .. }, Ready { stamp: b, .. })
            | (Ready { stamp: a, .. }, Error { stamp: b, .. })
            | (Error { stamp: a, .. }, Ready { stamp: b, .. })
            | (Error { stamp: a, .. }, Error { stamp: b, .. }) => {
                if a > b {
                    self.clone()
                } else if b > a {
                    other.clone()
                } else {
                    // Deterministic tie-break for commutativity when stamps
                    // Are equal.
                    match (self, other) {
                        (Ready { .. }, _) => self.clone(),
                        (_, Ready { .. }) => other.clone(),
                        (Error { msg: m1, .. }, Error { msg: m2, .. }) => {
                            // Lexicographic tie-break ensures commutativity
                            // For Error vs Error.
                            if m1 >= m2 {
                                self.clone()
                            } else {
                                other.clone()
                            }
                        }
                        _ => self.clone(),
                    }
                }
            }
        }
    }
}

/// Unified fetch+join path for all cache writes.
///
/// Checks cache first, only fetches if needed (not present, stale, or
/// error), then joins the result into the cache. Returns the final
/// FetchState.
///
/// This is the single source of truth for fetch+join semantics.
pub(crate) async fn fetch_with_join(
    client: &reqwest::Client,
    base_url: &str,
    reference: &str,
    cache: &mut HashMap<String, FetchState<NodePayload>>,
    refresh_gen: u64,
    seq_counter: &mut u64,
    force: bool,
) -> FetchState<NodePayload> {
    let cached_state = cache.get(reference);
    let should_fetch = if force {
        true
    } else {
        match cached_state {
            None => true,
            Some(FetchState::Unknown) => true,
            Some(FetchState::Error { .. }) => true,
            Some(FetchState::Ready { generation, .. }) => *generation < refresh_gen,
        }
    };

    if should_fetch {
        // Generate stamp.
        *seq_counter += 1;
        let ts_micros = RealClock
            .system_time_now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        let stamp = Stamp {
            ts_micros,
            seq: *seq_counter,
        };

        // Fetch and wrap in FetchState.
        let new_state = match fetch_node_raw(client, base_url, reference).await {
            Ok(payload) => FetchState::Ready {
                stamp,
                generation: refresh_gen,
                value: payload,
            },
            Err(e) => FetchState::Error { stamp, msg: e },
        };

        // Join into cache.
        cache
            .entry(reference.to_string())
            .and_modify(|s| *s = s.join(&new_state))
            .or_insert(new_state);
    }

    cache.get(reference).cloned().unwrap_or(FetchState::Unknown)
}

/// Fetch a single node payload from the admin API.
///
/// Free-function form of `App::fetch_node` so callers that hold
/// partial borrows of `App` can avoid borrowing all of `&self`.
pub(crate) async fn fetch_node_raw(
    client: &reqwest::Client,
    base_url: &str,
    reference: &str,
) -> Result<NodePayload, String> {
    let url = format!("{}/v1/{}", base_url, urlencoding::encode(reference));
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("Request failed: {}", e))?;
    if resp.status().is_success() {
        resp.json::<NodePayload>()
            .await
            .map_err(|e| format!("Parse error: {}", e))
    } else {
        Err(format!("HTTP {}", resp.status()))
    }
}

/// Extract cached payload from FetchState cache (free function).
pub(crate) fn get_cached_payload<'a>(
    cache: &'a HashMap<String, FetchState<NodePayload>>,
    reference: &str,
) -> Option<&'a NodePayload> {
    cache.get(reference).and_then(|state| match state {
        FetchState::Ready { value, .. } => Some(value),
        _ => None,
    })
}

/// Recursively build a tree node from a reference.
///
/// Returns `Option<TreeNode>` - `None` if the node should be
/// filtered out or fetch fails.
///
/// Cycle detection: Instead of using a global visited set, we track
/// the current path from root to this node. A node is only rejected
/// if it appears in its own ancestor path (true cycle). This allows
/// legitimate dual appearances (nodes appearing in both supervision
/// tree and flat list).
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_tree_node<'a>(
    client: &'a reqwest::Client,
    base_url: &'a str,
    show_system: bool,
    show_stopped: bool,
    cache: &'a mut HashMap<String, FetchState<NodePayload>>,
    path: &'a mut Vec<String>,
    reference: &'a str,
    depth: usize,
    expanded_keys: &'a HashSet<(String, usize)>,
    refresh_gen: u64,
    seq_counter: &'a mut u64,
) -> Pin<Box<dyn Future<Output = Option<TreeNode>> + Send + 'a>> {
    Box::pin(async move {
        // Depth guard.
        if depth >= MAX_TREE_DEPTH {
            return None;
        }

        // Cycle guard: only reject if reference is in the current path
        // (true cycle), not if it appears elsewhere in the tree.
        if path.contains(&reference.to_string()) {
            return None;
        }
        path.push(reference.to_string());

        // Fetch using unified fetch+join path (force=false for cache-aware).
        let state = fetch_with_join(
            client,
            base_url,
            reference,
            cache,
            refresh_gen,
            seq_counter,
            false,
        )
        .await;

        let payload = match state {
            FetchState::Ready { value, .. } => value,
            FetchState::Error { .. } | FetchState::Unknown => {
                path.pop();
                return None;
            }
        };

        // Filter system procs and system actors.
        if !show_system && is_system_node(&payload.properties) {
            path.pop();
            return None;
        }

        // Filter stopped actors (failed nodes always visible).
        if !show_stopped
            && is_stopped_node(&payload.properties)
            && !is_failed_node(&payload.properties)
        {
            path.pop();
            return None;
        }

        let label = derive_label(&payload);
        let node_type = NodeType::from_properties(&payload.properties);
        let has_children = !payload.children.is_empty();
        let is_expanded = expanded_keys.contains(&(reference.to_string(), depth));

        // Build children if expanded.
        let mut children = Vec::new();
        if is_expanded && has_children {
            let is_proc_or_actor = matches!(
                payload.properties,
                NodeProperties::Proc { .. } | NodeProperties::Actor { .. }
            );

            // Extract system_children from the parent so we can filter
            // lazily without fetching each child individually.
            let system_children: HashSet<&str> = match &payload.properties {
                NodeProperties::Root {
                    system_children, ..
                }
                | NodeProperties::Host {
                    system_children, ..
                }
                | NodeProperties::Proc {
                    system_children, ..
                } => system_children.iter().map(|s| s.as_str()).collect(),
                _ => HashSet::new(),
            };

            // Extract stopped_children from proc payloads for lazy
            // filtering/graying without per-child fetches.
            let (stopped_children, parent_is_poisoned): (HashSet<&str>, bool) =
                match &payload.properties {
                    NodeProperties::Proc {
                        stopped_children,
                        is_poisoned,
                        ..
                    } => (
                        stopped_children.iter().map(|s| s.as_str()).collect(),
                        *is_poisoned,
                    ),
                    _ => (HashSet::new(), false),
                };

            let sorted = sorted_children(&payload);

            for child_ref in &sorted {
                // Filter order: system first, then stopped.
                if !show_system && system_children.contains(child_ref.as_str()) {
                    continue;
                }

                let child_is_stopped = stopped_children.contains(child_ref.as_str());
                let child_is_system = system_children.contains(child_ref.as_str());

                // Failed nodes are always visible (never filtered by show_stopped).
                // If the parent proc is poisoned, its stopped children may be
                // failed — don't filter them out (cache may be empty on first load).
                let child_is_failed = parent_is_poisoned
                    || get_cached_payload(cache, child_ref)
                        .is_some_and(|c| is_failed_node(&c.properties));
                if !show_stopped && child_is_stopped && !child_is_failed {
                    continue;
                }

                if is_proc_or_actor {
                    // Lazy: create placeholder for unexpanded children,
                    // but recursively build expanded ones.
                    let child_is_expanded =
                        expanded_keys.contains(&(child_ref.to_string(), depth + 1));

                    if child_is_expanded {
                        if let Some(child_node) = build_tree_node(
                            client,
                            base_url,
                            show_system,
                            show_stopped,
                            cache,
                            path,
                            child_ref,
                            depth + 1,
                            expanded_keys,
                            refresh_gen,
                            seq_counter,
                        )
                        .await
                        {
                            children.push(child_node);
                        } else {
                            // Recursive build failed - fall back to placeholder.
                            if let Some(cached) = get_cached_payload(cache, child_ref) {
                                // Fallback: also check cached payload.
                                if !show_stopped
                                    && is_stopped_node(&cached.properties)
                                    && !is_failed_node(&cached.properties)
                                {
                                    continue;
                                }
                                let mut node = TreeNode::from_payload(child_ref.clone(), cached);
                                node.stopped = node.stopped || child_is_stopped;
                                node.is_system = node.is_system || child_is_system;
                                children.push(node);
                            } else if child_is_stopped {
                                let mut node = TreeNode::placeholder_stopped(child_ref.clone());
                                node.is_system = child_is_system;
                                children.push(node);
                            } else {
                                let mut node = TreeNode::placeholder(child_ref.clone());
                                node.is_system = child_is_system;
                                children.push(node);
                            }
                        }
                    } else {
                        // Child is not expanded - use placeholder or cached data.
                        if let Some(cached) = get_cached_payload(cache, child_ref) {
                            // Fallback: also check cached payload.
                            if !show_stopped
                                && is_stopped_node(&cached.properties)
                                && !is_failed_node(&cached.properties)
                            {
                                continue;
                            }
                            let mut node = TreeNode::from_payload(child_ref.clone(), cached);
                            node.stopped = node.stopped || child_is_stopped;
                            node.is_system = node.is_system || child_is_system;
                            children.push(node);
                        } else if child_is_stopped {
                            let mut node = TreeNode::placeholder_stopped(child_ref.clone());
                            node.is_system = child_is_system;
                            children.push(node);
                        } else {
                            let mut node = TreeNode::placeholder(child_ref.clone());
                            node.is_system = child_is_system;
                            children.push(node);
                        }
                    }
                } else {
                    // Eager: recursively fetch (Root/Host parents).
                    if let Some(child_node) = build_tree_node(
                        client,
                        base_url,
                        show_system,
                        show_stopped,
                        cache,
                        path,
                        child_ref,
                        depth + 1,
                        expanded_keys,
                        refresh_gen,
                        seq_counter,
                    )
                    .await
                    {
                        children.push(child_node);
                    }
                }
            }
        }

        // A node is failed if it has failure info itself (actor with
        // failure_info, poisoned proc) OR if any of its children are
        // failed.  This propagates the unhealthy state upward so that
        // host and root nodes render red when the mesh contains failures.
        let children_failed = children.iter().any(|c| c.failed);
        let node = TreeNode {
            reference: reference.to_string(),
            label,
            node_type,
            expanded: is_expanded,
            fetched: true,
            has_children,
            stopped: is_stopped_node(&payload.properties),
            failed: is_failed_node(&payload.properties) || children_failed,
            is_system: is_system_node(&payload.properties),
            children,
        };

        // Pop from path before returning (restore path for sibling nodes).
        path.pop();

        Some(node)
    })
}

/// Compare reference strings using a "natural" order for trailing
/// `[N]` indices.
///
/// If both strings end with a bracketed numeric suffix (e.g.
/// `foo[2]`), compares their non-index prefixes lexicographically and
/// the numeric suffixes numerically so `...[2]` sorts before
/// `...[10]`. If either string lacks a trailing numeric index, falls
/// back to plain lexicographic comparison.
pub(crate) fn natural_ref_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    match (extract_trailing_index(a), extract_trailing_index(b)) {
        (Some((prefix_a, idx_a)), Some((prefix_b, idx_b))) => {
            prefix_a.cmp(prefix_b).then(idx_a.cmp(&idx_b))
        }
        _ => a.cmp(b),
    }
}

/// Clone and sort a payload's children by natural reference order.
pub(crate) fn sorted_children(payload: &NodePayload) -> Vec<String> {
    let mut children = payload.children.clone();
    children.sort_by(|a, b| natural_ref_cmp(a, b));
    children
}

/// Parse a trailing bracketed numeric index from a reference string.
///
/// Returns `(prefix, N)` for strings ending in `[N]` (e.g.
/// `foo[12]`), where `prefix` is everything before the final `[` and
/// `N` is the parsed `u64`. Returns `None` if the string does not end
/// in a well-formed numeric index.
pub(crate) fn extract_trailing_index(s: &str) -> Option<(&str, u64)> {
    let s = s.strip_suffix(']')?;
    let bracket = s.rfind('[')?;
    let num: u64 = s[bracket + 1..].parse().ok()?;
    Some((&s[..bracket], num))
}

#[cfg(test)]
mod tests {
    use algebra::JoinSemilattice;
    use hyperactor::introspect::NodePayload;
    use hyperactor::introspect::NodeProperties;

    use super::*;

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

    #[test]
    fn join_is_commutative() {
        let payload = mock_payload("test");
        let a = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 1,
            value: payload.clone(),
        };
        let b = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            generation: 1,
            value: payload.clone(),
        };

        let ab = a.join(&b);
        let ba = b.join(&a);

        match (&ab, &ba) {
            (
                FetchState::Ready {
                    stamp: stamp_ab, ..
                },
                FetchState::Ready {
                    stamp: stamp_ba, ..
                },
            ) => {
                assert_eq!(stamp_ab, stamp_ba);
            }
            _ => panic!("Both should be Ready"),
        }
    }

    #[test]
    fn join_is_associative() {
        let payload = mock_payload("test");
        let a = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 1,
            value: payload.clone(),
        };
        let b = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            generation: 1,
            value: payload.clone(),
        };
        let c = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 3000,
                seq: 1,
            },
            generation: 1,
            value: payload.clone(),
        };

        let ab_c = a.join(&b).join(&c);
        let a_bc = a.join(&b.join(&c));

        match (&ab_c, &a_bc) {
            (
                FetchState::Ready {
                    stamp: stamp1,
                    generation: gen1,
                    ..
                },
                FetchState::Ready {
                    stamp: stamp2,
                    generation: gen2,
                    ..
                },
            ) => {
                assert_eq!(stamp1, stamp2);
                assert_eq!(gen1, gen2);
            }
            _ => panic!("Both should be Ready"),
        }
    }

    #[test]
    fn join_is_idempotent() {
        let payload = mock_payload("test");
        let a = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 1,
            value: payload.clone(),
        };

        let aa = a.join(&a);

        match (&a, &aa) {
            (
                FetchState::Ready {
                    stamp: s1,
                    generation: g1,
                    ..
                },
                FetchState::Ready {
                    stamp: s2,
                    generation: g2,
                    ..
                },
            ) => {
                assert_eq!(s1, s2);
                assert_eq!(g1, g2);
            }
            _ => panic!("Both should be Ready"),
        }
    }

    #[test]
    fn join_unknown_is_identity() {
        let payload = mock_payload("test");
        let a = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 1,
            value: payload.clone(),
        };
        let unknown = FetchState::Unknown;

        let a_u = a.join(&unknown);
        let u_a = unknown.join(&a);

        match (&a_u, &u_a) {
            (
                FetchState::Ready {
                    stamp: s1,
                    generation: g1,
                    ..
                },
                FetchState::Ready {
                    stamp: s2,
                    generation: g2,
                    ..
                },
            ) => {
                assert_eq!(s1, s2);
                assert_eq!(g1, g2);
            }
            _ => panic!("Both should be Ready"),
        }
    }

    #[test]
    fn join_prefers_newer_stamp() {
        let payload = mock_payload("test");
        let older = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 1,
            value: payload.clone(),
        };
        let newer = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            generation: 1,
            value: payload.clone(),
        };

        let result = older.join(&newer);
        match result {
            FetchState::Ready { stamp, .. } => {
                assert_eq!(stamp.ts_micros, 2000);
            }
            _ => panic!("Expected Ready"),
        }
    }

    #[test]
    fn join_uses_seq_for_tie_break() {
        let payload = mock_payload("test");
        let first = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 1,
            value: payload.clone(),
        };
        let second = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 2,
            },
            generation: 1,
            value: payload.clone(),
        };

        let result = first.join(&second);
        match result {
            FetchState::Ready { stamp, .. } => {
                assert_eq!(stamp.seq, 2);
            }
            _ => panic!("Expected Ready"),
        }
    }

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
            msg: "fail".to_string(),
        };

        // Ready should win over Error with same stamp
        let result_re = ready.join(&error);
        assert!(matches!(result_re, FetchState::Ready { .. }));

        let result_er = error.join(&ready);
        assert!(matches!(result_er, FetchState::Ready { .. }));
    }

    #[test]
    fn join_error_states_newer_wins() {
        let e1 = FetchState::<NodePayload>::Error {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            msg: "first".to_string(),
        };
        let e2 = FetchState::Error {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            msg: "second".to_string(),
        };

        let result = e1.join(&e2);
        match result {
            FetchState::Error { stamp, msg } => {
                assert_eq!(stamp.ts_micros, 2000);
                assert_eq!(msg, "second");
            }
            _ => panic!("Expected Error"),
        }
    }

    #[test]
    fn join_error_equal_stamps_is_commutative() {
        let e1 = FetchState::<NodePayload>::Error {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            msg: "first".to_string(),
        };
        let e2 = FetchState::Error {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            msg: "second".to_string(),
        };

        let r1 = e1.join(&e2);
        let r2 = e2.join(&e1);

        match (&r1, &r2) {
            (FetchState::Error { msg: msg1, .. }, FetchState::Error { msg: msg2, .. }) => {
                assert_eq!(msg1, msg2);
            }
            _ => panic!("Both should be Error"),
        }
    }

    #[test]
    fn join_error_always_retries_on_fetch() {
        let error = FetchState::<NodePayload>::Error {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            msg: "network".to_string(),
        };

        let fresh_ready = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            generation: 5,
            value: mock_payload("fresh"),
        };

        let result = error.join(&fresh_ready);
        assert!(matches!(result, FetchState::Ready { .. }));

        let result2 = fresh_ready.join(&error);
        assert!(matches!(result2, FetchState::Ready { .. }));
    }

    #[test]
    fn join_refresh_staleness_triggers_refetch() {
        let payload = mock_payload("test");
        let stale = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 5,
            value: payload.clone(),
        };

        let fresh = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            generation: 10,
            value: payload.clone(),
        };

        let result = stale.join(&fresh);
        match result {
            FetchState::Ready { generation, .. } => {
                assert_eq!(generation, 10);
            }
            _ => panic!("Expected Ready"),
        }
    }

    #[test]
    fn cache_join_commutativity_ready_vs_error() {
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
            msg: "fail".to_string(),
        };

        let re = ready.join(&error);
        let er = error.join(&ready);

        assert!(matches!(re, FetchState::Ready { .. }));
        assert!(matches!(er, FetchState::Ready { .. }));
    }

    #[test]
    fn join_cache_preserves_ready_when_generation_matches() {
        let payload = mock_payload("current");
        let current = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 10,
            value: payload.clone(),
        };
        let fresh = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            generation: 10,
            value: payload.clone(),
        };

        let result = current.join(&fresh);
        match result {
            FetchState::Ready { generation, .. } => {
                assert_eq!(generation, 10);
            }
            _ => panic!("Expected Ready state"),
        }

        let stale = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 5,
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
                assert_eq!(generation, 10);
            }
            _ => panic!("Expected Ready state"),
        }
    }

    #[test]
    fn error_state_does_not_cache() {
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

        let result = error1.join(&error2);
        match result {
            FetchState::Error { stamp, .. } => {
                assert_eq!(stamp.ts_micros, 2000);
            }
            _ => panic!("Expected Error state"),
        }

        let unknown = FetchState::Unknown;
        let result = error1.join(&unknown);
        assert!(matches!(result, FetchState::Error { .. }));
    }

    #[test]
    fn cache_join_ready_vs_ready_equal_stamps() {
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

        let result = ready1.join(&ready2);
        match result {
            FetchState::Ready { generation, .. } => {
                assert_eq!(generation, 5);
            }
            _ => panic!("Expected Ready state"),
        }
    }

    #[test]
    fn stale_cache_recovery() {
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

    #[test]
    fn corrupted_cached_state_recovery() {
        let bad = FetchState::<NodePayload>::Unknown;
        let good = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 10,
            value: mock_payload("repaired"),
        };

        let result = bad.join(&good);
        match result {
            FetchState::Ready { value, .. } => {
                assert_eq!(value.identity, "repaired");
            }
            _ => panic!("Expected recovery to Ready"),
        }

        let result2 = good.join(&bad);
        match result2 {
            FetchState::Ready { value, .. } => {
                assert_eq!(value.identity, "repaired");
            }
            _ => panic!("Expected recovery to Ready"),
        }
    }

    #[test]
    fn out_of_order_fetch_completion() {
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

    #[test]
    fn timestamp_monotonicity_across_refreshes() {
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

        for i in 1..stamps.len() {
            assert!(stamps[i] > stamps[i - 1]);
        }
    }
}
