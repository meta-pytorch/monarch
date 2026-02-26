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
