/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::str::FromStr;
use std::time::Duration;

use hyperactor::ActorId;
use hyperactor::ProcId;
use hyperactor::introspect::NodePayload;
use hyperactor::introspect::NodeProperties;
use serde_json::Value;

/// Derive a human-friendly label for a resolved node payload.
///
/// Kept as a free function (rather than an inherent `NodePayload`
/// method) because `NodePayload` lives in
/// `hyperactor_mesh::mesh_admin`; adding an extension trait here
/// would be more ceremony than it's worth for a small formatting
/// helper.
///
/// Uses `NodeProperties` to format a concise label for the tree view:
/// roots show host counts, hosts show proc counts, procs show actor
/// counts, and actors are rendered as `name[pid]` when the identity
/// parses as an `ActorId`.
pub(crate) fn derive_label(payload: &NodePayload) -> String {
    match &payload.properties {
        NodeProperties::Root { num_hosts, .. } => format!("Mesh Root ({} hosts)", num_hosts),
        NodeProperties::Host {
            addr,
            num_procs,
            system_children,
            ..
        } => {
            let num_system = system_children.len();
            let num_user = num_procs.saturating_sub(num_system);
            let mut parts = Vec::new();
            if num_user > 0 {
                parts.push(format!("{} user", num_user));
            }
            if num_system > 0 {
                parts.push(format!("{} system", num_system));
            }
            if parts.is_empty() {
                format!("{}  ({} procs)", addr, num_procs)
            } else {
                format!("{}  ({} procs: {})", addr, num_procs, parts.join(", "))
            }
        }
        NodeProperties::Proc {
            proc_name,
            num_actors,
            system_children,
            stopped_children,
            stopped_retention_cap,
            is_poisoned,
            failed_actor_count,
            ..
        } => {
            let short = ProcId::from_str(proc_name)
                .ok()
                .and_then(|pid| pid.name().cloned())
                .unwrap_or_else(|| proc_name.clone());
            let num_system = system_children.len();
            let num_stopped = stopped_children.len();
            let num_live = num_actors.saturating_sub(num_system);
            let total = num_actors + num_stopped;
            let mut parts = Vec::new();
            if num_system > 0 {
                parts.push(format!("{} system", num_system));
            }
            if num_live > 0 {
                parts.push(format!("{} live", num_live));
            }
            if num_stopped > 0 {
                if num_stopped >= *stopped_retention_cap && *stopped_retention_cap > 0 {
                    parts.push(format!("{} stopped (max retained)", num_stopped));
                } else {
                    parts.push(format!("{} stopped", num_stopped));
                }
            }
            let base = if parts.is_empty() {
                format!("{}  ({} actors)", short, total)
            } else {
                format!("{}  ({} actors: {})", short, total, parts.join(", "))
            };
            if *is_poisoned {
                format!("{}  [POISONED: {} failed]", base, failed_actor_count)
            } else {
                base
            }
        }
        NodeProperties::Actor { .. } => match ActorId::from_str(&payload.identity) {
            Ok(actor_id) => format!("{}[{}]", actor_id.name(), actor_id.pid()),
            Err(_) => payload.identity.clone(),
        },
        NodeProperties::Error { code, message } => {
            format!("[error] {}: {}", code, message)
        }
    }
}

/// Derive a display label from an opaque reference string without
/// fetching.
///
/// If the reference parses as an `ActorId`, format it as `name[pid]`;
/// otherwise fall back to showing the raw reference.
pub(crate) fn derive_label_from_ref(reference: &str) -> String {
    match ActorId::from_str(reference) {
        Ok(actor_id) => format!("{}[{}]", actor_id.name(), actor_id.pid()),
        Err(_) => reference.to_string(),
    }
}

/// Produce a compact, human-readable summary string for a recorded
/// event.
///
/// Prefers common message-like fields (`message`, then `msg`),
/// otherwise renders a useful hint such as `handler: ...`. As a
/// fallback, formats up to three key/value pairs from the event
/// fields (using `format_value`) to keep the TUI line short; if
/// nothing matches, falls back to the event `name`.
pub(crate) fn format_event_summary(name: &str, fields: &Value) -> String {
    if let Some(obj) = fields.as_object() {
        if let Some(msg) = obj.get("message").and_then(|v| v.as_str()) {
            return msg.to_string();
        }
        if let Some(msg) = obj.get("msg").and_then(|v| v.as_str()) {
            return msg.to_string();
        }
        if let Some(handler) = obj.get("handler").and_then(|v| v.as_str()) {
            return format!("handler: {}", handler);
        }
        if !obj.is_empty() {
            let summary: Vec<String> = obj
                .iter()
                .take(3)
                .map(|(k, v)| format!("{}={}", k, format_value(v)))
                .collect();
            if !summary.is_empty() {
                return summary.join(" ");
            }
        }
    }
    name.to_string()
}

/// Format a JSON value into a short, single-line representation
/// suitable for the TUI.
///
/// Strings/numbers/bools render as-is; `null` renders as `"null"`.
/// Arrays and objects are summarized by their length/field count
/// (e.g. `"[3]"`, `"{5}"`) to avoid dumping large payloads into the
/// event list.
pub(crate) fn format_value(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        Value::Array(arr) => format!("[{}]", arr.len()),
        Value::Object(obj) => format!("{{{}}}", obj.len()),
    }
}

/// Convert an ISO 8601 UTC timestamp (e.g.
/// "2026-02-11T19:11:01.265Z") to a local-timezone HH:MM:SS string.
/// Falls back to extracting the raw UTC time portion if parsing
/// fails.
pub(crate) fn format_local_time(timestamp: &str) -> String {
    chrono::DateTime::parse_from_rfc3339(timestamp)
        .map(|dt| {
            dt.with_timezone(&chrono::Local)
                .format("%H:%M:%S")
                .to_string()
        })
        .unwrap_or_else(|_| timestamp.get(11..19).unwrap_or(timestamp).to_string())
}

/// Format an ISO-8601 timestamp as a human-readable relative time
/// from now (e.g. "just now", "5s ago", "3m 12s ago", "1h 7m ago").
pub(crate) fn format_relative_time(timestamp: &str) -> String {
    match chrono::DateTime::parse_from_rfc3339(timestamp) {
        Ok(parsed) => {
            let now = chrono::Utc::now();
            let duration = now.signed_duration_since(parsed);
            let total_secs = duration.num_seconds();
            if total_secs < 2 {
                "just now".to_string()
            } else if total_secs < 60 {
                format!("{}s ago", total_secs)
            } else if total_secs < 3600 {
                let mins = total_secs / 60;
                let secs = total_secs % 60;
                format!("{}m {}s ago", mins, secs)
            } else {
                let hours = total_secs / 3600;
                let mins = (total_secs % 3600) / 60;
                format!("{}h {}m ago", hours, mins)
            }
        }
        Err(_) => timestamp.to_string(),
    }
}

/// Format uptime duration from ISO-8601 start timestamp.
///
/// Rounds to nearest 30 seconds for cleaner display.
pub(crate) fn format_uptime(started_at: &str) -> String {
    match chrono::DateTime::parse_from_rfc3339(started_at) {
        Ok(start_time) => {
            let now = chrono::Utc::now();
            let duration = now.signed_duration_since(start_time);
            let total_secs = duration.num_seconds();
            let rounded_secs = ((total_secs + 15) / 30) * 30;
            let std_duration = Duration::from_secs(rounded_secs as u64);
            humantime::format_duration(std_duration).to_string()
        }
        Err(_) => "unknown".to_string(),
    }
}
