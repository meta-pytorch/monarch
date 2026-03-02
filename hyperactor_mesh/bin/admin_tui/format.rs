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

#[cfg(test)]
mod tests {
    use hyperactor::introspect::NodePayload;
    use hyperactor::introspect::NodeProperties;
    use serde_json::Value;

    use super::*;

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
        assert_eq!(derive_label(&payload), "10.0.0.1:8000  (2 procs: 2 system)");
    }

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
        assert_eq!(
            derive_label(&payload),
            "myproc  (5 actors: 2 system, 3 live)"
        );
    }

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
        assert_eq!(
            derive_label(&payload),
            "myproc  (5 actors: 3 live, 2 stopped)"
        );
    }

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
        assert!(derive_label(&payload).contains("3 stopped (max retained)"));
    }

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
        let label = derive_label(&payload);
        assert!(label.contains("1 stopped"));
        assert!(!label.contains("max retained"));
    }

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
        assert_eq!(
            derive_label(&payload),
            "myproc  (7 actors: 1 system, 4 live, 2 stopped)"
        );
    }

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
        assert_eq!(derive_label(&payload), "myproc  (2 actors: 2 stopped)");
    }

    #[test]
    fn derive_label_proc_saturating_sub_prevents_underflow() {
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
        let label = derive_label(&payload);
        assert!(label.contains("3 system"));
        assert!(!label.contains("live"));
    }

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

    #[test]
    fn format_value_string() {
        let v = Value::String("hello world".to_string());
        assert_eq!(format_value(&v), "hello world");
    }

    #[test]
    fn format_value_number() {
        let v = serde_json::json!(42);
        assert_eq!(format_value(&v), "42");
    }

    #[test]
    fn format_value_bool() {
        assert_eq!(format_value(&serde_json::json!(true)), "true");
        assert_eq!(format_value(&serde_json::json!(false)), "false");
    }

    #[test]
    fn format_value_null() {
        assert_eq!(format_value(&Value::Null), "null");
    }

    #[test]
    fn format_value_array() {
        let v = serde_json::json!([1, 2, 3]);
        assert_eq!(format_value(&v), "[3]");
    }

    #[test]
    fn format_value_object() {
        let v = serde_json::json!({"a": 1, "b": 2, "c": 3, "d": 4, "e": 5});
        assert_eq!(format_value(&v), "{5}");
    }

    #[test]
    fn format_event_summary_message_field() {
        let fields = serde_json::json!({"message": "something happened", "other": 42});
        assert_eq!(
            format_event_summary("event_name", &fields),
            "something happened"
        );
    }

    #[test]
    fn format_event_summary_handler_field() {
        let fields = serde_json::json!({"handler": "on_tick", "count": 7});
        assert_eq!(
            format_event_summary("event_name", &fields),
            "handler: on_tick"
        );
    }

    #[test]
    fn format_event_summary_fallback_to_name() {
        let fields = Value::Null;
        assert_eq!(format_event_summary("my_event", &fields), "my_event");
    }

    #[test]
    fn format_local_time_invalid_string_fallback() {
        assert_eq!(format_local_time("xxxxxxxxxxx12:34:56yyy"), "12:34:56");
    }

    #[test]
    fn format_local_time_too_short_fallback() {
        assert_eq!(format_local_time("short"), "short");
    }

    #[test]
    fn format_relative_time_parse_failure_fallback() {
        assert_eq!(format_relative_time("not-a-date"), "not-a-date");
    }

    #[test]
    fn format_uptime_parse_failure() {
        assert_eq!(format_uptime("garbage"), "unknown");
    }
}
