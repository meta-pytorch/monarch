/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::str::FromStr;
use std::time::Duration;
use std::time::SystemTime;

use hyperactor::reference as hyperactor_reference;
use hyperactor_mesh::introspect::NodePayload;
use hyperactor_mesh::introspect::NodeProperties;
use hyperactor_mesh::introspect::NodeRef;
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
            addr, num_procs, ..
        } => {
            format!("{}  ({} procs)", addr, num_procs)
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
            let short = hyperactor_reference::ProcId::from_str(proc_name)
                .map(|pid| {
                    pid.label()
                        .map(|l| l.as_str().to_string())
                        .unwrap_or_else(|| pid.id().to_string())
                })
                .unwrap_or_else(|_| proc_name.clone());
            let num_system = system_children.len();
            let num_stopped = stopped_children.len();
            let num_user = num_actors.saturating_sub(num_system);
            let total = num_actors + num_stopped;
            let mut parts = Vec::new();
            if num_system > 0 {
                parts.push(format!("{} system", num_system));
            }
            if num_user > 0 {
                parts.push(format!("{} user", num_user));
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
        NodeProperties::Actor { .. } => match &payload.identity {
            NodeRef::Actor(actor_id) => format!("{}[{}]", actor_id.name(), actor_id.pid()),
            other => other.to_string(),
        },
        NodeProperties::Error { code, message } => {
            format!("[error] {}: {}", code, message)
        }
    }
}

/// Derive a display label from a typed node reference without
/// fetching.
///
/// For actor references, format as `name[pid]`; for all others, fall
/// back to the `Display` representation.
pub(crate) fn derive_label_from_ref(reference: &NodeRef) -> String {
    match reference {
        NodeRef::Actor(actor_id) => format!("{}[{}]", actor_id.name(), actor_id.pid()),
        other => other.to_string(),
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

/// Convert a `SystemTime` to a `chrono::DateTime<Utc>`.
fn system_time_to_chrono(t: &SystemTime) -> chrono::DateTime<chrono::Utc> {
    let dur = t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    chrono::DateTime::from_timestamp(dur.as_secs() as i64, dur.subsec_nanos()).unwrap_or_default()
}

/// Format a `SystemTime` as a local-timezone HH:MM:SS string.
pub(crate) fn format_system_time_local(t: &SystemTime) -> String {
    system_time_to_chrono(t)
        .with_timezone(&chrono::Local)
        .format("%H:%M:%S")
        .to_string()
}

/// Format a `SystemTime` as a human-readable relative time from now
/// (e.g. "just now", "5s ago", "3m 12s ago").
pub(crate) fn format_system_time_relative(t: &SystemTime) -> String {
    match t.elapsed() {
        Ok(d) if d.as_secs() < 2 => "just now".to_string(),
        Ok(d) => format!("{} ago", humantime::format_duration(d)),
        Err(_) => "just now".to_string(),
    }
}

/// Format uptime from a `SystemTime` start point.
///
/// Rounds to nearest 30 seconds for cleaner display.
pub(crate) fn format_system_time_uptime(started_at: &SystemTime) -> String {
    match started_at.elapsed() {
        Ok(d) => {
            let total_secs = d.as_secs();
            let rounded_secs = ((total_secs + 15) / 30) * 30;
            humantime::format_duration(Duration::from_secs(rounded_secs)).to_string()
        }
        Err(_) => "unknown".to_string(),
    }
}

/// Format a `SystemTime` as an ISO-8601 string for display.
pub(crate) fn format_system_time_iso(t: &SystemTime) -> String {
    system_time_to_chrono(t).to_rfc3339()
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::time::SystemTime;

    use hyperactor::reference as hyperactor_reference;
    use hyperactor_mesh::introspect::NodePayload;
    use hyperactor_mesh::introspect::NodeProperties;
    use serde_json::Value;

    use super::*;

    fn mock_actor_ref(name: &str) -> NodeRef {
        let id_str = format!("unix:@test,world,{}[0]", name);
        NodeRef::Actor(hyperactor_reference::ActorId::from_str(&id_str).unwrap())
    }

    fn mock_proc_ref(name: &str) -> NodeRef {
        let id_str = format!("unix:@test,{}", name);
        NodeRef::Proc(hyperactor_reference::ProcId::from_str(&id_str).unwrap())
    }

    fn mock_host_ref(name: &str) -> NodeRef {
        let id_str = format!("unix:@test,world,{}[0]", name);
        NodeRef::Host(hyperactor_reference::ActorId::from_str(&id_str).unwrap())
    }

    fn proc_payload(proc_name: &str, props: NodeProperties) -> NodePayload {
        NodePayload {
            identity: mock_proc_ref(proc_name),
            properties: props,
            children: vec![],
            parent: None,
            as_of: SystemTime::now(),
        }
    }

    #[test]
    fn derive_label_root_basic() {
        let payload = NodePayload {
            identity: NodeRef::Root,
            properties: NodeProperties::Root {
                num_hosts: 3,
                started_at: SystemTime::UNIX_EPOCH,
                started_by: "testuser".to_string(),
                system_children: vec![mock_actor_ref("sys1")],
            },
            children: vec![
                mock_host_ref("h1"),
                mock_host_ref("h2"),
                mock_host_ref("h3"),
            ],
            parent: None,
            as_of: SystemTime::now(),
        };
        assert_eq!(derive_label(&payload), "Mesh Root (3 hosts)");
    }

    #[test]
    fn derive_label_host_no_system_children() {
        let payload = NodePayload {
            identity: mock_host_ref("h1"),
            properties: NodeProperties::Host {
                addr: "10.0.0.1:8000".to_string(),
                num_procs: 3,
                system_children: vec![],
            },
            children: vec![],
            parent: None,
            as_of: SystemTime::now(),
        };
        assert_eq!(derive_label(&payload), "10.0.0.1:8000  (3 procs)");
    }

    #[test]
    fn derive_label_host_with_system_children() {
        let payload = NodePayload {
            identity: mock_host_ref("h1"),
            properties: NodeProperties::Host {
                addr: "10.0.0.1:8000".to_string(),
                num_procs: 5,
                system_children: vec![mock_actor_ref("sys1"), mock_actor_ref("sys2")],
            },
            children: vec![],
            parent: None,
            as_of: SystemTime::now(),
        };
        assert_eq!(derive_label(&payload), "10.0.0.1:8000  (5 procs)");
    }

    #[test]
    fn derive_label_host_all_system() {
        let payload = NodePayload {
            identity: mock_host_ref("h1"),
            properties: NodeProperties::Host {
                addr: "10.0.0.1:8000".to_string(),
                num_procs: 2,
                system_children: vec![mock_actor_ref("s1"), mock_actor_ref("s2")],
            },
            children: vec![],
            parent: None,
            as_of: SystemTime::now(),
        };
        assert_eq!(derive_label(&payload), "10.0.0.1:8000  (2 procs)");
    }

    #[test]
    fn derive_label_proc_no_system_no_stopped() {
        let payload = proc_payload(
            "myproc",
            NodeProperties::Proc {
                proc_name: "myproc".to_string(),
                num_actors: 4,
                system_children: vec![],
                stopped_children: vec![],
                stopped_retention_cap: 0,
                is_poisoned: false,
                failed_actor_count: 0,
            },
        );
        assert_eq!(derive_label(&payload), "myproc  (4 actors: 4 user)");
    }

    #[test]
    fn derive_label_proc_with_system_no_stopped() {
        let payload = proc_payload(
            "myproc",
            NodeProperties::Proc {
                proc_name: "myproc".to_string(),
                num_actors: 5,
                system_children: vec![mock_actor_ref("sys1"), mock_actor_ref("sys2")],
                stopped_children: vec![],
                stopped_retention_cap: 0,
                is_poisoned: false,
                failed_actor_count: 0,
            },
        );
        assert_eq!(
            derive_label(&payload),
            "myproc  (5 actors: 2 system, 3 user)"
        );
    }

    #[test]
    fn derive_label_proc_with_stopped() {
        let payload = proc_payload(
            "myproc",
            NodeProperties::Proc {
                proc_name: "myproc".to_string(),
                num_actors: 3,
                system_children: vec![],
                stopped_children: vec![mock_actor_ref("s1"), mock_actor_ref("s2")],
                stopped_retention_cap: 100,
                is_poisoned: false,
                failed_actor_count: 0,
            },
        );
        assert_eq!(
            derive_label(&payload),
            "myproc  (5 actors: 3 user, 2 stopped)"
        );
    }

    #[test]
    fn derive_label_proc_stopped_at_retention_cap() {
        let payload = proc_payload(
            "myproc",
            NodeProperties::Proc {
                proc_name: "myproc".to_string(),
                num_actors: 1,
                system_children: vec![],
                stopped_children: vec![
                    mock_actor_ref("s1"),
                    mock_actor_ref("s2"),
                    mock_actor_ref("s3"),
                ],
                stopped_retention_cap: 3,
                is_poisoned: false,
                failed_actor_count: 0,
            },
        );
        assert!(derive_label(&payload).contains("3 stopped (max retained)"));
    }

    #[test]
    fn derive_label_proc_stopped_retention_cap_zero_never_annotates() {
        let payload = proc_payload(
            "myproc",
            NodeProperties::Proc {
                proc_name: "myproc".to_string(),
                num_actors: 0,
                system_children: vec![],
                stopped_children: vec![mock_actor_ref("s1")],
                stopped_retention_cap: 0,
                is_poisoned: false,
                failed_actor_count: 0,
            },
        );
        let label = derive_label(&payload);
        assert!(label.contains("1 stopped"));
        assert!(!label.contains("max retained"));
    }

    #[test]
    fn derive_label_proc_system_and_stopped_and_user() {
        let payload = proc_payload(
            "myproc",
            NodeProperties::Proc {
                proc_name: "myproc".to_string(),
                num_actors: 5,
                system_children: vec![mock_actor_ref("sys1")],
                stopped_children: vec![mock_actor_ref("dead1"), mock_actor_ref("dead2")],
                stopped_retention_cap: 100,
                is_poisoned: false,
                failed_actor_count: 0,
            },
        );
        assert_eq!(
            derive_label(&payload),
            "myproc  (7 actors: 1 system, 4 user, 2 stopped)"
        );
    }

    #[test]
    fn derive_label_proc_all_stopped_none_user() {
        let payload = proc_payload(
            "myproc",
            NodeProperties::Proc {
                proc_name: "myproc".to_string(),
                num_actors: 0,
                system_children: vec![],
                stopped_children: vec![mock_actor_ref("d1"), mock_actor_ref("d2")],
                stopped_retention_cap: 100,
                is_poisoned: false,
                failed_actor_count: 0,
            },
        );
        assert_eq!(derive_label(&payload), "myproc  (2 actors: 2 stopped)");
    }

    #[test]
    fn derive_label_proc_saturating_sub_prevents_underflow() {
        let payload = proc_payload(
            "myproc",
            NodeProperties::Proc {
                proc_name: "myproc".to_string(),
                num_actors: 1,
                system_children: vec![
                    mock_actor_ref("s1"),
                    mock_actor_ref("s2"),
                    mock_actor_ref("s3"),
                ],
                stopped_children: vec![],
                stopped_retention_cap: 0,
                is_poisoned: false,
                failed_actor_count: 0,
            },
        );
        let label = derive_label(&payload);
        assert!(label.contains("3 system"));
        assert!(!label.contains("user"));
    }

    #[test]
    fn derive_label_proc_poisoned() {
        let payload = proc_payload(
            "myproc",
            NodeProperties::Proc {
                proc_name: "myproc".to_string(),
                num_actors: 2,
                system_children: vec![],
                stopped_children: vec![mock_actor_ref("dead1")],
                stopped_retention_cap: 100,
                is_poisoned: true,
                failed_actor_count: 1,
            },
        );
        let label = derive_label(&payload);
        assert!(label.contains("[POISONED: 1 failed]"));
    }

    #[test]
    fn derive_label_proc_not_poisoned() {
        let payload = proc_payload(
            "myproc",
            NodeProperties::Proc {
                proc_name: "myproc".to_string(),
                num_actors: 3,
                system_children: vec![],
                stopped_children: vec![],
                stopped_retention_cap: 100,
                is_poisoned: false,
                failed_actor_count: 0,
            },
        );
        let label = derive_label(&payload);
        assert!(!label.contains("POISONED"));
    }

    #[test]
    fn derive_label_actor_standard_actor_id() {
        let actor_id =
            hyperactor_reference::ActorId::from_str("unix:@abc123,myworld,worker[3]").unwrap();
        let proc_id = hyperactor_reference::ProcId::from_str("unix:@abc123,myworld").unwrap();
        let payload = NodePayload {
            identity: NodeRef::Actor(actor_id),
            properties: NodeProperties::Actor {
                actor_status: "Running".to_string(),
                actor_type: "Worker".to_string(),
                messages_processed: 42,
                created_at: Some(SystemTime::UNIX_EPOCH),
                last_message_handler: Some("handle_task".to_string()),
                total_processing_time_us: 1000,
                flight_recorder: None,
                failure_info: None,
                is_system: false,
            },
            children: vec![],
            parent: Some(NodeRef::Proc(proc_id)),
            as_of: SystemTime::now(),
        };
        assert_eq!(derive_label(&payload), "worker[3]");
    }

    #[test]
    fn derive_label_actor_non_actor_identity_falls_back() {
        // When an Actor node has a non-Actor identity (shouldn't
        // happen in practice), derive_label falls back to Display.
        let payload = NodePayload {
            identity: NodeRef::Root,
            properties: NodeProperties::Actor {
                actor_status: "Running".to_string(),
                actor_type: "Unknown".to_string(),
                messages_processed: 0,
                created_at: Some(SystemTime::UNIX_EPOCH),
                last_message_handler: None,
                total_processing_time_us: 0,
                flight_recorder: None,
                failure_info: None,
                is_system: false,
            },
            children: vec![],
            parent: None,
            as_of: SystemTime::now(),
        };
        assert_eq!(derive_label(&payload), "root");
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
}
