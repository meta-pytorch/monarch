/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use hyperactor_mesh::introspect::NodeProperties;

/// Returns true if the node's actor status indicates it has stopped or failed.
pub(crate) fn is_stopped_node(properties: &NodeProperties) -> bool {
    matches!(
        properties,
        NodeProperties::Actor { actor_status, .. }
            if actor_status.starts_with("stopped:") || actor_status.starts_with("failed:")
    )
}

/// Returns true if the node has structured failure info (actor with failure_info)
/// or is a poisoned proc.
pub(crate) fn is_failed_node(properties: &NodeProperties) -> bool {
    matches!(
        properties,
        NodeProperties::Actor {
            failure_info: Some(_),
            ..
        } | NodeProperties::Proc {
            is_poisoned: true,
            ..
        }
    )
}

/// Returns true if the node properties indicate a system actor.
pub(crate) fn is_system_node(properties: &NodeProperties) -> bool {
    // Procs are never system — only actors are. Procs are always
    // visible regardless of the 's' toggle.
    matches!(
        properties,
        NodeProperties::Actor {
            is_system: true,
            ..
        }
    )
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use hyperactor_mesh::introspect::FailureInfo;

    use super::*;

    fn mock_actor_id() -> hyperactor::reference::ActorId {
        use std::str::FromStr;
        hyperactor::reference::ActorId::from_str("unix:@test,world,a[0]").unwrap()
    }

    fn actor_props(status: &str) -> NodeProperties {
        NodeProperties::Actor {
            actor_status: status.to_string(),
            actor_type: "test".to_string(),
            messages_processed: 0,
            created_at: Some(SystemTime::UNIX_EPOCH),
            last_message_handler: None,
            total_processing_time_us: 0,
            flight_recorder: None,
            failure_info: None,
            is_system: false,
        }
    }

    #[test]
    fn is_stopped_node_true_for_stopped_prefix() {
        assert!(is_stopped_node(&actor_props(
            "stopped:sleep completed (2.3s)"
        )));
    }

    #[test]
    fn is_stopped_node_true_for_failed_prefix() {
        assert!(is_stopped_node(&actor_props("failed:panic in handler")));
    }

    #[test]
    fn is_stopped_node_false_for_running() {
        assert!(!is_stopped_node(&actor_props("Running")));
    }

    #[test]
    fn is_stopped_node_false_without_colon() {
        assert!(!is_stopped_node(&actor_props("stopped")));
        assert!(!is_stopped_node(&actor_props("failed")));
    }

    #[test]
    fn is_stopped_node_false_for_non_actor_variants() {
        let root = NodeProperties::Root {
            num_hosts: 1,
            started_at: SystemTime::UNIX_EPOCH,
            started_by: "".to_string(),
            system_children: vec![],
        };
        assert!(!is_stopped_node(&root));

        let host = NodeProperties::Host {
            addr: "127.0.0.1:1234".to_string(),
            num_procs: 1,
            system_children: vec![],
            memory: Default::default(),
        };
        assert!(!is_stopped_node(&host));

        let proc_props = NodeProperties::Proc {
            proc_name: "proc".to_string(),
            num_actors: 0,
            system_children: vec![],
            stopped_children: vec![],
            stopped_retention_cap: 0,
            is_poisoned: false,
            failed_actor_count: 0,
            debug: Default::default(),
        };
        assert!(!is_stopped_node(&proc_props));
    }

    #[test]
    fn is_failed_node_with_failure_info() {
        let props = NodeProperties::Actor {
            actor_status: "failed:panic".to_string(),
            actor_type: "test".to_string(),
            messages_processed: 0,
            created_at: Some(SystemTime::UNIX_EPOCH),
            last_message_handler: None,
            total_processing_time_us: 0,
            flight_recorder: None,
            failure_info: Some(FailureInfo {
                error_message: "boom".to_string(),
                root_cause_actor: mock_actor_id(),
                root_cause_name: None,
                occurred_at: SystemTime::UNIX_EPOCH,
                is_propagated: false,
            }),
            is_system: false,
        };
        assert!(is_failed_node(&props));
    }

    #[test]
    fn is_failed_node_without_failure_info() {
        assert!(!is_failed_node(&actor_props("Running")));
    }

    #[test]
    fn is_failed_node_returns_true_for_poisoned_proc() {
        let props = NodeProperties::Proc {
            proc_name: "myproc".to_string(),
            num_actors: 1,
            system_children: vec![],
            stopped_children: vec![],
            stopped_retention_cap: 0,
            is_poisoned: true,
            failed_actor_count: 1,
            debug: Default::default(),
        };
        assert!(is_failed_node(&props));
    }

    #[test]
    fn is_failed_node_returns_false_for_healthy_proc() {
        let props = NodeProperties::Proc {
            proc_name: "myproc".to_string(),
            num_actors: 1,
            system_children: vec![],
            stopped_children: vec![],
            stopped_retention_cap: 0,
            is_poisoned: false,
            failed_actor_count: 0,
            debug: Default::default(),
        };
        assert!(!is_failed_node(&props));
    }
}
