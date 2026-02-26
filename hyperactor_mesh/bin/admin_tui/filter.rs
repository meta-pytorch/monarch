/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use hyperactor::introspect::NodeProperties;

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

/// Returns true if the node properties indicate a system actor or proc.
pub(crate) fn is_system_node(properties: &NodeProperties) -> bool {
    matches!(
        properties,
        NodeProperties::Proc {
            is_system: true,
            ..
        } | NodeProperties::Actor {
            is_system: true,
            ..
        }
    )
}

#[cfg(test)]
mod tests {
    use hyperactor::introspect::FailureInfo;

    use super::*;

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
