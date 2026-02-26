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
