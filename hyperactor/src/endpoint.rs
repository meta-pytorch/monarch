/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Generic send endpoints.

use std::fmt;

use hyperactor_config::Flattrs;
use serde::Deserialize;
use serde::Serialize;

use crate::ActorAddr;
use crate::PortAddr;
use crate::context;
use crate::mailbox::PortLocation;

/// The logical location of an endpoint.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, typeuri::Named)]
pub enum EndpointLocation {
    /// An actor endpoint.
    Actor(ActorAddr),
    /// A port endpoint.
    Port(PortAddr),
    /// A local port handle that has not been bound to a routable port.
    Local {
        /// The actor that owns the local endpoint.
        actor: ActorAddr,
        /// The local endpoint's message type.
        message_type: String,
    },
}

impl EndpointLocation {
    /// The actor address associated with this endpoint location.
    pub fn actor_addr(&self) -> ActorAddr {
        match self {
            Self::Actor(actor) => actor.clone(),
            Self::Port(port) => port.actor_addr(),
            Self::Local { actor, .. } => actor.clone(),
        }
    }
}

impl From<PortLocation> for EndpointLocation {
    fn from(location: PortLocation) -> Self {
        match location {
            PortLocation::Bound(port) => Self::Port(port),
            PortLocation::Unbound(actor, message_type) => Self::Local {
                actor,
                message_type: message_type.to_string(),
            },
        }
    }
}

impl fmt::Display for EndpointLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Actor(actor) => write!(f, "{}", actor),
            Self::Port(port) => write!(f, "{}", port),
            Self::Local {
                actor,
                message_type,
            } => write!(f, "{}<{}>", actor, message_type),
        }
    }
}

/// A typed endpoint that can receive `M`.
///
/// This trait abstracts over local actor handles, local port handles, remote
/// actor refs, remote port refs, and one-shot ports. It is sealed so that
/// Hyperactor owns the post semantics for each endpoint kind.
pub trait Endpoint<M>: crate::private::Sealed {
    /// The logical location of this endpoint.
    fn endpoint_location(&self) -> EndpointLocation;

    /// Post `message` to this endpoint from `cx`.
    fn post<C>(self, cx: &C, message: M)
    where
        C: context::Actor;
}

/// A typed endpoint that can receive `M` with message headers.
///
/// `RemoteEndpoint` is implemented only for endpoints whose post path preserves
/// headers.
pub trait RemoteEndpoint<M>: Endpoint<M> {
    /// Post `message` and `headers` to this endpoint from `cx`.
    fn post_with_headers<C>(self, cx: &C, headers: Flattrs, message: M)
    where
        C: context::Actor;
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use hyperactor_config::Flattrs;
    use hyperactor_config::declare_attrs;
    use tokio::sync::mpsc;
    use typeuri::Named;

    use super::*;
    use crate::Actor;
    use crate::Handler;
    use crate::PortRef;
    use crate::actor::Referable;
    use crate::actor::RemoteHandles;
    use crate::proc::Context;
    use crate::proc::Proc;

    declare_attrs! {
        attr ENDPOINT_TEST_HEADER: u64;
    }

    #[derive(Debug)]
    struct EchoActor {
        tx: PortRef<u64>,
    }

    #[async_trait]
    impl Actor for EchoActor {}

    #[async_trait]
    impl Handler<u64> for EchoActor {
        async fn handle(&mut self, cx: &Context<Self>, message: u64) -> anyhow::Result<()> {
            Endpoint::post(&self.tx, cx, message);
            Ok(())
        }
    }

    struct TestBehavior;

    impl Named for TestBehavior {
        fn typename() -> &'static str {
            "hyperactor::endpoint::tests::TestBehavior"
        }
    }

    impl Referable for TestBehavior {}
    impl RemoteHandles<u64> for TestBehavior {}

    #[tokio::test]
    async fn test_endpoint_actor_handle() {
        let proc = Proc::isolated();
        let (client, _) = proc.client("client").unwrap();
        let (tx, mut rx) = client.open_port();
        let handle = proc
            .spawn("echo", EchoActor { tx: tx.bind() })
            .expect("spawn should succeed");

        Endpoint::post(&handle, &client, 123u64);

        assert_eq!(rx.recv().await.expect("message should arrive"), 123);
    }

    #[tokio::test]
    async fn test_endpoint_port_handle() {
        let proc = Proc::isolated();
        let (client, _) = proc.client("client").unwrap();
        let (tx, mut rx) = client.open_port();

        Endpoint::post(&tx, &client, 123u64);

        assert_eq!(rx.recv().await.expect("message should arrive"), 123);
    }

    #[tokio::test]
    async fn test_endpoint_once_port_handle() {
        let proc = Proc::isolated();
        let (client, _) = proc.client("client").unwrap();
        let (tx, rx) = client.open_once_port();

        Endpoint::post(tx, &client, 123u64);

        assert_eq!(rx.recv().await.expect("message should arrive"), 123);
    }

    #[tokio::test]
    async fn test_endpoint_actor_ref() {
        let proc = Proc::isolated();
        let (client, actor_ref, mut rx) = proc
            .attach_actor::<TestBehavior, u64>("remote_actor")
            .expect("attach actor should succeed");

        Endpoint::post(&actor_ref, &client, 123u64);

        assert_eq!(rx.recv().await.expect("message should arrive"), 123);
    }

    #[tokio::test]
    async fn test_endpoint_port_ref() {
        let proc = Proc::isolated();
        let (client, _) = proc.client("client").unwrap();
        let (tx, mut rx) = client.open_port();
        let port_ref = tx.bind();

        Endpoint::post(&port_ref, &client, 123u64);

        assert_eq!(rx.recv().await.expect("message should arrive"), 123);
    }

    #[tokio::test]
    async fn test_endpoint_once_port_ref() {
        let proc = Proc::isolated();
        let (client, _) = proc.client("client").unwrap();
        let (tx, rx) = client.open_once_port();
        let port_ref = tx.bind();

        Endpoint::post(port_ref, &client, 123u64);

        assert_eq!(rx.recv().await.expect("message should arrive"), 123);
    }

    #[tokio::test]
    async fn test_remote_endpoint_headers() {
        let proc = Proc::isolated();
        let (client, _) = proc.client("client").unwrap();
        let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
        let port = client.mailbox_for_py().open_handler_enqueue_port(
            move |headers: Flattrs, message: u64| {
                observed_tx
                    .send((
                        headers
                            .get(ENDPOINT_TEST_HEADER)
                            .expect("header should be present"),
                        message,
                    ))
                    .expect("test receiver should be alive");
                Ok(())
            },
        );
        let port_ref = port.bind();
        let mut headers = Flattrs::new();
        headers.set(ENDPOINT_TEST_HEADER, 456u64);

        RemoteEndpoint::post_with_headers(&port_ref, &client, headers, 123u64);

        assert_eq!(
            observed_rx.recv().await.expect("message should arrive"),
            (456, 123)
        );
    }
}
