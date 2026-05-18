/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Generic send endpoints.

use hyperactor_config::Flattrs;

use crate::Actor;
use crate::ActorHandle;
use crate::ActorRef;
use crate::Handler;
use crate::Message;
use crate::OncePortHandle;
use crate::OncePortRef;
use crate::PortHandle;
use crate::PortRef;
use crate::RemoteHandles;
use crate::RemoteMessage;
use crate::actor::Referable;
use crate::context;
use crate::mailbox::MailboxSenderError;

/// A typed endpoint that can receive `M`.
///
/// This trait abstracts over local actor handles, local port handles, remote
/// actor refs, remote port refs, and one-shot ports. It is sealed so that
/// Hyperactor owns the send semantics for each endpoint kind.
pub trait Endpoint<M>: crate::private::Sealed {
    /// Send `message` to this endpoint from `cx`.
    fn send<C>(self, cx: &C, message: M) -> Result<(), MailboxSenderError>
    where
        C: context::Actor;
}

/// A typed endpoint that can receive `M` with message headers.
///
/// `RemoteEndpoint` is implemented only for endpoints whose send path preserves
/// headers.
pub trait RemoteEndpoint<M>: Endpoint<M> {
    /// Send `message` and `headers` to this endpoint from `cx`.
    fn send_with_headers<C>(
        self,
        cx: &C,
        headers: Flattrs,
        message: M,
    ) -> Result<(), MailboxSenderError>
    where
        C: context::Actor;
}

impl<A, M> Endpoint<M> for &ActorHandle<A>
where
    A: Actor + Handler<M>,
    M: Message,
{
    fn send<C>(self, cx: &C, message: M) -> Result<(), MailboxSenderError>
    where
        C: context::Actor,
    {
        ActorHandle::send(self, cx, message)
    }
}

impl<M> Endpoint<M> for &PortHandle<M>
where
    M: Message,
{
    fn send<C>(self, cx: &C, message: M) -> Result<(), MailboxSenderError>
    where
        C: context::Actor,
    {
        PortHandle::send(self, cx, message)
    }
}

impl<M> Endpoint<M> for OncePortHandle<M>
where
    M: Message,
{
    fn send<C>(self, cx: &C, message: M) -> Result<(), MailboxSenderError>
    where
        C: context::Actor,
    {
        OncePortHandle::send(self, cx, message)
    }
}

impl<A, M> Endpoint<M> for &ActorRef<A>
where
    A: Referable + RemoteHandles<M>,
    M: RemoteMessage,
{
    fn send<C>(self, cx: &C, message: M) -> Result<(), MailboxSenderError>
    where
        C: context::Actor,
    {
        ActorRef::send(self, cx, message)
    }
}

impl<A, M> RemoteEndpoint<M> for &ActorRef<A>
where
    A: Referable + RemoteHandles<M>,
    M: RemoteMessage,
{
    fn send_with_headers<C>(
        self,
        cx: &C,
        headers: Flattrs,
        message: M,
    ) -> Result<(), MailboxSenderError>
    where
        C: context::Actor,
    {
        ActorRef::send_with_headers(self, cx, headers, message)
    }
}

impl<M> Endpoint<M> for &PortRef<M>
where
    M: RemoteMessage,
{
    fn send<C>(self, cx: &C, message: M) -> Result<(), MailboxSenderError>
    where
        C: context::Actor,
    {
        PortRef::send(self, cx, message)
    }
}

impl<M> RemoteEndpoint<M> for &PortRef<M>
where
    M: RemoteMessage,
{
    fn send_with_headers<C>(
        self,
        cx: &C,
        headers: Flattrs,
        message: M,
    ) -> Result<(), MailboxSenderError>
    where
        C: context::Actor,
    {
        PortRef::send_with_headers(self, cx, headers, message)
    }
}

impl<M> Endpoint<M> for OncePortRef<M>
where
    M: RemoteMessage,
{
    fn send<C>(self, cx: &C, message: M) -> Result<(), MailboxSenderError>
    where
        C: context::Actor,
    {
        OncePortRef::send(self, cx, message)
    }
}

impl<M> RemoteEndpoint<M> for OncePortRef<M>
where
    M: RemoteMessage,
{
    fn send_with_headers<C>(
        self,
        cx: &C,
        headers: Flattrs,
        message: M,
    ) -> Result<(), MailboxSenderError>
    where
        C: context::Actor,
    {
        OncePortRef::send_with_headers(self, cx, headers, message)
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use hyperactor_config::Flattrs;
    use hyperactor_config::declare_attrs;
    use tokio::sync::mpsc;
    use typeuri::Named;

    use super::*;
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
            Endpoint::send(&self.tx, cx, message)?;
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

        Endpoint::send(&handle, &client, 123u64).expect("send to actor handle should succeed");

        assert_eq!(rx.recv().await.expect("message should arrive"), 123);
    }

    #[tokio::test]
    async fn test_endpoint_port_handle() {
        let proc = Proc::isolated();
        let (client, _) = proc.client("client").unwrap();
        let (tx, mut rx) = client.open_port();

        Endpoint::send(&tx, &client, 123u64).expect("send to port handle should succeed");

        assert_eq!(rx.recv().await.expect("message should arrive"), 123);
    }

    #[tokio::test]
    async fn test_endpoint_once_port_handle() {
        let proc = Proc::isolated();
        let (client, _) = proc.client("client").unwrap();
        let (tx, rx) = client.open_once_port();

        Endpoint::send(tx, &client, 123u64).expect("send to once port handle should succeed");

        assert_eq!(rx.recv().await.expect("message should arrive"), 123);
    }

    #[tokio::test]
    async fn test_endpoint_actor_ref() {
        let proc = Proc::isolated();
        let (client, actor_ref, mut rx) = proc
            .attach_actor::<TestBehavior, u64>("remote_actor")
            .expect("attach actor should succeed");

        Endpoint::send(&actor_ref, &client, 123u64).expect("send to actor ref should succeed");

        assert_eq!(rx.recv().await.expect("message should arrive"), 123);
    }

    #[tokio::test]
    async fn test_endpoint_port_ref() {
        let proc = Proc::isolated();
        let (client, _) = proc.client("client").unwrap();
        let (tx, mut rx) = client.open_port();
        let port_ref = tx.bind();

        Endpoint::send(&port_ref, &client, 123u64).expect("send to port ref should succeed");

        assert_eq!(rx.recv().await.expect("message should arrive"), 123);
    }

    #[tokio::test]
    async fn test_endpoint_once_port_ref() {
        let proc = Proc::isolated();
        let (client, _) = proc.client("client").unwrap();
        let (tx, rx) = client.open_once_port();
        let port_ref = tx.bind();

        Endpoint::send(port_ref, &client, 123u64).expect("send to once port ref should succeed");

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

        RemoteEndpoint::send_with_headers(&port_ref, &client, headers, 123u64)
            .expect("send with headers should succeed");

        assert_eq!(
            observed_rx.recv().await.expect("message should arrive"),
            (456, 123)
        );
    }
}
