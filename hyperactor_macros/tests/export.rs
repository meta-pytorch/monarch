/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fmt::Debug;

use async_trait::async_trait;
use hyperactor as reference;
use hyperactor::Actor;
use hyperactor::Context;
use hyperactor::Endpoint as _;
use hyperactor::Handler;
use hyperactor::port::Port;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

#[derive(Debug)]
#[hyperactor::export(
    TestMessage { cast = true },
    () { cast = true },
    MyGeneric<()> { cast = true },
    u64,
)]
struct TestActor {
    // Forward the received message to this port, so it can be inspected by
    // the unit test.
    forward_port: reference::PortRef<String>,
}

impl TestActor {
    fn new(forward_port: reference::PortRef<String>) -> Self {
        Self { forward_port }
    }
}

impl Actor for TestActor {}

#[derive(Debug)]
#[hyperactor::export(GenericMessage<T>)]
struct GenericActor<T> {
    forward_port: reference::PortRef<String>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> GenericActor<T> {
    fn new(forward_port: reference::PortRef<String>) -> Self {
        Self {
            forward_port,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T> Actor for GenericActor<T>
where
    T: Debug + Send + Sync + Serialize + Named + 'static,
    for<'de> T: Deserialize<'de>,
{
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Named)]
struct GenericMessage<T>(T);

#[async_trait]
impl<T> Handler<GenericMessage<T>> for GenericActor<T>
where
    T: Debug + Send + Sync + Serialize + Named + 'static,
    for<'de> T: Deserialize<'de>,
{
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        GenericMessage(message): GenericMessage<T>,
    ) -> anyhow::Result<()> {
        self.forward_port.post(cx, format!("{message:?}"));
        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Named)]
struct TestMessage(String);

#[async_trait]
impl Handler<TestMessage> for TestActor {
    async fn handle(&mut self, cx: &Context<Self>, msg: TestMessage) -> anyhow::Result<()> {
        self.forward_port.post(cx, msg.0);
        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Named)]
struct MyGeneric<T>(T);

#[async_trait]
impl Handler<()> for TestActor {
    async fn handle(&mut self, cx: &Context<Self>, _msg: ()) -> anyhow::Result<()> {
        self.forward_port.post(cx, "()".to_string());
        Ok(())
    }
}

#[async_trait]
impl Handler<MyGeneric<()>> for TestActor {
    async fn handle(&mut self, cx: &Context<Self>, _msg: MyGeneric<()>) -> anyhow::Result<()> {
        self.forward_port.post(cx, "MyGeneric<()>".to_string());
        Ok(())
    }
}

#[async_trait]
impl Handler<u64> for TestActor {
    async fn handle(&mut self, cx: &Context<Self>, msg: u64) -> anyhow::Result<()> {
        self.forward_port.post(cx, format!("u64: {msg}"));
        Ok(())
    }
}

hyperactor::behavior!(
    TestActorAlias,
    TestMessage { cast = true },
    () { cast = true },
    MyGeneric<()> { cast = true },
    u64,
);

#[cfg(test)]
mod tests {
    use hyperactor::Endpoint as _;
    use hyperactor::proc::Proc;
    use timed_test::async_timed_test;

    use super::*;

    // Ports::new is a private function, so we cannot test it directly. As a
    // workaround, we test whether we can send a message through the message's
    // named port. If the macro is not implemented correctly, the named port
    // will not be bound, and the send will fail.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_binds() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let (tx, mut rx) = client.open_port();
        let actor_handle = proc.spawn(TestActor::new(tx.bind()));
        //  This will call binds
        actor_handle.bind::<TestActor>();
        // Verify that the ports can be gotten successfully.
        {
            // TestMessage type
            let port_id = actor_handle
                .actor_addr()
                .port_addr(Port::handler::<TestMessage>());
            let port_ref: reference::PortRef<TestMessage> = reference::PortRef::attest(port_id);
            port_ref.post(&client, TestMessage("abc".to_string()));
            assert_eq!(rx.recv().await.unwrap(), "abc");
        }
        {
            // () type
            let port_id = actor_handle.actor_addr().port_addr(Port::handler::<()>());
            let port_ref: reference::PortRef<()> = reference::PortRef::attest(port_id);
            port_ref.post(&client, ());
            assert_eq!(rx.recv().await.unwrap(), "()");
        }
        {
            // u64 type
            let port_id = actor_handle.actor_addr().port_addr(Port::handler::<u64>());
            let port_ref: reference::PortRef<u64> = reference::PortRef::attest(port_id);
            port_ref.post(&client, 987654321);
            assert_eq!(rx.recv().await.unwrap(), "u64: 987654321");
        }
        {
            // MyGeneric<()> type
            let port_id = actor_handle
                .actor_addr()
                .port_addr(Port::handler::<MyGeneric<()>>());
            let port_ref: reference::PortRef<MyGeneric<()>> = reference::PortRef::attest(port_id);
            port_ref.post(&client, MyGeneric(()));
            assert_eq!(rx.recv().await.unwrap(), "MyGeneric<()>");
        }
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_ref_alias() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let (tx, mut rx) = client.open_port();
        let actor_handle = proc.spawn(TestActor::new(tx.bind()));

        actor_handle.post(&client, 123u64);
        actor_handle.post(&client, TestMessage("foo".to_string()));

        let myref: reference::ActorRef<TestActorAlias> = actor_handle.bind();
        myref.port().post(&client, MyGeneric(()));
        myref.port().post(&client, TestMessage("biz".to_string()));
        myref.port().post(&client, 999u64);
        myref.port().post(&client, ());

        assert_eq!(rx.recv().await.unwrap(), "u64: 123");
        assert_eq!(rx.recv().await.unwrap(), "foo");
        assert_eq!(rx.recv().await.unwrap(), "MyGeneric<()>");
        assert_eq!(rx.recv().await.unwrap(), "biz");
        assert_eq!(rx.recv().await.unwrap(), "u64: 999");
        assert_eq!(rx.recv().await.unwrap(), "()");
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_generic_export() {
        let proc = Proc::isolated();
        let client = proc.client("client");
        let (tx, mut rx) = client.open_port();
        let actor_handle = proc.spawn(GenericActor::<u64>::new(tx.bind()));

        actor_handle.bind::<GenericActor<u64>>();

        let port_id = actor_handle
            .actor_addr()
            .port_addr(Port::handler::<GenericMessage<u64>>());
        let port_ref: reference::PortRef<GenericMessage<u64>> = reference::PortRef::attest(port_id);
        port_ref.post(&client, GenericMessage(42));
        assert_eq!(rx.recv().await.unwrap(), "42");

        assert_ne!(
            <GenericActor<u64> as Named>::typename(),
            <GenericActor<String> as Named>::typename()
        );
    }
}
