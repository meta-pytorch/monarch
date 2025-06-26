/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::Bind;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::Named;
use hyperactor::PortRef;
use hyperactor::Unbind;
use hyperactor::data::intern_typename;
use serde::Deserialize;

use crate::Serialize;

#[derive(Debug)]
#[hyperactor::export(
    handlers = [
        TestMessage { cast = true },
        () { cast = true },
        MyGeneric<()> { cast = true },
        u64,
    ],
)]
struct TestActor {
    // Forward the received message to this port, so it can be inspected by
    // the unit test.
    forward_port: PortRef<String>,
}

#[derive(Debug, Clone, Named, Serialize, Deserialize)]
struct TestActorParams {
    forward_port: PortRef<String>,
}

#[async_trait]
impl Actor for TestActor {
    type Params = TestActorParams;

    async fn new(params: Self::Params) -> anyhow::Result<Self> {
        let Self::Params { forward_port } = params;
        Ok(Self { forward_port })
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Named, Bind, Unbind)]
struct TestMessage(String);

#[async_trait]
impl Handler<TestMessage> for TestActor {
    async fn handle(&mut self, this: &Instance<Self>, msg: TestMessage) -> anyhow::Result<()> {
        self.forward_port.send(this, msg.0)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Bind, Unbind)]
struct MyGeneric<T>(T);

impl<T: Named> Named for MyGeneric<T> {
    fn typename() -> &'static str {
        intern_typename!(Self, "hyperactor_macros::tests::export::MyGeneric<{}>", T)
    }
}

#[async_trait]
impl Handler<()> for TestActor {
    async fn handle(&mut self, this: &Instance<Self>, _msg: ()) -> anyhow::Result<()> {
        self.forward_port.send(this, "()".to_string())?;
        Ok(())
    }
}

#[async_trait]
impl Handler<MyGeneric<()>> for TestActor {
    async fn handle(&mut self, this: &Instance<Self>, _msg: MyGeneric<()>) -> anyhow::Result<()> {
        self.forward_port.send(this, "MyGeneric<()>".to_string())?;
        Ok(())
    }
}

#[async_trait]
impl Handler<u64> for TestActor {
    async fn handle(&mut self, this: &Instance<Self>, msg: u64) -> anyhow::Result<()> {
        self.forward_port.send(this, format!("u64: {msg}"))?;
        Ok(())
    }
}

hyperactor::alias!(
    TestActorAlias,
    TestMessage { cast = true },
    () { cast = true },
    MyGeneric<()> { cast = true },
    u64,
);

#[cfg(test)]
mod tests {
    use hyperactor::ActorRef;
    use hyperactor::PortRef;
    use hyperactor::message::ErasedUnbound;
    use hyperactor::message::IndexedErasedUnbound;
    use hyperactor::proc::Proc;
    use timed_test::async_timed_test;

    use super::*;

    // Ports::new is a private function, so we cannot test it directly. As a
    // workaround, we test whether we can send a message through the message's
    // named port. If the macro is not implemented correctly, the named port
    // will not be bound, and the send will fail.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_binds() {
        let proc = Proc::local();
        let client = proc.attach("client").unwrap();
        let (tx, mut rx) = client.open_port();
        let params = TestActorParams {
            forward_port: tx.bind(),
        };
        let actor_handle = proc.spawn::<TestActor>("foo", params).await.unwrap();
        //  This will call binds
        actor_handle.bind::<TestActor>();
        // Verify that the ports can be gotten successfully.
        {
            // TestMessage type
            let port_id = actor_handle.actor_id().port_id(TestMessage::port());
            let port_ref: PortRef<TestMessage> = PortRef::attest(port_id);
            port_ref
                .send(&client, TestMessage("abc".to_string()))
                .unwrap();
            assert_eq!(rx.recv().await.unwrap(), "abc");
        }
        {
            // () type
            let port_id = actor_handle.actor_id().port_id(<()>::port());
            let port_ref: PortRef<()> = PortRef::attest(port_id);
            port_ref.send(&client, ()).unwrap();
            assert_eq!(rx.recv().await.unwrap(), "()");
        }
        {
            // u64 type
            let port_id = actor_handle.actor_id().port_id(<u64>::port());
            let port_ref: PortRef<u64> = PortRef::attest(port_id);
            port_ref.send(&client, 987654321).unwrap();
            assert_eq!(rx.recv().await.unwrap(), "u64: 987654321");
        }
        {
            // MyGeneric<()> type
            let port_id = actor_handle.actor_id().port_id(MyGeneric::<()>::port());
            let port_ref: PortRef<MyGeneric<()>> = PortRef::attest(port_id);
            port_ref.send(&client, MyGeneric(())).unwrap();
            assert_eq!(rx.recv().await.unwrap(), "MyGeneric<()>");
        }
        {
            // IndexedErasedUnbound<TestMessage> type, which is added due to
            // the `castable` flag.
            let erased_msg =
                ErasedUnbound::try_from_message(TestMessage("efg".to_string())).unwrap();
            let indexed_msg = IndexedErasedUnbound::<TestMessage>::from(erased_msg);
            let port_id = actor_handle
                .actor_id()
                .port_id(<IndexedErasedUnbound<TestMessage>>::port());
            let port_ref: PortRef<IndexedErasedUnbound<TestMessage>> = PortRef::attest(port_id);
            port_ref.send(&client, indexed_msg).unwrap();
            assert_eq!(rx.recv().await.unwrap(), "efg");
        }
        {
            // IndexedErasedUnbound<()> type, which is added due to the `castable`
            // flag.
            let erased_msg = ErasedUnbound::try_from_message(()).unwrap();
            let indexed_msg = IndexedErasedUnbound::<()>::from(erased_msg);
            let port_id = actor_handle
                .actor_id()
                .port_id(<IndexedErasedUnbound<()>>::port());
            let port_ref: PortRef<IndexedErasedUnbound<()>> = PortRef::attest(port_id);
            port_ref.send(&client, indexed_msg).unwrap();
            assert_eq!(rx.recv().await.unwrap(), "()");
        }
        {
            // IndexedErasedUnbound<MyGeneric<()>> type, which is added due to the
            // `castable` flag.
            let erased_msg = ErasedUnbound::try_from_message(()).unwrap();
            let indexed_msg = IndexedErasedUnbound::<MyGeneric<()>>::from(erased_msg);
            let port_id = actor_handle
                .actor_id()
                .port_id(<IndexedErasedUnbound<MyGeneric<()>>>::port());
            let port_ref: PortRef<IndexedErasedUnbound<MyGeneric<()>>> = PortRef::attest(port_id);
            port_ref.send(&client, indexed_msg).unwrap();
            assert_eq!(rx.recv().await.unwrap(), "MyGeneric<()>");
        }
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_ref_alias() {
        let proc = Proc::local();
        let client = proc.attach("client").unwrap();
        let (tx, mut rx) = client.open_port();
        let params = TestActorParams {
            forward_port: tx.bind(),
        };
        let actor_handle = proc.spawn::<TestActor>("actor", params).await.unwrap();

        actor_handle.send(123u64).unwrap();
        actor_handle.send(TestMessage("foo".to_string())).unwrap();

        let myref: ActorRef<TestActorAlias> = actor_handle.bind();
        myref.port().send(&client, MyGeneric(())).unwrap();
        myref
            .port()
            .send(&client, TestMessage("biz".to_string()))
            .unwrap();
        myref.port().send(&client, 999u64).unwrap();
        myref.port().send(&client, ()).unwrap();
        {
            let erased_msg =
                ErasedUnbound::try_from_message(TestMessage("bar".to_string())).unwrap();
            let indexed_msg = IndexedErasedUnbound::<TestMessage>::from(erased_msg);
            myref.port().send(&client, indexed_msg).unwrap();
        }
        {
            let erased_msg = ErasedUnbound::try_from_message(()).unwrap();
            let indexed_msg = IndexedErasedUnbound::<MyGeneric<()>>::from(erased_msg);
            myref.port().send(&client, indexed_msg).unwrap();
        }

        assert_eq!(rx.recv().await.unwrap(), "u64: 123");
        assert_eq!(rx.recv().await.unwrap(), "foo");
        assert_eq!(rx.recv().await.unwrap(), "MyGeneric<()>");
        assert_eq!(rx.recv().await.unwrap(), "biz");
        assert_eq!(rx.recv().await.unwrap(), "u64: 999");
        assert_eq!(rx.recv().await.unwrap(), "()");
        assert_eq!(rx.recv().await.unwrap(), "bar");
        assert_eq!(rx.recv().await.unwrap(), "MyGeneric<()>");
    }
}
