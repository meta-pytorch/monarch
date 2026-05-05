/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Link implementation actors.
//!
//! A link is represented by a registered actor spawned as a supervised child
//! of the actor that owns the remote supervision proxy. The actor's ordinary
//! lifecycle is the link lifecycle: if the link actor fails, local supervision
//! observes that failure; if the parent stops, the link actor stops with it.

use hyperactor::Actor;
use hyperactor::AnyActorHandle;
use hyperactor::Bind;
use hyperactor::Data;
use hyperactor::Instance;
use hyperactor::Uid;
use hyperactor::Unbind;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

/// Specification for a link implementation actor.
#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    Named,
    PartialEq,
    Eq,
    Bind,
    Unbind
)]
pub struct LinkSpec {
    actor_type: String,
    uid: Uid,
    params: Data,
}
wirevalue::register_type!(LinkSpec);

impl LinkSpec {
    /// Create a link spec for the registered actor type with a fresh uid.
    pub fn for_actor<A: Actor + Named>(params: Data) -> Self {
        Self::new(A::typename(), params)
    }

    /// Create a link spec for the registered actor type with an explicit uid.
    pub fn for_actor_uid<A: Actor + Named>(uid: Uid, params: Data) -> Self {
        Self::with_uid(A::typename(), uid, params)
    }

    /// Create a link spec with a fresh uid.
    pub fn new(actor_type: impl Into<String>, params: Data) -> Self {
        Self::with_uid(actor_type, Uid::instance(), params)
    }

    /// Create a link spec with an explicit uid.
    pub fn with_uid(actor_type: impl Into<String>, uid: Uid, params: Data) -> Self {
        Self {
            actor_type: actor_type.into(),
            uid,
            params,
        }
    }

    /// The registered actor type used to spawn this link.
    pub fn actor_type(&self) -> &str {
        &self.actor_type
    }

    /// The uid that will identify this link actor.
    pub fn uid(&self) -> &Uid {
        &self.uid
    }

    /// The serialized parameters passed to the link actor.
    pub fn params(&self) -> &[u8] {
        &self.params
    }

    /// Spawn the link actor as a supervised child of `parent`.
    pub async fn spawn<A: Actor>(self, parent: &Instance<A>) -> anyhow::Result<AnyActorHandle> {
        parent
            .gspawn_uid(&self.actor_type, self.uid, self.params)
            .await
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use hyperactor::Actor;
    use hyperactor::Context;
    use hyperactor::Handler;
    use hyperactor::Label;
    use hyperactor::PortRef;
    use hyperactor::Proc;
    use hyperactor::RemoteSpawn;
    use hyperactor::Uid;
    use hyperactor::actor::ActorErrorKind;
    use hyperactor::actor::ActorStatus;
    use hyperactor::supervision::ActorSupervisionEvent;
    use hyperactor_config::Flattrs;

    use super::*;

    #[derive(Debug)]
    #[hyperactor::export(())]
    struct TestLinkActor;

    #[async_trait]
    impl Actor for TestLinkActor {}

    #[async_trait]
    impl RemoteSpawn for TestLinkActor {
        type Params = ();

        async fn new(_params: (), _environment: Flattrs) -> anyhow::Result<Self> {
            Ok(Self)
        }
    }

    #[async_trait]
    impl Handler<()> for TestLinkActor {
        async fn handle(&mut self, _cx: &Context<Self>, _message: ()) -> anyhow::Result<()> {
            Ok(())
        }
    }

    hyperactor::register_spawnable!(TestLinkActor);

    #[derive(Debug)]
    struct TestParentActor {
        link: Option<LinkSpec>,
        events: PortRef<ActorSupervisionEvent>,
    }

    #[async_trait]
    impl Actor for TestParentActor {
        async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
            let link = self.link.take().unwrap();
            let handle = link.spawn(this).await?;
            handle.kill("link failed")?;
            Ok(())
        }

        async fn handle_supervision_event(
            &mut self,
            this: &Instance<Self>,
            event: &ActorSupervisionEvent,
        ) -> anyhow::Result<bool> {
            self.events.send(this, event.clone())?;
            Ok(true)
        }
    }

    #[tokio::test]
    async fn test_link_spec_spawns_supervised_child() {
        let proc = Proc::local();
        let (parent, _parent_handle) = proc.instance("parent").unwrap();
        let uid = Uid::instance_labeled(Label::new("link").unwrap());

        let link = LinkSpec::for_actor_uid::<TestLinkActor>(
            uid.clone(),
            bincode::serde::encode_to_vec((), bincode::config::legacy()).unwrap(),
        );

        let handle = link.spawn(&parent).await.unwrap();

        assert_eq!(handle.actor_id().uid(), &uid);
        assert!(handle.downcast::<TestLinkActor>().is_some());

        handle.stop("test").unwrap();
        handle.await;
    }

    #[tokio::test]
    async fn test_link_actor_failure_propagates_to_parent() {
        let proc = Proc::local();
        let (client, _client_handle) = proc.instance("client").unwrap();
        let (events, mut event_rx) = client.open_port::<ActorSupervisionEvent>();
        let uid = Uid::instance_labeled(Label::new("link").unwrap());
        let link = LinkSpec::for_actor_uid::<TestLinkActor>(
            uid.clone(),
            bincode::serde::encode_to_vec((), bincode::config::legacy()).unwrap(),
        );
        let parent = proc
            .spawn(
                "parent",
                TestParentActor {
                    link: Some(link),
                    events: events.bind(),
                },
            )
            .unwrap();

        let event = event_rx.recv().await.unwrap();

        assert_eq!(event.actor_id.uid(), &uid);
        assert!(matches!(
            event.actor_status,
            ActorStatus::Failed(ActorErrorKind::Generic(ref reason))
                if reason == "actor explicitly aborted due to: link failed"
        ));
        assert!(!event.actor_id.is_root());

        parent.stop("test").unwrap();
        parent.await;
    }
}
