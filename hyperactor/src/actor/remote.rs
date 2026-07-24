/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Management of actor registration for remote spawning.

use std::any::TypeId;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::LazyLock;

use hyperactor_config::Flattrs;

use crate::Actor;
use crate::ActorEnvironment;
use crate::AnyActorHandle;
use crate::Data;
use crate::id::Uid;
use crate::proc::InstanceCell;
use crate::proc::Proc;

/// The offset of user-defined ports (i.e., arbitrarily bound).
pub const USER_PORT_OFFSET: u64 = 1024;

/// Register an actor type so that it can be spawned remotely. The actor
/// type must implement [`typeuri::Named`], which will be used to identify
/// the actor globally.
///
/// Example:
///
/// ```ignore
/// struct MyActor { ... }
///
/// register_spawnable!(MyActor);
/// ```
#[macro_export]
macro_rules! register_spawnable {
    ($actor:ty) => {
        const _: () = {
            static NAME: std::sync::LazyLock<&'static str> = std::sync::LazyLock::new(|| {
                <$actor as $crate::internal_macro_support::typeuri::Named>::typename()
            });

            $crate::internal_macro_support::inventory::submit! {
                $crate::actor::remote::SpawnableActor {
                    name: &NAME,
                    gspawn_root_bind: <$actor as $crate::actor::RemoteSpawn>::gspawn_root_bind,
                    gspawn_child: <$actor as $crate::actor::RemoteSpawn>::gspawn_child,
                    get_type_id: <$actor as $crate::actor::RemoteSpawn>::get_type_id,
                }
            }
        };
    };
}

/// A type-erased actor registration entry. These are constructed via
/// [`crate::register_spawnable`].
#[derive(Debug)]
pub struct SpawnableActor {
    /// A URI that globally identifies an actor. It is an error to register
    /// multiple actors with the same name.
    ///
    /// This is a LazyLock because the names are provided through a trait
    /// implementation, which can not yet be `const`.
    pub name: &'static LazyLock<&'static str>,

    /// Type-erased root spawn function. This is the type's
    /// [`RemoteSpawn::gspawn_root_bind`].
    pub gspawn_root_bind: fn(
        &Proc,
        Uid,
        Data,
        Flattrs,
    ) -> Pin<
        Box<dyn Future<Output = Result<crate::ActorAddr, anyhow::Error>> + Send>,
    >,

    /// Type-erased child spawn function. This is the type's
    /// [`RemoteSpawn::gspawn_child`]. The `ActorEnvironment` is the persistent
    /// environment stored on the new instance; the `Flattrs` are the transient
    /// constructor headers overlaid only for `RemoteSpawn::new`.
    pub gspawn_child:
        fn(
            &Proc,
            InstanceCell,
            Uid,
            Data,
            ActorEnvironment,
            Flattrs,
        ) -> Pin<Box<dyn Future<Output = Result<AnyActorHandle, anyhow::Error>> + Send>>,

    /// A function to retrieve the type id of the actor itself. This is
    /// used to translate a concrete type to a global name.
    pub get_type_id: fn() -> TypeId,
}

inventory::collect!(SpawnableActor);

/// Registry of actors linked into this image and registered by way of
/// [`crate::register_spawnable`].
#[derive(Debug)]
pub struct Remote {
    by_name: HashMap<&'static str, &'static SpawnableActor>,
    by_type_id: HashMap<TypeId, &'static SpawnableActor>,
}

impl Remote {
    /// Construct a registry. Panics if there are conflicting registrations.
    pub fn collect() -> Self {
        let mut by_name = HashMap::new();
        let mut by_type_id = HashMap::new();
        for entry in inventory::iter::<SpawnableActor> {
            if by_name.insert(**entry.name, entry).is_some() {
                panic!("actor name {} registered multiple times", **entry.name);
            }
            let type_id = (entry.get_type_id)();
            if by_type_id.insert(type_id, entry).is_some() {
                panic!(
                    "type id {:?} ({}) registered multiple times",
                    type_id, **entry.name
                );
            }
        }
        Self {
            by_name,
            by_type_id,
        }
    }

    /// Return the process-wide remote spawn registry.
    pub fn global() -> &'static Self {
        static REMOTE: LazyLock<Remote> = LazyLock::new(Remote::collect);
        &REMOTE
    }

    /// Returns the name of the provided actor, if registered.
    pub fn name_of<A: Actor>(&self) -> Option<&'static str> {
        self.by_type_id
            .get(&TypeId::of::<A>())
            .map(|entry| **entry.name)
    }

    /// Spawns the actor with the provided sender, actor uid,
    /// and serialized parameters. Returns an error if the actor is not
    /// registered, or if the actor's spawn fails.
    pub async fn gspawn(
        &self,
        proc: &Proc,
        actor_type: &str,
        actor_uid: Uid,
        params: Data,
        environment: Flattrs,
    ) -> Result<crate::ActorAddr, anyhow::Error> {
        let entry = self
            .by_name
            .get(actor_type)
            .ok_or_else(|| anyhow::anyhow!("actor type {} not registered", actor_type))?;
        (entry.gspawn_root_bind)(proc, actor_uid, params, environment).await
    }

    /// Spawns the actor as a child of the provided parent. Returns an
    /// erased lifecycle handle.
    pub async fn gspawn_child(
        &self,
        proc: &Proc,
        parent: InstanceCell,
        actor_type: &str,
        actor_uid: Uid,
        params: Data,
        transient: Flattrs,
    ) -> Result<AnyActorHandle, anyhow::Error> {
        let environment = parent.actor_environment().clone();
        let entry = self
            .by_name
            .get(actor_type)
            .ok_or_else(|| anyhow::anyhow!("actor type {} not registered", actor_type))?;
        (entry.gspawn_child)(proc, parent, actor_uid, params, environment, transient).await
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use async_trait::async_trait;
    use hyperactor_config::Flattrs;

    use super::*;
    use crate as hyperactor; // for macros
    use crate::Context;
    use crate::Handler;
    use crate::RemoteSpawn;
    use crate::id::Label;

    #[derive(Debug)]
    #[hyperactor::export(())]
    struct MyActor;

    #[async_trait]
    impl Actor for MyActor {}

    #[async_trait]
    impl RemoteSpawn for MyActor {
        type Params = bool;

        async fn new(params: bool, _environment: Flattrs) -> Result<Self, anyhow::Error> {
            if params {
                Ok(MyActor)
            } else {
                Err(anyhow::anyhow!("some failure"))
            }
        }
    }

    #[async_trait]
    impl Handler<()> for MyActor {
        async fn handle(&mut self, _cx: &Context<Self>, _message: ()) -> anyhow::Result<()> {
            unimplemented!()
        }
    }

    register_spawnable!(MyActor);

    #[derive(Debug, Default)]
    #[hyperactor::export(())]
    struct GenericActor<T>(std::marker::PhantomData<T>);

    #[async_trait]
    impl<T: Send + 'static> Actor for GenericActor<T> {}

    #[async_trait]
    impl<T: Send + Sync + 'static> Handler<()> for GenericActor<T> {
        async fn handle(&mut self, _cx: &Context<Self>, _message: ()) -> anyhow::Result<()> {
            unimplemented!()
        }
    }

    register_spawnable!(GenericActor<u64>);
    register_spawnable!(GenericActor<bool>);

    #[tokio::test]
    async fn test_registry() {
        let remote = Remote::collect();
        assert_matches!(
            remote.name_of::<MyActor>(),
            Some("hyperactor::actor::remote::tests::MyActor")
        );
        assert_matches!(
            remote.name_of::<GenericActor<u64>>(),
            Some("hyperactor::actor::remote::tests::GenericActor<u64>")
        );
        assert_matches!(
            remote.name_of::<GenericActor<bool>>(),
            Some("hyperactor::actor::remote::tests::GenericActor<bool>")
        );
        assert_ne!(
            <GenericActor<u64> as typeuri::Named>::typename(),
            <GenericActor<bool> as typeuri::Named>::typename()
        );

        let _ = remote
            .gspawn(
                &Proc::isolated(),
                "hyperactor::actor::remote::tests::MyActor",
                Uid::instance(Label::new("actor").unwrap()),
                bincode::serde::encode_to_vec(true, bincode::config::legacy()).unwrap(),
                Flattrs::default(),
            )
            .await
            .unwrap();

        let err = remote
            .gspawn(
                &Proc::isolated(),
                "hyperactor::actor::remote::tests::MyActor",
                Uid::instance(Label::new("actor").unwrap()),
                bincode::serde::encode_to_vec(false, bincode::config::legacy()).unwrap(),
                Flattrs::default(),
            )
            .await
            .unwrap_err();

        assert_eq!(err.to_string().as_str(), "some failure");
    }

    #[tokio::test]
    async fn test_instance_gspawn_child_returns_erased_handle() {
        let proc = Proc::isolated();
        let parent = proc.client("parent");

        let child = parent
            .gspawn(
                "hyperactor::actor::remote::tests::MyActor",
                bincode::serde::encode_to_vec(true, bincode::config::legacy()).unwrap(),
            )
            .await
            .unwrap();

        assert!(!child.actor_id().is_root());
        assert!(child.downcast::<MyActor>().is_some());
        assert!(child.downcast::<GenericActor<u64>>().is_none());

        child.stop("test").unwrap();
        child.await;
    }

    #[tokio::test]
    async fn test_instance_gspawn_uid_uses_explicit_uid() {
        let proc = Proc::isolated();
        let parent = proc.client("parent");
        let uid = Uid::instance(Label::new("child").unwrap());

        let child = parent
            .gspawn_uid(
                "hyperactor::actor::remote::tests::MyActor",
                uid.clone(),
                bincode::serde::encode_to_vec(true, bincode::config::legacy()).unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(child.actor_id().uid(), &uid);

        child.stop("test").unwrap();
        child.await;
    }

    #[tokio::test]
    async fn test_instance_gspawn_uid_rejects_duplicate_uid() {
        let proc = Proc::isolated();
        let parent = proc.client("parent");
        let uid = Uid::instance(Label::new("child").unwrap());

        let child = parent
            .gspawn_uid(
                "hyperactor::actor::remote::tests::MyActor",
                uid.clone(),
                bincode::serde::encode_to_vec(true, bincode::config::legacy()).unwrap(),
            )
            .await
            .unwrap();

        let err = parent
            .gspawn_uid(
                "hyperactor::actor::remote::tests::MyActor",
                uid,
                bincode::serde::encode_to_vec(true, bincode::config::legacy()).unwrap(),
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("has already been spawned"));

        child.stop("test").unwrap();
        child.await;
    }
}
