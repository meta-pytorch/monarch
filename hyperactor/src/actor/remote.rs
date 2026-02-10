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

use hyperactor_config::Attrs;

use crate::Actor;
use crate::Data;
use crate::proc::Proc;
use crate::reference::ActorId;

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
/// remote!(MyActor);
/// ```
#[macro_export]
macro_rules! remote {
    ($actor:ty) => {
        $crate::internal_macro_support::paste! {
            static [<$actor:snake:upper _NAME>]: std::sync::LazyLock<&'static str> =
              std::sync::LazyLock::new(|| <$actor as $crate::internal_macro_support::typeuri::Named>::typename());
            $crate::internal_macro_support::inventory::submit! {
                $crate::actor::remote::SpawnableActor {
                    name: &[<$actor:snake:upper _NAME>],
                    gspawn: <$actor as $crate::actor::RemoteSpawn>::gspawn,
                    get_type_id: <$actor as $crate::actor::RemoteSpawn>::get_type_id,
                }
            }
        }
    };
}

/// A type-erased actor registration entry. These are constructed via
/// [`crate::remote`].
#[derive(Debug)]
pub struct SpawnableActor {
    /// A URI that globally identifies an actor. It is an error to register
    /// multiple actors with the same name.
    ///
    /// This is a LazyLock because the names are provided through a trait
    /// implementation, which can not yet be `const`.
    pub name: &'static LazyLock<&'static str>,

    /// Type-erased spawn function. This is the type's [`RemoteSpawn::gspawn`].
    pub gspawn: fn(
        &Proc,
        &str,
        Data,
        Attrs,
    ) -> Pin<Box<dyn Future<Output = Result<ActorId, anyhow::Error>> + Send>>,

    /// A function to retrieve the type id of the actor itself. This is
    /// used to translate a concrete type to a global name.
    pub get_type_id: fn() -> TypeId,
}

inventory::collect!(SpawnableActor);

/// Registry of actors linked into this image and registered by way of
/// [`crate::remote`].
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

    /// Returns the name of the provided actor, if registered.
    pub fn name_of<A: Actor>(&self) -> Option<&'static str> {
        self.by_type_id
            .get(&TypeId::of::<A>())
            .map(|entry| **entry.name)
    }

    /// Spawns the named actor with the provided sender, actor id,
    /// and serialized parameters. Returns an error if the actor is not
    /// registered, or if the actor's spawn fails.
    pub async fn gspawn(
        &self,
        proc: &Proc,
        actor_type: &str,
        actor_name: &str,
        params: Data,
        environment: Attrs,
    ) -> Result<ActorId, anyhow::Error> {
        let entry = self
            .by_name
            .get(actor_type)
            .ok_or_else(|| anyhow::anyhow!("actor type {} not registered", actor_type))?;
        (entry.gspawn)(proc, actor_name, params, environment).await
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use async_trait::async_trait;
    use hyperactor_config::Attrs;

    use super::*;
    use crate as hyperactor; // for macros
    use crate::Context;
    use crate::Handler;
    use crate::RemoteSpawn;

    #[derive(Debug)]
    #[hyperactor::export(handlers = [()])]
    struct MyActor;

    #[async_trait]
    impl Actor for MyActor {}

    #[async_trait]
    impl RemoteSpawn for MyActor {
        type Params = bool;

        async fn new(params: bool, _environment: Attrs) -> Result<Self, anyhow::Error> {
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

    remote!(MyActor);

    #[tokio::test]
    async fn test_registry() {
        let remote = Remote::collect();
        assert_matches!(
            remote.name_of::<MyActor>(),
            Some("hyperactor::actor::remote::tests::MyActor")
        );

        let _ = remote
            .gspawn(
                &Proc::local(),
                "hyperactor::actor::remote::tests::MyActor",
                "actor",
                bincode::serialize(&true).unwrap(),
                Attrs::default(),
            )
            .await
            .unwrap();

        let err = remote
            .gspawn(
                &Proc::local(),
                "hyperactor::actor::remote::tests::MyActor",
                "actor",
                bincode::serialize(&false).unwrap(),
                Attrs::default(),
            )
            .await
            .unwrap_err();

        assert_eq!(err.to_string().as_str(), "some failure");
    }
}
