/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This module provides a framework for mutating serialized messages without
//! the need to deserialize them. This capability is useful when sending messages
//! to a remote destination throughout intermeidate nodes, where the intermediate
//! nodes do not contain the message's type information.
//!
//! Briefly, it works by following these steps:
//!
//! 1. On the sender side, mutable information is extracted from the typed
//!    message through [Unbind], and stored in a [Bindings] object. This object
//!    is bundled with the serialized message in an [ErasedUnbound] object, which
//!    is sent over the wire.
//! 2. On intermediate nodes, the [ErasedUnbound] object is relayed. The
//!    muation is applied on its bindings field, if needed.
//! 3. One the receiver side, the [ErasedUnbound] object is received as
//!    [IndexedErasedUnbound], where the type information is restored. Mutated
//!    information contained in its bindings field is applied to the message
//!    through [Bind], which results in the final typed message.
//!
//! One main use case of this framework is to mutate the reply ports of a
//! muticast message, so the replies can be relayed through intermediate nodes,
//! rather than directly sent to the original sender. Therefore, a [Castable]
//! trait is defined, which collects requirements for message types using
//! multicast.

use std::collections::VecDeque;
use std::marker::PhantomData;

use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use typeuri::Named;

// for macros
use crate::ActorRef;
use crate::Mailbox;
use crate::RemoteHandles;
use crate::RemoteMessage;
use crate::actor::Referable;
use crate::context;
use crate::data::Serialized;

/// An object `T` that is [`Unbind`] can extract a set of parameters from itself,
/// and store in [`Bindings`]. The extracted parameters in [`Bindings`] can be
/// independently manipulated, and then later reconstituted (rebound) into
/// a `T`-typed object again.
pub trait Unbind: Sized {
    /// Extract parameters from itself and store them in bindings.
    fn unbind(&self, bindings: &mut Bindings) -> anyhow::Result<()>;
}

/// An object `T` that is [`Bind`] can bind a set of externally provided
/// parameters into itself.
pub trait Bind: Sized {
    /// Remove parameters from bindings, and use them to update itself.
    fn bind(&mut self, bindings: &mut Bindings) -> anyhow::Result<()>;
}

/// This trait collects the necessary requirements for messages that are can be
/// cast.
pub trait Castable: RemoteMessage + Bind + Unbind {}
impl<T: RemoteMessage + Bind + Unbind> Castable for T {}

/// Information extracted from a message through [Unbind], which can be merged
/// back to the message through [Bind].
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct Bindings(VecDeque<(u64, Serialized)>);

impl Bindings {
    /// Push a value into this bindings.
    pub fn push_back<T: Serialize + Named>(&mut self, value: &T) -> anyhow::Result<()> {
        let ser = Serialized::serialize(value)?;
        self.0.push_back((T::typehash(), ser));
        Ok(())
    }

    /// Removes the first pushed element in this bindings, deserialize it into
    /// type T, and return it. Return [`None`] if this bindings is empty.
    /// If the type of the first pushed element does not match T, an error is
    /// returned.
    pub fn pop_front<T: DeserializeOwned + Named>(&mut self) -> anyhow::Result<Option<T>> {
        match self.0.pop_front() {
            None => Ok(None),
            Some((t, v)) => {
                if t != T::typehash() {
                    anyhow::bail!(
                        "type mismatch: expected {} with hash {}, found {} in binding",
                        T::typename(),
                        T::typehash(),
                        t,
                    );
                }
                Ok(Some(v.deserialized::<T>()?))
            }
        }
    }

    /// Fallible version of [Bindings::pop_front].
    pub fn try_pop_front<T: DeserializeOwned + Named>(&mut self) -> anyhow::Result<T> {
        self.pop_front::<T>()?.ok_or_else(|| {
            anyhow::anyhow!("expect a {} binding, but none was found", T::typename())
        })
    }

    fn visit_mut<T: Serialize + DeserializeOwned + Named>(
        &mut self,
        mut f: impl FnMut(&mut T) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        for v in self.0.iter_mut() {
            if v.0 == T::typehash() {
                let mut t = v.1.deserialized::<T>()?;
                f(&mut t)?;
                v.1 = Serialized::serialize(&t)?;
            }
        }
        Ok(())
    }
}

/// An object contains a message, and its bindings extracted through [Unbind].
#[derive(Debug, PartialEq)]
pub struct Unbound<M> {
    message: M,
    bindings: Bindings,
}

impl<M> Unbound<M> {
    /// Build a new object.
    pub fn new(message: M, bindings: Bindings) -> Self {
        Self { message, bindings }
    }

    /// Use the provided function to update values inside bindings in the same
    /// order as they were pushed into bindings.
    pub fn visit_mut<T: Serialize + DeserializeOwned + Named>(
        &mut self,
        f: impl FnMut(&mut T) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        self.bindings.visit_mut(f)
    }
}

impl<M: Bind> Unbound<M> {
    /// Bind its bindings to its message through [Bind], and return the result.
    pub fn bind(mut self) -> anyhow::Result<M> {
        self.message.bind(&mut self.bindings)?;
        anyhow::ensure!(
            self.bindings.0.is_empty(),
            "there are still {} elements left in bindings",
            self.bindings.0.len()
        );
        Ok(self.message)
    }
}

impl<M: Unbind> Unbound<M> {
    /// Create an object from a typed message.
    // Note: cannot implement TryFrom<T> due to conflict with core crate's blanket impl.
    // More can be found in this issue: https://github.com/rust-lang/rust/issues/50133
    pub fn try_from_message(message: M) -> anyhow::Result<Self> {
        let mut bindings = Bindings::default();
        message.unbind(&mut bindings)?;
        Ok(Unbound { message, bindings })
    }
}

/// Unbound, with its message type M erased through serialization.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, typeuri::Named)]
pub struct ErasedUnbound {
    message: Serialized,
    bindings: Bindings,
}
crate::register_type!(ErasedUnbound);

impl ErasedUnbound {
    /// Create an object directly from Serialized without binding.
    pub fn new(message: Serialized) -> Self {
        Self {
            message,
            bindings: Bindings::default(),
        }
    }

    /// Create an object from a typed message.
    // Note: cannot implement TryFrom<T> due to conflict with core crate's blanket impl.
    // More can be found in this issue: https://github.com/rust-lang/rust/issues/50133
    pub fn try_from_message<T: Unbind + Serialize + Named>(msg: T) -> Result<Self, anyhow::Error> {
        let unbound = Unbound::try_from_message(msg)?;
        let serialized = Serialized::serialize(&unbound.message)?;
        Ok(Self {
            message: serialized,
            bindings: unbound.bindings,
        })
    }

    /// Use the provided function to update values inside bindings in the same
    /// order as they were pushed into bindings.
    pub fn visit_mut<T: Serialize + DeserializeOwned + Named>(
        &mut self,
        f: impl FnMut(&mut T) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        self.bindings.visit_mut(f)
    }

    fn downcast<M: DeserializeOwned + Named>(self) -> anyhow::Result<Unbound<M>> {
        let message: M = self.message.deserialized_unchecked()?;
        Ok(Unbound {
            message,
            bindings: self.bindings,
        })
    }
}

/// Type used for indexing an erased unbound.
/// Has the same serialized representation as `ErasedUnbound`.
#[derive(Debug, PartialEq, Serialize, Deserialize, typeuri::Named)]
#[serde(from = "ErasedUnbound")]
pub struct IndexedErasedUnbound<M>(ErasedUnbound, PhantomData<M>);

impl<M: DeserializeOwned + Named> IndexedErasedUnbound<M> {
    pub(crate) fn downcast(self) -> anyhow::Result<Unbound<M>> {
        self.0.downcast()
    }
}

impl<M: Bind> IndexedErasedUnbound<M> {
    /// Used in unit tests to bind CastBlobT<M> to the given actor. Do not use in
    /// production.
    pub fn bind_for_test_only<A, C>(
        actor_ref: ActorRef<A>,
        cx: C,
        mailbox: Mailbox,
    ) -> anyhow::Result<()>
    where
        A: Referable + RemoteHandles<M> + RemoteHandles<IndexedErasedUnbound<M>>,
        M: RemoteMessage,
        C: context::Actor + Send + Sync + 'static,
    {
        let port_handle = mailbox.open_enqueue_port::<IndexedErasedUnbound<M>>({
            move |_, m| {
                let bound_m = m.downcast()?.bind()?;
                actor_ref.send(&cx, bound_m)?;
                Ok(())
            }
        });
        port_handle.bind_actor_port();
        Ok(())
    }
}

impl<M> From<ErasedUnbound> for IndexedErasedUnbound<M> {
    fn from(erased: ErasedUnbound) -> Self {
        Self(erased, PhantomData)
    }
}

macro_rules! impl_bind_unbind_basic {
    ($t:ty) => {
        impl Bind for $t {
            fn bind(&mut self, bindings: &mut Bindings) -> anyhow::Result<()> {
                anyhow::ensure!(
                    bindings.0.is_empty(),
                    "bindings for {} should be empty, but found {} elements left",
                    stringify!($t),
                    bindings.0.len(),
                );
                Ok(())
            }
        }

        impl Unbind for $t {
            fn unbind(&self, _bindings: &mut Bindings) -> anyhow::Result<()> {
                Ok(())
            }
        }
    };
}

impl_bind_unbind_basic!(());
impl_bind_unbind_basic!(bool);
impl_bind_unbind_basic!(i8);
impl_bind_unbind_basic!(u8);
impl_bind_unbind_basic!(i16);
impl_bind_unbind_basic!(u16);
impl_bind_unbind_basic!(i32);
impl_bind_unbind_basic!(u32);
impl_bind_unbind_basic!(i64);
impl_bind_unbind_basic!(u64);
impl_bind_unbind_basic!(i128);
impl_bind_unbind_basic!(u128);
impl_bind_unbind_basic!(isize);
impl_bind_unbind_basic!(usize);
impl_bind_unbind_basic!(String);

impl<T: Unbind> Unbind for Option<T> {
    fn unbind(&self, bindings: &mut Bindings) -> anyhow::Result<()> {
        match self {
            Some(t) => t.unbind(bindings),
            None => Ok(()),
        }
    }
}

impl<T: Bind> Bind for Option<T> {
    fn bind(&mut self, bindings: &mut Bindings) -> anyhow::Result<()> {
        match self {
            Some(t) => t.bind(bindings),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate as hyperactor; // for macros
    use crate::Bind;
    use crate::PortRef;
    use crate::Unbind;
    use crate::accum::ReducerMode;
    use crate::accum::ReducerSpec;
    use crate::id;
    use crate::reference::UnboundPort;

    // Used to demonstrate a user defined reply type.
    #[derive(Debug, PartialEq, Serialize, Deserialize, typeuri::Named)]
    struct MyReply(String);

    // Used to demonstrate a two-way message type.
    #[derive(
        Debug,
        Clone,
        PartialEq,
        Serialize,
        Deserialize,
        typeuri::Named,
        Bind,
        Unbind
    )]
    struct MyMessage {
        arg0: bool,
        arg1: u32,
        #[binding(include)]
        reply0: PortRef<String>,
        #[binding(include)]
        reply1: PortRef<MyReply>,
    }

    #[test]
    fn test_castable() {
        let original_port0 = PortRef::attest(id!(world[0].actor[0][123]));
        let original_port1 = PortRef::attest_reducible(
            id!(world[1].actor1[0][456]),
            Some(ReducerSpec {
                typehash: 123,
                builder_params: None,
            }),
            ReducerMode::default(),
        );
        let my_message = MyMessage {
            arg0: true,
            arg1: 42,
            reply0: original_port0.clone(),
            reply1: original_port1.clone(),
        };

        let serialized_my_message = Serialized::serialize(&my_message).unwrap();

        // convert to ErasedUnbound
        let mut erased = ErasedUnbound::try_from_message(my_message.clone()).unwrap();
        assert_eq!(
            erased,
            ErasedUnbound {
                message: serialized_my_message.clone(),
                bindings: Bindings(
                    [
                        (
                            UnboundPort::typehash(),
                            Serialized::serialize(&UnboundPort::from(&original_port0)).unwrap(),
                        ),
                        (
                            UnboundPort::typehash(),
                            Serialized::serialize(&UnboundPort::from(&original_port1)).unwrap(),
                        ),
                    ]
                    .into_iter()
                    .collect()
                ),
            }
        );

        // Modify the port in the erased
        let new_port_id0 = id!(world[0].comm[0][680]);
        assert_ne!(&new_port_id0, original_port0.port_id());
        let new_port_id1 = id!(world[1].comm[0][257]);
        assert_ne!(&new_port_id1, original_port1.port_id());

        let mut new_ports = vec![&new_port_id0, &new_port_id1].into_iter();
        erased
            .visit_mut::<UnboundPort>(|b| {
                let port = new_ports.next().unwrap();
                b.update(port.clone());
                Ok(())
            })
            .unwrap();

        let new_port0 = PortRef::<String>::attest(new_port_id0);
        let new_port1 = PortRef::<MyReply>::attest_reducible(
            new_port_id1,
            Some(ReducerSpec {
                typehash: 123,
                builder_params: None,
            }),
            ReducerMode::default(),
        );
        let new_bindings = Bindings(
            [
                (
                    UnboundPort::typehash(),
                    Serialized::serialize(&UnboundPort::from(&new_port0)).unwrap(),
                ),
                (
                    UnboundPort::typehash(),
                    Serialized::serialize(&UnboundPort::from(&new_port1)).unwrap(),
                ),
            ]
            .into_iter()
            .collect(),
        );
        assert_eq!(
            erased,
            ErasedUnbound {
                message: serialized_my_message.clone(),
                bindings: new_bindings.clone(),
            }
        );

        // convert back to MyMessage
        let unbound = erased.downcast::<MyMessage>().unwrap();
        assert_eq!(
            unbound,
            Unbound {
                message: my_message,
                bindings: new_bindings,
            }
        );
        let new_my_message = unbound.bind().unwrap();
        assert_eq!(
            new_my_message,
            MyMessage {
                arg0: true,
                arg1: 42,
                reply0: new_port0,
                reply1: new_port1,
            }
        );
    }
}
