/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Opaque rendezvous tokens for actor reference exchange.
//!
//! A token is a capability to contact a rendezvous actor. The creator can hand
//! the token out through any external mechanism; holders can join and exchange
//! typed actor references with the creator.
//!
//! The protocol has three roles:
//!
//! - The creator calls [`create`] with its own typed ref, a private
//!   [`PortRef<Joined<J>>`] that receives join notifications, and [`Options`].
//!   `create` spawns a supervised child rendezvous actor and returns a
//!   serializable [`Token<C, J>`] that points at that child.
//! - The token holder calls [`Token::join`] with its typed ref and a private
//!   [`PortRef<JoinResult<C>>`]. The call only sends a join request; the
//!   outcome is delivered asynchronously on that result port.
//! - The rendezvous actor delivers [`Joined<J>`] to the creator and
//!   [`JoinResult<C>`] to the joiner. With [`Policy::Multi`], every join is
//!   accepted while the rendezvous actor is alive. With [`Policy::Once`], the
//!   first join is accepted, and later joins receive
//!   [`JoinResult::Rejected`].
//!
//! Tokens serialize as a single base64-encoded compact JSON string. The JSON
//! payload includes the rendezvous actor ref and the type URIs for `C`, `J`,
//! and [`RendezvousLike<C, J>`], so deserialization rejects tokens used with
//! incompatible peer types. [`std::fmt::Display`] prints the canonical token,
//! followed by `#`, followed by the compact JSON payload for inspection.
//! [`FromStr`] accepts either the canonical token or this display form; the
//! suffix after `#` is informational and ignored.

use std::fmt;
use std::str::FromStr;

use base64::Engine as _;
use base64::prelude::BASE64_STANDARD;
use hyperactor::Actor;
use hyperactor::ActorRef;
use hyperactor::Bind;
use hyperactor::Context;
use hyperactor::Endpoint as _;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::PortRef;
use hyperactor::RefClient;
use hyperactor::Unbind;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use typeuri::Named;

/// Type that can be exchanged through a rendezvous token.
pub trait TokenPeer:
    Named + Serialize + DeserializeOwned + Send + Sync + fmt::Debug + 'static
{
}

impl<T> TokenPeer for T where
    T: Named + Serialize + DeserializeOwned + Send + Sync + fmt::Debug + 'static
{
}

/// Token join options.
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
pub struct Options {
    /// Policy that governs how many joins are accepted.
    pub policy: Policy,
}
wirevalue::register_type!(Options);

impl Default for Options {
    fn default() -> Self {
        Self {
            policy: Policy::Multi,
        }
    }
}

/// Token join policy.
#[derive(
    Clone,
    Copy,
    Debug,
    Serialize,
    Deserialize,
    Named,
    PartialEq,
    Eq,
    Bind,
    Unbind
)]
pub enum Policy {
    /// Accept every join while the rendezvous actor is alive.
    Multi,
    /// Accept the first join, then reject later joins.
    Once,
}
wirevalue::register_type!(Policy);

/// Notification delivered to the token creator when a peer joins.
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
#[serde(bound(serialize = "P: TokenPeer"))]
#[serde(bound(deserialize = "P: TokenPeer"))]
pub struct Joined<P: TokenPeer> {
    /// The peer that joined.
    pub peer: P,
}

/// Result delivered to a joiner after it attempts to join.
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
#[serde(bound(serialize = "P: TokenPeer"))]
#[serde(bound(deserialize = "P: TokenPeer"))]
pub enum JoinResult<P: TokenPeer> {
    /// The join was accepted, and the peer ref is available.
    Joined {
        /// The peer on the other side of the rendezvous.
        peer: P,
    },
    /// The join was rejected by rendezvous policy.
    Rejected {
        /// Concise lowercase rejection reason.
        reason: String,
    },
}

/// Request sent to a rendezvous actor by a token holder.
#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    Named,
    Handler,
    HandleClient,
    RefClient,
    Bind,
    Unbind
)]
#[serde(bound(serialize = "C: TokenPeer, J: TokenPeer"))]
#[serde(bound(deserialize = "C: TokenPeer, J: TokenPeer"))]
pub struct Join<C: TokenPeer, J: TokenPeer> {
    /// Joiner ref to deliver to the token creator.
    pub joiner: J,
    /// Port that receives the join result.
    #[binding(include)]
    pub result: PortRef<JoinResult<C>>,
}

hyperactor::behavior!(RendezvousLike<C, J>, Join<C, J>);

/// Create a rendezvous token owned by `this`.
///
/// The token is backed by a child actor, so it is supervised by the creator.
/// The creator receives [`Joined`] notifications on `creator_joined`; joiners
/// receive [`JoinResult`] on the result port they pass to [`Token::join`].
pub fn create<A, C, J>(
    this: &Instance<A>,
    creator: C,
    creator_joined: PortRef<Joined<J>>,
    options: Options,
) -> anyhow::Result<Token<C, J>>
where
    A: Actor,
    C: TokenPeer + Clone,
    J: TokenPeer,
{
    let rendezvous = this.spawn(Rendezvous::new(creator, creator_joined, options))?;
    Ok(Token::new(rendezvous.bind::<RendezvousLike<C, J>>()))
}

/// Opaque rendezvous token.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Token<C: TokenPeer, J: TokenPeer> {
    rendezvous: ActorRef<RendezvousLike<C, J>>,
}

impl<C: TokenPeer, J: TokenPeer> Token<C, J> {
    /// Create a token from a rendezvous actor ref.
    pub fn new(rendezvous: ActorRef<RendezvousLike<C, J>>) -> Self {
        Self { rendezvous }
    }

    /// The rendezvous actor ref contained by this token.
    pub fn rendezvous(&self) -> &ActorRef<RendezvousLike<C, J>> {
        &self.rendezvous
    }

    /// Send a join request to the rendezvous actor.
    pub fn join(
        &self,
        cx: &impl hyperactor::context::Actor,
        joiner: J,
        result: PortRef<JoinResult<C>>,
    ) -> anyhow::Result<()> {
        self.rendezvous.post(cx, Join { joiner, result });
        Ok(())
    }

    fn payload(&self) -> TokenPayload<C, J> {
        TokenPayload {
            creator_type_uri: C::typename().to_string(),
            joiner_type_uri: J::typename().to_string(),
            rendezvous_type_uri: <RendezvousLike<C, J> as Named>::typename().to_string(),
            rendezvous: self.rendezvous.clone(),
        }
    }

    fn encode(&self) -> Result<String, serde_json::Error> {
        Ok(BASE64_STANDARD.encode(serde_json::to_string(&self.payload())?))
    }

    fn compact_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self.payload())
    }
}

impl<C, J> Serialize for Token<C, J>
where
    C: TokenPeer,
    J: TokenPeer,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.encode().map_err(serde::ser::Error::custom)?)
    }
}

impl<'de, C, J> Deserialize<'de> for Token<C, J>
where
    C: TokenPeer,
    J: TokenPeer,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let token = String::deserialize(deserializer)?;
        let json = BASE64_STANDARD
            .decode(token)
            .map_err(serde::de::Error::custom)?;
        let payload: TokenPayload<C, J> =
            serde_json::from_slice(&json).map_err(serde::de::Error::custom)?;
        payload.validate().map_err(serde::de::Error::custom)?;
        Ok(Self {
            rendezvous: payload.rendezvous,
        })
    }
}

impl<C, J> FromStr for Token<C, J>
where
    C: TokenPeer,
    J: TokenPeer,
{
    type Err = anyhow::Error;

    fn from_str(token: &str) -> Result<Self, Self::Err> {
        let token = token.split_once('#').map_or(token, |(token, _)| token);
        let json = BASE64_STANDARD.decode(token)?;
        let payload: TokenPayload<C, J> = serde_json::from_slice(&json)?;
        payload.validate()?;
        Ok(Self {
            rendezvous: payload.rendezvous,
        })
    }
}

impl<C, J> fmt::Display for Token<C, J>
where
    C: TokenPeer,
    J: TokenPeer,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let token = self.encode().map_err(|_| fmt::Error)?;
        let json = self.compact_json().map_err(|_| fmt::Error)?;
        write!(f, "{}#{}", token, json)
    }
}

#[derive(Debug)]
struct Rendezvous<C: TokenPeer, J: TokenPeer> {
    creator: C,
    creator_joined: PortRef<Joined<J>>,
    options: Options,
    joined: bool,
}

impl<C: TokenPeer, J: TokenPeer> Rendezvous<C, J> {
    fn new(creator: C, creator_joined: PortRef<Joined<J>>, options: Options) -> Self {
        Self {
            creator,
            creator_joined,
            options,
            joined: false,
        }
    }
}

#[async_trait::async_trait]
impl<C: TokenPeer, J: TokenPeer> Actor for Rendezvous<C, J> {}

#[async_trait::async_trait]
impl<C: TokenPeer + Clone, J: TokenPeer> Handler<Join<C, J>> for Rendezvous<C, J> {
    async fn handle(&mut self, cx: &Context<Self>, message: Join<C, J>) -> anyhow::Result<()> {
        if self.options.policy == Policy::Once && self.joined {
            message.result.post(
                cx,
                JoinResult::Rejected {
                    reason: "token already joined".to_string(),
                },
            );
            return Ok(());
        }

        self.creator_joined.post(
            cx,
            Joined {
                peer: message.joiner,
            },
        );

        message.result.post(
            cx,
            JoinResult::Joined {
                peer: self.creator.clone(),
            },
        );
        self.joined = true;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound(serialize = "C: TokenPeer, J: TokenPeer"))]
#[serde(bound(deserialize = "C: TokenPeer, J: TokenPeer"))]
struct TokenPayload<C: TokenPeer, J: TokenPeer> {
    creator_type_uri: String,
    joiner_type_uri: String,
    rendezvous_type_uri: String,
    rendezvous: ActorRef<RendezvousLike<C, J>>,
}

impl<C: TokenPeer, J: TokenPeer> TokenPayload<C, J> {
    fn validate(&self) -> Result<(), TokenTypeError> {
        check_type_uri("creator", &self.creator_type_uri, C::typename())?;
        check_type_uri("joiner", &self.joiner_type_uri, J::typename())?;
        check_type_uri(
            "rendezvous",
            &self.rendezvous_type_uri,
            <RendezvousLike<C, J> as Named>::typename(),
        )
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{kind} type uri mismatch: expected {expected}, got {actual}")]
struct TokenTypeError {
    kind: &'static str,
    expected: &'static str,
    actual: String,
}

fn check_type_uri(
    kind: &'static str,
    actual: &str,
    expected: &'static str,
) -> Result<(), TokenTypeError> {
    if actual == expected {
        return Ok(());
    }
    Err(TokenTypeError {
        kind,
        expected,
        actual: actual.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use hyperactor::Actor;
    use hyperactor::Context;
    use hyperactor::PortHandle;
    use hyperactor::Proc;

    use super::*;

    #[derive(Debug)]
    #[hyperactor::export]
    struct RendezvousStub;

    #[async_trait::async_trait]
    impl Actor for RendezvousStub {}

    #[async_trait::async_trait]
    impl Handler<Join<CreatorRef, JoinerRef>> for RendezvousStub {
        async fn handle(
            &mut self,
            _cx: &Context<Self>,
            _message: Join<CreatorRef, JoinerRef>,
        ) -> anyhow::Result<()> {
            Ok(())
        }
    }

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
    struct CreatorRef;

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
    struct JoinerRef;

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
    struct OtherJoinerRef;

    #[derive(
        Clone,
        Debug,
        Serialize,
        Deserialize,
        Named,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        Bind,
        Unbind
    )]
    enum MultiJoinerRef {
        One,
        Two,
        Three,
    }

    #[derive(Debug)]
    struct CreatorActor {
        creator_joined: PortHandle<Joined<JoinerRef>>,
        token_out: PortHandle<Token<CreatorRef, JoinerRef>>,
    }

    #[async_trait::async_trait]
    impl Actor for CreatorActor {
        async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
            let token = create(
                this,
                CreatorRef,
                self.creator_joined.bind(),
                Options::default(),
            )?;
            self.token_out.post(this, token);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_create_delivers_join_to_both_sides() {
        let proc = Proc::isolated();
        let (creator, _creator_handle) = proc.instance("creator").unwrap();
        let (creator_joined, mut creator_joined_rx) = creator.open_port::<Joined<JoinerRef>>();
        let token = create(
            &creator,
            CreatorRef,
            creator_joined.bind(),
            Options::default(),
        )
        .unwrap();
        let (joiner, _joiner_handle) = proc.instance("joiner").unwrap();
        let (join_result, mut join_result_rx) = joiner.open_port::<JoinResult<CreatorRef>>();

        token.join(&joiner, JoinerRef, join_result.bind()).unwrap();

        assert_eq!(creator_joined_rx.recv().await.unwrap().peer, JoinerRef);
        assert_eq!(
            join_result_rx.recv().await.unwrap(),
            JoinResult::Joined { peer: CreatorRef }
        );
    }

    #[tokio::test]
    async fn test_once_token_rejects_later_joins() {
        let proc = Proc::isolated();
        let (creator, _creator_handle) = proc.instance("creator").unwrap();
        let (creator_joined, mut creator_joined_rx) = creator.open_port::<Joined<JoinerRef>>();
        let token = create(
            &creator,
            CreatorRef,
            creator_joined.bind(),
            Options {
                policy: Policy::Once,
            },
        )
        .unwrap();
        let (joiner, _joiner_handle) = proc.instance("joiner").unwrap();
        let (first_result, mut first_result_rx) = joiner.open_port::<JoinResult<CreatorRef>>();
        let (second_result, mut second_result_rx) = joiner.open_port::<JoinResult<CreatorRef>>();

        token.join(&joiner, JoinerRef, first_result.bind()).unwrap();
        token
            .join(&joiner, JoinerRef, second_result.bind())
            .unwrap();

        assert_eq!(creator_joined_rx.recv().await.unwrap().peer, JoinerRef);
        assert_eq!(
            first_result_rx.recv().await.unwrap(),
            JoinResult::Joined { peer: CreatorRef }
        );
        assert_eq!(
            second_result_rx.recv().await.unwrap(),
            JoinResult::Rejected {
                reason: "token already joined".to_string()
            }
        );
    }

    #[tokio::test]
    async fn test_multi_token_accepts_every_join() {
        let proc = Proc::isolated();
        let (creator, _creator_hndl) = proc.instance("creator").unwrap();
        let (creator_joined, mut creator_joined_rx) = creator.open_port::<Joined<MultiJoinerRef>>();
        let token = create(
            &creator,
            CreatorRef,
            creator_joined.bind(),
            Options {
                policy: Policy::Multi,
            },
        )
        .unwrap();

        let (joiner1, _joiner1_handle) = proc.instance("joiner1").unwrap();
        let (joiner2, _joiner2_handle) = proc.instance("joiner2").unwrap();
        let (joiner3, _joiner3_handle) = proc.instance("joiner3").unwrap();

        let (r1, mut r1_rx) = joiner1.open_port::<JoinResult<CreatorRef>>();
        let (r2, mut r2_rx) = joiner2.open_port::<JoinResult<CreatorRef>>();
        let (r3, mut r3_rx) = joiner3.open_port::<JoinResult<CreatorRef>>();

        token
            .join(&joiner1, MultiJoinerRef::One, r1.bind())
            .unwrap();
        token
            .join(&joiner2, MultiJoinerRef::Two, r2.bind())
            .unwrap();
        token
            .join(&joiner3, MultiJoinerRef::Three, r3.bind())
            .unwrap();

        let mut joined = vec![
            creator_joined_rx.recv().await.unwrap().peer,
            creator_joined_rx.recv().await.unwrap().peer,
            creator_joined_rx.recv().await.unwrap().peer,
        ];
        joined.sort();

        assert_eq!(
            joined,
            vec![
                MultiJoinerRef::One,
                MultiJoinerRef::Two,
                MultiJoinerRef::Three,
            ]
        );

        assert_eq!(
            r1_rx.recv().await.unwrap(),
            JoinResult::Joined { peer: CreatorRef }
        );
        assert_eq!(
            r2_rx.recv().await.unwrap(),
            JoinResult::Joined { peer: CreatorRef }
        );
        assert_eq!(
            r3_rx.recv().await.unwrap(),
            JoinResult::Joined { peer: CreatorRef }
        );
    }

    #[tokio::test]
    async fn test_token_serializes_as_single_base64_json_string() {
        let proc = Proc::isolated();
        let rendezvous = proc.spawn("rendezvous", RendezvousStub).unwrap();
        let token = Token::<CreatorRef, JoinerRef>::new(
            rendezvous.bind::<RendezvousLike<CreatorRef, JoinerRef>>(),
        );

        let serialized = serde_json::to_string(&token).unwrap();
        let encoded: String = serde_json::from_str(&serialized).unwrap();
        assert!(!encoded.contains('#'));

        let json = BASE64_STANDARD.decode(encoded).unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(
            payload["creator_type_uri"],
            <CreatorRef as Named>::typename()
        );
        assert_eq!(payload["joiner_type_uri"], <JoinerRef as Named>::typename());
        assert_eq!(
            payload["rendezvous_type_uri"],
            <RendezvousLike<CreatorRef, JoinerRef> as Named>::typename()
        );

        rendezvous.stop("test").unwrap();
        rendezvous.await;
    }

    #[tokio::test]
    async fn test_token_display_includes_compact_json_suffix() {
        let proc = Proc::isolated();
        let rendezvous = proc.spawn("rendezvous", RendezvousStub).unwrap();
        let token = Token::<CreatorRef, JoinerRef>::new(
            rendezvous.bind::<RendezvousLike<CreatorRef, JoinerRef>>(),
        );

        let display = token.to_string();
        let (encoded, json) = display.split_once('#').unwrap();
        assert_eq!(encoded, token.encode().unwrap());
        assert_eq!(json, token.compact_json().unwrap());

        rendezvous.stop("test").unwrap();
        rendezvous.await;
    }

    #[tokio::test]
    async fn test_token_from_str_accepts_display_suffix() {
        let proc = Proc::isolated();
        let rendezvous = proc.spawn("rendezvous", RendezvousStub).unwrap();
        let token = Token::<CreatorRef, JoinerRef>::new(
            rendezvous.bind::<RendezvousLike<CreatorRef, JoinerRef>>(),
        );

        let encoded = token.encode().unwrap();
        let parsed_encoded: Token<CreatorRef, JoinerRef> = encoded.parse().unwrap();
        let parsed_display: Token<CreatorRef, JoinerRef> = token.to_string().parse().unwrap();
        let parsed_ignored_suffix: Token<CreatorRef, JoinerRef> =
            format!("{encoded}#not json").parse().unwrap();

        assert_eq!(parsed_encoded, token);
        assert_eq!(parsed_display, token);
        assert_eq!(parsed_ignored_suffix, token);

        rendezvous.stop("test").unwrap();
        rendezvous.await;
    }

    #[tokio::test]
    async fn test_token_rejects_incompatible_type_parameters() {
        let proc = Proc::isolated();
        let rendezvous = proc.spawn("rendezvous", RendezvousStub).unwrap();
        let token = Token::<CreatorRef, JoinerRef>::new(
            rendezvous.bind::<RendezvousLike<CreatorRef, JoinerRef>>(),
        );
        let serialized = serde_json::to_string(&token).unwrap();

        let result = serde_json::from_str::<Token<CreatorRef, OtherJoinerRef>>(&serialized);

        assert!(result.is_err());

        rendezvous.stop("test").unwrap();
        rendezvous.await;
    }

    #[test]
    fn test_token_from_str_rejects_corrupt_base64() {
        let result: Result<Token<CreatorRef, JoinerRef>, _> = "%%%".parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_token_from_str_rejects_invalid_json() {
        let encoded = BASE64_STANDARD.encode(b"not json at all");
        let result: Result<Token<CreatorRef, JoinerRef>, _> = encoded.parse();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_token_from_str_rejects_mismatched_type_uris() {
        let proc = Proc::isolated();
        let rendezvous = proc.spawn("rendezvous", RendezvousStub).unwrap();
        let token = Token::<CreatorRef, JoinerRef>::new(
            rendezvous.bind::<RendezvousLike<CreatorRef, JoinerRef>>(),
        );

        let encoded = token.encode().unwrap();
        let result: Result<Token<CreatorRef, OtherJoinerRef>, _> = encoded.parse();
        assert!(result.is_err());

        rendezvous.stop("test").unwrap();
        rendezvous.await;
    }

    #[tokio::test]
    async fn test_join_fails_after_creator_stops() {
        let proc = Proc::isolated();
        let (inst, _inst_handle) = proc.instance("inst").unwrap();
        let (creator_joined, _creator_joined_rx) = inst.open_port::<Joined<JoinerRef>>();
        let (token_out, mut token_out_rx) = inst.open_port::<Token<CreatorRef, JoinerRef>>();

        let creator_handle = proc
            .spawn(
                "creator",
                CreatorActor {
                    creator_joined,
                    token_out,
                },
            )
            .unwrap();

        let token = token_out_rx.recv().await.unwrap();

        // Stop the creator; its supervised rendezvous actor dies with
        // it.
        creator_handle.stop("test").unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), creator_handle)
            .await
            .unwrap();

        let (joiner, _joiner_handle) = proc.instance("joiner").unwrap();
        let (result, mut result_rx) = joiner.open_port::<JoinResult<CreatorRef>>();
        token.join(&joiner, JoinerRef, result.bind()).unwrap();

        // No rendezvous actor remains to send a result.
        let timed_out =
            tokio::time::timeout(std::time::Duration::from_millis(500), result_rx.recv()).await;
        assert!(
            timed_out.is_err(),
            "join should produce no result after creator stops"
        );
    }
}
