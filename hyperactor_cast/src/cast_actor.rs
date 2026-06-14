/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The CastActor: a system actor that bootstraps and manages casting domains.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorAddr;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::Endpoint as _;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::Label;
use hyperactor::OncePortRefRepr;
use hyperactor::PortRef;
use hyperactor::PortRefRepr;
use hyperactor::RemoteEndpoint as _;
use hyperactor::Uid;
use hyperactor::accum::ReducerMode;
use hyperactor::context;
use hyperactor::mailbox::MailboxSender;
use hyperactor::mailbox::MessageEnvelope;
use hyperactor::mailbox::Undeliverable;
use hyperactor::mailbox::UndeliverableMailboxSender;
use hyperactor::mailbox::UndeliverableMessageError;
use hyperactor::mailbox::monitored_return_handle;
use hyperactor::message::ErasedUnbound;
use hyperactor::ordering::SEQ_INFO;
use hyperactor::ordering::SeqInfo;
use hyperactor::port::Port;
use hyperactor::value_mesh::ValueMesh;
use hyperactor_config::Flattrs;
use ndslice::Point;
use ndslice::Region;
use ndslice::view::MapIntoExt;
use ndslice::view::View;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;
use uuid::Uuid;

use crate::tile::MaterializedTile;
use crate::tile::Tile;
use crate::tile::TilingPolicy;

hyperactor_config::declare_attrs! {
    /// Header stamped on each locally delivered message with the
    /// recipient's point within the casting domain.
    pub attr CAST_POINT: Point;

    /// Header stamped on each locally delivered message with the
    /// original sender that initiated the cast.
    pub attr CAST_ORIGINATING_SENDER: ActorAddr;

    /// The multicast phase that attached context to a delivery failure.
    pub attr CAST_FAILURE_PHASE: String;

    /// The cast actor that attached multicast context to a delivery failure.
    pub attr CAST_FAILURE_CAST_ACTOR: ActorAddr;

    /// The originating cast sender.
    pub attr CAST_FAILURE_ORIGIN: ActorAddr;

    /// The return port used to send the undeliverable message to the origin.
    pub attr CAST_FAILURE_RETURN_PORT: String;
}

#[cfg(test)]
hyperactor_config::declare_attrs! {
    /// Header stamped in tests with the cast tree path used to reach this
    /// recipient.
    pub attr CAST_LINEAGE: Vec<usize>;
}
/// Pure, data-only identifier for a cast domain.
///
/// This type contains no runtime references (e.g. `ActorRef`) and can
/// be freely serialized, cloned, and shared.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CastDomainId {
    /// Unique identifier for this domain's installed communication plan.
    domain_id: Uid,
}

impl CastDomainId {
    /// Create a new domain id.
    pub fn new() -> Self {
        Self {
            domain_id: Uid::anonymous(),
        }
    }

    /// The domain's unique identifier.
    pub fn domain_id(&self) -> &Uid {
        &self.domain_id
    }

    /// Materialize this domain id over concrete members and return an
    /// addressable domain handle.
    ///
    /// `members` maps domain rank to member actor address. `region` describes
    /// the logical root region of the domain; tiling and communication are
    /// derived internally.
    pub fn materialize(
        self,
        cx: &impl context::Actor,
        members: HashMap<usize, ActorAddr>,
        region: Region,
        tiling_policy: TilingPolicy,
    ) -> anyhow::Result<CastDomainRef> {
        anyhow::ensure!(
            members.len() == region.num_ranks()
                && region
                    .slice()
                    .iter()
                    .all(|rank| members.contains_key(&rank)),
            "members must contain exactly one actor address for every domain rank"
        );

        let root_rank = region.slice().offset();
        let entry_point = members
            .get(&root_rank)
            .expect("members coverage was checked above")
            .clone();
        let member_mesh = Arc::new(ValueMesh::new(
            region.clone(),
            region
                .slice()
                .iter()
                .map(|rank| {
                    members
                        .get(&rank)
                        .expect("members coverage was checked above")
                        .clone()
                })
                .collect(),
        )?);

        let domain_ref = CastDomainRef::from_entry_point(
            self.clone(),
            cast_actor_ref_for_member(&entry_point),
            Arc::clone(&member_mesh),
        );
        let root_tile =
            MaterializedTile::from_value_mesh_with_tile(Tile::from_view(&region), member_mesh);

        domain_ref.entry_point.post(
            cx,
            CreateCastDomain {
                cast_domain_id: self,
                region,
                tiling_policy,
                tile: root_tile,
            },
        );
        Ok(domain_ref)
    }
}

impl std::fmt::Display for CastDomainId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.domain_id)
    }
}

/// Opaque local handle for initiating work against a materialized cast domain.
///
/// Unlike [`CastDomainId`], this includes an entry-point [`CastActor`] ref so
/// callers can cast into and otherwise address the domain. Callers obtain this
/// only by materializing a [`CastDomainId`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CastDomainRef {
    id: CastDomainId,
    /// Entry-point [`CastActor`] ref for initiating casts.
    entry_point: ActorRef<CastActor>,
    /// Destination actor addresses keyed by this domain's rank space.
    members: Arc<ValueMesh<ActorAddr>>,
}

impl CastDomainRef {
    /// Rebuild a cast-domain handle from a pure id plus its entry-point ref.
    fn from_entry_point(
        id: CastDomainId,
        entry_point: ActorRef<CastActor>,
        members: Arc<ValueMesh<ActorAddr>>,
    ) -> Self {
        Self {
            id,
            entry_point,
            members,
        }
    }

    /// The pure identifier for this domain.
    pub fn id(&self) -> CastDomainId {
        self.id.clone()
    }

    /// The domain's unique identifier.
    pub fn domain_id(&self) -> &Uid {
        self.id.domain_id()
    }

    /// The [`CastActor`] entry point for this domain.
    pub fn entry_point(&self) -> &ActorRef<CastActor> {
        &self.entry_point
    }

    /// Destination actor addresses keyed by this domain's rank space.
    pub fn members(&self) -> &ValueMesh<ActorAddr> {
        &self.members
    }

    /// Materialize a new slice domain described relative to this domain.
    ///
    /// The returned ref is the handle for the slice. It gets a fresh domain id
    /// whose entrypoint is the slice's actual root CastActor. The parent ref's
    /// dense members are used only to derive the slice definition and sender
    /// side sequence map; the cast path enters directly at the slice root.
    pub fn materialize_slice(
        &self,
        cx: &impl context::Actor,
        region: Region,
        tiling_policy: TilingPolicy,
    ) -> anyhow::Result<CastDomainRef> {
        let slice_id = CastDomainId::new();
        let slice_member_mesh = Arc::new(self.members.subset(region.clone())?);
        let slice_tile = MaterializedTile::from_value_mesh_with_tile(
            Tile::from_view(&region),
            Arc::clone(&slice_member_mesh),
        );

        let slice_entrypoint = cast_actor_ref_for_member(
            slice_tile
                .root_item()
                .ok_or_else(|| anyhow::anyhow!("slice root tile must have at least one member"))?,
        );

        let slice_ref = CastDomainRef::from_entry_point(
            slice_id.clone(),
            slice_entrypoint.clone(),
            slice_member_mesh,
        );

        slice_entrypoint.post(
            cx,
            CreateCastDomain {
                cast_domain_id: slice_id.clone(),
                region,
                tiling_policy,
                tile: slice_tile,
            },
        );
        Ok(slice_ref)
    }

    /// Cast a message to all members of this domain with caller-supplied headers.
    ///
    /// `headers` are the destination envelope headers supplied by the caller.
    /// The cast layer stamps cast-owned fields on top before sending the
    /// [`CastMessage`] through the domain entry point.
    pub fn cast<M: Serialize + Named>(
        &self,
        cx: &impl context::Actor,
        headers: Flattrs,
        message: M,
    ) -> anyhow::Result<()> {
        let data = ErasedUnbound::try_from_message(message)?;
        let sender = cx.mailbox().actor_addr().clone();
        let dest_port = M::port();
        let (session_id, seqs) = self.seqs_for_cast(cx, dest_port)?;

        let cast_headers = headers.clone();
        self.entry_point.port().post_with_headers(
            cx,
            headers,
            CastMessage {
                cast_domain_id: self.id.clone(),
                sender,
                session_id,
                seqs,
                #[cfg(test)]
                lineage: Vec::new(),
                headers: cast_headers,
                dest_port,
                data,
            },
        );
        Ok(())
    }

    /// Tear down this cast domain.
    ///
    /// This is a best-effort, fire-and-forget operation: the request is posted
    /// to the domain entry point and then forwarded through the installed cast
    /// tree. Duplicate destroy requests are intentionally ignored by receivers.
    /// If the mailbox layer bounces a downstream destroy message, the failure is
    /// returned to the originating actor's handler port, but successful teardown
    /// is not acknowledged.
    pub fn destroy(&self, cx: &impl context::Actor) {
        self.entry_point.post(
            cx,
            DestroyCastDomain {
                domain_id: self.id.clone(),
                origin: cx.mailbox().actor_addr().clone(),
            },
        );
    }

    /// Allocate one normal sender-side sequence number per destination rank.
    ///
    /// This is the same model used by v1 `CommActor`: the cast message carries
    /// a complete `rank -> seq` snapshot, so forwarding hops do not need
    /// route-local metadata to derive receiver ordering. `ValueMesh` preserves
    /// the domain rank space while allowing compact representations when seqs
    /// happen to be compressible.
    fn seqs_for_cast(
        &self,
        cx: &impl context::Actor,
        dest_port: u64,
    ) -> Result<(Uuid, ValueMesh<u64>)> {
        let sequencer = cx.instance().sequencer();
        let seqs = self.members.as_ref().map_into(|member| {
            let port = member.port_addr(Port::handler_id(dest_port, None));
            let SeqInfo::Session { session_id: _, seq } = sequencer.assign_seq(&port) else {
                unreachable!("assign_seq always returns SeqInfo::Session");
            };
            seq
        });
        Ok((sequencer.session_id(), seqs))
    }
}

/// Return the tiles directly reached from `tile` by the current communication
/// algorithm.
///
/// This asks only for the current tile's outgoing edges without materializing
/// the full domain tree. The returned [`MaterializedTile`]s are still tiles of
/// destination actors. Setup/forwarding derives the target [`CastActor`] from
/// each child tile's root destination actor.
///
/// ```text
/// current MaterializedTile:
/// T0 [ A0 A1 A2 A3
///      A4 A5 A6 A7 ]
///
/// next_tiles(current), rendered by destination actor rank:
/// A0
/// |-- T1 [ A1 ]          -> CastActor on A1's proc
/// |-- T2 [ A2 ]          -> CastActor on A2's proc
/// |-- T3 [ A3 ]          -> CastActor on A3's proc
/// `-- T4 [ A4 A5 A6 A7 ] -> CastActor on A4's proc
/// ```
fn next_tiles(
    tiling_policy: TilingPolicy,
    tile: &MaterializedTile<ActorAddr>,
) -> Vec<MaterializedTile<ActorAddr>> {
    tiling_policy
        .children(tile.tile())
        .into_iter()
        .map(|child| tile.subtile(child))
        .collect()
}

/// Well-known actor name for the [`CastActor`] system actor.
///
/// One `CastActor` is expected to run on every proc under this name. Internal
/// setup uses this known address to route setup commands to child tile roots.
pub const CAST_ACTOR_NAME: &str = "cast";

/// System actor that establishes casting domains.
///
/// One CastActor lives on every proc (well-known name [`CAST_ACTOR_NAME`]).
/// It installs and propagates [`CreateCastDomain`]. After a domain is set up,
/// the CastActor stores tile-local execution state in [`CastHop`] for
/// subsequent multicast routing.
#[derive(Debug, Default)]
#[hyperactor::export(
    handlers = [
        CreateCastDomain,
        DestroyCastDomain,
        CastMessage
    ],
)]
#[hyperactor::spawnable]
pub struct CastActor {
    /// Per-hop routing state installed on this actor.
    installed_hops: HashMap<CastDomainId, CastHop>,
}

/// One tile-local hop in an installed cast tree.
#[derive(Debug, Clone)]
struct CastHop {
    /// Current hop representative's point in the full domain region.
    point_in_domain: Point,
    /// Current hop representative's base rank in the full domain region.
    base_rank_in_domain: usize,
    /// Precomputed outgoing routes to communication-child tiles.
    next_hops: Vec<ActorRef<CastActor>>,
    /// Actor that receives local delivery when this hop is reached.
    local_actor: ActorAddr,
}

fn cast_actor_ref_for_member(member: &ActorAddr) -> ActorRef<CastActor> {
    ActorRef::attest(ActorAddr::root(
        member.proc_addr(),
        Label::strip(CAST_ACTOR_NAME),
    ))
}

fn annotate_cast_failure(
    envelope: &mut MessageEnvelope,
    cast_actor: &ActorAddr,
    phase: &str,
    origin: &ActorAddr,
    return_port: &hyperactor::PortAddr,
) {
    if let Some(failure) = envelope.root_delivery_failure_mut() {
        failure.attrs.set(CAST_FAILURE_PHASE, phase.to_string());
        failure
            .attrs
            .set(CAST_FAILURE_CAST_ACTOR, cast_actor.clone());
        failure.attrs.set(CAST_FAILURE_ORIGIN, origin.clone());
        failure
            .attrs
            .set(CAST_FAILURE_RETURN_PORT, return_port.to_string());
    }
}

#[async_trait]
impl Actor for CastActor {
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        this.set_system();
        Ok(())
    }

    async fn handle_undeliverable_message(
        &mut self,
        cx: &Instance<Self>,
        _reason: hyperactor::mailbox::UndeliverableReason,
        undelivered: Undeliverable<MessageEnvelope>,
    ) -> Result<(), anyhow::Error> {
        self.return_delivery_failure_to_origin(cx, undelivered)
            .await
    }

    async fn handle_invalid_reference(
        &mut self,
        cx: &Instance<Self>,
        _invalid: hyperactor::mailbox::InvalidReference,
        undelivered: Undeliverable<MessageEnvelope>,
    ) -> Result<(), anyhow::Error> {
        self.return_delivery_failure_to_origin(cx, undelivered)
            .await
    }
}

impl CastActor {
    async fn return_delivery_failure_to_origin(
        &mut self,
        cx: &Instance<Self>,
        undelivered: Undeliverable<MessageEnvelope>,
    ) -> Result<(), anyhow::Error> {
        // This is almost 1-1 copied from `hyperactor_mesh::comm::CommActor`.
        let mut message_envelope = match undelivered {
            Undeliverable::Returned(message_envelope) => message_envelope,
            Undeliverable::Report(report) => {
                anyhow::bail!(UndeliverableMessageError::Report { report });
            }
        };

        // 1. Case delivery failure at a "forwarding" step.
        if let Ok(message) = message_envelope.deserialized::<CastMessage>() {
            let return_port = PortRef::attest_handler_port(&message.sender);
            annotate_cast_failure(
                &mut message_envelope,
                cx.self_addr(),
                "forward",
                &message.sender,
                return_port.port_addr(),
            );

            // Needed so that the receiver of the undeliverable message can easily find the
            // original sender of the cast message.
            message_envelope.set_header(CAST_ORIGINATING_SENDER, message.sender.clone());

            return_port.post(cx, Undeliverable::Returned(message_envelope.clone()));
            return Ok(());
        }

        // 2. Failure while forwarding a destroy request.
        if let Ok(message) = message_envelope.deserialized::<DestroyCastDomain>() {
            let return_port = PortRef::attest_handler_port(&message.origin);
            annotate_cast_failure(
                &mut message_envelope,
                cx.self_addr(),
                "destroy",
                &message.origin,
                return_port.port_addr(),
            );
            message_envelope.set_header(CAST_ORIGINATING_SENDER, message.origin.clone());
            return_port.post(cx, Undeliverable::Returned(message_envelope.clone()));
            return Ok(());
        }

        // 3. Failure while delivering from this CastActor to the local
        // destination actor.
        if let Some(sender) = message_envelope.headers().get(CAST_ORIGINATING_SENDER) {
            let return_port = PortRef::attest_handler_port(&sender);
            annotate_cast_failure(
                &mut message_envelope,
                cx.self_addr(),
                "deliver_here",
                &sender,
                return_port.port_addr(),
            );
            return_port.post(cx, Undeliverable::Returned(message_envelope.clone()));
            return Ok(());
        }

        // 4. A return of an undeliverable message was itself returned.
        UndeliverableMailboxSender
            .post(message_envelope, /*unused */ monitored_return_handle());
        Ok(())
    }
}

/// Install one hop of a cast domain and propagate setup down the routing tree.
///
/// Root materialization sends this to the root tile's [`CastActor`]. Each
/// receiving [`CastActor`] stores its [`CastHop`], computes outgoing next hops
/// from its materialized tile, and forwards this same message with the
/// corresponding communication-child tile.
#[derive(Debug, Serialize, Deserialize, typeuri::Named)]
struct CreateCastDomain {
    cast_domain_id: CastDomainId,
    region: Region,
    tiling_policy: TilingPolicy,
    tile: MaterializedTile<ActorAddr>,
}
wirevalue::register_type!(CreateCastDomain);

#[async_trait]
impl Handler<CreateCastDomain> for CastActor {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            domain_id = %message.cast_domain_id,
            rank = message.tile.root_rank(),
            num_members = message.tile.rank_count(),
        )
    )]
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: CreateCastDomain,
    ) -> Result<(), anyhow::Error> {
        let CreateCastDomain {
            cast_domain_id,
            region,
            tiling_policy,
            tile,
        } = message;
        if self.installed_hops.contains_key(&cast_domain_id) {
            return Ok(());
        }

        let mut next_hops = Vec::new();

        for next_tile in next_tiles(tiling_policy, &tile) {
            let next_hop_cast_actor = cast_actor_ref_for_member(
                next_tile
                    .root_item()
                    .ok_or_else(|| anyhow::anyhow!("next tile must have at least one member"))?,
            );

            next_hop_cast_actor.post(
                cx,
                CreateCastDomain {
                    cast_domain_id: cast_domain_id.clone(),
                    region: region.clone(),
                    tiling_policy,
                    tile: next_tile,
                },
            );

            next_hops.push(next_hop_cast_actor);
        }

        let cast_hop = CastHop {
            point_in_domain: region.point_of_base_rank(tile.root_rank())?,
            base_rank_in_domain: tile.root_rank(),
            next_hops,
            local_actor: tile
                .root_item()
                .ok_or_else(|| anyhow::anyhow!("tile must have at least one member"))?
                .clone(),
        };

        #[cfg(test)]
        {
            tests::capture_installed_domain(cx, cast_domain_id.domain_id(), &cast_hop);
        }

        self.installed_hops.insert(cast_domain_id, cast_hop);

        Ok(())
    }
}

/// Rewrite reply port parts in the serialized message so that downstream
/// actors reply through local proxy ports on this CastActor instead
/// of directly to the original sender. Each proxy port reduces
/// replies from downstream next hops plus the optional local delivery,
/// forming a reduction tree that mirrors the cast tree.
///
/// Ported from `hyperactor_mesh::comm::split_ports`.
fn split_ports(
    cx: &Context<'_, CastActor>,
    data: &mut ErasedUnbound,
    num_next_hops: usize,
    deliver_here: bool,
) -> Result<()> {
    data.visit_mut::<PortRefRepr>(|port| {
        if port.unsplit() {
            return Ok(());
        }

        let split = port.port_addr().split(
            cx,
            port.reducer_spec().clone(),
            ReducerMode::Streaming(port.streaming_opts().clone()),
            port.get_return_undeliverable(),
        )?;

        #[cfg(test)]
        {
            tests::collect_split_port(port.port_addr(), &split, deliver_here);
        }

        port.update_port_addr(split);
        Ok(())
    })?;

    data.visit_mut::<OncePortRefRepr>(|port| {
        if port.unsplit() || port.reducer_spec().is_none() {
            // OncePorts without reducers cannot be split. Pass through as-is.
            // Using the port more than once will cause a delivery error downstream.
            return Ok(());
        }

        let peer_count = num_next_hops + if deliver_here { 1 } else { 0 };
        let split = port.port_addr().split(
            cx,
            port.reducer_spec().clone(),
            ReducerMode::Once(peer_count),
            true,
        )?;

        #[cfg(test)]
        {
            tests::collect_split_port(port.port_addr(), &split, deliver_here);
        }

        port.update_port_addr(split);
        Ok(())
    })?;

    Ok(())
}
/// Test-only forwarding path metadata.
///
/// In production this is zero-sized and optimized away. In tests, each
/// forwarded message carries the semantic tile-root ranks already traversed,
/// and local delivery appends the current tile root.
#[derive(Debug, Clone, Default)]
struct ForwardLineage {
    #[cfg(test)]
    ranks: Vec<usize>,
}

impl ForwardLineage {
    #[cfg(test)]
    fn from_message(message: &CastMessage) -> Self {
        Self {
            ranks: message.lineage.clone(),
        }
    }

    #[cfg(not(test))]
    fn from_message(_message: &CastMessage) -> Self {
        Self {}
    }

    fn through(&self, rank: usize) -> Self {
        #[cfg(test)]
        {
            let mut ranks = self.ranks.clone();
            ranks.push(rank);
            Self { ranks }
        }
        #[cfg(not(test))]
        {
            let _ = rank;
            Self {}
        }
    }

    #[cfg(test)]
    fn ranks(&self) -> Vec<usize> {
        self.ranks.clone()
    }
}

/// Multicast payload routed through a cast domain.
///
/// Clients send this to a domain entry point. CastActors forward the same
/// message type to child hops; internal forwards differ only in test-only
/// lineage.
#[derive(Debug, Serialize, Deserialize, typeuri::Named)]
struct CastMessage {
    /// The domain to cast into.
    cast_domain_id: CastDomainId,
    /// Actor that initiated the cast.
    sender: ActorAddr,
    /// Sender-side sequencer session for this cast.
    session_id: Uuid,
    /// Per-domain-rank sequence numbers allocated by the sender before routing.
    seqs: ValueMesh<u64>,
    /// Test-only semantic path of tile root ranks traversed so far.
    #[cfg(test)]
    lineage: Vec<usize>,
    /// Message headers.
    headers: Flattrs,
    /// The target port index on each destination actor.
    dest_port: u64,
    /// The serialized message data.
    data: ErasedUnbound,
}

wirevalue::register_type!(CastMessage);

#[async_trait]
impl Handler<CastMessage> for CastActor {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            domain_id = %message.cast_domain_id.domain_id(),
        )
    )]
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: CastMessage,
    ) -> Result<(), anyhow::Error> {
        let lineage = ForwardLineage::from_message(&message);
        let domain = self
            .installed_hops
            .get(&message.cast_domain_id)
            .ok_or_else(|| anyhow::anyhow!("unknown domain {}", message.cast_domain_id))?;

        // Split reply ports so that downstream next hops reply through this
        // CastActor's local proxy ports instead of directly to the original
        // sender.
        let mut data = message.data.clone();
        split_ports(cx, &mut data, domain.next_hops.len(), true)?;

        let local_lineage = lineage.through(domain.base_rank_in_domain);

        // Deliver to destination actor.
        {
            let seq = *message
                .seqs
                .get_by_base_rank(domain.base_rank_in_domain)
                .ok_or_else(|| {
                    anyhow::anyhow!("missing seq for base rank {}", domain.base_rank_in_domain)
                })?;
            let mut headers = message.headers.clone();
            headers.set(CAST_POINT, domain.point_in_domain.clone());
            headers.set(CAST_ORIGINATING_SENDER, message.sender.clone());
            let seq_info = SeqInfo::Session {
                session_id: message.session_id,
                seq,
            };
            headers.set(SEQ_INFO, seq_info.clone());

            #[cfg(not(test))]
            let _ = &local_lineage;

            #[cfg(test)]
            headers.set(CAST_LINEAGE, local_lineage.ranks());

            let dest = domain
                .local_actor
                .port_addr(Port::handler_id(message.dest_port, None));
            hyperactor::mailbox::headers::stamp_sender_actor_id(
                &mut headers,
                &seq_info,
                &dest,
                &message.sender,
            );
            cx.post_with_external_seq_info(dest, headers, data.clone().into_message());
        }

        for next_hop in &domain.next_hops {
            #[cfg(not(test))]
            let _ = &local_lineage;
            let forward_headers = message.headers.clone();
            next_hop.port().post_with_headers(
                cx,
                forward_headers,
                CastMessage {
                    cast_domain_id: message.cast_domain_id.clone(),
                    sender: message.sender.clone(),
                    session_id: message.session_id,
                    seqs: message.seqs.clone(),
                    #[cfg(test)]
                    lineage: local_lineage.ranks(),
                    headers: message.headers.clone(),
                    dest_port: message.dest_port,
                    data: data.clone(),
                },
            );
        }

        Ok(())
    }
}

/// Internal command to destroy a casting domain and release its local routing state.
///
/// Sent to the entry-point [`CastActor`] by [`CastDomainRef::destroy`]. The
/// teardown propagates through the tree: each node removes its [`CastHop`] and
/// forwards [`DestroyCastDomain`] to its next hops. This is intentionally
/// idempotent and best-effort; unknown domains are ignored, and mailbox-level
/// undeliverable failures are returned to [`DestroyCastDomain::origin`].
#[derive(Debug, Serialize, Deserialize, typeuri::Named)]
struct DestroyCastDomain {
    /// The domain to tear down.
    domain_id: CastDomainId,
    /// Actor that initiated teardown and should receive undeliverable returns.
    origin: ActorAddr,
}
wirevalue::register_type!(DestroyCastDomain);

#[async_trait]
impl Handler<DestroyCastDomain> for CastActor {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(domain_id = %message.domain_id)
    )]
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: DestroyCastDomain,
    ) -> Result<(), anyhow::Error> {
        let Some(cast_hop) = self.installed_hops.remove(&message.domain_id) else {
            return Ok(());
        };

        #[cfg(test)]
        {
            tests::capture_destroyed_domain(cx, message.domain_id.domain_id());
        }

        for next_hop in &cast_hop.next_hops {
            next_hop.post(
                cx,
                DestroyCastDomain {
                    domain_id: message.domain_id.clone(),
                    origin: message.origin.clone(),
                },
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::collections::HashMap;
    use std::num::NonZeroUsize;
    use std::sync::Mutex;
    use std::sync::OnceLock;
    use std::time::Duration;

    use hyperactor::Client;
    use hyperactor::PortAddr;
    use hyperactor::ProcAddr;
    use hyperactor::channel::ChannelTransport;
    use hyperactor::proc::Proc;
    use ndslice::Shape;
    use ndslice::Slice;
    use ndslice::ViewExt;
    use ndslice::shape;
    use ndslice::view::BuildFromRegionIndexed;
    use ndslice::view::Ranked;
    use proptest::prelude::*;
    use timed_test::async_timed_test;
    use typeuri::Named;

    use super::*;

    fn small_shape_sizes() -> impl Strategy<Value = Vec<usize>> {
        prop::collection::vec(1usize..=4, 1..=4).prop_filter("shape must stay small", |sizes| {
            sizes.iter().product::<usize>() <= 64
        })
    }

    fn shape_from_sizes(sizes: &[usize]) -> Shape {
        Shape::new(
            (0..sizes.len()).map(|dim| format!("d{dim}")).collect(),
            Slice::new_row_major(sizes.to_vec()),
        )
        .unwrap()
    }

    fn member(rank: usize) -> ActorAddr {
        ActorAddr::root(
            format!("proc{rank}@inproc://{rank}")
                .parse::<ProcAddr>()
                .unwrap(),
            Label::strip("member"),
        )
    }

    fn validate_domain_tree(
        members: &HashMap<usize, ActorAddr>,
        tile: &MaterializedTile<ActorAddr>,
        seen_roots: &mut BTreeSet<usize>,
    ) -> Result<(), TestCaseError> {
        let expected_members = tile
            .tile()
            .space()
            .iter()
            .map(|rank| members[&rank].clone())
            .collect::<Vec<_>>();
        prop_assert_eq!(tile.items().cloned().collect::<Vec<_>>(), expected_members);
        prop_assert!(seen_roots.insert(tile.root_rank()));

        for child in next_tiles(TilingPolicy::BlockPartitioning, tile) {
            validate_domain_tree(members, &child, seen_roots)?;
        }

        Ok(())
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub(crate) struct CastHopSnapshot {
        point_in_domain: Point,
        base_rank_in_domain: usize,
        next_hop_procs: BTreeSet<String>,
        local_actor_proc: String,
    }

    static INSTALLED_DOMAINS: OnceLock<Mutex<HashMap<Uid, BTreeMap<String, CastHopSnapshot>>>> =
        OnceLock::new();
    static DESTROYED_DOMAINS: OnceLock<Mutex<HashMap<Uid, BTreeSet<String>>>> = OnceLock::new();

    fn installed_domains() -> &'static Mutex<HashMap<Uid, BTreeMap<String, CastHopSnapshot>>> {
        INSTALLED_DOMAINS.get_or_init(|| Mutex::new(HashMap::new()))
    }

    fn destroyed_domains() -> &'static Mutex<HashMap<Uid, BTreeSet<String>>> {
        DESTROYED_DOMAINS.get_or_init(|| Mutex::new(HashMap::new()))
    }

    pub(crate) fn capture_installed_domain(
        cx: &Context<'_, CastActor>,
        domain_id: &Uid,
        cast_hop: &CastHop,
    ) {
        let proc_name = cx.self_addr().proc_addr().log_name().to_string();
        let snapshot = CastHopSnapshot {
            point_in_domain: cast_hop.point_in_domain.clone(),
            base_rank_in_domain: cast_hop.base_rank_in_domain,
            next_hop_procs: cast_hop
                .next_hops
                .iter()
                .map(|next_hop| next_hop.actor_addr().proc_addr().log_name().to_string())
                .collect(),
            local_actor_proc: cast_hop.local_actor.proc_addr().log_name().to_string(),
        };
        installed_domains()
            .lock()
            .unwrap()
            .entry(domain_id.clone())
            .or_default()
            .insert(proc_name, snapshot);
    }

    pub(crate) fn capture_destroyed_domain(cx: &Context<'_, CastActor>, domain_id: &Uid) {
        let proc_name = cx.self_addr().proc_addr().log_name().to_string();
        destroyed_domains()
            .lock()
            .unwrap()
            .entry(domain_id.clone())
            .or_default()
            .insert(proc_name);
    }

    fn clear_captured_domains() {
        installed_domains().lock().unwrap().clear();
        destroyed_domains().lock().unwrap().clear();
    }

    fn captured_domain_snapshots(domain_id: &Uid) -> BTreeMap<String, CastHopSnapshot> {
        installed_domains()
            .lock()
            .unwrap()
            .get(domain_id)
            .cloned()
            .unwrap_or_default()
    }

    fn destroyed_domain_snapshots(domain_id: &Uid) -> BTreeSet<String> {
        destroyed_domains()
            .lock()
            .unwrap()
            .get(domain_id)
            .cloned()
            .unwrap_or_default()
    }

    fn members(count: usize) -> Vec<ActorAddr> {
        (0..count)
            .map(|i| {
                ActorAddr::root(
                    format!("proc{i}@inproc://{i}").parse::<ProcAddr>().unwrap(),
                    Label::strip("member"),
                )
            })
            .collect()
    }

    #[test]
    fn test_subset_members_preserves_selected_member_order() {
        // parent local ranks / members:
        //
        // row=0: 0  1      members[0] members[1]
        // row=1: 2  3      members[2] members[3]
        // row=2: 4  5      members[4] members[5]
        // row=3: 6  7      members[6] members[7]
        let parent_view = Region::from(shape!(row = 4, col = 2));

        // selected region = row 1..3
        let child_view = parent_view
            .range("row", ndslice::Range(1, Some(3), 1))
            .unwrap();
        let members = members(8);
        let parent_members = ValueMesh::new(parent_view.clone(), members.clone()).unwrap();

        let child_members = parent_members.subset(child_view.clone()).unwrap();
        let child_members = (0..Ranked::region(&child_members).num_ranks())
            .map(|rank| Ranked::get(&child_members, rank).unwrap().clone())
            .collect::<Vec<_>>();

        // child local ranks -> parent/base ranks:
        // row=0: 0->2  1->3
        // row=1: 2->4  3->5
        assert_eq!(child_members, &members[2..6]);
    }

    // -- Integration test infrastructure --

    /// A simple castable message type for testing delivery.
    #[derive(Debug, Clone, Serialize, Deserialize, typeuri::Named)]
    struct TestDelivery {
        payload: String,
    }
    wirevalue::register_type!(TestDelivery);

    /// Delivery record kept by test receivers in handler execution order.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, typeuri::Named)]
    struct TestDeliveryRecord {
        payload: String,
        lineage: Vec<usize>,
        operation_endpoint: Option<String>,
    }
    wirevalue::register_type!(TestDeliveryRecord);

    #[derive(
        Debug,
        Clone,
        Default,
        PartialEq,
        Eq,
        Serialize,
        Deserialize,
        typeuri::Named
    )]
    struct TestDeliveryHistories {
        by_proc: BTreeMap<String, Vec<TestDeliveryRecord>>,
    }
    wirevalue::register_type!(TestDeliveryHistories);

    impl TestDeliveryHistories {
        fn single(proc_name: String, deliveries: Vec<TestDeliveryRecord>) -> Self {
            Self {
                by_proc: [(proc_name, deliveries)].into_iter().collect(),
            }
        }

        fn merge(&mut self, other: Self) -> anyhow::Result<()> {
            for (proc_name, history) in other.by_proc {
                anyhow::ensure!(
                    self.by_proc.insert(proc_name.clone(), history).is_none(),
                    "duplicate history reply from {proc_name}",
                );
            }
            Ok(())
        }
    }

    #[derive(Debug, Serialize, Deserialize, typeuri::Named)]
    struct GetHistory {
        reply_to: hyperactor::OncePortRef<TestDeliveryHistories>,
    }
    wirevalue::register_type!(GetHistory);

    /// An actor that records delivered messages in local handler order.
    #[derive(Debug, Default)]
    #[hyperactor::export(
        handlers = [
            TestDelivery { cast = true },
            GetHistory { cast = true },
        ],
    )]
    struct TestReceiver {
        deliveries: Vec<TestDeliveryRecord>,
    }

    #[async_trait]
    impl Actor for TestReceiver {
        async fn init(&mut self, _this: &Instance<Self>) -> Result<(), anyhow::Error> {
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<TestDelivery> for TestReceiver {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            msg: TestDelivery,
        ) -> Result<(), anyhow::Error> {
            let _seq_info = cx
                .headers()
                .get(SEQ_INFO)
                .expect("cast delivery should stamp SEQ_INFO");
            let lineage = cx.headers().get(CAST_LINEAGE).unwrap_or_default();
            let operation_endpoint = cx
                .headers()
                .get(hyperactor::mailbox::headers::OPERATION_ENDPOINT);
            self.deliveries.push(TestDeliveryRecord {
                payload: msg.payload,
                lineage,
                operation_endpoint,
            });
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<GetHistory> for TestReceiver {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            msg: GetHistory,
        ) -> Result<(), anyhow::Error> {
            msg.reply_to.post(
                cx,
                TestDeliveryHistories::single(
                    cx.self_addr().proc_addr().log_name().to_string(),
                    self.deliveries.clone(),
                ),
            );
            Ok(())
        }
    }

    #[derive(typeuri::Named)]
    struct TestDeliveryHistoriesReducer;

    impl hyperactor::accum::CommReducer for TestDeliveryHistoriesReducer {
        type Update = TestDeliveryHistories;

        fn reduce(
            &self,
            mut left: Self::Update,
            right: Self::Update,
        ) -> anyhow::Result<Self::Update> {
            left.merge(right)?;
            Ok(left)
        }
    }

    inventory::submit! {
        hyperactor::accum::ReducerFactory {
            typehash_f: <TestDeliveryHistoriesReducer as Named>::typehash,
            builder_f: |_| Ok(Box::new(TestDeliveryHistoriesReducer)),
        }
    }

    struct TestDeliveryHistoriesAccumulator;

    impl hyperactor::accum::Accumulator for TestDeliveryHistoriesAccumulator {
        type State = TestDeliveryHistories;
        type Update = TestDeliveryHistories;

        fn accumulate(&self, state: &mut Self::State, update: Self::Update) -> anyhow::Result<()> {
            state.merge(update)
        }

        fn reducer_spec(&self) -> Option<hyperactor::accum::ReducerSpec> {
            Some(hyperactor::accum::ReducerSpec {
                typehash: <TestDeliveryHistoriesReducer as Named>::typehash(),
                builder_params: None,
            })
        }
    }

    struct CastTestMesh {
        _client_proc: Proc,
        client: Client,
        _procs: Vec<Proc>,
        member_ids: HashMap<usize, ActorAddr>,
        receiver_ids: Vec<ActorAddr>,
    }

    impl CastTestMesh {
        fn new(n: usize) -> Self {
            let client_proc =
                Proc::direct(ChannelTransport::Unix.any(), "client_proc".into()).unwrap();
            let client = client_proc.client("client");

            let procs: Vec<Proc> = (0..n)
                .map(|i| {
                    let proc =
                        Proc::direct(ChannelTransport::Unix.any(), format!("proc_{i}")).unwrap();
                    let cast_handle = proc
                        .spawn_with_uid(
                            Uid::singleton(Label::strip(CAST_ACTOR_NAME)),
                            CastActor::default(),
                        )
                        .unwrap();
                    let _: ActorRef<CastActor> = cast_handle.bind::<CastActor>();
                    proc
                })
                .collect();
            let member_ids = procs
                .iter()
                .enumerate()
                .map(|(rank, proc)| {
                    (
                        rank,
                        ActorAddr::root(proc.proc_addr().clone(), Label::strip("member")),
                    )
                })
                .collect();

            Self {
                _client_proc: client_proc,
                client,
                _procs: procs,
                member_ids,
                receiver_ids: Vec::new(),
            }
        }

        fn spawn_delivery_receivers(&mut self) {
            self.receiver_ids = self
                ._procs
                .iter()
                .map(|proc| {
                    let recv_handle = proc
                        .spawn_with_uid(
                            Uid::singleton(Label::strip("receiver")),
                            TestReceiver::default(),
                        )
                        .unwrap();
                    let _: ActorRef<TestReceiver> = recv_handle.bind::<TestReceiver>();
                    ActorAddr::root(proc.proc_addr().clone(), Label::strip("receiver"))
                })
                .collect();
        }

        fn spawn_split_port_receivers(&mut self) {
            self.receiver_ids = self
                ._procs
                .iter()
                .map(|proc| {
                    let recv_handle = proc
                        .spawn_with_uid(Uid::singleton(Label::strip("receiver")), SplitPortReceiver)
                        .unwrap();
                    let _: ActorRef<SplitPortReceiver> = recv_handle.bind::<SplitPortReceiver>();
                    ActorAddr::root(proc.proc_addr().clone(), Label::strip("receiver"))
                })
                .collect();
        }

        fn domain_members(&self) -> HashMap<usize, ActorAddr> {
            if self.receiver_ids.is_empty() {
                self.member_ids.clone()
            } else {
                self.receiver_ids
                    .iter()
                    .cloned()
                    .enumerate()
                    .collect::<HashMap<_, _>>()
            }
        }

        fn root_domain(&self, region: Region) -> CastDomainRef {
            CastDomainId::new()
                .materialize(
                    &self.client,
                    self.domain_members(),
                    region,
                    TilingPolicy::BlockPartitioning,
                )
                .unwrap()
        }

        fn proc_names(&self) -> Vec<String> {
            (0..self.domain_members().len())
                .map(|i| format!("proc_{i}"))
                .collect()
        }

        fn root_domain_with_policy(&self, region: Region, policy: TilingPolicy) -> CastDomainRef {
            CastDomainId::new()
                .materialize(&self.client, self.member_ids.clone(), region, policy)
                .unwrap()
        }

        async fn wait_for_domain_snapshots(
            &self,
            domain_id: &Uid,
            expected_count: usize,
        ) -> BTreeMap<String, CastHopSnapshot> {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let snapshots = captured_domain_snapshots(domain_id);
                    if snapshots.len() == expected_count {
                        return snapshots;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "timed out waiting for {expected_count} installed domain snapshots; saw {:?}",
                    captured_domain_snapshots(domain_id).keys()
                )
            })
        }

        async fn wait_for_destroyed_domain_snapshots(
            &self,
            domain_id: &Uid,
            expected_count: usize,
        ) -> BTreeSet<String> {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let snapshots = destroyed_domain_snapshots(domain_id);
                    if snapshots.len() == expected_count {
                        return snapshots;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "timed out waiting for {expected_count} destroyed domain snapshots; saw {:?}",
                    destroyed_domain_snapshots(domain_id)
                )
            })
        }
    }

    proptest! {
        #[test]
        fn prop_domain_tree_materialization_covers_each_rank_once(sizes in small_shape_sizes()) {
            let shape = shape_from_sizes(&sizes);
            let region = Region::from(shape);
            let members = region
                .slice()
                .iter()
                .map(|rank| (rank, member(rank)))
                .collect::<HashMap<_, _>>();
            let root_tile = MaterializedTile::from_value_mesh_with_tile(
                Tile::from_view(&region),
                Arc::new(ValueMesh::build_indexed(region.clone(), members.clone()).unwrap()),
            );

            let mut seen_roots = BTreeSet::new();
            validate_domain_tree(&members, &root_tile, &mut seen_roots)?;

            let expected_roots = region.slice().iter().collect::<BTreeSet<_>>();
            prop_assert_eq!(seen_roots, expected_roots);
        }

        #[test]
        fn prop_domain_destinations_preserve_member_mapping(sizes in small_shape_sizes()) {
            let shape = shape_from_sizes(&sizes);
            let region = Region::from(shape);
            let members = region
                .slice()
                .iter()
                .map(|rank| (rank, member(rank)))
                .collect::<HashMap<_, _>>();
            let root = MaterializedTile::from_value_mesh_with_tile(
                Tile::from_view(&region),
                Arc::new(ValueMesh::build_indexed(region.clone(), members.clone()).unwrap()),
            );
            let mut seen_roots = BTreeSet::new();

            validate_domain_tree(&members, &root, &mut seen_roots)?;

            prop_assert_eq!(
                seen_roots.into_iter().collect::<Vec<_>>(),
                region.slice().iter().collect::<Vec<_>>(),
            );
        }
    }

    async fn cast_and_collect_histories(
        test_mesh: &CastTestMesh,
        cast_domain: &CastDomainRef,
    ) -> BTreeMap<String, Vec<TestDeliveryRecord>> {
        let (reply_handle, reply_rx) = context::Mailbox::mailbox(&test_mesh.client)
            .open_reduce_port(TestDeliveryHistoriesAccumulator);
        let reply_ref = reply_handle.bind();

        cast_domain
            .cast(
                &test_mesh.client,
                Flattrs::new(),
                GetHistory {
                    reply_to: reply_ref,
                },
            )
            .unwrap();

        match tokio::time::timeout(Duration::from_secs(5), reply_rx.recv()).await {
            Ok(Ok(histories)) => histories.by_proc,
            Ok(Err(e)) => panic!("history recv error: {e}"),
            Err(_) => panic!("timed out waiting for reduced histories"),
        }
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_create_cast_domain_installs_expected_hops() {
        clear_captured_domains();

        let test_mesh = CastTestMesh::new(8);
        let root_domain = test_mesh.root_domain(shape!(a = 2, b = 2, c = 2).into());
        let snapshots = test_mesh
            .wait_for_domain_snapshots(root_domain.domain_id(), 8)
            .await;

        let region = Region::from(shape!(a = 2, b = 2, c = 2));
        let expected_next_hops: BTreeMap<String, BTreeSet<String>> = [
            ("proc_0", vec!["proc_1", "proc_2", "proc_4"]),
            ("proc_1", vec![]),
            ("proc_2", vec!["proc_3"]),
            ("proc_3", vec![]),
            ("proc_4", vec!["proc_5", "proc_6"]),
            ("proc_5", vec![]),
            ("proc_6", vec!["proc_7"]),
            ("proc_7", vec![]),
        ]
        .into_iter()
        .map(|(proc_name, next_hops)| {
            (
                proc_name.to_string(),
                next_hops
                    .into_iter()
                    .map(str::to_string)
                    .collect::<BTreeSet<_>>(),
            )
        })
        .collect();

        assert_eq!(
            snapshots.keys().cloned().collect::<BTreeSet<_>>(),
            (0..8).map(|rank| format!("proc_{rank}")).collect()
        );

        for rank in 0..8 {
            let proc_name = format!("proc_{rank}");
            let snapshot = snapshots
                .get(&proc_name)
                .unwrap_or_else(|| panic!("missing snapshot for {proc_name}"));

            assert_eq!(snapshot.base_rank_in_domain, rank);
            assert_eq!(
                snapshot.point_in_domain,
                region.point_of_base_rank(rank).unwrap()
            );
            assert_eq!(snapshot.local_actor_proc, proc_name);
            assert_eq!(snapshot.next_hop_procs, expected_next_hops[&proc_name]);
        }
    }

    fn expected_reply_counts(proc_names: &[&str]) -> BTreeMap<String, u64> {
        proc_names
            .iter()
            .map(|proc_name| (proc_name.to_string(), 1))
            .collect()
    }

    async fn cast_and_collect_reply_counts(
        test_mesh: &CastTestMesh,
        cast_domain: &CastDomainRef,
        payload: &str,
    ) -> BTreeMap<String, u64> {
        let (reply_handle, reply_rx) = context::Mailbox::mailbox(&test_mesh.client)
            .open_reduce_port(TestReplyCountsAccumulator);
        let reply_ref = reply_handle.bind();

        cast_domain
            .cast(
                &test_mesh.client,
                Flattrs::new(),
                TestRequestWithReply {
                    payload: payload.to_string(),
                    reply_to: reply_ref,
                },
            )
            .unwrap();

        match tokio::time::timeout(Duration::from_secs(5), reply_rx.recv()).await {
            Ok(Ok(reply_counts)) => reply_counts.counts_by_proc,
            Ok(Err(e)) => panic!("reply recv error: {e}"),
            Err(_) => panic!("timed out waiting for reduced replies"),
        }
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_cast_message_delivery_8_procs() {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(
            hyperactor::config::ENABLE_DEST_ACTOR_REORDERING_BUFFER,
            true,
        );

        let n = 8;
        let mut test_mesh = CastTestMesh::new(n);
        test_mesh.spawn_delivery_receivers();
        let root_domain = test_mesh.root_domain(shape!(a = 2, b = 2, c = 2).into());

        let expected_payloads = vec![
            "hello-0".to_string(),
            "hello-1".to_string(),
            "hello-2".to_string(),
        ];
        for payload in &expected_payloads {
            root_domain
                .cast(
                    &test_mesh.client,
                    Flattrs::new(),
                    TestDelivery {
                        payload: payload.clone(),
                    },
                )
                .unwrap();
        }

        let expected_histories: BTreeMap<String, Vec<String>> = test_mesh
            .proc_names()
            .into_iter()
            .map(|proc_name| (proc_name, expected_payloads.clone()))
            .collect();
        let histories = cast_and_collect_histories(&test_mesh, &root_domain).await;

        let observed_payloads: BTreeMap<String, Vec<String>> = histories
            .iter()
            .map(|(proc_name, history)| {
                (
                    proc_name.clone(),
                    history
                        .iter()
                        .map(|delivery| delivery.payload.clone())
                        .collect(),
                )
            })
            .collect();
        assert_eq!(observed_payloads, expected_histories);

        let mut lineage_by_proc = BTreeMap::new();
        for (proc_name, history) in histories {
            assert_eq!(
                history.len(),
                expected_payloads.len(),
                "proc {proc_name} received the wrong number of deliveries"
            );
            let first_lineage = history[0].lineage.clone();
            for delivery in &history {
                assert_eq!(
                    delivery.lineage, first_lineage,
                    "proc {proc_name} changed lineage across root casts"
                );
            }
            lineage_by_proc.insert(proc_name, first_lineage);
        }

        let expected_lineage: BTreeMap<String, Vec<usize>> = [
            ("proc_0".to_string(), vec![0]),
            ("proc_1".to_string(), vec![0, 1]),
            ("proc_2".to_string(), vec![0, 2]),
            ("proc_3".to_string(), vec![0, 2, 3]),
            ("proc_4".to_string(), vec![0, 4]),
            ("proc_5".to_string(), vec![0, 4, 5]),
            ("proc_6".to_string(), vec![0, 4, 6]),
            ("proc_7".to_string(), vec![0, 4, 6, 7]),
        ]
        .into_iter()
        .collect();
        assert_eq!(lineage_by_proc, expected_lineage);
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_cast_preserves_supplied_operation_context_headers() {
        let n = 2;
        let mut test_mesh = CastTestMesh::new(n);
        test_mesh.spawn_delivery_receivers();
        let root_domain = test_mesh.root_domain(shape!(rank = 2).into());

        let mut headers = Flattrs::new();
        headers.set(
            hyperactor::mailbox::headers::OPERATION_ENDPOINT,
            "endpoint.call()".to_string(),
        );
        root_domain
            .cast(
                &test_mesh.client,
                headers,
                TestDelivery {
                    payload: "with-operation-context".to_string(),
                },
            )
            .unwrap();

        let histories = cast_and_collect_histories(&test_mesh, &root_domain).await;
        for history in histories.values() {
            assert_eq!(
                history[0].operation_endpoint.as_deref(),
                Some("endpoint.call()")
            );
        }
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_destroy_domain_rejects_later_casts() {
        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client_proc".into()).unwrap();
        let client = client_proc.client("client");
        let cast_proc = Proc::direct(ChannelTransport::Unix.any(), "cast_proc".into()).unwrap();
        let actor_instance = cast_proc.actor_instance::<CastActor>("cast").unwrap();
        let mut cast_actor = CastActor::default();
        let cx = Context::new(&actor_instance.instance, Flattrs::new());

        let receiver_id = ActorAddr::root(cast_proc.proc_addr().clone(), Label::strip("receiver"));
        let cast_domain_id = CastDomainId::new();
        let region = Region::from(Shape::unity());
        let root_tile = MaterializedTile::from_value_mesh_with_tile(
            Tile::from_view(&region),
            Arc::new(ValueMesh::new(region.clone(), vec![receiver_id]).unwrap()),
        );

        Handler::<CreateCastDomain>::handle(
            &mut cast_actor,
            &cx,
            CreateCastDomain {
                cast_domain_id: cast_domain_id.clone(),
                region,
                tiling_policy: TilingPolicy::BlockPartitioning,
                tile: root_tile,
            },
        )
        .await
        .unwrap();

        Handler::<DestroyCastDomain>::handle(
            &mut cast_actor,
            &cx,
            DestroyCastDomain {
                domain_id: cast_domain_id.clone(),
                origin: client.self_addr().clone(),
            },
        )
        .await
        .unwrap();

        // Destroy is idempotent: a repeated teardown is a benign no-op.
        Handler::<DestroyCastDomain>::handle(
            &mut cast_actor,
            &cx,
            DestroyCastDomain {
                domain_id: cast_domain_id.clone(),
                origin: client.self_addr().clone(),
            },
        )
        .await
        .unwrap();

        let err = Handler::<CastMessage>::handle(
            &mut cast_actor,
            &cx,
            CastMessage {
                cast_domain_id,
                sender: client.self_addr().clone(),
                session_id: client.sequencer().session_id(),
                seqs: ValueMesh::new(Region::from(Shape::unity()), vec![1]).unwrap(),
                lineage: Vec::new(),
                headers: Flattrs::new(),
                dest_port: TestDelivery::port(),
                data: ErasedUnbound::try_from_message(TestDelivery {
                    payload: "hello".to_string(),
                })
                .unwrap(),
            },
        )
        .await
        .unwrap_err();

        let err = err.to_string();
        assert!(err.contains("unknown domain"), "unexpected error: {err}");
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_destroy_domain_propagates_to_all_hops() {
        clear_captured_domains();

        let test_mesh = CastTestMesh::new(8);
        let root_domain = test_mesh.root_domain(shape!(a = 2, b = 2, c = 2).into());
        let domain_id = root_domain.domain_id().clone();
        test_mesh.wait_for_domain_snapshots(&domain_id, 8).await;

        root_domain.destroy(&test_mesh.client);

        let destroyed = test_mesh
            .wait_for_destroyed_domain_snapshots(&domain_id, 8)
            .await;
        assert_eq!(
            destroyed,
            (0..8).map(|rank| format!("proc_{rank}")).collect()
        );
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_slice_cast_reaches_exactly_selected_members() {
        let n = 8;
        let mut test_mesh = CastTestMesh::new(n);
        test_mesh.spawn_split_port_receivers();
        let root_cast_domain = test_mesh.root_domain(shape!(a = 2, b = 2, c = 2).into());

        for (case_name, range, expected_procs) in [
            (
                "a_0_to_1",
                ndslice::Range(0, Some(1), 1),
                ["proc_0", "proc_1", "proc_2", "proc_3"],
            ),
            (
                "a_1_to_2",
                ndslice::Range(1, Some(2), 1),
                ["proc_4", "proc_5", "proc_6", "proc_7"],
            ),
        ] {
            let payload = format!("slice-{case_name}");
            let slice_region = Region::from(shape!(a = 2, b = 2, c = 2))
                .range("a", range)
                .unwrap();
            let slice_cast_domain = root_cast_domain
                .materialize_slice(
                    &test_mesh.client,
                    slice_region,
                    TilingPolicy::BlockPartitioning,
                )
                .unwrap();

            assert_eq!(
                cast_and_collect_reply_counts(&test_mesh, &slice_cast_domain, &payload,).await,
                expected_reply_counts(&expected_procs)
            );
        }
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_sender_sequenced_casts_are_observed_in_projection_order() {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(
            hyperactor::config::ENABLE_DEST_ACTOR_REORDERING_BUFFER,
            true,
        );

        let n = 4;
        let mut test_mesh = CastTestMesh::new(n);
        test_mesh.spawn_delivery_receivers();
        let root = test_mesh.root_domain(shape!(rank = 4).into());

        let rank_0_to_2 = root
            .materialize_slice(
                &test_mesh.client,
                Region::from(shape!(rank = 4))
                    .range("rank", ndslice::Range(0, Some(2), 1))
                    .unwrap(),
                TilingPolicy::BlockPartitioning,
            )
            .unwrap();

        let rank_1_to_3 = root
            .materialize_slice(
                &test_mesh.client,
                Region::from(shape!(rank = 4))
                    .range("rank", ndslice::Range(1, Some(3), 1))
                    .unwrap(),
                TilingPolicy::BlockPartitioning,
            )
            .unwrap();

        let rank_2_to_4 = root
            .materialize_slice(
                &test_mesh.client,
                Region::from(shape!(rank = 4))
                    .range("rank", ndslice::Range(2, Some(4), 1))
                    .unwrap(),
                TilingPolicy::BlockPartitioning,
            )
            .unwrap();

        let casts = [
            (
                &root,
                "root-0",
                vec!["proc_0", "proc_1", "proc_2", "proc_3"],
            ),
            (&rank_0_to_2, "rank_0_to_2-1", vec!["proc_0", "proc_1"]),
            (&rank_1_to_3, "rank_1_to_3-2", vec!["proc_1", "proc_2"]),
            (&rank_2_to_4, "rank_2_to_4-3", vec!["proc_2", "proc_3"]),
            (
                &root,
                "root-4",
                vec!["proc_0", "proc_1", "proc_2", "proc_3"],
            ),
        ];

        let mut expected_histories: BTreeMap<String, Vec<String>> = test_mesh
            .proc_names()
            .into_iter()
            .map(|proc_name| (proc_name, Vec::new()))
            .collect();

        for (domain, payload, expected_receivers) in &casts {
            domain
                .cast(
                    &test_mesh.client,
                    Flattrs::new(),
                    TestDelivery {
                        payload: payload.to_string(),
                    },
                )
                .unwrap();

            for proc_name in expected_receivers {
                expected_histories
                    .get_mut(*proc_name)
                    .unwrap()
                    .push(payload.to_string());
            }
        }

        let observed_histories: BTreeMap<String, Vec<String>> =
            cast_and_collect_histories(&test_mesh, &root)
                .await
                .into_iter()
                .map(|(proc_name, history)| {
                    (
                        proc_name,
                        history
                            .into_iter()
                            .map(|delivery| delivery.payload)
                            .collect(),
                    )
                })
                .collect();

        assert_eq!(observed_histories, expected_histories);
    }

    // -- Port splitting test infrastructure --
    //
    // Ported from `hyperactor_mesh::comm::tests`.  The `split_ports`
    // function records (original, split, deliver_here) edges into a
    // global vec under `#[cfg(test)]`.  After a cast we reconstruct
    // the split-port tree and verify it mirrors the cast tree.

    #[derive(Debug, Clone)]
    struct SplitEdge {
        from: PortAddr,
        to: PortAddr,
        is_leaf: bool,
    }

    struct SplitPortRecording {
        root: PortAddr,
        edges: Vec<SplitEdge>,
    }

    struct SplitPortRecordingGuard;

    static SPLIT_PORT_TREE: OnceLock<Mutex<Option<SplitPortRecording>>> = OnceLock::new();

    fn split_port_tree() -> &'static Mutex<Option<SplitPortRecording>> {
        SPLIT_PORT_TREE.get_or_init(|| Mutex::new(None))
    }

    pub(crate) fn collect_split_port(original: &PortAddr, split: &PortAddr, deliver_here: bool) {
        let mut guard = split_port_tree().lock().unwrap();
        let Some(recording) = guard.as_mut() else {
            return;
        };
        if original != &recording.root && !recording.edges.iter().any(|edge| &edge.to == original) {
            return;
        }
        recording.edges.push(SplitEdge {
            from: original.clone(),
            to: split.clone(),
            is_leaf: deliver_here,
        });
    }

    fn record_split_port_tree(root: PortAddr) -> SplitPortRecordingGuard {
        *split_port_tree().lock().unwrap() = Some(SplitPortRecording {
            root,
            edges: Vec::new(),
        });
        SplitPortRecordingGuard
    }

    impl SplitPortRecordingGuard {
        fn edges(&self) -> Vec<SplitEdge> {
            split_port_tree()
                .lock()
                .unwrap()
                .as_ref()
                .map(|recording| recording.edges.clone())
                .unwrap_or_default()
        }
    }

    impl Drop for SplitPortRecordingGuard {
        fn drop(&mut self) {
            *split_port_tree().lock().unwrap() = None;
        }
    }

    /// Reconstruct split-port paths from leaf to root.
    /// Returns a map from leaf `PortAddr` to the root-first split path.
    fn build_split_paths(edges: &[SplitEdge]) -> BTreeMap<PortAddr, Vec<PortAddr>> {
        let mut child_to_parent: HashMap<PortAddr, PortAddr> = HashMap::new();
        let mut leaves = Vec::new();

        for edge in edges {
            child_to_parent.insert(edge.to.clone(), edge.from.clone());
            if edge.is_leaf {
                leaves.push(edge.to.clone());
            }
        }

        let mut result = BTreeMap::new();
        for leaf in leaves {
            let mut path = vec![leaf.clone()];
            let mut current = leaf.clone();
            while let Some(parent) = child_to_parent.get(&current) {
                path.push(parent.clone());
                current = parent.clone();
            }
            path.reverse();
            result.insert(leaf, path);
        }
        result
    }

    /// Extract the proc name (rank) from each `PortAddr` in a split-port
    /// path, stripping the root (client) entry.
    fn split_path_ranks(
        paths: &BTreeMap<PortAddr, Vec<PortAddr>>,
        rank_lookup: &HashMap<String, usize>,
    ) -> BTreeMap<usize, Vec<usize>> {
        paths
            .iter()
            .map(|(leaf, path)| {
                // First entry is the client's port — skip it.
                let ranks: Vec<usize> = path[1..]
                    .iter()
                    .map(|pid| {
                        let proc_name = pid.actor_addr().proc_addr().log_name().to_string();
                        *rank_lookup
                            .get(&proc_name)
                            .unwrap_or_else(|| panic!("unknown proc {proc_name} in split path"))
                    })
                    .collect();
                let leaf_proc = leaf.actor_addr().proc_addr().log_name().to_string();
                let leaf_rank = rank_lookup[&leaf_proc];
                (leaf_rank, ranks)
            })
            .collect()
    }

    /// A reply type for the port splitting test.
    #[derive(
        Debug,
        Clone,
        Default,
        PartialEq,
        Eq,
        Serialize,
        Deserialize,
        typeuri::Named
    )]
    struct TestReplyCounts {
        counts_by_proc: BTreeMap<String, u64>,
    }
    wirevalue::register_type!(TestReplyCounts);

    impl TestReplyCounts {
        fn single(proc_name: String) -> Self {
            Self {
                counts_by_proc: [(proc_name, 1)].into_iter().collect(),
            }
        }

        fn merge(&mut self, other: Self) {
            for (proc_name, count) in other.counts_by_proc {
                *self.counts_by_proc.entry(proc_name).or_default() += count;
            }
        }
    }

    #[derive(typeuri::Named)]
    struct TestReplyCountsReducer;

    impl hyperactor::accum::CommReducer for TestReplyCountsReducer {
        type Update = TestReplyCounts;

        fn reduce(
            &self,
            mut left: Self::Update,
            right: Self::Update,
        ) -> anyhow::Result<Self::Update> {
            left.merge(right);
            Ok(left)
        }
    }

    inventory::submit! {
        hyperactor::accum::ReducerFactory {
            typehash_f: <TestReplyCountsReducer as Named>::typehash,
            builder_f: |_| Ok(Box::new(TestReplyCountsReducer)),
        }
    }

    struct TestReplyCountsAccumulator;

    impl hyperactor::accum::Accumulator for TestReplyCountsAccumulator {
        type State = TestReplyCounts;
        type Update = TestReplyCounts;

        fn accumulate(&self, state: &mut Self::State, update: Self::Update) -> anyhow::Result<()> {
            state.merge(update);
            Ok(())
        }

        fn reducer_spec(&self) -> Option<hyperactor::accum::ReducerSpec> {
            Some(hyperactor::accum::ReducerSpec {
                typehash: <TestReplyCountsReducer as Named>::typehash(),
                builder_params: None,
            })
        }
    }

    /// A castable message with a reply port for testing port splitting.
    #[derive(Debug, Clone, Serialize, Deserialize, typeuri::Named)]
    struct TestRequestWithReply {
        payload: String,
        reply_to: hyperactor::OncePortRef<TestReplyCounts>,
    }
    wirevalue::register_type!(TestRequestWithReply);

    /// An actor that receives a cast message and sends a reply back
    /// through the (potentially split) reply port.
    #[derive(Debug, Default)]
    #[hyperactor::export(
        handlers = [TestRequestWithReply { cast = true }],
    )]
    struct SplitPortReceiver;

    #[async_trait]
    impl Actor for SplitPortReceiver {
        async fn init(&mut self, _this: &Instance<Self>) -> Result<(), anyhow::Error> {
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<TestRequestWithReply> for SplitPortReceiver {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            msg: TestRequestWithReply,
        ) -> Result<(), anyhow::Error> {
            msg.reply_to.post(
                cx,
                TestReplyCounts::single(cx.self_addr().proc_addr().log_name().to_string()),
            );
            Ok(())
        }
    }

    /// Verify that port splitting rewrites reply ports to mirror the
    /// cast tree, and that replies flow back through the split ports
    /// to the original sender.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_port_splitting_replies_and_tree() {
        let n = 8;
        let mut test_mesh = CastTestMesh::new(n);
        test_mesh.spawn_split_port_receivers();
        let root_domain = test_mesh.root_domain(shape!(a = 2, b = 2, c = 2).into());

        let (reply_handle, reply_rx) = context::Mailbox::mailbox(&test_mesh.client)
            .open_reduce_port(TestReplyCountsAccumulator);
        let reply_ref = reply_handle.bind();
        let split_port_recording = record_split_port_tree(reply_ref.port_addr().clone());

        // Cast a message with the reply port.
        root_domain
            .cast(
                &test_mesh.client,
                Flattrs::new(),
                TestRequestWithReply {
                    payload: "split_test".to_string(),
                    reply_to: reply_ref,
                },
            )
            .unwrap();

        let reply_counts = match tokio::time::timeout(Duration::from_secs(5), reply_rx.recv()).await
        {
            Ok(Ok(reply_counts)) => reply_counts.counts_by_proc,
            Ok(Err(e)) => panic!("reply recv error: {e}"),
            Err(_) => panic!("timed out waiting for reduced replies"),
        };

        // Every proc should have sent exactly one reply. Since counts are
        // preserved, duplicate deliveries show up as counts greater than one.
        let expected_counts: BTreeMap<String, u64> =
            (0..n).map(|i| (format!("proc_{i}"), 1)).collect();
        assert_eq!(reply_counts, expected_counts);

        // Verify the split-port tree mirrors the cast tree.
        let edges = split_port_recording.edges();
        let paths = build_split_paths(&edges);

        let rank_lookup: HashMap<String, usize> =
            (0..n).map(|i| (format!("proc_{i}"), i)).collect();
        let rank_paths = split_path_ranks(&paths, &rank_lookup);

        let expected: BTreeMap<usize, Vec<usize>> = [
            (0, vec![0]),
            (1, vec![0, 1]),
            (2, vec![0, 2]),
            (3, vec![0, 2, 3]),
            (4, vec![0, 4]),
            (5, vec![0, 4, 5]),
            (6, vec![0, 4, 6]),
            (7, vec![0, 4, 6, 7]),
        ]
        .into_iter()
        .collect();

        assert_eq!(
            rank_paths, expected,
            "split-port tree doesn't mirror cast tree"
        );
    }

    // Tests that a serialized BoundedFanout policy drives cast-domain setup
    // end to end. Each CastActor installs one CastHop; the expected_next_hops
    // map below is the adjacency-list representation of the send tree that
    // BoundedFanout should produce for an 8-rank 1D domain with fanout 2.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_create_cast_domain_with_bounded_fanout_installs_expected_hops() {
        clear_captured_domains();

        let test_mesh = CastTestMesh::new(8);
        let root_domain = test_mesh.root_domain_with_policy(
            shape!(a = 8).into(),
            TilingPolicy::BoundedFanout {
                fanout: NonZeroUsize::new(2).unwrap(),
            },
        );
        let snapshots = test_mesh
            .wait_for_domain_snapshots(root_domain.domain_id(), 8)
            .await;

        let region = Region::from(shape!(a = 8));
        // Hand-derived from a 1D extent-8 root with fanout=2: away-from-root
        // indices [1..8) split into two left-heavy groups [1..5) and [5..8);
        // each group recurses with the same policy.
        let expected_next_hops: BTreeMap<String, BTreeSet<String>> = [
            ("proc_0", vec!["proc_1", "proc_5"]),
            ("proc_1", vec!["proc_2", "proc_4"]),
            ("proc_2", vec!["proc_3"]),
            ("proc_3", vec![]),
            ("proc_4", vec![]),
            ("proc_5", vec!["proc_6", "proc_7"]),
            ("proc_6", vec![]),
            ("proc_7", vec![]),
        ]
        .into_iter()
        .map(|(proc_name, next_hops)| {
            (
                proc_name.to_string(),
                next_hops
                    .into_iter()
                    .map(str::to_string)
                    .collect::<BTreeSet<_>>(),
            )
        })
        .collect();

        assert_eq!(
            snapshots.keys().cloned().collect::<BTreeSet<_>>(),
            (0..8).map(|rank| format!("proc_{rank}")).collect()
        );

        for rank in 0..8 {
            let proc_name = format!("proc_{rank}");
            let snapshot = snapshots
                .get(&proc_name)
                .unwrap_or_else(|| panic!("missing snapshot for {proc_name}"));

            assert_eq!(snapshot.base_rank_in_domain, rank);
            assert_eq!(
                snapshot.point_in_domain,
                region.point_of_base_rank(rank).unwrap()
            );
            assert_eq!(snapshot.local_actor_proc, proc_name);
            assert_eq!(snapshot.next_hop_procs, expected_next_hops[&proc_name]);
        }
    }

    // Tests that a serialized Bisection policy drives cast-domain setup end
    // to end. Each CastActor installs one CastHop; the expected_next_hops
    // map below is the adjacency-list representation of the send tree that
    // Bisection should produce for an 8-rank 1D domain.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_create_cast_domain_with_bisection_installs_expected_hops() {
        clear_captured_domains();

        let test_mesh = CastTestMesh::new(8);
        let root_domain =
            test_mesh.root_domain_with_policy(shape!(a = 8).into(), TilingPolicy::Bisection);
        let snapshots = test_mesh
            .wait_for_domain_snapshots(root_domain.domain_id(), 8)
            .await;

        let region = Region::from(shape!(a = 8));
        // Hand-derived from recursive halving on a 1D extent-8 root: each
        // step emits an upper sibling and a lower anchor; anchor contraction
        // promotes sibling descendants from the lower-half spine.
        let expected_next_hops: BTreeMap<String, BTreeSet<String>> = [
            ("proc_0", vec!["proc_1", "proc_2", "proc_4"]),
            ("proc_1", vec![]),
            ("proc_2", vec!["proc_3"]),
            ("proc_3", vec![]),
            ("proc_4", vec!["proc_5", "proc_6"]),
            ("proc_5", vec![]),
            ("proc_6", vec!["proc_7"]),
            ("proc_7", vec![]),
        ]
        .into_iter()
        .map(|(proc_name, next_hops)| {
            (
                proc_name.to_string(),
                next_hops
                    .into_iter()
                    .map(str::to_string)
                    .collect::<BTreeSet<_>>(),
            )
        })
        .collect();

        assert_eq!(
            snapshots.keys().cloned().collect::<BTreeSet<_>>(),
            (0..8).map(|rank| format!("proc_{rank}")).collect()
        );

        for rank in 0..8 {
            let proc_name = format!("proc_{rank}");
            let snapshot = snapshots
                .get(&proc_name)
                .unwrap_or_else(|| panic!("missing snapshot for {proc_name}"));

            assert_eq!(snapshot.base_rank_in_domain, rank);
            assert_eq!(
                snapshot.point_in_domain,
                region.point_of_base_rank(rank).unwrap()
            );
            assert_eq!(snapshot.local_actor_proc, proc_name);
            assert_eq!(snapshot.next_hop_procs, expected_next_hops[&proc_name]);
        }
    }
}
