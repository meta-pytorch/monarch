/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The CastActor: a system actor that bootstraps and manages casting domains.
//!
//! ## Cast actor invariants (CA-*)
//!
//! - **CA-1 (direct-delivery equivalence):** The cast overlay is a transport
//!   optimization. It preserves the logical behavior of sending directly to
//!   every destination. It does not provide atomic fanout; each destination
//!   can fail independently.
//! - **CA-2 (domain coverage):** A domain's routing branches are nonempty,
//!   disjoint, and together cover the complete domain region. In the absence
//!   of delivery failures, each destination receives the message exactly once.
//! - **CA-3 (setup ordering):** Domain materialization sends setup before any
//!   cast from the returned handle. Correctness relies on Hyperactor preserving
//!   message order between each sender and relay entry point.
//! - **CA-4 (destination ordering):** The originating sender allocates one
//!   sequence number for each destination before fanout. Routing partitions
//!   these sequences but does not replace or reorder them.
//! - **CA-5 (route independence):** Route topology, including direct terminal
//!   delivery, must not change message headers, port behavior, or the logical
//!   result of a valid reducer.
//! - **CA-6 (domain isolation):** A `CastActor` can serve many domains, including
//!   domains with overlapping destination actors. Creating or destroying one
//!   domain must not change another domain.
//! - **CA-7 (domain lifecycle):** `CastDomainRef::destroy` is idempotent,
//!   best-effort routing-state cleanup, not destination revocation. A domain
//!   outlives its destination actors, callers do not cast after destruction,
//!   and CastActors retain no tombstones for destroyed domains.
//! - **CA-8 (failure containment):** Cast-message processing and delivery
//!   failures must not fail the shared `CastActor`. Failures are returned to
//!   the originating sender when the origin can be recovered.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;

use anyhow::Result;
use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorAddr;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::Endpoint as _;
use hyperactor::EndpointLocation;
use hyperactor::Handler;
use hyperactor::IdleFlushPortRefRepr;
use hyperactor::Instance;
use hyperactor::OncePortRefRepr;
use hyperactor::PortRef;
use hyperactor::PortRefRepr;
use hyperactor::ProcAddr;
use hyperactor::RemoteEndpoint as _;
use hyperactor::Uid;
use hyperactor::accum::ReducerMode;
use hyperactor::context;
use hyperactor::id::Label;
use hyperactor::mailbox::DeliveryFailure;
use hyperactor::mailbox::DeliveryFailureReport;
use hyperactor::mailbox::MailboxSender;
use hyperactor::mailbox::MessageEnvelope;
use hyperactor::mailbox::TransportFailure;
use hyperactor::mailbox::TransportFailureReason;
use hyperactor::mailbox::Undeliverable;
use hyperactor::mailbox::UndeliverableMailboxSender;
use hyperactor::mailbox::UndeliverableReason;
use hyperactor::mailbox::monitored_return_handle;
use hyperactor::ordering::SEQ_INFO;
use hyperactor::ordering::SeqInfo;
use hyperactor::ordering::SeqKey;
use hyperactor::port::Port;
use hyperactor::value_mesh::ValueMesh;
use hyperactor_config::Flattrs;
use hyperactor_config::NonZeroUsize as ConfigNonZeroUsize;
use ndslice::Point;
use ndslice::Region;
use ndslice::Slice;
use ndslice::view::RankedSliceable;
use ndslice::view::View;
use ndslice::view::ViewExt;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;
use uuid::Uuid;

use crate::tile::MaterializedTile;
use crate::tile::Tile;
pub use crate::tile::TilingPolicy;

const CAST_ACTOR_DIM: &str = "cast_actor";
const CAST_DESTINATION_DIM: &str = "cast_destination";

hyperactor_config::declare_attrs! {
    /// Point associated with a cast delivery or actor construction.
    ///
    /// Message headers carry the recipient's point in the current casting
    /// domain. An `ActorEnvironment` may carry an inherited construction point;
    /// it does not establish actor-mesh membership.
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
    pub attr CAST_LINEAGE: Vec<ActorAddr>;
}

/// Wire-compatible mirror of `hyperactor_mesh::resource::RankRepr`.
///
/// `hyperactor_cast` cannot depend on `hyperactor_mesh` without creating a
/// crate cycle, but cast delivery still needs to stamp the rank multipart part
/// for messages whose handlers read rank from the payload. The part is tagged
/// with `RankRepr`'s typename, so this mirror must match it exactly.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ResourceRankRepr(Option<usize>);

impl Named for ResourceRankRepr {
    fn typename() -> &'static str {
        "hyperactor_mesh::resource::RankRepr"
    }
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

    /// Materialize this domain id over concrete destinations and return an
    /// addressable domain handle.
    ///
    /// `destinations` and `host_cast_actors` use the same domain ranks. Tiling
    /// and communication are derived internally.
    pub fn materialize(
        self,
        cx: &impl context::Actor,
        destinations: Arc<ValueMesh<CastDestination>>,
        host_cast_actors: Arc<ValueMesh<ActorRef<CastActor>>>,
        tiling_policy: TilingPolicy,
        headers: Flattrs,
    ) -> anyhow::Result<CastDomainRef> {
        let nodes = Arc::new(cast_node_mesh(&destinations, &host_cast_actors)?);

        let root_tile = MaterializedTile::from_value_mesh_with_tile(
            Tile::from_view(&nodes.region()),
            Arc::clone(&nodes),
        );

        let destination_region = Region::new(
            vec![CAST_DESTINATION_DIM.to_string()],
            Slice::new_row_major(vec![destinations.region().num_ranks()]),
        );

        let root_region = nodes
            .region()
            .range(CAST_ACTOR_DIM, ndslice::Range(0, Some(1), 1))?;

        let root_only_tile = root_tile.subtile(Tile::from_view(&root_region));
        let child_tiles = std::iter::once(root_only_tile)
            .chain(next_tiles(tiling_policy, &root_tile))
            .collect::<Vec<_>>();

        let subtrees = child_tiles
            .iter()
            .map(|tile| CastSubtree::try_from_tile(&root_tile, &destination_region, tile))
            .collect::<Result<Vec<_>>>()?;

        for (subtree, tile) in subtrees.iter().zip(child_tiles) {
            subtree.cast_actor.port().post_with_headers(
                cx,
                headers.clone(),
                CreateCastDomain {
                    cast_domain_id: self.clone(),
                    tiling_policy,
                    tile,
                    served_region: subtree.served_region.clone(),
                },
            );
        }

        Ok(CastDomainRef {
            id: self,
            subtrees,
            sequencing: CastSequencing {
                nodes,
                region: destination_region,
                seq_keys: Arc::new(OnceLock::new()),
            },
        })
    }
}

impl std::fmt::Display for CastDomainId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.domain_id)
    }
}

/// Opaque local handle for initiating work against a materialized cast domain.
///
/// Unlike [`CastDomainId`], this includes the root-heaved relay branches used
/// to address the domain. Callers obtain this only by materializing a
/// [`CastDomainId`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CastDomainRef {
    id: CastDomainId,
    /// Independently seeded routes for initiating casts into subtrees.
    subtrees: Vec<CastSubtree>,
    /// State used to assign one sequence number to each destination.
    sequencing: CastSequencing,
}

/// State used to assign destination sequence numbers for each cast.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CastSequencing {
    /// One routing node for each host CastActor.
    nodes: Arc<ValueMesh<CastNode>>,
    /// One sequence slot for each destination, in host-node order.
    region: Region,
    /// Per-destination sequence keys in host-node order.
    #[serde(skip)]
    seq_keys: Arc<OnceLock<Arc<Vec<SeqKey>>>>,
}

/// One outgoing CastActor branch and the destination region it serves.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CastSubtree {
    cast_actor: ActorRef<CastActor>,
    served_region: Region,
}

impl CastSubtree {
    /// Build a relay branch and its served region from a child host tile.
    ///
    /// Input:
    ///
    /// ```text
    /// parent hosts: [host_0{a0,a1}, host_1{b0}, host_2{c0,c1}]
    /// parent served region: [a0 a1 b0 c0 c1]
    /// child tile:                       [host_1{b0}, host_2{c0,c1}]
    /// ```
    ///
    /// Output:
    ///
    /// ```text
    /// CastSubtree {
    ///   cast_actor: host_1::cast,
    ///   served_region: 2..5,
    /// }
    /// ```
    fn try_from_tile(
        parent_tile: &MaterializedTile<CastNode>,
        parent_served_region: &Region,
        tile: &MaterializedTile<CastNode>,
    ) -> anyhow::Result<Self> {
        let root_node = tile
            .root_item()
            .ok_or_else(|| anyhow::anyhow!("cast routing tile must contain a root node"))?;

        let child_root_rank = tile.root_rank();

        let destinations_before = parent_tile
            .tile()
            .ranks()
            .take_while(|rank| *rank != child_root_rank)
            .map(|rank| {
                parent_tile
                    .item_at(rank)
                    .expect("parent tile must contain each of its ranks")
                    .destinations
                    .len()
            })
            .sum::<usize>();

        let destination_count = tile
            .items()
            .map(|node| node.destinations.len())
            .sum::<usize>();

        let served_region = parent_served_region.range(
            CAST_DESTINATION_DIM,
            ndslice::Range(
                destinations_before,
                Some(destinations_before + destination_count),
                1,
            ),
        )?;

        Ok(Self {
            cast_actor: root_node.cast_actor.clone(),
            served_region,
        })
    }
}

impl CastDomainRef {
    /// The pure identifier for this domain.
    pub fn id(&self) -> CastDomainId {
        self.id.clone()
    }

    /// The domain's unique identifier.
    pub fn domain_id(&self) -> &Uid {
        self.id.domain_id()
    }

    /// Cast a message to all members of this domain with caller-supplied headers.
    ///
    /// `headers` are the destination envelope headers supplied by the caller.
    /// The cast layer stamps cast-owned fields on top before sending the
    /// [`CastMessage`] through each subtree route.
    pub fn cast<M: Serialize + Named>(
        &self,
        cx: &impl context::Actor,
        headers: Flattrs,
        message: M,
    ) -> anyhow::Result<()> {
        let mut data = wirevalue::Any::<wirevalue::encoding::Multipart>::serialize(&message)?;
        let sender = cx.mailbox().actor_addr().clone();
        let dest_port = M::port();
        let (session_id, seqs) = self.sequencing.seqs_for_cast(cx)?;

        let cast_headers = headers.clone();
        let subtree_seqs = self
            .subtrees
            .iter()
            .map(|subtree| seqs.sliced(subtree.served_region.clone()))
            .collect::<Vec<_>>();

        split_ports(
            cx,
            &mut data,
            SplitFanout {
                peer_count: self.subtrees.len(),
                num_destinations: seqs.region().num_ranks(),
            },
        )?;

        for (subtree, seqs) in self.subtrees.iter().zip(subtree_seqs) {
            subtree.cast_actor.port().post_with_headers(
                cx,
                headers.clone(),
                CastMessage {
                    cast_domain_id: self.id.clone(),
                    sender: sender.clone(),
                    session_id,
                    seqs,
                    #[cfg(test)]
                    lineage: Vec::new(),
                    headers: cast_headers.clone(),
                    dest_port,
                    data: data.clone(),
                },
            );
        }

        Ok(())
    }

    /// Release this cast domain after all destination actors have shut down.
    ///
    /// Before calling this method, the caller must ensure that every destination
    /// actor has reached a terminal state and cannot process further messages.
    /// Do not use `destroy` to prevent message delivery; shut down the destination
    /// actors first. Later casts may be rejected by the cast routing layer or by
    /// the stopped destination actors.
    ///
    /// This operation is best-effort and does not acknowledge completion.
    pub fn destroy(&self, cx: &impl context::Actor) {
        let origin = cx.mailbox().actor_addr().clone();
        for subtree in &self.subtrees {
            subtree.cast_actor.post(
                cx,
                DestroyCastDomain {
                    domain_id: self.id.clone(),
                    origin: origin.clone(),
                },
            );
        }
    }
}

impl CastSequencing {
    /// Allocate one normal sender-side sequence number per destination rank.
    ///
    /// This is the same model used by v1 `CommActor`: a complete `rank -> seq`
    /// snapshot is allocated once, then partitioned among region roots, so
    /// forwarding hops do not need route-local metadata to derive receiver
    /// ordering. `ValueMesh` preserves the domain rank space while allowing
    /// compact representations when seqs happen to be compressible.
    fn seqs_for_cast(&self, cx: &impl context::Actor) -> Result<(Uuid, ValueMesh<u64>)> {
        let sequencer = cx.instance().sequencer();

        Ok((
            sequencer.session_id(),
            ValueMesh::from_ranges_with_default(
                self.region.clone(),
                0,
                sequencer.assign_seqs(self.seq_keys.get_or_init(|| {
                    Arc::new(
                        self.nodes
                            .values()
                            .flat_map(|node| node.destinations)
                            .map(|destination| SeqKey::for_handler(destination.actor()))
                            .collect(),
                    )
                })),
            )?,
        ))
    }
}

/// Return the tiles directly reached from `tile` by the current communication
/// algorithm.
///
/// This asks only for the current tile's outgoing edges without materializing
/// the full domain tree. The returned [`MaterializedTile`]s are still tiles of
/// the input value type.
///
/// ```text
/// current MaterializedTile:
/// T0 [ A0 A1 A2 A3
///      A4 A5 A6 A7 ]
///
/// next_tiles(current), rendered by destination actor rank:
/// A0
/// |-- T1 [ A1 ]
/// |-- T2 [ A2 ]
/// |-- T3 [ A3 ]
/// `-- T4 [ A4 A5 A6 A7 ]
/// ```
fn next_tiles<T: 'static>(
    tiling_policy: TilingPolicy,
    tile: &MaterializedTile<T>,
) -> Vec<MaterializedTile<T>> {
    tiling_policy
        .children(tile.tile())
        .into_iter()
        .map(|child| tile.subtile(child))
        .collect()
}

/// Well-known actor name for the [`CastActor`] system actor.
///
/// One `CastActor` runs on each host system proc under this name.
pub const CAST_ACTOR_NAME: &str = "cast";

/// System actor that establishes casting domains.
///
/// Each CastActor installs and propagates [`CreateCastDomain`]. After a domain
/// is set up, it stores the outgoing routes for its host-local hop.
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

impl CastActor {
    /// Return the typed CastActor reference for a system proc.
    ///
    /// Input: host proc `host_0`. Output: `host_0::cast`.
    pub fn ref_for_proc(proc: ProcAddr) -> ActorRef<Self> {
        ActorRef::attest(ActorAddr::root(proc, Label::strip(CAST_ACTOR_NAME)))
    }
}

/// One delivery and relay step in a cast tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CastHop {
    /// Deliveries performed directly by this hop.
    deliveries: Vec<CastDestination>,
    /// Region covered by the deliveries performed at this hop.
    delivery_region: Region,
    /// Precomputed outgoing relay subtrees.
    next_hops: Vec<CastSubtree>,
}

/// One destination before it is placed into the cast routing tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CastDestination {
    point_in_domain: Point,
    base_rank_in_domain: usize,
    actor: ActorAddr,
}

impl CastDestination {
    /// Build one logical destination for each actor.
    ///
    /// Input:
    ///
    /// ```text
    /// region ranks: [0 1]
    /// actors: [actor_0, actor_1]
    /// ```
    ///
    /// Output:
    ///
    /// ```text
    /// [
    ///   Destination(point=0, base_rank=0, actor=actor_0),
    ///   Destination(point=1, base_rank=1, actor=actor_1),
    /// ]
    /// ```
    pub fn mesh(region: Region, actors: Vec<ActorAddr>) -> anyhow::Result<ValueMesh<Self>> {
        anyhow::ensure!(
            actors.len() == region.num_ranks(),
            "cast domain member count must match the logical region"
        );

        let destinations = region
            .slice()
            .iter()
            .zip(actors)
            .map(|(base_rank_in_domain, actor)| {
                Ok(Self {
                    point_in_domain: region.point_of_base_rank(base_rank_in_domain)?,
                    base_rank_in_domain,
                    actor,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        ValueMesh::new(region, destinations).map_err(Into::into)
    }

    /// Select destinations and rebuild their points for the sliced region.
    ///
    /// Input:
    ///
    /// ```text
    /// destinations: [actor_0, actor_1, actor_2, actor_3]
    /// region: select base ranks [1 3]
    /// ```
    ///
    /// Output:
    ///
    /// ```text
    /// [actor_1, actor_3]
    /// ```
    ///
    /// Each output node keeps its base rank and gets a point in the selected
    /// region.
    pub fn subset(
        destinations: &ValueMesh<Self>,
        region: Region,
    ) -> anyhow::Result<ValueMesh<Self>> {
        anyhow::ensure!(
            region.is_subset(&destinations.region()),
            "cast domain slice must be a subset of the logical region"
        );

        let actors = region
            .slice()
            .iter()
            .map(|base_rank| {
                destinations
                    .get_by_base_rank(base_rank)
                    .map(|destination| destination.actor.clone())
                    .ok_or_else(|| anyhow::anyhow!("missing cast destination for rank {base_rank}"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        Self::mesh(region, actors)
    }

    /// Return this destination's actor address.
    ///
    /// Input: `Destination(actor=host_0_actor_0)`.
    /// Output: `host_0_actor_0`.
    pub fn actor(&self) -> &ActorAddr {
        &self.actor
    }
}

/// A node in the `CastActor` tree that is conceptually made up of **only** `CastActor`s as opposed
/// to both `CastActor`s and destination `Actor`s. This node contains a list of destination `Actor`s
/// that the `CastActor` delivers to
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CastNode {
    cast_actor: ActorRef<CastActor>,
    destinations: Vec<CastDestination>,
}

/// Group destinations by host CastActor into a one-dimensional routing mesh.
///
/// Input:
///
/// ```text
/// destinations:
/// +----------------------------+----------------------------+----------------------------+
/// | host_0_actor_0             | host_0_actor_1             | host_1_actor_0             |
/// +----------------------------+----------------------------+----------------------------+
/// host CastActors:
/// +----------------------------+----------------------------+----------------------------+
/// | host_0::cast               | host_0::cast               | host_1::cast               |
/// +----------------------------+----------------------------+----------------------------+
/// ```
///
/// Output:
///
/// ```text
/// +--------------------------------------------------+-------------------------------------+
/// | host_0::cast {host_0_actor_0, host_0_actor_1}    | host_1::cast {host_1_actor_0}       |
/// +--------------------------------------------------+-------------------------------------+
/// ```
fn cast_node_mesh(
    destinations: &ValueMesh<CastDestination>,
    host_cast_actors: &ValueMesh<ActorRef<CastActor>>,
) -> anyhow::Result<ValueMesh<CastNode>> {
    anyhow::ensure!(
        destinations.region() == host_cast_actors.region(),
        "cast destinations and host CastActors must use the same region"
    );

    let mut nodes: Vec<CastNode> = Vec::new();
    let mut group_by_cast_actor: HashMap<ActorRef<CastActor>, usize> = HashMap::new();

    for (destination, cast_actor) in destinations.values().zip(host_cast_actors.values()) {
        if let Some(group) = group_by_cast_actor.get(&cast_actor).copied() {
            nodes[group].destinations.push(destination.clone());
        } else {
            group_by_cast_actor.insert(cast_actor.clone(), nodes.len());
            nodes.push(CastNode {
                cast_actor: cast_actor.clone(),
                destinations: vec![destination.clone()],
            });
        }
    }

    anyhow::ensure!(
        !nodes.is_empty(),
        "cast domain must contain at least one destination"
    );

    let routing_region = Region::new(
        vec![CAST_ACTOR_DIM.to_string()],
        Slice::new_row_major(vec![nodes.len()]),
    );

    Ok(ValueMesh::new(routing_region, nodes)?)
}

fn annotate_cast_failure(
    envelope: &mut MessageEnvelope,
    cast_actor: &ActorAddr,
    phase: &str,
    origin: &ActorAddr,
    return_port: &hyperactor::PortAddr,
) {
    if let Some(failure) = envelope.root_delivery_failure_mut() {
        annotate_cast_delivery_failure(failure, cast_actor, phase, origin, return_port);
    }
}

fn annotate_cast_delivery_failure(
    failure: &mut DeliveryFailure,
    cast_actor: &ActorAddr,
    phase: &str,
    origin: &ActorAddr,
    return_port: &hyperactor::PortAddr,
) {
    failure.attrs.set(CAST_FAILURE_PHASE, phase.to_string());
    failure
        .attrs
        .set(CAST_FAILURE_CAST_ACTOR, cast_actor.clone());
    failure.attrs.set(CAST_FAILURE_ORIGIN, origin.clone());
    failure
        .attrs
        .set(CAST_FAILURE_RETURN_PORT, return_port.to_string());
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
    fn return_cast_error_to_origin(
        cx: &Context<Self>,
        message: &CastMessage,
        phase: &str,
        reason: TransportFailureReason,
    ) {
        let destination = cx.self_addr().port_addr(Port::handler::<CastMessage>());
        let mut failure = DeliveryFailure::new(UndeliverableReason::Transport(
            TransportFailure::new(destination.clone(), reason),
        ));
        let return_port: PortRef<Undeliverable<MessageEnvelope>> =
            PortRef::attest_handler_port(&message.sender);

        match wirevalue::Any::serialize(message) {
            Ok(data) => {
                let mut message_envelope = MessageEnvelope::new(
                    message.sender.clone(),
                    destination,
                    data,
                    cx.headers().clone(),
                );
                message_envelope.push_delivery_failure(failure);
                Self::return_cast_failure_to_origin(cx, message_envelope, message, phase);
            }
            Err(error) => {
                tracing::error!(%error, "failed to serialize returned cast message");
                annotate_cast_delivery_failure(
                    &mut failure,
                    cx.self_addr(),
                    phase,
                    &message.sender,
                    return_port.port_addr(),
                );
                return_port.post(
                    cx,
                    Undeliverable::Report(DeliveryFailureReport::new(
                        cx.self_addr().clone(),
                        EndpointLocation::Port(destination),
                        Some(CastMessage::typename().to_string()),
                        failure,
                    )),
                );
            }
        }
    }

    fn return_cast_failure_to_origin(
        cx: &Instance<Self>,
        mut message_envelope: MessageEnvelope,
        message: &CastMessage,
        phase: &str,
    ) {
        let return_port = PortRef::attest_handler_port(&message.sender);
        annotate_cast_failure(
            &mut message_envelope,
            cx.self_addr(),
            phase,
            &message.sender,
            return_port.port_addr(),
        );
        message_envelope.set_header(CAST_ORIGINATING_SENDER, message.sender.clone());
        return_port.post(cx, Undeliverable::Returned(message_envelope));
    }

    async fn return_delivery_failure_to_origin(
        &mut self,
        cx: &Instance<Self>,
        undelivered: Undeliverable<MessageEnvelope>,
    ) -> Result<(), anyhow::Error> {
        let mut message_envelope = match undelivered {
            Undeliverable::Returned(message_envelope) => message_envelope,
            Undeliverable::Report(report) => {
                tracing::error!(?report, "cast delivery failed without a returned message");
                return Ok(());
            }
        };

        // 1. Case delivery failure at a "forwarding" step.
        if let Ok(message) = message_envelope.deserialized::<CastMessage>() {
            Self::return_cast_failure_to_origin(cx, message_envelope, &message, "forward");
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
/// Materialization sends this to each relay subtree root's [`CastActor`].
/// Each receiving [`CastActor`] stores its [`CastHop`], computes outgoing next
/// hops from its materialized tile, and forwards this same message with the
/// corresponding communication-child tile.
#[derive(Debug, Serialize, Deserialize, typeuri::Named)]
struct CreateCastDomain {
    cast_domain_id: CastDomainId,
    served_region: Region,
    tiling_policy: TilingPolicy,
    tile: MaterializedTile<CastNode>,
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
            num_cast_actors = message.tile.rank_count(),
        )
    )]
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        message: CreateCastDomain,
    ) -> Result<(), anyhow::Error> {
        let CreateCastDomain {
            cast_domain_id,
            served_region,
            tiling_policy,
            tile,
        } = message;
        if self.installed_hops.contains_key(&cast_domain_id) {
            return Ok(());
        }

        let deliveries = {
            let root_node = tile
                .root_item()
                .ok_or_else(|| anyhow::anyhow!("cast routing tile must contain a root node"))?;
            anyhow::ensure!(
                root_node.cast_actor.actor_addr() == cx.self_addr(),
                "CastActor received a routing tile for a different relay"
            );

            root_node.destinations.clone()
        };
        let delivery_region = served_region.range(
            CAST_DESTINATION_DIM,
            ndslice::Range(0, Some(deliveries.len()), 1),
        )?;

        let child_tiles = next_tiles(tiling_policy, &tile);
        let next_hops = child_tiles
            .iter()
            .map(|next_tile| CastSubtree::try_from_tile(&tile, &served_region, next_tile))
            .collect::<Result<Vec<_>>>()?;

        for (next_hop, next_tile) in next_hops.iter().zip(child_tiles) {
            next_hop.cast_actor.post(
                cx,
                CreateCastDomain {
                    cast_domain_id: cast_domain_id.clone(),
                    tiling_policy,
                    tile: next_tile,
                    served_region: next_hop.served_region.clone(),
                },
            );
        }

        let cast_hop = CastHop {
            deliveries,
            delivery_region,
            next_hops,
        };

        #[cfg(test)]
        {
            tests::capture_installed_domain(cx, cast_domain_id.domain_id(), &cast_hop);
        }

        self.installed_hops.insert(cast_domain_id, cast_hop);

        Ok(())
    }
}

/// Fanout counts used to configure reducers for one cast hop.
#[derive(Debug, Clone, Copy)]
struct SplitFanout {
    /// Number of immediate reply sources: child hops and direct deliveries.
    peer_count: usize,
    /// Total number of destinations served by this hop and its descendants.
    num_destinations: usize,
}

/// Rewrite reply port parts in the serialized message so that downstream
/// actors reply through local proxy ports on the current sender or CastActor
/// instead of directly to the original sender. Each proxy port reduces replies
/// from downstream next hops plus local deliveries, forming a reduction tree
/// that mirrors the cast tree.
fn split_ports(
    cx: &impl context::Actor,
    data: &mut wirevalue::Any<wirevalue::encoding::Multipart>,
    fanout: SplitFanout,
) -> Result<()> {
    data.visit_multipart_parts_mut::<PortRefRepr, anyhow::Error>(|port| {
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
            tests::collect_split_port(port.port_addr(), &split);
        }

        port.update_port_addr(split);
        Ok(())
    })?;

    data.visit_multipart_parts_mut::<IdleFlushPortRefRepr, anyhow::Error>(|port| {
        if port.unsplit() || port.reducer_spec().is_none() {
            return Ok(());
        }

        let opts = port.reducer_opts();

        let expected = fanout
            .num_destinations
            .checked_mul(opts.expected_updates_per_destination.get())
            .and_then(ConfigNonZeroUsize::new)
            .ok_or_else(|| {
                anyhow::anyhow!("expected reducer update count must be nonzero and cannot overflow")
            })?;

        let split = port.port_addr().split(
            cx,
            port.reducer_spec().cloned(),
            ReducerMode::IdleFlush {
                expected,
                idle_timeout: opts.idle_timeout,
                abandon_timeout: opts.abandon_timeout,
            },
            port.get_return_undeliverable(),
        )?;

        #[cfg(test)]
        {
            tests::collect_split_port(port.port_addr(), &split);
        }

        port.update_port_addr(split);
        Ok(())
    })?;

    data.visit_multipart_parts_mut::<OncePortRefRepr, anyhow::Error>(|port| {
        if port.unsplit() || port.reducer_spec().is_none() {
            // OncePorts without reducers cannot be split. Pass through as-is.
            // Using the port more than once will cause a delivery error downstream.
            return Ok(());
        }

        let split = port.port_addr().split(
            cx,
            port.reducer_spec().clone(),
            ReducerMode::Once(fanout.peer_count),
            true,
        )?;

        #[cfg(test)]
        {
            tests::collect_split_port(port.port_addr(), &split);
        }

        port.update_port_addr(split);
        Ok(())
    })?;

    Ok(())
}

/// Test-only forwarding path metadata.
///
/// In production this is zero-sized and optimized away. In tests, each
/// forwarded message carries the actor addresses already traversed, and local
/// delivery appends the destination actor.
#[derive(Debug, Clone, Default)]
struct ForwardLineage {
    #[cfg(test)]
    actors: Vec<ActorAddr>,
}

impl ForwardLineage {
    #[cfg(test)]
    fn from_message(message: &CastMessage) -> Self {
        Self {
            actors: message.lineage.clone(),
        }
    }

    #[cfg(not(test))]
    fn from_message(_message: &CastMessage) -> Self {
        Self {}
    }

    fn through(&self, actor: &ActorAddr) -> Self {
        #[cfg(test)]
        {
            let mut actors = self.actors.clone();
            actors.push(actor.clone());
            Self { actors }
        }
        #[cfg(not(test))]
        {
            let _ = actor;
            Self {}
        }
    }

    #[cfg(test)]
    fn actors(&self) -> Vec<ActorAddr> {
        self.actors.clone()
    }
}

/// Multicast payload routed through a cast domain.
///
/// Clients send this through routes that enter a CastActor. CastActors forward
/// the same message type to child hops; internal forwards differ only in
/// test-only lineage. Direct routes deliver the underlying payload without
/// constructing this envelope.
#[derive(Debug, Serialize, Deserialize, typeuri::Named)]
struct CastMessage {
    /// The domain to cast into.
    cast_domain_id: CastDomainId,
    /// Actor that initiated the cast.
    sender: ActorAddr,
    /// Sender-side sequencer session for this cast.
    session_id: Uuid,
    /// Sequence numbers for the destinations in the current routing subtree.
    seqs: ValueMesh<u64>,
    /// Test-only path of actor addresses traversed so far.
    #[cfg(test)]
    lineage: Vec<ActorAddr>,
    /// Message headers.
    headers: Flattrs,
    /// The target port index on each destination actor.
    dest_port: u64,
    /// The serialized message data.
    data: wirevalue::Any<wirevalue::encoding::Multipart>,
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
        let Some(domain) = self.installed_hops.get(&message.cast_domain_id) else {
            Self::return_cast_error_to_origin(
                cx,
                &message,
                "unknown_domain",
                TransportFailureReason::NoRoute,
            );
            return Ok(());
        };

        let lineage = ForwardLineage::from_message(&message).through(cx.self_addr());
        if let Err(error) = domain.route(cx, &message, &lineage) {
            tracing::error!(
                %error,
                domain_id = %message.cast_domain_id,
                "failed to route cast message",
            );
            Self::return_cast_error_to_origin(
                cx,
                &message,
                "route",
                TransportFailureReason::LinkUnavailable(error.to_string()),
            );
        }

        Ok(())
    }
}

struct CastDelivery<'a> {
    sender: &'a ActorAddr,
    session_id: Uuid,
    seq: u64,
    headers: &'a Flattrs,
    dest_port: u64,
}

fn deliver_to_destination(
    cx: &impl context::Actor,
    delivery: &CastDelivery<'_>,
    destination: &CastDestination,
    mut data: wirevalue::Any<wirevalue::encoding::Multipart>,
    lineage: &ForwardLineage,
) -> Result<()> {
    let rank = destination.point_in_domain.rank();

    data.visit_multipart_parts_mut::<ResourceRankRepr, anyhow::Error>(
        |ResourceRankRepr(resource_rank)| {
            *resource_rank = Some(rank);
            Ok(())
        },
    )?;

    let mut headers = delivery.headers.clone();
    headers.set(CAST_POINT, destination.point_in_domain.clone());
    headers.set(CAST_ORIGINATING_SENDER, delivery.sender.clone());
    let seq_info = SeqInfo::Session {
        session_id: delivery.session_id,
        seq: delivery.seq,
    };
    headers.set(SEQ_INFO, seq_info.clone());

    #[cfg(not(test))]
    let _ = lineage;

    #[cfg(test)]
    headers.set(CAST_LINEAGE, lineage.actors());

    let dest = destination
        .actor
        .port_addr(Port::handler_id(delivery.dest_port, None));
    hyperactor::mailbox::headers::stamp_sender_actor_id_hash(&mut headers, delivery.sender);
    hyperactor::mailbox::headers::stamp_sender_actor_id(
        &mut headers,
        &seq_info,
        &dest,
        delivery.sender,
    );
    cx.instance()
        .post_with_external_seq_info(dest, headers, data.erase_encoding());
    Ok(())
}

impl CastHop {
    fn route(
        &self,
        cx: &impl context::Actor,
        message: &CastMessage,
        lineage: &ForwardLineage,
    ) -> Result<(), anyhow::Error> {
        // Split reply ports so that downstream next hops reply through this
        // hop's local proxy ports instead of directly to the original sender.
        let mut data = message.data.clone();
        split_ports(
            cx,
            &mut data,
            SplitFanout {
                peer_count: self.deliveries.len() + self.next_hops.len(),
                num_destinations: message.seqs.region().num_ranks(),
            },
        )?;

        let next_hop_seqs = self
            .next_hops
            .iter()
            .map(|next_hop| message.seqs.sliced(next_hop.served_region.clone()))
            .collect::<Vec<_>>();

        for (next_hop, seqs) in self.next_hops.iter().zip(next_hop_seqs) {
            next_hop.cast_actor.port().post_with_headers(
                cx,
                message.headers.clone(),
                CastMessage {
                    cast_domain_id: message.cast_domain_id.clone(),
                    sender: message.sender.clone(),
                    session_id: message.session_id,
                    seqs,
                    #[cfg(test)]
                    lineage: lineage.actors(),
                    headers: message.headers.clone(),
                    dest_port: message.dest_port,
                    data: data.clone(),
                },
            );
        }

        for (destination, seq) in self
            .deliveries
            .iter()
            .zip(message.seqs.sliced(self.delivery_region.clone()).values())
        {
            let direct_lineage = lineage.through(&destination.actor);

            deliver_to_destination(
                cx,
                &CastDelivery {
                    sender: &message.sender,
                    session_id: message.session_id,
                    seq,
                    headers: &message.headers,
                    dest_port: message.dest_port,
                },
                destination,
                data.clone(),
                &direct_lineage,
            )?;
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
            next_hop.cast_actor.post(
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
    // Cast actor invariant coverage:
    //
    // CA-1: Partial - exact delivery is covered; independent destination
    //       failure is not covered.
    // CA-2: Direct - materialization properties and delivery tests cover it.
    // CA-3: Indirect - delivery tests cast immediately after materialization.
    // CA-4: Direct - the projection ordering test covers it.
    // CA-5: Mostly direct - headers, reducers, ports, and routed delivery are
    //       covered; equivalent route topologies are not compared.
    // CA-6: Partial - overlapping casts are covered; isolated destruction of
    //       one live overlapping domain is not covered.
    // CA-7: Direct - stopped destinations, repeated teardown, and post-destroy
    //       rejection are covered.
    // CA-8: Direct - unknown domains, routing errors, and report-only failures
    //       are covered.

    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::collections::HashMap;
    use std::num::NonZeroUsize;
    use std::sync::Mutex;
    use std::sync::OnceLock;
    use std::time::Duration;

    use hyperactor::Client;
    use hyperactor::IdleFlushPortRef;
    use hyperactor::Label;
    use hyperactor::PortAddr;
    use hyperactor::ProcAddr;
    use hyperactor::accum::IdleFlushReducerOpts;
    use hyperactor::channel::ChannelTransport;
    use hyperactor::proc::Proc;
    use ndslice::Shape;
    use ndslice::Slice;
    use ndslice::ViewExt;
    use ndslice::shape;
    use ndslice::strategy::gen_region_strided;
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
        next_hop_procs: BTreeSet<String>,
        direct_hop_procs: BTreeSet<String>,
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
            next_hop_procs: cast_hop
                .next_hops
                .iter()
                .map(|next_hop| {
                    next_hop
                        .cast_actor
                        .actor_addr()
                        .proc_addr()
                        .log_name()
                        .to_string()
                })
                .chain(
                    cast_hop
                        .deliveries
                        .iter()
                        .map(|destination| destination.actor.proc_addr().log_name().to_string()),
                )
                .collect(),
            direct_hop_procs: cast_hop
                .deliveries
                .iter()
                .map(|destination| destination.actor.proc_addr().log_name().to_string())
                .collect(),
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

    /// Build a test destination mesh from a rank-to-member map.
    ///
    /// Input:
    ///
    /// ```text
    /// region ranks: [0 1]
    /// members: {0: proc0::member, 1: proc1::member}
    /// ```
    ///
    /// Output:
    ///
    /// ```text
    /// destinations:    [proc0::member, proc1::member]
    /// host CastActors: [proc0::cast,   proc1::cast]
    /// ```
    fn destination_mesh(
        region: Region,
        members: &HashMap<usize, ActorAddr>,
    ) -> anyhow::Result<(
        Arc<ValueMesh<CastDestination>>,
        Arc<ValueMesh<ActorRef<CastActor>>>,
    )> {
        let actors = region
            .slice()
            .iter()
            .map(|rank| {
                members
                    .get(&rank)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("missing test member for rank {rank}"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        let host_cast_actors = Arc::new(ValueMesh::new(
            region.clone(),
            actors
                .iter()
                .map(|actor| CastActor::ref_for_proc(actor.proc_addr()))
                .collect(),
        )?);

        Ok((
            Arc::new(CastDestination::mesh(region, actors)?),
            host_cast_actors,
        ))
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

    /// A simple message type for testing cast delivery.
    #[derive(Debug, Clone, Serialize, Deserialize, typeuri::Named)]
    struct TestDelivery {
        payload: String,
    }
    wirevalue::register_type!(TestDelivery);

    #[derive(Debug, Serialize, Deserialize, typeuri::Named)]
    enum CastStoppedDomain {
        Cast,
        DestroyAndCast,
    }
    wirevalue::register_type!(CastStoppedDomain);

    #[hyperactor::export(handlers = [CastStoppedDomain])]
    struct CastFailureProbe {
        members: HashMap<usize, ActorAddr>,
        region: Region,
        failures: PortRef<Undeliverable<MessageEnvelope>>,
        domain: Option<CastDomainRef>,
    }

    #[async_trait]
    impl Actor for CastFailureProbe {
        async fn handle_delivery_failure_event(
            &mut self,
            cx: &Instance<Self>,
            undeliverable: Undeliverable<MessageEnvelope>,
        ) -> Result<(), anyhow::Error> {
            self.failures.post(cx, undeliverable);
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<CastStoppedDomain> for CastFailureProbe {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            message: CastStoppedDomain,
        ) -> Result<(), anyhow::Error> {
            if self.domain.is_none() {
                let (destinations, host_cast_actors) =
                    destination_mesh(self.region.clone(), &self.members)?;

                self.domain = Some(CastDomainId::new().materialize(
                    cx,
                    destinations,
                    host_cast_actors,
                    TilingPolicy::BlockPartitioning,
                    Flattrs::new(),
                )?);
            }
            let domain = self
                .domain
                .as_ref()
                .expect("cast domain was initialized above");
            let payload = match message {
                CastStoppedDomain::Cast => "before-destroy",
                CastStoppedDomain::DestroyAndCast => {
                    domain.destroy(cx);
                    domain.destroy(cx);
                    "after-destroy"
                }
            };
            domain.cast(
                cx,
                Flattrs::new(),
                TestDelivery {
                    payload: payload.to_string(),
                },
            )
        }
    }

    /// Delivery record kept by test receivers in handler execution order.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, typeuri::Named)]
    struct TestDeliveryRecord {
        payload: String,
        lineage: Vec<ActorAddr>,
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
            TestDelivery,
            GetHistory,
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
        _client_proc: Arc<Proc>,
        client: Client,
        _procs: Arc<Vec<Proc>>,
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
                _client_proc: Arc::new(client_proc),
                client,
                _procs: Arc::new(procs),
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
                self.member_ids
                    .keys()
                    .map(|rank| (*rank, self.receiver_ids[*rank].clone()))
                    .collect::<HashMap<_, _>>()
            }
        }

        /// Returns a test mesh containing only the members selected by `region`.
        ///
        /// For a mesh with ranks `0..8`, a region containing ranks `4..8`
        /// produces a mesh with member ranks `4`, `5`, `6`, and `7` that shares
        /// the original live procs.
        fn sliced(&self, region: Region) -> Self {
            let all_members = self.domain_members();
            let member_ids = region
                .slice()
                .iter()
                .map(|rank| {
                    (
                        rank,
                        all_members
                            .get(&rank)
                            .expect("test mesh must contain every selected rank")
                            .clone(),
                    )
                })
                .collect();

            Self {
                _client_proc: Arc::clone(&self._client_proc),
                client: self.client.clone(),
                _procs: Arc::clone(&self._procs),
                member_ids,
                receiver_ids: self.receiver_ids.clone(),
            }
        }

        fn root_domain(&self, region: Region) -> CastDomainRef {
            let members = self.domain_members();
            let (destinations, host_cast_actors) = destination_mesh(region, &members).unwrap();

            CastDomainId::new()
                .materialize(
                    &self.client,
                    destinations,
                    host_cast_actors,
                    TilingPolicy::BlockPartitioning,
                    Flattrs::new(),
                )
                .unwrap()
        }

        fn proc_names(&self) -> Vec<String> {
            let mut ranks = self.domain_members().into_keys().collect::<Vec<_>>();
            ranks.sort_unstable();

            ranks
                .into_iter()
                .map(|rank| format!("proc_{rank}"))
                .collect()
        }

        fn root_domain_with_policy(&self, region: Region, policy: TilingPolicy) -> CastDomainRef {
            let all_members = self.domain_members();
            let (destinations, host_cast_actors) = destination_mesh(region, &all_members).unwrap();

            CastDomainId::new()
                .materialize(
                    &self.client,
                    destinations,
                    host_cast_actors,
                    policy,
                    Flattrs::new(),
                )
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

        async fn wait_for_any_domain_snapshot(&self, domain_id: &Uid) {
            tokio::time::timeout(Duration::from_secs(5), async {
                while captured_domain_snapshots(domain_id).is_empty() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap_or_else(|_| {
                panic!("timed out waiting for an installed domain snapshot for {domain_id}")
            });
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

    fn materialization_regions() -> impl Strategy<Value = Region> {
        prop_oneof![
            small_shape_sizes().prop_map(|sizes| Region::from(shape_from_sizes(sizes.as_slice()))),
            gen_region_strided(1..=4, 4, 3, 0),
        ]
    }

    fn tiling_policies() -> impl Strategy<Value = TilingPolicy> {
        prop_oneof![
            Just(TilingPolicy::BlockPartitioning),
            (1usize..=4).prop_map(|fanout| TilingPolicy::BoundedFanout {
                fanout: NonZeroUsize::new(fanout).expect("generated fanout is nonzero"),
            }),
            Just(TilingPolicy::Bisection),
        ]
    }

    fn validate_root_heaved_routing_tiles(
        region: Region,
        policy: TilingPolicy,
    ) -> Result<(), TestCaseError> {
        let members = region
            .slice()
            .iter()
            .map(|rank| (rank, member(rank)))
            .collect::<HashMap<_, _>>();
        let (destinations, host_cast_actors) = destination_mesh(region.clone(), &members)
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        let nodes = Arc::new(
            cast_node_mesh(&destinations, &host_cast_actors)
                .map_err(|error| TestCaseError::fail(error.to_string()))?,
        );
        let root = MaterializedTile::from_value_mesh_with_tile(
            Tile::from_view(Ranked::region(nodes.as_ref())),
            nodes.clone(),
        );
        let root_region = Ranked::region(nodes.as_ref())
            .range(CAST_ACTOR_DIM, ndslice::Range(0, Some(1), 1))
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        let entry_tiles = std::iter::once(root.subtile(Tile::from_view(&root_region)))
            .chain(next_tiles(policy, &root))
            .collect::<Vec<_>>();
        let served_region = Region::new(
            vec![CAST_DESTINATION_DIM.to_string()],
            Slice::new_row_major(vec![Ranked::region(destinations.as_ref()).num_ranks()]),
        );

        let expected_relays = host_cast_actors
            .values()
            .map(|cast_actor| cast_actor.actor_addr().clone())
            .collect::<BTreeSet<_>>();
        let observed_relays = entry_tiles
            .iter()
            .flat_map(MaterializedTile::items)
            .map(|node| node.cast_actor.actor_addr().clone())
            .collect::<BTreeSet<_>>();
        let expected_ranks = region.slice().iter().collect::<BTreeSet<_>>();
        let observed_ranks = entry_tiles
            .iter()
            .flat_map(MaterializedTile::items)
            .flat_map(|node| node.destinations.iter())
            .map(|destination| destination.base_rank_in_domain)
            .collect::<BTreeSet<_>>();

        prop_assert_eq!(observed_relays, expected_relays);
        prop_assert_eq!(observed_ranks, expected_ranks);
        prop_assert!(
            entry_tiles
                .iter()
                .all(|tile| CastSubtree::try_from_tile(&root, &served_region, tile).is_ok())
        );
        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 64,
            ..ProptestConfig::default()
        })]

        // CA-2 (domain coverage).
        #[test]
        fn prop_root_heaved_routing_tiles_cover_the_domain(
            region in materialization_regions(),
            policy in tiling_policies(),
        ) {
            validate_root_heaved_routing_tiles(region, policy)?;
        }
    }

    async fn cast_and_collect_histories(
        client: &Client,
        cast_domain: &CastDomainRef,
    ) -> BTreeMap<String, Vec<TestDeliveryRecord>> {
        let (reply_handle, reply_rx) =
            context::Mailbox::mailbox(client).open_reduce_port(TestDeliveryHistoriesAccumulator);
        let reply_ref = reply_handle.bind();

        cast_domain
            .cast(
                client,
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

        assert_eq!(
            snapshots.keys().cloned().collect::<BTreeSet<_>>(),
            (0..8).map(|rank| format!("proc_{rank}")).collect()
        );

        for rank in 0..8 {
            let proc_name = format!("proc_{rank}");
            let snapshot = snapshots
                .get(&proc_name)
                .unwrap_or_else(|| panic!("missing snapshot for {proc_name}"));

            assert_eq!(
                snapshot.direct_hop_procs,
                [proc_name.clone()].into_iter().collect()
            );
            assert!(snapshot.next_hop_procs.contains(&proc_name));
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

    // CA-1 (direct-delivery equivalence), CA-3 (setup ordering), and CA-5
    // (route independence).
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
        let histories = cast_and_collect_histories(&test_mesh.client, &root_domain).await;

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

        let relays = test_mesh
            .member_ids
            .values()
            .map(|member| CastActor::ref_for_proc(member.proc_addr()).into_actor_addr())
            .collect::<BTreeSet<_>>();

        let destinations = test_mesh
            .receiver_ids
            .iter()
            .enumerate()
            .map(|(rank, actor)| (format!("proc_{rank}"), actor))
            .collect::<BTreeMap<_, _>>();

        for (proc_name, lineage) in lineage_by_proc {
            let (destination, relay_lineage) = lineage
                .split_last()
                .expect("every delivery lineage must include its destination");

            assert_eq!(destination, destinations[&proc_name]);
            assert!(
                relay_lineage.iter().all(|actor| relays.contains(actor)),
                "lineage before {proc_name} must contain only CastActors"
            );
        }
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_host_relay_lineage() {
        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client_proc".into()).unwrap();
        let client = client_proc.client("client");

        let hosts = (0..8)
            .map(|host_rank| {
                let proc = Proc::direct(ChannelTransport::Unix.any(), format!("host_{host_rank}"))
                    .unwrap();
                let cast_handle = proc
                    .spawn_with_uid(
                        Uid::singleton(Label::strip(CAST_ACTOR_NAME)),
                        CastActor::default(),
                    )
                    .unwrap();
                let cast_actor: ActorRef<CastActor> = cast_handle.bind();

                (proc, cast_actor)
            })
            .collect::<Vec<_>>();

        let mut worker_procs = Vec::new();
        let destination_addrs = hosts
            .iter()
            .enumerate()
            .map(|(host_rank, _)| {
                (0..2)
                    .map(|actor_rank| {
                        let proc_name = format!("host_{host_rank}_actor_{actor_rank}");
                        let proc = Proc::direct(ChannelTransport::Unix.any(), proc_name).unwrap();
                        let receiver_handle = proc
                            .spawn_with_uid(
                                Uid::singleton(Label::strip("receiver")),
                                TestReceiver::default(),
                            )
                            .unwrap();
                        let _: ActorRef<TestReceiver> = receiver_handle.bind();
                        let destination =
                            ActorAddr::root(proc.proc_addr().clone(), Label::strip("receiver"));

                        worker_procs.push(proc);
                        destination
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let (members, host_cast_actors): (Vec<_>, Vec<_>) = hosts
            .iter()
            .zip(&destination_addrs)
            .flat_map(|((_, cast_actor), destination_addrs)| {
                destination_addrs
                    .iter()
                    .map(|destination| (destination.clone(), cast_actor.clone()))
            })
            .unzip();

        let region = Region::from(shape!(hosts = 8, actors = 2));
        let destinations = Arc::new(CastDestination::mesh(region.clone(), members).unwrap());
        let host_cast_actors = Arc::new(ValueMesh::new(region, host_cast_actors).unwrap());

        let domain = CastDomainId::new()
            .materialize(
                &client,
                Arc::clone(&destinations),
                Arc::clone(&host_cast_actors),
                TilingPolicy::BoundedFanout {
                    fanout: NonZeroUsize::new(2).unwrap(),
                },
                Flattrs::new(),
            )
            .unwrap();

        domain
            .cast(
                &client,
                Flattrs::new(),
                TestDelivery {
                    payload: "hello".to_string(),
                },
            )
            .unwrap();

        let lineage_by_proc = cast_and_collect_histories(&client, &domain)
            .await
            .into_iter()
            .map(|(proc_name, history)| {
                assert_eq!(history.len(), 1, "{proc_name} must receive one delivery");
                assert_eq!(history[0].payload, "hello");

                (proc_name, history[0].lineage.clone())
            })
            .collect::<BTreeMap<_, _>>();

        // client
        // |-- host_0::cast
        // |   |-- host_0_actor_0::receiver
        // |   `-- host_0_actor_1::receiver
        // |-- host_1::cast
        // |   |-- host_1_actor_0::receiver
        // |   |-- host_1_actor_1::receiver
        // |   |-- host_2::cast
        // |   |   |-- host_2_actor_0::receiver
        // |   |   |-- host_2_actor_1::receiver
        // |   |   `-- host_3::cast
        // |   |       |-- host_3_actor_0::receiver
        // |   |       `-- host_3_actor_1::receiver
        // |   `-- host_4::cast
        // |       |-- host_4_actor_0::receiver
        // |       `-- host_4_actor_1::receiver
        // `-- host_5::cast
        //     |-- host_5_actor_0::receiver
        //     |-- host_5_actor_1::receiver
        //     |-- host_6::cast
        //     |   |-- host_6_actor_0::receiver
        //     |   `-- host_6_actor_1::receiver
        //     `-- host_7::cast
        //         |-- host_7_actor_0::receiver
        //         `-- host_7_actor_1::receiver
        let expected_lineage: BTreeMap<String, Vec<ActorAddr>> = [
            (
                "host_0_actor_0".to_string(),
                vec![
                    hosts[0].1.actor_addr().clone(),
                    destination_addrs[0][0].clone(),
                ],
            ),
            (
                "host_0_actor_1".to_string(),
                vec![
                    hosts[0].1.actor_addr().clone(),
                    destination_addrs[0][1].clone(),
                ],
            ),
            (
                "host_1_actor_0".to_string(),
                vec![
                    hosts[1].1.actor_addr().clone(),
                    destination_addrs[1][0].clone(),
                ],
            ),
            (
                "host_1_actor_1".to_string(),
                vec![
                    hosts[1].1.actor_addr().clone(),
                    destination_addrs[1][1].clone(),
                ],
            ),
            (
                "host_2_actor_0".to_string(),
                vec![
                    hosts[1].1.actor_addr().clone(),
                    hosts[2].1.actor_addr().clone(),
                    destination_addrs[2][0].clone(),
                ],
            ),
            (
                "host_2_actor_1".to_string(),
                vec![
                    hosts[1].1.actor_addr().clone(),
                    hosts[2].1.actor_addr().clone(),
                    destination_addrs[2][1].clone(),
                ],
            ),
            (
                "host_3_actor_0".to_string(),
                vec![
                    hosts[1].1.actor_addr().clone(),
                    hosts[2].1.actor_addr().clone(),
                    hosts[3].1.actor_addr().clone(),
                    destination_addrs[3][0].clone(),
                ],
            ),
            (
                "host_3_actor_1".to_string(),
                vec![
                    hosts[1].1.actor_addr().clone(),
                    hosts[2].1.actor_addr().clone(),
                    hosts[3].1.actor_addr().clone(),
                    destination_addrs[3][1].clone(),
                ],
            ),
            (
                "host_4_actor_0".to_string(),
                vec![
                    hosts[1].1.actor_addr().clone(),
                    hosts[4].1.actor_addr().clone(),
                    destination_addrs[4][0].clone(),
                ],
            ),
            (
                "host_4_actor_1".to_string(),
                vec![
                    hosts[1].1.actor_addr().clone(),
                    hosts[4].1.actor_addr().clone(),
                    destination_addrs[4][1].clone(),
                ],
            ),
            (
                "host_5_actor_0".to_string(),
                vec![
                    hosts[5].1.actor_addr().clone(),
                    destination_addrs[5][0].clone(),
                ],
            ),
            (
                "host_5_actor_1".to_string(),
                vec![
                    hosts[5].1.actor_addr().clone(),
                    destination_addrs[5][1].clone(),
                ],
            ),
            (
                "host_6_actor_0".to_string(),
                vec![
                    hosts[5].1.actor_addr().clone(),
                    hosts[6].1.actor_addr().clone(),
                    destination_addrs[6][0].clone(),
                ],
            ),
            (
                "host_6_actor_1".to_string(),
                vec![
                    hosts[5].1.actor_addr().clone(),
                    hosts[6].1.actor_addr().clone(),
                    destination_addrs[6][1].clone(),
                ],
            ),
            (
                "host_7_actor_0".to_string(),
                vec![
                    hosts[5].1.actor_addr().clone(),
                    hosts[7].1.actor_addr().clone(),
                    destination_addrs[7][0].clone(),
                ],
            ),
            (
                "host_7_actor_1".to_string(),
                vec![
                    hosts[5].1.actor_addr().clone(),
                    hosts[7].1.actor_addr().clone(),
                    destination_addrs[7][1].clone(),
                ],
            ),
        ]
        .into_iter()
        .collect();
        assert_eq!(lineage_by_proc, expected_lineage);

        // A flattened rank slice leaves two destinations on host 0 and one on
        // host 1.
        let uneven_region = Region::from(shape!(rank = 16))
            .range("rank", ndslice::Range(0, Some(3), 1))
            .unwrap();
        let uneven_domain = CastDomainId::new()
            .materialize(
                &client,
                Arc::new(CastDestination::subset(&destinations, uneven_region.clone()).unwrap()),
                Arc::new(host_cast_actors.sliced(uneven_region)),
                TilingPolicy::BoundedFanout {
                    fanout: NonZeroUsize::new(2).unwrap(),
                },
                Flattrs::new(),
            )
            .unwrap();

        uneven_domain
            .cast(
                &client,
                Flattrs::new(),
                TestDelivery {
                    payload: "uneven".to_string(),
                },
            )
            .unwrap();

        let histories = cast_and_collect_histories(&client, &domain).await;
        for (proc_name, history) in histories {
            let expected_payloads = if ["host_0_actor_0", "host_0_actor_1", "host_1_actor_0"]
                .contains(&proc_name.as_str())
            {
                vec!["hello", "uneven"]
            } else {
                vec!["hello"]
            };

            assert_eq!(
                history
                    .iter()
                    .map(|delivery| delivery.payload.as_str())
                    .collect::<Vec<_>>(),
                expected_payloads,
                "unexpected payloads for {proc_name}"
            );
        }
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_root_heaving_block_partitioning_delivers_once() {
        // GIVEN: four hosts with four proc ranks per host.
        let mut test_mesh = CastTestMesh::new(16);
        test_mesh.spawn_split_port_receivers();

        // WHEN: block partitioning is applied to every dimension at the root.
        let root_domain = test_mesh.root_domain(shape!(hosts = 4, procs = 4).into());

        // THEN: every proc receives exactly one reply-producing delivery.
        let proc_names = test_mesh.proc_names();
        assert_eq!(
            cast_and_collect_reply_counts(&test_mesh, &root_domain, "root-heaved").await,
            expected_reply_counts(&proc_names.iter().map(String::as_str).collect::<Vec<_>>())
        );
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_cast_delivers_to_multiple_actors_per_proc() {
        let client_proc = Proc::direct(ChannelTransport::Unix.any(), "client_proc".into()).unwrap();
        let client = client_proc.client("client");

        let hosts = (0..2)
            .map(|host_rank| {
                let proc = Proc::direct(ChannelTransport::Unix.any(), format!("host_{host_rank}"))
                    .unwrap();
                let cast_handle = proc
                    .spawn_with_uid(
                        Uid::singleton(Label::strip(CAST_ACTOR_NAME)),
                        CastActor::default(),
                    )
                    .unwrap();
                let cast_actor: ActorRef<CastActor> = cast_handle.bind();

                (proc, cast_actor)
            })
            .collect::<Vec<_>>();
        let worker_procs = (0..2)
            .map(|host_rank| {
                Proc::direct(
                    ChannelTransport::Unix.any(),
                    format!("host_{host_rank}_proc_0"),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let (members, host_cast_actors): (Vec<_>, Vec<_>) = hosts
            .iter()
            .zip(&worker_procs)
            .flat_map(|((_, cast_actor), proc)| {
                (0..2).map(move |actor_rank| {
                    let actor_name = format!("receiver_{actor_rank}");
                    let receiver_handle = proc
                        .spawn_with_uid(
                            Uid::singleton(Label::strip(&actor_name)),
                            SplitPortReceiver,
                        )
                        .unwrap();
                    let _: ActorRef<SplitPortReceiver> = receiver_handle.bind();

                    (
                        ActorAddr::root(proc.proc_addr().clone(), Label::strip(&actor_name)),
                        cast_actor.clone(),
                    )
                })
            })
            .unzip();
        let region = Region::from(shape!(hosts = 2, actors = 2));
        let destinations = Arc::new(CastDestination::mesh(region.clone(), members).unwrap());
        let host_cast_actors = Arc::new(ValueMesh::new(region, host_cast_actors).unwrap());

        let domain = CastDomainId::new()
            .materialize(
                &client,
                destinations,
                host_cast_actors,
                TilingPolicy::BlockPartitioning,
                Flattrs::new(),
            )
            .unwrap();

        // client
        // |-- host_0_proc_0::receiver_0
        // |-- host_0_proc_0::receiver_1
        // `-- host_1::cast
        //     |-- host_1_proc_0::receiver_0
        //     `-- host_1_proc_0::receiver_1
        let (reply_handle, reply_rx) =
            context::Mailbox::mailbox(&client).open_reduce_port(TestReplyCountsAccumulator);
        let reply_ref = reply_handle.bind();

        domain
            .cast(
                &client,
                Flattrs::new(),
                TestRequestWithReply {
                    payload: "multiple-actors-per-proc".to_string(),
                    reply_to: reply_ref,
                },
            )
            .unwrap();

        let reply_counts = tokio::time::timeout(Duration::from_secs(5), reply_rx.recv())
            .await
            .expect("timed out waiting for reduced replies")
            .expect("reply receive must succeed");

        assert_eq!(
            reply_counts.counts_by_proc,
            [
                ("host_0_proc_0".to_string(), 2),
                ("host_1_proc_0".to_string(), 2),
            ]
            .into_iter()
            .collect()
        );
    }

    // CA-5 (route independence).
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

        let histories = cast_and_collect_histories(&test_mesh.client, &root_domain).await;
        for history in histories.values() {
            assert_eq!(
                history[0].operation_endpoint.as_deref(),
                Some("endpoint.call()")
            );
        }
    }

    // CA-6 (domain isolation), CA-7 (domain lifecycle), and CA-8 (failure
    // containment).
    #[async_timed_test(timeout_secs = 30)]
    async fn test_stopped_actors_return_casts_before_and_after_domain_destroy() {
        clear_captured_domains();

        // GIVEN: all destination actors are stopped before one actor creates
        // and destroys their cast domain.
        let mut test_mesh = CastTestMesh::new(8);
        test_mesh.spawn_delivery_receivers();
        for (proc, receiver) in test_mesh._procs.iter().zip(&test_mesh.receiver_ids) {
            let mut status = proc
                .stop_actor(receiver.id(), "cast domain test shutdown".to_string())
                .expect("delivery receiver should be running");
            status
                .wait_for(|status| status.is_terminal())
                .await
                .expect("delivery receiver status should remain observable");
        }
        let region = Region::from(shape!(a = 2, b = 2, c = 2));
        let (failure_handle, mut failure_receiver) =
            context::Mailbox::mailbox(&test_mesh.client).open_port();
        let probe_handle = test_mesh
            ._client_proc
            .spawn_with_uid(
                Uid::singleton(Label::strip("cast_failure_probe")),
                CastFailureProbe {
                    members: test_mesh.domain_members(),
                    region: region.clone(),
                    failures: failure_handle.bind(),
                    domain: None,
                },
            )
            .unwrap();
        let probe_ref: ActorRef<CastFailureProbe> = probe_handle.bind::<CastFailureProbe>();

        // WHEN: the probe casts through the live domain to the stopped actors.
        probe_ref
            .port::<CastStoppedDomain>()
            .post(&test_mesh.client, CastStoppedDomain::Cast);

        // THEN: delivery to a stopped actor returns to the original sender.
        let returned = tokio::time::timeout(Duration::from_secs(5), failure_receiver.recv())
            .await
            .expect("stopped actor should return the cast to the sender")
            .expect("cast failure receiver should remain open");
        let Undeliverable::Returned(envelope) = returned else {
            panic!("stopped actor should return the original message envelope");
        };
        assert_eq!(
            envelope.headers().get(CAST_ORIGINATING_SENDER),
            Some(probe_ref.actor_addr().clone()),
        );

        // WHEN: the probe destroys the same domain and casts again.
        probe_ref
            .port::<CastStoppedDomain>()
            .post(&test_mesh.client, CastStoppedDomain::DestroyAndCast);

        // THEN: the unavailable route also returns the cast to the sender.
        // Direct destinations can also return failures, so wait for a return
        // from a destroyed CastActor route without assuming arrival order.
        let envelope = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let returned = failure_receiver
                    .recv()
                    .await
                    .expect("cast failure receiver should remain open");
                if let Undeliverable::Returned(envelope) = returned
                    && envelope
                        .root_delivery_failure()
                        .and_then(|failure| failure.attrs.get(CAST_FAILURE_PHASE))
                        == Some("unknown_domain".to_string())
                {
                    break envelope;
                }
            }
        })
        .await
        .expect("destroyed CastActor route should return to the sender");
        assert_eq!(
            envelope
                .root_delivery_failure()
                .and_then(|failure| failure.attrs.get(CAST_FAILURE_PHASE)),
            Some("unknown_domain".to_string()),
        );
        assert_eq!(
            envelope.headers().get(CAST_ORIGINATING_SENDER),
            Some(probe_ref.actor_addr().clone()),
        );

        // The failed domain must not prevent the shared CastActor from serving
        // another domain.
        let live_domain = test_mesh.root_domain(region);
        test_mesh
            .wait_for_any_domain_snapshot(live_domain.domain_id())
            .await;
    }

    // CA-8 (failure containment).
    #[async_timed_test(timeout_secs = 30)]
    async fn test_delivery_failure_report_does_not_fail_cast_actor() {
        // GIVEN: a delivery failure report without the original message.
        let proc = Proc::direct(ChannelTransport::Unix.any(), "cast_proc".into()).unwrap();
        let actor_instance = proc.actor_instance::<CastActor>("cast").unwrap();
        let mut cast_actor = CastActor::default();
        let destination = actor_instance
            .instance
            .self_addr()
            .port_addr(Port::handler::<CastMessage>());
        let report = DeliveryFailureReport::new(
            actor_instance.instance.self_addr().clone(),
            EndpointLocation::Port(destination.clone()),
            Some(CastMessage::typename().to_string()),
            DeliveryFailure::new(UndeliverableReason::Transport(TransportFailure::new(
                destination,
                TransportFailureReason::NoRoute,
            ))),
        );

        // WHEN: the report is handled.
        let result = cast_actor
            .return_delivery_failure_to_origin(
                &actor_instance.instance,
                Undeliverable::Report(report),
            )
            .await;

        // THEN: report-only failures do not fail the shared CastActor.
        result.expect("delivery failure reports must not fail the CastActor");
    }

    // CA-7 (domain lifecycle).
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
    async fn test_cast_message_delivery_slice() {
        let config = hyperactor_config::global::lock();
        let _guard = config.override_key(
            hyperactor::config::ENABLE_DEST_ACTOR_REORDERING_BUFFER,
            true,
        );

        let n = 8;
        let mut test_mesh = CastTestMesh::new(n);
        test_mesh.spawn_delivery_receivers();

        let slice_region = Region::from(shape!(a = 2, b = 2, c = 2))
            .range("a", ndslice::Range(1, Some(2), 1))
            .unwrap();
        let slice_mesh = test_mesh.sliced(slice_region.clone());
        let slice_domain =
            slice_mesh.root_domain_with_policy(slice_region, TilingPolicy::BlockPartitioning);

        let expected_payloads = vec![
            "hello-0".to_string(),
            "hello-1".to_string(),
            "hello-2".to_string(),
        ];
        for payload in &expected_payloads {
            slice_domain
                .cast(
                    &test_mesh.client,
                    Flattrs::new(),
                    TestDelivery {
                        payload: payload.clone(),
                    },
                )
                .unwrap();
        }

        let expected_histories: BTreeMap<String, Vec<String>> = [
            "proc_4".to_string(),
            "proc_5".to_string(),
            "proc_6".to_string(),
            "proc_7".to_string(),
        ]
        .into_iter()
        .map(|proc_name| (proc_name, expected_payloads.clone()))
        .collect();
        let histories = cast_and_collect_histories(&slice_mesh.client, &slice_domain).await;

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
                    "proc {proc_name} changed lineage across slice casts"
                );
            }
            lineage_by_proc.insert(proc_name, first_lineage);
        }

        let relays = test_mesh
            .member_ids
            .values()
            .map(|member| CastActor::ref_for_proc(member.proc_addr()).into_actor_addr())
            .collect::<BTreeSet<_>>();

        let destinations = test_mesh
            .receiver_ids
            .iter()
            .enumerate()
            .map(|(rank, actor)| (format!("proc_{rank}"), actor))
            .collect::<BTreeMap<_, _>>();

        for (proc_name, lineage) in lineage_by_proc {
            let (destination, relay_lineage) = lineage
                .split_last()
                .expect("every delivery lineage must include its destination");

            assert_eq!(destination, destinations[&proc_name]);
            assert!(
                relay_lineage.iter().all(|actor| relays.contains(actor)),
                "lineage before {proc_name} must contain only CastActors"
            );
        }
    }

    // CA-4 (destination ordering) and CA-6 (domain isolation).
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

        let rank_0_to_2_region = Region::from(shape!(rank = 4))
            .range("rank", ndslice::Range(0, Some(2), 1))
            .unwrap();
        let rank_0_to_2 = test_mesh
            .sliced(rank_0_to_2_region.clone())
            .root_domain_with_policy(rank_0_to_2_region, TilingPolicy::BlockPartitioning);

        let rank_1_to_3_region = Region::from(shape!(rank = 4))
            .range("rank", ndslice::Range(1, Some(3), 1))
            .unwrap();
        let rank_1_to_3 = test_mesh
            .sliced(rank_1_to_3_region.clone())
            .root_domain_with_policy(rank_1_to_3_region, TilingPolicy::BlockPartitioning);

        let rank_2_to_4_region = Region::from(shape!(rank = 4))
            .range("rank", ndslice::Range(2, Some(4), 1))
            .unwrap();
        let rank_2_to_4 = test_mesh
            .sliced(rank_2_to_4_region.clone())
            .root_domain_with_policy(rank_2_to_4_region, TilingPolicy::BlockPartitioning);

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
            cast_and_collect_histories(&test_mesh.client, &root)
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

    pub(crate) fn collect_split_port(original: &PortAddr, split: &PortAddr) {
        let mut guard = split_port_tree().lock().unwrap();
        let Some(recording) = guard.as_mut() else {
            return;
        };
        if original != &recording.root && !recording.edges.iter().any(|edge| &edge.to == original) {
            return;
        }
        let is_leaf = original != &recording.root;

        recording.edges.push(SplitEdge {
            from: original.clone(),
            to: split.clone(),
            is_leaf,
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
        let child_to_parent = edges
            .iter()
            .map(|edge| (edge.to.clone(), edge.from.clone()))
            .collect::<HashMap<_, _>>();

        edges
            .iter()
            .filter(|edge| edge.is_leaf)
            .map(|edge| {
                let mut path = vec![edge.to.clone()];
                let mut current = edge.to.clone();
                while let Some(parent) = child_to_parent.get(&current) {
                    path.push(parent.clone());
                    current = parent.clone();
                }
                path.reverse();

                (edge.to.clone(), path)
            })
            .collect()
    }

    /// Extract the proc name (rank) from each `PortAddr` in a split-port path,
    /// stripping leading sender entries before the path enters the domain.
    fn split_path_ranks(
        paths: &BTreeMap<PortAddr, Vec<PortAddr>>,
        rank_lookup: &HashMap<String, usize>,
    ) -> BTreeMap<usize, Vec<usize>> {
        paths
            .iter()
            .map(|(leaf, path)| {
                let ranks = path
                    .iter()
                    .map(|port| port.actor_addr().proc_addr().log_name().to_string())
                    .skip_while(|proc_name| !rank_lookup.contains_key(proc_name))
                    .map(|proc_name| {
                        *rank_lookup
                            .get(&proc_name)
                            .unwrap_or_else(|| panic!("unknown proc {proc_name} in split path"))
                    })
                    .collect();
                let leaf_proc = leaf.actor_addr().proc_addr().log_name().to_string();

                (rank_lookup[&leaf_proc], ranks)
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

    /// A message with a reply port for testing port splitting.
    #[derive(Debug, Clone, Serialize, Deserialize, typeuri::Named)]
    struct TestRequestWithReply {
        payload: String,
        reply_to: hyperactor::OncePortRef<TestReplyCounts>,
    }
    wirevalue::register_type!(TestRequestWithReply);

    #[derive(Debug, Clone, Serialize, Deserialize, typeuri::Named)]
    struct TestIdleFlushRequestWithReply {
        delayed_proc: String,
        delay: Duration,
        replies_per_destination: usize,
        reply_to: IdleFlushPortRef<TestReplyCounts>,
    }
    wirevalue::register_type!(TestIdleFlushRequestWithReply);

    /// An actor that receives a cast message and sends a reply back
    /// through the (potentially split) reply port.
    #[derive(Debug, Default)]
    #[hyperactor::export(
        handlers = [TestRequestWithReply, TestIdleFlushRequestWithReply],
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

    #[async_trait]
    impl Handler<TestIdleFlushRequestWithReply> for SplitPortReceiver {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            msg: TestIdleFlushRequestWithReply,
        ) -> Result<(), anyhow::Error> {
            let proc_name = cx.self_addr().proc_addr().log_name().to_string();

            if proc_name == msg.delayed_proc {
                tokio::time::sleep(msg.delay).await;
            }

            for _ in 0..msg.replies_per_destination {
                msg.reply_to
                    .post(cx, TestReplyCounts::single(proc_name.clone()));
            }

            Ok(())
        }
    }

    /// CA-5 (route independence): verify that port splitting rewrites reply
    /// ports to mirror the cast tree and returns replies to the original sender.
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

        let rank_lookup = (0..n)
            .map(|rank| (format!("proc_{rank}"), rank))
            .collect::<HashMap<_, _>>();
        let rank_paths = split_path_ranks(
            &build_split_paths(&split_port_recording.edges()),
            &rank_lookup,
        );
        let expected_paths = (0..n)
            .map(|rank| (rank, vec![rank]))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(rank_paths, expected_paths);
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_idle_flush_split_reducer_preserves_late_reply() {
        let n = 8;
        let mut test_mesh = CastTestMesh::new(n);
        test_mesh.spawn_split_port_receivers();
        let root_domain = test_mesh.root_domain(shape!(a = 2, b = 2, c = 2).into());

        let (reply_handle, mut reply_rx) = context::Mailbox::mailbox(&test_mesh.client)
            .open_accum_port(TestReplyCountsAccumulator);
        let reply_to = reply_handle.bind().into_idle_flush(IdleFlushReducerOpts {
            idle_timeout: Duration::from_millis(50),
            abandon_timeout: Duration::from_secs(30),
            expected_updates_per_destination: NonZeroUsize::new(2).expect("2 is non-zero").into(),
        });
        let split_port_recording = record_split_port_tree(reply_to.port_addr().clone());

        root_domain
            .cast(
                &test_mesh.client,
                Flattrs::new(),
                TestIdleFlushRequestWithReply {
                    delayed_proc: "proc_7".to_string(),
                    delay: Duration::from_millis(1500),
                    replies_per_destination: 2,
                    reply_to,
                },
            )
            .unwrap();

        let partial = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let reply = reply_rx.recv().await.unwrap();
                if reply.counts_by_proc.len() == n - 1 {
                    break reply;
                }
            }
        })
        .await
        .expect("responsive subtrees should flush before the delayed reply");

        assert_eq!(
            partial.counts_by_proc,
            [
                "proc_0", "proc_1", "proc_2", "proc_3", "proc_4", "proc_5", "proc_6"
            ]
            .into_iter()
            .map(|proc_name| (proc_name.to_string(), 2))
            .collect()
        );

        let complete = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let reply = reply_rx.recv().await.unwrap();
                if reply.counts_by_proc.len() == n {
                    break reply;
                }
            }
        })
        .await
        .expect("the late reply should complete the idle-flush reduction");

        assert_eq!(
            complete.counts_by_proc,
            (0..n).map(|rank| (format!("proc_{rank}"), 2)).collect()
        );

        let rank_lookup = (0..n)
            .map(|rank| (format!("proc_{rank}"), rank))
            .collect::<HashMap<_, _>>();
        let rank_paths = split_path_ranks(
            &build_split_paths(&split_port_recording.edges()),
            &rank_lookup,
        );
        let expected_paths = (0..n)
            .map(|rank| (rank, vec![rank]))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(rank_paths, expected_paths);
    }

    // Tests that a serialized BoundedFanout policy drives root-heaved
    // cast-domain setup end to end.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_root_heaved_bounded_fanout_installs_expected_hops() {
        clear_captured_domains();

        // GIVEN: an 8-rank domain with fanout 2.
        let test_mesh = CastTestMesh::new(8);

        // WHEN: the domain is materialized.
        let root_domain = test_mesh.root_domain_with_policy(
            shape!(a = 8).into(),
            TilingPolicy::BoundedFanout {
                fanout: NonZeroUsize::new(2).unwrap(),
            },
        );
        let snapshots = test_mesh
            .wait_for_domain_snapshots(root_domain.domain_id(), 8)
            .await;

        // THEN: the caller seeds the root relay and two heaved relay subtrees.
        assert_eq!(
            root_domain
                .subtrees
                .iter()
                .map(|subtree| {
                    subtree
                        .cast_actor
                        .actor_addr()
                        .proc_addr()
                        .log_name()
                        .to_string()
                })
                .collect::<BTreeSet<_>>(),
            ["proc_0", "proc_1", "proc_5"]
                .into_iter()
                .map(str::to_string)
                .collect()
        );

        assert_eq!(
            snapshots.keys().cloned().collect::<BTreeSet<_>>(),
            (0..8).map(|rank| format!("proc_{rank}")).collect()
        );

        for rank in 0..8 {
            let proc_name = format!("proc_{rank}");
            let snapshot = snapshots
                .get(&proc_name)
                .unwrap_or_else(|| panic!("missing snapshot for {proc_name}"));

            assert_eq!(
                snapshot.direct_hop_procs,
                [proc_name.clone()].into_iter().collect()
            );
            assert!(snapshot.next_hop_procs.len() <= 3);
        }
    }

    // Tests that a serialized Bisection policy drives root-heaved cast-domain
    // setup end to end.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_root_heaved_bisection_installs_expected_hops() {
        clear_captured_domains();

        // GIVEN: an 8-rank domain with bisection tiling.
        let test_mesh = CastTestMesh::new(8);

        // WHEN: the domain is materialized.
        let root_domain =
            test_mesh.root_domain_with_policy(shape!(a = 8).into(), TilingPolicy::Bisection);
        let snapshots = test_mesh
            .wait_for_domain_snapshots(root_domain.domain_id(), 8)
            .await;

        // THEN: the caller seeds the root relay and three heaved relay subtrees.
        assert_eq!(
            root_domain
                .subtrees
                .iter()
                .map(|subtree| {
                    subtree
                        .cast_actor
                        .actor_addr()
                        .proc_addr()
                        .log_name()
                        .to_string()
                })
                .collect::<BTreeSet<_>>(),
            ["proc_0", "proc_1", "proc_2", "proc_4"]
                .into_iter()
                .map(str::to_string)
                .collect()
        );

        assert_eq!(
            snapshots.keys().cloned().collect::<BTreeSet<_>>(),
            (0..8).map(|rank| format!("proc_{rank}")).collect()
        );

        for rank in 0..8 {
            let proc_name = format!("proc_{rank}");
            let snapshot = snapshots
                .get(&proc_name)
                .unwrap_or_else(|| panic!("missing snapshot for {proc_name}"));

            assert_eq!(
                snapshot.direct_hop_procs,
                [proc_name.clone()].into_iter().collect()
            );
        }
    }
}
