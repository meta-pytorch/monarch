/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The CastActor: a system actor that bootstraps and manages casting domains.

use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorAddr;
use hyperactor::ActorRef;
use hyperactor::Client;
use hyperactor::Context;
use hyperactor::Endpoint as _;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::Label;
use hyperactor::Uid;
use hyperactor::context;
use ndslice::Point;
use ndslice::Region;
use ndslice::Shape;
use serde::Deserialize;
use serde::Serialize;

use crate::tile::MaterializedTile;
use crate::tile::Tile;
use crate::tile::TilingPolicy;

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
    /// `members` maps domain rank to member actor address. `shape` describes
    /// the logical root shape of the domain; tiling and communication are
    /// derived internally.
    pub fn materialize(
        self,
        cx: &impl context::Actor,
        members: HashMap<usize, ActorAddr>,
        shape: Shape,
        tiling_policy: TilingPolicy,
    ) -> anyhow::Result<CastDomainRef> {
        let region = Region::from(shape);
        anyhow::ensure!(
            members.len() == region.num_ranks()
                && region
                    .slice()
                    .iter()
                    .all(|rank| members.contains_key(&rank)),
            "members must contain exactly one actor address for every domain rank"
        );

        let root_rank = region.slice().offset();
        let entry_point = &members[&root_rank];
        let domain_ref =
            CastDomainRef::from_entry_point(self.clone(), cast_actor_ref_for_member(entry_point));
        let root_tile = MaterializedTile::from_map(Tile::from_view(&region), members);
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
}

impl CastDomainRef {
    /// Rebuild a cast-domain handle from a pure id plus its entry-point ref.
    fn from_entry_point(id: CastDomainId, entry_point: ActorRef<CastActor>) -> Self {
        Self { id, entry_point }
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
const CAST_ACTOR_NAME: &str = "cast";

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

#[async_trait]
impl Actor for CastActor {
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        this.set_system();
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
            num_members = message.tile.len(),
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::OnceLock;
    use std::time::Duration;

    use hyperactor::ProcAddr;
    use hyperactor::channel::ChannelTransport;
    use hyperactor::proc::Proc;
    use ndslice::Shape;
    use ndslice::Slice;
    use ndslice::shape;
    use proptest::prelude::*;
    use timed_test::async_timed_test;

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

    fn installed_domains() -> &'static Mutex<HashMap<Uid, BTreeMap<String, CastHopSnapshot>>> {
        INSTALLED_DOMAINS.get_or_init(|| Mutex::new(HashMap::new()))
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

    fn clear_captured_domains() {
        installed_domains().lock().unwrap().clear();
    }

    fn captured_domain_snapshots(domain_id: &Uid) -> BTreeMap<String, CastHopSnapshot> {
        installed_domains()
            .lock()
            .unwrap()
            .get(domain_id)
            .cloned()
            .unwrap_or_default()
    }

    struct CastTestMesh {
        _client_proc: Proc,
        client: Client,
        _procs: Vec<Proc>,
        member_ids: HashMap<usize, ActorAddr>,
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
            }
        }

        fn root_domain(&self, shape: Shape) -> CastDomainRef {
            CastDomainId::new()
                .materialize(
                    &self.client,
                    self.member_ids.clone(),
                    shape,
                    TilingPolicy::BlockPartitioning,
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
            let root_tile = MaterializedTile::from_map(Tile::from_view(&region), members.clone());

            let mut seen_roots = BTreeSet::new();
            validate_domain_tree(&members, &root_tile, &mut seen_roots)?;

            let expected_roots = region.slice().iter().collect::<BTreeSet<_>>();
            prop_assert_eq!(seen_roots, expected_roots);
        }
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_create_cast_domain_installs_expected_hops() {
        clear_captured_domains();

        let test_mesh = CastTestMesh::new(8);
        let root_domain = test_mesh.root_domain(shape!(a = 2, b = 2, c = 2));
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
}
