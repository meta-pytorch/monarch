/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

pub mod mesh_agent;

use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;

use hyperactor::ActorRef;
use hyperactor::Named;
use hyperactor::ProcId;
use hyperactor::channel::ChannelAddr;
use hyperactor::context;
use ndslice::Extent;
use ndslice::Region;
use ndslice::view;
use ndslice::view::Ranked;
use ndslice::view::RegionParseError;
use serde::Deserialize;
use serde::Serialize;

use crate::alloc::Alloc;
use crate::resource::CreateOrUpdateClient;
use crate::v1;
use crate::v1::Name;
use crate::v1::ProcMesh;
use crate::v1::ProcMeshRef;
use crate::v1::host_mesh::mesh_agent::HostMeshAgent;
use crate::v1::host_mesh::mesh_agent::HostMeshAgentProcMeshTrampoline;
use crate::v1::proc_mesh::ProcRef;

/// A reference to a single host.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Named, Serialize, Deserialize)]
pub struct HostRef(ChannelAddr);

impl HostRef {
    /// The host mesh agent associated with this host.
    fn mesh_agent(&self) -> ActorRef<HostMeshAgent> {
        ActorRef::attest(self.service_proc().actor_id("agent", 0))
    }

    /// The ProcId for the proc with name `name` on this host.
    fn named_proc(&self, name: &Name) -> ProcId {
        ProcId::Direct(self.0.clone(), name.to_string())
    }

    /// The service proc on this host.
    fn service_proc(&self) -> ProcId {
        ProcId::Direct(self.0.clone(), "service".to_string())
    }
}

impl std::fmt::Display for HostRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for HostRef {
    type Err = <ChannelAddr as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(HostRef(ChannelAddr::from_str(s)?))
    }
}

/// An owned mesh of hosts.
pub struct HostMesh {
    name: Name,
    extent: Extent,
    allocation: HostMeshAllocation,
    current_ref: HostMeshRef,
}

enum HostMeshAllocation {
    /// The host mesh was bootstrapped from a proc mesh.
    /// This is to support providing host meshes through Allocs.
    ProcMesh {
        proc_mesh: ProcMesh,
        proc_mesh_ref: ProcMeshRef,
        hosts: Vec<HostRef>,
    },
}

impl HostMesh {
    /// Allocate a host mesh from an [`Alloc`]. This creates a HostMesh with the same extent
    /// as the provided alloc. Allocs generate procs, and thus we define and run a Host for each
    /// proc allocated by it.
    ///
    /// ## Allocation strategy
    ///
    /// Because HostMeshes use direct-addressed procs, and must fully control the procs they are
    /// managing, `HostMesh::allocate` uses a trampoline actor to launch the host, which in turn
    /// runs a [`crate::v1::host_mesh::mesh_agent::HostMeshAgent`] actor to manage the host itself.
    /// The host (and thus all of its procs) are exposed directly through a separate listening
    /// channel, established by the host.
    ///
    /// ```text
    ///                        ┌ ─ ─┌────────────────────┐                   
    ///                             │allocated Proc:     │                   
    ///                        │    │ ┌─────────────────┐│                   
    ///                             │ │TrampolineActor  ││                   
    ///                        │    │ │ ┌──────────────┐││                   
    ///                             │ │ │Host          │││                   
    ///               ┌────┬ ─ ┘    │ │ │ ┌──────────┐ │││                   
    ///            ┌─▶│Proc│        │ │ │ │HostAgent │ │││                   
    ///            │  └────┴ ─ ┐    │ │ │ └──────────┘ │││                   
    ///            │  ┌────┐        │ │ │             ██████                 
    /// ┌────────┐ ├─▶│Proc│   │    │ │ └──────────────┘││ ▲                 
    /// │ Client │─┤  └────┘        │ └─────────────────┘│ listening channel
    /// └────────┘ │  ┌────┐   └ ─ ─└────────────────────┘                   
    ///            ├─▶│Proc│                                                 
    ///            │  └────┘                                                 
    ///            │  ┌────┐                                                 
    ///            └─▶│Proc│                                                 
    ///               └────┘                                                 
    ///                 ▲                                                    
    ///                                                                     
    ///          `Alloc`-provided                                            
    ///                procs               
    /// ```                                  
    pub async fn allocate(
        cx: &impl context::Actor,
        alloc: impl Alloc + Send + Sync + 'static,
        name: &str,
    ) -> v1::Result<Self> {
        let transport = alloc.transport();
        let extent = alloc.extent().clone();
        let proc_mesh = ProcMesh::allocate(cx, Box::new(alloc), name).await?;
        let name = Name::new(name);

        // TODO: figure out how to deal with MAST allocs. It requires an extra dimension,
        // into which it launches multiple procs, so we need to always specify an additional
        // sub-host dimension of size 1.

        let (mesh_agents, mut mesh_agents_rx) = cx.mailbox().open_port();
        let _trampoline_actor_mesh = proc_mesh
            .spawn::<HostMeshAgentProcMeshTrampoline>(
                cx,
                "host_mesh_trampoline",
                &(transport, mesh_agents.bind()),
            )
            .await?;

        // TODO: don't re-rank the hosts
        let mut hosts = Vec::new();
        for _rank in 0..extent.num_ranks() {
            let mesh_agent = mesh_agents_rx.recv().await?;

            let Some((addr, _)) = mesh_agent.actor_id().proc_id().as_direct() else {
                return Err(v1::Error::HostMeshAgentConfigurationError(
                    mesh_agent.actor_id().clone(),
                    "host mesh agent must be a direct actor".to_string(),
                ));
            };

            let host_ref = HostRef(addr.clone());
            if host_ref.mesh_agent() != mesh_agent {
                return Err(v1::Error::HostMeshAgentConfigurationError(
                    mesh_agent.actor_id().clone(),
                    format!(
                        "expected mesh agent actor id to be {}",
                        host_ref.mesh_agent().actor_id()
                    ),
                ));
            }
            hosts.push(host_ref);
        }

        let proc_mesh_ref = proc_mesh.clone();
        Ok(Self {
            name,
            extent: extent.clone(),
            allocation: HostMeshAllocation::ProcMesh {
                proc_mesh,
                proc_mesh_ref,
                hosts: hosts.clone(),
            },
            current_ref: HostMeshRef::new(extent.into(), hosts).unwrap(),
        })
    }
}

impl Deref for HostMesh {
    type Target = HostMeshRef;

    fn deref(&self) -> &Self::Target {
        &self.current_ref
    }
}

/// A reference to a mesh of hosts. Logically this is a data structure that
/// contains a set of ranked hosts organized into a [`Region`]. HostMeshRefs
/// can be sliced to produce new HostMeshRefs that contain a subset of the
/// hosts in the original mesh.
///
/// HostMeshRefs have a concrete syntax, implemented by its `Display` and `FromStr`
/// implementations.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Named, Serialize, Deserialize)]
pub struct HostMeshRef {
    region: Region,
    ranks: Arc<Vec<HostRef>>,
}

impl HostMeshRef {
    /// Create a new (raw) HostMeshRef from the provided region and associated
    /// ranks, which must match in cardinality.
    fn new(region: Region, ranks: Vec<HostRef>) -> v1::Result<Self> {
        if region.num_ranks() != ranks.len() {
            return Err(v1::Error::InvalidRankCardinality {
                expected: region.num_ranks(),
                actual: ranks.len(),
            });
        }
        Ok(Self {
            region,
            ranks: Arc::new(ranks),
        })
    }

    /// Spawn a ProcMesh onto this host mesh.
    // TODO: add an "additional dims" API
    pub async fn spawn(&self, cx: &impl context::Actor, name: &str) -> v1::Result<ProcMesh> {
        let name = Name::new(name);
        let mut procs = Vec::new();
        for (rank, host) in self.ranks.iter().enumerate() {
            let ok = host
                .mesh_agent()
                .create_or_update(cx, name.clone(), ())
                .await
                .map_err(|e| {
                    v1::Error::HostMeshAgentConfigurationError(
                        host.mesh_agent().actor_id().clone(),
                        format!("failed while creating proc: {}", e),
                    )
                })?;
            procs.push(ProcRef::new(
                host.named_proc(&name),
                rank,
                // TODO: specify or retrieve from state instead, to avoid attestation.
                ActorRef::attest(host.named_proc(&name).actor_id("agent", 0)),
            ));
        }

        ProcMesh::create_owned_unchecked(cx, name, self.clone(), procs).await
    }
}

impl view::Ranked for HostMeshRef {
    type Item = HostRef;

    fn region(&self) -> &Region {
        &self.region
    }

    fn get(&self, rank: usize) -> Option<&Self::Item> {
        self.ranks.get(rank)
    }
}

impl view::RankedSliceable for HostMeshRef {
    fn sliced(&self, region: Region) -> Self {
        let ranks = self
            .region()
            .remap(&region)
            .unwrap()
            .map(|index| self.get(index).unwrap().clone());
        Self::new(region, ranks.collect()).unwrap()
    }
}

impl std::fmt::Display for HostMeshRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (rank, host) in self.ranks.iter().enumerate() {
            if rank > 0 {
                write!(f, ",")?;
            }
            write!(f, "{}", host)?;
        }
        write!(f, "@{}", self.region)
    }
}

/// The type of error occuring during `HostMeshRef` parsing.
#[derive(thiserror::Error, Debug)]
pub enum HostMeshRefParseError {
    #[error(transparent)]
    RegionParseError(#[from] RegionParseError),

    #[error("invalid host mesh ref: missing region")]
    MissingRegion,

    #[error(transparent)]
    InvalidHostMeshRef(#[from] Box<v1::Error>),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<v1::Error> for HostMeshRefParseError {
    fn from(err: v1::Error) -> Self {
        Self::InvalidHostMeshRef(Box::new(err))
    }
}

impl FromStr for HostMeshRef {
    type Err = HostMeshRefParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (hosts, region) = s
            .split_once('@')
            .ok_or(HostMeshRefParseError::MissingRegion)?;
        let hosts = hosts
            .split(',')
            .map(|host| host.trim())
            .map(|host| host.parse::<HostRef>())
            .collect::<Result<Vec<_>, _>>()?;
        let region = region.parse()?;
        Ok(HostMeshRef::new(region, hosts)?)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::collections::VecDeque;

    use hyperactor::PortRef;
    use hyperactor::context::Mailbox as _;
    use itertools::Itertools;
    use ndslice::ViewExt;
    use ndslice::extent;
    use tokio::process::Command;

    use super::*;
    use crate::alloc::AllocSpec;
    use crate::alloc::Allocator;
    use crate::alloc::ProcessAllocator;
    use crate::v1::ActorMesh;
    use crate::v1::ActorMeshRef;
    use crate::v1::testactor;
    use crate::v1::testing;

    #[test]
    fn test_host_mesh_subset() {
        let hosts: HostMeshRef = "local:1,local:2,local:3,local:4@replica=2/2,host=2/1"
            .parse()
            .unwrap();
        assert_eq!(
            hosts.range("replica", 1).unwrap().to_string(),
            "local:3,local:4@2+replica=1/2,host=2/1"
        );
    }

    #[test]
    fn test_host_mesh_ref_parse_roundtrip() {
        let host_mesh_ref = HostMeshRef::new(
            extent!(replica = 2, host = 2).into(),
            vec![
                "tcp:127.0.0.1:123".parse().unwrap(),
                "tcp:127.0.0.1:123".parse().unwrap(),
                "tcp:127.0.0.1:123".parse().unwrap(),
                "tcp:127.0.0.1:123".parse().unwrap(),
            ],
        )
        .unwrap();

        assert_eq!(
            host_mesh_ref.to_string().parse::<HostMeshRef>().unwrap(),
            host_mesh_ref
        );
    }

    #[tokio::test]
    async fn test_allocate() {
        // This only works with the process allocator since we assume a working
        // bootstrap binary.
        //
        // TODO: have multiple trampoline modes (self_exe, etc.)

        let instance = testing::instance().await;

        let mut allocator = ProcessAllocator::new(Command::new(
            buck_resources::get("monarch/hyperactor_mesh/bootstrap").unwrap(),
        ));
        let alloc = allocator
            .allocate(AllocSpec {
                extent: extent!(replicas = 4),
                constraints: Default::default(),
                proc_name: None,
            })
            .await
            .unwrap();

        let host_mesh = HostMesh::allocate(instance, alloc, "test").await.unwrap();
        let proc_mesh1 = host_mesh.spawn(instance, "test_1").await.unwrap();
        let actor_mesh1: ActorMesh<testactor::TestActor> =
            proc_mesh1.spawn(instance, "test", &()).await.unwrap();
        let proc_mesh2 = host_mesh.spawn(instance, "test_2").await.unwrap();
        let actor_mesh2: ActorMesh<testactor::TestActor> =
            proc_mesh2.spawn(instance, "test", &()).await.unwrap();

        // Host meshes can be dereferenced to produce a concrete ref.
        let host_mesh_ref: HostMeshRef = host_mesh.clone();
        // Here, the underlying host mesh does not change:
        assert_eq!(
            host_mesh_ref.iter().collect::<Vec<_>>(),
            host_mesh.iter().collect::<Vec<_>>(),
        );

        // Validate we can cast:

        let (port, mut rx) = instance.mailbox().open_port();
        actor_mesh1
            .cast(instance, testactor::GetActorId(port.bind()))
            .unwrap();

        let mut expected_actor_ids: HashSet<_> = actor_mesh1
            .values()
            .map(|actor_ref| actor_ref.actor_id().clone())
            .collect();

        while !expected_actor_ids.is_empty() {
            let actor_id = rx.recv().await.unwrap();
            assert!(
                expected_actor_ids.remove(&actor_id),
                "got {actor_id}, expect {expected_actor_ids:?}"
            );
        }

        // Now forward a message through all directed edges across the two meshes.
        // This tests the full connectivity of all the hosts, procs, and actors
        // involved in these two meshes.
        let mut to_visit: VecDeque<_> = actor_mesh1
            .values()
            .chain(actor_mesh2.values())
            .map(|actor_ref| actor_ref.port())
            // Each ordered pair of ports
            .permutations(2)
            // Flatten them to create a path:
            .flatten()
            .collect();

        let expect_visited: Vec<_> = to_visit.clone().into();

        // We are going to send to the first, and then set up a port to receive the last.
        let (last, mut last_rx) = instance.mailbox().open_port();
        to_visit.push_back(last.bind());

        let forward = testactor::Forward {
            to_visit,
            visited: Vec::new(),
        };
        let first = forward.to_visit.front().unwrap().clone();
        first.send(instance, forward).unwrap();

        let forward = last_rx.recv().await.unwrap();
        assert_eq!(forward.visited, expect_visited);
    }
}
