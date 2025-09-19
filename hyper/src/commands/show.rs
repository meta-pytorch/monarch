/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::io;
use std::io::Write;

use anyhow::Context;
use chrono::DateTime;
use chrono::Local;
#[cfg(not(fbcode_build))]
use hyper::utils::system_address::SystemAddr;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor::proc::ActorTreeSnapshot;
use hyperactor::reference::Index;
use hyperactor::reference::Reference;
use hyperactor_multiprocess::proc_actor::ProcActor;
use hyperactor_multiprocess::proc_actor::ProcMessageClient;
use hyperactor_multiprocess::system_actor::SYSTEM_ACTOR_REF;
use hyperactor_multiprocess::system_actor::SystemMessageClient;
use hyperactor_multiprocess::system_actor::SystemSnapshotFilter;
use tabwriter::TabWriter;
#[cfg(fbcode_build)]
use utils::system_address::SystemAddr;

#[derive(clap::Args, Debug)]
pub struct ShowCommand {
    /// The address of the system. Can be a channel address or a MAST job name.
    system_addr: SystemAddr,

    /// The string repsentation of what we want to show, such as world, proc,
    /// actor, etc. Default to system if not provided.
    reference: Option<Reference>,
}

impl ShowCommand {
    pub async fn run(self) -> anyhow::Result<()> {
        let mut system = hyperactor_multiprocess::System::new(self.system_addr.clone().into());
        let client = system.attach().await.expect("failed to attach to system");

        match self.reference {
            None => {
                let snapshot = SYSTEM_ACTOR_REF
                    .snapshot(&client, SystemSnapshotFilter::all())
                    .await
                    .expect("failed to snapshot system");

                let mut tw = TabWriter::new(io::stdout());
                write!(tw, "Execution ID: {}\n", snapshot.execution_id)?;
                for (world, world_snapshot) in snapshot.worlds {
                    write!(tw, "{}\t{}\n", world, world_snapshot.status)?;
                }
                tw.flush()?;
            }
            Some(Reference::World(world_id)) => {
                let snapshot = SYSTEM_ACTOR_REF
                    .snapshot(&client, SystemSnapshotFilter::all())
                    .await
                    .expect("failed to snapshot system");

                let world = snapshot.worlds.get(&world_id).expect("world not found");
                let mut tw = TabWriter::new(io::stdout());
                write!(tw, "status: {}\n", world.status)?;
                if !world.labels.is_empty() {
                    write!(tw, "labels:\n")?;
                    for (label, value) in &world.labels {
                        write!(tw, "\t{}={}\n", label, value)?;
                    }
                }

                let mut procs: Vec<_> = world.procs.keys().collect();
                if procs.is_empty() {
                    write!(tw, "(no procs)\n")?;
                } else {
                    procs.sort();
                    write!(tw, "procs:\n")?;
                    for proc_id in procs {
                        write!(tw, "\t{}\n", proc_id)?;
                    }
                }
                tw.flush()?;
            }
            Some(Reference::Proc(proc_id)) => {
                // TODO: we should really look up the proc actor from the system
                let proc_ref: ActorRef<ProcActor> = ActorRef::attest(proc_id.actor_id("proc", 0));
                let snapshot = proc_ref
                    .snapshot(&client)
                    .await
                    .expect("failed to snapshot proc");
                let mut tw = TabWriter::new(io::stdout());
                write!(tw, "state: {}\n", snapshot.state)?;
                tw.flush()?; // this also resets tabwriter's alignment
                let mut roots: Vec<_> = snapshot.actors.roots.keys().collect();
                if roots.is_empty() {
                    write!(tw, "(no actors)\n")?;
                } else {
                    roots.sort();
                    for root_actor_id in roots {
                        let tree = &snapshot.actors.roots[root_actor_id];
                        write!(
                            tw,
                            "{}\t{}\t{}\t{}\n",
                            root_actor_id, tree.type_name, tree.status, tree.stats
                        )?;
                    }
                }
                tw.flush()?;
            }
            Some(Reference::Actor(actor_id)) => {
                let proc_id = actor_id.proc_id();
                // TODO: we should really look up the proc actor from the system
                let proc_ref: ActorRef<ProcActor> = ActorRef::attest(proc_id.actor_id("proc", 0));
                let snapshot = proc_ref
                    .snapshot(&client)
                    .await
                    .expect("failed to snapshot proc");
                let mut tw = TabWriter::new(io::stdout());
                let root_actor_id = actor_id.child_id(0);
                let tree = snapshot.actors.roots.get(&root_actor_id).with_context(|| {
                    format!("root actor {} not found in proc {}", root_actor_id, proc_id)
                })?;
                let tree = Self::find_tree(tree, actor_id.pid()).with_context(|| {
                    format!(
                        "actor {} not found in tree rooted at {}",
                        actor_id, root_actor_id
                    )
                })?;

                let actor_id = root_actor_id.child_id(tree.pid);
                write!(tw, "{}:\n", actor_id)?;
                write!(tw, "status: {}\n", tree.status)?;
                write!(tw, "type: {}\n", tree.type_name)?;
                write!(tw, "stats: {}\n", tree.stats)?;
                write!(tw, "handlers:\n")?;
                for (port, name) in tree.handlers.iter() {
                    write!(tw, "\t{}: {}\n", actor_id.port_id(*port), name)?;
                }
                write!(tw, "children:\n")?;
                for (pid, _) in tree.children.iter() {
                    write!(tw, "\t{}\n", root_actor_id.child_id(*pid))?;
                }
                write!(tw, "events:\n")?;
                for event in tree.events.iter() {
                    let map: serde_json::Map<_, _> = event
                        .fields
                        .iter()
                        .map(|(key, value)| (key.clone(), value.to_json()))
                        .collect();
                    let dt: DateTime<Local> = event.time.into();

                    write!(
                        tw,
                        "{}\t{:8}\t{}\n",
                        dt,
                        event.seq,
                        serde_json::to_string(&map)?
                    )?;
                }
                write!(tw, "spans:\n")?;
                for spans in tree.spans.iter() {
                    let mut iter = spans.iter();
                    let Some(first) = iter.next() else {
                        continue;
                    };
                    write!(tw, "\t{}\n", first)?;
                    for frame in iter {
                        write!(tw, "\t\t{}\n", frame)?;
                    }
                }

                tw.flush()?;
            }
            Some(Reference::Port(_)) => {
                anyhow::bail!("'hyper show port' not yet implemented");
            }
            Some(Reference::Gang(gang_id)) => {
                let snapshot = SYSTEM_ACTOR_REF
                    .snapshot(&client, SystemSnapshotFilter::all())
                    .await
                    .expect("failed to snapshot system");

                let world = snapshot
                    .worlds
                    .get(gang_id.world_id())
                    .cloned()
                    .context("world not found")?;

                // Do in serial to avoid sharing client instance.
                let mut procs = Vec::new();
                for proc_id in world.procs.keys() {
                    let proc_ref: ActorRef<ProcActor> =
                        ActorRef::attest(proc_id.actor_id("proc", 0));
                    procs.push((
                        proc_id,
                        RealClock
                            .timeout(
                                std::time::Duration::from_secs(5),
                                proc_ref.snapshot(&client),
                            )
                            .await,
                    ));
                }

                let mut procs: Vec<_> = procs
                    .into_iter()
                    .filter_map(|(proc_id, result)| match result {
                        Ok(Ok(snapshot)) => Some((proc_id.clone(), snapshot)),
                        Err(elapsed) => {
                            eprintln!(
                                "failed to snapshot proc {}: timeout after {}",
                                proc_id, elapsed
                            );
                            None
                        }
                        Ok(Err(err)) => {
                            eprintln!("failed to snapshot proc {}: {}", proc_id, err);
                            None
                        }
                    })
                    .collect();

                let mut tw = TabWriter::new(io::stdout());
                procs.sort_by_key(|(proc_id, _)| proc_id.clone());
                for (proc_id, snapshot) in procs {
                    let root_actor_id = proc_id.actor_id(gang_id.name(), 0);
                    let Some(tree) = snapshot.actors.roots.get(&root_actor_id) else {
                        eprintln!("root actor {} not found in proc {}", root_actor_id, proc_id);
                        continue;
                    };

                    write!(
                        tw,
                        "{}\t{}\t{}\t{}\n",
                        root_actor_id, tree.type_name, tree.status, tree.stats
                    )?;
                }
                tw.flush()?;
            }
        }

        Ok(())
    }

    fn find_tree(snapshot: &ActorTreeSnapshot, pid: Index) -> Option<&ActorTreeSnapshot> {
        if pid == snapshot.pid {
            Some(snapshot)
        } else {
            for child in snapshot.children.values() {
                if let Some(tree) = Self::find_tree(child, pid) {
                    return Some(tree);
                }
            }
            None
        }
    }

    #[allow(dead_code)]
    fn print_tree(
        w: &mut impl Write,
        level: usize,
        root_actor_id: &ActorId,
        tree: &ActorTreeSnapshot,
    ) {
        let prefix = "\t".repeat(level);
        let actor_id = root_actor_id.child_id(tree.pid);
        write!(
            w,
            "{}  {} {} {} {}:\n",
            prefix, actor_id, tree.type_name, tree.status, tree.stats
        )
        .unwrap();
        let mut handlers: Vec<_> = tree.handlers.iter().collect();
        handlers.sort();
        for (port, handler) in &tree.handlers {
            write!(w, "{}+ {} {}\n", prefix, actor_id.port_id(*port), handler).unwrap();
        }
        for child in tree.children.values() {
            Self::print_tree(w, level + 1, root_actor_id, child);
        }
    }
}
