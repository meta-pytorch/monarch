/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::time::Duration;

use clap::Parser;
use clap::command;
use hyperactor::WorldId;
use hyperactor::channel;
use hyperactor::channel::ChannelTransport;
use hyperactor::channel::Tx;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor_mesh::alloc::Alloc;
use hyperactor_mesh::alloc::AllocSpec;
use hyperactor_mesh::alloc::ProcState;
use hyperactor_mesh::alloc::remoteprocess::MockRemoteProcessAllocInitializer;
use hyperactor_mesh::alloc::remoteprocess::RemoteProcessAlloc;
use hyperactor_mesh::alloc::remoteprocess::RemoteProcessAllocHost;
use hyperactor_mesh::extent;

// Spawn 2 proc_meshes each with 2 hosts
#[derive(Parser, Debug)]
#[command(
    about = "spawns a number of `RemoteProcessAlloc`s for alloc/remoteprocess.rs test_remote_process_alloc_signal_handler"
)]
pub struct Args {
    #[arg(
        long,
        help = "The address to send a message signaling that allocation has completed in the form: \
                `{transport}!{address}:{port}` (e.g. `tcp!127.0.0.1:26600`)."
    )]
    pub done_allocating_addr: String,

    #[arg(
        long,
        help = "The addresses `RemoteProcessAllocator`s are being served on separated by commas in the form: \
                `{transport}!{address}:{port}` (e.g. `tcp!127.0.0.1:26600`)."
    )]
    pub addresses: String,

    #[arg(long)]
    pub num_proc_meshes: usize,

    #[arg(long)]
    pub hosts_per_proc_mesh: usize,

    #[arg(long)]
    pub pid_addr: String,
}
#[tokio::main]
async fn main() {
    let args = Args::parse();
    hyperactor::initialize_with_current_runtime();

    let addresses = args.addresses.split(",").collect::<Vec<_>>();
    let num_proc_meshes = args.num_proc_meshes;
    let hosts_per_proc_mesh = args.hosts_per_proc_mesh;

    assert_eq!(addresses.len(), (hosts_per_proc_mesh * num_proc_meshes));
    let mut addresses = addresses.into_iter();

    let config = hyperactor_config::global::lock();
    let _guard = config.override_key(
        hyperactor::config::MESSAGE_DELIVERY_TIMEOUT,
        Duration::from_secs(5),
    );
    let mut allocs = Vec::new();

    let pid_tx = channel::dial(args.pid_addr.parse().unwrap()).unwrap();
    for proc_num in 0..num_proc_meshes {
        let mut initializer = MockRemoteProcessAllocInitializer::new();

        let mut remote_process_alloc_hosts = Vec::new();
        for host_num in 0..hosts_per_proc_mesh {
            remote_process_alloc_hosts.push(RemoteProcessAllocHost {
                hostname: addresses.next().unwrap().to_string(),
                id: format!("task{}", (proc_num * hosts_per_proc_mesh) + host_num),
            });
        }
        initializer
            .expect_initialize_alloc()
            .return_once(move || Ok(remote_process_alloc_hosts));

        let mut alloc = RemoteProcessAlloc::new(
            AllocSpec {
                extent: extent!(host = hosts_per_proc_mesh, gpu = 1),
                constraints: Default::default(),
                proc_name: None,
                transport: ChannelTransport::Unix,
                proc_allocation_mode: Default::default(),
            },
            WorldId("test_world_id".to_string()),
            0,
            initializer,
        )
        .await
        .unwrap();

        let mut running = 0;
        while running < hosts_per_proc_mesh {
            match alloc.next().await {
                Some(ProcState::Running { .. }) => {
                    running += 1;
                }
                Some(ProcState::Created { pid, .. }) => {
                    pid_tx.post(pid);
                }
                _ => {}
            }
        }
        allocs.push(alloc);
    }

    channel::dial(args.done_allocating_addr.parse().unwrap())
        .unwrap()
        .post(());

    RealClock.sleep(Duration::from_secs(100)).await;
}
