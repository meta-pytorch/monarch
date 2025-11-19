/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![feature(assert_matches)]
// NOTE: Until https://github.com/PyO3/pyo3/pull/4674, `pyo3::pymethods` trigger
// and unsafe-op-in-unsafe-fn warnings.
#![allow(unsafe_op_in_unsafe_fn)]

pub mod bootstrap;
pub mod history;

use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::GangId;
use hyperactor::GangRef;
use hyperactor::Handler;
use hyperactor::Named;
use hyperactor::actor::ActorHandle;
use hyperactor::actor::ActorStatus;
use hyperactor::channel::ChannelAddr;
use hyperactor::clock::Clock;
use hyperactor::context;
use hyperactor::data::Serialized;
use hyperactor_mesh::comm::CommActor;
use hyperactor_mesh::comm::CommActorMode;
use hyperactor_mesh::comm::multicast::CastMessage;
use hyperactor_mesh::comm::multicast::CastMessageEnvelope;
use hyperactor_mesh::comm::multicast::DestinationPort;
use hyperactor_mesh::comm::multicast::Uslice;
use hyperactor_mesh::reference::ActorMeshId;
use hyperactor_mesh::reference::ProcMeshId;
use hyperactor_multiprocess::proc_actor::ProcActor;
use hyperactor_multiprocess::proc_actor::spawn;
use hyperactor_multiprocess::supervision::WorldSupervisionMessageClient;
use hyperactor_multiprocess::supervision::WorldSupervisor;
use hyperactor_multiprocess::system_actor::ProcLifecycleMode;
use hyperactor_multiprocess::system_actor::SYSTEM_ACTOR_REF;
use monarch_messages::client::ClientActor;
use monarch_messages::client::ClientMessageClient;
use monarch_messages::client::Exception;
use monarch_messages::client::LogLevel;
use monarch_messages::controller::ControllerMessage;
use monarch_messages::controller::ControllerMessageHandler;
use monarch_messages::controller::DeviceFailure;
use monarch_messages::controller::Ranks;
use monarch_messages::controller::Seq;
use monarch_messages::controller::WorkerError;
use monarch_messages::debugger::DebuggerAction;
use monarch_messages::worker::Ref;
use monarch_messages::worker::WorkerActor;
use monarch_messages::worker::WorkerMessage;
use ndslice::Selection;
use ndslice::Shape;
use ndslice::Slice;
use ndslice::reshape::Limit;
use ndslice::reshape::ReshapeShapeExt;
use ndslice::selection::dsl;
use ndslice::shape::Range;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::OnceCell;

const CASTING_FANOUT_SIZE: usize = 8;

/// A controller for the workers that will be leveraged by the client to do the actual
/// compute tasks. This acts a proxy managing comms with the workers and handling things like history,
/// data dependency, worker lifecycles etc for the client abstracting it away.
#[derive(Debug)]
#[hyperactor::export(
    spawn = true,
    handlers = [
        ControllerMessage,
    ],
)]
pub(crate) struct ControllerActor {
    client_actor_ref: OnceCell<ActorRef<ClientActor>>,
    comm_actor_ref: ActorRef<CommActor>,
    worker_gang_ref: GangRef<WorkerActor>,
    history: history::History,
    supervision_query_interval: Duration,
    system_supervision_actor_ref: ActorRef<WorldSupervisor>,
    worker_progress_check_interval: Duration,
    operation_timeout: Duration,
    operations_per_worker_progress_request: u64,
    // The Seq and time we last sent out a WorkerMessage::RequestStatus.
    last_controller_request_status: Option<(Seq, tokio::time::Instant)>,
    fail_on_worker_timeout: bool,
    world_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Named)]
pub(crate) struct ControllerParams {
    /// The world size to track the size of all the workers.
    pub(crate) world_size: usize,

    /// Reference to the comm actor. It must be configured to target
    /// the worker gang. The controller takes "ownership" of this actor:
    /// it is immediately configured to target the worker gang.
    /// This is a temporary workaround until we are fully on meshes.
    pub(crate) comm_actor_ref: ActorRef<CommActor>,

    /// Reference to the workers to send commands to.
    pub(crate) worker_gang_ref: GangRef<WorkerActor>,

    // How often to query world supervision status against system actor.
    pub(crate) supervision_query_interval: Duration,

    // How often to query for if workers are making progress.
    pub(crate) worker_progress_check_interval: Duration,

    // How long to wait for an operation to complete before considering it timed out.
    pub(crate) operation_timeout: Duration,

    // How many operations are enqueued before we request a progress update on workers.
    pub(crate) operations_per_worker_progress_request: u64,

    // If a failure should be propagated back to the client if workers are detected to be stuck.
    pub(crate) fail_on_worker_timeout: bool,
}

#[async_trait]
impl Actor for ControllerActor {
    type Params = ControllerParams;

    async fn new(params: ControllerParams) -> Result<Self, anyhow::Error> {
        Ok(Self {
            client_actor_ref: OnceCell::new(),
            comm_actor_ref: params.comm_actor_ref,
            worker_gang_ref: params.worker_gang_ref,
            history: history::History::new(params.world_size),
            supervision_query_interval: params.supervision_query_interval,
            system_supervision_actor_ref: ActorRef::attest(SYSTEM_ACTOR_REF.actor_id().clone()),
            worker_progress_check_interval: params.worker_progress_check_interval,
            operation_timeout: params.operation_timeout,
            operations_per_worker_progress_request: params.operations_per_worker_progress_request,
            last_controller_request_status: None,
            fail_on_worker_timeout: params.fail_on_worker_timeout,
            world_size: params.world_size,
        })
    }

    async fn init(&mut self, cx: &hyperactor::Instance<Self>) -> Result<(), anyhow::Error> {
        self.comm_actor_ref.send(
            cx,
            CommActorMode::ImplicitWithWorldId(self.worker_gang_ref.gang_id().world_id().clone()),
        )?;
        Ok(())
    }
}

impl ControllerActor {
    /// Bootstrap the controller actor. This will create a new proc, join the system at `bootstrap_addr`
    /// and spawn the controller actor into the proc. `labels` is an arbitrary set of name/value pairs
    /// to be attached to the proc in system registry which can be used later to query and find the proc(s)
    /// using system's snapshot api.
    pub async fn bootstrap(
        controller_id: ActorId,
        listen_addr: ChannelAddr,
        bootstrap_addr: ChannelAddr,
        params: ControllerParams,
        supervision_update_interval: Duration,
        labels: HashMap<String, String>,
    ) -> Result<(ActorHandle<ProcActor>, ActorRef<ControllerActor>), anyhow::Error> {
        let bootstrap = ProcActor::bootstrap(
            controller_id.proc_id().clone(),
            controller_id
                .proc_id()
                .world_id()
                .expect("multiprocess supports only ranked procs")
                .clone(), // REFACTOR(marius): make world_id a parameter of ControllerActor::bootstrap
            listen_addr,
            bootstrap_addr.clone(),
            supervision_update_interval,
            labels,
            ProcLifecycleMode::ManagedBySystem,
        )
        .await?;

        let mut system = hyperactor_multiprocess::System::new(bootstrap_addr);
        let client = system.attach().await?;

        let controller_actor_ref = spawn::<ControllerActor>(
            &client,
            &bootstrap.proc_actor.bind(),
            controller_id.clone().name(),
            &ControllerParams {
                comm_actor_ref: bootstrap.comm_actor.bind(),
                ..params
            },
        )
        .await?;

        Ok((bootstrap.proc_actor, controller_actor_ref))
    }

    fn client(&self) -> Result<ActorRef<ClientActor>, anyhow::Error> {
        self.client_actor_ref
            .get()
            .ok_or_else(|| anyhow::anyhow!("client actor ref not set"))
            .cloned()
    }

    // Send a request_status for the seq we expect to complete by our next deadline if it is more than
    // N ops ahead of our last request_status, or if M seconds passed where:
    //
    // N = self.operations_per_worker_progress_request
    // M = self.worker_progress_check_interval
    async fn request_status_if_needed(
        &mut self,
        cx: &Context<'_, Self>,
    ) -> Result<(), anyhow::Error> {
        if let Some((expected_seq, ..)) = self.history.deadline(
            self.operations_per_worker_progress_request,
            self.operation_timeout,
            cx.clock(),
        ) {
            if self.last_controller_request_status.is_none_or(
                |(last_requested_seq, last_requested_time)| {
                    (expected_seq
                        >= (u64::from(last_requested_seq)
                            + self.operations_per_worker_progress_request)
                            .into()
                        || last_requested_time.elapsed() > self.worker_progress_check_interval)
                        && last_requested_seq != expected_seq
                },
            ) {
                // Send to all workers.
                self.send(
                    cx,
                    Ranks::Slice(
                        ndslice::Slice::new(0, vec![self.history.world_size()], vec![1]).unwrap(),
                    ),
                    Serialized::serialize(&WorkerMessage::RequestStatus {
                        seq: expected_seq.clone(),
                        controller: true,
                    })
                    .unwrap(),
                )
                .await?;

                self.last_controller_request_status =
                    Some((expected_seq.clone(), cx.clock().now()));
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
struct CheckWorkerProgress;

#[async_trait]
impl Handler<CheckWorkerProgress> for ControllerActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        _check_worker_progress: CheckWorkerProgress,
    ) -> Result<(), anyhow::Error> {
        let client = self.client()?;

        if let Some((expected_seq, deadline, reported)) = self.history.deadline(
            self.operations_per_worker_progress_request,
            self.operation_timeout,
            cx.clock(),
        ) {
            if !reported
                && cx.clock().now() > deadline
                && expected_seq >= self.history.min_incomplete_seq_reported()
            {
                let timed_out_ranks = self
                    .history
                    .first_incomplete_seqs_controller()
                    .iter()
                    .enumerate()
                    .filter(|(_, seq)| seq <= &&expected_seq)
                    .map(|(rank, _)| rank)
                    .collect::<Vec<_>>();

                let failed_rank = timed_out_ranks.first().unwrap().clone();

                let timed_out_ranks_string = timed_out_ranks
                    .into_iter()
                    .map(|rank| rank.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");

                let message = format!(
                    "ranks {} have operations that have not completed after {} seconds",
                    timed_out_ranks_string,
                    self.operation_timeout.as_secs()
                );
                if client
                    .log(cx, LogLevel::Warn, message.clone())
                    .await
                    .is_ok()
                {
                    self.history.report_deadline_missed();
                }

                if self.fail_on_worker_timeout {
                    client
                        .result(
                            cx,
                            expected_seq,
                            Some(Err(Exception::Failure(DeviceFailure {
                                actor_id: self.worker_gang_ref.rank(failed_rank).actor_id().clone(),
                                address: "unknown".into(),
                                backtrace: message,
                            }))),
                        )
                        .await?;
                }
            }
            self.request_status_if_needed(cx).await?;
        }

        cx.self_message_with_delay(CheckWorkerProgress, self.worker_progress_check_interval)?;
        Ok(())
    }
}

/// Hacky translation from a sub-`Slice` to a `Selection.
fn slice_to_selection(slice: Slice) -> Selection {
    match (slice.sizes(), slice.strides()) {
        // Special case exact rank `Selection`.
        ([], []) => dsl::range(slice.offset()..=slice.offset(), dsl::true_()),
        // Special case trivial range `Selection`.
        ([size, rsizes @ ..], [stride, ..]) if rsizes.iter().all(|s| *s == 1) => dsl::range(
            Range(
                slice.offset(),
                Some(slice.offset() + *size * *stride),
                *stride,
            ),
            dsl::true_(),
        ),
        // Fallback to more heavy-weight translation for everything else.
        _ => {
            let mut selection = Selection::False;
            let mut selected_ranks = HashSet::new();
            for rank in slice.iter() {
                if !selected_ranks.insert(rank) {
                    continue;
                }
                selection = dsl::union(dsl::range(rank..=rank, dsl::true_()), selection);
            }
            selection
        }
    }
}

#[async_trait]
#[hyperactor::forward(ControllerMessage)]
impl ControllerMessageHandler for ControllerActor {
    async fn attach(
        &mut self,
        cx: &Context<Self>,
        client_actor: ActorRef<ClientActor>,
    ) -> Result<(), anyhow::Error> {
        tracing::debug!("attaching client actor {}", client_actor);
        self.client_actor_ref
            .set(client_actor)
            .map_err(|actor_ref| anyhow::anyhow!("client actor {} already attached", actor_ref))?;

        // Trigger periodical checking of supervision status and worker progress.
        cx.self_message_with_delay(
            ControllerMessage::CheckSupervision {},
            self.supervision_query_interval,
        )?;
        cx.self_message_with_delay(CheckWorkerProgress, self.worker_progress_check_interval)?;
        Ok(())
    }

    async fn node(
        &mut self,
        cx: &Context<Self>,
        seq: Seq,
        defs: Vec<Ref>,
        uses: Vec<Ref>,
    ) -> Result<(), anyhow::Error> {
        let failures = self.history.add_invocation(seq, uses, defs);
        let client = self.client()?;
        for (seq, failure) in failures {
            let _ = client.result(cx, seq, failure).await;
        }
        self.request_status_if_needed(cx).await?;

        Ok(())
    }

    async fn drop_refs(
        &mut self,
        _cx: &Context<Self>,
        refs: Vec<Ref>,
    ) -> Result<(), anyhow::Error> {
        self.history.delete_invocations_for_refs(refs);
        Ok(())
    }

    async fn send(
        &mut self,
        cx: &Context<Self>,
        ranks: Ranks,
        message: Serialized,
    ) -> Result<(), anyhow::Error> {
        let selection = match ranks {
            Ranks::Slice(slice) => {
                if slice.len() == self.world_size {
                    // All ranks are selected.
                    Selection::True
                } else {
                    slice_to_selection(slice)
                }
            }
            Ranks::SliceList(slices) => slices.into_iter().fold(dsl::false_(), |sel, slice| {
                dsl::union(sel, slice_to_selection(slice))
            }),
        };

        let slice = Slice::new(0usize, vec![self.world_size], vec![1])?;
        // Use a made-up label to create a fake shape. This shape is used by
        // comm actor to determine the cast rank. Cast rank is not used by
        // DeviceMesh, but we still need a shape there to make the logic happy.
        let made_up_shape = Shape::new(vec!["fake_in_controller".to_string()], slice.clone())?
            .reshape(Limit::from(CASTING_FANOUT_SIZE))
            .shape;

        let message = CastMessageEnvelope::from_serialized(
            ActorMeshId::V0(
                ProcMeshId(self.worker_gang_ref.gang_id().world_id().to_string()),
                self.worker_gang_ref.gang_id().name().to_string(),
            ),
            cx.self_id().clone(),
            DestinationPort::new::<WorkerActor, WorkerMessage>(
                // This is awkward, but goes away entirely with meshes.
                self.worker_gang_ref
                    .gang_id()
                    .actor_id(0)
                    .name()
                    .to_string(),
            ),
            made_up_shape,
            message,
        );

        self.comm_actor_ref.send(
            cx,
            CastMessage {
                dest: Uslice {
                    // TODO: pass both slice and selection from client side
                    slice,
                    selection,
                },
                message,
            },
        )?;
        Ok(())
    }

    async fn remote_function_failed(
        &mut self,
        cx: &Context<Self>,
        seq: Seq,
        error: WorkerError,
    ) -> Result<(), anyhow::Error> {
        let rank = error.worker_actor_id.rank();
        self.history
            .propagate_exception(seq, Exception::Error(seq, seq, error.clone()));
        mark_worker_complete_and_propagate_exceptions(cx, self, rank, &seq).await?;
        Ok(())
    }

    async fn status(
        &mut self,
        cx: &Context<Self>,
        seq: Seq,
        worker_actor_id: ActorId,
        controller: bool,
    ) -> Result<(), anyhow::Error> {
        let rank = worker_actor_id.rank();

        if controller {
            self.history.update_deadline_tracking(rank, seq);
        } else {
            mark_worker_complete_and_propagate_exceptions(cx, self, rank, &seq).await?;
        }
        Ok(())
    }

    async fn fetch_result(
        &mut self,
        _cx: &Context<Self>,
        seq: Seq,
        result: Result<Serialized, WorkerError>,
    ) -> Result<(), anyhow::Error> {
        self.history.set_result(seq, result);
        Ok(())
    }

    async fn check_supervision(&mut self, cx: &Context<Self>) -> Result<(), anyhow::Error> {
        let gang_id: GangId = self.worker_gang_ref.clone().into();
        let world_state = self
            .system_supervision_actor_ref
            .state(cx, gang_id.world_id().clone())
            .await?;

        if let Some(world_state) = world_state {
            if !world_state.procs.is_empty() {
                tracing::error!(
                    "found procs with failures in world {}, state: {:?}",
                    gang_id.world_id(),
                    world_state
                );

                // Randomly pick a failed proc as the failed actor.
                let (_, failed_state) = world_state.procs.iter().next().unwrap();
                let (failed_actor, failure_reason) =
                    failed_state.failed_actors.first().map_or_else(
                        || {
                            let proc_id = &failed_state.proc_id;
                            (
                                ActorId(proc_id.clone(), "none".into(), 0),
                                format!(
                                    "proc is dead due to heartbeat timeout; no backtrace is \
                                    available; check the log of host {} running proc {} to \
                                    figure out the root cause",
                                    failed_state.proc_addr, proc_id
                                ),
                            )
                        },
                        |(actor, status)| {
                            (
                                actor.clone(),
                                match status {
                                    ActorStatus::Failed(msg) => msg.to_string(),
                                    _ => format!("unexpected actor status {status}"),
                                },
                            )
                        },
                    );

                let exc = Exception::Failure(DeviceFailure {
                    actor_id: failed_actor,
                    address: failed_state.proc_addr.to_string(),
                    backtrace: failure_reason,
                });
                tracing::error!("Sending failure to client: {exc:?}");
                // Seq does not matter as the client will raise device error immediately before setting the results.
                self.client()?
                    .result(cx, Seq::default(), Some(Err(exc)))
                    .await?;
                tracing::error!("Failure successfully sent to client");

                // No need to set history failures as we are directly sending back failure results.
            }
        }

        // Schedule the next supervision check.
        cx.self_message_with_delay(
            ControllerMessage::CheckSupervision {},
            self.supervision_query_interval,
        )?;
        Ok(())
    }

    async fn debugger_message(
        &mut self,
        cx: &Context<Self>,
        debugger_actor_id: ActorId,
        action: DebuggerAction,
    ) -> Result<(), anyhow::Error> {
        self.client()?
            .debugger_message(cx, debugger_actor_id, action)
            .await
    }

    #[cfg(test)]
    async fn get_first_incomplete_seqs_unit_tests_only(
        &mut self,
        _cx: &Context<Self>,
    ) -> Result<Vec<Seq>, anyhow::Error> {
        Ok(self.history.first_incomplete_seqs().to_vec())
    }

    #[cfg(not(test))]
    async fn get_first_incomplete_seqs_unit_tests_only(
        &mut self,
        _cx: &Context<Self>,
    ) -> Result<Vec<Seq>, anyhow::Error> {
        unimplemented!("get_first_incomplete_seqs_unit_tests_only is only for unit tests")
    }
}

async fn mark_worker_complete_and_propagate_exceptions(
    cx: &impl context::Actor,
    actor: &mut ControllerActor,
    rank: usize,
    seq: &Seq,
) -> Result<(), anyhow::Error> {
    let results = actor.history.rank_completed(rank, seq.clone());
    let client = actor.client()?;
    // Propagate the failures to the clients.
    for (seq, result) in results.iter() {
        let _ = client.result(cx, seq.clone(), result.clone()).await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use core::panic;
    use std::assert_matches::assert_matches;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::time::Duration;

    use hyperactor::HandleClient;
    use hyperactor::Handler;
    use hyperactor::RefClient;
    use hyperactor::channel;
    use hyperactor::channel::ChannelTransport;
    use hyperactor::clock::Clock;
    use hyperactor::clock::RealClock;
    use hyperactor::context::Mailbox as _;
    use hyperactor::id;
    use hyperactor::mailbox::BoxedMailboxSender;
    use hyperactor::mailbox::DialMailboxRouter;
    use hyperactor::mailbox::Mailbox;
    use hyperactor::mailbox::MailboxClient;
    use hyperactor::mailbox::MailboxServer;
    use hyperactor::mailbox::PortHandle;
    use hyperactor::mailbox::PortReceiver;
    use hyperactor::message::IndexedErasedUnbound;
    use hyperactor::panic_handler;
    use hyperactor::proc::Proc;
    use hyperactor::reference::GangId;
    use hyperactor::reference::ProcId;
    use hyperactor::reference::WorldId;
    use hyperactor::simnet;
    use hyperactor_mesh::comm::CommActorParams;
    use hyperactor_multiprocess::System;
    use hyperactor_multiprocess::proc_actor::ProcMessage;
    use hyperactor_multiprocess::supervision::ProcSupervisionMessage;
    use hyperactor_multiprocess::supervision::ProcSupervisor;
    use hyperactor_multiprocess::system_actor::SystemMessage;
    use monarch_messages::client::ClientMessage;
    use monarch_messages::controller::ControllerMessageClient;
    use monarch_messages::wire_value::WireValue;
    use monarch_messages::worker::CallFunctionParams;
    use monarch_messages::worker::WorkerMessage;
    use monarch_types::PyTree;
    use timed_test::async_timed_test;
    use torch_sys::RValue;

    use super::*;

    #[tokio::test]
    async fn basic_controller() {
        // TODO: Add a proper multiworker test
        let proc = Proc::local();
        let (client, client_ref, mut client_rx) = proc
            .attach_actor::<ClientActor, ClientMessage>("client")
            .unwrap();
        let (worker, worker_ref, mut worker_rx) = proc
            .attach_actor::<WorkerActor, WorkerMessage>("worker")
            .unwrap();

        IndexedErasedUnbound::<WorkerMessage>::bind_for_test_only(
            worker_ref.clone(),
            worker.clone_for_py(),
            worker.mailbox().clone(),
        )
        .unwrap();

        let comm_handle = proc
            .spawn::<CommActor>("comm", CommActorParams {})
            .await
            .unwrap();

        let controller_handle = proc
            .spawn::<ControllerActor>(
                "controller",
                ControllerParams {
                    world_size: 1,
                    comm_actor_ref: comm_handle.bind(),
                    worker_gang_ref: GangRef::attest(GangId(
                        WorldId(
                            proc.proc_id()
                                .world_name()
                                .expect("only ranked actors are supported in the controller tests")
                                .to_string(),
                        ),
                        "worker".to_string(),
                    )),
                    supervision_query_interval: Duration::from_secs(1),
                    worker_progress_check_interval: Duration::from_secs(3),
                    operation_timeout: Duration::from_secs(30),
                    operations_per_worker_progress_request: 100,
                    fail_on_worker_timeout: false,
                },
            )
            .await
            .unwrap();

        controller_handle.attach(&client, client_ref).await.unwrap();

        controller_handle
            .node(&client, 0.into(), vec![0.into()], vec![])
            .await
            .unwrap();
        controller_handle
            .node(&client, 1.into(), vec![1.into(), 2.into()], vec![0.into()])
            .await
            .unwrap();
        controller_handle
            .node(&client, 20.into(), vec![3.into(), 4.into()], vec![])
            .await
            .unwrap();

        ControllerMessageClient::send(
            &controller_handle,
            &worker,
            Ranks::Slice(ndslice::Slice::new(0, vec![1], vec![1]).unwrap()),
            Serialized::serialize(&WorkerMessage::CallFunction(CallFunctionParams {
                seq: 1.into(),
                results: vec![Some(1.into()), Some(2.into())],
                mutates: vec![],
                function: "os.path.split".into(),
                args: vec![WireValue::String("/fbs/fbc/foo/bar".into())],
                kwargs: HashMap::new(),
                stream: 1.into(),
                remote_process_groups: vec![],
            }))
            .unwrap(),
        )
        .await
        .unwrap();

        ControllerMessageClient::status(
            &controller_handle,
            &worker,
            0.into(),
            worker_ref.actor_id().clone(),
            false,
        )
        .await
        .unwrap();
        let incomplete_seqs = controller_handle
            .get_first_incomplete_seqs_unit_tests_only(&worker)
            .await
            .unwrap();
        assert_eq!(incomplete_seqs[0], 0.into());

        controller_handle
            .remote_function_failed(
                &worker,
                1.into(),
                WorkerError {
                    backtrace: "some failure happened!".to_string(),
                    worker_actor_id: worker_ref.actor_id().clone(),
                },
            )
            .await
            .unwrap();
        ControllerMessageClient::status(
            &controller_handle,
            &worker,
            2.into(),
            worker_ref.actor_id().clone(),
            false,
        )
        .await
        .unwrap();

        let incomplete_seqs = controller_handle
            .get_first_incomplete_seqs_unit_tests_only(&worker)
            .await
            .unwrap();
        assert_eq!(incomplete_seqs[0], 2.into());

        controller_handle
            .fetch_result(
                &worker,
                20.into(),
                Ok(Serialized::serialize(&PyTree::from(RValue::Int(42))).unwrap()),
            )
            .await
            .unwrap();

        // Omly a status message can trigger a fetch result to the client.
        ControllerMessageClient::status(
            &controller_handle,
            &worker,
            21.into(),
            worker_ref.actor_id().clone(),
            false,
        )
        .await
        .unwrap();

        let incomplete_seqs = controller_handle
            .get_first_incomplete_seqs_unit_tests_only(&worker)
            .await
            .unwrap();
        assert_eq!(incomplete_seqs[0], 21.into());

        controller_handle.drain_and_stop().unwrap();
        controller_handle.await;
        let worker_messages: Vec<WorkerMessage> = worker_rx.drain();
        assert_eq!(
            worker_messages
                .iter()
                .filter(|msg| !matches!(msg, WorkerMessage::RequestStatus { .. }))
                .count(),
            1
        );
        let client_messages = client_rx.drain();
        assert_eq!(client_messages.len(), 3);
        let client_message = client_messages[1].clone().into_result().unwrap();
        assert_eq!(client_message.0, 1.into());
        assert_eq!(
            client_message.1,
            Some(Err(Exception::Error(
                1.into(),
                1.into(),
                WorkerError {
                    backtrace: "some failure happened!".to_string(),
                    worker_actor_id: worker_ref.actor_id().clone(),
                }
            )))
        );

        let client_message = client_messages[2].clone().into_result().unwrap();
        assert_eq!(client_message.0, 20.into());
        assert_matches!(
            client_message
                .1
                .unwrap()
                .unwrap()
                .deserialized::<PyTree<RValue>>()
                .unwrap()
                .into_leaf()
                .unwrap(),
            RValue::Int(42),
        );
    }

    #[tokio::test]
    async fn worker_timeout() {
        tokio::time::pause();
        let timeout_secs = 3;
        let proc = Proc::local();

        let (client, client_ref, mut client_rx) = proc
            .attach_actor::<ClientActor, ClientMessage>("client")
            .unwrap();
        let (worker, worker_ref, mut worker_rx) = proc
            .attach_actor::<WorkerActor, WorkerMessage>("worker")
            .unwrap();
        IndexedErasedUnbound::<WorkerMessage>::bind_for_test_only(
            worker_ref.clone(),
            worker.clone_for_py(),
            worker.mailbox().clone(),
        )
        .unwrap();

        let comm_handle = proc
            .spawn::<CommActor>("comm", CommActorParams {})
            .await
            .unwrap();

        let controller_handle = proc
            .spawn::<ControllerActor>(
                "controller",
                ControllerParams {
                    world_size: 1,
                    comm_actor_ref: comm_handle.bind(),
                    worker_gang_ref: GangRef::attest(GangId(
                        WorldId(
                            proc.proc_id()
                                .world_name()
                                .expect("only ranked actors are supported in the controller tests")
                                .to_string(),
                        ),
                        "worker".to_string(),
                    )),
                    supervision_query_interval: Duration::from_secs(100000),
                    worker_progress_check_interval: Duration::from_secs(1),
                    operation_timeout: Duration::from_secs(timeout_secs),
                    operations_per_worker_progress_request: 100,
                    fail_on_worker_timeout: false,
                },
            )
            .await
            .unwrap();

        controller_handle.attach(&client, client_ref).await.unwrap();

        controller_handle
            .node(&client, 0.into(), vec![0.into()], vec![])
            .await
            .unwrap();

        // Expect that our handler for CheckWorkerProgress will issue RequestWorkerCompletedSeq
        match worker_rx.recv().await.unwrap().into_request_status().ok() {
            Some((seq, controller)) if seq == 0.into() && controller => {
                // Simulate WorkerActor::RequestWorkerCompletedSeq if joining streams takes shorter
                // than timeout
                for _ in 0..timeout_secs {
                    tokio::time::advance(Duration::from_secs(1)).await;
                }

                ControllerMessageClient::status(
                    &controller_handle,
                    &worker,
                    1.into(),
                    worker_ref.actor_id().clone(),
                    true,
                )
                .await
                .unwrap();
            }
            _ => panic!("Expected request status message for seq 0"),
        }

        // Should have no warnings
        let client_messages = client_rx.drain();
        assert_eq!(client_messages.len(), 0);

        controller_handle
            .node(&client, 1.into(), vec![], vec![])
            .await
            .unwrap();

        // Expect that our handler for CheckWorkerProgress will issue RequestWorkerCompletedSeq
        match worker_rx.recv().await.unwrap().into_request_status().ok() {
            Some((seq, controller)) if seq == 1.into() && controller => {
                // Simulate WorkerActor::RequestWorkerCompletedSeq if joining streams takes longer
                // than timeout
                for _ in 0..timeout_secs * 2 {
                    tokio::time::advance(Duration::from_secs(1)).await;
                }

                ControllerMessageClient::status(
                    &controller_handle,
                    &worker,
                    2.into(),
                    worker_ref.actor_id().clone(),
                    true,
                )
                .await
                .unwrap();
            }
            _ => panic!("Expected request status message for seq 1"),
        }

        let client_messages = client_rx.drain();
        assert_eq!(client_messages.len(), 1);

        let (level, message) = client_messages[0].clone().into_log().unwrap();
        assert_matches!(level, LogLevel::Warn);
        assert_eq!(
            message,
            "ranks 0 have operations that have not completed after 3 seconds"
        );
    }

    #[tokio::test]
    async fn test_failure_on_worker_timeout() {
        tokio::time::pause();
        let timeout_secs = 3;
        let proc = Proc::local();

        let (client, client_ref, mut client_rx) = proc
            .attach_actor::<ClientActor, ClientMessage>("client")
            .unwrap();

        let (worker, worker_ref, mut worker_rx) = proc
            .attach_actor::<WorkerActor, WorkerMessage>("worker")
            .unwrap();
        IndexedErasedUnbound::<WorkerMessage>::bind_for_test_only(
            worker_ref.clone(),
            worker.clone_for_py(),
            worker.mailbox().clone(),
        )
        .unwrap();

        let comm_handle = proc
            .spawn::<CommActor>("comm", CommActorParams {})
            .await
            .unwrap();

        let world_id = WorldId(
            proc.proc_id()
                .world_name()
                .expect("only ranked actors are supported in the controller tests")
                .to_string(),
        );
        let controller_handle = proc
            .spawn::<ControllerActor>(
                "controller",
                ControllerParams {
                    world_size: 1,
                    comm_actor_ref: comm_handle.bind(),
                    worker_gang_ref: GangRef::attest(GangId(world_id, "worker".to_string())),
                    supervision_query_interval: Duration::from_secs(100000),
                    worker_progress_check_interval: Duration::from_secs(1),
                    operation_timeout: Duration::from_secs(timeout_secs),
                    operations_per_worker_progress_request: 100,
                    fail_on_worker_timeout: true,
                },
            )
            .await
            .unwrap();

        controller_handle.attach(&client, client_ref).await.unwrap();

        controller_handle
            .node(&client, 0.into(), vec![0.into()], vec![])
            .await
            .unwrap();

        // Expect that our handler for CheckWorkerProgress will issue RequestWorkerCompletedSeq
        match worker_rx.recv().await.unwrap().into_request_status().ok() {
            Some((seq, controller)) if seq == 0.into() && controller => {
                // Simulate WorkerActor::RequestWorkerCompletedSeq if joining streams takes shorter
                // than timeout
                for _ in 0..timeout_secs {
                    tokio::time::advance(Duration::from_secs(1)).await;
                }

                ControllerMessageClient::status(
                    &controller_handle,
                    &worker,
                    1.into(),
                    worker_ref.actor_id().clone(),
                    true,
                )
                .await
                .unwrap();
            }
            _ => panic!("Expected request status message for seq 0"),
        }

        // Should have no warnings
        let client_messages = client_rx.drain();
        assert_eq!(client_messages.len(), 0);

        controller_handle
            .node(&client, 1.into(), vec![], vec![])
            .await
            .unwrap();

        // Expect that our handler for CheckWorkerProgress will issue RequestWorkerCompletedSeq
        match worker_rx.recv().await.unwrap().into_request_status().ok() {
            Some((seq, controller)) if seq == 1.into() && controller => {
                // Simulate WorkerActor::RequestWorkerCompletedSeq if joining streams takes longer
                // than timeout
                for _ in 0..timeout_secs * 2 {
                    tokio::time::advance(Duration::from_secs(1)).await;
                }

                ControllerMessageClient::status(
                    &controller_handle,
                    &worker,
                    2.into(),
                    worker_ref.actor_id().clone(),
                    true,
                )
                .await
                .unwrap();
            }
            _ => panic!("Expected request status message for seq 1"),
        }

        let client_messages = client_rx.drain();
        assert_eq!(client_messages.len(), 2);

        let (level, message) = client_messages[0].clone().into_log().unwrap();
        assert_matches!(level, LogLevel::Warn);
        assert_eq!(
            message,
            "ranks 0 have operations that have not completed after 3 seconds"
        );

        let (seq, failure) = client_messages[1].clone().into_result().unwrap();
        assert_eq!(seq, 1.into());
        let DeviceFailure {
            backtrace,
            actor_id,
            ..
        } = failure
            .unwrap()
            .err()
            .unwrap()
            .as_failure()
            .unwrap()
            .clone();
        assert_eq!(actor_id, proc.proc_id().actor_id("worker", 0));
        assert!(
            backtrace.contains("ranks 0 have operations that have not completed after 3 seconds")
        );
    }

    #[tokio::test]
    async fn failure_propagation() {
        // Serve a system.
        let server_handle = System::serve(
            ChannelAddr::any(ChannelTransport::Local),
            Duration::from_secs(10),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
        let mut system = System::new(server_handle.local_addr().clone());

        // Build a supervisor.
        let sup_mail = system.attach().await.unwrap();
        let (_sup_tx, _sup_rx) = sup_mail.bind_actor_port::<ProcSupervisionMessage>();
        let sup_ref = ActorRef::<ProcSupervisor>::attest(sup_mail.self_id().clone());

        // Construct a system sender.
        let system_sender = BoxedMailboxSender::new(MailboxClient::new(
            channel::dial(server_handle.local_addr().clone()).unwrap(),
        ));

        // Construct a proc forwarder in terms of the system sender.
        let listen_addr = ChannelAddr::any(ChannelTransport::Local);
        let proc_forwarder =
            BoxedMailboxSender::new(DialMailboxRouter::new_with_default(system_sender));

        // Bootstrap proc 'local[0]', join the system.
        let world_id = id!(local);
        let proc = Proc::new(world_id.proc_id(0), proc_forwarder.clone());
        let proc_actor_0 = ProcActor::bootstrap_for_proc(
            proc.clone(),
            world_id.clone(),
            listen_addr,
            server_handle.local_addr().clone(),
            sup_ref.clone(),
            Duration::from_secs(2),
            HashMap::new(),
            ProcLifecycleMode::ManagedBySystem,
        )
        .await
        .unwrap();

        // Bootstrap proc 'local[1]', join the system.
        let proc2 = Proc::new(world_id.proc_id(1), proc_forwarder.clone());
        let _proc_actor_1 = ProcActor::bootstrap_for_proc(
            proc2.clone(),
            world_id.clone(),
            ChannelAddr::any(ChannelTransport::Local),
            server_handle.local_addr().clone(),
            sup_ref.clone(),
            Duration::from_secs(2),
            HashMap::new(),
            ProcLifecycleMode::ManagedBySystem,
        )
        .await
        .unwrap();

        // Test
        let (client, client_ref, mut client_rx) = proc
            .attach_actor::<ClientActor, ClientMessage>("client")
            .unwrap();
        let (worker1, worker1_ref, _) = proc
            .attach_actor::<WorkerActor, WorkerMessage>("worker")
            .unwrap();
        IndexedErasedUnbound::<WorkerMessage>::bind_for_test_only(
            worker1_ref.clone(),
            worker1.clone_for_py(),
            worker1.mailbox().clone(),
        )
        .unwrap();
        let (worker2, worker2_ref, _) = proc2
            .attach_actor::<WorkerActor, WorkerMessage>("worker")
            .unwrap();
        IndexedErasedUnbound::<WorkerMessage>::bind_for_test_only(
            worker2_ref.clone(),
            worker2.clone_for_py(),
            worker2.mailbox().clone(),
        )
        .unwrap();

        let controller_handle = proc
            .spawn::<ControllerActor>(
                "controller",
                ControllerParams {
                    world_size: 2,
                    comm_actor_ref: proc_actor_0.comm_actor.bind(),
                    worker_gang_ref: GangRef::attest(GangId(
                        WorldId(world_id.name().to_string()),
                        "worker".to_string(),
                    )),
                    supervision_query_interval: Duration::from_secs(1),
                    worker_progress_check_interval: Duration::from_secs(3),
                    operation_timeout: Duration::from_secs(30),
                    operations_per_worker_progress_request: 100,
                    fail_on_worker_timeout: false,
                },
            )
            .await
            .unwrap();

        controller_handle.attach(&client, client_ref).await.unwrap();

        controller_handle
            .node(&client, 0.into(), vec![1.into(), 2.into()], vec![])
            .await
            .unwrap();
        controller_handle
            .node(&client, 1.into(), vec![3.into()], vec![1.into()])
            .await
            .unwrap();
        controller_handle
            .node(&client, 2.into(), vec![4.into()], vec![3.into()])
            .await
            .unwrap();
        controller_handle
            .node(&client, 3.into(), vec![5.into()], vec![3.into()])
            .await
            .unwrap();
        controller_handle
            .node(&client, 4.into(), vec![6.into()], vec![3.into()])
            .await
            .unwrap();
        controller_handle
            .node(&client, 5.into(), vec![7.into()], vec![4.into()])
            .await
            .unwrap();
        controller_handle
            .node(&client, 6.into(), vec![8.into()], vec![4.into()])
            .await
            .unwrap();

        ControllerMessageClient::status(
            &controller_handle,
            &worker1,
            1.into(),
            worker1_ref.actor_id().clone(),
            false,
        )
        .await
        .unwrap();
        ControllerMessageClient::status(
            &controller_handle,
            &worker2,
            1.into(),
            worker2_ref.actor_id().clone(),
            false,
        )
        .await
        .unwrap();
        controller_handle
            .remote_function_failed(
                &worker1,
                2.into(),
                WorkerError {
                    backtrace: "some failure happened!".to_string(),
                    worker_actor_id: worker1_ref.actor_id().clone(),
                },
            )
            .await
            .unwrap();
        controller_handle
            .remote_function_failed(
                &worker2,
                2.into(),
                WorkerError {
                    backtrace: "some failure happened!".to_string(),
                    worker_actor_id: worker2_ref.actor_id().clone(),
                },
            )
            .await
            .unwrap();
        for s in 3..=7 {
            ControllerMessageClient::status(
                &controller_handle,
                &worker1,
                s.into(),
                worker1_ref.actor_id().clone(),
                false,
            )
            .await
            .unwrap();
            ControllerMessageClient::status(
                &controller_handle,
                &worker2,
                s.into(),
                worker2_ref.actor_id().clone(),
                false,
            )
            .await
            .unwrap();
        }

        controller_handle.drain_and_stop().unwrap();
        controller_handle.await;
        let mut client_messages = client_rx.drain();
        client_messages.sort_by_key(|msg| msg.clone().into_result().unwrap().0);
        assert_eq!(client_messages.len(), 7);
        let client_message = client_messages[2].clone().into_result().unwrap();
        assert_eq!(client_message.0, 2.into());
        assert_eq!(
            client_message.1,
            Some(Err(Exception::Error(
                2.into(),
                2.into(),
                WorkerError {
                    backtrace: "some failure happened!".to_string(),
                    worker_actor_id: worker1_ref.actor_id().clone(),
                }
            )))
        );

        assert_eq!(
            client_messages
                .into_iter()
                .map(|msg| msg.into_result().unwrap().0)
                .collect::<HashSet<Seq>>(),
            HashSet::from([
                0.into(),
                3.into(),
                1.into(),
                4.into(),
                2.into(),
                5.into(),
                6.into()
            ])
        )
    }

    #[tokio::test]
    async fn test_eager_failure_reporting() {
        // Serve a system.
        let server_handle = System::serve(
            ChannelAddr::any(ChannelTransport::Local),
            Duration::from_secs(10),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
        let mut system = System::new(server_handle.local_addr().clone());

        // Build a supervisor.
        let sup_mail = system.attach().await.unwrap();
        let (_sup_tx, _sup_rx) = sup_mail.bind_actor_port::<ProcSupervisionMessage>();
        let sup_ref = ActorRef::<ProcSupervisor>::attest(sup_mail.self_id().clone());

        // Construct a system sender.
        let system_sender = BoxedMailboxSender::new(MailboxClient::new(
            channel::dial(server_handle.local_addr().clone()).unwrap(),
        ));

        // Construct a proc forwarder in terms of the system sender.
        let listen_addr = ChannelAddr::any(ChannelTransport::Local);
        let proc_forwarder =
            BoxedMailboxSender::new(DialMailboxRouter::new_with_default(system_sender));

        // Bootstrap proc 'local[0]', join the system.
        let world_id = id!(local);
        let proc = Proc::new(world_id.proc_id(0), proc_forwarder.clone());
        let proc_actor_0 = ProcActor::bootstrap_for_proc(
            proc.clone(),
            world_id.clone(),
            listen_addr,
            server_handle.local_addr().clone(),
            sup_ref.clone(),
            Duration::from_secs(2),
            HashMap::new(),
            ProcLifecycleMode::ManagedBySystem,
        )
        .await
        .unwrap();

        // Bootstrap proc 'local[1]', join the system.
        let proc2 = Proc::new(world_id.proc_id(1), proc_forwarder.clone());
        let _proc_actor_1 = ProcActor::bootstrap_for_proc(
            proc2.clone(),
            world_id.clone(),
            ChannelAddr::any(ChannelTransport::Local),
            server_handle.local_addr().clone(),
            sup_ref.clone(),
            Duration::from_secs(2),
            HashMap::new(),
            ProcLifecycleMode::ManagedBySystem,
        )
        .await
        .unwrap();

        // Test
        let (client, client_ref, mut client_rx) = proc
            .attach_actor::<ClientActor, ClientMessage>("client")
            .unwrap();
        let (worker1, worker1_ref, _) = proc
            .attach_actor::<WorkerActor, WorkerMessage>("worker")
            .unwrap();

        let controller_handle = proc
            .spawn::<ControllerActor>(
                "controller",
                ControllerParams {
                    world_size: 1,
                    comm_actor_ref: proc_actor_0.comm_actor.bind(),
                    worker_gang_ref: GangRef::attest(GangId(
                        WorldId(world_id.name().to_string()),
                        "worker".to_string(),
                    )),
                    supervision_query_interval: Duration::from_secs(1),
                    worker_progress_check_interval: Duration::from_secs(3),
                    operation_timeout: Duration::from_secs(30),
                    operations_per_worker_progress_request: 100,
                    fail_on_worker_timeout: false,
                },
            )
            .await
            .unwrap();

        controller_handle.attach(&client, client_ref).await.unwrap();

        controller_handle
            .node(&client, 0.into(), vec![1.into()], vec![])
            .await
            .unwrap();

        controller_handle
            .node(&client, 1.into(), vec![2.into()], vec![1.into()])
            .await
            .unwrap();

        controller_handle
            .node(&client, 2.into(), vec![3.into()], vec![2.into()])
            .await
            .unwrap();

        controller_handle
            .node(&client, 3.into(), vec![], vec![3.into()])
            .await
            .unwrap();

        controller_handle
            .node(&client, 4.into(), vec![], vec![])
            .await
            .unwrap();

        controller_handle
            .remote_function_failed(
                &worker1,
                0.into(),
                WorkerError {
                    backtrace: "some failure happened!".to_string(),
                    worker_actor_id: worker1_ref.actor_id().clone(),
                },
            )
            .await
            .unwrap();

        controller_handle
            .remote_function_failed(
                &worker1,
                3.into(),
                WorkerError {
                    backtrace: "some failure happened!".to_string(),
                    worker_actor_id: worker1_ref.actor_id().clone(),
                },
            )
            .await
            .unwrap();

        ControllerMessageClient::status(
            &controller_handle,
            &worker1,
            5.into(),
            worker1_ref.actor_id().clone(),
            false,
        )
        .await
        .unwrap();

        controller_handle.drain_and_stop().unwrap();
        controller_handle.await;

        let client_messages = client_rx.drain();
        // no double reported messages
        assert_eq!(client_messages.len(), 5);

        let (errors, successes) =
            client_messages
                .into_iter()
                .fold((0, 0), |(errors, successes), client_message| {
                    let (_, result) = client_message.clone().into_result().unwrap();
                    match result {
                        Some(Err(Exception::Error(_, _, _))) => (errors + 1, successes),
                        None => (errors, successes + 1),
                        _ => {
                            panic!("should only be exceptions or no result");
                        }
                    }
                });

        // Assert that we have 4 error messages and 1 non-error message
        assert_eq!(errors, 4);
        assert_eq!(successes, 1);
    }

    #[tokio::test]
    async fn test_bootstrap() {
        let server_handle = System::serve(
            ChannelAddr::any(ChannelTransport::Local),
            Duration::from_secs(10),
            Duration::from_secs(10),
        )
        .await
        .unwrap();

        let controller_id = id!(controller[0].root);
        let proc_id = id!(world[0]);
        let (proc_handle, actor_ref) = ControllerActor::bootstrap(
            controller_id.clone(),
            ChannelAddr::any(ChannelTransport::Local),
            server_handle.local_addr().clone(),
            ControllerParams {
                world_size: 1,
                comm_actor_ref: ActorRef::attest(controller_id.proc_id().actor_id("comm", 0)),
                worker_gang_ref: GangRef::attest(GangId(
                    WorldId(
                        proc_id
                            .world_name()
                            .expect("only ranked actors are supported in the controller tests")
                            .to_string(),
                    ),
                    "worker".to_string(),
                )),
                supervision_query_interval: Duration::from_secs(1),
                worker_progress_check_interval: Duration::from_secs(3),
                operation_timeout: Duration::from_secs(30),
                operations_per_worker_progress_request: 100,
                fail_on_worker_timeout: false,
            },
            Duration::from_secs(1),
            HashMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(*actor_ref.actor_id(), controller_id);

        proc_handle.drain_and_stop().unwrap();
    }

    async fn mock_proc_actor(
        idx: usize,
        rank: usize,
    ) -> (
        WorldId,
        ProcId,
        ChannelAddr,
        Mailbox,
        PortHandle<ProcMessage>,
        PortReceiver<ProcMessage>,
    ) {
        let world_id = id!(world);
        // Set up a local actor.
        let local_proc_id = world_id.proc_id(rank);
        let (local_proc_addr, local_proc_rx) =
            channel::serve(ChannelAddr::any(ChannelTransport::Local)).unwrap();
        let local_proc_mbox = Mailbox::new_detached(
            local_proc_id.actor_id(format!("test_dummy_proc{}", idx).to_string(), 0),
        );
        let (local_proc_message_port, local_proc_message_receiver) = local_proc_mbox.open_port();
        local_proc_message_port.bind();

        let _local_proc_serve_handle = local_proc_mbox.clone().serve(local_proc_rx);
        (
            world_id,
            local_proc_id,
            local_proc_addr,
            local_proc_mbox,
            local_proc_message_port,
            local_proc_message_receiver,
        )
    }

    #[tokio::test]
    async fn test_sim_supervision_failure() {
        // Start system actor.
        simnet::start();
        simnet::simnet_handle()
            .unwrap()
            .set_training_script_state(simnet::TrainingScriptState::Waiting);

        let system_sim_addr =
            ChannelAddr::any(ChannelTransport::Sim(Box::new(ChannelTransport::Unix)));
        // Set very long supervision_update_timeout
        let server_handle = System::serve(
            system_sim_addr.clone(),
            Duration::from_secs(1000),
            Duration::from_secs(1000),
        )
        .await
        .unwrap();

        let mut system = System::new(server_handle.local_addr().clone());
        let client_mailbox = system.attach().await.unwrap();

        // Bootstrap the controller
        let controller_id = id!(controller[0].root);
        let proc_id = id!(world[0]);
        let controller_proc_listen_addr =
            ChannelAddr::any(ChannelTransport::Sim(Box::new(ChannelTransport::Unix)));

        let (_, actor_ref) = ControllerActor::bootstrap(
            controller_id.clone(),
            controller_proc_listen_addr,
            system_sim_addr,
            ControllerParams {
                world_size: 1,
                comm_actor_ref: ActorRef::attest(controller_id.proc_id().actor_id("comm", 0)),
                worker_gang_ref: GangRef::attest(GangId(
                    WorldId(
                        proc_id
                            .world_name()
                            .expect("only ranked actors are supported in the controller tests")
                            .to_string(),
                    ),
                    "worker".to_string(),
                )),
                supervision_query_interval: Duration::from_secs(100),
                worker_progress_check_interval: Duration::from_secs(100),
                operation_timeout: Duration::from_secs(1000),
                operations_per_worker_progress_request: 100,
                fail_on_worker_timeout: false,
            },
            Duration::from_secs(100),
            HashMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(*actor_ref.actor_id(), controller_id);

        actor_ref
            .attach(
                &client_mailbox,
                ActorRef::attest(client_mailbox.self_id().clone()),
            )
            .await
            .unwrap();

        let (_client_supervision_tx, mut client_supervision_rx) =
            client_mailbox.bind_actor_port::<ClientMessage>();

        // mock a proc actor that doesn't update supervision state
        let (
            world_id,
            local_proc_id,
            local_proc_addr,
            _,
            local_proc_message_port,
            mut local_proc_message_receiver,
        ) = mock_proc_actor(0, 1).await;

        // Join the world.
        server_handle
            .system_actor_handle()
            .send(SystemMessage::Join {
                proc_id: local_proc_id.clone(),
                world_id,
                proc_message_port: local_proc_message_port.bind(),
                proc_addr: local_proc_addr,
                labels: HashMap::new(),
                lifecycle_mode: ProcLifecycleMode::ManagedBySystem,
            })
            .unwrap();

        assert_matches!(
            local_proc_message_receiver.recv().await.unwrap(),
            ProcMessage::Joined()
        );

        // expect that supervision timeout which takes 1000 real seconds is hit super quickly
        // due to simulated time
        let result = client_supervision_rx
            .recv()
            .await
            .unwrap()
            .into_result()
            .unwrap();
        assert_eq!(result.0, Seq::default());
        assert!(result.1.expect("result").is_err());

        let records = simnet::simnet_handle().unwrap().close().await.unwrap();
        eprintln!("{}", serde_json::to_string_pretty(&records).unwrap());
    }
    #[tokio::test]
    async fn test_supervision_failure() {
        // Start system actor.
        let timeout: Duration = Duration::from_secs(6);
        let server_handle = System::serve(
            ChannelAddr::any(ChannelTransport::Local),
            timeout.clone(),
            timeout.clone(),
        )
        .await
        .unwrap();

        // Client actor.
        let mut system = System::new(server_handle.local_addr().clone());
        let client_mailbox = system.attach().await.unwrap();
        let (_client_supervision_tx, mut client_supervision_rx) =
            client_mailbox.bind_actor_port::<ClientMessage>();

        // Bootstrap the controller
        let controller_id = id!(controller[0].root);
        let proc_id = id!(world[0]);
        let (_, actor_ref) = ControllerActor::bootstrap(
            controller_id.clone(),
            ChannelAddr::any(ChannelTransport::Local),
            server_handle.local_addr().clone(),
            ControllerParams {
                world_size: 1,
                comm_actor_ref: ActorRef::attest(controller_id.proc_id().actor_id("comm", 0)),
                worker_gang_ref: GangRef::attest(GangId(
                    WorldId(
                        proc_id
                            .world_name()
                            .expect("only ranked actors are supported in the controller tests")
                            .to_string(),
                    ),
                    "worker".to_string(),
                )),
                supervision_query_interval: Duration::from_secs(1),
                worker_progress_check_interval: Duration::from_secs(3),
                operation_timeout: Duration::from_secs(30),
                operations_per_worker_progress_request: 100,
                fail_on_worker_timeout: false,
            },
            Duration::from_secs(1),
            HashMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(*actor_ref.actor_id(), controller_id);

        actor_ref
            .attach(
                &client_mailbox,
                ActorRef::attest(client_mailbox.self_id().clone()),
            )
            .await
            .unwrap();

        // mock a proc actor that doesn't update supervision state
        let (
            world_id,
            local_proc_id,
            local_proc_addr,
            _,
            local_proc_message_port,
            mut local_proc_message_receiver,
        ) = mock_proc_actor(0, 1).await;

        // Join the world.
        server_handle
            .system_actor_handle()
            .send(SystemMessage::Join {
                proc_id: local_proc_id.clone(),
                world_id,
                proc_message_port: local_proc_message_port.bind(),
                proc_addr: local_proc_addr,
                labels: HashMap::new(),
                lifecycle_mode: ProcLifecycleMode::ManagedBySystem,
            })
            .unwrap();

        assert_matches!(
            local_proc_message_receiver.recv().await.unwrap(),
            ProcMessage::Joined()
        );

        // Wait a bit; supervision update should time out.
        RealClock.sleep(2 * timeout.clone()).await;

        // Should've gotten the supervision message indicating supervision failure
        let result = client_supervision_rx
            .recv()
            .await
            .unwrap()
            .into_result()
            .unwrap();
        assert_eq!(result.0, Seq::default());
        assert!(result.1.expect("result").is_err());
    }

    #[derive(
        Handler,
        HandleClient,
        RefClient,
        Named,
        Debug,
        Clone,
        Serialize,
        Deserialize,
        PartialEq
    )]
    enum PanickingMessage {
        Panic(String),
    }

    #[derive(Debug, Default, Actor)]
    #[hyperactor::export(
        handlers = [
            PanickingMessage,
        ],
    )]
    struct PanickingActor;

    #[async_trait]
    #[hyperactor::forward(PanickingMessage)]
    impl PanickingMessageHandler for PanickingActor {
        async fn panic(
            &mut self,
            _cx: &Context<Self>,
            err_msg: String,
        ) -> Result<(), anyhow::Error> {
            panic!("{}", err_msg);
        }
    }

    hyperactor::remote!(PanickingActor);

    #[async_timed_test(timeout_secs = 30)]
    // times out (both internal and external).
    #[cfg_attr(not(fbcode_build), ignore)]
    async fn test_supervision_fault() {
        // Need this custom hook to store panic backtrace in task_local.
        panic_handler::set_panic_hook();

        // Start system actor.
        let timeout: Duration = Duration::from_secs(6);
        let server_handle = System::serve(
            ChannelAddr::any(ChannelTransport::Local),
            timeout.clone(),
            timeout.clone(),
        )
        .await
        .unwrap();

        // Client actor.
        let mut system = System::new(server_handle.local_addr().clone());
        let client_mailbox = system.attach().await.unwrap();
        let (_client_supervision_tx, mut client_supervision_rx) =
            client_mailbox.bind_actor_port::<ClientMessage>();

        // Bootstrap the controller
        let controller_id = id!(controller[0].root);
        let proc_id = id!(world[0]);
        let (_, actor_ref) = ControllerActor::bootstrap(
            controller_id.clone(),
            ChannelAddr::any(ChannelTransport::Local),
            server_handle.local_addr().clone(),
            ControllerParams {
                world_size: 1,
                comm_actor_ref: ActorRef::attest(controller_id.proc_id().actor_id("comm", 0)),
                worker_gang_ref: GangRef::attest(GangId(
                    WorldId(
                        proc_id
                            .world_name()
                            .expect("only ranked actors are supported in the controller tests")
                            .to_string(),
                    ),
                    "worker".to_string(),
                )),
                supervision_query_interval: Duration::from_secs(1),
                worker_progress_check_interval: Duration::from_secs(3),
                operation_timeout: Duration::from_secs(30),
                operations_per_worker_progress_request: 100,
                fail_on_worker_timeout: false,
            },
            Duration::from_secs(1),
            HashMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(*actor_ref.actor_id(), controller_id);

        actor_ref
            .attach(
                &client_mailbox,
                ActorRef::attest(client_mailbox.self_id().clone()),
            )
            .await
            .unwrap();

        // bootstreap an actor that panics
        let world_id = id!(world);
        let panic_proc_id = world_id.proc_id(1);
        let bootstrap = ProcActor::bootstrap(
            panic_proc_id,
            world_id,
            ChannelAddr::any(ChannelTransport::Local),
            server_handle.local_addr().clone(),
            Duration::from_secs(3),
            HashMap::new(),
            ProcLifecycleMode::ManagedBySystem,
        )
        .await
        .unwrap();
        let actor_handle = spawn::<PanickingActor>(
            &client_mailbox,
            &bootstrap.proc_actor.bind(),
            "panicker",
            &(),
        )
        .await
        .unwrap();

        actor_handle
            .panic(&client_mailbox, "some random failure".to_string())
            .await
            .unwrap();

        // Get the supervision message with the panic
        let result = client_supervision_rx
            .recv()
            .await
            .unwrap()
            .into_result()
            .unwrap();
        assert_eq!(result.0, Seq::default());
        assert!(result.1.is_some() && result.1.as_ref().unwrap().is_err());
        let Exception::Failure(err) = result.1.unwrap().unwrap_err() else {
            panic!("Expected Failure exception");
        };
        assert!(
            err.backtrace.contains("some random failure"),
            "got: {}",
            err.backtrace
        );
    }
}
