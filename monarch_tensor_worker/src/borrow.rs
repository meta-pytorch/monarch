/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::mem::replace;
use std::sync::Arc;

use anyhow::Result;
use hyperactor::Mailbox;
use hyperactor::PortHandle;
use hyperactor::actor::ActorHandle;
use hyperactor::context;
use hyperactor::mailbox::PortReceiver;
use tokio::sync::Mutex;
use torch_sys_cuda::cuda::Event;

use crate::Ref;
use crate::stream::StreamActor;
use crate::stream::StreamMessageClient;
use crate::stream::TensorCellResult;

/// Manages the state machine for a borrow, and provides runtime enforcement
/// that a borrow can never be in a weird state.
#[derive(Debug)]
pub struct Borrow {
    #[allow(dead_code)]
    tensor_ref: Ref,
    result: Ref,
    from_stream: Arc<ActorHandle<StreamActor>>,
    to_stream: Arc<ActorHandle<StreamActor>>,
    state: BorrowState,
    id: u64,
}

/// Allowable borrow states.
#[derive(Debug)]
enum BorrowState {
    /// The borrow is created, but not used yet.
    Created {
        first_use_receiver: PortReceiver<(Option<Event>, TensorCellResult)>,
        last_use_sender: PortHandle<(Option<Event>, TensorCellResult)>,
        last_use_receiver: PortReceiver<(Option<Event>, TensorCellResult)>,
    },
    /// `FirstUsed`: First use of the borrow on the receiving stream. The
    /// receiving stream must wait for the sending stream to reach the
    /// `BorrowCreate`` point before being able to use the borrowed value.
    FirstUsed {
        last_use_sender: PortHandle<(Option<Event>, TensorCellResult)>,
        last_use_receiver: PortReceiver<(Option<Event>, TensorCellResult)>,
    },
    /// `LastUsed`: Last use of the borrow on the receiving stream.
    LastUsed {
        last_use_receiver: PortReceiver<(Option<Event>, TensorCellResult)>,
    },
    /// `Dropped`: The borrow is dropped. The sending stream must wait for the
    /// receiving stream to reach the `LastUse`` point before being able to use
    /// the borrowed value.
    Dropped,
    /// Helper state so we can `mem::replace` to avoid a refcount bump.
    Intermediate,
}

impl Borrow {
    pub async fn create(
        cx: &impl context::Actor,
        borrow_id: u64,
        tensor_ref: Ref,
        result: Ref,
        from_stream: Arc<ActorHandle<StreamActor>>,
        to_stream: Arc<ActorHandle<StreamActor>>,
    ) -> Result<Borrow> {
        let (first_use_sender, first_use_receiver) =
            Mailbox::new_detached(to_stream.actor_id().clone()).open_port();
        let (last_use_sender, last_use_receiver) =
            Mailbox::new_detached(from_stream.actor_id().clone()).open_port();

        from_stream
            .borrow_create(cx, borrow_id, tensor_ref, first_use_sender)
            .await?;

        let state = BorrowState::Created {
            first_use_receiver,
            last_use_sender,
            last_use_receiver,
        };

        Ok(Self {
            tensor_ref,
            result,
            from_stream,
            to_stream,
            state,
            id: borrow_id,
        })
    }

    pub async fn first_use(&mut self, cx: &impl context::Actor) -> Result<()> {
        let state = replace(&mut self.state, BorrowState::Intermediate);
        let (last_use_sender, last_use_receiver) = match state {
            BorrowState::Created {
                first_use_receiver,
                last_use_sender,
                last_use_receiver,
            } => {
                self.to_stream
                    .borrow_first_use(
                        cx,
                        self.id,
                        self.result,
                        Arc::new(Mutex::new(first_use_receiver)),
                    )
                    .await?;
                (last_use_sender, last_use_receiver)
            }
            _ => panic!(
                "Called `first_use` on borrow in unexpected state: {:?}",
                state
            ),
        };

        self.state = BorrowState::FirstUsed {
            last_use_sender,
            last_use_receiver,
        };

        Ok(())
    }

    pub async fn last_use(&mut self, cx: &impl context::Actor) -> Result<()> {
        let state = replace(&mut self.state, BorrowState::Intermediate);
        let last_use_receiver = match state {
            BorrowState::FirstUsed {
                last_use_sender,
                last_use_receiver,
            } => {
                self.to_stream
                    .borrow_last_use(cx, self.id, self.result, last_use_sender)
                    .await?;
                last_use_receiver
            }
            _ => panic!(
                "Called `last_use` on borrow in unexpected state: {:?}",
                state
            ),
        };

        self.state = BorrowState::LastUsed { last_use_receiver };
        Ok(())
    }

    pub async fn drop(&mut self, cx: &impl context::Actor) -> Result<()> {
        let state = replace(&mut self.state, BorrowState::Intermediate);
        match state {
            BorrowState::LastUsed { last_use_receiver } => {
                self.from_stream
                    .borrow_drop(cx, self.id, Arc::new(Mutex::new(last_use_receiver)))
                    .await?;
            }
            _ => panic!("Called `drop` on borrow in unexpected state: {:?}", state),
        }

        self.state = BorrowState::Dropped;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use anyhow::Context;
    use anyhow::Result;
    use hyperactor::RemoteSpawn;
    use hyperactor::proc::Proc;
    use hyperactor_config::Flattrs;
    use monarch_messages::controller::ControllerMessage;
    use monarch_messages::worker::ArgsKwargs;
    use monarch_messages::worker::WorkerMessage;
    use monarch_messages::worker::WorkerMessageClient;
    use monarch_messages::worker::WorkerParams;
    use timed_test::async_timed_test;
    use torch_sys2::Device;

    use super::*;
    use crate::CallFunctionParams;
    use crate::StreamCreationMode;
    use crate::WireValue;
    use crate::WorkerActor;
    use crate::test_util::test_setup;

    async fn basic_borrow_test_impl(device: Device) -> Result<()> {
        test_setup()?;

        let proc = Proc::local();
        let (client, controller_ref, mut controller_rx) = proc.attach_actor("controller").unwrap();

        let worker_handle = proc
            .spawn::<WorkerActor>(
                "worker",
                WorkerActor::new(
                    WorkerParams {
                        world_size: 1,
                        rank: 0,
                        device_index: None,
                        controller_actor: controller_ref,
                    },
                    Flattrs::default(),
                )
                .await?,
            )
            .unwrap();

        worker_handle
            .command_group(
                &client,
                vec![
                    // Start with one stream (stream 0)
                    WorkerMessage::CreateStream {
                        id: 0.into(),
                        stream_creation: StreamCreationMode::CreateNewStream,
                    },
                    // Create a tensor on this stream.
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 0.into(),
                        results: vec![Some(Ref { id: 1 })],
                        mutates: vec![],
                        function: "torch.ops.aten.ones.default".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![WireValue::IntList(vec![2, 3])],
                            HashMap::from([("device".into(), WireValue::Device(device))]),
                        )
                        .unwrap(),
                        stream: 0.into(),
                        remote_process_groups: vec![],
                    }),
                    // Make a new stream (stream 3)
                    WorkerMessage::CreateStream {
                        id: 3.into(),
                        stream_creation: StreamCreationMode::CreateNewStream,
                    },
                    // Create a tensor on the new stream.
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 1.into(),
                        results: vec![Some(Ref { id: 4 })],
                        mutates: vec![],
                        function: "torch.ops.aten.ones.default".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![WireValue::IntList(vec![2, 3])],
                            HashMap::from([("device".into(), WireValue::Device(device))]),
                        )
                        .unwrap(),
                        stream: 3.into(),
                        remote_process_groups: vec![],
                    }),
                    // Borrow new tensor from stream 3 -> 0
                    WorkerMessage::BorrowCreate {
                        result: Ref { id: 5 },
                        borrow: 0,
                        tensor: Ref { id: 4 },
                        from_stream: 3.into(),
                        to_stream: 0.into(),
                    },
                    WorkerMessage::BorrowFirstUse { borrow: 0 },
                    // On stream 0, use the borrowed value in a mutating operation!
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 2.into(),
                        results: vec![Some(Ref { id: 6 })],
                        mutates: vec![],
                        function: "torch.ops.aten.sub_.Tensor".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![WireValue::Ref(Ref { id: 5 }), WireValue::Ref(Ref { id: 1 })],
                            HashMap::new(),
                        )
                        .unwrap(),
                        stream: 0.into(),
                        remote_process_groups: vec![],
                    }),
                    WorkerMessage::BorrowLastUse { borrow: 0 },
                    WorkerMessage::BorrowDrop { borrow: 0 },
                    // Check the value on stream 3, first by creating a zeros tensor...
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 3.into(),
                        results: vec![Some(Ref { id: 7 })],
                        mutates: vec![],
                        function: "torch.ops.aten.zeros.default".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![WireValue::IntList(vec![2, 3])],
                            HashMap::from([("device".into(), WireValue::Device(device))]),
                        )
                        .unwrap(),
                        stream: 3.into(),
                        remote_process_groups: vec![],
                    }),
                    // ...then doing an allclose
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 4.into(),
                        results: vec![Some(Ref { id: 8 })],
                        mutates: vec![],
                        function: "torch.ops.aten.allclose.default".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![WireValue::Ref(Ref { id: 4 }), WireValue::Ref(Ref { id: 7 })],
                            HashMap::new(),
                        )
                        .unwrap(),
                        stream: 3.into(),
                        remote_process_groups: vec![],
                    }),
                ],
            )
            .await
            .unwrap();

        let result: bool = worker_handle
            .get_ref_unit_tests_only(&client, Ref { id: 8 }, 3.into())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .try_into()
            .unwrap();
        assert!(result);

        worker_handle.drain_and_stop("test").unwrap();
        worker_handle.await;
        let error_responses = controller_rx.drain();
        assert!(
            error_responses.is_empty(),
            "Expected no error responses, got: {:#?}",
            error_responses
        );

        Ok(())
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn borrow_cpu() -> Result<()> {
        basic_borrow_test_impl("cpu".parse().unwrap()).await
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn borrow_cuda() -> Result<()> {
        basic_borrow_test_impl("cuda".parse().unwrap()).await
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn borrow_errored_value() -> Result<()> {
        test_setup()?;

        let proc = Proc::local();
        let (client, controller_ref, mut controller_rx) = proc.attach_actor("controller").unwrap();

        let worker_handle = proc
            .spawn::<WorkerActor>(
                "worker",
                WorkerActor::new(
                    WorkerParams {
                        world_size: 1,
                        rank: 0,
                        device_index: None,
                        controller_actor: controller_ref,
                    },
                    Flattrs::default(),
                )
                .await
                .unwrap(),
            )
            .unwrap();

        worker_handle
            .command_group(
                &client,
                vec![
                    // Start with one stream (stream 0)
                    WorkerMessage::CreateStream {
                        id: 0.into(),
                        stream_creation: StreamCreationMode::CreateNewStream,
                    },
                    // Create an errored value on this stream
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 0.into(),
                        results: vec![Some(Ref { id: 1 })],
                        mutates: vec![],
                        function: "torch.ops.aten.idont.exist".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(vec![], HashMap::new()).unwrap(),
                        stream: 0.into(),
                        remote_process_groups: vec![],
                    }),
                    // Make a new stream (stream 2)
                    WorkerMessage::CreateStream {
                        id: 2.into(),
                        stream_creation: StreamCreationMode::CreateNewStream,
                    },
                    // Borrow errored from stream 0 -> 2
                    WorkerMessage::BorrowCreate {
                        result: 3.into(),
                        borrow: 0,
                        tensor: Ref { id: 1 },
                        from_stream: 0.into(),
                        to_stream: 2.into(),
                    },
                    WorkerMessage::BorrowFirstUse { borrow: 0 },
                    // On stream 2, use the errored borrow in an op.
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 1.into(),
                        results: vec![Some(Ref { id: 4 })],
                        mutates: vec![],
                        function: "torch.ops.aten.sub_.Scalar".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![WireValue::Ref(3.into()), WireValue::Int(1)],
                            HashMap::new(),
                        )
                        .unwrap(),
                        stream: 2.into(),
                        remote_process_groups: vec![],
                    }),
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 1.into(),
                        results: vec![Some(Ref { id: 5 })],
                        mutates: vec![],
                        function: "torch.ops.aten.allclose.default".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![WireValue::Ref(Ref { id: 4 }), WireValue::Ref(Ref { id: 4 })],
                            HashMap::new(),
                        )
                        .unwrap(),
                        stream: 2.into(),
                        remote_process_groups: vec![],
                    }),
                    WorkerMessage::BorrowLastUse { borrow: 0 },
                    WorkerMessage::BorrowDrop { borrow: 0 },
                ],
            )
            .await
            .unwrap();

        let result = worker_handle
            .get_ref_unit_tests_only(&client, Ref { id: 5 }, 2.into())
            .await?;

        // Stop/drain worker before asserts to avoid hangs.
        worker_handle.drain_and_stop("test")?;
        worker_handle.await;
        let error_responses = controller_rx.drain();

        // Unpack and verify result.
        let error = result
            .context("no such ref")?
            .err()
            .context("expected error")?;
        assert!(
            error.contains("failed to resolve function"),
            "If a borrowed value contains an error, downstream calls should propagate that error (unexpected error string: {})",
            error,
        );

        // Should receive exactly one one error response, corresponding to the
        // first failure.
        assert_eq!(
            error_responses.len(),
            1,
            "Expected exactly one error response, got: {:#?}",
            error_responses
        );
        match &error_responses[0] {
            ControllerMessage::RemoteFunctionFailed { seq, .. } => {
                assert_eq!(seq, &0.into())
            }
            _ => panic!("unexpected response {:#?}", error_responses[0]),
        };
        Ok(())
    }
}
