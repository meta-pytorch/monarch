/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use super::*;

struct PendingSend {
    state: Arc<SendState>,
    _lease: Arc<BufferLease>,
    buffer: Option<QuicheBuffer>,
}

struct PendingReceive {
    operation: chrysalis_transport_core::OperationId,
    buffer: PostedBuffer,
    options: ReceiveOptions,
    initial_length: usize,
    cancellation: OperationCancellation,
}

struct PendingDiscard {
    submission: DiscardSubmission,
    discarded: usize,
}

enum SendOp {
    Data(PendingSend),
    Finish(FinishSubmission),
    Reset(ResetSubmission),
}

enum ReceiveOp {
    Receive(PendingReceive),
    Discard(PendingDiscard),
    Stop(StopSubmission),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) enum SendHalfState {
    #[default]
    Open,
    FinQueued,
    ResetQueued,
    Finished,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) enum ReceiveHalfState {
    #[default]
    Open,
    Fin,
    Reset(u64),
    Stopped(u64),
    Closed,
}

impl ReceiveHalfState {
    fn terminal_status(self) -> Option<ReceiveStatus> {
        match self {
            Self::Open => None,
            Self::Fin => Some(ReceiveStatus::Fin),
            Self::Reset(code) => Some(ReceiveStatus::Reset(code)),
            Self::Stopped(code) => Some(ReceiveStatus::Stopped(code)),
            Self::Closed => Some(ReceiveStatus::Closed),
        }
    }
}

#[derive(Default)]
pub(super) struct StreamState {
    incoming_notified: bool,
    next_send_offset: u64,
    pub(super) send_state: SendHalfState,
    send_ops: VecDeque<SendOp>,
    send_order: VecDeque<Arc<SendState>>,
    pub(super) receive_state: ReceiveHalfState,
    receive_ops: VecDeque<ReceiveOp>,
}

#[cfg(test)]
impl StreamState {
    pub(super) fn terminal() -> Self {
        Self {
            send_state: SendHalfState::Finished,
            receive_state: ReceiveHalfState::Fin,
            ..Self::default()
        }
    }
}

pub(super) struct ConnectionState {
    pub(super) connection: quiche::Connection<BufferFactory>,
    local_route: Pid,
    pub(super) is_server: bool,
    pub(super) established_notified: bool,
    readable_streams: Vec<u64>,
    writable_streams: Vec<u64>,
    pub(super) runnable_streams: VecDeque<u64>,
    queued_streams: HashSet<u64>,
    send_completions: Arc<SendCompletionSink>,
    pub(super) streams: HashMap<u64, StreamState>,
    next_local_bidi: Option<u64>,
    expected_peer: Option<Pid>,
    pub(super) peer: Option<Pid>,
    pub(super) remote_address: std::net::SocketAddr,
    max_streams: usize,
    pub(super) reclaimed_streams: u64,
}

impl ConnectionState {
    pub(super) fn new(
        connection: quiche::Connection<BufferFactory>,
        local_route: Pid,
        is_server: bool,
        expected_peer: Option<Pid>,
        remote_address: std::net::SocketAddr,
        max_streams: NonZeroUsize,
    ) -> Self {
        Self {
            connection,
            local_route,
            is_server,
            established_notified: false,
            readable_streams: Vec::new(),
            writable_streams: Vec::new(),
            runnable_streams: VecDeque::new(),
            queued_streams: HashSet::new(),
            send_completions: Arc::new(SendCompletionSink::default()),
            streams: HashMap::new(),
            next_local_bidi: Some(u64::from(is_server)),
            expected_peer,
            peer: None,
            remote_address,
            max_streams: max_streams.get(),
            reclaimed_streams: 0,
        }
    }

    /// Allocates the next locally initiated bidirectional stream ID.
    pub(super) fn allocate_bidi(&mut self, connection: ConnectionId) -> Result<StreamId, Error> {
        if self.streams.len() == self.max_streams {
            return Err(Error::StreamLimit);
        }
        let stream = self.next_local_bidi.ok_or(Error::StreamIdExhausted)?;
        self.next_local_bidi = stream.checked_add(4).filter(|next| *next < (1_u64 << 62));
        self.streams.entry(stream).or_default();
        Ok(StreamId::new(connection, stream))
    }

    pub(super) fn has_pending_operations(&self) -> bool {
        self.streams.values().any(|stream| {
            !stream.send_ops.is_empty()
                || !stream.send_order.is_empty()
                || !stream.receive_ops.is_empty()
        })
    }

    pub(super) fn enqueue(&mut self, submission: Submission, completions: &mut Vec<Completion>) {
        let stream_id = submission.stream().stream();
        let send_completions = self.send_completions.clone();
        let Some(stream) = self.streams.get_mut(&stream_id) else {
            complete_unknown(submission, completions);
            return;
        };
        match submission {
            Submission::Send(submission) => {
                Self::enqueue_send(stream, submission, send_completions, completions)
            }
            Submission::Receive(submission) => Self::enqueue_receive(stream, submission),
            Submission::Finish(submission) => Self::enqueue_finish(stream, submission, completions),
            Submission::Discard(submission) => {
                stream
                    .receive_ops
                    .push_back(ReceiveOp::Discard(PendingDiscard {
                        submission,
                        discarded: 0,
                    }));
            }
            Submission::Reset(submission) => Self::enqueue_reset(stream, submission, completions),
            Submission::Stop(submission) => {
                stream.receive_ops.push_back(ReceiveOp::Stop(submission));
            }
        }
        self.mark_runnable(stream_id);
    }

    fn enqueue_send(
        stream: &mut StreamState,
        submission: SendSubmission,
        send_completions: Arc<SendCompletionSink>,
        completions: &mut Vec<Completion>,
    ) {
        let (operation, stream_id, payload) = submission.into_parts();
        let bytes = payload.len();
        if stream.send_state != SendHalfState::Open {
            drop(payload);
            completions.push(Completion::Send {
                operation,
                stream: stream_id,
                bytes,
                outcome: SendOutcome::Rejected,
            });
            return;
        }
        let end = stream
            .next_send_offset
            .checked_add(bytes as u64)
            .expect("stream send offset should not overflow");
        stream.next_send_offset = end;
        let state = Arc::new(SendState::new(operation, stream_id, bytes, end));
        stream.send_order.push_back(state.clone());
        if bytes == 0 {
            drop(payload);
            return;
        }
        let lease = Arc::new(BufferLease::new(state.clone(), send_completions));
        let buffer = QuicheBuffer::submission(payload, lease.clone());
        stream.send_ops.push_back(SendOp::Data(PendingSend {
            state,
            _lease: lease,
            buffer: Some(buffer),
        }));
    }

    fn enqueue_finish(
        stream: &mut StreamState,
        submission: FinishSubmission,
        completions: &mut Vec<Completion>,
    ) {
        if stream.send_state == SendHalfState::Open {
            stream.send_state = SendHalfState::FinQueued;
            stream.send_ops.push_back(SendOp::Finish(submission));
        } else {
            completions.push(Completion::Finish {
                operation: submission.operation(),
                stream: submission.stream(),
                outcome: ControlOutcome::Rejected,
            });
        }
    }

    fn enqueue_reset(
        stream: &mut StreamState,
        submission: ResetSubmission,
        completions: &mut Vec<Completion>,
    ) {
        if stream.send_state == SendHalfState::Open {
            stream.send_state = SendHalfState::ResetQueued;
            stream.send_ops.push_back(SendOp::Reset(submission));
        } else {
            completions.push(Completion::Reset {
                operation: submission.operation(),
                stream: submission.stream(),
                outcome: ControlOutcome::Rejected,
            });
        }
    }

    fn enqueue_receive(stream: &mut StreamState, submission: ReceiveSubmission) {
        let cancellation = submission.cancellation();
        let (operation, _, buffer, options) = submission.into_parts();
        let initial_length = buffer.buffer().len();
        stream
            .receive_ops
            .push_back(ReceiveOp::Receive(PendingReceive {
                operation,
                buffer,
                options,
                initial_length,
                cancellation,
            }));
    }

    pub(super) fn progress(
        &mut self,
        connection_id: ConnectionId,
        discard_buffer: &mut [u8],
        completions: &mut Vec<Completion>,
    ) -> bool {
        let mut became_established = false;
        let mut newly_established = false;
        if self.connection.is_established() && !self.established_notified {
            self.established_notified = true;
            became_established = self.is_server;
            newly_established = true;
            let actual = self.connection.peer_cert().map(certificate_pid);
            if actual.is_none()
                || self
                    .expected_peer
                    .is_some_and(|expected| Some(expected) != actual)
            {
                completions.push(Completion::AuthenticationFailed(AuthenticationFailed::new(
                    connection_id,
                    self.expected_peer,
                    actual,
                )));
                let _ = self.connection.close(
                    true,
                    AUTHENTICATION_ERROR_CODE,
                    b"peer identity mismatch",
                );
                return became_established;
            }
            self.peer = actual;
            completions.push(Completion::ConnectionEstablished(
                ConnectionEstablishedCompletion::new(
                    connection_id,
                    self.local_route,
                    actual.expect("authenticated peer certificate should produce a PID"),
                ),
            ));
        }

        if newly_established {
            self.writable_streams.clear();
            self.writable_streams.extend(self.streams.keys().copied());
            for index in 0..self.writable_streams.len() {
                self.mark_runnable(self.writable_streams[index]);
            }
        }

        while let Some(stream) = self.send_completions.pop() {
            self.mark_runnable(stream);
        }

        self.readable_streams.clear();
        self.readable_streams.extend(self.connection.readable());
        for index in 0..self.readable_streams.len() {
            let stream = self.readable_streams[index];
            if !self.streams.contains_key(&stream) && self.streams.len() == self.max_streams {
                let _ = self
                    .connection
                    .stream_shutdown(stream, quiche::Shutdown::Read, 0);
                continue;
            }
            let local_initiator = u64::from(self.is_server);
            let state = self.streams.entry(stream).or_default();
            let notify =
                stream & 0x2 == 0 && stream & 0x1 != local_initiator && !state.incoming_notified;
            if notify {
                state.incoming_notified = true;
                completions.push(Completion::IncomingStream(StreamId::new(
                    connection_id,
                    stream,
                )));
            }
            self.mark_runnable(stream);
        }

        self.writable_streams.clear();
        self.writable_streams.extend(self.connection.writable());
        for index in 0..self.writable_streams.len() {
            self.mark_runnable(self.writable_streams[index]);
        }

        while let Some(stream_id) = self.runnable_streams.pop_front() {
            self.queued_streams.remove(&stream_id);
            let Some(stream) = self.streams.get_mut(&stream_id) else {
                continue;
            };
            Self::progress_send_ops(&mut self.connection, stream_id, stream, completions);
            Self::progress_receive_ops(
                &mut self.connection,
                connection_id,
                stream_id,
                stream,
                discard_buffer,
                completions,
            );
            Self::collect_send_completions(stream, completions);

            if Self::stream_is_terminal(stream) {
                let error_code = match stream.receive_state {
                    ReceiveHalfState::Reset(code) => Some(code),
                    _ => None,
                };
                self.streams.remove(&stream_id);
                self.reclaimed_streams = self.reclaimed_streams.saturating_add(1);
                completions.push(Completion::Closed {
                    stream: StreamId::new(connection_id, stream_id),
                    error_code,
                });
            }
        }
        became_established
    }

    pub(super) fn mark_runnable(&mut self, stream: u64) {
        if self.queued_streams.insert(stream) {
            self.runnable_streams.push_back(stream);
        }
    }

    pub(super) fn stream_is_terminal(stream: &StreamState) -> bool {
        stream.send_state == SendHalfState::Finished
            && stream.receive_state.terminal_status().is_some()
            && stream.send_ops.is_empty()
            && stream.send_order.is_empty()
            && stream.receive_ops.is_empty()
    }

    fn ensure_send_stream(connection: &mut quiche::Connection<BufferFactory>, stream_id: u64) {
        if matches!(
            connection.stream_capacity(stream_id),
            Err(quiche::Error::InvalidStreamState(_))
        ) {
            match connection.stream_send(stream_id, &[], false) {
                Ok(0) | Err(quiche::Error::Done) => {}
                Ok(written) => panic!("empty stream creation wrote {written} bytes"),
                Err(_) => {}
            }
        }
    }

    fn progress_send_ops(
        connection: &mut quiche::Connection<BufferFactory>,
        stream_id: u64,
        stream: &mut StreamState,
        completions: &mut Vec<Completion>,
    ) {
        if stream.send_ops.is_empty() || !connection.is_established() {
            return;
        }
        Self::ensure_send_stream(connection, stream_id);
        while let Some(operation) = stream.send_ops.front_mut() {
            match operation {
                SendOp::Data(pending) => {
                    let capacity = match connection.stream_capacity(stream_id) {
                        Ok(0) | Err(quiche::Error::Done) => break,
                        Ok(capacity) => capacity,
                        Err(quiche::Error::StreamStopped(_)) => {
                            Self::abandon_send_half(stream, completions);
                            break;
                        }
                        Err(_) => break,
                    };
                    let buffer = pending
                        .buffer
                        .take()
                        .expect("pending send should retain its unwritten suffix");
                    match connection.stream_send_zc(stream_id, buffer, Some(capacity), false) {
                        Ok((_, Some(remainder))) => pending.buffer = Some(remainder),
                        Ok((_, None)) => {
                            stream.send_ops.pop_front();
                        }
                        Err(_) => {
                            pending.state.abandon();
                            Self::abandon_send_half(stream, completions);
                            break;
                        }
                    }
                }
                SendOp::Finish(finish) => match connection.stream_send(stream_id, &[], true) {
                    Ok(0) => {
                        let finish = *finish;
                        stream.send_ops.pop_front();
                        stream.send_state = SendHalfState::Finished;
                        completions.push(Completion::Finish {
                            operation: finish.operation(),
                            stream: finish.stream(),
                            outcome: ControlOutcome::Complete,
                        });
                    }
                    Err(quiche::Error::Done) => break,
                    Ok(written) => panic!("empty FIN wrote {written} bytes"),
                    Err(_) => {
                        let finish = *finish;
                        stream.send_ops.pop_front();
                        stream.send_state = SendHalfState::Finished;
                        completions.push(Completion::Finish {
                            operation: finish.operation(),
                            stream: finish.stream(),
                            outcome: ControlOutcome::Abandoned,
                        });
                    }
                },
                SendOp::Reset(reset) => {
                    let reset = *reset;
                    let outcome = if connection
                        .stream_shutdown(stream_id, quiche::Shutdown::Write, reset.error_code())
                        .is_ok()
                    {
                        ControlOutcome::Complete
                    } else {
                        ControlOutcome::Abandoned
                    };
                    stream.send_ops.pop_front();
                    for state in &stream.send_order {
                        state.abandon();
                    }
                    stream.send_state = SendHalfState::Finished;
                    completions.push(Completion::Reset {
                        operation: reset.operation(),
                        stream: reset.stream(),
                        outcome,
                    });
                }
            }
        }
    }

    fn progress_receive_ops(
        connection: &mut quiche::Connection<BufferFactory>,
        connection_id: ConnectionId,
        stream_id: u64,
        stream: &mut StreamState,
        discard_buffer: &mut [u8],
        completions: &mut Vec<Completion>,
    ) {
        while !stream.receive_ops.is_empty() {
            let status = if let Some(status) = stream.receive_state.terminal_status() {
                Some(status)
            } else {
                match stream.receive_ops.front_mut().unwrap() {
                    ReceiveOp::Receive(pending) => {
                        if pending.cancellation.is_cancelled() {
                            Some(ReceiveStatus::Cancelled)
                        } else {
                            let result = {
                                let buffer = pending.buffer.buffer_mut();
                                let initial_length = buffer.len();
                                let spare = buffer.spare_capacity_mut();
                                // SAFETY: MaybeUninit<u8> has the same layout as u8. quiche writes at
                                // most the supplied length, and set_len exposes that initialized prefix.
                                let output = unsafe {
                                    std::slice::from_raw_parts_mut(
                                        spare.as_mut_ptr().cast(),
                                        spare.len(),
                                    )
                                };
                                match connection.stream_recv(stream_id, output) {
                                    Ok((read, fin)) => {
                                        // SAFETY: quiche initialized the first `read` spare bytes.
                                        unsafe { buffer.set_len(initial_length + read) };
                                        Ok((read, fin))
                                    }
                                    Err(error) => Err(error),
                                }
                            };
                            match result {
                                Ok((_, true)) => Some(ReceiveStatus::Fin),
                                Ok(_)
                                    if pending.buffer.buffer().len() - pending.initial_length
                                        >= pending.options.min_bytes().get() =>
                                {
                                    Some(ReceiveStatus::Data)
                                }
                                Ok(_) => None,
                                Err(quiche::Error::Done | quiche::Error::InvalidStreamState(_)) => {
                                    break;
                                }
                                Err(quiche::Error::StreamReset(error)) => {
                                    Some(ReceiveStatus::Reset(error))
                                }
                                Err(_) => Some(ReceiveStatus::Closed),
                            }
                        }
                    }
                    ReceiveOp::Discard(pending) => {
                        let remaining = pending.submission.max_bytes().get() - pending.discarded;
                        let output_length = cmp::min(remaining, discard_buffer.len());
                        let output = &mut discard_buffer[..output_length];
                        match connection.stream_recv(stream_id, output) {
                            Ok((read, fin)) => {
                                pending.discarded += read;
                                if fin {
                                    Some(ReceiveStatus::Fin)
                                } else if pending.discarded == pending.submission.max_bytes().get()
                                {
                                    Some(ReceiveStatus::Data)
                                } else {
                                    None
                                }
                            }
                            Err(quiche::Error::Done | quiche::Error::InvalidStreamState(_)) => {
                                break;
                            }
                            Err(quiche::Error::StreamReset(error)) => {
                                Some(ReceiveStatus::Reset(error))
                            }
                            Err(_) => Some(ReceiveStatus::Closed),
                        }
                    }
                    ReceiveOp::Stop(stop) => {
                        let status = ReceiveStatus::Stopped(stop.error_code());
                        let _ = connection.stream_shutdown(
                            stream_id,
                            quiche::Shutdown::Read,
                            stop.error_code(),
                        );
                        Some(status)
                    }
                }
            };
            let Some(status) = status else {
                continue;
            };
            stream.receive_state = match status {
                ReceiveStatus::Data | ReceiveStatus::Cancelled => ReceiveHalfState::Open,
                ReceiveStatus::Fin => ReceiveHalfState::Fin,
                ReceiveStatus::Reset(code) => ReceiveHalfState::Reset(code),
                ReceiveStatus::Stopped(code) => ReceiveHalfState::Stopped(code),
                ReceiveStatus::Closed => ReceiveHalfState::Closed,
            };
            match stream.receive_ops.pop_front().unwrap() {
                ReceiveOp::Receive(pending) => {
                    completions.push(Completion::Receive(ReceiveCompletion::new(
                        pending.operation,
                        StreamId::new(connection_id, stream_id),
                        pending.buffer,
                        pending.initial_length,
                        status,
                    )));
                }
                ReceiveOp::Discard(pending) => completions.push(Completion::Discard {
                    operation: pending.submission.operation(),
                    stream: pending.submission.stream(),
                    bytes: pending.discarded,
                    status,
                }),
                ReceiveOp::Stop(stop) => completions.push(Completion::Stop {
                    operation: stop.operation(),
                    stream: stop.stream(),
                    outcome: ControlOutcome::Complete,
                }),
            }
        }
    }

    fn collect_send_completions(stream: &mut StreamState, completions: &mut Vec<Completion>) {
        while stream
            .send_order
            .front()
            .is_some_and(|state| state.is_completion_ready())
        {
            let state = stream.send_order.pop_front().unwrap();
            let outcome = if state.is_abandoned() {
                SendOutcome::Abandoned
            } else {
                SendOutcome::Acknowledged {
                    acknowledged_through: state.acknowledged_through,
                }
            };
            completions.push(Completion::Send {
                operation: state.operation,
                stream: state.stream,
                bytes: state.bytes,
                outcome,
            });
        }
    }

    fn abandon_send_half(stream: &mut StreamState, completions: &mut Vec<Completion>) {
        for state in &stream.send_order {
            state.abandon();
        }
        while let Some(operation) = stream.send_ops.pop_front() {
            match operation {
                SendOp::Finish(finish) => completions.push(Completion::Finish {
                    operation: finish.operation(),
                    stream: finish.stream(),
                    outcome: ControlOutcome::Abandoned,
                }),
                SendOp::Reset(reset) => completions.push(Completion::Reset {
                    operation: reset.operation(),
                    stream: reset.stream(),
                    outcome: ControlOutcome::Abandoned,
                }),
                SendOp::Data(_) => {}
            }
        }
        stream.send_state = SendHalfState::Finished;
    }

    pub(super) fn abandon(
        mut self,
        connection_id: ConnectionId,
        completions: &mut Vec<Completion>,
    ) {
        for (stream_id, mut stream) in self.streams.drain() {
            for state in stream.send_order.drain(..) {
                state.abandon();
                completions.push(Completion::Send {
                    operation: state.operation,
                    stream: state.stream,
                    bytes: state.bytes,
                    outcome: SendOutcome::Abandoned,
                });
            }
            while let Some(operation) = stream.send_ops.pop_front() {
                match operation {
                    SendOp::Finish(finish) => completions.push(Completion::Finish {
                        operation: finish.operation(),
                        stream: finish.stream(),
                        outcome: ControlOutcome::Abandoned,
                    }),
                    SendOp::Reset(reset) => completions.push(Completion::Reset {
                        operation: reset.operation(),
                        stream: reset.stream(),
                        outcome: ControlOutcome::Abandoned,
                    }),
                    SendOp::Data(_) => {}
                }
            }
            while let Some(operation) = stream.receive_ops.pop_front() {
                match operation {
                    ReceiveOp::Receive(receive) => {
                        completions.push(Completion::Receive(ReceiveCompletion::new(
                            receive.operation,
                            StreamId::new(connection_id, stream_id),
                            receive.buffer,
                            receive.initial_length,
                            ReceiveStatus::Closed,
                        )));
                    }
                    ReceiveOp::Discard(discard) => completions.push(Completion::Discard {
                        operation: discard.submission.operation(),
                        stream: discard.submission.stream(),
                        bytes: discard.discarded,
                        status: ReceiveStatus::Closed,
                    }),
                    ReceiveOp::Stop(stop) => completions.push(Completion::Stop {
                        operation: stop.operation(),
                        stream: stop.stream(),
                        outcome: ControlOutcome::Abandoned,
                    }),
                }
            }
        }
    }
}
