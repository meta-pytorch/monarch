/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Runtime-neutral stream I/O primitives for Chrysalis transports.
//!
//! The design is sympathetic to completion-oriented I/O systems such as `io_uring`. Submission
//! transfers buffer ownership to the driver, and completion returns it. This shape lets drivers
//! map operations onto native submission and completion queues without imposing borrowed buffer
//! lifetimes or an async runtime. Bounded queues make resource limits and backpressure explicit.
//!
//! Applications submit owned buffers through bounded nonblocking queues. A transport driver
//! consumes those submissions on its owning thread and returns operation completions through a
//! second bounded queue. [`Notifier`] connects either queue to an eventfd, condition variable,
//! async-runtime waker, or foreign-language callback without putting runtime policy in this crate.

mod command;
mod completion;
mod credit;
mod endpoint_submission;
mod id;
mod notify;
mod queue;
mod submission;

pub use command::CommandReceipt;
pub use command::CommandReceiver;
pub use command::CommandSender;
pub use command::CommandSubmission;
pub use command::TryCommandError;
pub use command::command_queue;
pub use command::command_queue_with_credits;
pub use completion::AuthenticationFailed;
pub use completion::CommandCompletion;
pub use completion::CommandError;
pub use completion::CommandResult;
pub use completion::Completion;
pub use completion::CompletionQueue;
pub use completion::CompletionSender;
pub use completion::ConnectionEstablished;
pub use completion::ControlOutcome;
pub use completion::LeasedCompletion;
pub use completion::ReceiveCompletion;
pub use completion::ReceiveStatus;
pub use completion::SendOutcome;
pub use completion::TryCompleteError;
pub use completion::completion_queue;
pub use credit::CompletionCredits;
pub use credit::CompletionPermit;
pub use endpoint_submission::EndpointSubmission;
pub use endpoint_submission::EndpointSubmissionReceiver;
pub use endpoint_submission::endpoint_submission_queue;
pub use endpoint_submission::endpoint_submission_queue_with_credits;
pub use id::ConnectionId;
pub use id::DriverId;
pub use id::OperationId;
pub use id::RequestId;
pub use id::StreamId;
pub use notify::NoopNotifier;
pub use notify::Notifier;
pub use submission::AdmittedSubmission;
pub use submission::ControlBlockReason;
pub use submission::DiscardSubmission;
pub use submission::FinishSubmission;
pub use submission::OperationCancellation;
pub use submission::PostedBuffer;
pub use submission::ReceiveBlockReason;
pub use submission::ReceiveOptions;
pub use submission::ReceiveSubmission;
pub use submission::ResetSubmission;
pub use submission::RetainedBytes;
pub use submission::SendBlockReason;
pub use submission::SendSubmission;
pub use submission::StopSubmission;
pub use submission::Submission;
pub use submission::SubmissionLimits;
pub use submission::SubmissionReceipt;
pub use submission::SubmissionReceiver;
pub use submission::SubmissionSender;
pub use submission::TryControlError;
pub use submission::TryReceiveError;
pub use submission::TrySendError;
pub use submission::submission_queue;
pub use submission::submission_queue_with_credits;
