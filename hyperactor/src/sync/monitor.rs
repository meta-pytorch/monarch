/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Monitors supervise a set of related tasks, aborting them on any failure.
//!
//! ```
//! # use hyperactor::sync::monitor;
//! # use hyperactor::sync::flag;
//!
//! # tokio_test::block_on(async {
//! let (group, handle) = monitor::group();
//! let (flag, guard) = flag::guarded();
//! group.spawn(async move {
//!     flag.await;
//!     Result::<(), ()>::Err(())
//! });
//! group.spawn(async move {
//!     guard.signal();
//!     Result::<(), ()>::Ok(())
//! });
//! assert_eq!(handle.await, monitor::Status::Failed);
//! # })
//! ```

// EnumAsInner generates code that triggers a false positive
// unused_assignments lint on struct variant fields. #[allow] on the
// enum itself doesn't propagate into derive-macro-generated code, so
// the suppression must be at module scope.
#![allow(unused_assignments)]

use std::future::Future;
use std::future::IntoFuture;
use std::sync::Arc;
use std::sync::Mutex;

use enum_as_inner::EnumAsInner;
use tokio::task::JoinSet;

use crate::sync::flag;

/// Create a new monitored group and handle. The group is aborted
/// if either group or its handle are dropped.
pub fn group() -> (Group, Handle) {
    let (flag, guard) = flag::guarded();
    let state = Arc::new(Mutex::new(State::Running {
        _guard: guard,
        tasks: JoinSet::new(),
    }));

    let group = Group(Arc::clone(&state));
    let handle = Handle(Some((flag, state)));

    (group, handle)
}

/// A handle to a monitored task group. Handles may be awaited to
/// wait for the completion of the group (failure or abortion).
pub struct Handle(Option<(flag::Flag, Arc<Mutex<State>>)>);

impl Handle {
    /// The current status of the group.
    pub fn status(&self) -> Status {
        self.unwrap_state().lock().unwrap().status()
    }

    /// Abort the group. This aborts all tasks and returns immediately.
    /// Note that the group status is not guaranteed to converge to
    /// [`Status::Aborted`] as this call may race with failing tasks.
    pub fn abort(&self) {
        self.unwrap_state().lock().unwrap().stop(true)
    }

    fn unwrap_state(&self) -> &Arc<Mutex<State>> {
        &self.0.as_ref().unwrap().1
    }

    fn take(&mut self) -> Option<(flag::Flag, Arc<Mutex<State>>)> {
        self.0.take()
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        if let Some((_, ref state)) = self.0 {
            state.lock().unwrap().stop(true);
        }
    }
}

impl IntoFuture for Handle {
    type Output = Status;
    type IntoFuture = impl Future<Output = Self::Output>;
    fn into_future(mut self) -> Self::IntoFuture {
        async move {
            let (flag, state) = self.take().unwrap();
            flag.await;
            #[allow(clippy::let_and_return)]
            let status = state.lock().unwrap().status();
            status
        }
    }
}

/// A group of tasks that share a common fate. Any tasks that are spawned onto
/// the group will be aborted if any task fails or if the group is aborted.
///
/// The group is also aborted if the group itself is dropped.
#[derive(Clone)]
pub struct Group(Arc<Mutex<State>>);

/// The status of a group. Groups start out in [`Status::Running`]
/// and transition exactly zero or one time to either [`Status::Failed`]
/// or [`Status::Aborted`].
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Status {
    /// The group is running zero or more tasks,
    /// none of which have failed.
    Running,
    /// One of the group's tasks has failed, and
    /// the remaining tasks have been canceled.
    Failed,
    /// The group was aborted by calling [`Group::abort`],
    /// and any running tasks have been canceled.
    Aborted,
}

impl Group {
    /// Spawn a new task onto this group. If the task fails, the group is
    /// aborted. If tasks are spawned onto an already stopped group, they are
    /// simply never run.
    pub fn spawn<F, T, E>(&self, fut: F)
    where
        F: Future<Output = Result<T, E>> + Send + 'static,
    {
        let state = Arc::clone(&self.0);
        if let Some((_, tasks)) = self.0.lock().unwrap().as_running_mut() {
            tasks.spawn(async move {
                if fut.await.is_ok() {
                    return;
                }
                state.lock().unwrap().stop(false);
            });
        }
    }

    /// Fail the group. This is equivalent to spawning a task that
    /// immediately fails.
    pub fn fail(&self) {
        self.0.lock().unwrap().stop(false);
    }
}

impl Drop for Group {
    fn drop(&mut self) {
        self.0.lock().unwrap().stop(true);
    }
}

#[derive(EnumAsInner, Debug)]
enum State {
    Running {
        _guard: flag::Guard,
        tasks: JoinSet<()>,
    },
    Stopped(bool /*aborted*/),
}

impl State {
    fn stop(&mut self, aborted: bool) {
        if self.is_running() {
            // This drops both `tasks` and `_guard` which will
            // abort tasks and notify any waiters.
            *self = State::Stopped(aborted);
        }
    }

    pub fn status(&self) -> Status {
        match self {
            State::Running { .. } => Status::Running,
            State::Stopped(false) => Status::Failed,
            State::Stopped(true) => Status::Aborted,
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::future;

    use super::*;

    #[tokio::test]
    async fn test_basic() {
        let (_group, handle) = group();
        assert_eq!(handle.status(), Status::Running);
        handle.abort();
        assert_eq!(handle.status(), Status::Aborted);
        handle.await;
    }

    #[tokio::test]
    async fn test_group_drop() {
        let (group, handle) = group();
        assert_eq!(handle.status(), Status::Running);
        drop(group);
        assert_eq!(handle.status(), Status::Aborted);
        handle.await;
    }

    #[tokio::test]
    async fn test_abort_with_active_tasks() {
        let (group, handle) = group();
        let (flag, guard) = flag::guarded();

        group.spawn(async move {
            let _guard = guard;
            future::pending::<Result<(), ()>>().await
        });

        assert!(!flag.signalled());
        handle.abort();
        assert_eq!(handle.status(), Status::Aborted);

        flag.await;
    }

    #[tokio::test]
    async fn test_fail_on_task_failure() {
        let (group, handle) = group();

        let (first_task_is_scheduled, first_task_is_scheduled_guard) = flag::guarded();
        let (first_task_is_aborted, _first_task_is_aborted_guard) = flag::guarded();

        group.spawn(async move {
            let _guard = _first_task_is_aborted_guard;
            first_task_is_scheduled_guard.signal();
            future::pending::<Result<(), ()>>().await
        });

        let (second_task_should_fail, second_task_should_fail_guard) = flag::guarded();
        group.spawn(async move {
            second_task_should_fail.await;
            Result::<(), ()>::Err(())
        });

        first_task_is_scheduled.await;
        second_task_should_fail_guard.signal();
        first_task_is_aborted.await;

        assert_eq!(handle.await, Status::Failed);
    }
}
