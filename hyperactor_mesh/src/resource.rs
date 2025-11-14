/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This modules defines a set of common message types used for managing resources
//! in hyperactor meshes.

pub mod mesh;

use core::slice::GetDisjointMutIndex as _;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::hash::Hash;
use std::mem::replace;
use std::mem::take;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ops::Range;
use std::time::Duration;

use enum_as_inner::EnumAsInner;
use hyperactor::Bind;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Named;
use hyperactor::PortRef;
use hyperactor::RefClient;
use hyperactor::RemoteMessage;
use hyperactor::Unbind;
use hyperactor::attrs::Attrs;
use hyperactor::mailbox::PortReceiver;
use hyperactor::message::Bind;
use hyperactor::message::Bindings;
use hyperactor::message::Unbind;
use ndslice::Region;
use ndslice::ViewExt;
use serde::Deserialize;
use serde::Serialize;

use crate::bootstrap;
use crate::v1::Name;
use crate::v1::StatusOverlay;

/// The current lifecycle status of a resource.
#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    Named,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
    Hash,
    EnumAsInner,
    strum::Display
)]
pub enum Status {
    /// The resource does not exist.
    NotExist,
    /// The resource is being created.
    Initializing,
    /// The resource is running.
    Running,
    /// The resource is being stopped.
    Stopping,
    /// The resource is stopped.
    Stopped,
    /// The resource has failed, with an error message.
    #[strum(to_string = "Failed({0})")]
    Failed(String),
    /// The resource has been declared failed after a timeout.
    #[strum(to_string = "Timeout({0:?})")]
    Timeout(Duration),
}

impl Status {
    /// Returns whether the status is a terminating status.
    pub fn is_terminating(&self) -> bool {
        matches!(
            self,
            Status::Stopping | Status::Stopped | Status::Failed(_) | Status::Timeout(_)
        )
    }

    /// Tells whether the status represents a failure. A failure is both terminating
    /// (the resource is not running), but also means abnormal exit (the resource
    /// did not stop cleanly).
    pub fn is_failure(&self) -> bool {
        matches!(self, Self::Failed(_) | Self::Timeout(_))
    }

    pub fn is_healthy(&self) -> bool {
        matches!(self, Status::Initializing | Status::Running)
    }
}

impl From<bootstrap::ProcStatus> for Status {
    fn from(status: bootstrap::ProcStatus) -> Self {
        use bootstrap::ProcStatus;
        match status {
            ProcStatus::Starting => Status::Initializing,
            ProcStatus::Running { .. } | ProcStatus::Ready { .. } => Status::Running,
            ProcStatus::Stopping { .. } => Status::Stopping,
            ProcStatus::Stopped { .. } => Status::Stopped,
            ProcStatus::Failed { reason } => Status::Failed(reason),
            ProcStatus::Killed { .. } => Status::Failed(format!("{}", status)),
        }
    }
}

/// Data type used to communicate ranks.
/// Implements [`Bind`] and [`Unbind`]; the comm actor replaces
/// instances with the delivered rank.
#[derive(Clone, Debug, Serialize, Deserialize, Named, PartialEq, Eq, Default)]
pub struct Rank(pub Option<usize>);

impl Rank {
    /// Create a new rank with the provided value.
    pub fn new(rank: usize) -> Self {
        Self(Some(rank))
    }

    /// Unwrap the rank; panics if not set.
    pub fn unwrap(&self) -> usize {
        self.0.unwrap()
    }
}

impl Unbind for Rank {
    fn unbind(&self, bindings: &mut Bindings) -> anyhow::Result<()> {
        bindings.push_back(self)
    }
}

impl Bind for Rank {
    fn bind(&mut self, bindings: &mut Bindings) -> anyhow::Result<()> {
        let bound = bindings.try_pop_front::<Rank>()?;
        self.0 = bound.0;
        Ok(())
    }
}

/// Get the status of a resource across the mesh.
///
/// This message is cast to all ranks; each rank replies with a sparse
/// status **overlay**. The comm reducer merges overlays (right-wins)
/// and the accumulator applies them to produce **full StatusMesh
/// snapshots** on the receiver side.
#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    Named,
    Handler,
    HandleClient,
    RefClient,
    Bind,
    Unbind
)]
pub struct GetRankStatus {
    /// The name of the resource.
    pub name: Name,
    /// Sparse status updates (overlays) from a rank.
    #[binding(include)]
    pub reply: PortRef<StatusOverlay>,
}

impl GetRankStatus {
    pub async fn wait(
        mut rx: PortReceiver<crate::v1::StatusMesh>,
        num_ranks: usize,
        max_idle_time: Duration,
        region: Region, // used only for fallback
    ) -> Result<crate::v1::StatusMesh, crate::v1::StatusMesh> {
        debug_assert_eq!(region.num_ranks(), num_ranks, "region/num_ranks mismatch");

        let mut alarm = hyperactor::time::Alarm::new();
        alarm.arm(max_idle_time);

        // Fallback snapshot if we time out before receiving anything.
        let mut snapshot =
            crate::v1::StatusMesh::from_single(region, crate::resource::Status::NotExist);

        loop {
            let mut sleeper = alarm.sleeper();
            tokio::select! {
                _ = sleeper.sleep() => return Err(snapshot),
                next = rx.recv() => {
                    match next {
                        Ok(mesh) => { snapshot = mesh; }   // latest-wins snapshot
                        Err(_)   => return Err(snapshot),
                    }
                }
            }

            alarm.arm(max_idle_time);

            // Completion: once every rank (among the first
            // `num_ranks`) has reported at least something (i.e.
            // moved off NotExist).
            if snapshot
                .values()
                .take(num_ranks)
                .all(|s| !matches!(s, crate::resource::Status::NotExist))
            {
                break Ok(snapshot);
            }
        }
    }
}

/// Get the status of all resources across the mesh.
#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    Named,
    Handler,
    HandleClient,
    RefClient,
    Bind,
    Unbind
)]
pub struct GetAllRankStatus {
    /// Returns the status and rank of all resources.
    /// TODO: migrate to a ValueOverlay.
    #[binding(include)]
    pub reply: PortRef<Vec<(usize, Status)>>,
}

/// The state of a resource.
#[derive(Clone, Debug, Serialize, Deserialize, Named, PartialEq, Eq)]
pub struct State<S> {
    /// The name of the resource.
    pub name: Name,
    /// Its status.
    pub status: Status,
    /// Optionally, a resource-defined state.
    pub state: Option<S>,
}

impl<S: Serialize> fmt::Display for State<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Use serde_json to serialize the struct to a compact JSON string
        match serde_json::to_string(self) {
            Ok(json) => write!(f, "{}", json),
            Err(e) => write!(f, "<state: serde_json error: {}>", e),
        }
    }
}

/// Create or update a resource according to a spec.
#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    Named,
    Handler,
    HandleClient,
    RefClient,
    Bind,
    Unbind
)]
pub struct CreateOrUpdate<S> {
    /// The name of the resource to create or update.
    pub name: Name,
    /// The rank of the resource, when available.
    #[binding(include)]
    pub rank: Rank,
    /// The specification of the resource.
    pub spec: S,
}

/// Stop a resource according to a spec.
#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    Named,
    Handler,
    HandleClient,
    RefClient,
    Bind,
    Unbind
)]
pub struct Stop {
    /// The name of the resource to stop.
    pub name: Name,
}

/// Stop all resources owned by the receiver of this message.
/// No reply, this just issues the stop command.
/// Use GetRankStatus to determine if it has successfully stopped.
#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    Named,
    Handler,
    HandleClient,
    RefClient,
    Bind,
    Unbind
)]
pub struct StopAll {}

/// Retrieve the current state of the resource.
#[derive(Debug, Serialize, Deserialize, Named, Handler, HandleClient, RefClient)]
pub struct GetState<S> {
    /// The name of the resource.
    pub name: Name,
    /// A reply containing the state.
    #[reply]
    pub reply: PortRef<State<S>>,
}

// Cannot derive Bind and Unbind for this generic, implement manually.
impl<S> Unbind for GetState<S>
where
    S: RemoteMessage,
    S: Unbind,
{
    fn unbind(&self, bindings: &mut Bindings) -> anyhow::Result<()> {
        self.reply.unbind(bindings)
    }
}

impl<S> Bind for GetState<S>
where
    S: RemoteMessage,
    S: Bind,
{
    fn bind(&mut self, bindings: &mut Bindings) -> anyhow::Result<()> {
        self.reply.bind(bindings)
    }
}

impl<S> Clone for GetState<S>
where
    S: RemoteMessage,
{
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            reply: self.reply.clone(),
        }
    }
}

/// A trait that bundles a set of types that together define a resource.
pub trait Resource {
    /// The spec specification for this resource.
    type Spec: Named + Serialize + for<'de> Deserialize<'de> + Send + Sync + std::fmt::Debug;

    /// The state for this resource.
    type State: Named + Serialize + for<'de> Deserialize<'de> + Send + Sync + std::fmt::Debug;
}

// A behavior defining the interface for a mesh controller.
hyperactor::behavior!(
    Controller<R: Resource>,
    CreateOrUpdate<R::Spec>,
    GetState<R::State>,
    Stop,
);

/// RankedValues compactly represents rank-indexed values of type T.
/// It stores contiguous values in a set of intervals; thus it is
/// efficient and compact when the cardinality of T-typed values is
/// low.
#[derive(Debug, Clone, Named, Serialize, Deserialize)]
pub struct RankedValues<T> {
    intervals: Vec<(Range<usize>, T)>,
}

impl<T: PartialEq> PartialEq for RankedValues<T> {
    fn eq(&self, other: &Self) -> bool {
        self.intervals == other.intervals
    }
}

impl<T: Eq> Eq for RankedValues<T> {}

impl<T> Default for RankedValues<T> {
    fn default() -> Self {
        Self {
            intervals: Vec::new(),
        }
    }
}

impl<T> RankedValues<T> {
    /// Iterate over contiguous rank intervals of values.
    pub fn iter(&self) -> impl Iterator<Item = &(Range<usize>, T)> + '_ {
        self.intervals.iter()
    }

    /// The (set) rank of the RankedValues is the number of values stored with
    /// rank less than `value`.
    pub fn rank(&self, value: usize) -> usize {
        self.iter()
            .take_while(|(ranks, _)| ranks.start <= value)
            .map(|(ranks, _)| ranks.end.min(value) - ranks.start)
            .sum()
    }
}

impl<T: Clone> RankedValues<T> {
    pub fn materialized_iter(&self, until: usize) -> impl Iterator<Item = &T> + '_ {
        assert_eq!(self.rank(until), until, "insufficient rank");
        self.iter()
            .flat_map(|(range, value)| std::iter::repeat_n(value, range.end - range.start))
    }
}

impl<T: Hash + Eq + Clone> RankedValues<T> {
    /// Invert this ranked values into a [`ValuesByRank<T>`].
    pub fn invert(&self) -> ValuesByRank<T> {
        let mut inverted: HashMap<T, Vec<Range<usize>>> = HashMap::new();
        for (range, value) in self.iter() {
            inverted
                .entry(value.clone())
                .or_default()
                .push(range.clone());
        }
        ValuesByRank { values: inverted }
    }
}

impl<T: Eq + Clone> RankedValues<T> {
    /// Merge `other` into this set of ranked values. Values in `other` that overlap
    /// with `self` take prededence.
    ///
    /// This currently uses a simple algorithm that merges the full set of RankedValues.
    /// This remains efficient when the cardinality of T-typed values is low. However,
    /// it does not efficiently merge high cardinality value sets. Consider using interval
    /// trees or bitmap techniques like Roaring Bitmaps in these cases.
    pub fn merge_from(&mut self, other: Self) {
        let mut left_iter = take(&mut self.intervals).into_iter();
        let mut right_iter = other.intervals.into_iter();

        let mut left = left_iter.next();
        let mut right = right_iter.next();

        while left.is_some() && right.is_some() {
            let (left_ranks, left_value) = left.as_mut().unwrap();
            let (right_ranks, right_value) = right.as_mut().unwrap();

            if left_ranks.is_overlapping(right_ranks) {
                if left_value == right_value {
                    let ranks = left_ranks.start.min(right_ranks.start)..right_ranks.end;
                    let (_, value) = replace(&mut right, right_iter.next()).unwrap();
                    left_ranks.start = ranks.end;
                    if left_ranks.is_empty() {
                        left = left_iter.next();
                    }
                    self.append(ranks, value);
                } else if left_ranks.start < right_ranks.start {
                    let ranks = left_ranks.start..right_ranks.start;
                    left_ranks.start = ranks.end;
                    // TODO: get rid of clone
                    self.append(ranks, left_value.clone());
                } else {
                    let (ranks, value) = replace(&mut right, right_iter.next()).unwrap();
                    left_ranks.start = ranks.end;
                    if left_ranks.is_empty() {
                        left = left_iter.next();
                    }
                    self.append(ranks, value);
                }
            } else if left_ranks.start < right_ranks.start {
                let (ranks, value) = replace(&mut left, left_iter.next()).unwrap();
                self.append(ranks, value);
            } else {
                let (ranks, value) = replace(&mut right, right_iter.next()).unwrap();
                self.append(ranks, value);
            }
        }

        while let Some((left_ranks, left_value)) = left {
            self.append(left_ranks, left_value);
            left = left_iter.next();
        }
        while let Some((right_ranks, right_value)) = right {
            self.append(right_ranks, right_value);
            right = right_iter.next();
        }
    }

    /// Merge the contents of this RankedValues into another RankedValues.
    pub fn merge_into(self, other: &mut Self) {
        other.merge_from(self);
    }

    fn append(&mut self, range: Range<usize>, value: T) {
        if let Some(last) = self.intervals.last_mut()
            && last.0.end == range.start
            && last.1 == value
        {
            last.0.end = range.end;
        } else {
            self.intervals.push((range, value));
        }
    }
}

impl RankedValues<Status> {
    pub fn first_terminating(&self) -> Option<(usize, Status)> {
        self.intervals
            .iter()
            .find(|(_, status)| status.is_terminating())
            .map(|(range, status)| (range.start, status.clone()))
    }

    pub fn first_failed(&self) -> Option<(usize, Status)> {
        self.intervals
            .iter()
            .find(|(_, status)| matches!(status, Status::Failed(_) | Status::Timeout(_)))
            .map(|(range, status)| (range.start, status.clone()))
    }
}

impl<T> From<(usize, T)> for RankedValues<T> {
    fn from((rank, value): (usize, T)) -> Self {
        Self {
            intervals: vec![(rank..rank + 1, value)],
        }
    }
}

impl<T> From<(Range<usize>, T)> for RankedValues<T> {
    fn from((range, value): (Range<usize>, T)) -> Self {
        Self {
            intervals: vec![(range, value)],
        }
    }
}

/// An inverted index of RankedValues, providing all ranks for
/// which each unique T-typed value appears.
#[derive(Clone, Debug)]
pub struct ValuesByRank<T> {
    values: HashMap<T, Vec<Range<usize>>>,
}

impl<T: Eq + Hash> PartialEq for ValuesByRank<T> {
    fn eq(&self, other: &Self) -> bool {
        self.values == other.values
    }
}

impl<T: Eq + Hash> Eq for ValuesByRank<T> {}

impl<T: fmt::Display> fmt::Display for ValuesByRank<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first_value = true;
        for (value, ranges) in self.iter() {
            if first_value {
                first_value = false;
            } else {
                write!(f, ";")?;
            }
            write!(f, "{}=", value)?;
            let mut first_range = true;
            for range in ranges.iter() {
                if first_range {
                    first_range = false;
                } else {
                    write!(f, ",")?;
                }
                write!(f, "{}..{}", range.start, range.end)?;
            }
        }
        Ok(())
    }
}

impl<T> Deref for ValuesByRank<T> {
    type Target = HashMap<T, Vec<Range<usize>>>;

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

impl<T> DerefMut for ValuesByRank<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.values
    }
}

/// Enabled for test only because we have to guarantee that the input
/// iterator is well-formed.
#[cfg(test)]
impl<T> FromIterator<(Range<usize>, T)> for RankedValues<T> {
    fn from_iter<I: IntoIterator<Item = (Range<usize>, T)>>(iter: I) -> Self {
        Self {
            intervals: iter.into_iter().collect(),
        }
    }
}

/// Spec for a host mesh agent to use when spawning a new proc.
#[derive(Clone, Debug, Serialize, Deserialize, Named, Default)]
pub(crate) struct ProcSpec {
    /// Config values to set on the spawned proc's global config,
    /// at the `ClientOverride` layer.
    pub(crate) client_config_override: Attrs,
}

impl ProcSpec {
    pub(crate) fn new(client_config_override: Attrs) -> Self {
        Self {
            client_config_override,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ranked_values_merge() {
        #[derive(PartialEq, Debug, Eq, Clone)]
        enum Side {
            Left,
            Right,
            Both,
        }
        use Side::Both;
        use Side::Left;
        use Side::Right;

        let mut left: RankedValues<Side> = [
            (0..10, Left),
            (15..20, Left),
            (30..50, Both),
            (60..70, Both),
        ]
        .into_iter()
        .collect();

        let right: RankedValues<Side> = [
            (9..12, Right),
            (25..30, Right),
            (30..40, Both),
            (40..50, Right),
            (50..60, Both),
        ]
        .into_iter()
        .collect();

        left.merge_from(right);
        assert_eq!(
            left.iter().cloned().collect::<Vec<_>>(),
            vec![
                (0..9, Left),
                (9..12, Right),
                (15..20, Left),
                (25..30, Right),
                (30..40, Both),
                (40..50, Right),
                // Merge consecutive:
                (50..70, Both)
            ]
        );

        assert_eq!(left.rank(5), 5);
        assert_eq!(left.rank(10), 10);
        assert_eq!(left.rank(16), 13);
        assert_eq!(left.rank(70), 62);
        assert_eq!(left.rank(100), 62);
    }

    #[test]
    fn test_equality() {
        assert_eq!(
            RankedValues::from((0..10, 123)),
            RankedValues::from((0..10, 123))
        );
        assert_eq!(
            RankedValues::from((0..10, Status::Failed("foo".to_string()))),
            RankedValues::from((0..10, Status::Failed("foo".to_string()))),
        );
    }

    #[test]
    fn test_default_through_merging() {
        let values: RankedValues<usize> =
            [(0..10, 1), (15..20, 1), (30..50, 1)].into_iter().collect();

        let mut default = RankedValues::from((0..50, 0));
        default.merge_from(values);

        assert_eq!(
            default.iter().cloned().collect::<Vec<_>>(),
            vec![
                (0..10, 1),
                (10..15, 0),
                (15..20, 1),
                (20..30, 0),
                (30..50, 1)
            ]
        );
    }
}
