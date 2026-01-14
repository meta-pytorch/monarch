/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Defines the accumulator trait and some common accumulators.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::OnceLock;
use std::time::Duration;

use algebra::JoinSemilattice;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use typeuri::Named;

// for macros
use crate::config;
use crate::reference::Index;

/// An accumulator is a object that accumulates updates into a state.
pub trait Accumulator {
    /// The type of the accumulated state.
    type State;
    /// The type of the updates sent to the accumulator. Updates will be
    /// accumulated into type [Self::State].
    type Update;

    /// Accumulate an update into the current state.
    fn accumulate(&self, state: &mut Self::State, update: Self::Update) -> anyhow::Result<()>;

    /// The specification used to build the reducer.
    fn reducer_spec(&self) -> Option<ReducerSpec>;
}

/// Serializable information needed to build a comm reducer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, typeuri::Named)]
pub struct ReducerSpec {
    /// The typehash of the underlying [Self::Reducer] type.
    pub typehash: u64,
    /// The parameters used to build the reducer.
    pub builder_params: Option<wirevalue::Any>,
}
wirevalue::register_type!(ReducerSpec);

/// Runtime behavior of reducers.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    typeuri::Named,
    Default
)]
pub struct ReducerOpts {
    /// The maximum interval between updates. When unspecified, a default
    /// interval is used.
    pub max_update_interval: Option<Duration>,
    /// The initial interval for the first update. When unspecified, defaults to 1ms.
    /// This allows quick flushing of single messages while using exponential backoff
    /// to reach max_update_interval for batched messages.
    pub initial_update_interval: Option<Duration>,
}

impl ReducerOpts {
    pub(crate) fn max_update_interval(&self) -> Duration {
        self.max_update_interval
            .unwrap_or(hyperactor_config::global::get(config::SPLIT_MAX_BUFFER_AGE))
    }

    pub(crate) fn initial_update_interval(&self) -> Duration {
        self.initial_update_interval
            .unwrap_or(Duration::from_millis(1))
    }
}

/// Commutative reducer for an accumulator. This is used to coallesce updates.
/// For example, if the accumulator is a sum, its reducer calculates and returns
/// the sum of 2 updates. This is helpful in split ports, where a large number
/// of updates can be reduced into a smaller number of updates before being sent
/// to the parent port.
pub trait CommReducer {
    /// The type of updates to be reduced.
    type Update;

    /// Reduce 2 updates into a single update.
    fn reduce(&self, left: Self::Update, right: Self::Update) -> anyhow::Result<Self::Update>;
}

/// Type erased version of [CommReducer].
pub trait ErasedCommReducer {
    /// Reduce 2 updates into a single update.
    fn reduce_erased(
        &self,
        left: &wirevalue::Any,
        right: &wirevalue::Any,
    ) -> anyhow::Result<wirevalue::Any>;

    /// Reducer an non-empty vector of updates. Return Error if the vector is
    /// empty.
    fn reduce_updates(
        &self,
        updates: Vec<wirevalue::Any>,
    ) -> Result<wirevalue::Any, (anyhow::Error, Vec<wirevalue::Any>)> {
        if updates.is_empty() {
            return Err((anyhow::anyhow!("empty updates"), updates));
        }
        if updates.len() == 1 {
            return Ok(updates.into_iter().next().expect("checked above"));
        }

        let mut iter = updates.iter();
        let first = iter.next().unwrap();
        let second = iter.next().unwrap();
        let init = match self.reduce_erased(first, second) {
            Ok(v) => v,
            Err(e) => return Err((e, updates)),
        };
        let reduced = match iter.try_fold(init, |acc, e| self.reduce_erased(&acc, e)) {
            Ok(v) => v,
            Err(e) => return Err((e, updates)),
        };
        Ok(reduced)
    }

    /// Typehash of the underlying [`CommReducer`] type.
    fn typehash(&self) -> u64;
}

impl<R, T> ErasedCommReducer for R
where
    R: CommReducer<Update = T> + Named,
    T: Serialize + DeserializeOwned + Named,
{
    fn reduce_erased(
        &self,
        left: &wirevalue::Any,
        right: &wirevalue::Any,
    ) -> anyhow::Result<wirevalue::Any> {
        let left = left.deserialized::<T>()?;
        let right = right.deserialized::<T>()?;
        let result = self.reduce(left, right)?;
        Ok(wirevalue::Any::serialize(&result)?)
    }

    fn typehash(&self) -> u64 {
        R::typehash()
    }
}

/// A factory for [`ErasedCommReducer`]s. This is used to register a
/// [`ErasedCommReducer`] type. We cannot register [`ErasedCommReducer`] trait
/// object directly because the object could have internal state, and cannot be
/// shared.
pub struct ReducerFactory {
    /// Return the typehash of the [`ErasedCommReducer`] type built by this
    /// factory.
    pub typehash_f: fn() -> u64,
    /// The builder function to build the [`ErasedCommReducer`] type.
    pub builder_f: fn(
        Option<wirevalue::Any>,
    ) -> anyhow::Result<Box<dyn ErasedCommReducer + Sync + Send + 'static>>,
}

inventory::collect!(ReducerFactory);

inventory::submit! {
    ReducerFactory {
        typehash_f: <SumReducer<i64> as Named>::typehash,
        builder_f: |_| Ok(Box::new(SumReducer::<i64>(PhantomData))),
    }
}
inventory::submit! {
    ReducerFactory {
        typehash_f: <SumReducer<u64> as Named>::typehash,
        builder_f: |_| Ok(Box::new(SumReducer::<u64>(PhantomData))),
    }
}
inventory::submit! {
    ReducerFactory {
        typehash_f: <MaxReducer::<i64> as Named>::typehash,
        builder_f: |_| Ok(Box::new(MaxReducer::<i64>(PhantomData))),
    }
}
inventory::submit! {
    ReducerFactory {
        typehash_f: <MaxReducer::<u64> as Named>::typehash,
        builder_f: |_| Ok(Box::new(MaxReducer::<u64>(PhantomData))),
    }
}
inventory::submit! {
    ReducerFactory {
        typehash_f: <MinReducer::<i64> as Named>::typehash,
        builder_f: |_| Ok(Box::new(MinReducer::<i64>(PhantomData))),
    }
}
inventory::submit! {
    ReducerFactory {
        typehash_f: <MinReducer::<u64> as Named>::typehash,
        builder_f: |_| Ok(Box::new(MinReducer::<u64>(PhantomData))),
    }
}
inventory::submit! {
    ReducerFactory {
        typehash_f: <WatermarkUpdateReducer::<i64> as Named>::typehash,
        builder_f: |_| Ok(Box::new(WatermarkUpdateReducer::<i64>(PhantomData))),
    }
}
inventory::submit! {
    ReducerFactory {
        typehash_f: <WatermarkUpdateReducer::<u64> as Named>::typehash,
        builder_f: |_| Ok(Box::new(WatermarkUpdateReducer::<u64>(PhantomData))),
    }
}
inventory::submit! {
    ReducerFactory {
        typehash_f: <GCounterReducer as Named>::typehash,
        builder_f: |_| Ok(Box::new(GCounterReducer)),
    }
}
inventory::submit! {
    ReducerFactory {
        typehash_f: <PNCounterReducer as Named>::typehash,
        builder_f: |_| Ok(Box::new(PNCounterReducer)),
    }
}

/// Build a reducer object with the given typehash's [CommReducer] type, and
/// return the type-erased version of it.
pub(crate) fn resolve_reducer(
    typehash: u64,
    builder_params: Option<wirevalue::Any>,
) -> anyhow::Result<Option<Box<dyn ErasedCommReducer + Sync + Send + 'static>>> {
    static FACTORY_MAP: OnceLock<HashMap<u64, &'static ReducerFactory>> = OnceLock::new();
    let factories = FACTORY_MAP.get_or_init(|| {
        let mut map = HashMap::new();
        for factory in inventory::iter::<ReducerFactory> {
            map.insert((factory.typehash_f)(), factory);
        }
        map
    });

    factories
        .get(&typehash)
        .map(|f| (f.builder_f)(builder_params))
        .transpose()
}

#[derive(typeuri::Named)]
struct SumReducer<T>(PhantomData<T>);

impl<T: std::ops::Add<Output = T> + Copy + 'static> CommReducer for SumReducer<T> {
    type Update = T;

    fn reduce(&self, left: T, right: T) -> anyhow::Result<T> {
        Ok(left + right)
    }
}

/// Accumulate the sum of received updates. The inner function performs the
/// summation between an update and the current state.
struct SumAccumulator<T>(PhantomData<T>);

impl<T: std::ops::Add<Output = T> + Copy + Named + 'static> Accumulator for SumAccumulator<T> {
    type State = T;
    type Update = T;

    fn accumulate(&self, state: &mut T, update: T) -> anyhow::Result<()> {
        *state = *state + update;
        Ok(())
    }

    fn reducer_spec(&self) -> Option<ReducerSpec> {
        Some(ReducerSpec {
            typehash: <SumReducer<T> as Named>::typehash(),
            builder_params: None,
        })
    }
}

/// Accumulate the sum of received updates.
///
/// # Note: Not a CRDT
///
/// This accumulator is *not idempotent* and is therefore *not
/// suitable* for distributed scatter/gather patterns with
/// at-least-once delivery semantics. Duplicate updates will be
/// counted multiple times:
///
/// ```text
/// sum(1, 2, 2, 3) = 8  (expected 6 if second 2 is duplicate)
/// ```
///
/// ## When to use:
/// - Single-source accumulation with exactly-once delivery
/// - Local (non-distributed) aggregation
/// - When upstream deduplication is guaranteed
///
/// ## CRDT Alternative:
/// For distributed use cases, consider using a GCounter CRDT instead,
/// which tracks per-replica increments and uses pointwise-max for
/// merging (commutative, associative, and idempotent).
///
/// *See also*: [`max`], [`min`] (proper lattice-based CRDTs)
pub fn sum<T: std::ops::Add<Output = T> + Copy + Named + 'static>()
-> impl Accumulator<State = T, Update = T> {
    SumAccumulator(PhantomData)
}

/// Re-export Max from algebra.
pub use algebra::Max;

#[derive(typeuri::Named)]
struct MaxReducer<T>(PhantomData<T>);

impl<T: Ord + Clone> CommReducer for MaxReducer<T> {
    type Update = Max<T>;

    fn reduce(&self, left: Max<T>, right: Max<T>) -> anyhow::Result<Max<T>> {
        Ok(left.join(&right))
    }
}

/// Accumulate the max of received updates.
struct MaxAccumulator<T>(PhantomData<T>);

impl<T: Ord + Clone + Named + 'static> Accumulator for MaxAccumulator<T> {
    type State = Max<T>;
    type Update = Max<T>;

    fn accumulate(&self, state: &mut Max<T>, update: Max<T>) -> anyhow::Result<()> {
        *state = state.join(&update);
        Ok(())
    }

    fn reducer_spec(&self) -> Option<ReducerSpec> {
        Some(ReducerSpec {
            typehash: <MaxReducer<T> as Named>::typehash(),
            builder_params: None,
        })
    }
}

/// Accumulate the max of received updates (i.e. the largest value of all
/// received updates).
pub fn max<T: Ord + Clone + Named + 'static>() -> impl Accumulator<State = Max<T>, Update = Max<T>>
{
    MaxAccumulator(PhantomData::<T>)
}

/// Re-export Min from algebra.
pub use algebra::Min;

#[derive(typeuri::Named)]
struct MinReducer<T>(PhantomData<T>);

impl<T: Ord + Clone> CommReducer for MinReducer<T> {
    type Update = Min<T>;

    fn reduce(&self, left: Min<T>, right: Min<T>) -> anyhow::Result<Min<T>> {
        Ok(left.join(&right))
    }
}

/// Accumulate the min of received updates.
struct MinAccumulator<T>(PhantomData<T>);

impl<T: Ord + Clone + Named + 'static> Accumulator for MinAccumulator<T> {
    type State = Min<T>;
    type Update = Min<T>;

    fn accumulate(&self, state: &mut Min<T>, update: Min<T>) -> anyhow::Result<()> {
        *state = state.join(&update);
        Ok(())
    }

    fn reducer_spec(&self) -> Option<ReducerSpec> {
        Some(ReducerSpec {
            typehash: <MinReducer<T> as Named>::typehash(),
            builder_params: None,
        })
    }
}

/// Accumulate the min of received updates (i.e. the smallest value of
/// all received updates).
pub fn min<T: Ord + Clone + Named + 'static>() -> impl Accumulator<State = Min<T>, Update = Min<T>>
{
    MinAccumulator(PhantomData)
}

/// Update from ranks for watermark accumulator using Last-Writer-Wins
/// CRDT.
///
/// This is a proper CRDT that tracks the latest value from each rank
/// using logical timestamps. When updates from the same rank are
/// merged, the one with the higher timestamp wins. This allows ranks
/// to report values that may decrease (e.g., during failure recovery)
/// while maintaining proper commutativity and idempotence.
///
/// # CRDT Properties
///
/// - *Commutative*: Merge order doesn't matter (timestamps resolve
///   conflicts)
/// - *Idempotent*: Merging duplicate updates has no effect
/// - *Convergent*: All replicas converge to the same state
///
/// # Watermark Semantics
///
/// The watermark is the minimum value across all ranks' *latest*
/// reports. "Latest" is determined by logical timestamp, not arrival
/// order.
#[derive(Default, Debug, Clone, Serialize, Deserialize, typeuri::Named)]
pub struct WatermarkUpdate<T>(algebra::LatticeMap<Index, algebra::LWW<T>>);

impl<T: Ord + Clone> WatermarkUpdate<T> {
    /// Get the watermark value (minimum of all ranks' current values).
    ///
    /// WatermarkUpdate is guaranteed to be initialized by the accumulator
    /// before it is sent to the user.
    pub fn get(&self) -> &T {
        self.0
            .iter()
            .map(|(_, lww)| &lww.value)
            .min()
            .expect("watermark should have been initialized")
    }

    /// Get the current value for a specific rank, if present.
    pub fn get_rank(&self, rank: Index) -> Option<&T> {
        self.0.get(&rank).map(|lww| &lww.value)
    }

    /// Get the number of ranks currently tracked.
    pub fn num_ranks(&self) -> usize {
        self.0.len()
    }
}

impl<T> From<(Index, T, u64)> for WatermarkUpdate<T> {
    /// Create a watermark update from (rank, value, timestamp).
    ///
    /// The timestamp should be a logical clock value (Lamport clock, sequence
    /// number, or monotonic counter) that increases with each update from
    /// the same rank.
    fn from((rank, value, timestamp): (Index, T, u64)) -> Self {
        let mut map = algebra::LatticeMap::new();
        // Use rank as replica ID - each rank is a unique writer
        map.insert(rank, algebra::LWW::new(value, timestamp, rank as u64));
        Self(map)
    }
}

/// Reducer for WatermarkUpdate using lattice join (LWW semantics).
///
/// Merges two watermark updates by joining their underlying LatticeMap.
/// For each rank, the LWW value with the higher timestamp wins.
#[derive(typeuri::Named)]
struct WatermarkUpdateReducer<T>(PhantomData<T>);

impl<T: Clone + PartialEq> CommReducer for WatermarkUpdateReducer<T> {
    type Update = WatermarkUpdate<T>;

    fn reduce(&self, left: Self::Update, right: Self::Update) -> anyhow::Result<Self::Update> {
        Ok(WatermarkUpdate(left.0.join(&right.0)))
    }
}

struct LowWatermarkUpdateAccumulator<T>(PhantomData<T>);

impl<T: Ord + Clone + PartialEq + Named + 'static> Accumulator
    for LowWatermarkUpdateAccumulator<T>
{
    type State = WatermarkUpdate<T>;
    type Update = WatermarkUpdate<T>;

    fn accumulate(&self, state: &mut Self::State, update: Self::Update) -> anyhow::Result<()> {
        *state = WatermarkUpdate(state.0.join(&update.0));
        Ok(())
    }

    fn reducer_spec(&self) -> Option<ReducerSpec> {
        Some(ReducerSpec {
            typehash: <WatermarkUpdateReducer<T> as Named>::typehash(),
            builder_params: None,
        })
    }
}

/// Accumulate the min value among the ranks, aka. low watermark, based on the
/// ranks' latest updates. Ranks' previous updates are discarded, and not used
/// in the min value calculation.
///
/// The main difference bwtween low wartermark accumulator and [`MinAccumulator`]
/// is, `MinAccumulator` takes previous updates into consideration too, and thus
/// returns the min of the whole history.
pub fn low_watermark<T: Ord + Clone + PartialEq + Named + 'static>()
-> impl Accumulator<State = WatermarkUpdate<T>, Update = WatermarkUpdate<T>> {
    LowWatermarkUpdateAccumulator(PhantomData)
}

/// State for a grow-only distributed counter (GCounter CRDT).
///
/// Each rank maintains its own count. The total value is the sum of
/// all ranks' counts. Merge takes pointwise max.
///
/// # CRDT Properties
///
/// - *Commutative*: Merge order doesn't matter
/// - *Associative*: Grouping doesn't matter
/// - *Idempotent*: Merging duplicate updates has no effect
/// - *Convergent*: All replicas converge to the same state
#[derive(Default, Debug, Clone, Serialize, Deserialize, typeuri::Named)]
pub struct GCounterUpdate(algebra::LatticeMap<Index, Max<u64>>);
wirevalue::register_type!(GCounterUpdate);

impl GCounterUpdate {
    /// Total counter value (sum of all ranks' counts).
    pub fn get(&self) -> u64 {
        self.0.iter().map(|(_, max)| max.0).sum()
    }

    /// Get count for a specific rank.
    pub fn get_rank(&self, rank: Index) -> Option<u64> {
        self.0.get(&rank).map(|max| max.0)
    }

    /// Number of ranks that have contributed.
    pub fn num_ranks(&self) -> usize {
        self.0.len()
    }
}

impl From<(Index, u64)> for GCounterUpdate {
    /// Create a GCounter update from (rank, count).
    fn from((rank, count): (Index, u64)) -> Self {
        let mut map = algebra::LatticeMap::new();
        map.insert(rank, Max(count));
        Self(map)
    }
}

/// Reducer for GCounterUpdate using lattice join (pointwise max).
#[derive(typeuri::Named)]
struct GCounterReducer;

impl CommReducer for GCounterReducer {
    type Update = GCounterUpdate;

    fn reduce(&self, left: Self::Update, right: Self::Update) -> anyhow::Result<Self::Update> {
        Ok(GCounterUpdate(left.0.join(&right.0)))
    }
}

struct GCounterAccumulator;

impl Accumulator for GCounterAccumulator {
    type State = GCounterUpdate;
    type Update = GCounterUpdate;

    fn accumulate(&self, state: &mut Self::State, update: Self::Update) -> anyhow::Result<()> {
        *state = GCounterUpdate(state.0.join(&update.0));
        Ok(())
    }

    fn reducer_spec(&self) -> Option<ReducerSpec> {
        Some(ReducerSpec {
            typehash: <GCounterReducer as Named>::typehash(),
            builder_params: None,
        })
    }
}

/// Accumulate a grow-only distributed counter (GCounter CRDT).
///
/// Each rank maintains its own count. The total value is the sum of
/// all ranks' counts. Merge uses pointwise max, making this
/// commutative, associative, and idempotent.
pub fn gcounter() -> impl Accumulator<State = GCounterUpdate, Update = GCounterUpdate> {
    GCounterAccumulator
}

/// State for an increment/decrement distributed counter (PNCounter
/// CRDT).
///
/// Internally uses two GCounters: one for increments (P), one for
/// decrements (N). The value is P - N. Each is merged independently
/// via pointwise max.
#[derive(Default, Debug, Clone, Serialize, Deserialize, typeuri::Named)]
pub struct PNCounterUpdate {
    p: algebra::LatticeMap<Index, Max<u64>>,
    n: algebra::LatticeMap<Index, Max<u64>>,
}
wirevalue::register_type!(PNCounterUpdate);

impl PNCounterUpdate {
    /// Counter value (sum of increments minus sum of decrements).
    pub fn get(&self) -> i64 {
        let p: u64 = self.p.iter().map(|(_, m)| m.0).sum();
        let n: u64 = self.n.iter().map(|(_, m)| m.0).sum();
        p as i64 - n as i64
    }

    /// Create an increment update for a rank.
    pub fn inc(rank: Index, delta: u64) -> Self {
        let mut p = algebra::LatticeMap::new();
        p.insert(rank, Max(delta));
        Self {
            p,
            n: algebra::LatticeMap::new(),
        }
    }

    /// Create a decrement update for a rank.
    pub fn dec(rank: Index, delta: u64) -> Self {
        let mut n = algebra::LatticeMap::new();
        n.insert(rank, Max(delta));
        Self {
            p: algebra::LatticeMap::new(),
            n,
        }
    }

    /// Number of ranks that have contributed increments.
    pub fn num_inc_ranks(&self) -> usize {
        self.p.len()
    }

    /// Number of ranks that have contributed decrements.
    pub fn num_dec_ranks(&self) -> usize {
        self.n.len()
    }
}

/// Reducer for PNCounterUpdate using lattice join on both components.
#[derive(typeuri::Named)]
struct PNCounterReducer;

impl CommReducer for PNCounterReducer {
    type Update = PNCounterUpdate;

    fn reduce(&self, left: Self::Update, right: Self::Update) -> anyhow::Result<Self::Update> {
        Ok(PNCounterUpdate {
            p: left.p.join(&right.p),
            n: left.n.join(&right.n),
        })
    }
}

struct PNCounterAccumulator;

impl Accumulator for PNCounterAccumulator {
    type State = PNCounterUpdate;
    type Update = PNCounterUpdate;

    fn accumulate(&self, state: &mut Self::State, update: Self::Update) -> anyhow::Result<()> {
        *state = PNCounterUpdate {
            p: state.p.join(&update.p),
            n: state.n.join(&update.n),
        };
        Ok(())
    }

    fn reducer_spec(&self) -> Option<ReducerSpec> {
        Some(ReducerSpec {
            typehash: <PNCounterReducer as Named>::typehash(),
            builder_params: None,
        })
    }
}

/// Accumulate an increment/decrement distributed counter (PNCounter CRDT).
///
/// Each rank can increment or decrement. The value is the sum of all
/// increments minus the sum of all decrements. Merge uses pointwise max
/// on both components independently.
pub fn pncounter() -> impl Accumulator<State = PNCounterUpdate, Update = PNCounterUpdate> {
    PNCounterAccumulator
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use maplit::hashmap;
    use typeuri::Named;

    use super::*;

    fn serialize<T: Serialize + Named>(values: Vec<T>) -> Vec<wirevalue::Any> {
        values
            .into_iter()
            .map(|n| wirevalue::Any::serialize(&n).unwrap())
            .collect()
    }

    #[test]
    fn test_comm_reducer_numeric() {
        let u64_numbers_sum: Vec<_> = serialize(vec![1u64, 3u64, 1100u64]);
        let i64_numbers_sum: Vec<_> = serialize(vec![-123i64, 33i64, 110i64]);
        let u64_numbers_max: Vec<_> = serialize(vec![Max(1u64), Max(3u64), Max(1100u64)]);
        let i64_numbers_max: Vec<_> = serialize(vec![Max(-123i64), Max(33i64), Max(110i64)]);
        let u64_numbers_min: Vec<_> = serialize(vec![Min(1u64), Min(3u64), Min(1100u64)]);
        let i64_numbers_min: Vec<_> = serialize(vec![Min(-123i64), Min(33i64), Min(110i64)]);
        {
            let typehash = <MaxReducer<u64> as Named>::typehash();
            assert_eq!(
                resolve_reducer(typehash, None)
                    .unwrap()
                    .unwrap()
                    .reduce_updates(u64_numbers_max.clone())
                    .unwrap()
                    .deserialized::<Max<u64>>()
                    .unwrap(),
                Max(1100u64),
            );

            let typehash = <MinReducer<u64> as Named>::typehash();
            assert_eq!(
                resolve_reducer(typehash, None)
                    .unwrap()
                    .unwrap()
                    .reduce_updates(u64_numbers_min.clone())
                    .unwrap()
                    .deserialized::<Min<u64>>()
                    .unwrap(),
                Min(1u64),
            );

            let typehash = <SumReducer<u64> as Named>::typehash();
            assert_eq!(
                resolve_reducer(typehash, None)
                    .unwrap()
                    .unwrap()
                    .reduce_updates(u64_numbers_sum)
                    .unwrap()
                    .deserialized::<u64>()
                    .unwrap(),
                1104u64,
            );
        }

        {
            let typehash = <MaxReducer<i64> as Named>::typehash();
            assert_eq!(
                resolve_reducer(typehash, None)
                    .unwrap()
                    .unwrap()
                    .reduce_updates(i64_numbers_max.clone())
                    .unwrap()
                    .deserialized::<Max<i64>>()
                    .unwrap(),
                Max(110i64),
            );

            let typehash = <MinReducer<i64> as Named>::typehash();
            assert_eq!(
                resolve_reducer(typehash, None)
                    .unwrap()
                    .unwrap()
                    .reduce_updates(i64_numbers_min.clone())
                    .unwrap()
                    .deserialized::<Min<i64>>()
                    .unwrap(),
                Min(-123i64),
            );

            let typehash = <SumReducer<i64> as Named>::typehash();
            assert_eq!(
                resolve_reducer(typehash, None)
                    .unwrap()
                    .unwrap()
                    .reduce_updates(i64_numbers_sum)
                    .unwrap()
                    .deserialized::<i64>()
                    .unwrap(),
                20i64,
            );
        }
    }

    #[test]
    fn test_comm_reducer_watermark() {
        // With LWW, we need timestamps. Assign in order of appearance.
        let u64_updates = serialize::<WatermarkUpdate<u64>>(
            vec![
                (1, 1, 0),   // rank 1: value 1, ts 0
                (0, 2, 1),   // rank 0: value 2, ts 1
                (0, 1, 2),   // rank 0: value 1, ts 2 (later ts, wins over value 2)
                (3, 35, 3),  // rank 3: value 35, ts 3
                (0, 9, 4),   // rank 0: value 9, ts 4 (latest for rank 0)
                (1, 10, 5),  // rank 1: value 10, ts 5 (latest for rank 1)
                (3, 32, 6),  // rank 3: value 32, ts 6
                (3, 0, 7),   // rank 3: value 0, ts 7
                (3, 321, 8), // rank 3: value 321, ts 8 (latest for rank 3)
            ]
            .into_iter()
            .map(|(k, v, ts)| WatermarkUpdate::from((k, v, ts)))
            .collect(),
        );
        let i64_updates: Vec<_> = serialize::<WatermarkUpdate<i64>>(
            vec![
                (0, 2, 0),   // rank 0: value 2, ts 0
                (1, 1, 1),   // rank 1: value 1, ts 1
                (3, 35, 2),  // rank 3: value 35, ts 2
                (0, 1, 3),   // rank 0: value 1, ts 3
                (1, -10, 4), // rank 1: value -10, ts 4
                (3, 32, 5),  // rank 3: value 32, ts 5
                (3, 0, 6),   // rank 3: value 0, ts 6
                (3, -99, 7), // rank 3: value -99, ts 7 (latest for rank 3)
                (0, -9, 8),  // rank 0: value -9, ts 8 (latest for rank 0)
            ]
            .into_iter()
            .map(WatermarkUpdate::from)
            .collect(),
        );

        fn verify<T: Ord + Clone + DeserializeOwned + Debug + Named>(
            updates: Vec<wirevalue::Any>,
            expected: HashMap<Index, T>,
        ) {
            let typehash = <WatermarkUpdateReducer<T> as Named>::typehash();
            let result = resolve_reducer(typehash, None)
                .unwrap()
                .unwrap()
                .reduce_updates(updates)
                .unwrap()
                .deserialized::<WatermarkUpdate<T>>()
                .unwrap();

            // Check each expected rank value
            for (rank, expected_value) in &expected {
                assert_eq!(
                    result.get_rank(*rank).unwrap(),
                    expected_value,
                    "Mismatch for rank {rank}"
                );
            }
            // Also verify no extra ranks
            assert_eq!(result.num_ranks(), expected.len());
        }

        verify::<i64>(
            i64_updates,
            hashmap! {
                0 => -9,   // latest ts for rank 0
                1 => -10,  // latest ts for rank 1
                3 => -99,  // latest ts for rank 3
            },
        );

        verify::<u64>(
            u64_updates,
            hashmap! {
                0 => 9,    // latest ts for rank 0
                1 => 10,   // latest ts for rank 1
                3 => 321,  // latest ts for rank 3
            },
        );
    }

    #[test]
    fn test_accum_reducer_numeric() {
        assert_eq!(
            sum::<u64>().reducer_spec().unwrap().typehash,
            <SumReducer::<u64> as Named>::typehash(),
        );
        assert_eq!(
            sum::<i64>().reducer_spec().unwrap().typehash,
            <SumReducer::<i64> as Named>::typehash(),
        );

        assert_eq!(
            min::<u64>().reducer_spec().unwrap().typehash,
            <MinReducer::<u64> as Named>::typehash(),
        );
        assert_eq!(
            min::<i64>().reducer_spec().unwrap().typehash,
            <MinReducer::<i64> as Named>::typehash(),
        );

        assert_eq!(
            max::<u64>().reducer_spec().unwrap().typehash,
            <MaxReducer::<u64> as Named>::typehash(),
        );
        assert_eq!(
            max::<i64>().reducer_spec().unwrap().typehash,
            <MaxReducer::<i64> as Named>::typehash(),
        );
    }

    #[test]
    fn test_accum_reducer_watermark() {
        fn verify<T: Ord + Clone + Named>() {
            assert_eq!(
                low_watermark::<T>().reducer_spec().unwrap().typehash,
                <WatermarkUpdateReducer::<T> as Named>::typehash(),
            );
        }
        verify::<u64>();
        verify::<i64>();
    }

    #[test]
    fn test_watermark_accumulator() {
        let accumulator = low_watermark::<u64>();
        let ranks_values_expectations = [
            // send in descending order (with timestamps 0, 1, 2)
            (0, 1003, 0, 1003),
            (1, 1002, 1, 1002),
            (2, 1001, 2, 1001),
            // send in ascending order (timestamps 3, 4, 5)
            (0, 100, 3, 100),
            (1, 101, 4, 100),
            (2, 102, 5, 100),
            // send same values (timestamps 6, 7, 8)
            (0, 100, 6, 100),
            (1, 101, 7, 100),
            (2, 102, 8, 100),
            // shuffle rank 0 to be largest, and make rank 1 smallest (timestamps 9, 10, 11)
            (0, 1000, 9, 101),
            // shuffle rank 1 to be largest, and make rank 2 smallest
            (1, 1100, 10, 102),
            // shuffle rank 2 to be largest, and make rank 0 smallest
            (2, 1200, 11, 1000),
            // Increase their value, but do not change their order (timestamps 12, 13, 14)
            (0, 1001, 12, 1001),
            (1, 1101, 13, 1001),
            (2, 1201, 14, 1001),
            // decrease their values (timestamps 15, 16, 17)
            (2, 102, 15, 102),
            (1, 101, 16, 101),
            (0, 100, 17, 100),
        ];
        let mut state = WatermarkUpdate::default();
        for (rank, value, ts, expected) in ranks_values_expectations {
            accumulator
                .accumulate(&mut state, WatermarkUpdate::from((rank, value, ts)))
                .unwrap();
            assert_eq!(
                state.get(),
                &expected,
                "rank is {rank}; value is {value}; ts is {ts}"
            );
        }
    }

    #[test]
    fn test_comm_reducer_gcounter() {
        // Updates from different ranks
        let updates = serialize::<GCounterUpdate>(vec![
            GCounterUpdate::from((0, 10)),
            GCounterUpdate::from((1, 20)),
            GCounterUpdate::from((0, 15)), // rank 0 increases to 15
            GCounterUpdate::from((2, 5)),
            GCounterUpdate::from((1, 25)), // rank 1 increases to 25
        ]);

        let typehash = <GCounterReducer as Named>::typehash();
        let result = resolve_reducer(typehash, None)
            .unwrap()
            .unwrap()
            .reduce_updates(updates)
            .unwrap()
            .deserialized::<GCounterUpdate>()
            .unwrap();

        // Each rank should have its max value
        assert_eq!(result.get_rank(0), Some(15));
        assert_eq!(result.get_rank(1), Some(25));
        assert_eq!(result.get_rank(2), Some(5));
        assert_eq!(result.num_ranks(), 3);
        // Total is sum of max values: 15 + 25 + 5 = 45
        assert_eq!(result.get(), 45);
    }

    #[test]
    fn test_accum_reducer_gcounter() {
        assert_eq!(
            gcounter().reducer_spec().unwrap().typehash,
            <GCounterReducer as Named>::typehash(),
        );
    }

    #[test]
    fn test_gcounter_accumulator() {
        let accumulator = gcounter();
        // (rank, count, expected_total)
        let ranks_counts_expectations: [(Index, u64, u64); 17] = [
            // initialize all 3 ranks in descending order
            (0, 1000, 1000),
            (1, 100, 1100),
            (2, 10, 1110),
            // increase in ascending order
            (2, 20, 1120),
            (1, 200, 1220),
            (0, 2000, 2220),
            // same values (idempotent - no change)
            (0, 2000, 2220),
            (1, 200, 2220),
            (2, 20, 2220),
            // lower values (ignored - max wins)
            (0, 1, 2220),
            (1, 1, 2220),
            (2, 1, 2220),
            // shuffle which rank has max: make rank 2 largest
            (2, 5000, 7200), // 2000 + 200 + 5000
            // make rank 1 largest
            (1, 6000, 13000), // 2000 + 6000 + 5000
            // make rank 0 largest again
            (0, 10000, 21000), // 10000 + 6000 + 5000
            // all ranks increase together
            (0, 10001, 21001),
            (1, 6001, 21002),
        ];
        let mut state = GCounterUpdate::default();
        for (rank, count, expected) in ranks_counts_expectations {
            accumulator
                .accumulate(&mut state, GCounterUpdate::from((rank, count)))
                .unwrap();
            assert_eq!(state.get(), expected, "rank is {rank}; count is {count}");
        }
        // Verify final per-rank values
        assert_eq!(state.get_rank(0), Some(10001));
        assert_eq!(state.get_rank(1), Some(6001));
        assert_eq!(state.get_rank(2), Some(5000));
        assert_eq!(state.get_rank(3), None);
        assert_eq!(state.num_ranks(), 3);
    }

    #[test]
    fn test_gcounter_commutativity() {
        // Verify that order of accumulation doesn't matter
        let updates = [
            GCounterUpdate::from((0, 10)),
            GCounterUpdate::from((1, 20)),
            GCounterUpdate::from((0, 15)),
            GCounterUpdate::from((2, 5)),
            GCounterUpdate::from((1, 25)),
        ];

        // Forward order
        let accumulator = gcounter();
        let mut forward = GCounterUpdate::default();
        for update in updates.iter().cloned() {
            accumulator.accumulate(&mut forward, update).unwrap();
        }

        // Reverse order
        let mut reverse = GCounterUpdate::default();
        for update in updates.iter().rev().cloned() {
            accumulator.accumulate(&mut reverse, update).unwrap();
        }

        assert_eq!(forward.get(), reverse.get());
        assert_eq!(forward.get(), 45); // 15 + 25 + 5
        assert_eq!(forward.get_rank(0), reverse.get_rank(0));
        assert_eq!(forward.get_rank(1), reverse.get_rank(1));
        assert_eq!(forward.get_rank(2), reverse.get_rank(2));
    }

    #[test]
    fn test_comm_reducer_pncounter() {
        // Updates from different ranks with increments and decrements
        let updates = serialize::<PNCounterUpdate>(vec![
            PNCounterUpdate::inc(0, 10),
            PNCounterUpdate::inc(1, 20),
            PNCounterUpdate::dec(0, 5),
            PNCounterUpdate::inc(0, 15), // rank 0 inc increases to 15
            PNCounterUpdate::dec(1, 8),
            PNCounterUpdate::dec(0, 7), // rank 0 dec increases to 7
        ]);

        let typehash = <PNCounterReducer as Named>::typehash();
        let result = resolve_reducer(typehash, None)
            .unwrap()
            .unwrap()
            .reduce_updates(updates)
            .unwrap()
            .deserialized::<PNCounterUpdate>()
            .unwrap();

        // Each rank should have its max values for both inc and dec
        // rank 0: inc=15, dec=7 -> contribution = 15-7 = 8
        // rank 1: inc=20, dec=8 -> contribution = 20-8 = 12
        // Total: 8 + 12 = 20
        assert_eq!(result.get(), 20);
        assert_eq!(result.num_inc_ranks(), 2);
        assert_eq!(result.num_dec_ranks(), 2);
    }

    #[test]
    fn test_accum_reducer_pncounter() {
        assert_eq!(
            pncounter().reducer_spec().unwrap().typehash,
            <PNCounterReducer as Named>::typehash(),
        );
    }

    #[test]
    fn test_pncounter_accumulator() {
        let accumulator = pncounter();
        // Helper to make updates clearer
        #[derive(Clone, Copy, Debug)]
        enum Op {
            Inc(Index, u64),
            Dec(Index, u64),
        }
        use Op::*;

        // (operation, expected_total)
        // State tracked: p0, p1, p2 (increments), n0, n1, n2 (decrements)
        // Total = (p0 + p1 + p2) - (n0 + n1 + n2)
        let ops_expectations = [
            // initialize all 3 ranks with increments
            (Inc(0, 100), 100), // p: 100,0,0 n: 0,0,0 = 100
            (Inc(1, 50), 150),  // p: 100,50,0 n: 0,0,0 = 150
            (Inc(2, 25), 175),  // p: 100,50,25 n: 0,0,0 = 175
            // add decrements
            (Dec(0, 10), 165), // p: 100,50,25 n: 10,0,0 = 175-10 = 165
            (Dec(1, 5), 160),  // p: 100,50,25 n: 10,5,0 = 175-15 = 160
            (Dec(2, 2), 158),  // p: 100,50,25 n: 10,5,2 = 175-17 = 158
            // increase increments
            (Inc(0, 200), 258), // p: 200,50,25 n: 10,5,2 = 275-17 = 258
            (Inc(1, 100), 308), // p: 200,100,25 n: 10,5,2 = 325-17 = 308
            (Inc(2, 50), 333),  // p: 200,100,50 n: 10,5,2 = 350-17 = 333
            // increase decrements
            (Dec(0, 20), 323), // p: 200,100,50 n: 20,5,2 = 350-27 = 323
            (Dec(1, 15), 313), // p: 200,100,50 n: 20,15,2 = 350-37 = 313
            (Dec(2, 5), 310),  // p: 200,100,50 n: 20,15,5 = 350-40 = 310
            // duplicate updates (idempotent - no change)
            (Inc(0, 200), 310),
            (Dec(1, 15), 310),
            // lower values (ignored - max wins)
            (Inc(0, 1), 310),
            (Dec(0, 1), 310),
            // make decrements larger than increments for some ranks
            (Dec(2, 60), 255),  // p: 200,100,50 n: 20,15,60 = 350-95 = 255
            (Dec(1, 120), 150), // p: 200,100,50 n: 20,120,60 = 350-200 = 150
            // rank 1 now contributes negatively: 100 - 120 = -20
            (Inc(2, 60), 160), // p: 200,100,60 n: 20,120,60 = 360-200 = 160
            // shuffle: make rank 0 contribute most
            (Inc(0, 1000), 960), // p: 1000,100,60 n: 20,120,60 = 1160-200 = 960
            (Dec(2, 100), 920),  // p: 1000,100,60 n: 20,120,100 = 1160-240 = 920
        ];

        let mut state = PNCounterUpdate::default();
        for (i, (op, expected)) in ops_expectations.iter().enumerate() {
            let update = match op {
                Inc(rank, delta) => PNCounterUpdate::inc(*rank, *delta),
                Dec(rank, delta) => PNCounterUpdate::dec(*rank, *delta),
            };
            accumulator.accumulate(&mut state, update).unwrap();
            assert_eq!(state.get(), *expected, "step {i}: {op:?}");
        }

        // Verify final state
        assert_eq!(state.num_inc_ranks(), 3);
        assert_eq!(state.num_dec_ranks(), 3);
    }

    #[test]
    fn test_pncounter_commutativity() {
        // Verify that order of accumulation doesn't matter
        let updates = [
            PNCounterUpdate::inc(0, 10),
            PNCounterUpdate::inc(1, 20),
            PNCounterUpdate::dec(0, 5),
            PNCounterUpdate::inc(0, 15),
            PNCounterUpdate::dec(1, 8),
            PNCounterUpdate::dec(2, 3),
            PNCounterUpdate::inc(2, 12),
        ];

        // Forward order
        let accumulator = pncounter();
        let mut forward = PNCounterUpdate::default();
        for update in updates.iter().cloned() {
            accumulator.accumulate(&mut forward, update).unwrap();
        }

        // Reverse order
        let mut reverse = PNCounterUpdate::default();
        for update in updates.iter().rev().cloned() {
            accumulator.accumulate(&mut reverse, update).unwrap();
        }

        assert_eq!(forward.get(), reverse.get());
        assert_eq!(forward.get(), 31); // (15 + 20 + 12) - (5 + 8 + 3) = 47 - 16 = 31
        assert_eq!(forward.num_inc_ranks(), reverse.num_inc_ranks());
        assert_eq!(forward.num_dec_ranks(), reverse.num_dec_ranks());
    }
}
