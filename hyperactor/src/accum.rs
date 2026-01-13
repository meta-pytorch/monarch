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

use ndslice::algebra::JoinSemilattice;
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

/// The mode in which a reducer operates.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, typeuri::Named)]
pub enum ReducerMode {
    /// Streaming mode: continuously reduce and emit updates based on buffer size/timeout.
    Streaming {
        /// The maximum interval between updates. When unspecified, a default
        /// interval is used.
        max_update_interval: Option<Duration>,
        /// The initial interval for the first update. When unspecified, defaults to 1ms.
        /// This allows quick flushing of single messages while using exponential backoff
        /// to reach max_update_interval for batched messages.
        initial_update_interval: Option<Duration>,
    },
    /// Once mode: accumulate exactly `n` values, emit a single reduced update, then tear down.
    Once(usize),
}

impl Default for ReducerMode {
    fn default() -> Self {
        ReducerMode::Streaming {
            max_update_interval: None,
            initial_update_interval: None,
        }
    }
}

impl ReducerMode {
    pub(crate) fn max_update_interval(&self) -> Duration {
        match self {
            ReducerMode::Streaming {
                max_update_interval,
                ..
            } => max_update_interval
                .unwrap_or(hyperactor_config::global::get(config::SPLIT_MAX_BUFFER_AGE)),
            ReducerMode::Once(_) => Duration::MAX,
        }
    }

    pub(crate) fn initial_update_interval(&self) -> Duration {
        match self {
            ReducerMode::Streaming {
                initial_update_interval,
                ..
            } => initial_update_interval.unwrap_or(Duration::from_millis(1)),
            ReducerMode::Once(_) => Duration::MAX,
        }
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

/// Re-export Max from ndslice algebra module.
pub use ndslice::algebra::Max;

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

/// Re-export Min from ndslice algebra module.
pub use ndslice::algebra::Min;

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

/// Accumulate the min of received updates (i.e. the smallest value of all
/// received updates).
pub fn min<T: Ord + Clone + Named + 'static>() -> impl Accumulator<State = Min<T>, Update = Min<T>>
{
    MinAccumulator(PhantomData)
}

/// Update from ranks for watermark accumulator using Last-Writer-Wins CRDT.
///
/// This is a proper CRDT that tracks the latest value from each rank using
/// logical timestamps. When updates from the same rank are merged, the one
/// with the higher timestamp wins. This allows ranks to report values that
/// may decrease (e.g., during failure recovery) while maintaining proper
/// commutativity and idempotence.
///
/// # CRDT Properties
///
/// - *Commutative*: Merge order doesn't matter (timestamps resolve conflicts)
/// - *Idempotent*: Merging duplicate updates has no effect
/// - *Convergent*: All replicas converge to the same state
///
/// # Watermark Semantics
///
/// The watermark is the minimum value across all ranks' *latest* reports.
/// "Latest" is determined by logical timestamp, not arrival order.
#[derive(Default, Debug, Clone, Serialize, Deserialize, typeuri::Named)]
pub struct WatermarkUpdate<T>(ndslice::algebra::LatticeMap<Index, ndslice::algebra::LWW<T>>);

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
        let mut map = ndslice::algebra::LatticeMap::new();
        // Use rank as replica ID - each rank is a unique writer
        map.insert(
            rank,
            ndslice::algebra::LWW::new(value, timestamp, rank as u64),
        );
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
        // Use lattice join - fully commutative and idempotent!
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
        // Use lattice join - no need for replace, just join in place
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
}
