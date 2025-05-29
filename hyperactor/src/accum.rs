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

use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::Named;
use crate::data::Serialized;
use crate::intern_typename;
use crate::reference::Index;

/// An accumulator is a object that accumulates updates into a state.
pub trait Accumulator {
    /// The type of the accumulated state.
    type State;
    /// The type of the updates sent to the accumulator. Updates will be
    /// accumulated into type [Self::State].
    type Update;
    /// The type of the comm reducer used by this accumulator.
    type Reducer: CommReducer<Update = Self::Update> + Named;

    /// Accumulate an update into the current state.
    fn accumulate(&self, state: &mut Self::State, update: Self::Update);

    /// The typehash of the underlying [Self::Reducer] type.
    fn reducer_typehash(&self) -> u64 {
        <Self::Reducer as Named>::typehash()
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
    fn reduce(&self, left: Self::Update, right: Self::Update) -> Self::Update;
}

/// Type erased version of [CommReducer].
pub(crate) trait ErasedCommReducer {
    /// Reduce 2 updates into a single update.
    fn reduce_erased(&self, left: &Serialized, right: &Serialized) -> anyhow::Result<Serialized>;

    /// Reducer an non-empty vector of updates. Return Error if the vector is
    /// empty.
    fn reduce_updates(
        &self,
        updates: Vec<Serialized>,
    ) -> Result<Serialized, (anyhow::Error, Vec<Serialized>)> {
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
    fn reduce_erased(&self, left: &Serialized, right: &Serialized) -> anyhow::Result<Serialized> {
        let left = left.deserialized::<T>()?;
        let right = right.deserialized::<T>()?;
        let result = self.reduce(left, right);
        Ok(Serialized::serialize(&result)?)
    }

    fn typehash(&self) -> u64 {
        R::typehash()
    }
}

// Register factory instead of ErasedCommReducer trait object because the
// object could have internal state, and cannot be shared.
struct ReducerFactory(fn() -> Box<dyn ErasedCommReducer + Sync + Send + 'static>);

inventory::collect!(ReducerFactory);

inventory::submit! {
    ReducerFactory(|| Box::new(SumReducer::<i64>(PhantomData)))
}
inventory::submit! {
    ReducerFactory(|| Box::new(SumReducer::<u64>(PhantomData)))
}
inventory::submit! {
    ReducerFactory(|| Box::new(MaxReducer::<i64>(PhantomData)))
}
inventory::submit! {
    ReducerFactory(|| Box::new(MaxReducer::<u64>(PhantomData)))
}
inventory::submit! {
    ReducerFactory(|| Box::new(MinReducer::<i64>(PhantomData)))
}
inventory::submit! {
    ReducerFactory(|| Box::new(MinReducer::<u64>(PhantomData)))
}
inventory::submit! {
    ReducerFactory(|| Box::new(WatermarkUpdateReducer::<i64>(PhantomData)))
}
inventory::submit! {
    ReducerFactory(|| Box::new(WatermarkUpdateReducer::<u64>(PhantomData)))
}

/// Build a reducer object with the given typehash's [CommReducer] type, and
/// return the type-erased version of it.
pub(crate) fn resolve_reducer(
    typehash: u64,
) -> Option<Box<dyn ErasedCommReducer + Sync + Send + 'static>> {
    static FACTORY_MAP: OnceLock<HashMap<u64, &'static ReducerFactory>> = OnceLock::new();
    let factories = FACTORY_MAP.get_or_init(|| {
        let mut map = HashMap::new();
        for factory in inventory::iter::<ReducerFactory> {
            map.insert(factory.0().typehash(), factory);
        }
        map
    });

    factories.get(&typehash).map(|f| f.0())
}

struct SumReducer<T>(PhantomData<T>);

impl<T: std::ops::Add<Output = T> + Copy + 'static> CommReducer for SumReducer<T> {
    type Update = T;

    fn reduce(&self, left: T, right: T) -> T {
        left + right
    }
}

impl<T: Named> Named for SumReducer<T> {
    fn typename() -> &'static str {
        intern_typename!(Self, "hyperactor::accum::SumReducer<{}>", T)
    }
}

/// Accumulate the sum of received updates. The inner function performs the
/// summation between an update and the current state.
struct SumAccumulator<T>(PhantomData<T>);

impl<T: std::ops::Add<Output = T> + Copy + Named + 'static> Accumulator for SumAccumulator<T> {
    type State = T;
    type Update = T;
    type Reducer = SumReducer<T>;

    fn accumulate(&self, state: &mut T, update: T) {
        *state = *state + update;
    }
}

/// Accumulate the sum of received updates.
pub fn sum<T: std::ops::Add<Output = T> + Copy + Named + 'static>()
-> impl Accumulator<State = T, Update = T> {
    SumAccumulator(PhantomData)
}

struct MaxReducer<T>(PhantomData<T>);

impl<T: Ord> CommReducer for MaxReducer<T> {
    type Update = T;

    fn reduce(&self, left: T, right: T) -> T {
        std::cmp::max(left, right)
    }
}

impl<T: Named> Named for MaxReducer<T> {
    fn typename() -> &'static str {
        intern_typename!(Self, "hyperactor::accum::MaxReducer<{}>", T)
    }
}

/// Accumulate the max of received updates.
struct MaxAccumulator<T>(PhantomData<T>);

impl<T: Ord + Copy + Named + 'static> Accumulator for MaxAccumulator<T> {
    type State = T;
    type Update = T;
    type Reducer = MaxReducer<T>;

    fn accumulate(&self, state: &mut T, update: T) {
        *state = std::cmp::max(*state, update);
    }
}

/// Accumulate the max of received updates (i.e. the largest value of all
/// received updates).
pub fn max<T: Ord + Copy + Named + 'static>() -> impl Accumulator<State = T, Update = T> {
    MaxAccumulator(PhantomData::<T>)
}

struct MinReducer<T>(PhantomData<T>);

impl<T: Ord> CommReducer for MinReducer<T> {
    type Update = T;

    fn reduce(&self, left: T, right: T) -> T {
        std::cmp::min(left, right)
    }
}

impl<T: Named> Named for MinReducer<T> {
    fn typename() -> &'static str {
        intern_typename!(Self, "hyperactor::accum::MinReducer<{}>", T)
    }
}

/// Accumulate the min of received updates.
struct MinAccumulator<T>(PhantomData<T>);

impl<T: Ord + Copy + Named + 'static> Accumulator for MinAccumulator<T> {
    type State = T;
    type Update = T;
    type Reducer = MinReducer<T>;

    fn accumulate(&self, state: &mut T, update: T) {
        *state = std::cmp::min(*state, update);
    }
}

/// Accumulate the min of received updates (i.e. the smallest value of all
/// received updates).
pub fn min<T: Ord + Copy + Named + 'static>() -> impl Accumulator<State = T, Update = T> {
    MinAccumulator(PhantomData)
}

/// Update from ranks for watermark accumulator, where map' key is the rank, and
/// map's value is the update from that rank.
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct WatermarkUpdate<T>(HashMap<Index, T>);

impl<T: Named> Named for WatermarkUpdate<T> {
    fn typename() -> &'static str {
        intern_typename!(Self, "hyperactor::accum::WatermarkUpdate<{}>", T)
    }
}

impl<T: Ord> WatermarkUpdate<T> {
    /// Get the watermark value. WatermarkUpdate is guarranteed to be initialized by
    /// accumulator before it is sent to the user.
    // TODO(pzhang) optimize this and only iterate when there is a new min.
    pub fn get(&self) -> &T {
        self.0
            .values()
            .min()
            .expect("watermark should have been intialized.")
    }
}

impl<T: PartialEq> WatermarkUpdate<T> {
    /// See [`WatermarkUpdateReducer`]'s documentation for the merge semantics.
    fn merge(old: Self, new: Self) -> Self {
        let mut map = old.0;
        for (k, v) in new.0 {
            map.insert(k, v);
        }
        Self(map)
    }
}

impl<T> From<(Index, T)> for WatermarkUpdate<T> {
    fn from((rank, value): (Index, T)) -> Self {
        let mut map = HashMap::with_capacity(1);
        map.insert(rank, value);
        Self(map)
    }
}

/// Merge an old update and a new update. If a rank exists in boths updates,
/// only keep its value from the new update.
struct WatermarkUpdateReducer<T>(PhantomData<T>);

impl<T: PartialEq> CommReducer for WatermarkUpdateReducer<T> {
    type Update = WatermarkUpdate<T>;

    fn reduce(&self, left: Self::Update, right: Self::Update) -> Self::Update {
        WatermarkUpdate::merge(left, right)
    }
}

impl<T: Named> Named for WatermarkUpdateReducer<T> {
    fn typename() -> &'static str {
        intern_typename!(Self, "hyperactor::accum::WatermarkUpdateReducer<{}>", T)
    }
}

struct LowWatermarkUpdateAccumulator<T>(PhantomData<T>);

impl<T: Ord + Copy + Named + 'static> Accumulator for LowWatermarkUpdateAccumulator<T> {
    type State = WatermarkUpdate<T>;
    type Update = WatermarkUpdate<T>;
    type Reducer = WatermarkUpdateReducer<T>;

    fn accumulate(&self, state: &mut Self::State, update: Self::Update) {
        let current = std::mem::replace(&mut *state, WatermarkUpdate(HashMap::new()));
        // TODO(pzhang) optimize this and only iterate when there is a new state.
        *state = WatermarkUpdate::merge(current, update);
    }
}

/// Accumulate the min value among the ranks, aka. low watermark, based on the
/// ranks' latest updates. Ranks' previous updates are discarded, and not used
/// in the min value calculation.
///
/// The main difference bwtween low wartermark accumulator and [`MinAccumulator`]
/// is, `MinAccumulator` takes previous updates into consideration too, and thus
/// returns the min of the whole history.
pub fn low_watermark<T: Ord + Copy + Named + 'static>()
-> impl Accumulator<State = WatermarkUpdate<T>, Update = WatermarkUpdate<T>> {
    LowWatermarkUpdateAccumulator(PhantomData)
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use maplit::hashmap;

    use super::*;
    use crate::Named;

    fn serialize<T: Serialize + Named>(values: Vec<T>) -> Vec<Serialized> {
        values
            .into_iter()
            .map(|n| Serialized::serialize(&n).unwrap())
            .collect()
    }

    #[test]
    fn test_comm_reducer_numeric() {
        let u64_numbers: Vec<_> = serialize(vec![1u64, 3u64, 1100u64]);
        let i64_numbers: Vec<_> = serialize(vec![-123i64, 33i64, 110i64]);
        {
            let typehash = <MaxReducer<u64> as Named>::typehash();
            assert_eq!(
                resolve_reducer(typehash)
                    .unwrap()
                    .reduce_updates(u64_numbers.clone())
                    .unwrap()
                    .deserialized::<u64>()
                    .unwrap(),
                1100u64,
            );

            let typehash = <MinReducer<u64> as Named>::typehash();
            assert_eq!(
                resolve_reducer(typehash)
                    .unwrap()
                    .reduce_updates(u64_numbers.clone())
                    .unwrap()
                    .deserialized::<u64>()
                    .unwrap(),
                1u64,
            );

            let typehash = <SumReducer<u64> as Named>::typehash();
            assert_eq!(
                resolve_reducer(typehash)
                    .unwrap()
                    .reduce_updates(u64_numbers)
                    .unwrap()
                    .deserialized::<u64>()
                    .unwrap(),
                1104u64,
            );
        }

        {
            let typehash = <MaxReducer<i64> as Named>::typehash();
            assert_eq!(
                resolve_reducer(typehash)
                    .unwrap()
                    .reduce_updates(i64_numbers.clone())
                    .unwrap()
                    .deserialized::<i64>()
                    .unwrap(),
                110i64,
            );

            let typehash = <MinReducer<i64> as Named>::typehash();
            assert_eq!(
                resolve_reducer(typehash)
                    .unwrap()
                    .reduce_updates(i64_numbers.clone())
                    .unwrap()
                    .deserialized::<i64>()
                    .unwrap(),
                -123i64,
            );

            let typehash = <SumReducer<i64> as Named>::typehash();
            assert_eq!(
                resolve_reducer(typehash)
                    .unwrap()
                    .reduce_updates(i64_numbers)
                    .unwrap()
                    .deserialized::<i64>()
                    .unwrap(),
                20i64,
            );
        }
    }

    #[test]
    fn test_comm_reducer_watermark() {
        let u64_updates = serialize::<WatermarkUpdate<u64>>(
            vec![
                (1, 1),
                (0, 2),
                (0, 1),
                (3, 35),
                (0, 9),
                (1, 10),
                (3, 32),
                (3, 0),
                (3, 321),
            ]
            .into_iter()
            .map(|(k, v)| WatermarkUpdate::from((k, v)))
            .collect(),
        );
        let i64_updates: Vec<_> = serialize::<WatermarkUpdate<i64>>(
            vec![
                (0, 2),
                (1, 1),
                (3, 35),
                (0, 1),
                (1, -10),
                (3, 32),
                (3, 0),
                (3, -99),
                (0, -9),
            ]
            .into_iter()
            .map(WatermarkUpdate::from)
            .collect(),
        );

        fn verify<T: PartialEq + DeserializeOwned + Debug>(
            updates: Vec<Serialized>,
            expected: HashMap<Index, T>,
        ) {
            let typehash = <WatermarkUpdateReducer<i64> as Named>::typehash();
            assert_eq!(
                resolve_reducer(typehash)
                    .unwrap()
                    .reduce_updates(updates)
                    .unwrap()
                    .deserialized::<WatermarkUpdate<T>>()
                    .unwrap()
                    .0,
                expected,
            );
        }

        verify::<i64>(
            i64_updates,
            hashmap! {
                0 => -9,
                1 => -10,
                3 => -99,
            },
        );

        verify::<u64>(
            u64_updates,
            hashmap! {
                0 => 9,
                1 => 10,
                3 => 321,
            },
        );
    }

    #[test]
    fn test_accum_reducer_numeric() {
        assert_eq!(
            sum::<u64>().reducer_typehash(),
            <SumReducer::<u64> as Named>::typehash(),
        );
        assert_eq!(
            sum::<i64>().reducer_typehash(),
            <SumReducer::<i64> as Named>::typehash(),
        );

        assert_eq!(
            min::<u64>().reducer_typehash(),
            <MinReducer::<u64> as Named>::typehash(),
        );
        assert_eq!(
            min::<i64>().reducer_typehash(),
            <MinReducer::<i64> as Named>::typehash(),
        );

        assert_eq!(
            max::<u64>().reducer_typehash(),
            <MaxReducer::<u64> as Named>::typehash(),
        );
        assert_eq!(
            max::<i64>().reducer_typehash(),
            <MaxReducer::<i64> as Named>::typehash(),
        );
    }

    #[test]
    fn test_accum_reducer_watermark() {
        fn verify<T: Ord + Copy + Named>() {
            assert_eq!(
                low_watermark::<T>().reducer_typehash(),
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
            // send in descending order
            (0, 1003, 1003),
            (1, 1002, 1002),
            (2, 1001, 1001),
            // send in asscending order
            (0, 100, 100),
            (1, 101, 100),
            (2, 102, 100),
            // send same as accumulator's cache
            (0, 100, 100),
            (1, 101, 100),
            (2, 102, 100),
            // shuffle rank 0 to be largest, and make rank 1 smallest
            (0, 1000, 101),
            // shuffle rank 1 to be largest, and make rank 2 smallest
            (1, 1100, 102),
            // shuffle rank 2 to be largest, and make rank 0 smallest
            (2, 1200, 1000),
            // Increase their value, but do not change their order
            (0, 1001, 1001),
            (1, 1101, 1001),
            (2, 1201, 1001),
            // decrease their values
            (2, 102, 102),
            (1, 101, 101),
            (0, 100, 100),
        ];
        let mut state = WatermarkUpdate(HashMap::new());
        for (rank, value, expected) in ranks_values_expectations {
            accumulator.accumulate(&mut state, WatermarkUpdate::from((rank, value)));
            assert_eq!(state.get(), &expected, "rank is {rank}; value is {value}");
        }
    }
}
