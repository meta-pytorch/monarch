/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Provides cells that can provide snapshot and update streams.

use std::ops::Deref;
use std::ops::DerefMut;

use tokio::sync::broadcast;

use crate::Diff;

/// A watch provides stateful value updates, sending incremental patches
/// to a set of subscribers.
pub struct Watch<T: Diff>
where
    T::Patch: Clone,
{
    value: T,
    sender: broadcast::Sender<T::Patch>,
}

impl<T: Diff> Watch<T>
where
    T::Patch: Clone,
{
    /// Create a new watch holding the provided initial value.
    pub fn new(value: T) -> Self {
        let (sender, _) = broadcast::channel(1024);
        Watch { value, sender }
    }

    /// The current value of the watch.
    pub fn value(&self) -> &T {
        &self.value
    }

    /// Subscribe to the watch's value updates. The broadcast receiver has a maximum
    /// buffer size of 1024. If a subscriber is lagging (see [`tokio::sync::broadcast::Receiver`]
    /// for details), is the subscriber's responsibility to re-establish the subscription.
    ///
    /// Typically, subscribers will read the current value, clone it, and then subscribe to the watch
    /// for further updates.
    pub fn subscribe(&self) -> broadcast::Receiver<T::Patch> {
        self.sender.subscribe()
    }
}

impl<T: Diff + Clone> Watch<T>
where
    T::Patch: Clone,
{
    /// Returns a guard that allows mutating the value and publishes the diff when dropped.
    /// We could define a 'Mutator' type to avoid the extra clone here.
    ///
    /// Currently, watch updates require cloning the value in order to compute the update
    /// (comparing old and new values). We could expand this crate's repertoir by providing
    /// a "Mutator" trait (and derive macro) that tracks mutations directly into a patch,
    /// avoiding the clone.
    pub fn update(&mut self) -> WatchUpdate<'_, T> {
        WatchUpdate {
            original: self.value.clone(),
            value: &mut self.value,
            sender: &self.sender,
        }
    }
}

/// An update guard (like a "Ref") used to borrow the watch while updating its value.
pub struct WatchUpdate<'a, T: Diff + Clone>
where
    T::Patch: Clone,
{
    original: T,
    value: &'a mut T,
    sender: &'a broadcast::Sender<T::Patch>,
}

impl<'a, T: Diff + Clone> Deref for WatchUpdate<'a, T>
where
    T::Patch: Clone,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.value
    }
}

impl<'a, T: Diff + Clone> DerefMut for WatchUpdate<'a, T>
where
    T::Patch: Clone,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.value
    }
}

impl<'a, T: Diff + Clone> Drop for WatchUpdate<'a, T>
where
    T::Patch: Clone,
{
    fn drop(&mut self) {
        let patch = self.original.diff(self.value);
        let _ = self.sender.send(patch);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate as struct_diff_patch; // for macro expansion
    use crate::Patch;

    #[derive(Debug, Clone, PartialEq, Diff, Patch)]
    struct TestStruct {
        name: String,
        count: u32,
    }

    #[tokio::test]
    async fn update_sends_patch_on_drop() {
        let mut watch = Watch::new(TestStruct {
            name: "start".into(),
            count: 1,
        });
        let mut rx = watch.subscribe();

        {
            let mut u = watch.update();
            u.name = "end".into();
            u.count = 2;
        }

        let patch = rx.recv().await.unwrap();

        let mut value = TestStruct {
            name: "start".into(),
            count: 1,
        };
        let _ = patch.apply(&mut value);

        assert_eq!(
            value,
            TestStruct {
                name: "end".into(),
                count: 2
            }
        );
        assert_eq!(watch.value().name, "end");
        assert_eq!(watch.value().count, 2);
    }
}
