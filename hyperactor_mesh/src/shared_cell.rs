/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use dashmap::DashMap;
use futures::future::join_all;
use futures::future::try_join_all;
use preempt_rwlock::OwnedPreemptibleRwLockReadGuard;
use preempt_rwlock::PreemptibleRwLock;
use tokio::sync::TryLockError;

#[derive(thiserror::Error, Debug)]
pub struct EmptyCellError {}

impl From<TryLockError> for EmptyCellError {
    fn from(_err: TryLockError) -> Self {
        Self {}
    }
}

impl std::fmt::Display for EmptyCellError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "already taken")
    }
}

#[derive(thiserror::Error, Debug)]
pub enum TryTakeError {
    #[error("already taken")]
    Empty,
    #[error("cannot lock: {0}")]
    TryLockError(#[from] TryLockError),
}

struct PoolRef {
    map: Arc<DashMap<usize, Arc<dyn SharedCellDiscard + Send + Sync>>>,
    key: usize,
}

impl Debug for PoolRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolRef").field("key", &self.key).finish()
    }
}

#[derive(Debug)]
struct Inner<T> {
    value: Option<T>,
    pool: Option<PoolRef>,
}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        if let Some(pool) = &self.pool {
            pool.map.remove(&pool.key);
        }
    }
}

/// A wrapper class that facilitates sharing an item across different users, supporting:
/// - Ability grab a reference-counted reference to the item
/// - Ability to consume the item, leaving the cell in an unusable state
#[derive(Debug)]
pub struct SharedCell<T> {
    inner: Arc<PreemptibleRwLock<Inner<T>>>,
}

impl<T> Clone for SharedCell<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> From<T> for SharedCell<T> {
    fn from(value: T) -> Self {
        Self {
            inner: Arc::new(PreemptibleRwLock::new(Inner {
                value: Some(value),
                pool: None,
            })),
        }
    }
}

impl<T> SharedCell<T> {
    fn with_pool(value: T, pool: PoolRef) -> Self {
        Self {
            inner: Arc::new(PreemptibleRwLock::new(Inner {
                value: Some(value),
                pool: Some(pool),
            })),
        }
    }
}

pub struct SharedCellRef<T, U = T> {
    guard: OwnedPreemptibleRwLockReadGuard<Inner<T>, U>,
}

impl<T> SharedCellRef<T> {
    fn from(guard: OwnedPreemptibleRwLockReadGuard<Inner<T>>) -> Result<Self, EmptyCellError> {
        if guard.value.is_none() {
            return Err(EmptyCellError {});
        }
        Ok(Self {
            guard: OwnedPreemptibleRwLockReadGuard::map(guard, |guard| {
                guard.value.as_ref().unwrap()
            }),
        })
    }

    pub fn map<F, U>(self, f: F) -> SharedCellRef<T, U>
    where
        F: FnOnce(&T) -> &U,
    {
        SharedCellRef {
            guard: OwnedPreemptibleRwLockReadGuard::map(self.guard, f),
        }
    }

    pub async fn preempted(&self) {
        self.guard.preempted().await
    }
}

impl<T, U: Debug> Debug for SharedCellRef<T, U> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&**self, f)
    }
}

impl<T, U> Deref for SharedCellRef<T, U> {
    type Target = U;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<T> SharedCell<T> {
    /// Borrow the cell, returning a reference to the item. If the cell is empty, returns an error.
    /// While references are held, the cell cannot be taken below.
    pub fn borrow(&self) -> Result<SharedCellRef<T>, EmptyCellError> {
        SharedCellRef::from(self.inner.clone().try_read_owned()?)
    }

    /// Execute given closure with write access to the underlying data. If the cell is empty, returns an error.
    pub async fn with_mut<F, R>(&self, f: F) -> Result<R, EmptyCellError>
    where
        F: FnOnce(&mut T) -> R,
    {
        let mut inner = self.inner.write(true).await;
        let value = inner.value.as_mut().ok_or(EmptyCellError {})?;
        Ok(f(value))
    }

    /// Non-async variant of [`with_mut`](Self::with_mut). Returns
    /// `Err` if the write lock cannot be acquired immediately or if
    /// the cell has already been taken.
    pub fn try_with_mut<F, R>(&self, f: F) -> Result<R, EmptyCellError>
    where
        F: FnOnce(&mut T) -> R,
    {
        let mut inner = self.inner.try_write(false).map_err(|_| EmptyCellError {})?;
        let value = inner.value.as_mut().ok_or(EmptyCellError {})?;
        Ok(f(value))
    }

    /// Take the item out of the cell, leaving it in an unusable state.
    pub async fn take(&self) -> Result<T, EmptyCellError> {
        let mut inner = self.inner.write(true).await;
        inner.value.take().ok_or(EmptyCellError {})
    }

    pub fn blocking_take(&self) -> Result<T, EmptyCellError> {
        let mut inner = self.inner.blocking_write(true);
        inner.value.take().ok_or(EmptyCellError {})
    }

    pub fn try_take(&self) -> Result<T, TryTakeError> {
        let mut inner = self.inner.try_write(true)?;
        inner.value.take().ok_or(TryTakeError::Empty)
    }
}

/// A pool of `SharedCell`s which can be used to mass `take()` and discard them all at once.
pub struct SharedCellPool {
    map: Arc<DashMap<usize, Arc<dyn SharedCellDiscard + Send + Sync>>>,
    token: AtomicUsize,
}

impl Default for SharedCellPool {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedCellPool {
    pub fn new() -> Self {
        Self {
            map: Arc::new(DashMap::new()),
            token: AtomicUsize::new(0),
        }
    }

    pub fn insert<T>(&self, value: T) -> SharedCell<T>
    where
        T: Send + Sync + 'static,
    {
        let map = self.map.clone();
        let key = self.token.fetch_add(1, Ordering::Relaxed);
        let pool = PoolRef { map, key };
        let value: SharedCell<_> = SharedCell::with_pool(value, pool);
        self.map.entry(key).insert(Arc::new(value.clone()));
        value
    }

    /// Run `take` on all cells in the pool and immediately drop them.
    pub async fn discard_all(self) -> Result<(), EmptyCellError> {
        try_join_all(
            self.map
                .iter()
                .map(|r| async move { r.value().discard().await }),
        )
        .await?;
        Ok(())
    }

    /// Run `take` on all cells in the pool and immediately drop them or produce an error if the cell has already been taken
    pub async fn discard_or_error_all(self) -> Vec<Result<(), EmptyCellError>> {
        join_all(
            self.map
                .iter()
                .map(|r| async move { r.value().discard().await }),
        )
        .await
    }
}

/// Trait to facilitate storing `SharedCell`s of different types in a single pool.
#[async_trait]
pub trait SharedCellDiscard {
    async fn discard(&self) -> Result<(), EmptyCellError>;
    fn blocking_discard(&self) -> Result<(), EmptyCellError>;
    fn try_discard(&self) -> Result<(), TryTakeError>;
}

#[async_trait]
impl<T: Send + Sync> SharedCellDiscard for SharedCell<T> {
    fn try_discard(&self) -> Result<(), TryTakeError> {
        self.try_take()?;
        Ok(())
    }

    async fn discard(&self) -> Result<(), EmptyCellError> {
        self.take().await?;
        Ok(())
    }

    fn blocking_discard(&self) -> Result<(), EmptyCellError> {
        self.blocking_take()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::*;

    #[tokio::test]
    async fn borrow_after_take() -> Result<()> {
        let cell = SharedCell::from(0);
        let _ = cell.take().await;
        assert!(cell.borrow().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn take_after_borrow() -> Result<()> {
        let cell = SharedCell::from(0);
        let b = cell.borrow()?;
        assert!(cell.try_take().is_err());
        std::mem::drop(b);
        cell.try_take()?;
        Ok(())
    }
}
