/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::future::Future;
use std::pin::Pin;

use chrysalis_core::Pid;

use crate::EnumerationCursor;
use crate::EnumerationResult;
use crate::ParentLink;
use crate::Resolution;
use crate::ResolveConsistency;
use crate::ResolveError;

/// A stable query path toward an ancestor nameserver.
pub trait UpstreamNameserver: Send + Sync {
    /// Resolves one PID with the requested cache consistency.
    fn resolve(
        &self,
        pid: Pid,
        consistency: ResolveConsistency,
    ) -> Pin<Box<dyn Future<Output = Result<Resolution, ResolveError>> + Send + '_>>;

    /// Enumerates one page with the requested cache consistency.
    fn enumerate(
        &self,
        cursor: Option<EnumerationCursor>,
        limit: u32,
        consistency: ResolveConsistency,
    ) -> Pin<Box<dyn Future<Output = Result<EnumerationResult, ResolveError>> + Send + '_>>;
}

impl UpstreamNameserver for ParentLink {
    fn resolve(
        &self,
        pid: Pid,
        consistency: ResolveConsistency,
    ) -> Pin<Box<dyn Future<Output = Result<Resolution, ResolveError>> + Send + '_>> {
        Box::pin(ParentLink::resolve(self, pid, consistency))
    }

    fn enumerate(
        &self,
        cursor: Option<EnumerationCursor>,
        limit: u32,
        consistency: ResolveConsistency,
    ) -> Pin<Box<dyn Future<Output = Result<EnumerationResult, ResolveError>> + Send + '_>> {
        Box::pin(ParentLink::enumerate(self, cursor, limit, consistency))
    }
}
