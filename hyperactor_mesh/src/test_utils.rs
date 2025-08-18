/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::Bind;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::Named;
use hyperactor::Unbind;
use serde::Deserialize;
use serde::Serialize;

/// Message that can be sent to an EmptyActor.
#[derive(Serialize, Deserialize, Debug, Named, Clone, Bind, Unbind)]
pub struct EmptyMessage();

#[derive(Debug, PartialEq, Default, Actor)]
#[hyperactor::export(
    handlers = [
        EmptyMessage { cast = true },
    ],
)]
pub struct EmptyActor();

#[async_trait]
impl Handler<EmptyMessage> for EmptyActor {
    async fn handle(&mut self, _: &Context<Self>, _: EmptyMessage) -> Result<(), anyhow::Error> {
        Ok(())
    }
}
hyperactor::remote!(EmptyActor);
