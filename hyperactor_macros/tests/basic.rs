/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![allow(dead_code)]
pub mod castable;
pub mod export;

use std::fmt::Debug;

use anyhow::Result;
use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Named;
use hyperactor::OncePortRef;
use hyperactor::RefClient;
use hyperactor::forward;
use hyperactor::instrument;
use hyperactor::instrument_infallible;
use serde::Deserialize;
use serde::Serialize;

#[derive(Handler, Debug, Named, Serialize, Deserialize)]
enum ShoppingList {
    // Oneway messages dispatch messages asynchronously, with no reply.
    Add(String),
    Remove {
        item: String,
    }, // both tuple and struct variants are supported.

    // Call messages dispatch a request, expecting a reply to the
    // provided port, which must be in the last position.
    Exists(String, #[reply] OncePortRef<bool>),

    // Tests macro hygience. We use 'result' as a keyword in the implementation.
    Clobber {
        arg: String,
        #[reply]
        result: OncePortRef<bool>,
    },
}

#[derive(Handler, HandleClient, RefClient, Debug, Serialize, Deserialize, Named)]
#[log_level(info)]
enum TestVariantForms {
    OneWayStruct {
        a: u64,
        b: u64,
    },

    #[log_level(error)]
    OneWayTuple(u64, u64),

    OneWayTupleNoArgs(),

    OneWayStructNoArgs {},

    CallStruct {
        a: u64,
        #[reply]
        b: OncePortRef<u64>,
    },

    CallTuple(u64, #[reply] OncePortRef<u64>),

    CallTupleNoArgs(#[reply] OncePortRef<u64>),

    CallStructNoArgs {
        #[reply]
        a: OncePortRef<u64>,
    },
}

#[instrument(fields(name = 4))]
async fn yolo() -> Result<i32, i32> {
    Ok(10)
}

#[instrument_infallible(fields(crow = "black"))]
async fn yeet() -> String {
    String::from("cawwww")
}

#[test]
fn basic() {
    // nothing, just checks whether this file will compile
}

#[derive(Debug, Handler, HandleClient)]
enum GenericArgMessage<A: Clone + Sync + Send + Debug + 'static> {
    Variant(A),
}

#[derive(Debug)]
struct GenericArgActor {}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GenericArgParams {}

#[async_trait]
impl Actor for GenericArgActor {
    type Params = GenericArgParams;

    async fn new(_params: Self::Params) -> Result<Self> {
        Ok(Self {})
    }
}

#[async_trait]
#[forward(GenericArgMessage<usize>)]
impl GenericArgMessageHandler<usize> for GenericArgActor {
    async fn variant(&mut self, _cx: &Context<Self>, _val: usize) -> Result<()> {
        Ok(())
    }
}

#[derive(Actor, Default, Debug)]
struct DefaultActorTest {
    value: u64,
}

static_assertions::assert_impl_all!(DefaultActorTest: Actor);

#[derive(Actor, Default, Debug)]
#[actor(passthrough)]
struct PassthroughActorTest {
    value: u64,
}

static_assertions::assert_impl_all!(PassthroughActorTest: Actor);
static_assertions::assert_type_eq_all!(
    <PassthroughActorTest as hyperactor::Actor>::Params,
    PassthroughActorTest
);

// Test struct support for Handler derive
#[derive(Handler, Debug, Named, Serialize, Deserialize)]
struct SimpleStructMessage {
    field1: String,
    field2: u32,
}
