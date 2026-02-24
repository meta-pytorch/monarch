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
use hyperactor::OncePortRef;
use hyperactor::RefClient;
use hyperactor::handle;
use hyperactor::instrument;
use hyperactor::instrument_infallible;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

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

#[derive(Debug, Default)]
#[hyperactor::export(handlers = [TestVariantForms])]
struct TestVariantFormsActor {}

impl Actor for TestVariantFormsActor {}

#[async_trait]
#[handle(TestVariantForms)]
impl TestVariantFormsHandler for TestVariantFormsActor {
    async fn one_way_struct(&mut self, _cx: &Context<Self>, _a: u64, _b: u64) -> Result<()> {
        Ok(())
    }

    async fn one_way_tuple(&mut self, _cx: &Context<Self>, _a: u64, _b: u64) -> Result<()> {
        Ok(())
    }

    async fn one_way_tuple_no_args(&mut self, _cx: &Context<Self>) -> Result<()> {
        Ok(())
    }

    async fn one_way_struct_no_args(&mut self, _cx: &Context<Self>) -> Result<()> {
        Ok(())
    }

    async fn call_struct(&mut self, _cx: &Context<Self>, a: u64) -> Result<u64> {
        Ok(a)
    }

    async fn call_tuple(&mut self, _cx: &Context<Self>, a: u64) -> Result<u64> {
        Ok(a)
    }

    async fn call_tuple_no_args(&mut self, _cx: &Context<Self>) -> Result<u64> {
        Ok(0)
    }

    async fn call_struct_no_args(&mut self, _cx: &Context<Self>) -> Result<u64> {
        Ok(0)
    }
}

#[instrument(fields(name = 4))]
async fn yolo() -> Result<i32, i32> {
    Ok(10)
}

#[instrument_infallible(fields(crow = "black"))]
async fn yeet() -> String {
    String::from("cawwww")
}

#[derive(Debug, Handler, HandleClient)]
enum GenericArgMessage<A: Clone + Sync + Send + Debug + 'static> {
    Variant(A),
}

#[derive(Debug)]
struct GenericArgActor {}

impl Actor for GenericArgActor {}

#[async_trait]
#[handle(GenericArgMessage<usize>)]
impl GenericArgMessageHandler<usize> for GenericArgActor {
    async fn variant(&mut self, _cx: &Context<Self>, _val: usize) -> Result<()> {
        Ok(())
    }
}

#[derive(Default, Debug)]
struct DefaultActorTest {
    value: u64,
}

impl Actor for DefaultActorTest {}

static_assertions::assert_impl_all!(DefaultActorTest: Actor);

// Test struct support for Handler derive
#[derive(Handler, Debug, Named, Serialize, Deserialize)]
struct SimpleStructMessage {
    field1: String,
    field2: u32,
}

#[cfg(test)]
mod tests {
    use hyperactor::proc::Proc;
    use timed_test::async_timed_test;

    use super::*;

    #[test]
    fn basic() {
        // nothing, just checks whether this file will compile
    }

    // Verify it compiles
    #[async_timed_test(timeout_secs = 30)]
    async fn test_client_macros() {
        let proc = Proc::local();
        let (client, _) = proc.instance("client").unwrap();
        let actor_handle = proc.spawn("foo", TestVariantFormsActor {}).unwrap();

        assert_eq!(actor_handle.call_struct(&client, 10).await.unwrap(), 10,);

        let actor_ref = actor_handle.bind::<TestVariantFormsActor>();
        assert_eq!(actor_ref.call_struct(&client, 10).await.unwrap(), 10,);
    }
}
