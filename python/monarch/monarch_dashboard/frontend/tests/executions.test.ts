/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import assert from "node:assert/strict";
import { test } from "node:test";
import { computeActorHealth } from "../src/lib/actorHealth";
import { executionDuration } from "../src/lib/executions";
import type { Execution, Actor } from "../src/types";

test("completed executions do not override current fleet failures", () => {
  const actors: Actor[] = [ {id:"1",timestamp_us:0,mesh_id:"1",rank:0,full_name:"worker<1>",latest_status:"Failed"},
    {id:"2",timestamp_us:0,mesh_id:"2",rank:0,full_name:"BashActor<2>",latest_status:"Idle",execution_id:"execution"},
    {id:"3",timestamp_us:0,mesh_id:"3",rank:0,full_name:"BashActor<3>",latest_status:"Failed",execution_id:"old-failure"} ];
  const health=computeActorHealth(actors);
  assert.equal(health.health,"unhealthy");
  assert.equal(health.down,1);
  assert.equal(health.execution,2);
  assert.equal(health.byStatus.execution,2);
  assert.equal(health.workload,1);
});

test("execution duration uses terminal time or the live clock and clamps negative intervals", () => {
  const execution={started_us:0,finished_us:65_000_000,status:"failed"} as Execution;
  assert.equal(executionDuration(execution),"1m 5s");
  assert.equal(executionDuration({...execution,finished_us:null},3_665_000),"1h 1m");
  assert.equal(executionDuration({...execution,finished_us:-1}),"0.0s");
});
