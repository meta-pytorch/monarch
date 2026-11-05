/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import assert from "node:assert/strict";
import { after, test } from "node:test";
import {
  actorInstance,
  actorRole,
  agentDisplayName,
  cleanNodeLabel,
  formatDateTime,
  formatShape,
  formatTime,
  leafName,
  prettyRole,
  splitMessages,
} from "../src/lib/format";

const originalTimezone = process.env.TZ;
process.env.TZ = "America/Los_Angeles";
after(() => {
  if (originalTimezone === undefined) delete process.env.TZ;
  else process.env.TZ = originalTimezone;
});

test("microsecond timestamps use local dates and daylight-saving offsets", () => {
  const winter = Date.UTC(2026, 0, 2, 0, 4, 5, 600) * 1000 + 789;
  assert.equal(formatTime(winter), "16:04:05.600");
  assert.equal(formatDateTime(winter), "2026-01-01 16:04:05");

  const summer = Date.UTC(2026, 6, 2, 0, 4, 5, 7) * 1000 + 999;
  assert.equal(formatTime(summer), "17:04:05.007");
  assert.equal(formatDateTime(summer), "2026-07-01 17:04:05");
  assert.equal(formatTime(0), "16:00:00.000");
  assert.equal(formatDateTime(0), "1969-12-31 16:00:00");
});

test("missing and invalid timestamps display a placeholder", () => {
  for (const timestamp of [null, undefined, NaN, Infinity, -Infinity, Number.MAX_VALUE]) {
    assert.equal(formatTime(timestamp), "—");
    assert.equal(formatDateTime(timestamp), "—");
  }
});

test("current and legacy shapes display their dimensions", () => {
  for (const shape of [{ inner: { sizes: [2, 4] } }, { sizes: [2, 4] }, { dims: [2, 4] }]) {
    assert.equal(formatShape(JSON.stringify(shape)), "[2, 4]");
  }
  assert.equal(formatShape('{"inner":{"sizes":[]}}'), "[]");
  assert.equal(formatShape('{"sizes":[0,1,8]}'), "[0, 1, 8]");
});

test("missing shapes use a placeholder and malformed or unknown shapes retain their text", () => {
  for (const shape of [null, undefined, ""]) assert.equal(formatShape(shape), "—");
  for (const shape of ['{"sizes":', '{"sizes":4}', '{"other":[2,4]}', "null"]) {
    assert.equal(formatShape(shape), shape);
  }
});

test("actor roles and instances are independent of transport suffixes", () => {
  const trainer = "trainer<o7B>.anon-0<dqZ>@anon-0<dqZ>.ipc:///tmp/session/hosts_0";
  assert.equal(actorRole(trainer), "trainer");
  assert.equal(actorInstance(trainer), "anon-0");
  assert.equal(leafName(trainer), "trainer<o7B>.anon-0<dqZ>");
  assert.equal(actorRole("host_agent.service<2>@tcp://worker:1234"), "host_agent");
  assert.equal(actorInstance("host_agent.service<2>@tcp://worker:1234"), "service");
  assert.equal(prettyRole("host_agent"), "Host Agent");
  assert.equal(prettyRole("proc-agent"), "Proc Agent");
  assert.equal(leafName("mesh/worker,trainer@tcp://worker:1234"), "trainer");
  for (const name of [null, undefined, ""]) {
    assert.equal(actorRole(name), "actor");
    assert.equal(actorInstance(name), null);
    assert.equal(leafName(name), "—");
  }
  assert.equal(actorInstance("trainer<o7B>@tcp://worker:1234"), null);
});

test("node labels remove identity suffixes and give hosts readable names", () => {
  assert.equal(cleanNodeLabel("trainer[o7B7whEtk1W]@tcp://worker:1234", "actor"), "trainer");
  assert.equal(cleanNodeLabel("worker-0<fa4wpgPjMyD>", "proc"), "worker-0");
  assert.equal(cleanNodeLabel("ipc:///tmp/session/hosts_0", "host"), "hosts_0");
  assert.equal(cleanNodeLabel("@91ytVCObcks", "host"), "controller");
  assert.equal(cleanNodeLabel("control (controller)", "host"), "control");
});

test("system agent display names use explicit ranks or legacy instance suffixes", () => {
  assert.equal(agentDisplayName("HostAgent[3]"), "Host Unit 3");
  assert.equal(agentDisplayName("HostAgent[3]", 0), "Host Unit 0");
  assert.equal(agentDisplayName("proc_agent.anon-3<x>@tcp://worker:1234", "7"), "Proc Unit 7");
  assert.equal(agentDisplayName("proc_agent.anon-3<x>@tcp://worker:1234"), "Proc Unit");
  assert.equal(agentDisplayName("trainer<x>.worker<y>@tcp://worker:1234"), null);
  assert.equal(agentDisplayName(null), null);
});

test("message directions preserve distinct large string IDs, order, and self-messages", () => {
  const actorId = "9007199254740993";
  const peerId = "9007199254740992";
  const outgoing = { id: "out", from_actor_id: actorId, to_actor_id: peerId };
  const incoming = { id: "in", from_actor_id: peerId, to_actor_id: actorId };
  const unrelated = { id: "other", from_actor_id: peerId, to_actor_id: peerId };
  const self = { id: "self", from_actor_id: actorId, to_actor_id: actorId };
  const messages = [outgoing, incoming, unrelated, self];
  assert.deepEqual(splitMessages(messages, actorId), {
    incoming: [incoming, self],
    outgoing: [outgoing, self],
  });
  assert.deepEqual(splitMessages([], actorId), { incoming: [], outgoing: [] });
  const numeric = { from_actor_id: 7, to_actor_id: "8" };
  assert.deepEqual(splitMessages([numeric], "7"), { incoming: [], outgoing: [numeric] });
  assert.deepEqual(splitMessages([numeric], 8), { incoming: [numeric], outgoing: [] });
});
