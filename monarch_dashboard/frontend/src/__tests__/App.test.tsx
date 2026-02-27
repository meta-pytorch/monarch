/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React from "react";
import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import App from "../App";

const MOCK_HOST_MESHES = [
  {
    id: 1, timestamp_us: 1700000000000000, class: "Host",
    given_name: "host_mesh_0", full_name: "/host_mesh_0",
    shape_json: '{"dims": [2]}', parent_mesh_id: null, parent_view_json: null,
  },
];

const MOCK_ALL_MESHES = [...MOCK_HOST_MESHES];
const MOCK_CHILDREN: any[] = [];
const MOCK_ACTORS = [
  {
    id: 1, timestamp_us: 1700000000000000, mesh_id: 1,
    rank: 0, full_name: "/host_mesh_0/HostMeshAgent[0]",
  },
];

const MOCK_ACTOR_DETAIL = {
  ...MOCK_ACTORS[0],
  latest_status: "idle",
  latest_status_timestamp_us: 1700000015000000,
};

const MOCK_MESSAGES: any[] = [];

const MOCK_SUMMARY = {
  mesh_counts: { total: 10, by_class: { Host: 2, Proc: 4 } },
  actor_counts: { total: 10, by_status: { idle: 5, failed: 1, stopped: 4 } },
  message_counts: { total: 82, by_status: { delivered: 77 }, by_endpoint: { train_step: 18 }, delivery_rate: 0.939 },
  errors: { failed_actors: [], stopped_actors: [], failed_messages: 0 },
  timeline: { start_us: 1700000000000000, end_us: 1700000300000000, failure_onset_us: null, total_status_events: 207, total_message_events: 246 },
  health_score: 64,
};

beforeEach(() => {
  jest.spyOn(global, "fetch").mockImplementation((url: any) => {
    const path = typeof url === "string" ? url : url.toString();

    if (path.includes("/summary")) {
      return Promise.resolve({
        ok: true,
        json: async () => MOCK_SUMMARY,
      } as Response);
    }
    if (path.includes("/meshes?class=Host")) {
      return Promise.resolve({
        ok: true,
        json: async () => MOCK_HOST_MESHES,
      } as Response);
    }
    if (path.match(/\/meshes\/\d+\/children/)) {
      return Promise.resolve({
        ok: true,
        json: async () => MOCK_CHILDREN,
      } as Response);
    }
    if (path.match(/\/meshes\/\d+$/)) {
      return Promise.resolve({
        ok: true,
        json: async () => MOCK_HOST_MESHES[0],
      } as Response);
    }
    if (path.match(/\/meshes$/)) {
      return Promise.resolve({
        ok: true,
        json: async () => MOCK_ALL_MESHES,
      } as Response);
    }
    if (path.match(/\/actors\/\d+$/)) {
      return Promise.resolve({
        ok: true,
        json: async () => MOCK_ACTOR_DETAIL,
      } as Response);
    }
    if (path.includes("/actors")) {
      return Promise.resolve({
        ok: true,
        json: async () => MOCK_ACTORS,
      } as Response);
    }
    if (path.includes("/messages")) {
      return Promise.resolve({
        ok: true,
        json: async () => MOCK_MESSAGES,
      } as Response);
    }
    return Promise.resolve({
      ok: true,
      json: async () => [],
    } as Response);
  });
});

afterEach(() => {
  jest.restoreAllMocks();
});

describe("App", () => {
  test("renders header with Monarch branding", async () => {
    render(<App />);
    expect(screen.getByText("Monarch")).toBeInTheDocument();
    expect(screen.getByText("dashboard")).toBeInTheDocument();
  });

  test("renders Summary tab as active by default", () => {
    render(<App />);
    const tab = screen.getByText("Summary");
    expect(tab).toHaveClass("active");
  });

  test("shows summary dashboard on load", async () => {
    render(<App />);
    await waitFor(() => {
      expect(screen.getByTestId("summary-dashboard")).toBeInTheDocument();
    });
  });

  test("switching to Hierarchy tab shows breadcrumb", async () => {
    render(<App />);
    fireEvent.click(screen.getByText("Hierarchy"));
    expect(screen.getByText("Host Meshes")).toBeInTheDocument();
  });

  test("shows host meshes table in Hierarchy tab", async () => {
    render(<App />);
    fireEvent.click(screen.getByText("Hierarchy"));
    await waitFor(() => {
      expect(screen.getByText("host_mesh_0")).toBeInTheDocument();
    });
  });

  test("switching to DAG tab shows DAG view", async () => {
    render(<App />);
    fireEvent.click(screen.getByText("DAG"));
    await waitFor(() => {
      const loading = screen.queryByText("Loading DAG data...");
      const container = screen.queryByTestId("dag-container");
      const empty = screen.queryByText("No mesh data available");
      expect(loading || container || empty).toBeTruthy();
    });
  });

  test("switching back to Summary from DAG works", async () => {
    render(<App />);
    fireEvent.click(screen.getByText("DAG"));
    fireEvent.click(screen.getByText("Summary"));
    await waitFor(() => {
      expect(screen.getByTestId("summary-dashboard")).toBeInTheDocument();
    });
  });

  test("all three tabs are rendered", () => {
    render(<App />);
    expect(screen.getByText("Summary")).toBeInTheDocument();
    expect(screen.getByText("Hierarchy")).toBeInTheDocument();
    expect(screen.getByText("DAG")).toBeInTheDocument();
  });
});
