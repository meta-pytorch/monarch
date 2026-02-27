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

// Mock fetch for all API calls.
const MOCK_HOST_MESHES = [
  {
    id: 1, timestamp_us: 1700000000000000, class: "Host",
    given_name: "host_mesh_0", full_name: "/host_mesh_0",
    shape_json: '{"dims": [2]}', parent_mesh_id: null, parent_view_json: null,
  },
];

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

beforeEach(() => {
  jest.spyOn(global, "fetch").mockImplementation((url: any) => {
    const path = typeof url === "string" ? url : url.toString();

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

  test("renders Hierarchy tab as active by default", () => {
    render(<App />);
    const tab = screen.getByText("Hierarchy");
    expect(tab).toHaveClass("active");
  });

  test("renders breadcrumb with Host Meshes root", () => {
    render(<App />);
    expect(screen.getByText("Host Meshes")).toBeInTheDocument();
  });

  test("shows host meshes table on load", async () => {
    render(<App />);
    await waitFor(() => {
      expect(screen.getByText("host_mesh_0")).toBeInTheDocument();
    });
  });

  test("switching to DAG tab shows placeholder", () => {
    render(<App />);
    fireEvent.click(screen.getByText("DAG"));
    expect(
      screen.getByText("DAG view will be implemented in Milestone 4")
    ).toBeInTheDocument();
  });

  test("switching back to Hierarchy resets breadcrumb", () => {
    render(<App />);
    fireEvent.click(screen.getByText("DAG"));
    fireEvent.click(screen.getByText("Hierarchy"));
    expect(screen.getByText("Host Meshes")).toBeInTheDocument();
  });
});
