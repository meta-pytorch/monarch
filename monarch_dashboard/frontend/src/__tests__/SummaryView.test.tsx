/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React from "react";
import { render, screen, waitFor } from "@testing-library/react";
import "@testing-library/jest-dom";
import { SummaryView } from "../components/SummaryView";

const MOCK_SUMMARY = {
  mesh_counts: {
    total: 10,
    by_class: { Host: 2, Proc: 4, "Python<Trainer>": 4 },
  },
  actor_counts: {
    total: 10,
    by_status: { idle: 3, processing: 2, failed: 1, stopped: 4 },
  },
  message_counts: {
    total: 82,
    by_status: { delivered: 77, failed: 5 },
    by_endpoint: {
      train_step: 18,
      aggregate_gradients: 15,
      checkpoint: 16,
      broadcast_params: 17,
      sync_state: 16,
    },
    delivery_rate: 0.939,
  },
  errors: {
    failed_actors: [
      {
        actor_id: 10,
        full_name: "//root/host_mesh_1/proc_mesh_1_1/actor_mesh_1_1/PythonActor<Trainer>[0]",
        reason: "CUDA OOM",
        timestamp_us: 1700000240000000,
        mesh_id: 10,
      },
    ],
    stopped_actors: [
      {
        actor_id: 6,
        full_name: "//root/host_mesh_1/proc_mesh_1_1/ProcMeshAgent[0]",
        reason: "death propagation from proc_mesh_1_1",
        timestamp_us: 1700000250000000,
        mesh_id: 6,
      },
    ],
    failed_messages: 5,
  },
  timeline: {
    start_us: 1700000000000000,
    end_us: 1700000300000000,
    failure_onset_us: 1700000240000000,
    total_status_events: 207,
    total_message_events: 246,
  },
  health_score: 52,
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
    return Promise.resolve({
      ok: true,
      json: async () => ({}),
    } as Response);
  });
});

afterEach(() => {
  jest.restoreAllMocks();
});

describe("SummaryView", () => {
  it("shows loading state initially", () => {
    render(<SummaryView />);
    expect(screen.getByText("Loading summary metrics...")).toBeInTheDocument();
  });

  it("renders the summary dashboard", async () => {
    render(<SummaryView />);
    await waitFor(() => {
      expect(screen.getByTestId("summary-dashboard")).toBeInTheDocument();
    });
  });

  it("renders the health gauge", async () => {
    render(<SummaryView />);
    await waitFor(() => {
      expect(screen.getByTestId("health-gauge")).toBeInTheDocument();
    });
  });

  it("displays the health score value", async () => {
    render(<SummaryView />);
    await waitFor(() => {
      expect(screen.getByText("52")).toBeInTheDocument();
    });
  });

  it("displays the health label", async () => {
    render(<SummaryView />);
    await waitFor(() => {
      expect(screen.getByText("Degraded")).toBeInTheDocument();
    });
  });

  it("renders overview cards", async () => {
    render(<SummaryView />);
    await waitFor(() => {
      expect(screen.getByTestId("overview-cards")).toBeInTheDocument();
    });
  });

  it("shows mesh count", async () => {
    render(<SummaryView />);
    await waitFor(() => {
      expect(screen.getByText("Meshes")).toBeInTheDocument();
    });
  });

  it("shows actor count", async () => {
    render(<SummaryView />);
    await waitFor(() => {
      expect(screen.getByText("Actors")).toBeInTheDocument();
    });
  });

  it("shows message count", async () => {
    render(<SummaryView />);
    await waitFor(() => {
      expect(screen.getByText("Messages")).toBeInTheDocument();
    });
  });

  it("renders status breakdown", async () => {
    render(<SummaryView />);
    await waitFor(() => {
      expect(screen.getByTestId("status-breakdown")).toBeInTheDocument();
    });
  });

  it("shows actor status labels", async () => {
    render(<SummaryView />);
    await waitFor(() => {
      expect(screen.getByText("Actor Status Breakdown")).toBeInTheDocument();
    });
  });

  it("renders error panel", async () => {
    render(<SummaryView />);
    await waitFor(() => {
      expect(screen.getByTestId("error-panel")).toBeInTheDocument();
    });
  });

  it("shows failed actor reason", async () => {
    render(<SummaryView />);
    await waitFor(() => {
      expect(screen.getByText("CUDA OOM")).toBeInTheDocument();
    });
  });

  it("shows stopped actors heading", async () => {
    render(<SummaryView />);
    await waitFor(() => {
      expect(screen.getByText("Stopped Actors")).toBeInTheDocument();
    });
  });

  it("shows failed messages count", async () => {
    render(<SummaryView />);
    await waitFor(() => {
      expect(screen.getByText("5 messages failed delivery")).toBeInTheDocument();
    });
  });

  it("renders message traffic section", async () => {
    render(<SummaryView />);
    await waitFor(() => {
      expect(screen.getByTestId("message-traffic")).toBeInTheDocument();
    });
  });

  it("shows delivery rate", async () => {
    render(<SummaryView />);
    await waitFor(() => {
      expect(screen.getByText("93.9% delivery rate")).toBeInTheDocument();
    });
  });

  it("shows endpoint names", async () => {
    render(<SummaryView />);
    await waitFor(() => {
      expect(screen.getByText("train_step")).toBeInTheDocument();
      expect(screen.getByText("checkpoint")).toBeInTheDocument();
    });
  });

  it("renders timeline bar", async () => {
    render(<SummaryView />);
    await waitFor(() => {
      expect(screen.getByTestId("timeline-bar")).toBeInTheDocument();
    });
  });

  it("shows timeline event counts", async () => {
    render(<SummaryView />);
    await waitFor(() => {
      expect(screen.getByText("207 status events")).toBeInTheDocument();
      expect(screen.getByText("246 message events")).toBeInTheDocument();
    });
  });

  it("renders mesh breakdown", async () => {
    render(<SummaryView />);
    await waitFor(() => {
      expect(screen.getByTestId("mesh-breakdown")).toBeInTheDocument();
    });
  });

  it("shows mesh class names", async () => {
    render(<SummaryView />);
    await waitFor(() => {
      expect(screen.getByText("Host")).toBeInTheDocument();
      expect(screen.getByText("Proc")).toBeInTheDocument();
    });
  });

  it("shows error state on fetch failure", async () => {
    jest.restoreAllMocks();
    jest.spyOn(global, "fetch").mockImplementation(() =>
      Promise.resolve({
        ok: false,
        status: 500,
        json: async () => ({}),
      } as Response)
    );
    render(<SummaryView />);
    await waitFor(() => {
      expect(screen.getByText(/Failed to load summary/)).toBeInTheDocument();
    });
  });
});
