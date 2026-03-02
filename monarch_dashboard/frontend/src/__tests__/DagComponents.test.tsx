/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React from "react";
import { render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";
import { DagLegend } from "../components/DagLegend";
import { DagNodeComponent } from "../components/DagNode";
import { DagEdgeComponent } from "../components/DagEdge";
import { DagNode, DagEdge } from "../utils/dagLayout";

describe("DagLegend", () => {
  it("renders the legend", () => {
    render(<DagLegend />);
    expect(screen.getByTestId("dag-legend")).toBeInTheDocument();
    expect(screen.getByText("Legend")).toBeInTheDocument();
  });

  it("shows all status labels", () => {
    render(<DagLegend />);
    expect(screen.getByText("Idle")).toBeInTheDocument();
    expect(screen.getByText("Processing")).toBeInTheDocument();
    expect(screen.getByText("Failed")).toBeInTheDocument();
    expect(screen.getByText("Stopped")).toBeInTheDocument();
    expect(screen.getByText("Transitional")).toBeInTheDocument();
  });

  it("shows node type labels", () => {
    render(<DagLegend />);
    expect(screen.getByText("Host Mesh")).toBeInTheDocument();
    expect(screen.getByText("Proc Mesh")).toBeInTheDocument();
    expect(screen.getByText("Actor Mesh")).toBeInTheDocument();
    expect(screen.getByText("Actor")).toBeInTheDocument();
  });

  it("shows edge type labels", () => {
    render(<DagLegend />);
    expect(screen.getByText("Hierarchy")).toBeInTheDocument();
    expect(screen.getByText("Message")).toBeInTheDocument();
  });
});

const sampleNode: DagNode = {
  id: "mesh-1",
  label: "host_mesh_0",
  subtitle: "Host",
  x: 120,
  y: 200,
  radius: 40,
  tier: "host",
  status: "idle",
  entityId: 1,
  meshClass: "Host",
};

describe("DagNodeComponent", () => {
  const renderInSvg = (ui: React.ReactElement) =>
    render(<svg>{ui}</svg>);

  it("renders the node with correct test id", () => {
    renderInSvg(
      <DagNodeComponent
        node={sampleNode}
        selected={false}
        onSelect={jest.fn()}
        onHover={jest.fn()}
      />
    );
    expect(screen.getByTestId("dag-node-mesh-1")).toBeInTheDocument();
  });

  it("renders the node label", () => {
    renderInSvg(
      <DagNodeComponent
        node={sampleNode}
        selected={false}
        onSelect={jest.fn()}
        onHover={jest.fn()}
      />
    );
    expect(screen.getByText("host_mesh_0")).toBeInTheDocument();
  });

  it("calls onSelect when clicked", () => {
    const onSelect = jest.fn();
    renderInSvg(
      <DagNodeComponent
        node={sampleNode}
        selected={false}
        onSelect={onSelect}
        onHover={jest.fn()}
      />
    );
    screen.getByTestId("dag-node-mesh-1").dispatchEvent(
      new MouseEvent("click", { bubbles: true })
    );
    expect(onSelect).toHaveBeenCalledWith(sampleNode);
  });

  it("renders glow ring when selected", () => {
    const { container } = renderInSvg(
      <DagNodeComponent
        node={sampleNode}
        selected={true}
        onSelect={jest.fn()}
        onHover={jest.fn()}
      />
    );
    const glowCircle = container.querySelector(".dag-node-glow");
    expect(glowCircle).toBeInTheDocument();
  });

  it("does not render glow ring when not selected", () => {
    const { container } = renderInSvg(
      <DagNodeComponent
        node={sampleNode}
        selected={false}
        onSelect={jest.fn()}
        onHover={jest.fn()}
      />
    );
    const glowCircle = container.querySelector(".dag-node-glow");
    expect(glowCircle).not.toBeInTheDocument();
  });

  it("truncates long labels", () => {
    const longNode = { ...sampleNode, label: "very_long_host_mesh_name_here" };
    renderInSvg(
      <DagNodeComponent
        node={longNode}
        selected={false}
        onSelect={jest.fn()}
        onHover={jest.fn()}
      />
    );
    const text = screen.getByText(/very_long_hos/);
    expect(text.textContent).toContain("\u2026");
  });
});

describe("DagEdgeComponent", () => {
  const nodeA: DagNode = { ...sampleNode, id: "actor-1", x: 100, y: 100, radius: 20 };
  const nodeB: DagNode = { ...sampleNode, id: "actor-2", x: 300, y: 200, radius: 20 };
  const nodeMap = new Map([
    [nodeA.id, nodeA],
    [nodeB.id, nodeB],
  ]);

  const renderInSvg = (ui: React.ReactElement) =>
    render(<svg>{ui}</svg>);

  it("renders a hierarchy edge", () => {
    const edge: DagEdge = {
      id: "hier-1-2",
      sourceId: "actor-1",
      targetId: "actor-2",
      type: "hierarchy",
    };
    renderInSvg(<DagEdgeComponent edge={edge} nodes={nodeMap} />);
    expect(screen.getByTestId("dag-edge-hier-1-2")).toBeInTheDocument();
  });

  it("renders a message edge with dashed style", () => {
    const edge: DagEdge = {
      id: "msg-1-2",
      sourceId: "actor-1",
      targetId: "actor-2",
      type: "message",
    };
    renderInSvg(<DagEdgeComponent edge={edge} nodes={nodeMap} />);
    const path = screen.getByTestId("dag-edge-msg-1-2");
    expect(path).toBeInTheDocument();
    expect(path.getAttribute("stroke-dasharray")).toBe("5 4");
  });

  it("returns null for missing nodes", () => {
    const edge: DagEdge = {
      id: "bad-edge",
      sourceId: "nonexistent",
      targetId: "actor-2",
      type: "hierarchy",
    };
    const { container } = renderInSvg(
      <DagEdgeComponent edge={edge} nodes={nodeMap} />
    );
    expect(container.querySelector("path")).toBeNull();
  });
});
