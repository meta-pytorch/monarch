/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React, { useState, useCallback } from "react";
import { Header } from "./components/Header";
import { Breadcrumb } from "./components/Breadcrumb";
import { MeshTable } from "./components/MeshTable";
import { ActorTable } from "./components/ActorTable";
import { ActorDetail } from "./components/ActorDetail";
import { DagView } from "./components/DagView";
import { NavItem, Mesh, Actor } from "./types";
import "./App.css";

const TABS = [
  { id: "hierarchy", label: "Hierarchy" },
  { id: "dag", label: "DAG" },
];

const HOST_COLUMNS = [
  { key: "given_name", label: "Name" },
  { key: "full_name", label: "Full Name" },
  { key: "shape_json", label: "Shape" },
  { key: "children", label: "Children" },
  { key: "status", label: "Status" },
  { key: "timestamp_us", label: "Created" },
];

const CHILD_MESH_COLUMNS = [
  { key: "given_name", label: "Name" },
  { key: "class", label: "Class" },
  { key: "shape_json", label: "Shape" },
  { key: "children", label: "Children" },
  { key: "status", label: "Status" },
];

function App() {
  const [activeTab, setActiveTab] = useState("hierarchy");
  const [navStack, setNavStack] = useState<NavItem[]>([
    { label: "Host Meshes", level: "hosts" },
  ]);

  const currentNav = navStack[navStack.length - 1];

  const pushNav = useCallback(
    (item: NavItem) => setNavStack((prev) => [...prev, item]),
    []
  );

  const navigateTo = useCallback(
    (index: number) => setNavStack((prev) => prev.slice(0, index + 1)),
    []
  );

  const handleHostClick = useCallback(
    (mesh: Mesh) => {
      pushNav({
        label: mesh.given_name,
        level: "procs",
        meshId: mesh.id,
      });
    },
    [pushNav]
  );

  const handleProcClick = useCallback(
    (mesh: Mesh) => {
      pushNav({
        label: mesh.given_name,
        level: "actor_meshes",
        meshId: mesh.id,
      });
    },
    [pushNav]
  );

  const handleActorMeshClick = useCallback(
    (mesh: Mesh) => {
      pushNav({
        label: mesh.given_name,
        level: "actors",
        meshId: mesh.id,
      });
    },
    [pushNav]
  );

  const handleActorClick = useCallback(
    (actor: Actor) => {
      pushNav({
        label: actor.full_name.split("/").pop() ?? `Actor #${actor.id}`,
        level: "actor_detail",
        actorId: actor.id,
      });
    },
    [pushNav]
  );

  const handleTabChange = useCallback((id: string) => {
    setActiveTab(id);
    setNavStack([{ label: "Host Meshes", level: "hosts" }]);
  }, []);

  const renderHierarchyView = () => {
    switch (currentNav.level) {
      case "hosts":
        return (
          <MeshTable
            apiPath="/meshes?class=Host"
            columns={HOST_COLUMNS}
            onRowClick={handleHostClick}
            title="Host Meshes"
          />
        );
      case "procs":
        return (
          <MeshTable
            apiPath={`/meshes/${currentNav.meshId}/children`}
            columns={CHILD_MESH_COLUMNS}
            onRowClick={handleProcClick}
            title="Proc Meshes"
          />
        );
      case "actor_meshes":
        return (
          <MeshTable
            apiPath={`/meshes/${currentNav.meshId}/children`}
            columns={CHILD_MESH_COLUMNS}
            onRowClick={handleActorMeshClick}
            title="Actor Meshes"
          />
        );
      case "actors":
        return (
          <ActorTable
            meshId={currentNav.meshId!}
            onActorClick={handleActorClick}
          />
        );
      case "actor_detail":
        return <ActorDetail actorId={currentNav.actorId!} />;
      default:
        return null;
    }
  };

  return (
    <div className="app">
      <Header tabs={TABS} activeTab={activeTab} onTabChange={handleTabChange} />
      <main className="main-content">
        {activeTab === "hierarchy" && (
          <>
            <Breadcrumb items={navStack} onNavigate={navigateTo} />
            <div className="view-container fade-in">
              {renderHierarchyView()}
            </div>
          </>
        )}
        {activeTab === "dag" && <DagView />}
      </main>
    </div>
  );
}

export default App;
