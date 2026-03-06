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
import { ActorDetail } from "./components/ActorDetail";
import { DagView } from "./components/DagView";
import { SummaryView } from "./components/SummaryView";
import { NavItem } from "./types";
import "./App.css";

const TABS = [
  { id: "summary", label: "Summary" },
  { id: "hierarchy", label: "Hierarchy" },
  { id: "dag", label: "DAG" },
];

const MESH_COLUMNS = [
  { key: "given_name", label: "Name" },
  { key: "shape_json", label: "Shape" },
  { key: "full_name", label: "Full Name" },
];

const AGENT_COLUMNS = [
  { key: "full_name", label: "Name" },
  { key: "mesh_class", label: "Class" },
  { key: "rank", label: "Rank" },
  { key: "latest_status", label: "Status" },
];

const ACTOR_COLUMNS = [
  { key: "full_name", label: "Name" },
  { key: "rank", label: "Rank" },
  { key: "latest_status", label: "Status" },
  { key: "status_timestamp_us", label: "Last Updated" },
];

/** Navigation graph: for each level, what comes next and how to label it. */
const LEVELS: Partial<Record<NavItem["level"], {
  next: NavItem["level"];
  label: (row: any) => string;
  idField: "meshId" | "actorId";
  idKey: string;
}>> = {
  host_meshes:  { next: "host_units",   label: (r) => r.given_name,                              idField: "meshId",  idKey: "id" },
  host_units:   { next: "proc_meshes",  label: (r) => r.full_name?.split("/").pop() ?? "Host",   idField: "meshId",  idKey: "mesh_id" },
  proc_meshes:  { next: "proc_units",   label: (r) => r.given_name,                              idField: "meshId",  idKey: "id" },
  proc_units:   { next: "actor_meshes", label: (r) => r.full_name?.split("/").pop() ?? "Proc",   idField: "meshId",  idKey: "mesh_id" },
  actor_meshes: { next: "actors",       label: (r) => r.given_name,                              idField: "meshId",  idKey: "id" },
  actors:       { next: "actor_detail", label: (r) => r.full_name?.split("/").pop() ?? "Actor",  idField: "actorId", idKey: "id" },
};

/** MeshTable config per hierarchy level. */
const LEVEL_CONFIG: Partial<Record<NavItem["level"], {
  apiPath: (n: NavItem) => string;
  columns: Array<{ key: string; label: string }>;
  title: string;
}>> = {
  host_meshes:  { apiPath: ()  => "/meshes?class=Host",            columns: MESH_COLUMNS,  title: "Host Meshes" },
  host_units:   { apiPath: (n) => `/actors?mesh_id=${n.meshId}`,   columns: AGENT_COLUMNS, title: "Host Units" },
  proc_meshes:  { apiPath: (n) => `/meshes/${n.meshId}/children`,  columns: MESH_COLUMNS,  title: "Proc Meshes" },
  proc_units:   { apiPath: (n) => `/actors?mesh_id=${n.meshId}`,   columns: AGENT_COLUMNS, title: "Proc Units" },
  actor_meshes: { apiPath: (n) => `/meshes/${n.meshId}/children`,  columns: MESH_COLUMNS,  title: "Actor Meshes" },
  actors:       { apiPath: (n) => `/actors?mesh_id=${n.meshId}`,   columns: ACTOR_COLUMNS, title: "Actors" },
};

function App() {
  const [activeTab, setActiveTab] = useState("summary");
  const [navStack, setNavStack] = useState<NavItem[]>([
    { label: "Host Meshes", level: "host_meshes" },
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

  const handleRowClick = useCallback(
    (entity: any) => {
      const cfg = LEVELS[currentNav.level];
      if (!cfg) return;
      pushNav({
        label: cfg.label(entity),
        level: cfg.next,
        [cfg.idField]: entity[cfg.idKey],
      } as NavItem);
    },
    [currentNav.level, pushNav]
  );

  const handleTabChange = useCallback((id: string) => {
    setActiveTab(id);
    setNavStack([{ label: "Host Meshes", level: "host_meshes" }]);
  }, []);

  const renderHierarchyView = () => {
    const cfg = LEVEL_CONFIG[currentNav.level];
    if (cfg) {
      return (
        <MeshTable
          apiPath={cfg.apiPath(currentNav)}
          columns={cfg.columns}
          onRowClick={handleRowClick}
          title={cfg.title}
        />
      );
    }
    if (currentNav.level === "actor_detail") {
      return <ActorDetail actorId={currentNav.actorId!} />;
    }
    return null;
  };

  return (
    <div className="app">
      <Header tabs={TABS} activeTab={activeTab} onTabChange={handleTabChange} />
      <main className="main-content">
        {activeTab === "summary" && (
          <div className="view-container fade-in">
            <SummaryView />
          </div>
        )}
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
