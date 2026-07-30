/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  Background,
  BackgroundVariant,
  Controls,
  Edge,
  MiniMap,
  Node,
  ReactFlow,
  ReactFlowProvider,
  useReactFlow,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { useApi } from "../../lib/useApi";
import { MessageStatsPayload } from "../../lib/useMessageStats";
import { Actor, ApiDagData, ApiDagEdge, ApiDagNode, DagTier, EntityId } from "../../types";
import { statusMeta } from "../../lib/status";
import { cleanNodeLabel, leafName } from "../../lib/format";
import { meshColor, nodeMesh } from "../../lib/mesh";
import { Drawer, Loading } from "../common/ui";
import { ActorDetail } from "../hierarchy/ActorDetail";
import { MonarchNode } from "./MonarchNode";
import { MessageEdge } from "./MessageEdge";
import {
  buildTree,
  countDescendants,
  Direction,
  layoutGraph,
  visibleSet,
} from "./layout";
import { collapseToMeshes } from "./meshView";
import {
  IconCollapse,
  IconDirection,
  IconExpand,
  IconEye,
  IconFit,
  IconLayers,
} from "../common/icons";

const nodeTypes = { monarch: MonarchNode };
const edgeTypes = { message: MessageEdge };

const MESH_TIERS = new Set<DagTier>(["host_mesh", "proc_mesh", "actor_mesh"]);
const MESH_UNIT: Partial<Record<DagTier, [string, string]>> = {
  host_mesh: ["host", "hosts"],
  proc_mesh: ["proc", "procs"],
  actor_mesh: ["actor", "actors"],
};

type ViewMode = "actors" | "meshes";

// Generous padding so the mesh count pills (which sit above a node's top edge)
// and the message arcs (which bow below the bottom row) aren't clipped when the
// graph is framed. maxZoom is capped so small graphs don't balloon.
const FIT_OPTS = { padding: 0.34, maxZoom: 1.1, duration: 300 } as const;

const TIER_LABEL: Partial<Record<DagTier, string>> = {
  host_mesh: "Host Mesh",
  host_unit: "Host Unit",
  proc_mesh: "Proc Mesh",
  proc_unit: "Proc Unit",
  actor_mesh: "Actor Mesh",
  actor: "Actor",
  host: "Host",
  proc: "Proc",
};

interface TipState {
  x: number;
  y: number;
  title: string;
  tier: string;
  status: string;
  mesh?: string;
  entityId: string;
  hidden: number;
  members?: number;
  isController: boolean;
}

export function TopologyView() {
  return (
    <ReactFlowProvider>
      <Flow />
    </ReactFlowProvider>
  );
}

function Flow() {
  const [hideSystem, setHideSystem] = useState(true);
  const [viewMode, setViewMode] = useState<ViewMode>("actors");
  const { data: rawData, loading } = useApi<ApiDagData>(`/dag?hide_system=${hideSystem}`, 5000);
  const { data: allActors } = useApi<Actor[]>("/actors", 6000);
  // Distinct actor pairs for the message overlay, from the shared cached
  // aggregate (coalesced with the overview's message-stats poll).
  const { data: msgStats } = useApi<MessageStatsPayload>("/message-stats", 5000);
  // Mesh view folds the per-entity DAG into one node per host/proc/actor mesh.
  const collapsed = useMemo(() => (rawData ? collapseToMeshes(rawData) : null), [rawData]);
  const data = viewMode === "meshes" ? collapsed?.data ?? null : rawData;
  const [direction, setDirection] = useState<Direction>("TB");
  const [autoFit, setAutoFit] = useState(true);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [hoverId, setHoverId] = useState<string | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [drawerActor, setDrawerActor] = useState<EntityId | null>(null);
  const [tip, setTip] = useState<TipState | null>(null);
  const [msgPairs, setMsgPairs] = useState<Array<[string, string]>>([]);
  const [meshLegend, setMeshLegend] = useState<Array<{ key: string; label: string; kind: string; color: string }>>([]);

  const [baseNodes, setBaseNodes] = useState<Node[]>([]);
  const [baseEdges, setBaseEdges] = useState<Edge[]>([]);
  const { fitView } = useReactFlow();
  const initExpanded = useRef(false);
  // Signature of the last framed layout (direction + visible node set). Auto-fit
  // only fires when this changes — never on the periodic data refresh — so a
  // manual zoom/pan isn't reset every poll.
  const lastFitSig = useRef("");

  const tree = useMemo(() => (data ? buildTree(data) : null), [data]);
  const nodeById = useMemo(() => {
    const m = new Map<string, ApiDagNode>();
    data?.nodes.forEach((n) => m.set(n.id, n));
    return m;
  }, [data]);

  // child -> parent, and telemetry_actor_id -> node id (for message overlay).
  const parentOf = useMemo(() => {
    const m = new Map<string, string>();
    data?.edges.forEach((e) => {
      if (e.type === "hierarchy") m.set(e.target_id, e.source_id);
    });
    return m;
  }, [data]);
  // actor id -> full_name (telemetry actors table, via /api/actors).
  const id2name = useMemo(() => {
    const m = new Map<string, string>();
    (allActors ?? []).forEach((a) => m.set(String(a.id), a.full_name));
    return m;
  }, [allActors]);

  // Monarch routes messages through per-actor "mailbox" pseudo-actors that
  // aren't real actor rows. Splice those single hops out so a
  // sender→mailbox→receiver chain becomes a direct sender→receiver edge.
  const splicedPairs = useMemo(() => {
    const mailIn = new Map<string, Set<string>>(); // mailbox -> real names it forwards to
    const mailOut = new Map<string, Set<string>>(); // mailbox -> real names feeding it
    const direct = new Set<string>();
    const add = (m: Map<string, Set<string>>, k: string, v: string) =>
      (m.get(k) ?? m.set(k, new Set()).get(k)!).add(v);
    for (const [f, t] of msgPairs) {
      const fn = id2name.get(f);
      const tn = id2name.get(t);
      if (fn && tn) direct.add(`${fn}\u0000${tn}`);
      else if (fn && !tn) add(mailOut, t, fn);
      else if (tn && !fn) add(mailIn, f, tn);
    }
    const out = new Set<string>(direct);
    const mailboxes = new Set<string>([...mailIn.keys(), ...mailOut.keys()]);
    for (const mb of mailboxes) {
      for (const x of mailOut.get(mb) ?? []) {
        for (const y of mailIn.get(mb) ?? []) {
          if (x !== y) out.add(`${x}\u0000${y}`);
        }
      }
    }
    return [...out].map((s) => s.split("\u0000") as [string, string]);
  }, [msgPairs, id2name]);

  // The admin snapshot DAG carries no message edges; overlay them from the
  // cached /message-stats aggregate (distinct actor pairs). Reading the shared
  // cached endpoint — rather than posting a raw per-poll scan of `messages` —
  // keeps the dashboard from inflating the telemetry tables it observes.
  useEffect(() => {
    setMsgPairs(
      (msgStats?.pairs ?? []).map((p) => [String(p[0]), String(p[1])] as [string, string])
    );
  }, [msgStats]);

  useEffect(() => {
    if (tree && !initExpanded.current) {
      initExpanded.current = true;
      setExpanded(new Set(tree.roots));
    }
  }, [tree]);

  useEffect(() => {
    if (!data || !tree) return;
    let cancelled = false;
    const vis = visibleSet(tree.roots, tree.children, expanded);
    const fitSig = `${direction}|${[...vis].sort().join(",")}`;
    const apiNodes = data.nodes.filter((n) => vis.has(n.id));

    const hierEdges = data.edges.filter(
      (e) => e.type === "hierarchy" && vis.has(e.source_id) && vis.has(e.target_id)
    );

    // Map actor full_name -> DAG node (the DAG actor entity_id IS the name),
    // then roll each message up to the nearest visible ancestor so flow is
    // visible at any expansion level. In mesh view the DAG nodes are meshes, so
    // use the collapse's original-entity -> mesh-node map instead.
    const ent2node =
      viewMode === "meshes" && collapsed
        ? collapsed.ent2node
        : new Map<string, string>(data.nodes.map((n) => [String(n.entity_id), n.id]));
    const nearestVisible = (id: string): string | null => {
      let cur: string | undefined = id;
      while (cur && !vis.has(cur)) cur = parentOf.get(cur);
      return cur ?? null;
    };
    // Dedupe undirected: a pair that messages both ways is bidirectional, so
    // single arc per pair rather than two overlapping ones.
    const msgEdges: ApiDagEdge[] = [];
    const seen = new Set<string>();
    for (const [xName, yName] of splicedPairs) {
      const sn = ent2node.get(xName);
      const tn = ent2node.get(yName);
      if (!sn || !tn) continue;
      const va = nearestVisible(sn);
      const vb = nearestVisible(tn);
      if (!va || !vb || va === vb) continue;
      const key = [va, vb].sort().join("::");
      if (seen.has(key)) continue;
      seen.add(key);
      msgEdges.push({ id: `msg-${key}`, source_id: va, target_id: vb, type: "message" });
    }

    const combinedEdges = [...hierEdges, ...msgEdges];

    // Collect the distinct meshes present in the visible graph for the legend.
    const meshSeen = new Map<string, { key: string; label: string; kind: string; color: string }>();

    const makeData = (n: ApiDagNode) => {
      const kids = tree.children[n.id] ?? [];
      const isMeshTier = MESH_TIERS.has(n.tier);
      const isController =
        !isMeshTier &&
        (/\(controller\)/i.test(n.label) || (n.tier === "host" && n.label.trim().startsWith("@")));
      let label = isMeshTier ? n.label : cleanNodeLabel(n.label, n.tier);
      if (n.tier === "host" && isController) label = "Controller";
      const mesh = isMeshTier ? null : nodeMesh(n.tier, label, n.mesh_name);
      const mColor = mesh ? meshColor(mesh.key) : undefined;
      if (mesh && !meshSeen.has(mesh.key)) {
        meshSeen.set(mesh.key, { key: mesh.key, label: mesh.label, kind: mesh.kind, color: mColor! });
      }
      const unit = MESH_UNIT[n.tier];
      return {
        tier: n.tier,
        label,
        subtitle: TIER_LABEL[n.tier] ?? n.subtitle,
        status: n.status,
        isController,
        meshLabel: mesh?.label,
        meshColor: mColor,
        count: isMeshTier ? n.memberCount : undefined,
        countUnit: isMeshTier && unit ? (n.memberCount === 1 ? unit[0] : unit[1]) : undefined,
        hasChildren: kids.length > 0,
        expanded: expanded.has(n.id),
        hiddenCount: expanded.has(n.id) ? 0 : countDescendants(n.id, tree.children),
        dimmed: false,
        selected: false,
      };
    };

    layoutGraph(apiNodes, combinedEdges, direction, makeData).then((res) => {
      if (cancelled) return;
      setBaseNodes(res.nodes);
      setBaseEdges(res.edges);
      setMeshLegend([...meshSeen.values()]);
      // Only re-frame on a structural change (and only if auto-fit is on);
      // periodic status refreshes keep the same signature and never re-fit.
      if (autoFit && fitSig !== lastFitSig.current) {
        setTimeout(() => fitView(FIT_OPTS), 40);
      }
      lastFitSig.current = fitSig;
    });
    return () => {
      cancelled = true;
    };
  }, [data, tree, expanded, direction, autoFit, fitView, splicedPairs, parentOf, viewMode, collapsed]);

  const neighbors = useMemo(() => {
    const m = new Map<string, Set<string>>();
    for (const e of baseEdges) {
      (m.get(e.source) ?? m.set(e.source, new Set()).get(e.source)!).add(e.target);
      (m.get(e.target) ?? m.set(e.target, new Set()).get(e.target)!).add(e.source);
    }
    return m;
  }, [baseEdges]);

  const displayNodes = useMemo(() => {
    return baseNodes.map((n) => {
      const dimmed =
        hoverId != null && n.id !== hoverId && !(neighbors.get(hoverId)?.has(n.id) ?? false);
      return { ...n, data: { ...n.data, dimmed, selected: n.id === selectedId } };
    });
  }, [baseNodes, hoverId, selectedId, neighbors]);

  const displayEdges = useMemo(() => {
    if (hoverId == null) return baseEdges;
    return baseEdges.map((e) => {
      const on = e.source === hoverId || e.target === hoverId;
      return { ...e, style: { ...e.style, opacity: on ? 1 : 0.08 } };
    });
  }, [baseEdges, hoverId]);

  const showTip = useCallback(
    (evt: React.MouseEvent, node: Node) => {
      setHoverId(node.id);
      const api = nodeById.get(node.id);
      if (!api || !tree) return;
      const d = node.data as { label: string; subtitle: string; isController: boolean };
      setTip({
        x: evt.clientX,
        y: evt.clientY,
        title: d.label,
        tier: d.subtitle,
        status: api.status,
        mesh: api.mesh_name,
        entityId: String(api.entity_id),
        hidden: expanded.has(node.id) ? 0 : countDescendants(node.id, tree.children),
        members: api.memberCount,
        isController: d.isController,
      });
    },
    [nodeById, tree, expanded]
  );

  const onNodeClick = useCallback(
    (_: React.MouseEvent, node: Node) => {
      const api = nodeById.get(node.id);
      if (!api) return;
      const kids = tree?.children[node.id] ?? [];
      if (kids.length > 0) {
        setExpanded((prev) => {
          const next = new Set(prev);
          next.has(node.id) ? next.delete(node.id) : next.add(node.id);
          return next;
        });
        setSelectedId(node.id);
      } else if (api.tier === "actor") {
        setSelectedId(node.id);
        setDrawerActor(String(api.telemetry_actor_id ?? api.entity_id));
      } else {
        setSelectedId(node.id);
      }
    },
    [nodeById, tree]
  );

  const expandAll = useCallback(() => {
    if (!data || !tree) return;
    const all = new Set<string>();
    for (const n of data.nodes) if ((tree.children[n.id] ?? []).length) all.add(n.id);
    setExpanded(all);
  }, [data, tree]);

  const collapseAll = useCallback(() => {
    if (tree) setExpanded(new Set(tree.roots));
  }, [tree]);

  const toggleSystem = useCallback(() => {
    initExpanded.current = false;
    setExpanded(new Set());
    setHideSystem((h) => !h);
  }, []);

  // Node ids differ between the two views, so reset expansion when switching.
  const toggleView = useCallback(() => {
    initExpanded.current = false;
    setExpanded(new Set());
    setSelectedId(null);
    setViewMode((m) => (m === "actors" ? "meshes" : "actors"));
  }, []);

  const minimapColor = useCallback((n: Node) => {
    const d = n.data as { status?: string; isController?: boolean };
    return d.isController ? "#6b8afd" : statusMeta(d.status ?? "n/a").color;
  }, []);

  if (loading && baseNodes.length === 0) {
    return <div className="topo"><Loading label="Building topology…" /></div>;
  }
  if (data && data.nodes.length === 0) {
    const msg = data.snapshot_pending
      ? "Topology view will be available 30 seconds after the job is fully started and running."
      : "No topology data available";
    return <div className="topo"><div className="state">{msg}</div></div>;
  }

  const msgEdgeCount = baseEdges.filter(
    (e) => (e.data as { kind?: string } | undefined)?.kind === "message"
  ).length;

  return (
    <div className="topo">
      <div className="topo-canvas">
      <div className="topo-toolbar">
        <button
          className={`btn${viewMode === "meshes" ? " active" : ""}`}
          onClick={toggleView}
          title="Switch the primary entity between Actors (one node per actor, mesh tagged) and Meshes (host / proc / actor meshes each collapsed to a single node). Mesh view keeps large jobs — hundreds of actors — legible."
        >
          <IconLayers /> {viewMode === "meshes" ? "Meshes" : "Actors"}
        </button>
        <button className="btn" onClick={() => setDirection((d) => (d === "TB" ? "LR" : "TB"))} title="Toggle layout direction">
          <IconDirection style={{ transform: direction === "LR" ? "rotate(-90deg)" : "none" }} />
          {direction === "TB" ? "Top-Down" : "Left-Right"}
        </button>
        <button className="btn" onClick={() => fitView(FIT_OPTS)} title="Frame the whole graph in view">
          <IconFit /> Fit
        </button>
        <button
          className={`btn${autoFit ? " active" : ""}`}
          onClick={() => setAutoFit((a) => !a)}
          title="Auto-fit re-frames the graph when you expand/collapse nodes or switch layout direction. It never re-fits on the periodic live data refresh. Turn this off to fully lock your manual zoom & pan."
        >
          <IconFit /> Auto-fit: {autoFit ? "On" : "Off"}
        </button>
        <button className="btn" onClick={expandAll}><IconExpand /> Expand All</button>
        <button className="btn" onClick={collapseAll}><IconCollapse /> Collapse</button>
        <button className={`btn${hideSystem ? " active" : ""}`} onClick={toggleSystem}>
          <IconEye /> {hideSystem ? "Show System" : "Hide System"}
        </button>
      </div>

      <ReactFlow
        nodes={displayNodes}
        edges={displayEdges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        onNodeClick={onNodeClick}
        onNodeMouseEnter={showTip}
        onNodeMouseMove={(e, n) => tip && showTip(e, n)}
        onNodeMouseLeave={() => { setHoverId(null); setTip(null); }}
        onPaneClick={() => setSelectedId(null)}
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable
        minZoom={0.15}
        maxZoom={2.5}
        proOptions={{ hideAttribution: true }}
        defaultEdgeOptions={{ interactionWidth: 0 }}
      >
        <Background variant={BackgroundVariant.Dots} gap={22} size={1} color="#232936" />
        <Controls showInteractive={false} />
        <MiniMap pannable zoomable nodeColor={minimapColor} nodeStrokeWidth={0} maskColor="rgba(10,12,16,0.7)"
          style={{ background: "#12151c", border: "1px solid #232936", borderRadius: 8 }} />
      </ReactFlow>

      {tip && (
        <div className="topo-tip" style={{ left: tip.x + 14, top: tip.y + 14 }}>
          <div className="tt-title">{tip.title}</div>
          <div className="tt-row"><span>Type</span><b>{tip.isController ? `Controller · ${tip.tier}` : tip.tier}</b></div>
          <div className="tt-row"><span>Status</span><b style={{ color: statusMeta(tip.status).color }}>{tip.status}</b></div>
          {typeof tip.members === "number" ? (
            <div className="tt-row"><span>Members</span><b>{tip.members}</b></div>
          ) : (
            <>
              {tip.mesh && <div className="tt-row"><span>Mesh</span><b>{tip.mesh}</b></div>}
              <div className="tt-row"><span>Ref</span><b className="mono tt-ref">{leafName(tip.entityId)}</b></div>
            </>
          )}
          {tip.hidden > 0 && <div className="tt-hint">click to expand · {tip.hidden} hidden</div>}
        </div>
      )}

      </div>
      <Legend msgEdges={msgEdgeCount} meshes={meshLegend} />

      {drawerActor && (
        <Drawer title="Actor Detail" subtitle={`#${drawerActor}`} onClose={() => setDrawerActor(null)}>
          <ActorDetail actorId={drawerActor} compact />
        </Drawer>
      )}
    </div>
  );
}

function Legend({
  msgEdges,
  meshes,
}: {
  msgEdges: number;
  meshes: Array<{ key: string; label: string; kind: string; color: string }>;
}) {
  const [open, setOpen] = useState(true);
  const statuses = ["idle", "processing", "failed", "stopped", "stopping", "unknown"];
  const kinds: Array<[string, string]> = [
    ["host", "Host"],
    ["proc", "Proc"],
    ["actor", "Actor"],
  ];
  return (
    <div className="topo-legend-bar">
      <button className="topo-legend-toggle" onClick={() => setOpen((o) => !o)}>
        {open ? "▾ Legend" : "▸ Legend"}
      </button>
      {open && (
        <div className="topo-legend-content">
          <div className="lg-group">
            <span className="lg-title">Status</span>
            {statuses.map((s) => (
              <span key={s} className="legend-item">
                <span className="dot-only" style={{ background: statusMeta(s).color }} />
                {s}
              </span>
            ))}
            <span className="legend-item"><span className="dot-only" style={{ background: "#6b8afd" }} />controller</span>
          </div>

          {meshes.length > 0 && (
            <div className="lg-group">
              <span className="lg-title">Meshes</span>
              {kinds.map(([kind, label]) => {
                const ms = meshes.filter((m) => m.kind === kind);
                if (ms.length === 0) return null;
                return (
                  <React.Fragment key={kind}>
                    <span className="lg-kind">{label}:</span>
                    {ms.map((m) => (
                      <span key={m.key} className="legend-item">
                        <span className="dot-only" style={{ background: m.color, borderRadius: 2 }} />
                        {m.label}
                      </span>
                    ))}
                  </React.Fragment>
                );
              })}
            </div>
          )}

          <div className="lg-group">
            <span className="lg-title">Edges</span>
            <span className="legend-item"><svg width="20" height="8"><line x1="1" y1="4" x2="19" y2="4" stroke="#3a4152" strokeWidth="1.6" /></svg>hierarchy</span>
            <span className="legend-item"><svg width="20" height="8"><line x1="1" y1="4" x2="19" y2="4" stroke="var(--accent-2)" strokeWidth="1.6" strokeDasharray="4 3" /></svg>message{msgEdges > 0 ? ` (${msgEdges})` : ""}</span>
          </div>
        </div>
      )}
    </div>
  );
}
