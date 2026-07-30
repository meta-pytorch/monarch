/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React, { useEffect, useMemo, useState } from "react";
import { useApi } from "../../lib/useApi";
import { ApiDagData, ApiDagNode, DagTier } from "../../types";
import { buildTree, countDescendants } from "../topology/layout";
import { cleanNodeLabel } from "../../lib/format";
import { nodeMesh, meshColor } from "../../lib/mesh";
import { statusMeta } from "../../lib/status";
import { EmptyState, Loading, StatusPill } from "../common/ui";
import { ActorDetail } from "./ActorDetail";
import { PySpyPanel } from "./PySpyPanel";
import {
  IconActor,
  IconChevron,
  IconEye,
  IconHost,
  IconProc,
  IconSearch,
} from "../common/icons";

const TIER_LABEL: Record<string, string> = {
  host_mesh: "Host Mesh",
  host_unit: "Host Unit",
  proc_mesh: "Proc Mesh",
  proc_unit: "Proc Unit",
  actor_mesh: "Actor Mesh",
  actor: "Actor",
  host: "Host",
  proc: "Proc",
};

function tierGlyph(tier: DagTier, size = 14) {
  if (tier.startsWith("host")) return <IconHost size={size} />;
  if (tier.startsWith("proc")) return <IconProc size={size} />;
  return <IconActor size={size} />;
}

function isControllerNode(n: ApiDagNode): boolean {
  return /\(controller\)/i.test(n.label) || (n.tier === "host" && n.label.trim().startsWith("@"));
}

function nodeLabel(n: ApiDagNode): string {
  if (isControllerNode(n) && n.tier === "host") return "Controller";
  return cleanNodeLabel(n.label, n.tier);
}

export function HierarchyView() {
  const [hideSystem, setHideSystem] = useState(true);
  const { data, loading } = useApi<ApiDagData>(`/dag?hide_system=${hideSystem}`, 5000);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [filter, setFilter] = useState("");
  const initExpanded = React.useRef(false);

  const { children, roots } = useMemo(
    () => (data ? buildTree(data) : { children: {}, roots: [] as string[] }),
    [data]
  );
  const nodeById = useMemo(() => {
    const m = new Map<string, ApiDagNode>();
    data?.nodes.forEach((n) => m.set(n.id, n));
    return m;
  }, [data]);
  const parentOf = useMemo(() => {
    const m = new Map<string, string>();
    data?.edges.forEach((e) => {
      if (e.type === "hierarchy") m.set(e.target_id, e.source_id);
    });
    return m;
  }, [data]);

  // Expand the roots once (and their immediate children) so the tree opens on
  // something useful instead of a bare top level.
  useEffect(() => {
    if (roots.length && !initExpanded.current) {
      initExpanded.current = true;
      const init = new Set<string>(roots);
      for (const r of roots) for (const k of children[r] ?? []) init.add(k);
      setExpanded(init);
    }
  }, [roots, children]);

  // Re-seed expansion when the system toggle rebuilds the tree.
  const toggleSystem = () => {
    initExpanded.current = false;
    setExpanded(new Set());
    setSelectedId(null);
    setHideSystem((h) => !h);
  };

  const q = filter.trim().toLowerCase();
  // Filter: keep matches, their ancestors (so they're reachable) and their
  // descendants (so an opened folder still shows its contents). Ancestors of a
  // match are force-expanded so results are visible without manual drilling.
  const { keep, forceOpen } = useMemo(() => {
    if (!q || !data) return { keep: null as Set<string> | null, forceOpen: new Set<string>() };
    const matched = new Set<string>();
    for (const n of data.nodes) if (nodeLabel(n).toLowerCase().includes(q)) matched.add(n.id);
    const keep = new Set<string>(matched);
    const forceOpen = new Set<string>();
    for (const id of matched) {
      let cur = parentOf.get(id);
      while (cur) {
        keep.add(cur);
        forceOpen.add(cur);
        cur = parentOf.get(cur);
      }
    }
    const addDesc = (id: string) => {
      for (const k of children[id] ?? []) {
        keep.add(k);
        addDesc(k);
      }
    };
    for (const id of matched) addDesc(id);
    return { keep, forceOpen };
  }, [q, data, parentOf, children]);

  const effExpanded = q ? forceOpen : expanded;

  const toggle = (id: string) =>
    setExpanded((prev) => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });

  const onRowClick = (n: ApiDagNode) => {
    setSelectedId(n.id);
    if ((children[n.id] ?? []).length > 0 && !q) toggle(n.id);
  };

  const expandAll = () => setExpanded(new Set(data?.nodes.map((n) => n.id) ?? []));
  const collapseAll = () => setExpanded(new Set(roots));

  const sortKids = (ids: string[]) =>
    [...ids].sort((a, b) => nodeLabel(nodeById.get(a)!).localeCompare(nodeLabel(nodeById.get(b)!)));

  const rows: React.ReactNode[] = [];
  const walk = (id: string, depth: number) => {
    if (keep && !keep.has(id)) return;
    const n = nodeById.get(id);
    if (!n) return;
    const kids = children[id] ?? [];
    const open = effExpanded.has(id) && kids.length > 0;
    rows.push(
      <TreeRow
        key={id}
        node={n}
        depth={depth}
        hasChildren={kids.length > 0}
        childCount={kids.length}
        hiddenDesc={open ? 0 : countDescendants(id, children)}
        open={open}
        selected={selectedId === id}
        onToggle={() => toggle(id)}
        onSelect={() => onRowClick(n)}
      />
    );
    if (open) for (const k of sortKids(kids)) walk(k, depth + 1);
  };
  for (const r of sortKids(roots)) walk(r, 0);

  const selected = selectedId ? nodeById.get(selectedId) : null;

  return (
    <div className="explorer">
      <div className="panel explorer-tree">
        <div className="explorer-toolbar">
          <div className="tree-search">
            <IconSearch size={14} />
            <input
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              placeholder="Filter entities…"
              spellCheck={false}
            />
            {filter && (
              <button className="tree-search-clear" onClick={() => setFilter("")} aria-label="Clear">
                ×
              </button>
            )}
          </div>
          <button className="btn sm" onClick={expandAll} title="Expand all">Expand</button>
          <button className="btn sm" onClick={collapseAll} title="Collapse to roots">Collapse</button>
          <button
            className={`btn sm${hideSystem ? "" : " active"}`}
            onClick={toggleSystem}
            title={hideSystem ? "Show system actors" : "Hide system actors"}
          >
            <IconEye size={13} /> {hideSystem ? "Show System" : "Hide System"}
          </button>
        </div>
        <div className="tree-scroll">
          {loading && !data ? (
            <Loading label="Loading hierarchy…" />
          ) : rows.length === 0 ? (
            <EmptyState label={q ? "No matches" : "No entities"} />
          ) : (
            rows
          )}
        </div>
      </div>

      <div className="panel explorer-detail">
        {selected ? (
          <DetailPane node={selected} nodeById={nodeById} parentOf={parentOf} children={children} onSelect={setSelectedId} />
        ) : (
          <div className="detail-empty">
            <IconActor size={30} />
            <div>Select an entity to inspect it</div>
            <span>Browse the mesh hierarchy on the left — expand hosts, procs and actor meshes down to individual actors.</span>
          </div>
        )}
      </div>
    </div>
  );
}

function TreeRow({
  node,
  depth,
  hasChildren,
  childCount,
  hiddenDesc,
  open,
  selected,
  onToggle,
  onSelect,
}: {
  node: ApiDagNode;
  depth: number;
  hasChildren: boolean;
  childCount: number;
  hiddenDesc: number;
  open: boolean;
  selected: boolean;
  onToggle: () => void;
  onSelect: () => void;
}) {
  const isController = isControllerNode(node) && node.tier === "host";
  const color = isController ? "#6b8afd" : statusMeta(node.status).color;
  const mesh = nodeMesh(node.tier, nodeLabel(node), node.mesh_name);
  return (
    <div
      className={`tree-row${selected ? " sel" : ""}`}
      style={{ paddingLeft: 8 + depth * 16 }}
      onClick={onSelect}
      title={node.label}
    >
      {hasChildren ? (
        <button
          className={`tree-chevron${open ? " open" : ""}`}
          onClick={(e) => {
            e.stopPropagation();
            onToggle();
          }}
          aria-label={open ? "Collapse" : "Expand"}
        >
          <IconChevron size={13} />
        </button>
      ) : (
        <span className="tree-chevron spacer" />
      )}
      <span className="tree-ico" style={{ color }}>{tierGlyph(node.tier)}</span>
      <span className="tree-label">{nodeLabel(node)}</span>
      {mesh && <span className="tree-mesh" style={{ color: meshColor(mesh.key) }} title={`mesh: ${mesh.label}`}>{mesh.label}</span>}
      <span className="tree-spacer" />
      {hasChildren && !open && hiddenDesc > 0 && <span className="tree-count">{hiddenDesc}</span>}
      {hasChildren && open && <span className="tree-count subtle">{childCount}</span>}
      <span className="tree-dot" style={{ background: color }} title={node.status} />
    </div>
  );
}

function DetailPane({
  node,
  nodeById,
  parentOf,
  children,
  onSelect,
}: {
  node: ApiDagNode;
  nodeById: Map<string, ApiDagNode>;
  parentOf: Map<string, string>;
  children: Record<string, string[]>;
  onSelect: (id: string) => void;
}) {
  // Ancestry breadcrumb (root → … → node).
  const path: ApiDagNode[] = [];
  let cur: string | undefined = node.id;
  while (cur) {
    const n = nodeById.get(cur);
    if (n) path.unshift(n);
    cur = parentOf.get(cur);
  }

  return (
    <div className="detail-inner">
      <div className="detail-crumbs">
        {path.map((p, i) => (
          <React.Fragment key={p.id}>
            {i > 0 && <span className="sep">›</span>}
            {i === path.length - 1 ? (
              <span className="cur">{nodeLabel(p)}</span>
            ) : (
              <button onClick={() => onSelect(p.id)}>{nodeLabel(p)}</button>
            )}
          </React.Fragment>
        ))}
      </div>

      {node.tier === "actor" ? (
        <div className="detail-scroll">
          {(() => {
            // py-spy is proc-level: attach to the actor's owning proc.
            const proc = [...path].reverse().find((p) => p.tier.startsWith("proc"));
            return proc ? (
              <PySpyPanel procRef={String(proc.entity_id)} procLabel={nodeLabel(proc)} />
            ) : null;
          })()}
          <ActorDetail actorId={String(node.telemetry_actor_id ?? node.entity_id)} compact />
        </div>
      ) : (
        <ContainerDetail node={node} nodeById={nodeById} children={children} onSelect={onSelect} />
      )}
    </div>
  );
}

function ContainerDetail({
  node,
  nodeById,
  children,
  onSelect,
}: {
  node: ApiDagNode;
  nodeById: Map<string, ApiDagNode>;
  children: Record<string, string[]>;
  onSelect: (id: string) => void;
}) {
  const kids = children[node.id] ?? [];
  // Roll up descendant actors by current status for an at-a-glance breakdown.
  const { actorCount, byStatus, tierCounts } = useMemo(() => {
    const byStatus: Record<string, number> = {};
    const tierCounts: Record<string, number> = {};
    let actorCount = 0;
    const walk = (id: string) => {
      for (const k of children[id] ?? []) {
        const c = nodeById.get(k);
        if (!c) continue;
        tierCounts[c.tier] = (tierCounts[c.tier] ?? 0) + 1;
        if (c.tier === "actor") {
          actorCount++;
          const s = (c.status ?? "unknown").toLowerCase();
          byStatus[s] = (byStatus[s] ?? 0) + 1;
        }
        walk(k);
      }
    };
    walk(node.id);
    return { actorCount, byStatus, tierCounts };
  }, [node.id, children, nodeById]);

  const mesh = nodeMesh(node.tier, nodeLabel(node), node.mesh_name);
  const statusEntries = Object.entries(byStatus).sort((a, b) => b[1] - a[1]);

  return (
    <div className="detail-scroll">
      <section className="drawer-section">
        <div className="eyebrow">{TIER_LABEL[node.tier] ?? node.tier}</div>
        <dl className="meta-list">
          <div className="row"><dt>Name</dt><dd className="cell-mono">{nodeLabel(node)}</dd></div>
          {mesh && <div className="row"><dt>Mesh</dt><dd className="cell-mono">{mesh.label}</dd></div>}
          <div className="row"><dt>Status</dt><dd><StatusPill status={node.status} /></dd></div>
          <div className="row"><dt>Direct children</dt><dd className="cell-mono">{kids.length}</dd></div>
          {actorCount > 0 && <div className="row"><dt>Actors below</dt><dd className="cell-mono">{actorCount}</dd></div>}
          <div className="row"><dt>Ref</dt><dd className="cell-mono detail-ref">{String(node.entity_id)}</dd></div>
        </dl>
      </section>

      {node.tier.startsWith("proc") && (
        <PySpyPanel procRef={String(node.entity_id)} procLabel={nodeLabel(node)} />
      )}

      {statusEntries.length > 0 && (
        <section className="drawer-section">
          <div className="eyebrow">Actor status below</div>
          <div className="chips">
            {statusEntries.map(([s, n]) => (
              <span key={s} className="chip">
                <span className="dot-only" style={{ background: statusMeta(s).color }} /> {s} <b>{n}</b>
              </span>
            ))}
          </div>
        </section>
      )}

      {kids.length > 0 && (
        <section className="drawer-section">
          <div className="eyebrow">Children <span className="count-tag">{kids.length}</span></div>
          <div className="child-list">
            {[...kids]
              .sort((a, b) => nodeLabel(nodeById.get(a)!).localeCompare(nodeLabel(nodeById.get(b)!)))
              .map((k) => {
                const c = nodeById.get(k)!;
                const cc = statusMeta(c.status).color;
                const grand = (children[k] ?? []).length;
                return (
                  <button key={k} className="child-item" onClick={() => onSelect(k)}>
                    <span className="tree-ico" style={{ color: cc }}>{tierGlyph(c.tier, 13)}</span>
                    <span className="ci-name">{nodeLabel(c)}</span>
                    <span className="ci-tier">{TIER_LABEL[c.tier] ?? c.tier}</span>
                    <span className="tree-spacer" />
                    {grand > 0 && <span className="tree-count subtle">{grand}</span>}
                    <span className="tree-dot" style={{ background: cc }} />
                  </button>
                );
              })}
          </div>
        </section>
      )}
    </div>
  );
}
