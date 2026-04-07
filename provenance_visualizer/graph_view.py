"""Graph rendering helpers for Streamlit visualizations."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Set, Tuple

from config import NODE_STYLE


# Escaping component: prevents malformed DOT when labels contain quotes/backslashes.
def escape_graphviz(text: str) -> str:
    return text.replace("\\", "\\\\").replace('"', '\\"')


# Backward-compatible component: converts explanation_graph nodes/edges to DOT syntax.
def build_dot(nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]]) -> str:
    lines = [
        "digraph provenance {",
        "rankdir=LR;",
        'node [style="filled", fontname="Helvetica"];',
        'edge [fontname="Helvetica"];',
    ]

    for node in nodes:
        node_id = str(node.get("id", "")).strip()
        if not node_id:
            continue

        node_type = str(node.get("type", "unknown"))
        label = str(node.get("label", node_id))
        shape, color = NODE_STYLE.get(node_type, ("oval", "#f0f0f0"))

        lines.append(
            f'"{escape_graphviz(node_id)}" '
            f'[label="{escape_graphviz(label)}", shape={shape}, fillcolor="{color}"];'
        )

    for edge in edges:
        src = str(edge.get("from", "")).strip()
        dst = str(edge.get("to", "")).strip()
        if not src or not dst:
            continue

        lines.append(f'"{escape_graphviz(src)}" -> "{escape_graphviz(dst)}";')

    lines.append("}")
    return "\n".join(lines)


def _extract_rule_id(node: Dict[str, Any]) -> str:
    if str(node.get("type", "")) != "rule":
        return ""

    node_id = str(node.get("id", ""))
    if node_id.startswith("rule:"):
        parts = node_id.split(":", 2)
        if len(parts) >= 2:
            return parts[1]

    label = str(node.get("label", "")).strip()
    if "(" in label:
        return label.split("(", 1)[0].strip()
    return label


# Filtering component: keeps only tuple + selected rule instances + their goals.
def filter_graph_by_rule_ids(
    nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]], selected_rule_ids: Set[str]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    node_map: Dict[str, Dict[str, Any]] = {}
    for node in nodes:
        node_id = str(node.get("id", "")).strip()
        if node_id:
            node_map[node_id] = node

    kept_ids: Set[str] = {
        node_id
        for node_id, node in node_map.items()
        if str(node.get("type", "")) == "tuple"
    }

    if not selected_rule_ids:
        filtered_nodes = [node for node in nodes if str(node.get("id", "")).strip() in kept_ids]
        filtered_edges: List[Dict[str, Any]] = []
        return filtered_nodes, filtered_edges

    kept_rule_ids: Set[str] = set()
    for node_id, node in node_map.items():
        if str(node.get("type", "")) != "rule":
            continue
        if _extract_rule_id(node) in selected_rule_ids:
            kept_rule_ids.add(node_id)

    kept_ids.update(kept_rule_ids)

    for edge in edges:
        src = str(edge.get("from", "")).strip()
        dst = str(edge.get("to", "")).strip()
        if src in kept_rule_ids and dst in node_map and str(node_map[dst].get("type", "")) == "goal":
            kept_ids.add(dst)

    filtered_nodes = [node for node in nodes if str(node.get("id", "")).strip() in kept_ids]
    filtered_edges = [
        edge
        for edge in edges
        if str(edge.get("from", "")).strip() in kept_ids and str(edge.get("to", "")).strip() in kept_ids
    ]

    return filtered_nodes, filtered_edges


def _normalize_nodes_for_vis(nodes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    shape_map = {
        "box": "box",
        "ellipse": "ellipse",
        "diamond": "diamond",
        "oval": "ellipse",
    }

    for node in nodes:
        node_id = str(node.get("id", "")).strip()
        if not node_id:
            continue

        node_type = str(node.get("type", "unknown"))
        label = str(node.get("label", node_id))
        shape, color = NODE_STYLE.get(node_type, ("oval", "#f0f0f0"))

        normalized.append(
            {
                "id": node_id,
                "label": label,
                "shape": shape_map.get(shape, "ellipse"),
                "color": {
                    "background": color,
                    "border": "#8a8a8a",
                    "highlight": {"background": color, "border": "#4f4f4f"},
                },
                "font": {"face": "Helvetica", "size": 14},
            }
        )

    return normalized


def _normalize_edges_for_vis(edges: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    normalized: List[Dict[str, str]] = []
    for edge in edges:
        src = str(edge.get("from", "")).strip()
        dst = str(edge.get("to", "")).strip()
        if not src or not dst:
            continue
        normalized.append({"from": src, "to": dst})
    return normalized


# Interactive component: renders a zoomable graph with pan + zoom controls.
def build_interactive_graph_html(nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]], height_px: int = 700) -> str:
    vis_nodes = _normalize_nodes_for_vis(nodes)
    vis_edges = _normalize_edges_for_vis(edges)

    nodes_json = json.dumps(vis_nodes)
    edges_json = json.dumps(vis_edges)
    height = max(400, int(height_px))

    return f"""
<!doctype html>
<html>
  <head>
    <meta charset=\"utf-8\" />
    <link
      rel=\"stylesheet\"
      href=\"https://unpkg.com/vis-network@9.1.9/styles/vis-network.min.css\"
    />
    <script src=\"https://unpkg.com/vis-network@9.1.9/dist/vis-network.min.js\"></script>
    <style>
      body {{ margin: 0; font-family: Helvetica, Arial, sans-serif; background: #ffffff; }}
      .toolbar {{
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        align-items: center;
        padding: 6px 0;
      }}
      .toolbar button {{
        border: 1px solid #cfcfcf;
        background: #f8f9fa;
        border-radius: 6px;
        padding: 6px 10px;
        cursor: pointer;
        font-size: 13px;
      }}
      .toolbar button:disabled {{
        opacity: 0.55;
        cursor: not-allowed;
      }}
      .toolbar button:hover:enabled {{ background: #f0f0f0; }}
      .toolbar input[type=\"text\"] {{
        border: 1px solid #cfcfcf;
        border-radius: 6px;
        padding: 6px 10px;
        font-size: 13px;
        min-width: 260px;
      }}
      #provenance-network {{
        width: 100%;
        height: {height}px;
        border: 1px solid #dddddd;
        border-radius: 8px;
        margin-top: 6px;
      }}
      .hint {{ color: #6b7280; font-size: 12px; }}
    </style>
  </head>
  <body>
    <div class=\"toolbar\">
      <button id=\"zoom-reset\" type=\"button\">Reset view</button>
      <button id=\"collapse-clusters\" type=\"button\">Collapse goals</button>
      <button id=\"expand-clusters\" type=\"button\" disabled>Expand clusters</button>
      <span class=\"hint\">Tip: mouse wheel to zoom, drag to pan.</span>
    </div>

    <div class=\"toolbar\">
      <input id=\"search-input\" type=\"text\" placeholder=\"Search node labels (tuple/rule/goal)\" />
      <button id=\"search-find\" type=\"button\">Find</button>
      <button id=\"search-prev\" type=\"button\" disabled>Prev</button>
      <button id=\"search-next\" type=\"button\" disabled>Next</button>
      <button id=\"search-clear\" type=\"button\" disabled>Clear</button>
      <span id=\"search-status\" class=\"hint\">No active search</span>
    </div>

    <div id=\"provenance-network\"></div>

    <script>
      const BASE_BORDER_COLOR = "#8a8a8a";
      const MATCH_BORDER_COLOR = "#2563eb";
      const ACTIVE_MATCH_BORDER_COLOR = "#ea580c";

      const container = document.getElementById("provenance-network");
      const nodes = new vis.DataSet({nodes_json});
      const edges = new vis.DataSet({edges_json});

      const options = {{
        physics: false,
        layout: {{
          hierarchical: {{
            enabled: true,
            direction: "LR",
            levelSeparation: 180,
            nodeSpacing: 170,
            sortMethod: "directed",
          }},
        }},
        interaction: {{
          hover: true,
          dragView: true,
          zoomView: true,
          keyboard: true,
        }},
        edges: {{
          arrows: {{ to: {{ enabled: true, scaleFactor: 0.75 }} }},
          smooth: {{ type: "cubicBezier", forceDirection: "horizontal", roundness: 0.45 }},
          color: {{ color: "#8a8a8a" }},
        }},
        nodes: {{
          margin: 12,
          borderWidth: 1,
          widthConstraint: {{ maximum: 280 }},
        }},
      }};

      const network = new vis.Network(container, {{ nodes, edges }}, options);

      const searchInput = document.getElementById("search-input");
      const searchStatus = document.getElementById("search-status");
      const searchFindBtn = document.getElementById("search-find");
      const searchPrevBtn = document.getElementById("search-prev");
      const searchNextBtn = document.getElementById("search-next");
      const searchClearBtn = document.getElementById("search-clear");
      const collapseClustersBtn = document.getElementById("collapse-clusters");
      const expandClustersBtn = document.getElementById("expand-clusters");

      let searchMatches = [];
      let searchIndex = -1;
      let highlightedNodeIds = new Set();
      let clusterIds = [];
      let clustersCollapsed = false;

      function resetView() {{
        network.fit({{ animation: {{ duration: 250, easingFunction: "easeInOutQuad" }} }});
      }}
      function setNodeBorder(nodeId, borderColor, borderWidth, shadowEnabled) {{
        const node = nodes.get(nodeId);
        if (!node) return;

        const color = node.color || {{}};
        nodes.update({{
          id: nodeId,
          borderWidth: borderWidth,
          color: {{ ...color, border: borderColor }},
          shadow: {{ enabled: shadowEnabled, color: borderColor, size: 14, x: 0, y: 0 }},
        }});
        highlightedNodeIds.add(nodeId);
      }}

      function clearHighlights() {{
        highlightedNodeIds.forEach((nodeId) => {{
          const node = nodes.get(nodeId);
          if (!node) return;
          const color = node.color || {{}};
          nodes.update({{
            id: nodeId,
            borderWidth: 1,
            color: {{ ...color, border: BASE_BORDER_COLOR }},
            shadow: {{ enabled: false }},
          }});
        }});
        highlightedNodeIds = new Set();
        network.unselectAll();
      }}

      function updateSearchControls() {{
        const hasMatches = searchMatches.length > 0;
        searchPrevBtn.disabled = !hasMatches;
        searchNextBtn.disabled = !hasMatches;
        searchClearBtn.disabled = !hasMatches && !searchInput.value.trim();
      }}

      function updateSearchStatus(text) {{
        searchStatus.textContent = text;
      }}

      function focusCurrentMatch() {{
        if (!searchMatches.length || searchIndex < 0) return;

        clearHighlights();
        searchMatches.forEach((nodeId) => setNodeBorder(nodeId, MATCH_BORDER_COLOR, 2, true));

        const currentId = searchMatches[searchIndex];
        setNodeBorder(currentId, ACTIVE_MATCH_BORDER_COLOR, 3, true);

        network.selectNodes([currentId]);
        network.focus(currentId, {{
          scale: Math.max(0.8, network.getScale()),
          animation: {{ duration: 240, easingFunction: "easeInOutQuad" }},
        }});

        updateSearchStatus(
          "Match " + (searchIndex + 1) + " / " + searchMatches.length + " (" + currentId + ")"
        );
      }}

      function runSearch() {{
        const term = searchInput.value.trim().toLowerCase();
        clearHighlights();

        if (!term) {{
          searchMatches = [];
          searchIndex = -1;
          updateSearchStatus("Enter text to search node labels");
          updateSearchControls();
          return;
        }}

        searchMatches = nodes
          .get()
          .filter((node) => String(node.label || "").toLowerCase().includes(term))
          .map((node) => node.id);

        if (!searchMatches.length) {{
          searchIndex = -1;
          updateSearchStatus("No nodes matched: " + term);
          updateSearchControls();
          return;
        }}

        searchIndex = 0;
        focusCurrentMatch();
        updateSearchControls();
      }}

      function cycleMatch(step) {{
        if (!searchMatches.length) return;
        searchIndex = (searchIndex + step + searchMatches.length) % searchMatches.length;
        focusCurrentMatch();
      }}

      function clearSearch() {{
        searchMatches = [];
        searchIndex = -1;
        clearHighlights();
        updateSearchStatus("No active search");
        updateSearchControls();
      }}

      function updateClusterControls() {{
        collapseClustersBtn.disabled = clustersCollapsed;
        expandClustersBtn.disabled = !clustersCollapsed;
      }}

      function collapseGoalClusters() {{
        if (clustersCollapsed) return;

        const ruleNodeIds = nodes.getIds({{
          filter: (node) => String(node.id).startsWith("rule:"),
        }});

        clusterIds = [];
        for (const rawRuleNodeId of ruleNodeIds) {{
          const ruleNodeId = String(rawRuleNodeId);
          const goalNodeIds = network
            .getConnectedNodes(ruleNodeId, "to")
            .map((value) => String(value))
            .filter((id) => id.startsWith("goal:") && !network.isCluster(id));

          if (goalNodeIds.length < 2) continue;

          const goalMembers = new Set(goalNodeIds);
          const clusterId = "cluster:goals:" + ruleNodeId;

          network.cluster({{
            joinCondition: (nodeOptions) => goalMembers.has(String(nodeOptions.id)),
            clusterNodeProperties: {{
              id: clusterId,
              label: goalNodeIds.length + " failed goals",
              shape: "hexagon",
              color: {{ background: "#d6eaff", border: "#4f4f4f" }},
              borderWidth: 1,
              font: {{ face: "Helvetica", size: 12 }},
            }},
          }});

          if (network.isCluster(clusterId)) {{
            clusterIds.push(clusterId);
          }}
        }}

        clustersCollapsed = clusterIds.length > 0;
        updateClusterControls();
      }}

      function expandGoalClusters() {{
        if (!clustersCollapsed) return;

        for (const clusterId of clusterIds) {{
          if (network.isCluster(clusterId)) {{
            network.openCluster(clusterId);
          }}
        }}

        clusterIds = [];
        clustersCollapsed = false;
        updateClusterControls();
      }}

      document.getElementById("zoom-reset").addEventListener("click", resetView);

      searchFindBtn.addEventListener("click", runSearch);
      searchPrevBtn.addEventListener("click", () => cycleMatch(-1));
      searchNextBtn.addEventListener("click", () => cycleMatch(1));
      searchClearBtn.addEventListener("click", clearSearch);
      searchInput.addEventListener("keydown", (event) => {{
        if (event.key === "Enter") runSearch();
      }});

      collapseClustersBtn.addEventListener("click", collapseGoalClusters);
      expandClustersBtn.addEventListener("click", expandGoalClusters);

      resetView();
      updateSearchControls();
      updateClusterControls();
    </script>
  </body>
</html>
"""

