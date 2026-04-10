"""Streamlit entrypoint for the modular provenance visualizer.

Run from repository root:
    streamlit run provenance_visualizer/app.py
"""

from __future__ import annotations

from typing import Any, Dict, List

import streamlit as st
import streamlit.components.v1 as components

from config import DEFAULT_LOCAL_PATH, PAGE_LAYOUT, PAGE_TITLE
from data_io import extract_graph, load_payload
from derivation_view import summarize_failed_derivations
import graph_view


# Layout/setup component: establishes the page title and wide layout.
st.set_page_config(page_title=PAGE_TITLE, layout=PAGE_LAYOUT)


# Header component: quick context so users know what this app visualizes.
st.title(PAGE_TITLE)
st.caption("Load provenance JSON and inspect the explanation graph and failed derivations.")


# Input control component: centralizes all input methods in the sidebar.
with st.sidebar:
    st.subheader("Input Source")
    source = st.radio(
        "Choose one input mode",
        ["Upload JSON file", "Read local JSON path", "Paste JSON text"],
        index=0,
    )

    uploaded_file = None
    local_path = ""
    pasted_json = ""

    if source == "Upload JSON file":
        uploaded_file = st.file_uploader("Upload provenance JSON", type=["json"])
    elif source == "Read local JSON path":
        local_path = st.text_input("Path to JSON file", value=DEFAULT_LOCAL_PATH)
    else:
        pasted_json = st.text_area("Paste provenance JSON", height=220)


# Loading component: parse input and show user-friendly validation errors.
payload: Dict[str, Any] | None = None
try:
    payload = load_payload(uploaded_file, local_path, pasted_json)
except Exception as exc:
    if source == "Upload JSON file" and uploaded_file is None:
        pass
    else:
        st.error(f"Unable to load provenance JSON: {exc}")

if payload is None:
    st.info("Provide a provenance JSON input from the sidebar to start visualizing.")
    st.stop()


# Summary component: top-level metrics for quick result inspection.
nodes, edges = extract_graph(payload)
failed_derivations = payload.get("failed_derivations", [])
if not isinstance(failed_derivations, list):
    failed_derivations = []

col1, col2, col3 = st.columns(3)
col1.metric("Failed derivations", int(payload.get("failed_derivation_count", len(failed_derivations))))
col2.metric("Graph nodes", len(nodes))
col3.metric("Graph edges", len(edges))

st.write(f"**Mode:** {payload.get('mode', 'N/A')}")
st.write(f"**Target:** {payload.get('target', 'N/A')}")
st.write(f"**Message:** {payload.get('message', 'N/A')}")


# Shared filter component: selected rules apply to both graph and table views.
rule_ids = sorted({str(rec.get("rule_id", "")) for rec in failed_derivations if isinstance(rec, dict)})
selected_rule_ids = st.multiselect(
    "Filter by rule_id (applies to graph + table)",
    options=rule_ids,
    default=rule_ids,
)
selected_set = set(selected_rule_ids)

filtered_nodes, filtered_edges = graph_view.filter_graph_by_rule_ids(nodes, edges, selected_set)


# Graph panel component: renders tuple -> failed-rule -> failed-goal structure.
st.subheader("Explanation Graph")
if filtered_nodes:
    st.caption(f"Showing {len(filtered_nodes)} nodes / {len(filtered_edges)} edges after filter")
    interactive_builder = getattr(graph_view, "build_interactive_graph_html", None)
    if callable(interactive_builder):
        graph_height = st.slider("Graph canvas height", min_value=450, max_value=1400, value=780, step=50)
        components.html(
            interactive_builder(filtered_nodes, filtered_edges, height_px=graph_height),
            height=graph_height + 95,
            scrolling=False,
        )
    else:
        st.info("Interactive zoom view is unavailable; showing static graph instead.")
        st.graphviz_chart(graph_view.build_dot(filtered_nodes, filtered_edges), use_container_width=True)
else:
    st.warning("No explanation graph found for the selected rule filter.")


# Table panel component: filter and inspect failed derivations in compact form.
st.subheader("Failed Derivations")
filtered_records: List[Dict[str, Any]] = [
    rec
    for rec in failed_derivations
    if isinstance(rec, dict) and str(rec.get("rule_id", "")) in selected_set
]

summary_rows = summarize_failed_derivations(filtered_records)
if summary_rows:
    st.dataframe(summary_rows, use_container_width=True, hide_index=True)
else:
    st.info("No failed derivations match the selected rule filter.")


# Raw payload component: useful for debugging and deep inspection.
with st.expander("Raw JSON payload"):
    st.json(payload)
