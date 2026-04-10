# Provenance Visualizer Quick Walkthrough

This directory contains a small Streamlit app to inspect WHY-NOT provenance results.

## 1) Start up

From the repository root, run:

```bash
streamlit run provenance_visualizer/app.py
```

The app entrypoint is `provenance_visualizer/app.py`.

---

## 2) What the app shows

After loading a provenance JSON payload, the app shows:

- **Summary metrics**: failed derivations, graph node count, edge count
- **Top-level fields**: mode, target, message
- **Explanation graph**: rendered with Graphviz
- **Failed derivation table**: filterable by `rule_id`
- **Raw JSON**: full payload viewer for debugging

---

## 3) Input modes

Use the sidebar to choose one input mode:

1. **Upload JSON file**
2. **Read local JSON path** (default path: `firing_rules/test_result.json`)
3. **Paste JSON text**

If input is missing or invalid JSON, the app will show an error message.

---

## 4) Expected JSON format

The app expects a top-level JSON object (`{ ... }`).

### Common top-level fields

- `mode` (string)
- `target` (string)
- `message` (string)
- `failed_derivation_count` (number)
- `failed_derivations` (array)
- `explanation_graph` (object)

### `explanation_graph` structure

```json
{
  "nodes": [
    {"id": "tuple:Q(2)", "type": "tuple", "label": "Missing tuple: Q(2)"},
    {"id": "rule:R1", "type": "rule", "label": "Failed rule R1"},
    {"id": "goal:G1", "type": "goal", "label": "2 > 2"}
  ],
  "edges": [
    {"from": "tuple:Q(2)", "to": "rule:R1"},
    {"from": "rule:R1", "to": "goal:G1"}
  ]
}
```

### `failed_derivations` structure (per item)

```json
{
  "rule_id": "R1",
  "binding": {"X": "2"},
  "goal_results": [
    {"goal": "2 > 2", "ok": false}
  ],
  "status": false
}
```

Notes:
- Missing/invalid `nodes`, `edges`, or `failed_derivations` are treated as empty in the UI.
- The table shows only failed goals (`ok = false`) in its compact summary.

---

## 5) Fast usage flow

1. Generate a provenance output JSON from your WHY-NOT query.
2. Open the Streamlit app.
3. Load JSON using upload/path/paste.
4. Check summary metrics.
5. Inspect the graph to trace tuple -> rule -> failed goals.
6. Use `rule_id` filter to focus on specific derivations.

---

## 6) Troubleshooting

- **Nothing displayed yet**: provide input from the sidebar.
- **JSON parse error**: ensure valid JSON syntax.
- **No graph shown**: confirm `explanation_graph.nodes` is present and non-empty.
- **No table rows**: confirm `failed_derivations` is present and matches selected `rule_id` values.
