#!/usr/bin/env python3
"""
Temporal 3D visualization showing part evolution over time.
X-axis: Lifecycle State
Y-axis: Object Type
Z-axis: Time (days since earliest part)

Each part revision/version is shown as a separate node to visualize evolution.
"""

import plotly.graph_objects as go
import pandas as pd
import numpy as np
from neo4j import GraphDatabase
from datetime import datetime
import json
import os
import webbrowser

# ── CONFIGURATION ─────────────────────────────────────────────────────
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "tstpwdpwd"

# Number of nodes to visualize
MAX_NODES = 1000

# Jitter for overlapping nodes
JITTER = 0.10

# ───────────────────────────────────────────────────────────────────────

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def fetch_temporal_data():
    """
    Fetch all parts, documents, and changes with temporal information.
    Uses revision/state to show evolution.
    """
    with driver.session() as session:
        # Query to get all parts with their properties
        query = """
        MATCH (n)
        WHERE n:Part OR n:WTPart OR n:Document OR n:Change OR n:ChangeNotice OR n:ChangeRequest
        WITH n LIMIT $limit
        RETURN
          id(n) AS node_id,
          labels(n) AS labels,
          coalesce(n.number, '') AS number,
          coalesce(n.name, '') AS name,
          coalesce(n.state, 'UNKNOWN') AS state,
          coalesce(n.type, head(labels(n))) AS object_type,
          coalesce(n.revision, 'A') AS revision,
          coalesce(n.view, '') AS view,
          coalesce(n.container, '') AS container,
          coalesce(n.source, '') AS source,
          properties(n) AS all_properties
        ORDER BY n.number, n.revision
        """

        result = session.run(query, {"limit": MAX_NODES})

        nodes = []
        for rec in result:
            # Create a synthetic timestamp based on revision (A=0, B=30, C=60 days, etc.)
            revision = rec["revision"] or "A"
            # Convert revision letter to days offset (A=0, B=30, C=60, etc.)
            if isinstance(revision, str) and len(revision) > 0:
                rev_offset = (ord(revision[0].upper()) - ord('A')) * 30  # 30 days per revision
            else:
                rev_offset = 0

            nodes.append({
                "id": rec["node_id"],
                "labels": rec["labels"],
                "number": rec["number"],
                "name": rec["name"],
                "state": rec["state"],
                "object_type": rec["object_type"],
                "revision": revision,
                "view": rec["view"],
                "container": rec["container"],
                "source": rec["source"],
                "all_properties": dict(rec.get("all_properties", {})),
                "temporal_offset_days": rev_offset,  # Synthetic time based on revision
            })

        print(f"Fetched {len(nodes)} nodes")

        # Get relationships
        node_ids = [n["id"] for n in nodes]
        edges_query = """
        MATCH (a)-[r]->(b)
        WHERE id(a) IN $node_ids AND id(b) IN $node_ids
        RETURN id(a) AS source, id(b) AS target, type(r) AS rel_type
        """
        edges_result = session.run(edges_query, {"node_ids": node_ids})
        edges = [(rec["source"], rec["target"], rec["rel_type"]) for rec in edges_result]

        print(f"Fetched {len(edges)} relationships")

        return nodes, edges

nodes_data, edges = fetch_temporal_data()
driver.close()

if len(nodes_data) == 0:
    raise ValueError("No nodes found in Neo4j database")

df = pd.DataFrame(nodes_data)

print(f"\nData Summary:")
print(f"Total nodes: {len(df)}")
print(f"States: {df['state'].unique()}")
print(f"Object types: {df['object_type'].unique()}")
print(f"Revisions: {df['revision'].unique()}")

# ── Map categorical properties to numeric coordinates ─────────────────

# X-axis: Lifecycle State
state_order = sorted(df['state'].fillna('UNKNOWN').unique())
state_map = {val: idx for idx, val in enumerate(state_order)}
df['x'] = df['state'].map(state_map)

# Y-axis: Object Type
type_order = sorted(df['object_type'].fillna('Unknown').unique())
type_map = {val: idx for idx, val in enumerate(type_order)}
df['y'] = df['object_type'].map(type_map)

# Z-axis: Temporal offset in days (based on revision)
df['z'] = df['temporal_offset_days']

# Type color mapping
TYPE_COLORS = {
    'WTPart': '#3498db',
    'Part': '#2ecc71',
    'Document': '#9b59b6',
    'Change': '#e74c3c',
    'ChangeNotice': '#f39c12',
    'ChangeRequest': '#e67e22',
    'Unknown': '#95a5a6'
}

def get_color(obj_type):
    for key in TYPE_COLORS:
        if key.lower() in str(obj_type).lower():
            return TYPE_COLORS[key]
    return TYPE_COLORS['Unknown']

df['type_color'] = df['object_type'].apply(get_color)

# Optional jitter for overlapping nodes
if JITTER > 0:
    df['x'] += np.random.normal(0, JITTER, size=len(df))
    df['y'] += np.random.normal(0, JITTER, size=len(df))

# ── Build Plotly figure ───────────────────────────────────────────────
fig = go.Figure()

# Edges
def build_edges():
    ex, ey, ez = [], [], []
    for src_id, tgt_id, rel_type in edges:
        src_rows = df[df['id'] == src_id]
        tgt_rows = df[df['id'] == tgt_id]
        if len(src_rows) == 0 or len(tgt_rows) == 0:
            continue
        src = src_rows.iloc[0]
        tgt = tgt_rows.iloc[0]
        ex += [src['x'], tgt['x'], None]
        ey += [src['y'], tgt['y'], None]
        ez += [src['z'], tgt['z'], None]
    return ex, ey, ez

edge_x, edge_y, edge_z = build_edges()

fig.add_trace(go.Scatter3d(
    x=edge_x,
    y=edge_y,
    z=edge_z,
    mode='lines',
    line=dict(color='rgb(180,180,180)', width=1),
    hoverinfo='none',
    name='Relationships'
))

# Nodes - prepare customdata with all properties
customdata_list = []
for _, row in df.iterrows():
    props_json = json.dumps(row.get('all_properties', {}), default=str)
    customdata_list.append([
        row['id'],
        row['number'],
        row['name'],
        row['state'],
        row['object_type'],
        row['revision'],
        props_json
    ])

fig.add_trace(go.Scatter3d(
    x=df['x'],
    y=df['y'],
    z=df['z'],
    mode='markers',
    marker=dict(
        size=12,
        color=df['type_color'],
        opacity=0.8,
        line=dict(width=2, color='white')
    ),
    customdata=customdata_list,
    text=[f"{row['number']} {row['revision']}<br>{row['name']}<br>State: {row['state']}<br>Type: {row['object_type']}"
          for _, row in df.iterrows()],
    hoverinfo='text',
    name='Nodes'
))

# Node outlines for better visibility
fig.add_trace(go.Scatter3d(
    x=df['x'],
    y=df['y'],
    z=df['z'],
    mode='markers',
    marker=dict(
        size=16,
        color='rgba(0,0,0,0)',
        opacity=1.0,
        line=dict(width=2, color='rgba(0,0,0,0.3)')
    ),
    hoverinfo='skip',
    name='Node Outlines'
))

# Layout
fig.update_layout(
    title="Temporal 3D Graph (X=Lifecycle State, Y=Object Type, Z=Time/Revision)",
    showlegend=False,
    scene=dict(
        xaxis=dict(
            title="Lifecycle State",
            tickmode='array',
            tickvals=list(state_map.values()),
            ticktext=list(state_map.keys()),
            backgroundcolor="rgb(230, 230, 250)",
        ),
        yaxis=dict(
            title="Object Type",
            tickmode='array',
            tickvals=list(type_map.values()),
            ticktext=list(type_map.keys()),
            backgroundcolor="rgb(230, 250, 230)",
        ),
        zaxis=dict(
            title="Time (Days from Revision A)",
            type='linear',
            backgroundcolor="rgb(250, 230, 230)",
        ),
        aspectmode='manual',
        aspectratio=dict(x=1, y=1, z=1.5),
        camera=dict(eye=dict(x=1.5, y=1.5, z=1.5))
    ),
    width=1400,
    height=1000
)

# Generate fullscreen HTML with click functionality
html_div = fig.to_html(full_html=False, include_plotlyjs=False, div_id='plot', config={'responsive': True, 'displaylogo': False})

html_template = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset='utf-8'/>
  <meta name='viewport' content='width=device-width, initial-scale=1'/>
  <script src='https://cdn.plot.ly/plotly-latest.min.js'></script>
  <style>
    html, body {{ height: 100%; margin: 0; font-family: Arial, sans-serif; }}
    #plot {{ width: 100vw; height: 100vh; }}
    #toolbar {{ position: fixed; top: 10px; right: 10px; z-index: 10; }}
    #toolbar button {{ padding: 8px 12px; margin-left: 6px; cursor: pointer; }}
  </style>
  <title>Temporal 3D Graph Visualization</title>
</head>
<body>
  <div id='toolbar'>
    <button onclick='enterFs()'>Fullscreen</button>
    <button onclick='exitFs()'>Exit Fullscreen</button>
  </div>
  {html_div}
  <script>
    function enterFs() {{
      const el = document.documentElement;
      if (el.requestFullscreen) el.requestFullscreen();
      else if (el.webkitRequestFullscreen) el.webkitRequestFullscreen();
    }}
    function exitFs() {{
      if (document.exitFullscreen) document.exitFullscreen();
      else if (document.webkitExitFullscreen) document.webkitExitFullscreen();
    }}

    function formatPropertyValue(val) {{
      if (val === null || val === undefined) return '<em>null</em>';
      if (typeof val === 'object') return JSON.stringify(val, null, 2);
      return String(val);
    }}

    window.addEventListener('load', () => {{
      const gd = document.getElementById('plot');

      gd.on('plotly_click', (ev) => {{
        if (!ev || !ev.points || !ev.points.length) return;
        const p = ev.points[0];
        if (!p.customdata) return;

        const nodeId = p.customdata[0];
        const number = p.customdata[1];
        const name = p.customdata[2];
        const state = p.customdata[3];
        const objType = p.customdata[4];
        const revision = p.customdata[5];
        const propsJson = p.customdata[6];

        let allProps = {{}};
        try {{
          allProps = JSON.parse(propsJson);
        }} catch(e) {{
          console.error('Failed to parse properties:', e);
        }}

        // Create or update info panel
        let panel = document.getElementById('info');
        if (!panel) {{
          panel = document.createElement('div');
          panel.id = 'info';
          panel.style.cssText = 'position:fixed;left:10px;top:60px;background:rgba(255,255,255,0.98);padding:16px;border:1px solid #ccc;border-radius:8px;z-index:20;max-width:450px;max-height:85vh;overflow-y:auto;box-shadow:0 4px 6px rgba(0,0,0,0.1);';
          document.body.appendChild(panel);
        }}

        panel.innerHTML = `
          <div style='font-weight:600;font-size:16px;margin-bottom:8px;color:#2c3e50;'>
            ${{number}} Rev. ${{revision}}
          </div>
          <div style='font-size:13px;color:#555;margin-bottom:12px;'>
            ${{name}}<br>
            <strong>State:</strong> ${{state}} • <strong>Type:</strong> ${{objType}}
          </div>
          <div style='margin-bottom:12px;padding:10px;background:#f8f9fa;border-radius:4px;max-height:350px;overflow-y:auto;'>
            <div style='font-weight:600;font-size:12px;margin-bottom:8px;color:#495057;'>All Properties:</div>
            <div id='info-properties' style='font-family:Consolas,Monaco,monospace;font-size:11px;'></div>
          </div>
          <div style='display:flex;gap:6px;flex-wrap:wrap;'>
            <button id='btn-close' style='padding:8px 12px;font-size:12px;cursor:pointer;'>Close</button>
          </div>
        `;

        const propsList = document.getElementById('info-properties');
        const sortedKeys = Object.keys(allProps).sort();
        sortedKeys.forEach(key => {{
          const value = allProps[key];
          const div = document.createElement('div');
          div.style.cssText = 'margin-bottom:6px;padding:4px;background:white;border-radius:2px;';
          div.innerHTML = `<strong style='color:#2980b9;'>${{key}}:</strong> ${{formatPropertyValue(value)}}`;
          propsList.appendChild(div);
        }});

        document.getElementById('btn-close').onclick = () => {{
          panel.style.display = 'none';
        }};

        panel.style.display = 'block';
      }});
    }});
  </script>
</body>
</html>
"""

out_path = os.path.join(os.path.dirname(__file__), 'temporal_neo3D.html')
with open(out_path, 'w', encoding='utf-8') as f:
    f.write(html_template)

print(f"\n✓ Visualization saved to: {out_path}")
webbrowser.open('file://' + os.path.abspath(out_path))
print("✓ Opened in browser")
