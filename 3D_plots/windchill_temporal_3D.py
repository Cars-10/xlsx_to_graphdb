#!/usr/bin/env python3
"""
Windchill Temporal 3D Visualization
Shows part evolution from Windchill with REAL creation dates.

X-axis: Lifecycle State
Y-axis: Object Type
Z-axis: Creation Date (actual Unix timestamp in days)
"""

import plotly.graph_objects as go
import pandas as pd
import numpy as np
from neo4j import GraphDatabase
from datetime import datetime
import json
import os
import webbrowser

# Configuration
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "tstpwdpwd"

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def fetch_windchill_temporal_data():
    """Fetch PartVersion and DocumentVersion nodes with real temporal data"""
    with driver.session() as session:
        query = """
        MATCH (n)
        WHERE n:PartVersion OR n:DocumentVersion
        RETURN
          id(n) AS node_id,
          labels(n) AS labels,
          coalesce(n.number, '') AS number,
          coalesce(n.name, '') AS name,
          coalesce(n.state, 'UNKNOWN') AS state,
          coalesce(n.object_type, 'Unknown') AS object_type,
          coalesce(n.revision, n.iteration, 'A') AS revision,
          coalesce(n.version, '') AS version,
          coalesce(n.view, '') AS view,
          coalesce(n.full_identifier, '') AS full_identifier,
          coalesce(n.created_date, 0) AS created_timestamp,
          coalesce(n.created_by, '') AS created_by,
          coalesce(n.modified_date, 0) AS modified_timestamp,
          n AS node_props
        ORDER BY n.number, n.revision
        """

        result = session.run(query)
        nodes = []

        for rec in result:
            created_ts = rec["created_timestamp"]
            # Convert timestamp to days since epoch for Z-axis
            days_since_epoch = created_ts / (24 * 3600) if created_ts > 0 else 0

            nodes.append({
                "id": rec["node_id"],
                "labels": rec["labels"],
                "number": rec["number"],
                "name": rec["name"],
                "state": rec["state"],
                "object_type": rec["object_type"],
                "revision": rec["revision"],
                "version": rec["version"],
                "view": rec["view"],
                "full_identifier": rec["full_identifier"],
                "created_timestamp": created_ts,
                "days_since_epoch": days_since_epoch,
                "created_by": rec["created_by"],
                "modified_timestamp": rec["modified_timestamp"],
                "all_properties": dict(rec["node_props"])
            })

        print(f"Fetched {len(nodes)} Windchill temporal nodes")
        return nodes

nodes_data = fetch_windchill_temporal_data()
driver.close()

if len(nodes_data) == 0:
    print("No Windchill temporal data found!")
    print("Run: python3 scripts/windchill_odata_temporal_import.py")
    exit(1)

df = pd.DataFrame(nodes_data)

# Map to coordinates
# X-axis: Lifecycle State
state_order = sorted(df['state'].fillna('UNKNOWN').unique())
state_map = {val: idx for idx, val in enumerate(state_order)}
df['x'] = df['state'].map(state_map)

# Shorten type names FIRST
def shorten_type(obj_type):
    type_map = {
        'Mechanical Part': 'MechPart',
        'Software Part': 'SoftPart',
        'WTPart': 'WTPart',
        'Document': 'Doc',
        'Part': 'Part',
        'Variant': 'Variant',
        'Unknown': 'Unknown'
    }
    return type_map.get(obj_type, obj_type[:8])  # Fallback: truncate to 8 chars

df['object_type_short'] = df['object_type'].apply(shorten_type)

# Update Y-axis mapping with short names
type_order_short = sorted(df['object_type_short'].fillna('Unknown').unique())
type_map = {val: idx for idx, val in enumerate(type_order_short)}
df['y'] = df['object_type_short'].map(type_map)

# Color by type
TYPE_COLORS = {
    'MechPart': '#3498db',
    'SoftPart': '#2ecc71',
    'WTPart': '#16a085',
    'Doc': '#9b59b6',
    'Part': '#e67e22',
    'Variant': '#e74c3c',
    'Unknown': '#95a5a6'
}

def get_color(obj_type_short):
    return TYPE_COLORS.get(obj_type_short, TYPE_COLORS['Unknown'])

df['type_color'] = df['object_type_short'].apply(get_color)

# Z-axis: Days since epoch (real creation date)
df['z'] = df['days_since_epoch']

# Format dates for display
def timestamp_to_date(ts):
    if ts > 0:
        try:
            return datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M')
        except:
            return ''
    return ''

df['display_date'] = df['created_timestamp'].apply(timestamp_to_date)

# Print summary after all columns are created
print(f"\nWindchill Temporal Data Summary:")
print(f"Total nodes: {len(df)}")
print(f"States: {df['state'].unique()}")
print(f"Object types (short): {df['object_type_short'].unique()}")
print(f"Revisions: {df['revision'].unique()}")
print(f"Date range: {df['created_timestamp'].min()} to {df['created_timestamp'].max()}")

# Jitter for visibility
JITTER = 0.08
if JITTER > 0:
    df['x'] += np.random.normal(0, JITTER, size=len(df))
    df['y'] += np.random.normal(0, JITTER, size=len(df))

# Build visualization
fig = go.Figure()

# Prepare customdata
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
        row['version'],
        row['display_date'],
        props_json
    ])

# Nodes - MUCH LARGER circles
fig.add_trace(go.Scatter3d(
    x=df['x'],
    y=df['y'],
    z=df['z'],
    mode='markers',
    marker=dict(
        size=35,  # MUCH larger size
        color=df['type_color'],
        opacity=0.9,
        line=dict(width=3, color='white')
    ),
    customdata=customdata_list,
    text=[f"<b>{row['number']} Rev.{row['revision']}</b><br>{row['name']}<br>State: {row['state']}<br>Created: {row['display_date']}"
          for _, row in df.iterrows()],
    hoverinfo='text',
    name='Parts'
))

# Outlines - even larger
fig.add_trace(go.Scatter3d(
    x=df['x'],
    y=df['y'],
    z=df['z'],
    mode='markers',
    marker=dict(
        size=42,  # Larger outline
        color='rgba(0,0,0,0)',
        opacity=1.0,
        line=dict(width=3, color='rgba(0,0,0,0.5)')  # More visible outline
    ),
    hoverinfo='skip',
    name='Outlines'
))

# Layout
fig.update_layout(
    title="Windchill Temporal Graph - Part Evolution (Real Creation Dates)",
    showlegend=False,
    scene=dict(
        xaxis=dict(
            title="Lifecycle State",
            tickmode='array',
            tickvals=list(state_map.values()),
            ticktext=list(state_map.keys()),
            backgroundcolor="rgb(230, 235, 250)",
        ),
        yaxis=dict(
            title="Object Type",
            tickmode='array',
            tickvals=list(type_map.values()),
            ticktext=list(type_map.keys()),
            backgroundcolor="rgb(230, 250, 235)",
        ),
        zaxis=dict(
            title="Creation Date (Days Since Epoch)",
            type='linear',
            backgroundcolor="rgb(250, 235, 230)",
        ),
        aspectmode='manual',
        aspectratio=dict(x=1, y=1, z=2),
        camera=dict(eye=dict(x=1.6, y=1.6, z=1.4))
    ),
    width=1500,
    height=1100
)

# Generate HTML with click functionality
html_div = fig.to_html(full_html=False, include_plotlyjs=False, div_id='plot', config={'responsive': True, 'displaylogo': False})

html = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset='utf-8'/>
  <meta name='viewport' content='width=device-width, initial-scale=1'/>
  <script src='https://cdn.plot.ly/plotly-latest.min.js'></script>
  <style>
    html, body {{ height: 100%; margin: 0; font-family: system-ui, -apple-system, sans-serif; }}
    #plot {{ width: 100vw; height: 100vh; }}
    #toolbar {{ position: fixed; top: 10px; right: 10px; z-index: 10; }}
    #toolbar button {{ padding: 10px 14px; margin-left: 6px; cursor: pointer; background: white; border: 1px solid #ccc; border-radius: 4px; }}
    #toolbar button:hover {{ background: #f0f0f0; }}
  </style>
  <title>Windchill Temporal 3D Visualization</title>
</head>
<body>
  <div id='toolbar'>
    <button onclick='enterFs()'>⛶ Fullscreen</button>
    <button onclick='exitFs()'>⤫ Exit</button>
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

    function formatValue(val) {{
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

        const [nodeId, number, name, state, objType, revision, version, date, propsJson] = p.customdata;

        let allProps = {{}};
        try {{ allProps = JSON.parse(propsJson); }} catch(e) {{}}

        let panel = document.getElementById('info');
        if (!panel) {{
          panel = document.createElement('div');
          panel.id = 'info';
          panel.style.cssText = `
            position: fixed; left: 10px; top: 70px;
            background: white; padding: 18px;
            border: 2px solid #2c3e50; border-radius: 8px;
            z-index: 20; max-width: 500px; max-height: 85vh;
            overflow-y: auto; box-shadow: 0 6px 12px rgba(0,0,0,0.15);
          `;
          document.body.appendChild(panel);
        }}

        panel.innerHTML = `
          <div style='font-weight: 700; font-size: 18px; margin-bottom: 10px; color: #2c3e50;'>
            ${{number}} <span style='color: #7f8c8d;'>Rev. ${{revision}}</span>
          </div>
          <div style='font-size: 14px; margin-bottom: 6px; color: #34495e;'>
            ${{name}}
          </div>
          <div style='font-size: 13px; color: #7f8c8d; margin-bottom: 14px;'>
            <strong>State:</strong> ${{state}} | <strong>Type:</strong> ${{objType}}<br>
            <strong>Version:</strong> ${{version}}<br>
            <strong>Created:</strong> ${{date}}
          </div>
          <div style='margin-bottom: 14px; padding: 12px; background: #ecf0f1; border-radius: 6px; max-height: 400px; overflow-y: auto;'>
            <div style='font-weight: 600; font-size: 13px; margin-bottom: 8px; color: #2c3e50;'>All Properties:</div>
            <div id='props-list' style='font-family: "Consolas", "Monaco", monospace; font-size: 12px;'></div>
          </div>
          <div style='display: flex; gap: 8px;'>
            <button id='btn-close' style='padding: 8px 14px; cursor: pointer; background: #e74c3c; color: white; border: none; border-radius: 4px;'>Close</button>
          </div>
        `;

        const propsList = document.getElementById('props-list');
        Object.keys(allProps).sort().forEach(key => {{
          const div = document.createElement('div');
          div.style.cssText = 'margin-bottom: 8px; padding: 6px; background: white; border-radius: 4px; border-left: 3px solid #3498db;';
          div.innerHTML = `<strong style='color: #2980b9;'>${{key}}:</strong> ${{formatValue(allProps[key])}}`;
          propsList.appendChild(div);
        }});

        document.getElementById('btn-close').onclick = () => {{ panel.style.display = 'none'; }};
        panel.style.display = 'block';
      }});
    }});
  </script>
</body>
</html>
"""

out_path = os.path.join(os.path.dirname(__file__), 'windchill_temporal_3D.html')
with open(out_path, 'w', encoding='utf-8') as f:
    f.write(html)

print(f"\n✓ Visualization saved to: {out_path}")
webbrowser.open('file://' + os.path.abspath(out_path))
print("✓ Opened in browser")
print(f"\nShowing {len(df)} parts with real creation dates from Windchill!")
