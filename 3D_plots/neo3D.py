import plotly.graph_objects as go
import pandas as pd
import numpy as np
from neo4j import GraphDatabase
from datetime import datetime
import json
import os
import webbrowser

# ── CONFIGURATION ─────────────────────────────────────────────────────
URI = "bolt://localhost:7687"
USERNAME = "neo4j"
PASSWORD = "tstpwdpwd"

PROP_STATE = "state"
PROP_TYPE  = "partType"
PROP_DATE_PRIMARY = "created_at"
PROP_DATE_FALLBACK1 = "create_date"
PROP_DATE_FALLBACK2 = "updated_at"

ROOT_NAME_FILTER = "T00000001"
Z_SOURCE = "first_change"

# Optional jitter (helps when many nodes have the same state+type)
JITTER = 0.15                      # set to 0.0 for exact grid positions
# ───────────────────────────────────────────────────────────────────────

driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

def fetch_data():
    with driver.session() as session:
        # Simplified query to get all nodes (or filter by ROOT_NAME_FILTER if set)
        if ROOT_NAME_FILTER:
            node_query = f"""
            MATCH (root:WTPart)
            WHERE toString(root.number) = $root_filter OR root.name CONTAINS $root_filter OR toString(root.number) CONTAINS $root_filter
            WITH root LIMIT 1
            MATCH (root)-[:HAS_COMPONENT*0..9]->(p)
            OPTIONAL MATCH (c:Change)-[:AFFECTS_PART]->(p)
            WITH p, min(c.create_date) AS change_min, max(c.create_date) AS change_max
            OPTIONAL MATCH (p)<-[r]-()
            WITH p, change_min, change_max, min(r.created_at) AS rmin
            RETURN
              id(p) AS node_id,
              coalesce(p.`{PROP_STATE}`, 'UNKNOWN') AS state,
              coalesce(p.`{PROP_TYPE}`, head(labels(p))) AS ptype,
              coalesce(p.`{PROP_DATE_PRIMARY}`, p.`{PROP_DATE_FALLBACK1}`, p.`{PROP_DATE_FALLBACK2}`) AS node_created,
              change_min AS change_min,
              change_max AS change_max,
              rmin AS relationship_created,
              p.name AS name,
              labels(p) AS labels,
              properties(p) AS all_properties
            UNION
            MATCH (root:WTPart)
            WHERE toString(root.number) = $root_filter OR root.name CONTAINS $root_filter OR toString(root.number) CONTAINS $root_filter
            WITH root LIMIT 1
            MATCH (root)-[:HAS_COMPONENT*0..9]->(p)
            MATCH (d:Document)-[:DESCRIBES]->(p)
            OPTIONAL MATCH (c:SnowmobileChange)-[:AFFECTS_PART]->(p)
            WITH d, min(c.create_date) AS change_min, max(c.create_date) AS change_max
            OPTIONAL MATCH (d)<-[r]-()
            WITH d, change_min, change_max, min(r.created_at) AS rmin
            RETURN
              id(d) AS node_id,
              coalesce(d.`{PROP_STATE}`, 'UNKNOWN') AS state,
              coalesce(d.`{PROP_TYPE}`, head(labels(d))) AS ptype,
              coalesce(d.`{PROP_DATE_PRIMARY}`, d.`{PROP_DATE_FALLBACK1}`, d.`{PROP_DATE_FALLBACK2}`) AS node_created,
              change_min AS change_min,
              change_max AS change_max,
              rmin AS relationship_created,
              d.name AS name,
              labels(d) AS labels,
              properties(d) AS all_properties
            """
            nodes_result = session.run(node_query, root_filter=ROOT_NAME_FILTER)
        else:
            # Get all nodes when no filter is set
            node_query = f"""
            MATCH (n)
            WHERE n:WTPart OR n:Document
            WITH n LIMIT 500
            OPTIONAL MATCH (n)<-[r]-()
            WITH n, min(r.created_at) AS rmin
            RETURN
              id(n) AS node_id,
              coalesce(n.`{PROP_STATE}`, 'UNKNOWN') AS state,
              coalesce(n.`{PROP_TYPE}`, head(labels(n))) AS ptype,
              coalesce(n.`{PROP_DATE_PRIMARY}`, n.`{PROP_DATE_FALLBACK1}`, n.`{PROP_DATE_FALLBACK2}`) AS node_created,
              null AS change_min,
              null AS change_max,
              rmin AS relationship_created,
              n.name AS name,
              labels(n) AS labels,
              properties(n) AS all_properties
            """
            nodes_result = session.run(node_query)

        nodes = []
        for rec in nodes_result:
            nodes.append({
                "id": rec["node_id"],
                "state": rec["state"],
                "ptype": rec["ptype"],
                "node_created": rec.get("node_created", None),
                "change_min": rec.get("change_min", None),
                "change_max": rec.get("change_max", None),
                "relationship_created": rec.get("relationship_created", None),
                "name": rec["name"],
                "labels": rec["labels"],
                "all_properties": dict(rec.get("all_properties", {})),
            })

        # Print diagnostic info
        print(f"Found {len(nodes)} nodes to visualize")

        node_ids = [n["id"] for n in nodes]

        edges_query = """
        MATCH (a)-[r]->(b)
        WHERE id(a) IN $node_ids AND id(b) IN $node_ids
        RETURN id(a) AS source, id(b) AS target
        """
        edges_result = session.run(edges_query, node_ids=node_ids)
        edges = [(rec["source"], rec["target"]) for rec in edges_result]

        return nodes, edges

nodes_data, edges = fetch_data()
driver.close()

if len(nodes_data) == 0:
    raise ValueError("No nodes found. Verify Neo4j data import and connection.")

df = pd.DataFrame(nodes_data)

# ── Map categorical properties to numeric coordinates ─────────────────
state_order = sorted(df['state'].fillna('UNKNOWN').unique())
state_map = {val: idx for idx, val in enumerate(state_order)}
df['x'] = df['state'].map(state_map)

type_order = sorted(df['ptype'].fillna('Unknown').unique())
type_map = {val: idx for idx, val in enumerate(type_order)}
df['y'] = df['ptype'].map(type_map)

# Type color mapping for node visualization
TYPE_COLOR = {
    'MechanicalPart': '#3498db',
    'SoftwarePart': '#9b59b6',
    'Variant': '#e74c3c',
    'WTPart': '#2ecc71',
    'Document': '#9b59b6',
    'Unknown': '#95a5a6'
}
df['type_color'] = df['ptype'].apply(lambda t: TYPE_COLOR.get(t, TYPE_COLOR['Unknown']))

# ── Convert created date to numeric (timestamp) ───────────────────────
def to_timestamp(val):
    if isinstance(val, datetime):
        return val.timestamp()
    if hasattr(val, 'to_native'):
        return val.to_native().timestamp()
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00")).timestamp()
        except Exception:
            try:
                return datetime.strptime(val, "%Y-%m-%d").timestamp()
            except Exception:
                return 0
    return 0

df['z_node'] = df.get('node_created', 0).apply(to_timestamp)
df['z_first_change'] = df.get('change_min', 0).apply(to_timestamp)
df['z_last_change'] = df.get('change_max', 0).apply(to_timestamp)
df['z_relationship'] = df.get('relationship_created', 0).apply(to_timestamp)
z_col = 'z_first_change'
df['z'] = df[z_col]
def to_datestr(ts):
    try:
        return datetime.utcfromtimestamp(float(ts)).strftime('%Y-%m-%d') if ts and ts != 0 else ''
    except Exception:
        return ''
df['display_date'] = df['z'].apply(to_datestr)

# Optional jitter so nodes with identical state+type don't completely overlap
if JITTER > 0:
    df['x'] += np.random.normal(0, JITTER, size=len(df))
    df['y'] += np.random.normal(0, JITTER, size=len(df))

# ── Build Plotly figure ───────────────────────────────────────────────
fig = go.Figure()

# Edges (gray thin lines)
def build_edges(z_series):
    ex, ey, ez = [], [], []
    for src_id, tgt_id in edges:
        src_rows = df[df['id'] == src_id]
        tgt_rows = df[df['id'] == tgt_id]
        if len(src_rows) == 0 or len(tgt_rows) == 0:
            continue
        src = src_rows.iloc[0]
        tgt = tgt_rows.iloc[0]
        ex += [src['x'], tgt['x'], None]
        ey += [src['y'], tgt['y'], None]
        ez += [z_series[src_rows.index[0]], z_series[tgt_rows.index[0]], None]
    return ex, ey, ez

edge_x, edge_y, edge_z = build_edges(df['z'])

fig.add_trace(go.Scatter3d(
    x=edge_x,
    y=edge_y,
    z=edge_z,
    mode='lines',
    line=dict(color='rgb(180,180,180)', width=1),
    hoverinfo='none',
    name='Relationships'
))

# Nodes - prepare customdata with all properties serialized as JSON
customdata_list = []
for _, row in df.iterrows():
    # Serialize all_properties to JSON string for passing to JavaScript
    props_json = json.dumps(row.get('all_properties', {}), default=str)
    customdata_list.append([
        row['id'],
        row['name'] if pd.notna(row['name']) else '',
        row['state'] if pd.notna(row['state']) else '',
        row['ptype'] if pd.notna(row['ptype']) else '',
        props_json  # JSON string of all properties
    ])

fig.add_trace(go.Scatter3d(
    x=df['x'],
    y=df['y'],
    z=df['z'],
    mode='markers',
    marker=dict(
        size=15,  # Much larger for better visibility
        color=df['type_color'],
        opacity=0.8,
        line=dict(width=2, color='white')
    ),
    customdata=customdata_list,
    text=[f"{row['name'] or ''}<br>State: {row['state']}<br>Type: {row['ptype']}<br>Date: {row['display_date']}" for _, row in df.iterrows()],
    hoverinfo='text',
    name='Nodes'
))

fig.add_trace(go.Scatter3d(
    x=df['x'],
    y=df['y'],
    z=df['z'],
    mode='markers',
    marker=dict(
        size=20,
        color='rgba(0,0,0,0)',
        opacity=1.0,
        line=dict(width=2, color='rgba(0,0,0,0.4)')
    ),
    hoverinfo='skip',
    name='Node Rings'
))

# Type-specific shape overlays
df['is_document'] = df['labels'].apply(lambda ls: isinstance(ls, list) and ('Document' in ls))
df['is_part'] = df['labels'].apply(lambda ls: isinstance(ls, list) and (('WTPart' in ls) or ('Part' in ls)))

def _build_document_squares(dframe):
    ex, ey, ez = [], [], []
    size = 0.35
    docs = dframe[dframe['is_document']]
    for _, r in docs.iterrows():
        x = float(r['x']); y = float(r['y']); z = float(r['z'])
        ex += [x-size, x+size, x+size, x-size, x-size, None]
        ey += [y-size, y-size, y+size, y+size, y-size, None]
        ez += [z, z, z, z, z, None]
    return ex, ey, ez

def _build_part_cross(dframe):
    ex, ey, ez = [], [], []
    size = 0.25
    parts = dframe[dframe['is_part']]
    for _, r in parts.iterrows():
        x = float(r['x']); y = float(r['y']); z = float(r['z'])
        ex += [x-size, x+size, None, x, x, None]
        ey += [y-size, y+size, None, y-size, y+size, None]
        ez += [z, z, None, z, z, None]
    return ex, ey, ez

doc_sx, doc_sy, doc_sz = _build_document_squares(df)
part_cx, part_cy, part_cz = _build_part_cross(df)

if len(doc_sx) > 0:
    fig.add_trace(go.Scatter3d(
        x=doc_sx,
        y=doc_sy,
        z=doc_sz,
        mode='lines',
        line=dict(color='rgba(155,89,182,0.9)', width=3),
        hoverinfo='skip',
        name='Document Squares'
    ))

if len(part_cx) > 0:
    fig.add_trace(go.Scatter3d(
        x=part_cx,
        y=part_cy,
        z=part_cz,
        mode='lines',
        line=dict(color='rgba(46,204,113,0.85)', width=2),
        hoverinfo='skip',
        name='Part Cross'
    ))

# Axis layout with proper labels
fig.update_layout(
    title="3D Graph Visualization (X=Lifecycle State, Y=Type, Z=Created Date)",
    showlegend=False,
    scene=dict(
        xaxis=dict(
            title=dict(text="\n\nLifecycle State", font=dict(size=22)),
            tickfont=dict(size=14),
            tickmode='array',
            tickvals=list(state_map.values()),
            ticktext=list(state_map.keys()),
            backgroundcolor="rgb(230, 230, 250)",
        ),
        yaxis=dict(
            title=dict(text="\n\nType", font=dict(size=22)),
            tickfont=dict(size=14),
            tickmode='array',
            tickvals=list(type_map.values()),
            ticktext=list(type_map.keys()),
            backgroundcolor="rgb(230, 250, 230)",
        ),
        zaxis=dict(
            title=dict(text="\n\nCreated Date", font=dict(size=22)),
            type='linear'
        ),
        aspectmode='manual',
        aspectratio=dict(x=1, y=1, z=1.5),  # stretch Z a bit if dates span many years
        camera=dict(eye=dict(x=1.5, y=1.5, z=1.5))
    ),
    width=1200,
    height=900
)

# Toggle for Z-axis source
edge_x_fc, edge_y_fc, edge_z_fc = build_edges(df['z_first_change'])
edge_x_lc, edge_y_lc, edge_z_lc = build_edges(df['z_last_change'])
edge_x_rel, edge_y_rel, edge_z_rel = build_edges(df['z_relationship'])
edge_x_node, edge_y_node, edge_z_node = build_edges(df['z_node'])

fig.update_layout(
    updatemenus=[
        dict(
            type='dropdown',
            x=0.02, y=1.05,
            buttons=[
                dict(
                    label='First Change',
                    method='update',
                    args=[
                        {'z': [edge_z_fc, df['z_first_change'].tolist(), df['z_first_change'].tolist()]},
                        {'scene': {'zaxis': {'title': {'text': 'Created Date (First Change)'}}}}
                    ]
                ),
                dict(
                    label='Last Change',
                    method='update',
                    args=[
                        {'z': [edge_z_lc, df['z_last_change'].tolist(), df['z_last_change'].tolist()]},
                        {'scene': {'zaxis': {'title': {'text': 'Created Date (Last Change)'}}}}
                    ]
                ),
                dict(
                    label='Relationship Created',
                    method='update',
                    args=[
                        {'z': [edge_z_rel, df['z_relationship'].tolist(), df['z_relationship'].tolist()]},
                        {'scene': {'zaxis': {'title': {'text': 'Created Date (Relationship)'}}}}
                    ]
                ),
                dict(
                    label='Node Date',
                    method='update',
                    args=[
                        {'z': [edge_z_node, df['z_node'].tolist(), df['z_node'].tolist()]},
                        {'scene': {'zaxis': {'title': {'text': 'Created Date (Node)'}}}}
                    ]
                ),
            ]
        )
    ]
)

# Compute helpful zoom ranges
def safe_range(series):
    s = series.dropna()
    if len(s) == 0:
        return [df['z'].min(), df['z'].max()]
    return [float(s.min()), float(s.max())]

x_idx = lambda key: state_map.get(key, list(state_map.values())[0] if state_map else 0)
y_idx = lambda key: type_map.get(key, list(type_map.values())[0] if type_map else 0)

rel_mech = df[(df['state'] == 'RELEASED') & (df['ptype'] == 'MechanicalPart')]
rel_mech_z = safe_range(rel_mech['z'])
des_docs = df[(df['state'] == 'DESIGN') & (df['ptype'] == 'Document')]
des_docs_z = safe_range(des_docs['z'])

released_x = x_idx('RELEASED'); mechanical_y = y_idx('MechanicalPart')
design_x = x_idx('DESIGN'); document_y = y_idx('Document')

def range_around(center, pad=0.8):
    return [center - pad, center + pad]

recent_max = float(df['z'].max()) if len(df['z']) else 0
recent_min = recent_max - 365 * 24 * 3600

# Add zoom buttons
fig.update_layout(
    updatemenus=list(fig.layout.updatemenus) + [
        dict(
            type='buttons',
            x=0.18, y=1.05,
            buttons=[
                dict(
                    label='Zoom: Released Mechanical',
                    method='relayout',
                    args=[{
                        'scene.xaxis.range': range_around(released_x),
                        'scene.yaxis.range': range_around(mechanical_y),
                        'scene.zaxis.range': rel_mech_z,
                        'scene.camera': {'eye': {'x': 1.4, 'y': 1.4, 'z': 1.2}}
                    }]
                ),
                dict(
                    label='Zoom: Design Docs',
                    method='relayout',
                    args=[{
                        'scene.xaxis.range': range_around(design_x),
                        'scene.yaxis.range': range_around(document_y),
                        'scene.zaxis.range': des_docs_z,
                        'scene.camera': {'eye': {'x': 1.2, 'y': 1.6, 'z': 1.4}}
                    }]
                ),
                dict(
                    label='Zoom: Recent Year',
                    method='relayout',
                    args=[{
                        'scene.zaxis.range': [recent_min, recent_max],
                        'scene.camera': {'eye': {'x': 1.5, 'y': 1.0, 'z': 1.5}}
                    }]
                ),
                dict(
                    label='Reset View',
                    method='relayout',
                    args=[{
                        'scene.xaxis.autorange': True,
                        'scene.yaxis.autorange': True,
                        'scene.zaxis.autorange': True,
                        'scene.camera': {'eye': {'x': 1.5, 'y': 1.5, 'z': 1.5}}
                    }]
                ),
            ]
        )
    ]
)

# Fullscreen HTML with responsive sizing and fullscreen buttons
html_div = fig.to_html(full_html=False, include_plotlyjs=False, div_id='plot', config={'responsive': True, 'displaylogo': False})
html = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset='utf-8'/>
  <meta name='viewport' content='width=device-width, initial-scale=1'/>
  <script src='https://cdn.plot.ly/plotly-latest.min.js'></script>
  <style>
    html, body {{ height: 100%; margin: 0; }}
    #plot {{ width: 100vw; height: 100vh; }}
    #toolbar {{ position: fixed; top: 10px; right: 10px; z-index: 10; }}
    #toolbar button {{ padding: 8px 12px; margin-left: 6px; }}
  </style>
  <title>3D Graph Visualization</title>
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
  </script>
  <div id='toolbar'>
    <button onclick='enterFs()'>Fullscreen</button>
    <button onclick='exitFs()'>Exit Fullscreen</button>
  </div>
</head>
<body>
{html_div}
</body>
</html>
"""

out_path = os.path.join(os.path.dirname(__file__), 'neo3D_fullscreen.html')
with open(out_path, 'w', encoding='utf-8') as f:
    node_ids = df['id'].tolist()
    node_x = df['x'].tolist()
    node_y = df['y'].tolist()
    node_z = df['z'].tolist()
    edges_pairs = [(int(s), int(t)) for s, t in edges]
    edge_x_js = [None if v is None else float(v) for v in edge_x]
    edge_y_js = [None if v is None else float(v) for v in edge_y]
    edge_z_js = [None if v is None else float(v) for v in edge_z]
    pos_map = {int(i): {'x': float(x), 'y': float(y), 'z': float(z)} for i, x, y, z in zip(node_ids, node_x, node_y, node_z)}
    extras = (
"<script>\n"
+ "const NODE_IDS = " + json.dumps(node_ids) + ";\n"
+ "const NODE_POS = " + json.dumps(pos_map) + ";\n"
+ "const EDGES = " + json.dumps(edges_pairs) + ";\n"
+ "const EDGE_X_ALL = " + json.dumps(edge_x_js) + ";\n"
+ "const EDGE_Y_ALL = " + json.dumps(edge_y_js) + ";\n"
+ "const EDGE_Z_ALL = " + json.dumps(edge_z_js) + ";\n"
+ "function buildEdgeLinesForNode(nodeId){const ex=[],ey=[],ez=[];for(let i=0;i<EDGES.length;i++){const s=EDGES[i][0];const t=EDGES[i][1];if(s===nodeId||t===nodeId){const sp=NODE_POS[s];const tp=NODE_POS[t];if(sp&&tp){ex.push(sp.x,tp.x,null);ey.push(sp.y,tp.y,null);ez.push(sp.z,tp.z,null);}}}return {ex,ey,ez};}\n"
+ "function formatPropertyValue(val){if(val===null||val===undefined)return'<em>null</em>';if(typeof val==='object')return JSON.stringify(val,null,2);return String(val);}\n"
+ "window.addEventListener('load',()=>{const gd=document.getElementById('plot');gd.on('plotly_click',(ev)=>{if(!ev||!ev.points||!ev.points.length)return;const p=ev.points[0];if(!p.customdata)return;const nid=p.customdata[0];const nm=p.customdata[1];const st=p.customdata[2];const tp=p.customdata[3];const propsJson=p.customdata[4];let allProps={};try{allProps=JSON.parse(propsJson);}catch(e){console.error('Failed to parse properties:',e);}const panel=document.getElementById('info');document.getElementById('info-name').textContent=nm||(\"Node \"+nid);document.getElementById('info-meta').textContent=\"State: \"+st+\" • Type: \"+tp;const propsList=document.getElementById('info-properties');propsList.innerHTML='';const sortedKeys=Object.keys(allProps).sort();sortedKeys.forEach(key=>{const value=allProps[key];const li=document.createElement('div');li.style.marginBottom='4px';li.style.fontSize='12px';li.innerHTML='<strong>'+key+':</strong> '+formatPropertyValue(value);propsList.appendChild(li);});panel.style.display='block';const xr=[p.x-0.8,p.x+0.8];const yr=[p.y-0.8,p.y+0.8];const zr=[p.z-60*24*3600,p.z+60*24*3600];document.getElementById('btn-focus').onclick=()=>{Plotly.relayout(gd,{\"scene.xaxis.range\":xr,\"scene.yaxis.range\":yr,\"scene.zaxis.range\":zr,\"scene.camera\":{eye:{x:1.6,y:1.6,z:1.2}}});};document.getElementById('btn-neighbors').onclick=()=>{const lines=buildEdgeLinesForNode(nid);Plotly.restyle(gd,{x:[lines.ex],y:[lines.ey],z:[lines.ez]},[0]);};document.getElementById('btn-edges-all').onclick=()=>{Plotly.restyle(gd,{x:[EDGE_X_ALL],y:[EDGE_Y_ALL],z:[EDGE_Z_ALL]},[0]);};document.getElementById('btn-close').onclick=()=>{panel.style.display='none';};});});\n"
+ "</script>\n"
+ "<div id='info' style='position:fixed;left:10px;top:60px;background:rgba(255,255,255,0.95);padding:12px 14px;border:1px solid #ccc;border-radius:6px;z-index:20;display:none;max-width:400px;max-height:80vh;overflow-y:auto;'>\n"
+ "  <div id='info-name' style='font-weight:600;margin-bottom:6px;font-size:14px;'></div>\n"
+ "  <div id='info-meta' style='font-size:13px;color:#333;margin-bottom:10px;'></div>\n"
+ "  <div style='margin-bottom:10px;padding:8px;background:#f5f5f5;border-radius:4px;max-height:300px;overflow-y:auto;'>\n"
+ "    <div style='font-weight:600;font-size:12px;margin-bottom:6px;color:#555;'>All Properties:</div>\n"
+ "    <div id='info-properties' style='font-family:monospace;'></div>\n"
+ "  </div>\n"
+ "  <div style='display:flex;gap:4px;flex-wrap:wrap;'>\n"
+ "    <button id='btn-focus' style='padding:6px 10px;font-size:11px;'>Focus Node</button>\n"
+ "    <button id='btn-neighbors' style='padding:6px 10px;font-size:11px;'>Show Neighbors</button>\n"
+ "    <button id='btn-edges-all' style='padding:6px 10px;font-size:11px;'>Show All Edges</button>\n"
+ "    <button id='btn-close' style='padding:6px 10px;font-size:11px;'>Close</button>\n"
+ "  </div>\n"
+ "</div>\n"
    )
    f.write(html.replace('</body>', extras + '\n</body>'))
webbrowser.open('file://' + os.path.abspath(out_path))