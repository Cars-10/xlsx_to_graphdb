#!/usr/bin/env python3
"""
Simple, reliable 3D visualization of Windchill parts.
Starting from scratch with a proven approach.
"""

import plotly.graph_objects as go
from neo4j import GraphDatabase
import webbrowser
import os
import json
from datetime import datetime
import numpy as np
import re

# Configuration
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "tstpwdpwd"

# Windchill URL for links
WINDCHILL_BASE = "https://pp-2511150853nt.portal.ptc.io"

print("Connecting to Neo4j...")
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# Fetch all parts and changes
from datetime import timezone

with driver.session() as session:
    # Sample strategy: Get a representative set from each category
    query = """
    MATCH (n)
    WHERE n:Part OR n:WTPart OR n:PartVersion OR n:Document OR n:Change OR n:ChangeNotice OR n:ChangeRequest
    RETURN
      elementId(n) AS node_id,
      labels(n) AS labels,
      coalesce(n.number, '') AS number,
      coalesce(n.name, '') AS name,
      coalesce(n.state, 'UNKNOWN') AS state,
      coalesce(n.object_type, n.type, head(labels(n))) AS type,
      coalesce(n.revision, 'A') AS revision,
      coalesce(n.version, '') AS version,
      coalesce(n.created_date, 0) AS created_ts,
      coalesce(n.created_by, '') AS created_by,
      coalesce(n.modified_date, n.lastModified, 0) AS modified_ts,
      coalesce(n.modified_by, '') AS modified_by,
      properties(n) AS all_properties
    """

    result = session.run(query)
    nodes = []

    for rec in result:
        created_ts = rec['created_ts'] or 0
        modified_ts = rec['modified_ts'] or 0

        # Format dates for display
        created_date = datetime.fromtimestamp(created_ts, timezone.utc).strftime('%Y-%m-%d %H:%M') if created_ts > 0 else ''
        modified_date = datetime.fromtimestamp(modified_ts, timezone.utc).strftime('%Y-%m-%d %H:%M') if modified_ts > 0 else ''

        # Determine node category based on labels and type
        labels = rec['labels']
        obj_type = rec['type'] or ''

        # Categorize: Document, Change, or Part
        is_document = any(lbl in ['Document', 'DocumentVersion'] for lbl in labels) or 'document' in obj_type.lower()
        is_change = any(lbl in ['Change', 'ChangeNotice', 'ChangeRequest'] for lbl in labels) or 'change' in obj_type.lower()

        if is_document:
            node_category = 'Document'
        elif is_change:
            node_category = 'Change'
        else:
            node_category = 'Part'

        nodes.append({
            'id': rec['node_id'],
            'labels': labels,
            'category': node_category,
            'number': rec['number'] or 'Unknown',
            'name': rec['name'] or '',
            'state': rec['state'] or 'UNKNOWN',
            'type': rec['type'] or 'Part',
            'revision': rec['revision'] or 'A',
            'version': rec['version'] or '',
            'created_ts': created_ts,
            'created_date': created_date,
            'created_by': rec['created_by'] or '',
            'modified_date': modified_date,
            'modified_by': rec['modified_by'] or '',
            'all_properties': dict(rec['all_properties']) if rec['all_properties'] else {}
        })

    parts = nodes

driver.close()

print(f"Found {len(parts)} nodes (Parts, Documents, and Changes)")

# Find earliest creation date to normalize timeline
creation_dates = [n['created_ts'] for n in parts if n['created_ts'] > 0]
if creation_dates:
    earliest_date = min(creation_dates)
    print(f"Earliest creation date: {datetime.fromtimestamp(earliest_date, timezone.utc).strftime('%Y-%m-%d')}")
    # Normalize all timestamps to start from 0
    for node in parts:
        if node['created_ts'] > 0:
            node['days_since_start'] = (node['created_ts'] - earliest_date) / 86400
        else:
            node['days_since_start'] = 0
else:
    earliest_date = 0
    for node in parts:
        node['days_since_start'] = 0

# Split into parts, documents, and changes
part_nodes = [n for n in parts if n['category'] == 'Part']
document_nodes = [n for n in parts if n['category'] == 'Document']
change_nodes = [n for n in parts if n['category'] == 'Change']

print(f"  Parts: {len(part_nodes)}")
print(f"  Documents: {len(document_nodes)}")
print(f"  Changes: {len(change_nodes)}")

if len(parts) == 0:
    print("ERROR: No nodes found!")
    print("Run: python3 scripts/windchill_odata_temporal_import.py")
    exit(1)

# Extract human-readable object type
def extract_readable_type(obj_type_raw):
    """Extract a human-readable type from the raw type string"""
    if not obj_type_raw:
        return 'Unknown'

    # Parse WCTYPE format: "WCTYPE|wt.part.WTPart|com.ptc.MechanicalPart"
    if 'WCTYPE|' in obj_type_raw:
        parts = obj_type_raw.split('|')
        if len(parts) >= 3:
            # Get the last part: com.ptc.MechanicalPart
            last_part = parts[-1]
            # Extract just the type name
            if '.' in last_part:
                type_name = last_part.split('.')[-1]  # MechanicalPart
            else:
                type_name = last_part

            # Make it readable by adding spaces before capitals
            readable = re.sub(r'([a-z])([A-Z])', r'\1 \2', type_name)
            return readable

    # Handle simple types and change types
    type_mapping = {
        'Part': 'Part',
        'WTPart': 'Part',
        'Document': 'Document',
        'Change': 'Change',
        'ChangeNotice': 'Change Notice',
        'ChangeRequest': 'Change Request',
        'ECO': 'Change Order',
        'ECN': 'Change Notice',
        'DEV': 'Development',
        'REV': 'Revision'
    }

    return type_mapping.get(obj_type_raw, obj_type_raw)

# Shorten type names for axis labels
def shorten_type(readable_type):
    """Shorten readable types for axis display"""
    type_map_short = {
        'Mechanical Part': 'Mech Part',
        'Software Part': 'Soft Part',
        'Part': 'Part',
        'Variant': 'Variant',
        'Document': 'Document',
        'Change': 'Change',
        'Change Notice': 'Change Notice',
        'Change Request': 'Change Req',
        'Change Order': 'Change Order',
        'Development': 'Development',
        'Revision': 'Revision',
        'Basic Node': 'Basic Node',
        'Structure Node': 'Struct Node',
        'Helicopter': 'Helicopter',
        'Snowmobile': 'Snowmobile',
        'Unknown': 'Unknown'
    }
    return type_map_short.get(readable_type, readable_type[:15])

# Normalize state names to combine IN_WORK and INWORK
def normalize_state(state):
    """Normalize state names to combine variations"""
    if not state:
        return 'UNKNOWN'
    # Combine IN_WORK and INWORK
    if state in ['IN_WORK', 'INWORK', 'IN WORK']:
        return 'INWORK'
    return state

# Apply state normalization
for part in parts:
    part['state'] = normalize_state(part['state'])

# Extract readable types and then shorten for display
for part in parts:
    part['type_readable'] = extract_readable_type(part['type'])
    part['type_short'] = shorten_type(part['type_readable'])

# Build chronologically ordered state mapping
# Different orderings for Parts/Documents vs Changes

# For Parts/Documents: Design - Prototype - In Work - Production Change - Released - Obsolescence - Under Review
PART_LIFECYCLE_ORDER = [
    'DESIGN',
    'INPLANNING',        # Prototype
    'INWORK',            # In Work
    'PRODUCTIONCHANGE',  # Production Change
    'RELEASEDFORPRODUCTION',
    'RELEASED',
    'CANCELLED',         # Obsolescence
    'Under Review',
    'REVIEW'
]

# For Changes: In Work - Under Review - Released - Cancelled
CHANGE_LIFECYCLE_ORDER = [
    'INWORK',           # In Work
    'OPEN',
    'Under Review',
    'REVIEW',
    'Resolved',
    'IMPLEMENTED',
    'RELEASED',
    'CANCELLED'
]

# Get all unique states from the data
all_unique_states = set(p['state'] for p in parts)

# Combine both orderings to create a unified state list
# Parts/Documents states come first, then Change-specific states
states = []
seen_states = set()

# Add part states first
for state in PART_LIFECYCLE_ORDER:
    if state in all_unique_states and state not in seen_states:
        states.append(state)
        seen_states.add(state)

# Add change states that aren't already included
for state in CHANGE_LIFECYCLE_ORDER:
    if state in all_unique_states and state not in seen_states:
        states.append(state)
        seen_states.add(state)

# Add any remaining states that weren't in our predefined orders
remaining_states = sorted(all_unique_states - seen_states)
states.extend(remaining_states)

types_short = sorted(set(p['type_short'] for p in parts))

print(f"States: {states}")
print(f"Types: {types_short}")

state_map = {s: i for i, s in enumerate(states)}
type_map = {t: i for i, t in enumerate(types_short)}

# Prepare data - separate lists for parts and changes
def prepare_node_data(nodes_list):
    """Prepare visualization data for a list of nodes"""
    x_coords = []
    y_coords = []
    z_coords = []
    labels = []
    colors = []
    customdata_list = []

    # Color map (using short names)
    type_colors = {
        'MechPart': '#3498db',
        'SoftPart': '#2ecc71',
        'WTPart': '#16a085',
        'Doc': '#9b59b6',
        'Part': '#e67e22',
        'Variant': '#e74c3c',
        'Change': '#f39c12',
        'CN': '#e67e22',
        'CR': '#e74c3c',
        'Unknown': '#95a5a6'
    }

    for node in nodes_list:
        x = state_map.get(node['state'], 0)
        y = type_map.get(node['type_short'], 0)

        # Z-axis: use normalized days since earliest part
        z = node.get('days_since_start', 0)

        # Add jitter to prevent overlapping - optimized for large dataset
        JITTER_XY = 0.25  # Moderate horizontal jitter
        JITTER_Z = 1.5    # Small jitter on Z since timeline is normalized
        x += np.random.uniform(-JITTER_XY, JITTER_XY)
        y += np.random.uniform(-JITTER_XY, JITTER_XY)
        z += np.random.uniform(-JITTER_Z, JITTER_Z)

        x_coords.append(x)
        y_coords.append(y)
        z_coords.append(z)

        label = f"<b>{node['number']} Rev.{node['revision']}</b><br>{node['name']}<br>State: {node['state']}<br>Created: {node['created_date']}"
        labels.append(label)

        color = type_colors.get(node['type_short'], type_colors['Unknown'])
        colors.append(color)

        # Prepare customdata for click events
        props_json = json.dumps(node['all_properties'], default=str)
        customdata_list.append([
            node['id'],
            node['number'],
            node['name'],
            node['state'],
            node['type'],
            node['revision'],
            node['version'],
            node['created_date'],
            node['created_by'],
            node['modified_date'],
            node['modified_by'],
            props_json
        ])

    return x_coords, y_coords, z_coords, labels, colors, customdata_list

# Prepare data for parts, documents, and changes separately
part_data = prepare_node_data(part_nodes)
document_data = prepare_node_data(document_nodes)
change_data = prepare_node_data(change_nodes)

print(f"Prepared visualization data:")
print(f"  Parts: {len(part_data[0])} nodes")
print(f"  Documents: {len(document_data[0])} nodes")
print(f"  Changes: {len(change_data[0])} nodes")

# Debug: Show distribution of parts by state and type
if len(part_nodes) > 0:
    print("\nPart distribution:")
    from collections import Counter
    state_counts = Counter([p['state'] for p in part_nodes])
    type_counts = Counter([p['type_short'] for p in part_nodes])
    print(f"  By State: {dict(state_counts)}")
    print(f"  By Type: {dict(type_counts)}")

    # Show first few parts
    print("\nFirst 3 parts:")
    for i, p in enumerate(part_nodes[:3]):
        print(f"  {i+1}. {p['number']} - {p['name'][:50] if p['name'] else 'No name'} - State: {p['state']}")

# Create figure
fig = go.Figure()

# Add PARTS as PLUS SIGNS (+)
if len(part_data[0]) > 0:
    part_x, part_y, part_z, part_labels, part_colors, part_customdata = part_data

    fig.add_trace(go.Scatter3d(
        x=part_x,
        y=part_y,
        z=part_z,
        mode='markers',  # Remove text mode for performance
        marker=dict(
            size=8,  # Smaller size for better performance with many objects
            symbol='cross',  # Plus sign for parts
            color=part_colors,
            opacity=0.8,
            line=dict(width=1, color='white')
        ),
        customdata=part_customdata,
        hovertext=part_labels,
        hoverinfo='text',
        name='Parts (+)'
    ))

# Add DOCUMENTS as SQUARES
if len(document_data[0]) > 0:
    doc_x, doc_y, doc_z, doc_labels, doc_colors, doc_customdata = document_data

    fig.add_trace(go.Scatter3d(
        x=doc_x,
        y=doc_y,
        z=doc_z,
        mode='markers',  # Remove text for performance
        marker=dict(
            size=10,
            symbol='square',  # Square for documents
            color=doc_colors,
            opacity=0.8,
            line=dict(width=1, color='white')
        ),
        customdata=doc_customdata,
        hovertext=doc_labels,
        hoverinfo='text',
        name='Documents (□)'
    ))

# Add CHANGES as CIRCLES
if len(change_data[0]) > 0:
    change_x, change_y, change_z, change_labels, change_colors, change_customdata = change_data

    fig.add_trace(go.Scatter3d(
        x=change_x,
        y=change_y,
        z=change_z,
        mode='markers',  # Remove text for performance
        marker=dict(
            size=10,
            symbol='circle',  # Circle for changes
            color=change_colors,
            opacity=0.8,
            line=dict(width=1, color='white')
        ),
        customdata=change_customdata,
        hovertext=change_labels,
        hoverinfo='text',
        name='Changes (○)'
    ))

# Configure layout
fig.update_layout(
    title=dict(
        text=f"Windchill Graph - {len(part_nodes)} Parts (+) • {len(document_nodes)} Documents (□) • {len(change_nodes)} Changes (○)",
        font=dict(size=16)
    ),
    scene=dict(
        xaxis=dict(
            title=dict(text='\n\n\nLifecycle State', font=dict(size=22)),
            tickfont=dict(size=14),
            ticks='outside',
            ticklen=10,
            tickmode='array',
            tickvals=list(range(len(states))),
            ticktext=states,
            backgroundcolor='rgb(230, 235, 250)',
            gridcolor='rgba(200,200,200,0.35)',
            gridwidth=1
        ),
        yaxis=dict(
            title=dict(text='\n\n\nObject Type', font=dict(size=22)),
            tickfont=dict(size=14),
            ticks='outside',
            ticklen=10,
            tickmode='array',
            tickvals=list(range(len(types_short))),
            ticktext=types_short,
            backgroundcolor='rgb(230, 250, 235)',
            gridcolor='rgba(200,200,200,0.35)',
            gridwidth=1
        ),
        zaxis=dict(
            title=dict(text='\n\n\nDays Since First Part', font=dict(size=22)),
            backgroundcolor='rgb(250, 235, 230)',
            ticks='outside',
            ticklen=10,
            gridcolor='rgba(200,200,200,0.35)',
            gridwidth=1,
            rangemode='tozero'  # Force z-axis to start at 0
        ),
        aspectmode='manual',
        aspectratio=dict(x=1, y=1, z=1.5),
        camera=dict(
            eye=dict(x=1.6, y=1.6, z=1.4)
        )
    ),
    width=1500,
    height=1100,
    margin=dict(l=80, r=80, b=80, t=100),
    showlegend=True,
    legend=dict(
        x=0.02,
        y=0.98,
        bgcolor='rgba(255, 255, 255, 0.9)',
        bordercolor='#ccc',
        borderwidth=1
    )
)

# Save to HTML with click functionality and fullscreen
output_file = os.path.join(os.path.dirname(__file__), 'simple_windchill_3d.html')

# Generate plot div
html_div = fig.to_html(full_html=False, include_plotlyjs=False, div_id='plot', config={'responsive': True, 'displaylogo': False})

html_content = f"""
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
    <button onclick='viewTop()'>⬇ Top (XY)</button>
    <button onclick='viewFront()'>⬅ Front (XZ)</button>
    <button onclick='viewSide()'>⬅ Side (YZ)</button>
    <button onclick='view3D()'>🔄 3D View</button>
    <button onclick='enterFs()'>⛶ Fullscreen</button>
    <button onclick='exitFs()'>⤫ Exit</button>
  </div>
  {html_div}
  <script>
    // View control functions
    function viewTop() {{
      // Top view - looking down the Z axis at XY plane
      Plotly.relayout('plot', {{
        'scene.camera.eye': {{x: 0, y: 0, z: 2.5}}
      }});
    }}

    function viewFront() {{
      // Front view - looking down the Y axis at XZ plane
      Plotly.relayout('plot', {{
        'scene.camera.eye': {{x: 0, y: -2.5, z: 0}}
      }});
    }}

    function viewSide() {{
      // Side view - looking down the X axis at YZ plane
      Plotly.relayout('plot', {{
        'scene.camera.eye': {{x: 2.5, y: 0, z: 0}}
      }});
    }}

    function view3D() {{
      // Default 3D view
      Plotly.relayout('plot', {{
        'scene.camera.eye': {{x: 1.6, y: 1.6, z: 1.4}}
      }});
    }}

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
        console.log('Click event received:', ev);
        if (!ev || !ev.points || !ev.points.length) {{
          console.log('No points in click event');
          return;
        }}
        const p = ev.points[0];
        console.log('Point data:', p);
        if (!p.customdata) {{
          console.log('No customdata in point');
          return;
        }}

        const [nodeId, number, name, state, objType, revision, version, createdDate, createdBy, modifiedDate, modifiedBy, propsJson] = p.customdata;
        console.log('Extracted data:', {{number, name, state}});

        let allProps = {{}};
        try {{ allProps = JSON.parse(propsJson); }} catch(e) {{
          console.error('Failed to parse properties:', e);
        }}

        console.log('All properties:', allProps);

        // Extract Windchill ID for link - only use actual Windchill OIDs, not Neo4j element IDs
        // Valid Windchill OIDs look like: OR:wt.part.WTPart:687084
        let windchillId = allProps.id || allProps.ID || allProps.oid || allProps.objectId || '';

        // Check if it's a valid Windchill OID (should start with OR: or VR: etc, not be a Neo4j element ID)
        const isValidWindchillId = windchillId && (
          windchillId.startsWith('OR:') ||
          windchillId.startsWith('VR:') ||
          windchillId.startsWith('wt.')
        );

        if (!isValidWindchillId) {{
          windchillId = '';  // Don't use Neo4j element IDs
        }}

        console.log('Extracted Windchill ID:', windchillId, '(valid:', isValidWindchillId, ')');

        const windchillUrl = windchillId ?
          '{WINDCHILL_BASE}/Windchill/app/#ptc1/tcomp/infoPage?oid=' + encodeURIComponent(windchillId) :
          '';

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
            <strong>Created:</strong> ${{createdDate}} by ${{createdBy}}<br>
            <strong>Modified:</strong> ${{modifiedDate}} by ${{modifiedBy}}
          </div>
          <div style='margin-bottom: 14px; padding: 12px; background: #ecf0f1; border-radius: 6px; max-height: 400px; overflow-y: auto;'>
            <div style='font-weight: 600; font-size: 13px; margin-bottom: 8px; color: #2c3e50;'>All Properties:</div>
            <div id='props-list' style='font-family: "Consolas", "Monaco", monospace; font-size: 12px;'></div>
          </div>
          <div style='display: flex; gap: 8px;'>
            <button id='btn-windchill' style='padding: 8px 14px; cursor: pointer; background: #3498db; color: white; border: none; border-radius: 4px;'>🔗 View in Windchill</button>
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

        // Add click handler for Windchill link
        const windchillBtn = document.getElementById('btn-windchill');
        if (windchillId && windchillUrl) {{
          console.log('Setting up Windchill button with URL:', windchillUrl);
          windchillBtn.onclick = (e) => {{
            e.preventDefault();
            console.log('Windchill button clicked, opening:', windchillUrl);
            const newWindow = window.open(windchillUrl, '_blank');
            if (!newWindow) {{
              console.error('Failed to open new window - popup blocked?');
              alert('Please allow popups for this site to view items in Windchill');
            }}
            return false;
          }};
          windchillBtn.disabled = false;
          windchillBtn.title = 'Open this object in Windchill: ' + windchillId;
          windchillBtn.style.opacity = '1.0';
          windchillBtn.style.cursor = 'pointer';
        }} else {{
          console.warn('No valid Windchill ID found in properties');
          windchillBtn.onclick = (e) => {{
            e.preventDefault();
            alert('This object does not have a Windchill OID.\\n\\nOnly objects imported from Windchill OData (PartVersion, DocumentVersion) have valid Windchill links.\\n\\nObject type: ' + objType);
            return false;
          }};
          windchillBtn.disabled = false;  // Keep enabled to show the message
          windchillBtn.style.opacity = '0.6';
          windchillBtn.style.cursor = 'not-allowed';
          windchillBtn.title = 'This object is not from Windchill (no OID available)';
        }}

        panel.style.display = 'block';
      }});
    }});
  </script>
</body>
</html>
"""

with open(output_file, 'w', encoding='utf-8') as f:
    f.write(html_content)

print(f"\n✓ Saved to: {output_file}")
webbrowser.open('file://' + os.path.abspath(output_file))
print("✓ Opened in browser")
print(f"\nVisualization contains:")
print(f"  • {len(part_nodes)} Parts displayed as + symbols")
print(f"  • {len(document_nodes)} Documents displayed as □ squares")
print(f"  • {len(change_nodes)} Changes displayed as ○ circles")
print("\nEach node is labeled with its name.")
print("Click any node to see all properties!")
print("Use view buttons to see different axes, fullscreen for better viewing.")
