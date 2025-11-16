# Neo4j Visualization Guide

## Overview

This guide shows you how to create stunning visualizations in Neo4j Browser after importing your Windchill data. The importer automatically applies rich styling properties to nodes including colors, sizes, and labels based on part properties.

## Quick Start

### 1. Import Your Data

Using the Web UI (recommended):
1. Start the web server: `python src/web_server.py`
2. Open http://localhost:5050
3. Select your Excel file
4. Check **Neo4j** as target database
5. Select your Neo4j database (e.g., "neo4j")
6. Click "Start Import"

Or use command line:
```bash
python src/spreadsheet_loader.py \
  --excel data/Snowmobile.xlsx \
  --bom data/bom.csv \
  --url bolt://localhost:7687 \
  --repo neo4j \
  --user neo4j \
  --password password
```

### 2. Open Neo4j Browser

Navigate to http://localhost:7474 and login with your credentials.

## Stunning Visualizations

### Basic Graph View

Show all parts and relationships:
```cypher
MATCH (p:Part)-[r:HAS_COMPONENT]->(child:Part)
RETURN p, r, child
LIMIT 100
```

### View by Part Name (Human-Friendly)

The importer uses part **names** as the primary display, making graphs immediately readable:

```cypher
MATCH (p:Part)
WHERE p.name CONTAINS 'Motor'
MATCH path = (p)-[*1..3]-(related)
RETURN path
```

### Color-Coded by State

Parts are automatically colored based on their state:
- **Green**: RELEASED (production-ready)
- **Blue**: DESIGN (in development)
- **Yellow**: INPLANNING (planning phase)
- **Orange**: UNDERREVIEW (review phase)

```cypher
MATCH (p:Part)
WHERE p.state IS NOT NULL
RETURN p
```

### Color-Coded by Source

Parts can also be colored by source:
- **Green**: Make (manufactured in-house)
- **Orange**: Buy (purchased from vendors)

```cypher
MATCH (p:Part)
WHERE p.source IN ['make', 'buy']
RETURN p
```

### Part Type Hierarchy

Different part types have different base colors:
- **Blue**: MechanicalPart
- **Purple**: SoftwarePart
- **Red**: Variant
- **Green**: WTPart

```cypher
MATCH (p:Part)
WHERE p.partType = 'MechanicalPart'
MATCH path = (p)-[:HAS_COMPONENT*1..2]->(child)
RETURN path
```

### Assembly Explosion View

View a complete assembly breakdown:

```cypher
// Find the top-level assembly
MATCH (top:Part)
WHERE NOT ()-[:HAS_COMPONENT]->(top)
  AND (top)-[:HAS_COMPONENT]->()
WITH top LIMIT 1

// Expand all components
MATCH path = (top)-[:HAS_COMPONENT*]->(component)
RETURN path
```

### Find Assemblies by Name

```cypher
MATCH (assembly:Part)
WHERE assembly.name CONTAINS 'Snowmobile'
MATCH path = (assembly)-[:HAS_COMPONENT*1..3]->(component)
RETURN path
```

### Alternate/Replacement Parts

View parts with alternates or replacements:

```cypher
MATCH path = (p:Part)-[r:HAS_ALTERNATE]->(alt:Part)
RETURN path
```

### Complex Components (Most Detailed)

Find parts with the most metadata (shown as larger nodes):

```cypher
MATCH (p:Part)
WHERE p.size > 40
MATCH path = (p)-[:HAS_COMPONENT*0..2]-(related)
RETURN path
```

### View by Container/Location

Group parts by their organizational container:

```cypher
MATCH (p:Part)
WHERE p.container IS NOT NULL
WITH p.container AS container, COLLECT(p) AS parts
RETURN container, parts
LIMIT 10
```

## Customizing the Visual Style

### Method 1: Using Browser Settings

Neo4j Browser automatically uses the `displayColor` and `size` properties set by the importer. To customize further:

1. Click on a node label (e.g., "Part") in the bottom panel
2. Adjust:
   - **Size**: Already set based on complexity
   - **Color**: Already set based on state/source/type
   - **Caption**: Change to display different properties

### Method 2: Custom Cypher Styling

You can override the default styling in your queries:

```cypher
MATCH (p:Part)-[r:HAS_COMPONENT]->(child)
RETURN p, r, child
```

Then click on "Part" in the bottom panel and set:
- **Caption**: `{name}` or `{number}` or `{name} ({partType})`
- **Size**: Use `size` property or set custom value
- **Color**: Use `displayColor` property or choose custom

### Method 3: APOC Styling (Advanced)

If you have APOC installed:

```cypher
MATCH (p:Part)
CALL apoc.create.vNode(['Part'], {
  name: p.name,
  color: p.displayColor,
  size: p.size
}) YIELD node
RETURN node
```

## Useful Queries

### Statistics

Get an overview of your data:

```cypher
// Count parts by type
MATCH (p:Part)
RETURN p.partType AS type, COUNT(p) AS count
ORDER BY count DESC
```

```cypher
// Count relationships
MATCH ()-[r:HAS_COMPONENT]->()
RETURN COUNT(r) AS componentRelationships
```

```cypher
// Find top-level assemblies
MATCH (p:Part)
WHERE NOT ()-[:HAS_COMPONENT]->(p)
  AND (p)-[:HAS_COMPONENT]->()
RETURN p.name, p.number
```

### Search by Properties

```cypher
// Find parts by number
MATCH (p:Part {number: '100 SNOW'})
MATCH path = (p)-[:HAS_COMPONENT*0..3]-(related)
RETURN path
```

```cypher
// Find parts in specific state
MATCH (p:Part)
WHERE p.state = 'RELEASED'
RETURN p.name, p.number, p.partType
LIMIT 20
```

```cypher
// Find make vs buy parts
MATCH (p:Part)
WHERE p.source IN ['make', 'buy']
RETURN p.source, COUNT(p) AS count
```

### Depth Analysis

```cypher
// Find parts with most components
MATCH (p:Part)-[:HAS_COMPONENT]->(child)
WITH p, COUNT(child) AS componentCount
ORDER BY componentCount DESC
LIMIT 10
RETURN p.name, componentCount
```

```cypher
// Find deepest assembly hierarchies
MATCH path = (p:Part)-[:HAS_COMPONENT*]->(leaf)
WHERE NOT (leaf)-[:HAS_COMPONENT]->()
WITH path, LENGTH(path) AS depth
ORDER BY depth DESC
LIMIT 10
RETURN path
```

## Graph Algorithms (with GDS)

If you have Graph Data Science library installed:

### Centrality

Find the most important parts:

```cypher
CALL gds.pageRank.stream({
  nodeProjection: 'Part',
  relationshipProjection: 'HAS_COMPONENT'
})
YIELD nodeId, score
MATCH (p:Part) WHERE id(p) = nodeId
RETURN p.name, score
ORDER BY score DESC
LIMIT 10
```

### Community Detection

Find clusters of related parts:

```cypher
CALL gds.louvain.stream({
  nodeProjection: 'Part',
  relationshipProjection: {
    HAS_COMPONENT: {
      orientation: 'UNDIRECTED'
    }
  }
})
YIELD nodeId, communityId
MATCH (p:Part) WHERE id(p) = nodeId
RETURN communityId, COLLECT(p.name) AS parts
LIMIT 10
```

## Export Visualizations

### Save as Image

In Neo4j Browser:
1. Run your query
2. Click the export icon (camera)
3. Choose PNG or SVG format

### Save as JSON

```cypher
MATCH (p:Part)-[r:HAS_COMPONENT]->(child)
WITH COLLECT({
  parent: p.name,
  child: child.name,
  parentColor: p.displayColor,
  childColor: child.displayColor
}) AS edges
RETURN edges
```

## Performance Tips

1. **Limit Results**: Always use `LIMIT` for large graphs:
   ```cypher
   MATCH (p:Part) RETURN p LIMIT 100
   ```

2. **Use Indexes**: The importer creates indexes automatically, but you can verify:
   ```cypher
   SHOW INDEXES
   ```

3. **Constrain Depth**: Limit path traversal depth:
   ```cypher
   MATCH path = (p)-[:HAS_COMPONENT*1..3]->(child)
   ```

4. **Profile Queries**: Use `PROFILE` to optimize:
   ```cypher
   PROFILE
   MATCH (p:Part)-[:HAS_COMPONENT]->(child)
   RETURN p, child
   ```

## Troubleshooting

### Issue: Nodes don't show colors

**Solution**: Make sure to set the caption display:
1. Click on "Part" label in bottom panel
2. Set Size by: `size`
3. Set Color by: `displayColor`
4. Set Caption: `name`

### Issue: Graph is too dense

**Solution**: Filter by specific criteria:
```cypher
MATCH (p:Part)
WHERE p.partType = 'MechanicalPart'
  AND p.state = 'RELEASED'
MATCH path = (p)-[:HAS_COMPONENT*1..2]->(child)
RETURN path
```

### Issue: Want to see part numbers instead of names

**Solution**: Change the caption in the style panel to `number` or use:
```cypher
MATCH (p:Part)
RETURN p.number + ' - ' + p.name AS display, p
```

## Next Steps

- Explore [Neo4j Browser Guide](https://neo4j.com/developer/neo4j-browser/)
- Learn [Cypher Query Language](https://neo4j.com/developer/cypher/)
- Install [APOC Procedures](https://neo4j.com/labs/apoc/) for advanced features
- Try [Graph Data Science Library](https://neo4j.com/product/graph-data-science/) for analytics

## Example: Complete Snowmobile Analysis

```cypher
// 1. Find the snowmobile assembly
MATCH (snowmobile:Part)
WHERE snowmobile.name CONTAINS 'Snowmobile'
  AND (snowmobile)-[:HAS_COMPONENT]->()

// 2. Get all components up to 3 levels deep
MATCH path = (snowmobile)-[:HAS_COMPONENT*1..3]->(component)

// 3. Return with highlighting
RETURN path
```

Adjust the visual style:
- Set caption to `{name} ({partType})`
- Color by `displayColor` (automatic based on state/source)
- Size by `size` (automatic based on complexity)

Enjoy your visually stunning knowledge graph!
