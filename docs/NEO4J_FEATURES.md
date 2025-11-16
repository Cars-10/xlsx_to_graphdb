# Neo4j Import Features

## Overview

The Neo4j importer creates **visually stunning** graph visualizations by automatically applying rich styling properties to nodes based on their metadata. Unlike the GraphDB RDF approach, Neo4j imports use the **part name as the primary display**, making graphs immediately readable and human-friendly.

## Key Features

### 1. Name-First Approach

**Primary Display**: Part names (not numbers) are used as the main node label
- Makes graphs instantly understandable
- Example: "Front Suspension Assembly" instead of "0114032"
- Part numbers are still stored and accessible as properties

### 2. Intelligent Color Coding

Nodes are automatically colored based on a **priority hierarchy**:

**Priority 1 - State** (highest priority):
- 🟢 Green (#27ae60): RELEASED - Production-ready parts
- 🔵 Blue (#3498db): DESIGN - Parts in development
- 🟡 Yellow (#f39c12): INPLANNING - Planning phase
- 🟠 Orange (#e67e22): UNDERREVIEW - Review phase
- ⚪ Gray (#95a5a6): Other states

**Priority 2 - Source**:
- 🟢 Green (#27ae60): Make - Manufactured in-house
- 🟠 Orange (#e67e22): Buy - Purchased from vendors

**Priority 3 - Part Type**:
- 🔵 Blue (#3498db): MechanicalPart
- 🟣 Purple (#9b59b6): SoftwarePart
- 🔴 Red (#e74c3c): Variant
- 🟢 Green (#2ecc71): WTPart
- ⚪ Gray (#95a5a6): Default

### 3. Dynamic Node Sizing

Node size automatically reflects **data richness**:
- Base size: 30 pixels
- +5 pixels for each metadata field present
- Larger nodes = more detailed parts
- Helps identify well-documented vs sparse data

Metadata fields that increase size:
- Type
- Source (make/buy)
- View (Design/Manufacturing/Service)
- State (RELEASED/DESIGN/etc.)
- Revision
- Container

### 4. Rich Property Graph

Each part node includes:

**Identity Properties:**
- `number`: Unique part number (constraint)
- `name`: Human-readable name (indexed)

**Classification Properties:**
- `partType`: MechanicalPart, SoftwarePart, Variant, WTPart (indexed)
- `type`: Additional type information from Excel
- `source`: make or buy (indexed)
- `state`: RELEASED, DESIGN, INPLANNING, etc. (indexed)
- `view`: Design, Manufacturing, Service

**Metadata Properties:**
- `revision`: Version/revision number
- `container`: Organizational container/folder

**Visual Properties** (used by Neo4j Browser):
- `displayColor`: Hex color code for node styling
- `size`: Node size in pixels

### 5. Relationship Types

**HAS_COMPONENT** (primary BOM relationships):
- Direction: Parent → Child
- Represents assembly structure
- Example: `(Snowmobile)-[:HAS_COMPONENT]->(Engine)`

**HAS_ALTERNATE** (alternate/replacement links):
- Direction: Part → Alternate
- Properties: `type` (alternate, replacement, etc.)
- Example: `(PartA)-[:HAS_ALTERNATE {type: 'replacement'}]->(PartB)`

### 6. Performance Optimizations

**Automatic Constraints:**
- Uniqueness constraint on `Part.number`

**Automatic Indexes:**
- `Part.name` - Fast name lookups
- `Part.partType` - Filter by type
- `Part.source` - Filter by make/buy
- `Part.state` - Filter by lifecycle state

**Batch Processing:**
- Configurable batch size (default: 1000)
- Efficient bulk imports using UNWIND
- Progress logging for large datasets

### 7. Alternate/Replacement Support

Full support for alternate and replacement part relationships:
- Parsed from `WTPartAlternateLink-Sheet`
- Stored as `HAS_ALTERNATE` relationships
- Includes replacement type metadata
- Helps identify interchangeable parts

## Visual Results

### What You Get

1. **Immediate Understanding**: Part names visible at a glance
2. **Color-Coded Context**: State/source/type visible through colors
3. **Size Indicators**: Important/detailed parts stand out
4. **Relationship Clarity**: Assembly hierarchies easy to trace
5. **Interactive Exploration**: Click to expand, filter by properties
6. **Beautiful Layouts**: Neo4j's force-directed layout algorithm

### Example Visualizations

**Assembly View:**
```
                     Snowmobile (Green, Large)
                           |
        +------------------+------------------+
        |                  |                  |
    Engine (Blue)     Chassis (Green)    Controls (Purple)
        |                  |                  |
    Carburetor       Frame                Display
```

**State-Based View:**
- Green nodes: Ready for production
- Blue nodes: Still in design
- Yellow nodes: In planning
- Orange nodes: Under review

**Source-Based View:**
- Green nodes: Make (manufactured)
- Orange nodes: Buy (purchased)

## Comparison with GraphDB

| Feature | Neo4j | GraphDB |
|---------|-------|---------|
| Primary Display | Part Name | Part Number |
| Visual Styling | Automatic colors & sizes | Manual configuration |
| Query Language | Cypher (intuitive) | SPARQL (standards-based) |
| Relationship Types | Typed (HAS_COMPONENT) | RDF predicates |
| Performance | Optimized for traversal | Optimized for reasoning |
| Use Case | Visual exploration | Semantic web, standards |

## Best Practices

### 1. Start with Limited Results

Always use `LIMIT` when exploring:
```cypher
MATCH (p:Part) RETURN p LIMIT 50
```

### 2. Filter Before Expanding

Narrow down before traversing:
```cypher
MATCH (p:Part)
WHERE p.state = 'RELEASED'
MATCH path = (p)-[:HAS_COMPONENT*1..2]->(child)
RETURN path
```

### 3. Use Property Filters

Take advantage of indexed properties:
```cypher
MATCH (p:Part)
WHERE p.partType = 'MechanicalPart'
  AND p.source = 'make'
RETURN p
```

### 4. Constrain Depth

Limit relationship traversal:
```cypher
MATCH path = (p)-[:HAS_COMPONENT*1..3]->(child)
WHERE p.name CONTAINS 'Snowmobile'
RETURN path
```

### 5. Leverage Visual Properties

Style by data:
- In Neo4j Browser, click on "Part" label
- Set Color by: `displayColor`
- Set Size by: `size`
- Set Caption: `name`

## Integration with Web UI

The web UI (http://localhost:5050) provides:
- Easy database selection (Neo4j checkbox)
- Repository/database dropdown
- Same configuration options as GraphDB
- Automatic detection and import
- Support for importing to both databases simultaneously

## Command-Line Usage

### Basic Import

```bash
python src/spreadsheet_loader.py \
  --excel data/Snowmobile.xlsx \
  --bom data/bom.csv \
  --url bolt://localhost:7687 \
  --repo neo4j \
  --user neo4j \
  --password password
```

### Quick Script

```bash
bash scripts/load_neo4j.sh
```

### Environment Variables

```bash
export NEO4J_URI="bolt://localhost:7687"
export NEO4J_DATABASE="neo4j"
export NEO4J_USER="neo4j"
export NEO4J_PASS="mypassword"
bash scripts/load_neo4j.sh
```

## Next Steps

1. **Import Your Data**: Use web UI or command line
2. **Open Neo4j Browser**: http://localhost:7474
3. **Explore Visualizations**: See [NEO4J_VISUALIZATION.md](NEO4J_VISUALIZATION.md)
4. **Run Queries**: Use examples from visualization guide
5. **Customize Styling**: Adjust colors, sizes, captions
6. **Export Results**: Save as images or JSON

## Troubleshooting

### Colors Not Showing

**Problem**: Nodes are all the same color
**Solution**:
1. Click "Part" in bottom panel
2. Set Color by: `displayColor`
3. Refresh visualization

### Sizes All the Same

**Problem**: All nodes are the same size
**Solution**:
1. Click "Part" in bottom panel
2. Set Size by: `size`
3. Refresh visualization

### Names Not Visible

**Problem**: Seeing numbers instead of names
**Solution**:
1. Click "Part" in bottom panel
2. Set Caption: `name` (or `{name}` for template)
3. Refresh visualization

### Connection Failed

**Problem**: Cannot connect to Neo4j
**Solution**:
- Check Neo4j is running: http://localhost:7474
- Verify URL: Should be `bolt://localhost:7687` (not HTTP)
- Check credentials: Default is `neo4j` / `password`
- Install driver: `pip install neo4j`

## Summary

The Neo4j importer creates **production-ready, visually stunning** graph visualizations with:

✅ Human-readable names as primary display
✅ Intelligent color coding (state > source > type)
✅ Dynamic node sizing based on data richness
✅ Rich property graph with full metadata
✅ Optimized indexes and constraints
✅ Batch processing for performance
✅ Alternate/replacement relationship support
✅ Full integration with web UI

Start visualizing your Windchill data beautifully in Neo4j today!
