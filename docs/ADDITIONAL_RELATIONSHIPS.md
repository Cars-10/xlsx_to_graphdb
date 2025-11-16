# Additional Relationships for Snowmobile GraphDB

## Currently Implemented Relationships

### 1. **hasComponent** ✅ Implemented
- **Type**: Structural relationship
- **Description**: Parent assembly contains child component
- **Count**: 1,543 relationships (2,072 in hierarchical BOM)
- **Example**: "100 SNOW" hasComponent "STANDARD EXHAUST SYSTEM"
- **Predicate**: `urn:ontology:hasComponent`

### 2. **hasAlternate** ✅ Implemented
- **Type**: Replacement/variant relationship
- **Description**: Part can be replaced by alternate part
- **Count**: 8 relationships
- **Example**: "TOE-PLASTIC, SKI" hasAlternate "TOE-PLASTIC, SKI, BLACK"
- **Predicate**: `urn:ontology:hasAlternate`

---

## Recommended Relationships to Implement

### 3. **usedIn** (Reverse BOM) 🔶 High Priority
- **Type**: Derived structural relationship
- **Description**: Component is used in parent assembly (inverse of hasComponent)
- **Benefit**: Enables "where-used" queries - critical for change impact analysis
- **Count**: Same as hasComponent (1,543), but reversed direction
- **Example**: "STANDARD EXHAUST SYSTEM" usedIn "100 SNOW"
- **Predicate**: `urn:ontology:usedIn`
- **Use Cases**:
  - Find all assemblies affected by a part change
  - Identify reusable components across product line
  - **567 parts are reused in multiple assemblies** (max 27 uses)

### 4. **hasView** 🔶 High Priority
- **Type**: Configuration relationship
- **Description**: Links part to its different views (Design, Manufacturing, Service)
- **Benefit**: Models different engineering perspectives of same part
- **Count**: ~2,073 view assignments
- **Views Found**:
  - **Design**: 2,031 BOM entries (primary engineering view)
  - **Manufacturing**: 17 BOM entries (manufacturing-specific BOMs)
  - **Service**: 25 BOM entries (service/maintenance view)
- **Example**: "PRO-RIDE CHASSIS - BLACK" hasView "Design", "Manufacturing"
- **Predicate**: `urn:ontology:hasView`
- **Use Cases**:
  - Separate design BOM from manufacturing BOM
  - Service parts planning
  - Multi-view PLM queries

### 5. **hasState** 🔶 Medium Priority
- **Type**: Lifecycle relationship
- **Description**: Part's current lifecycle state
- **Benefit**: Track product maturity and release status
- **Count**: ~937 state assignments
- **States Found**:
  - **RELEASED**: 787 parts (ready for production)
  - **DESIGN**: 102 parts (in development)
  - **RELEASEDFORPRODUCTION**: 12 parts (manufacturing-ready)
  - **PRODUCTIONCHANGE**: 11 parts (ECO in progress)
  - **INPLANNING**: 24 parts (early planning)
- **Example**: "800 Cleanfire® H.O. ENGINE" hasState "PRODUCTIONCHANGE"
- **Predicate**: `urn:ontology:hasState`
- **Use Cases**:
  - Filter unreleased parts from production BOMs
  - Track ECOs (Engineering Change Orders)
  - Maturity reporting

### 6. **hasPartType** 🔶 Medium Priority
- **Type**: Classification relationship
- **Description**: Categorizes part by engineering type
- **Benefit**: Enable type-specific queries and filtering
- **Count**: ~937 type assignments
- **Types Found**:
  - **MechanicalPart**: 879 parts
  - **Variant**: 39 parts (configurable options)
  - **WTPart**: 18 parts (generic Windchill parts)
  - **SoftwarePart**: 1 part
- **Example**: "7547332" hasPartType "MechanicalPart"
- **Predicate**: `urn:ontology:hasPartType` or `rdf:type`
- **Use Cases**:
  - Filter mechanical vs electrical vs software parts
  - Variant configuration management
  - Type-specific property queries

### 7. **hasSource** 🔷 Low Priority
- **Type**: Manufacturing sourcing relationship
- **Description**: Whether part is made in-house or purchased
- **Benefit**: Supply chain and make/buy analysis
- **Values**: "make" or "buy"
- **Example**: "STANDARD DRIVETRAIN" hasSource "make"
- **Predicate**: `urn:ontology:hasSource`
- **Use Cases**:
  - Make vs buy reports
  - Supplier management
  - Cost roll-up calculations

### 8. **partOfAssembly** (Transitive Closure) 🔷 Low Priority
- **Type**: Derived transitive relationship
- **Description**: Component is part of assembly at any level (not just direct child)
- **Benefit**: Enables deep BOM queries without recursion
- **Count**: Significantly more than hasComponent (includes all descendants)
- **Example**: "7518490" (a screw) partOfAssembly "100 SNOW" (even though it's 3 levels deep)
- **Predicate**: `urn:ontology:partOfAssembly`
- **Use Cases**:
  - Complete bill of materials queries
  - "Show me all fasteners in this snowmobile"
  - Compliance reporting (e.g., REACH/RoHS)

### 9. **hasRevision** 🔷 Low Priority
- **Type**: Version relationship
- **Description**: Links part to its revision
- **Benefit**: Track part versioning and changes
- **Count**: ~937 revision assignments
- **Revisions Found**: "1", "A", "-", various
- **Example**: "STANDARD FUEL SYSTEM" hasRevision "A"
- **Predicate**: `urn:ontology:hasRevision`
- **Note**: This dataset has minimal multi-revision parts
- **Use Cases**:
  - Configuration management
  - As-designed vs as-built tracking
  - Change history

### 10. **inContainer** 🔷 Low Priority
- **Type**: Organizational relationship
- **Description**: Part's location in organizational hierarchy
- **Benefit**: Multi-site or multi-project management
- **Example**: Part inContainer "/wt.pdmlink.PDMLinkProduct=Snowmobile"
- **Predicate**: `urn:ontology:inContainer`
- **Use Cases**:
  - Multi-product PLM
  - Access control
  - Organizational reporting

---

## Implementation Priority Recommendation

### Phase 1: High-Value Relationships (Implement Now)
1. **usedIn** - Critical for change impact analysis
   - 567 parts used in multiple assemblies
   - Most reused part appears 27 times
2. **hasView** - Essential for multi-view BOM management
   - Separate design from manufacturing BOMs
   - Service parts analysis

### Phase 2: Classification & Lifecycle (Implement Next)
3. **hasState** - Lifecycle tracking
4. **hasPartType** - Part classification

### Phase 3: Advanced Features (Future)
5. **partOfAssembly** - Transitive closure for deep queries
6. **hasRevision** - Version control
7. **hasSource** - Supply chain analysis
8. **inContainer** - Organizational hierarchy

---

## Example Enhanced Graph Schema

```turtle
# Part with all relationships
<urn:part:0114032>
    rdf:type ont:Part ;
    ont:hasPartType ont:MechanicalPart ;
    rdfs:label "STANDARD EXHAUST SYSTEM" ;
    ont:name "STANDARD EXHAUST SYSTEM" ;
    ont:hasState ont:RELEASED ;
    ont:hasRevision "1" ;
    ont:hasView ont:Design ;
    ont:hasSource ont:Make ;
    ont:usedIn <urn:part:100%20SNOW> ;
    ont:hasComponent <urn:part:1263038_SW> ;
    ont:hasComponent <urn:part:1262701> ;
    ont:partOfAssembly <urn:part:100%20SNOW> .
```

---

## Query Examples with New Relationships

### Where-Used Query (usedIn)
```sparql
# Find all assemblies that use a specific part
PREFIX ont: <urn:ontology:>
SELECT ?assemblyName
WHERE {
  <urn:part:7547332> ont:usedIn ?assembly .
  ?assembly rdfs:label ?assemblyName
}
# Returns 27 assemblies!
```

### Released Parts Only (hasState)
```sparql
# Get BOM with only released parts
PREFIX ont: <urn:ontology:>
SELECT ?parentName ?childName
WHERE {
  ?parent ont:hasComponent ?child ;
          ont:hasState ont:RELEASED ;
          rdfs:label ?parentName .
  ?child ont:hasState ont:RELEASED ;
         rdfs:label ?childName
}
```

### Manufacturing BOM (hasView)
```sparql
# Get manufacturing-specific BOM structure
PREFIX ont: <urn:ontology:>
SELECT ?parentName ?childName
WHERE {
  ?parent ont:hasComponent ?child ;
          ont:hasView ont:Manufacturing ;
          rdfs:label ?parentName .
  ?child rdfs:label ?childName
}
```

### All Fasteners in Snowmobile (partOfAssembly + type)
```sparql
# Find all screws/fasteners in the snowmobile
PREFIX ont: <urn:ontology:>
SELECT DISTINCT ?partName
WHERE {
  ?part partOfAssembly <urn:part:100%20SNOW> ;
        rdfs:label ?partName .
  FILTER(REGEX(?partName, "SCR-|WASHER|NUT-|BOLT", "i"))
}
```

---

## Summary Statistics

- **Total Parts**: 939
- **Reusable Parts**: 567 (61% of parts)
- **Maximum Part Reuse**: 27 assemblies
- **BOM Relationships**: 2,072
- **Alternate Links**: 8
- **Views**: 3 (Design, Manufacturing, Service)
- **States**: 5 (RELEASED, DESIGN, INPLANNING, PRODUCTIONCHANGE, RELEASEDFORPRODUCTION)
- **Part Types**: 4 (Mechanical, Variant, WTPart, Software)

**Recommendation**: Implement **usedIn** and **hasView** relationships first for maximum business value.
