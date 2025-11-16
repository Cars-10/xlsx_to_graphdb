# GraphDB Query Examples for Snowmobile Data

## Repository Information
- **URL**: http://127.0.0.1:7200
- **Repository**: Snowmobile
- **Total Parts**: 939
- **Total Triples**: 15,313

### Relationships
- **hasComponent**: 1,543 (parent → child BOM)
- **usedIn**: 1,543 (child → parent, reverse BOM)
- **partOfAssembly**: 4,616 (transitive closure)
- **hasAlternate**: 8 (variant/replacement parts)

### Part Metadata
- **hasView**: 939 (Design, Manufacturing, Service)
- **hasState**: 939 (RELEASED, DESIGN, INPLANNING, etc.)
- **hasPartType**: 938 (MechanicalPart, Variant, WTPart, SoftwarePart)
- **hasSource**: 939 (make, buy)
- **hasRevision**: 939 (revision identifiers)

## Useful SPARQL Queries

### 1. Get all direct children of a part

```sparql
PREFIX ont: <urn:ontology:>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?childNumber ?childName
WHERE {
  <urn:part:100%20SNOW> ont:hasComponent ?child .
  ?child rdfs:label ?childName .
  BIND(REPLACE(STR(?child), "urn:part:", "") AS ?childNumber)
}
ORDER BY ?childName
```

### 2. Get the complete BOM tree (all descendants)

```sparql
PREFIX ont: <urn:ontology:>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?parentName ?childName
WHERE {
  <urn:part:100%20SNOW> ont:hasComponent+ ?child .
  ?child rdfs:label ?childName .
  ?child ^ont:hasComponent ?parent .
  ?parent rdfs:label ?parentName .
}
ORDER BY ?parentName ?childName
```

### 3. Find parts by name (search)

```sparql
PREFIX ont: <urn:ontology:>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?partNumber ?partName
WHERE {
  ?part a ont:Part ;
        rdfs:label ?partName .
  BIND(REPLACE(STR(?part), "urn:part:", "") AS ?partNumber)
  FILTER(REGEX(?partName, "ENGINE", "i"))
}
ORDER BY ?partName
```

### 4. Count children for each part

```sparql
PREFIX ont: <urn:ontology:>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?partName (COUNT(?child) as ?childCount)
WHERE {
  ?part ont:hasComponent ?child ;
        rdfs:label ?partName .
}
GROUP BY ?partName
ORDER BY DESC(?childCount)
LIMIT 20
```

### 5. Find leaf parts (no children)

```sparql
PREFIX ont: <urn:ontology:>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?partNumber ?partName
WHERE {
  ?part a ont:Part ;
        rdfs:label ?partName .
  FILTER NOT EXISTS { ?part ont:hasComponent ?child }
  BIND(REPLACE(STR(?part), "urn:part:", "") AS ?partNumber)
}
ORDER BY ?partName
LIMIT 50
```

### 6. Find the path from root to a specific part

```sparql
PREFIX ont: <urn:ontology:>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?level0 ?level1 ?level2 ?level3
WHERE {
  <urn:part:100%20SNOW> rdfs:label ?level0 .
  OPTIONAL {
    <urn:part:100%20SNOW> ont:hasComponent ?l1 .
    ?l1 rdfs:label ?level1 .
    OPTIONAL {
      ?l1 ont:hasComponent ?l2 .
      ?l2 rdfs:label ?level2 .
      OPTIONAL {
        ?l2 ont:hasComponent ?l3 .
        ?l3 rdfs:label ?level3 .
      }
    }
  }
}
LIMIT 100
```

### 7. Get part details

```sparql
PREFIX ont: <urn:ontology:>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?property ?value
WHERE {
  <urn:part:100%20SNOW> ?property ?value .
}
```

### 8. Find assemblies with the most components

```sparql
PREFIX ont: <urn:ontology:>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?assemblyName (COUNT(?component) as ?componentCount)
WHERE {
  ?assembly ont:hasComponent ?component ;
            rdfs:label ?assemblyName .
}
GROUP BY ?assemblyName
ORDER BY DESC(?componentCount)
LIMIT 10
```

### 9. Get all alternate/replacement parts for a specific part

```sparql
PREFIX ont: <urn:ontology:>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?alternateName
WHERE {
  <urn:part:5435681> ont:hasAlternate ?alternate .
  ?alternate rdfs:label ?alternateName
}
ORDER BY ?alternateName
```

### 10. Find all parts with alternates/variants

```sparql
PREFIX ont: <urn:ontology:>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?partName (COUNT(?alternate) as ?alternateCount)
WHERE {
  ?part ont:hasAlternate ?alternate ;
        rdfs:label ?partName .
}
GROUP BY ?partName
ORDER BY DESC(?alternateCount)
```

### 11. Get bidirectional alternate relationships

```sparql
PREFIX ont: <urn:ontology:>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?part1Name ?part2Name
WHERE {
  ?part1 ont:hasAlternate ?part2 ;
         rdfs:label ?part1Name .
  ?part2 ont:hasAlternate ?part1 ;
         rdfs:label ?part2Name .
  FILTER(STR(?part1) < STR(?part2))
}
ORDER BY ?part1Name
```

### 12. Find all color variants of a part

```sparql
PREFIX ont: <urn:ontology:>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?basePart ?variantName
WHERE {
  ?base ont:hasAlternate ?variant ;
        rdfs:label ?basePart .
  ?variant rdfs:label ?variantName .
  FILTER(REGEX(?variantName, "BLACK|RED|BLUE|WHITE|ORANGE|LIME|GREEN", "i"))
}
ORDER BY ?basePart ?variantName
```

## Python Query Example

```python
from urllib.request import Request, urlopen
from urllib.parse import urlencode
import json

def query_graphdb(sparql_query):
    url = 'http://127.0.0.1:7200/repositories/Snowmobile'
    data = urlencode({'query': sparql_query}).encode('utf-8')
    req = Request(url, data=data, headers={'Accept': 'application/sparql-results+json'})
    resp = urlopen(req)
    return json.loads(resp.read())

# Example: Get children of 100 SNOW
query = '''
PREFIX ont: <urn:ontology:>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?childName
WHERE {
  <urn:part:100%20SNOW> ont:hasComponent ?child .
  ?child rdfs:label ?childName
}
'''

result = query_graphdb(query)
for binding in result['results']['bindings']:
    print(binding['childName']['value'])
```

## Accessing the GraphDB Workbench

Open in browser: http://127.0.0.1:7200/workbench

From there you can:
- Explore the graph visually
- Run SPARQL queries
- Export data
- View repository statistics

### 13. Where-Used Analysis (usedIn)

```sparql
PREFIX ont: <urn:ontology:>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?assemblyName
WHERE {
  <urn:part:7547332> ont:usedIn ?assembly .
  ?assembly rdfs:label ?assemblyName
}
ORDER BY ?assemblyName
```

### 14. Complete Bill of Materials (partOfAssembly)

```sparql
PREFIX ont: <urn:ontology:>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?partName
WHERE {
  ?part ont:partOfAssembly <urn:part:100%20SNOW> ;
        rdfs:label ?partName
}
ORDER BY ?partName
```

### 15. Filter by Part State (hasState)

```sparql
PREFIX ont: <urn:ontology:>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?partName
WHERE {
  ?part ont:hasState ont:RELEASED ;
        ont:partOfAssembly <urn:part:100%20SNOW> ;
        rdfs:label ?partName
}
ORDER BY ?partName
```

### 16. Manufacturing View BOM (hasView)

```sparql
PREFIX ont: <urn:ontology:>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?parentName ?childName
WHERE {
  ?parent ont:hasComponent ?child ;
          ont:hasView ont:Manufacturing ;
          rdfs:label ?parentName .
  ?child rdfs:label ?childName
}
ORDER BY ?parentName ?childName
```

### 17. Find all Mechanical Parts (hasPartType)

```sparql
PREFIX ont: <urn:ontology:>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?partName
WHERE {
  ?part ont:hasPartType ont:MechanicalPart ;
        rdfs:label ?partName
}
ORDER BY ?partName
LIMIT 50
```

### 18. Count Parts by State

```sparql
PREFIX ont: <urn:ontology:>

SELECT ?state (COUNT(*) as ?count)
WHERE {
  ?part ont:hasState ?state
}
GROUP BY ?state
ORDER BY DESC(?count)
```

### 19. Find Make vs Buy Parts (hasSource)

```sparql
PREFIX ont: <urn:ontology:>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?partName ?source
WHERE {
  ?part ont:hasSource ?source ;
        ont:partOfAssembly <urn:part:100%20SNOW> ;
        rdfs:label ?partName
}
ORDER BY ?source ?partName
LIMIT 50
```

### 20. Complex Query: Released Mechanical Parts Used in Multiple Assemblies

```sparql
PREFIX ont: <urn:ontology:>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?partName (COUNT(?assembly) as ?usageCount)
WHERE {
  ?part ont:hasState ont:RELEASED ;
        ont:hasPartType ont:MechanicalPart ;
        ont:usedIn ?assembly ;
        rdfs:label ?partName
}
GROUP BY ?partName
HAVING (COUNT(?assembly) > 5)
ORDER BY DESC(?usageCount)
```

## Advanced Queries

### Find All Screws in Snowmobile (Transitive + Pattern Matching)

```sparql
PREFIX ont: <urn:ontology:>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?partName
WHERE {
  ?part ont:partOfAssembly <urn:part:100%20SNOW> ;
        rdfs:label ?partName .
  FILTER(REGEX(?partName, "SCR-", "i"))
}
ORDER BY ?partName
```

### BOM Explosion with Depth Levels

```sparql
PREFIX ont: <urn:ontology:>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?l0Name ?l1Name ?l2Name
WHERE {
  <urn:part:100%20SNOW> rdfs:label ?l0Name ;
                         ont:hasComponent ?l1 .
  ?l1 rdfs:label ?l1Name .
  OPTIONAL {
    ?l1 ont:hasComponent ?l2 .
    ?l2 rdfs:label ?l2Name
  }
}
ORDER BY ?l1Name ?l2Name
LIMIT 100
```

### Impact Analysis: What's Affected by Changing This Part?

```sparql
PREFIX ont: <urn:ontology:>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?affectedAssembly ?assemblyName
WHERE {
  # Direct usage
  <urn:part:7547332> ont:usedIn ?affectedAssembly .
  ?affectedAssembly rdfs:label ?assemblyName .
  
  # Also find higher-level assemblies
  OPTIONAL {
    ?affectedAssembly ont:usedIn ?higherLevel .
    ?higherLevel rdfs:label ?higherLevelName
  }
}
ORDER BY ?assemblyName
```
