
import pandas as pd
import json
from urllib.parse import quote
import shlex
from rdflib import Graph, URIRef, Literal
from rdflib.namespace import RDF

def generate_load_script(bom_file, parts_file):
    """
    Generates a shell script with curl commands to load the BOM data
    into a SPARQL-endpoint graph database.
    """
    with open(parts_file, 'r') as f:
        parts = json.load(f)

    print("#!/bin/bash")
    print("set -x")

    for part_number, part_details in parts.items():
        
        print(f"# Processing part: {part_number}")

        part_number = str(part_number)
        
        part_name = part_details.get('name', '')
        if not part_name:
            part_name = part_number
        
        g = Graph()
        part_uri = URIRef(f"urn:part:{quote(part_number)}")
        g.add((part_uri, RDF.type, URIRef("urn:ontology:Part")))
        g.add((part_uri, URIRef("urn:ontology:name"), Literal(part_name)))
        
        query = g.serialize(format='ntriples')

        print(f"curl -X POST -H 'Content-Type: application/n-triples' --data-binary {shlex.quote(query)} 'http://127.0.0.1:7200/repositories/Snowmobile/statements'")


if __name__ == "__main__":
    generate_load_script('bom.csv', 'parts.json')
