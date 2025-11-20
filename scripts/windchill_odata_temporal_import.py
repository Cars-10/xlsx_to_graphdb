#!/usr/bin/env python3
"""
Fetch temporal version/iteration history from Windchill OData API and import to Neo4j.
Each version/iteration of a part becomes a separate temporal node.
"""

import requests
from requests.auth import HTTPBasicAuth
import json
from neo4j import GraphDatabase
from datetime import datetime
import urllib3
from typing import List, Dict, Any

# Suppress SSL warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── CONFIGURATION ─────────────────────────────────────────────────────
WINDCHILL_BASE_URL = "https://pp-2511150853nt.portal.ptc.io/Windchill/servlet/odata"
WINDCHILL_PRODMGMT_URL = f"{WINDCHILL_BASE_URL}/v7/ProdMgmt/"
WINDCHILL_DOCMGMT_URL = f"{WINDCHILL_BASE_URL}/v7/DocMgmt/"
WINDCHILL_USER = "wcadmin"
WINDCHILL_PASSWORD = "ptc"

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "tstpwdpwd"

# Query limits
MAX_PARTS = 200
MAX_DOCUMENTS = 100

# ───────────────────────────────────────────────────────────────────────

class WindchillODataImporter:
    def __init__(self):
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(WINDCHILL_USER, WINDCHILL_PASSWORD)
        self.session.verify = False  # Disable SSL verification for self-signed certs
        self.session.headers.update({
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self.neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        self.stats = {"parts": 0, "documents": 0, "versions": 0, "iterations": 0, "errors": 0}

    def query_odata(self, base_url: str, entity_set: str, select: str = None, filter: str = None,
                    expand: str = None, top: int = None) -> List[Dict[str, Any]]:
        """Query Windchill OData API"""
        url = f"{base_url}{entity_set}"
        params = {}

        if select:
            params['$select'] = select
        if filter:
            params['$filter'] = filter
        if expand:
            params['$expand'] = expand
        if top:
            params['$top'] = top

        try:
            print(f"Querying: {entity_set}...")
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if 'value' in data:
                results = data['value']
                print(f"  Found {len(results)} results")
                return results
            return []
        except Exception as e:
            print(f"Error querying {entity_set}: {e}")
            self.stats["errors"] += 1
            return []

    def get_all_parts(self) -> List[Dict[str, Any]]:
        """Get all Part objects with version/iteration info"""
        # Query Parts entity set - get all properties
        parts = self.query_odata(
            WINDCHILL_PRODMGMT_URL,
            "Parts",
            top=MAX_PARTS
        )
        return parts

    def get_all_documents(self) -> List[Dict[str, Any]]:
        """Get all Document objects with version/iteration info"""
        documents = self.query_odata(
            WINDCHILL_DOCMGMT_URL,
            "Documents",
            select="ID,Name,Number,State,View,Creator,CreatedOn,Modifier,ModifiedOn,Version,Iteration",
            top=MAX_DOCUMENTS
        )
        return documents

    def get_part_versions(self, part_master_id: str) -> List[Dict[str, Any]]:
        """Get all versions for a specific part master"""
        versions = self.query_odata(
            WINDCHILL_PRODMGMT_URL,
            "WTParts",
            filter=f"MasterReference/ID eq '{part_master_id}'",
            select="ID,Name,Number,State,View,Creator,CreatedOn,Modifier,ModifiedOn,Version,Iteration"
        )
        return versions

    def parse_timestamp(self, timestamp_str: str) -> int:
        """Parse OData timestamp to Unix epoch"""
        if not timestamp_str:
            return 0
        try:
            # OData format: /Date(1234567890000)/
            if timestamp_str.startswith('/Date('):
                ms = int(timestamp_str[6:-2])
                return ms // 1000
            # ISO format
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return int(dt.timestamp())
        except:
            return 0

    def clear_temporal_nodes(self):
        """Clear existing temporal version nodes from Neo4j"""
        print("\nClearing existing temporal nodes...")
        with self.neo4j_driver.session() as session:
            session.run("MATCH (n:PartVersion) DETACH DELETE n")
            session.run("MATCH (n:DocumentVersion) DETACH DELETE n")
        print("  Cleared temporal nodes")

    def import_part_to_neo4j(self, part: Dict[str, Any]):
        """Import a single part version/iteration as a temporal node"""
        with self.neo4j_driver.session() as session:
            query = """
            MERGE (pv:PartVersion {id: $id})
            SET pv.number = $number,
                pv.name = $name,
                pv.version = $version,
                pv.revision = $revision,
                pv.full_identifier = $full_id,
                pv.state = $state,
                pv.view = $view,
                pv.created_date = $created_date,
                pv.created_by = $created_by,
                pv.modified_date = $modified_date,
                pv.modified_by = $modified_by,
                pv.object_type = $object_type
            RETURN pv
            """

            version = part.get('Version', '')
            revision = part.get('Revision', '')
            full_id = f"{part.get('Number', '')}.{revision}"

            # Extract state value - it's an object with Value and Display
            state_obj = part.get('State', {})
            state = state_obj.get('Value', 'UNKNOWN') if isinstance(state_obj, dict) else str(state_obj)

            params = {
                "id": part.get('ID', ''),
                "number": part.get('Number', ''),
                "name": part.get('Name', ''),
                "version": version,
                "revision": revision,
                "full_id": full_id,
                "state": state,
                "view": part.get('View', ''),
                "created_date": self.parse_timestamp(part.get('CreatedOn', '')),
                "created_by": part.get('CreatedBy', ''),
                "modified_date": self.parse_timestamp(part.get('LastModified', '')),
                "modified_by": part.get('ModifiedBy', ''),
                "object_type": part.get('ObjectType', 'WTPart')
            }

            try:
                session.run(query, params)
                self.stats["versions"] += 1
            except Exception as e:
                print(f"  Error importing {full_id}: {e}")
                self.stats["errors"] += 1

    def import_document_to_neo4j(self, doc: Dict[str, Any]):
        """Import a single document version/iteration as a temporal node"""
        with self.neo4j_driver.session() as session:
            query = """
            MERGE (dv:DocumentVersion {id: $id})
            SET dv.number = $number,
                dv.name = $name,
                dv.version = $version,
                dv.iteration = $iteration,
                dv.full_identifier = $full_id,
                dv.state = $state,
                dv.view = $view,
                dv.created_date = $created_date,
                dv.creator = $creator,
                dv.modified_date = $modified_date,
                dv.modifier = $modifier,
                dv.object_type = 'Document'
            RETURN dv
            """

            version = doc.get('Version', '')
            iteration = doc.get('Iteration', '')
            full_id = f"{doc.get('Number', '')}.{version}.{iteration}"

            params = {
                "id": doc.get('ID', ''),
                "number": doc.get('Number', ''),
                "name": doc.get('Name', ''),
                "version": version,
                "iteration": iteration,
                "full_id": full_id,
                "state": doc.get('State', 'UNKNOWN'),
                "view": doc.get('View', ''),
                "created_date": self.parse_timestamp(doc.get('CreatedOn', '')),
                "creator": doc.get('Creator', ''),
                "modified_date": self.parse_timestamp(doc.get('ModifiedOn', '')),
                "modifier": doc.get('Modifier', '')
            }

            try:
                session.run(query, params)
                self.stats["versions"] += 1
            except Exception as e:
                print(f"  Error importing {full_id}: {e}")
                self.stats["errors"] += 1

    def create_version_relationships(self):
        """Create relationships between consecutive versions of the same part"""
        print("\nCreating version evolution relationships...")
        with self.neo4j_driver.session() as session:
            # Link parts by their base number and version sequence
            query = """
            MATCH (p1:PartVersion), (p2:PartVersion)
            WHERE p1.number = p2.number
              AND p1.version < p2.version
              AND NOT exists((p1)-[:EVOLVES_TO]->(:PartVersion))
            WITH p1, p2
            ORDER BY p1.number, p1.version, p2.version
            WITH p1, collect(p2)[0] AS next_version
            WHERE next_version IS NOT NULL
            MERGE (p1)-[:EVOLVES_TO]->(next_version)
            RETURN count(*) AS relationships_created
            """
            result = session.run(query).single()
            print(f"  Created {result['relationships_created']} evolution relationships")

    def run(self):
        """Main import process"""
        print("=" * 70)
        print("Windchill OData Temporal History Importer")
        print("=" * 70)
        print(f"Windchill URL: {WINDCHILL_BASE_URL}")
        print(f"User: {WINDCHILL_USER}")
        print()

        # Test connection
        print("Testing Windchill connection...")
        try:
            response = self.session.get(f"{WINDCHILL_PRODMGMT_URL}$metadata", timeout=10)
            if response.status_code == 200:
                print("✓ Connected to Windchill OData API\n")
            else:
                print(f"✗ Connection failed: {response.status_code}\n")
                return
        except Exception as e:
            print(f"✗ Connection error: {e}\n")
            return

        # Clear existing temporal data
        self.clear_temporal_nodes()

        # Fetch and import parts
        print("\n" + "─" * 70)
        print("Fetching Parts...")
        print("─" * 70)
        parts = self.get_all_parts()

        for i, part in enumerate(parts, 1):
            part_num = part.get('Number', 'unknown')
            version = part.get('Version', '')
            revision = part.get('Revision', '')
            print(f"[{i}/{len(parts)}] {part_num} Rev.{revision} ({version})")
            self.import_part_to_neo4j(part)
            self.stats["parts"] += 1

        # Fetch and import documents
        print("\n" + "─" * 70)
        print("Fetching Documents...")
        print("─" * 70)
        documents = self.get_all_documents()

        for i, doc in enumerate(documents, 1):
            doc_num = doc.get('Number', 'unknown')
            version = doc.get('Version', '')
            iteration = doc.get('Iteration', '')
            print(f"[{i}/{len(documents)}] {doc_num} v{version}.{iteration}")
            self.import_document_to_neo4j(doc)
            self.stats["documents"] += 1

        # Create evolution relationships
        self.create_version_relationships()

        # Summary
        print("\n" + "=" * 70)
        print("Import Complete!")
        print("=" * 70)
        print(f"Parts imported:       {self.stats['parts']}")
        print(f"Documents imported:   {self.stats['documents']}")
        print(f"Total versions:       {self.stats['versions']}")
        print(f"Errors encountered:   {self.stats['errors']}")
        print("=" * 70)

    def close(self):
        """Close connections"""
        self.neo4j_driver.close()
        self.session.close()


if __name__ == "__main__":
    importer = WindchillODataImporter()
    try:
        importer.run()
    finally:
        importer.close()
