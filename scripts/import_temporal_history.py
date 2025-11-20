#!/usr/bin/env python3
"""
Import temporal version history from Windchill to Neo4j.
Fetches all parts/documents and their version history, creating temporal nodes.
"""

import requests
import json
from neo4j import GraphDatabase
from datetime import datetime
import time

# Configuration
WINDCHILL_API = "http://localhost:3000"
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "tstpwdpwd"

# Limits
MAX_PARTS = 500
MAX_DOCUMENTS = 200
MAX_CHANGES = 100


class WindchillTemporalImporter:
    def __init__(self):
        self.neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        self.stats = {"parts": 0, "documents": 0, "changes": 0, "versions": 0, "errors": 0}

    def call_windchill_tool(self, tool_name, params=None):
        """Call a Windchill MCP tool"""
        try:
            payload = {
                "tool": tool_name,
                "arguments": params or {}
            }
            response = requests.post(f"{WINDCHILL_API}/call", json=payload, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error calling {tool_name}: {e}")
            self.stats["errors"] += 1
            return None

    def get_all_parts(self):
        """Get all parts from Windchill"""
        print(f"Fetching parts (limit {MAX_PARTS})...")
        result = self.call_windchill_tool("part_search", {"number": "*", "limit": MAX_PARTS})
        if result and "result" in result:
            parts = result["result"].get("parts", [])
            print(f"Found {len(parts)} parts")
            return parts
        return []

    def get_all_documents(self):
        """Get all documents from Windchill"""
        print(f"Fetching documents (limit {MAX_DOCUMENTS})...")
        result = self.call_windchill_tool("document_search", {"number": "*", "limit": MAX_DOCUMENTS})
        if result and "result" in result:
            docs = result["result"].get("documents", [])
            print(f"Found {len(docs)} documents")
            return docs
        return []

    def get_part_history(self, part_id):
        """Get version history for a part"""
        result = self.call_windchill_tool("part_get_version_history", {"id": part_id})
        if result and "result" in result:
            return result["result"].get("versions", [])
        return []

    def get_document_history(self, doc_id):
        """Get version history for a document"""
        result = self.call_windchill_tool("document_get_version_history", {"id": doc_id})
        if result and "result" in result:
            return result["result"].get("versions", [])
        return []

    def parse_date(self, date_str):
        """Parse various date formats to Unix timestamp"""
        if not date_str:
            return 0
        try:
            # Try ISO format first
            if isinstance(date_str, str):
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                return int(dt.timestamp())
            return 0
        except:
            return 0

    def import_part_versions_to_neo4j(self, part_base, versions):
        """Import all versions/iterations of a part as temporal nodes"""
        with self.neo4j_driver.session() as session:
            for version in versions:
                # Each version might have multiple iterations
                iterations = version.get("iterations", [version])  # Use version itself if no iterations

                for iteration in iterations:
                    # Create a temporal node for this specific version/iteration
                    query = """
                    CREATE (pv:PartVersion)
                    SET pv.base_number = $base_number,
                        pv.base_name = $base_name,
                        pv.version = $version,
                        pv.iteration = $iteration,
                        pv.full_identifier = $full_id,
                        pv.state = $state,
                        pv.created_date = $created_date,
                        pv.creator = $creator,
                        pv.modifier = $modifier,
                        pv.modified_date = $modified_date,
                        pv.object_type = 'WTPart',
                        pv.is_latest = $is_latest
                    """

                    params = {
                        "base_number": part_base.get("number", ""),
                        "base_name": part_base.get("name", ""),
                        "version": version.get("version", ""),
                        "iteration": iteration.get("iteration", ""),
                        "full_id": f"{part_base.get('number', '')}.{version.get('version', '')}.{iteration.get('iteration', '')}",
                        "state": iteration.get("state", "UNKNOWN"),
                        "created_date": self.parse_date(iteration.get("createdOn") or iteration.get("created")),
                        "creator": iteration.get("creator", ""),
                        "modifier": iteration.get("modifier", ""),
                        "modified_date": self.parse_date(iteration.get("modifiedOn") or iteration.get("modified")),
                        "is_latest": iteration.get("isLatest", False)
                    }

                    try:
                        session.run(query, params)
                        self.stats["versions"] += 1
                    except Exception as e:
                        print(f"Error importing {params['full_id']}: {e}")
                        self.stats["errors"] += 1

    def import_document_versions_to_neo4j(self, doc_base, versions):
        """Import all versions/iterations of a document as temporal nodes"""
        with self.neo4j_driver.session() as session:
            for version in versions:
                iterations = version.get("iterations", [version])

                for iteration in iterations:
                    query = """
                    CREATE (dv:DocumentVersion)
                    SET dv.base_number = $base_number,
                        dv.base_name = $base_name,
                        dv.version = $version,
                        dv.iteration = $iteration,
                        dv.full_identifier = $full_id,
                        dv.state = $state,
                        dv.created_date = $created_date,
                        dv.creator = $creator,
                        dv.modifier = $modifier,
                        dv.modified_date = $modified_date,
                        dv.object_type = 'Document',
                        dv.is_latest = $is_latest
                    """

                    params = {
                        "base_number": doc_base.get("number", ""),
                        "base_name": doc_base.get("name", ""),
                        "version": version.get("version", ""),
                        "iteration": iteration.get("iteration", ""),
                        "full_id": f"{doc_base.get('number', '')}.{version.get('version', '')}.{iteration.get('iteration', '')}",
                        "state": iteration.get("state", "UNKNOWN"),
                        "created_date": self.parse_date(iteration.get("createdOn") or iteration.get("created")),
                        "creator": iteration.get("creator", ""),
                        "modifier": iteration.get("modifier", ""),
                        "modified_date": self.parse_date(iteration.get("modifiedOn") or iteration.get("modified")),
                        "is_latest": iteration.get("isLatest", False)
                    }

                    try:
                        session.run(query, params)
                        self.stats["versions"] += 1
                    except Exception as e:
                        print(f"Error importing {params['full_id']}: {e}")
                        self.stats["errors"] += 1

    def clear_temporal_nodes(self):
        """Clear existing temporal version nodes"""
        print("Clearing existing temporal nodes...")
        with self.neo4j_driver.session() as session:
            session.run("MATCH (n:PartVersion) DELETE n")
            session.run("MATCH (n:DocumentVersion) DELETE n")
            print("Cleared temporal nodes")

    def run(self):
        """Main import process"""
        print("=" * 60)
        print("Windchill Temporal History Importer")
        print("=" * 60)

        # Clear existing temporal data
        self.clear_temporal_nodes()

        # Get all parts
        parts = self.get_all_parts()

        # Process each part
        for i, part in enumerate(parts[:MAX_PARTS], 1):
            part_id = part.get("id") or part.get("oid")
            part_num = part.get("number", "unknown")

            print(f"[{i}/{len(parts)}] Processing part {part_num} (id: {part_id})...")

            if part_id:
                # Get version history
                versions = self.get_part_history(part_id)
                if versions:
                    print(f"  Found {len(versions)} versions")
                    self.import_part_versions_to_neo4j(part, versions)
                    self.stats["parts"] += 1
                else:
                    print(f"  No version history found")

            # Rate limiting
            if i % 10 == 0:
                time.sleep(0.5)

        # Get all documents
        try:
            documents = self.get_all_documents()

            for i, doc in enumerate(documents[:MAX_DOCUMENTS], 1):
                doc_id = doc.get("id") or doc.get("oid")
                doc_num = doc.get("number", "unknown")

                print(f"[{i}/{len(documents)}] Processing document {doc_num}...")

                if doc_id:
                    versions = self.get_document_history(doc_id)
                    if versions:
                        print(f"  Found {len(versions)} versions")
                        self.import_document_versions_to_neo4j(doc, versions)
                        self.stats["documents"] += 1

                if i % 10 == 0:
                    time.sleep(0.5)
        except Exception as e:
            print(f"Document processing skipped: {e}")

        print("\n" + "=" * 60)
        print("Import Complete!")
        print("=" * 60)
        print(f"Parts processed:     {self.stats['parts']}")
        print(f"Documents processed: {self.stats['documents']}")
        print(f"Total versions:      {self.stats['versions']}")
        print(f"Errors:              {self.stats['errors']}")
        print("=" * 60)

    def close(self):
        """Close connections"""
        self.neo4j_driver.close()


if __name__ == "__main__":
    importer = WindchillTemporalImporter()
    try:
        importer.run()
    finally:
        importer.close()
