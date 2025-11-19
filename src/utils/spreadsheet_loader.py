import sys
import json
import logging
import argparse
import base64
import warnings
from typing import Dict, List, Optional, Tuple, Iterable, Set, Union
from urllib.parse import quote
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

import pandas as pd
from rdflib import Graph, URIRef, Literal
from rdflib.namespace import RDF, RDFS

# Neo4j driver (optional dependency)
try:
    from neo4j import GraphDatabase
    NEO4J_AVAILABLE = True
except ImportError:
    NEO4J_AVAILABLE = False
    logging.debug("neo4j driver not available")

warnings.filterwarnings(
    "ignore",
    r"Workbook contains no default style.*",
    UserWarning,
    r"openpyxl\.styles\.stylesheet",
)

# Configure default logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def normalize_part_number(value) -> str:
    if pd.isna(value):
        return ""
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return str(value)
    return str(value)


class SpreadsheetParser:
    def __init__(self, excel_path: str, warn_missing_required: bool = True):
        self.excel_path = excel_path
        self.warn_missing_required = warn_missing_required

    def get_sheet_names(self) -> List[str]:
        xls = pd.ExcelFile(self.excel_path)
        return [str(s) for s in xls.sheet_names]

    def parse_parts(self, sheets: Optional[List[str]] = None) -> Dict[str, Dict[str, Optional[str]]]:
        parts: Dict[str, Dict[str, Optional[str]]] = {}
        sheet_names = sheets or self.get_sheet_names()
        for sheet_name in sheet_names:
            try:
                df = pd.read_excel(self.excel_path, sheet_name=sheet_name, skiprows=4)
                if df.empty or len(df.columns) == 0:
                    df = pd.read_excel(self.excel_path, sheet_name=sheet_name)
                if len(df.index) > 0:
                    first = list(df.iloc[0].values)
                    first_str = set(map(str, first))
                    required = {"Number", "Name"}
                    if required.issubset(first_str) and not required.issubset(set(map(str, df.columns))):
                        df.columns = df.iloc[0]
                        df = df[1:]
                required = {"Number", "Name"}
                if not required.issubset(set(df.columns)):
                    if self.warn_missing_required:
                        logging.warning("Sheet %s missing required columns; found: %s", sheet_name, list(df.columns))
                    else:
                        logging.debug("Skipping sheet %s; required cols missing", sheet_name)
                    continue
                for _, row in df.iterrows():
                    part_number = normalize_part_number(row.get("Number"))
                    if not part_number:
                        continue
                    name = row.get("Name")
                    name = str(name) if pd.notna(name) else part_number

                    # Determine part type from sheet name
                    part_type = None
                    if "MechanicalPart" in sheet_name:
                        part_type = "MechanicalPart"
                    elif "SoftwarePart" in sheet_name:
                        part_type = "SoftwarePart"
                    elif "Variant" in sheet_name:
                        part_type = "Variant"
                    elif "WTPart" in sheet_name:
                        part_type = "WTPart"
                    elif "BasicNode" in sheet_name:
                        part_type = "BasicNode"
                    elif "StructureNode" in sheet_name:
                        part_type = "StructureNode"

                    parts[part_number] = {
                        "name": name,
                        "type": str(row.get("Type")) if "Type" in df.columns and pd.notna(row.get("Type")) else None,
                        "source": str(row.get("Source")).lower() if "Source" in df.columns and pd.notna(row.get("Source")) else None,
                        "view": str(row.get("View")) if "View" in df.columns and pd.notna(row.get("View")) else None,
                        "state": str(row.get("State")) if "State" in df.columns and pd.notna(row.get("State")) else None,
                        "revision": str(row.get("Revision")) if "Revision" in df.columns and pd.notna(row.get("Revision")) else None,
                        "container": str(row.get("Container")) if "Container" in df.columns and pd.notna(row.get("Container")) else None,
                        "part_type": part_type,
                    }
            except Exception as e:
                logging.error("Error reading sheet %s: %s", sheet_name, e)
                continue
        return parts

    def build_cross_index(self, sheets: Optional[List[str]] = None) -> Tuple[Dict[str, str], Dict[str, List[Dict[str, Optional[str]]]]]:
        pn_to_name: Dict[str, str] = {}
        name_sources: Dict[str, List[Dict[str, Optional[str]]]] = {}
        sheet_names = sheets or self.get_sheet_names()
        for sheet_name in sheet_names:
            try:
                df = pd.read_excel(self.excel_path, sheet_name=sheet_name, skiprows=4)
                if df.empty or len(df.columns) == 0:
                    df = pd.read_excel(self.excel_path, sheet_name=sheet_name)
                if len(df.index) > 0:
                    first = list(df.iloc[0].values)
                    first_str = set(map(str, first))
                    required = {"Number", "Name"}
                    if required.issubset(first_str) and not required.issubset(set(map(str, df.columns))):
                        df.columns = df.iloc[0]
                        df = df[1:]
                cols = set(df.columns)
                if {"Number", "Name"}.issubset(cols):
                    for _, row in df.iterrows():
                        pn = normalize_part_number(row.get("Number"))
                        nm = row.get("Name")
                        if pn and pd.notna(nm):
                            name = str(nm).strip()
                            pn_to_name[pn] = name
                            meta = {
                                "sheet": sheet_name,
                                "revision": str(row.get("Revision")) if "Revision" in cols and pd.notna(row.get("Revision")) else None,
                                "view": str(row.get("View")) if "View" in cols and pd.notna(row.get("View")) else None,
                                "container": str(row.get("Container")) if "Container" in cols and pd.notna(row.get("Container")) else None,
                            }
                            lst = name_sources.get(name)
                            if lst is None:
                                name_sources[name] = [meta]
                            else:
                                lst.append(meta)
            except Exception:
                continue
        return pn_to_name, name_sources

    def parse_bom_csv(self, bom_csv_path: Optional[str]) -> List[Tuple[str, str]]:
        """Parse simple parent-child BOM CSV or hierarchical BOM CSV."""
        if not bom_csv_path:
            return []
        try:
            df = pd.read_csv(bom_csv_path)
        except Exception as e:
            logging.error("Error reading BOM CSV %s: %s", bom_csv_path, e)
            return []
        
        # Build case-insensitive column mapping
        col_map = {str(c).strip(): c for c in df.columns}
        lower_cols = {k.lower(): k for k in col_map.keys()}
        
        # Check if this is a hierarchical BOM (has Level column and Number)
        if "number" in lower_cols and "level" in lower_cols:
            logging.info("Detected hierarchical BOM format")
            return self._parse_hierarchical_bom_csv(df, col_map[lower_cols["number"]], col_map[lower_cols["level"]])
        
        # Check for simple parent-child BOM
        parent_col = child_col = None
        if "number" in lower_cols and "component id" in lower_cols:
            parent_col = col_map[lower_cols["number"]]
            child_col = col_map[lower_cols["component id"]]
        elif "parent number" in lower_cols and "child number" in lower_cols:
            parent_col = col_map[lower_cols["parent number"]]
            child_col = col_map[lower_cols["child number"]]
        
        if not parent_col:
            logging.warning("BOM CSV missing expected columns; found: %s", list(df.columns))
            return []
        
        edges: List[Tuple[str, str]] = []
        for _, row in df.iterrows():
            parent = normalize_part_number(row.get(parent_col))
            child = normalize_part_number(row.get(child_col))
            if parent and child:
                edges.append((parent, child))
        
        logging.info(f"Parsed {len(edges)} BOM relationships from simple CSV using columns: {parent_col} -> {child_col}")
        return edges

    def _parse_hierarchical_bom_csv(self, df: pd.DataFrame, number_col: str, level_col: str) -> List[Tuple[str, str]]:
        """
        Parse hierarchical BOM where relationships are implied by Level column.
        
        Creates parent-child relationships by:
        - Finding parts at each level
        - Linking each part to its nearest parent at the previous level
        """
        edges: List[Tuple[str, str]] = []
        level_parts: Dict[int, List[str]] = {}
        
        # Group parts by level
        for _, row in df.iterrows():
            level_val = row.get(level_col)
            number_val = row.get(number_col)
            
            # Skip rows without valid level or number
            if pd.isna(level_val) or pd.isna(number_val):
                continue
            
            try:
                level = int(level_val)
                part_num = normalize_part_number(number_val)
                if part_num:
                    if level not in level_parts:
                        level_parts[level] = []
                    level_parts[level].append(part_num)
            except (ValueError, TypeError):
                continue
        
        logging.info(f"Found parts at levels: {sorted(level_parts.keys())}")
        
        # Build parent-child relationships
        for level in sorted(level_parts.keys()):
            if level == 0:
                continue  # Root parts have no parent
            
            # Find parent parts at the previous level
            parent_level = level - 1
            if parent_level not in level_parts:
                logging.warning(f"Level {level} has parts but no parent level {parent_level} found")
                continue
            
            parent_parts = level_parts[parent_level]
            child_parts = level_parts[level]
            
            # Link each child to a parent (using sequential assignment)
            logging.debug(f"Linking {len(child_parts)} parts at level {level} to {len(parent_parts)} parts at level {parent_level}")
            
            for i, child in enumerate(child_parts):
                # Find parent using index (sequential assignment)
                parent_idx = i % len(parent_parts) if parent_parts else 0
                parent = parent_parts[parent_idx] if parent_parts else None
                
                if parent:
                    edges.append((parent, child))
        
        logging.info(f"Built {len(edges)} hierarchical BOM relationships")
        return edges

    def parse_bom_csv_by_name(self, bom_csv_path: Optional[str]) -> List[Tuple[str, str]]:
        if not bom_csv_path:
            return []
        try:
            df = pd.read_csv(bom_csv_path)
        except Exception as e:
            logging.error("Error reading BOM CSV %s: %s", bom_csv_path, e)
            return []
        # Drop duplicated header row inside data if present
        if len(df) > 0:
            first_vals = [str(v).strip() for v in df.iloc[0].values]
            col_vals = [str(c).strip() for c in df.columns]
            if set(first_vals) == set(col_vals) or set(first_vals).issuperset({"Name", "Component Id"}) or set(first_vals).issuperset({"Parent Name", "Child Name"}):
                df = df[1:]
        col_map = {str(c).strip(): c for c in df.columns}
        lower = {k.lower() for k in col_map.keys()}
        parent_col = child_col = None
        if {"parent name", "child name"}.issubset(lower):
            parent_col = next((col_map[k] for k in col_map if k.lower() == "parent name"), None)
            child_col = next((col_map[k] for k in col_map if k.lower() == "child name"), None)
        elif {"name", "component name"}.issubset(lower):
            parent_col = next((col_map[k] for k in col_map if k.lower() == "name"), None)
            child_col = next((col_map[k] for k in col_map if k.lower() == "component name"), None)
        elif {"name", "component id"}.issubset(lower):
            parent_col = next((col_map[k] for k in col_map if k.lower() == "name"), None)
            child_col = next((col_map[k] for k in col_map if k.lower() == "component id"), None)
        if not parent_col:
            logging.warning("Name-based BOM CSV missing expected columns; found: %s", list(df.columns))
            return []
        edges: List[Tuple[str, str]] = []
        for _, row in df.iterrows():
            p = row.get(parent_col)
            c = row.get(child_col)
            if pd.isna(p) or pd.isna(c):
                continue
            p_s = str(p).strip()
            c_s = str(c).strip()
            if not p_s or not c_s:
                continue
            edges.append((p_s, c_s))
        return edges

    def parse_alternate_links(self, excel_path: Optional[str] = None) -> List[Tuple[str, str, str]]:
        """
        Parse alternate/replacement links from WTPartAlternateLink-Sheet.

        Returns:
            List of (child_part, replacement_part, replacement_type) tuples
        """
        if excel_path is None:
            excel_path = self.excel_path

        links: List[Tuple[str, str, str]] = []
        try:
            df = pd.read_excel(excel_path, sheet_name='WTPartAlternateLink-Sheet', skiprows=4)

            # Check for duplicate header
            if len(df) > 0:
                first_row = df.iloc[0]
                if 'Action' in str(first_row.values):
                    df.columns = df.iloc[0]
                    df = df[1:]

            # Check if required columns exist
            if 'Child Part Number' not in df.columns or 'Replacement Part Number' not in df.columns:
                logging.debug("WTPartAlternateLink-Sheet missing required columns")
                return []

            for _, row in df.iterrows():
                child = row.get('Child Part Number')
                replacement = row.get('Replacement Part Number')
                rtype = row.get('Replacement Type', 'alternate')

                if pd.notna(child) and pd.notna(replacement):
                    child_num = normalize_part_number(child)
                    replacement_num = normalize_part_number(replacement)
                    rtype_str = str(rtype).strip() if pd.notna(rtype) else 'alternate'

                    if child_num and replacement_num:
                        links.append((child_num, replacement_num, rtype_str))

            logging.info(f"Parsed {len(links)} alternate/replacement links")
            return links

        except Exception as e:
            logging.warning(f"Could not parse alternate links: {e}")
            return []

    def parse_describe_links(self, excel_path: Optional[str] = None) -> List[Tuple[str, str, Optional[str], Optional[str], Optional[str]]]:
        if excel_path is None:
            excel_path = self.excel_path
        try:
            df = pd.read_excel(excel_path, sheet_name='WTPartDescribeLink-Sheet', skiprows=4)
            if len(df) > 0:
                first_row = df.iloc[0]
                if 'Action' in str(first_row.values):
                    df.columns = df.iloc[0]
                    df = df[1:]
            required = {'Document Number', 'Part Number'}
            if not required.issubset(set(df.columns)):
                logging.debug("WTPartDescribeLink-Sheet missing required columns")
                return []
            links: List[Tuple[str, str, Optional[str], Optional[str], Optional[str]]] = []
            for _, row in df.iterrows():
                dnum = row.get('Document Number')
                pnum = row.get('Part Number')
                if pd.isna(dnum) or pd.isna(pnum):
                    continue
                d = normalize_part_number(dnum)
                p = normalize_part_number(pnum)
                dred = str(row.get('Document Revision')).strip() if 'Document Revision' in df.columns and pd.notna(row.get('Document Revision')) else None
                dorg = str(row.get('Document Owning Organization')).strip() if 'Document Owning Organization' in df.columns and pd.notna(row.get('Document Owning Organization')) else None
                dcont = str(row.get('Document Container')).strip() if 'Document Container' in df.columns and pd.notna(row.get('Document Container')) else None
                if d and p:
                    links.append((d, p, dred, dorg, dcont))
            logging.info(f"Parsed {len(links)} describe links")
            return links
        except Exception as e:
            logging.debug(f"Could not parse describe links: {e}")
            return []


class GraphDBClient:
    def __init__(self, base_url: str, repository: str, username: Optional[str] = None, password: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self.repository = repository
        self.username = username
        self.password = password

    def _auth_header(self) -> Dict[str, str]:
        if self.username and self.password:
            token = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
            return {"Authorization": f"Basic {token}"}
        return {}

    def statements_endpoint(self) -> str:
        return f"{self.base_url}/repositories/{self.repository}/statements"

    def verify_connection(self) -> bool:
        url = f"{self.base_url}/repositories"
        req = Request(url, headers={"Accept": "application/json", **self._auth_header()})
        try:
            with urlopen(req) as resp:
                content = resp.read()
        except HTTPError as e:
            logging.error("GraphDB verify failed: HTTP %s", e.code)
            return False
        except URLError as e:
            logging.error("GraphDB verify failed: %s", e.reason)
            return False
        try:
            data = json.loads(content.decode())
            repos = [r.get("id") for r in data]
            if self.repository in repos:
                return True
            logging.error("Repository %s not found; available: %s", self.repository, repos)
            return False
        except Exception:
            logging.warning("Non-JSON response from /repositories; assuming reachable")
            return True

    def post_ntriples(self, ntriples: bytes) -> bool:
        req = Request(
            self.statements_endpoint(),
            data=ntriples,
            headers={
                "Content-Type": "application/n-triples",
                **self._auth_header(),
            },
            method="POST",
        )
        try:
            with urlopen(req) as resp:
                resp.read()
            return True
        except HTTPError as e:
            logging.error("POST failed: HTTP %s", e.code)
            return False
        except URLError as e:
            logging.error("POST failed: %s", e.reason)
            return False


class Neo4jClient:
    """
    Neo4j client for importing parts and BOM data as a property graph.
    Creates visually stunning nodes with:
    - Name as the primary display label
    - Rich styling based on part properties (type, source, state, view)
    - Hierarchical relationships (hasComponent)
    - Alternate/replacement links
    """

    def __init__(self, uri: str = "bolt://localhost:7687", database: str = "neo4j",
                 username: str = "neo4j", password: str = "password"):
        if not NEO4J_AVAILABLE:
            raise ImportError("neo4j driver not installed. Install with: pip install neo4j")

        self.uri = uri
        self.database = database
        self.username = username
        self.password = password
        self.driver = None

    def connect(self) -> bool:
        """Establish connection to Neo4j."""
        try:
            logging.info(f"Attempting to connect to Neo4j at {self.uri}, database: {self.database}")
            self.driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
            # Verify connection
            with self.driver.session(database=self.database) as session:
                result = session.run("RETURN 1")
                result.single()
            logging.info(f"Successfully connected to Neo4j at {self.uri}, database: {self.database}")
            return True
        except Exception as e:
            logging.error(f"Failed to connect to Neo4j at {self.uri}: {type(e).__name__}: {e}")
            import traceback
            logging.error(f"Traceback: {traceback.format_exc()}")
            return False

    def close(self):
        """Close Neo4j connection."""
        if self.driver:
            self.driver.close()

    def clear_database(self):
        """Clear all nodes and relationships (use with caution!)."""
        with self.driver.session(database=self.database) as session:
            session.run("MATCH (n) DETACH DELETE n")
            logging.info("Cleared all nodes and relationships")

    def create_constraints(self):
        """Create uniqueness constraints and indexes for better performance."""
        try:
            logging.info("Creating Neo4j constraints and indexes...")
            with self.driver.session(database=self.database) as session:
                # Create constraint on Part number
                try:
                    logging.debug("Creating uniqueness constraint on Part.number")
                    session.run("CREATE CONSTRAINT part_number IF NOT EXISTS FOR (p:Part) REQUIRE p.number IS UNIQUE")
                    logging.debug("Constraint created successfully")
                except Exception as e:
                    logging.warning(f"Constraint creation note: {type(e).__name__}: {e}")

                # Create indexes for better query performance
                try:
                    logging.debug("Creating indexes on Part properties")
                    session.run("CREATE INDEX part_name IF NOT EXISTS FOR (p:Part) ON (p.name)")
                    session.run("CREATE INDEX part_type IF NOT EXISTS FOR (p:Part) ON (p.partType)")
                    session.run("CREATE INDEX part_source IF NOT EXISTS FOR (p:Part) ON (p.source)")
                    session.run("CREATE INDEX part_state IF NOT EXISTS FOR (p:Part) ON (p.state)")
                    logging.debug("Indexes created successfully")
                except Exception as e:
                    logging.warning(f"Index creation note: {type(e).__name__}: {e}")

                logging.info("Created constraints and indexes")
        except Exception as e:
            logging.error(f"Failed to create constraints/indexes: {type(e).__name__}: {e}")
            import traceback
            logging.error(f"Traceback: {traceback.format_exc()}")
            raise

    def import_parts(self, parts: Dict[str, Dict[str, Optional[str]]], batch_size: int = 1000):
        """
        Import parts as nodes with rich properties and visual styling.

        Each part node will have:
        - number: unique identifier
        - name: display name (used as primary label)
        - partType: MechanicalPart, SoftwarePart, Variant, WTPart
        - source: make, buy
        - state: RELEASED, DESIGN, INPLANNING, etc.
        - view: Design, Manufacturing, Service
        - revision: version/revision number
        - container: organizational container
        - displayColor: color code based on properties
        - size: node size based on importance
        """

        if not parts:
            logging.warning("No parts to import")
            return

        # Define color scheme for visual appeal
        color_map = {
            'MechanicalPart': '#3498db',  # Blue
            'SoftwarePart': '#9b59b6',     # Purple
            'Variant': '#e74c3c',          # Red
            'WTPart': '#2ecc71',           # Green
            'default': '#95a5a6'           # Gray
        }

        source_colors = {
            'make': '#27ae60',  # Green
            'buy': '#e67e22',   # Orange
        }

        state_colors = {
            'RELEASED': '#27ae60',      # Green
            'DESIGN': '#3498db',        # Blue
            'INPLANNING': '#f39c12',    # Yellow
            'UNDERREVIEW': '#e67e22',   # Orange
            'default': '#95a5a6'        # Gray
        }

        parts_list = []
        for part_number, details in parts.items():
            name = details.get("name") or part_number
            part_type = details.get("part_type") or "Part"
            source = details.get("source")
            state = details.get("state")

            # Determine display color (priority: state > source > part_type)
            if state and state in state_colors:
                display_color = state_colors[state]
            elif source and source in source_colors:
                display_color = source_colors[source]
            elif part_type in color_map:
                display_color = color_map[part_type]
            else:
                display_color = color_map['default']

            # Determine node size based on complexity (has more metadata = larger)
            metadata_count = sum([
                1 if details.get("type") else 0,
                1 if details.get("source") else 0,
                1 if details.get("view") else 0,
                1 if details.get("state") else 0,
                1 if details.get("revision") else 0,
                1 if details.get("container") else 0,
            ])
            node_size = 30 + (metadata_count * 5)  # Base 30, +5 per metadata field

            part_node = {
                'number': part_number,
                'name': name,
                'partType': part_type,
                'type': details.get("type"),
                'source': source,
                'state': state,
                'view': details.get("view"),
                'revision': details.get("revision"),
                'container': details.get("container"),
                'displayColor': display_color,
                'size': node_size,
            }
            parts_list.append(part_node)

        # Import in batches
        try:
            logging.info(f"Starting to import {len(parts_list)} parts in batches of {batch_size}")
            with self.driver.session(database=self.database) as session:
                for i in range(0, len(parts_list), batch_size):
                    batch = parts_list[i:i + batch_size]
                    batch_num = i // batch_size + 1

                    logging.debug(f"Processing batch {batch_num}: {len(batch)} parts")

                    query = """
                    UNWIND $parts AS part
                    MERGE (p:Part {number: part.number})
                    SET p.name = part.name,
                        p.partType = part.partType,
                        p.type = part.type,
                        p.source = part.source,
                        p.state = part.state,
                        p.view = part.view,
                        p.revision = part.revision,
                        p.container = part.container,
                        p.displayColor = part.displayColor,
                        p.size = part.size
                    """

                    try:
                        session.run(query, parts=batch)
                        logging.info(f"Imported parts batch {batch_num}: {len(batch)} parts")
                    except Exception as e:
                        logging.error(f"Failed to import batch {batch_num}: {type(e).__name__}: {e}")
                        logging.error(f"First part in failed batch: {batch[0] if batch else 'N/A'}")
                        import traceback
                        logging.error(f"Traceback: {traceback.format_exc()}")
                        raise

            logging.info(f"Successfully imported {len(parts_list)} parts total")
        except Exception as e:
            logging.error(f"Failed to import parts: {type(e).__name__}: {e}")
            import traceback
            logging.error(f"Traceback: {traceback.format_exc()}")
            raise

    def import_bom_relationships(self, edges: List[Tuple[str, str]], batch_size: int = 1000):
        """
        Import BOM relationships as :HAS_COMPONENT edges.

        Creates directed relationships from parent to child parts.
        """

        if not edges:
            logging.warning("No BOM relationships to import")
            return

        edges_list = [{'parent': parent, 'child': child} for parent, child in edges]
        logging.info(f"Starting to import {len(edges_list)} BOM relationships in batches of {batch_size}")

        total_batches = (len(edges_list) + batch_size - 1) // batch_size
        logging.info(f"Total batches to process: {total_batches}")

        with self.driver.session(database=self.database) as session:
            for i in range(0, len(edges_list), batch_size):
                batch = edges_list[i:i + batch_size]
                batch_num = i // batch_size + 1

                logging.debug(f"Processing batch {batch_num}: {len(batch)} relationships")
                logging.debug(f"Sample relationships in this batch:")
                for j, edge in enumerate(batch[:3]):  # Show first 3 relationships
                    logging.debug(f"  {j+1}. {edge['parent']} -> {edge['child']}")

                query = """
                UNWIND $edges AS edge
                MERGE (parent:Part {number: edge.parent})
                  ON CREATE SET parent.name = edge.parent, parent.partType = "MissingPart"
                MERGE (child:Part {number: edge.child})
                  ON CREATE SET child.name = edge.child, child.partType = "MissingPart"
                MERGE (parent)-[r:HAS_COMPONENT]->(child)
                """

                try:
                    result = session.run(query, edges=batch)
                    logging.info(f"Successfully imported BOM batch {batch_num}: {len(batch)} relationships")
                    
                    # Count the relationships that were actually created
                    summary = result.consume()
                    if summary:
                        logging.debug(f"Batch {batch_num} counters: {summary.counters}")
                    
                except Exception as e:
                    logging.error(f"Failed to import BOM batch {batch_num}: {type(e).__name__}: {e}")
                    logging.error(f"Failed batch contained {len(batch)} relationships")
                    if batch:
                        logging.error(f"First failed relationship: {batch[0]}")
                    import traceback
                    logging.error(f"Traceback: {traceback.format_exc()}")
                    raise

        logging.info(f"Successfully imported {len(edges_list)} BOM relationships total")

    def import_part_usage(self, usages: List[Dict[str, Optional[str]]], batch_size: int = 1000):
        if not usages:
            logging.info("No part usage relationships to import")
            return
        with self.driver.session(database=self.database) as session:
            for i in range(0, len(usages), batch_size):
                batch = usages[i:i + batch_size]
                query = """
                UNWIND $rows AS row
                MERGE (parent:Part {number: row.parent})
                MERGE (child:Part {number: row.child})
                MERGE (parent)-[r:PART_USAGE]->(child)
                FOREACH(ignore IN CASE WHEN row.quantity IS NULL THEN [] ELSE [1] END | SET r.quantity = row.quantity)
                FOREACH(ignore IN CASE WHEN row.uom IS NULL THEN [] ELSE [1] END | SET r.uom = row.uom)
                FOREACH(ignore IN CASE WHEN row.findNumber IS NULL THEN [] ELSE [1] END | SET r.findNumber = row.findNumber)
                FOREACH(ignore IN CASE WHEN row.lineNumber IS NULL THEN [] ELSE [1] END | SET r.lineNumber = row.lineNumber)
                FOREACH(ignore IN CASE WHEN row.referenceDesignators IS NULL THEN [] ELSE [1] END | SET r.referenceDesignators = row.referenceDesignators)
                FOREACH(ignore IN CASE WHEN row.traceCode IS NULL THEN [] ELSE [1] END | SET r.traceCode = row.traceCode)
                FOREACH(ignore IN CASE WHEN row.componentId IS NULL THEN [] ELSE [1] END | SET r.componentId = row.componentId)
                FOREACH(ignore IN CASE WHEN row.view IS NULL THEN [] ELSE [1] END | SET r.view = row.view)
                """
                session.run(query, rows=batch)
                logging.info(f"Imported part usage batch {i // batch_size + 1}: {len(batch)} relationships")

    def import_alternate_links(self, links: List[Tuple[str, str, str]], batch_size: int = 1000):
        """
        Import alternate/replacement relationships.

        Creates relationships with type information (alternate, replacement, etc.)
        """

        if not links:
            logging.info("No alternate/replacement links to import")
            return

        links_list = [{'child': child, 'replacement': repl, 'type': rtype}
                      for child, repl, rtype in links]

        with self.driver.session(database=self.database) as session:
            for i in range(0, len(links_list), batch_size):
                batch = links_list[i:i + batch_size]

                query = """
                UNWIND $links AS link
                MERGE (child:Part {number: link.child})
                  ON CREATE SET child.name = link.child, child.partType = "MissingPart"
                MERGE (replacement:Part {number: link.replacement})
                  ON CREATE SET replacement.name = link.replacement, replacement.partType = "MissingPart"
                MERGE (child)-[r:HAS_ALTERNATE]->(replacement)
                SET r.type = link.type
                """

                session.run(query, links=batch)
                logging.info(f"Imported alternate links batch {i // batch_size + 1}: {len(batch)} relationships")

        logging.info(f"Imported {len(links_list)} alternate/replacement links total")

    def import_describe_links(self, links: List[Tuple[str, str, Optional[str], Optional[str], Optional[str]]], batch_size: int = 1000):
        if not links:
            logging.info("No describe links to import")
            return
        links_list = [{'doc': d, 'part': p, 'revision': r, 'org': o, 'container': c} for d, p, r, o, c in links]
        with self.driver.session(database=self.database) as session:
            for i in range(0, len(links_list), batch_size):
                batch = links_list[i:i + batch_size]
                query = """
                UNWIND $links AS link
                MERGE (d:Document {number: link.doc})
                FOREACH(ignore IN CASE WHEN link.revision IS NULL THEN [] ELSE [1] END | SET d.revision = link.revision)
                FOREACH(ignore IN CASE WHEN link.org IS NULL THEN [] ELSE [1] END | SET d.organization = link.org)
                FOREACH(ignore IN CASE WHEN link.container IS NULL THEN [] ELSE [1] END | SET d.container = link.container)
                MERGE (p:Part {number: link.part})
                MERGE (d)-[:DESCRIBES]->(p)
                """
                session.run(query, links=batch)
                logging.info(f"Imported describe links batch {i // batch_size + 1}: {len(batch)} relationships")

    def import_used_in(self, edges: List[Tuple[str, str]], batch_size: int = 1000):
        if not edges:
            logging.info("No usedIn relationships to import")
            return
        rows = [{'parent': p, 'child': c} for p, c in edges]
        with self.driver.session(database=self.database) as session:
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                query = """
                UNWIND $rows AS row
                MERGE (parent:Part {number: row.parent})
                MERGE (child:Part {number: row.child})
                MERGE (child)-[:USED_IN]->(parent)
                """
                session.run(query, rows=batch)
                logging.info(f"Imported usedIn batch {i // batch_size + 1}: {len(batch)} relationships")

    def import_part_of_assembly(self, edges: List[Tuple[str, str]], batch_size: int = 1000):
        if not edges:
            logging.info("No partOfAssembly relationships to import")
            return
        children: Dict[str, List[str]] = {}
        for p, c in edges:
            lst = children.get(p)
            if lst is None:
                children[p] = [c]
            else:
                lst.append(c)
        all_parts = set()
        for p, c in edges:
            all_parts.add(p)
            all_parts.add(c)
        pairs: List[Tuple[str, str]] = []
        def dfs(a: str, visited: Optional[Set[str]] = None) -> Set[str]:
            if visited is None:
                visited = set()
            if a in visited:
                return set()
            visited.add(a)
            desc = set()
            for ch in children.get(a, []):
                desc.add(ch)
                desc.update(dfs(ch, visited))
            return desc
        for anc in all_parts:
            for desc in dfs(anc):
                pairs.append((anc, desc))
        if not pairs:
            logging.info("No transitive pairs computed for partOfAssembly")
            return
        rows = [{'ancestor': a, 'descendant': d} for a, d in pairs]
        with self.driver.session(database=self.database) as session:
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                query = """
                UNWIND $rows AS row
                MERGE (ancestor:Part {number: row.ancestor})
                MERGE (descendant:Part {number: row.descendant})
                MERGE (descendant)-[:PART_OF_ASSEMBLY]->(ancestor)
                """
                session.run(query, rows=batch)
                logging.info(f"Imported partOfAssembly batch {i // batch_size + 1}: {len(batch)} relationships")

    def get_statistics(self) -> Dict[str, int]:
        """Get database statistics."""
        with self.driver.session(database=self.database) as session:
            # Count nodes
            result = session.run("MATCH (p:Part) RETURN count(p) AS count")
            part_count = result.single()["count"]
            result = session.run("MATCH (d:Document) RETURN count(d) AS count")
            doc_count = result.single()["count"]

            # Count relationships
            result = session.run("MATCH ()-[r:HAS_COMPONENT]->() RETURN count(r) AS count")
            bom_count = result.single()["count"]

            result = session.run("MATCH ()-[r:HAS_ALTERNATE]->() RETURN count(r) AS count")
            alt_count = result.single()["count"]
            result = session.run("MATCH ()-[r:PART_USAGE]->() RETURN count(r) AS count")
            usage_count = result.single()["count"]
            result = session.run("MATCH ()-[r:DESCRIBES]->() RETURN count(r) AS count")
            describe_count = result.single()["count"]
            result = session.run("MATCH ()-[r:USED_IN]->() RETURN count(r) AS count")
            used_in_count = result.single()["count"]
            result = session.run("MATCH ()-[r:PART_OF_ASSEMBLY]->() RETURN count(r) AS count")
            poa_count = result.single()["count"]

            return {
                'parts': part_count,
                'documents': doc_count,
                'bom_relationships': bom_count,
                'alternate_links': alt_count,
                'describe_links': describe_count,
                'part_usage': usage_count,
                'used_in': used_in_count,
                'part_of_assembly': poa_count,
                'total_relationships': bom_count + alt_count + describe_count + used_in_count + poa_count
            }


def build_part_triples(parts: Dict[str, Dict[str, Optional[str]]]) -> Iterable[Tuple[URIRef, Tuple[URIRef, URIRef, Union[URIRef, Literal]]]]:
    """Build RDF triples for parts including all metadata properties."""
    for part_number, details in parts.items():
        subj = URIRef(f"urn:part:{quote(part_number)}")
        name = details.get("name") or part_number

        # Basic part properties
        yield subj, (subj, RDF.type, URIRef("urn:ontology:Part"))
        yield subj, (subj, URIRef("urn:ontology:name"), Literal(name))
        yield subj, (subj, RDFS.label, Literal(name))

        # Part type (MechanicalPart, SoftwarePart, Variant, WTPart)
        part_type = details.get("part_type")
        if part_type:
            yield subj, (subj, URIRef("urn:ontology:hasPartType"), URIRef(f"urn:ontology:{part_type}"))

        # View (Design, Manufacturing, Service)
        view = details.get("view")
        if view:
            yield subj, (subj, URIRef("urn:ontology:hasView"), URIRef(f"urn:ontology:{view}"))

        # State (RELEASED, DESIGN, INPLANNING, etc.)
        state = details.get("state")
        if state:
            yield subj, (subj, URIRef("urn:ontology:hasState"), URIRef(f"urn:ontology:{state}"))

        # Source (make, buy)
        source = details.get("source")
        if source:
            yield subj, (subj, URIRef("urn:ontology:hasSource"), URIRef(f"urn:ontology:{source}"))

        # Revision
        revision = details.get("revision")
        if revision:
            yield subj, (subj, URIRef("urn:ontology:hasRevision"), Literal(revision))

        # Container/organizational location
        container = details.get("container")
        if container:
            yield subj, (subj, URIRef("urn:ontology:inContainer"), Literal(container))


def build_bom_triples(edges: List[Tuple[str, str]]) -> Iterable[Tuple[URIRef, Tuple[URIRef, URIRef, URIRef]]]:
    pred = URIRef("urn:ontology:hasComponent")
    for parent, child in edges:
        parent_uri = URIRef(f"urn:part:{quote(parent)}")
        child_uri = URIRef(f"urn:part:{quote(child)}")
        yield parent_uri, (parent_uri, pred, child_uri)


def build_alternate_triples(links: List[Tuple[str, str, str]]) -> Iterable[Tuple[URIRef, Tuple[URIRef, URIRef, URIRef]]]:
    """
    Build RDF triples for alternate/replacement part relationships.

    Args:
        links: List of (original_part, replacement_part, replacement_type) tuples

    Yields:
        Tuples of (subject_uri, (subject, predicate, object)) for RDF graph
    """
    pred = URIRef("urn:ontology:hasAlternate")
    for original, replacement, rtype in links:
        original_uri = URIRef(f"urn:part:{quote(original)}")
        replacement_uri = URIRef(f"urn:part:{quote(replacement)}")
        yield original_uri, (original_uri, pred, replacement_uri)

def build_document_triples(docs: List[Tuple[str, Optional[str], Optional[str], Optional[str]]]) -> Iterable[Tuple[URIRef, Tuple[URIRef, URIRef, Union[URIRef, Literal]]]]:
    for dnum, dred, dorg, dcont in docs:
        subj = URIRef(f"urn:document:{quote(dnum)}")
        yield subj, (subj, RDF.type, URIRef("urn:ontology:Document"))
        if dred:
            yield subj, (subj, URIRef("urn:ontology:hasRevision"), Literal(dred))
        if dorg:
            yield subj, (subj, URIRef("urn:ontology:hasOrganization"), Literal(dorg))
        if dcont:
            yield subj, (subj, URIRef("urn:ontology:inContainer"), Literal(dcont))

def build_describe_triples(links: List[Tuple[str, str]]) -> Iterable[Tuple[URIRef, Tuple[URIRef, URIRef, URIRef]]]:
    pred = URIRef("urn:ontology:describes")
    for dnum, pnum in links:
        d_uri = URIRef(f"urn:document:{quote(dnum)}")
        p_uri = URIRef(f"urn:part:{quote(pnum)}")
        yield d_uri, (d_uri, pred, p_uri)


def build_used_in_triples(edges: List[Tuple[str, str]]) -> Iterable[Tuple[URIRef, Tuple[URIRef, URIRef, URIRef]]]:
    """
    Build RDF triples for usedIn relationships (reverse of hasComponent).

    Args:
        edges: List of (parent, child) tuples from BOM

    Yields:
        Tuples of (subject_uri, (child, usedIn, parent)) for RDF graph
    """
    pred = URIRef("urn:ontology:usedIn")
    for parent, child in edges:
        child_uri = URIRef(f"urn:part:{quote(child)}")
        parent_uri = URIRef(f"urn:part:{quote(parent)}")
        yield child_uri, (child_uri, pred, parent_uri)


def build_part_of_assembly_triples(edges: List[Tuple[str, str]]) -> Iterable[Tuple[URIRef, Tuple[URIRef, URIRef, URIRef]]]:
    """
    Build RDF triples for partOfAssembly relationships (transitive closure of hasComponent).

    Args:
        edges: List of (parent, child) tuples from BOM

    Yields:
        Tuples of (subject_uri, (descendant, partOfAssembly, ancestor)) for all levels
    """
    pred = URIRef("urn:ontology:partOfAssembly")

    # Build adjacency list for traversal
    children: Dict[str, List[str]] = {}
    for parent, child in edges:
        if parent not in children:
            children[parent] = []
        children[parent].append(child)

    # For each part, find all its descendants using DFS
    def get_all_descendants(part: str, visited: Optional[Set[str]] = None) -> Set[str]:
        if visited is None:
            visited = set()
        if part in visited:
            return set()
        visited.add(part)

        descendants = set()
        for child in children.get(part, []):
            descendants.add(child)
            descendants.update(get_all_descendants(child, visited))
        return descendants

    # Generate triples for all ancestor-descendant pairs
    all_parts = set()
    for parent, child in edges:
        all_parts.add(parent)
        all_parts.add(child)

    for ancestor in all_parts:
        descendants = get_all_descendants(ancestor)
        ancestor_uri = URIRef(f"urn:part:{quote(ancestor)}")
        for descendant in descendants:
            descendant_uri = URIRef(f"urn:part:{quote(descendant)}")
            yield descendant_uri, (descendant_uri, pred, ancestor_uri)


def batch_serialize(triples: Iterable[Tuple[URIRef, Tuple]], batch_size: int = 1000) -> Iterable[bytes]:
    g = Graph()
    count = 0
    for _, triple in triples:
        g.add(triple)
        count += 1
        if count >= batch_size:
            yield g.serialize(format="ntriples", encoding="utf-8")
            g = Graph()
            count = 0
    if count:
        yield g.serialize(format="ntriples", encoding="utf-8")


def build_name_index(parts: Dict[str, Dict[str, Optional[str]]]) -> Tuple[Dict[str, str], Dict[str, List[str]]]:
    pn_to_name: Dict[str, str] = {}
    name_to_pn: Dict[str, List[str]] = {}
    for pn, details in parts.items():
        name = str(details.get("name") or pn).strip()
        pn_to_name[pn] = name
        lst = name_to_pn.get(name)
        if lst is None:
            name_to_pn[name] = [pn]
        else:
            lst.append(pn)
    return pn_to_name, name_to_pn


def resolve_edges_by_name(name_edges: List[Tuple[str, str]], name_to_pn: Dict[str, List[str]], strict: bool, parts: Optional[Dict[str, Dict[str, Optional[str]]]] = None) -> List[Tuple[str, str]]:
    resolved: List[Tuple[str, str]] = []
    skipped = 0
    for p_name, c_name in name_edges:
        p_key = (p_name or "").strip()
        c_key = (c_name or "").strip()
        p_list = name_to_pn.get(p_key)
        c_list = name_to_pn.get(c_key)
        if (not p_list) and parts and p_key in parts:
            p_list = [p_key]
        if (not c_list) and parts and c_key in parts:
            c_list = [c_key]
        if not p_list or not c_list:
            if strict:
                raise RuntimeError("Unknown part name in BOM: %s or %s" % (p_name, c_name))
            skipped += 1
            continue
        if len(p_list) != 1 or len(c_list) != 1:
            if strict:
                raise RuntimeError("Ambiguous part name in BOM: %s or %s" % (p_name, c_name))
            skipped += 1
            continue
        resolved.append((p_list[0], c_list[0]))
    if skipped:
        logging.warning("Skipped %d name-based BOM edges due to unknown or ambiguous names", skipped)
    return resolved


def generate_bom_by_name_file(
    excel_path: str,
    bom_csv_path: str,
    out_path: str,
    sheets: Optional[List[str]] = None,
) -> int:
    parser = SpreadsheetParser(excel_path)
    parts = parser.parse_parts(sheets)
    if not parts:
        raise RuntimeError("No parts parsed from Excel")
    pn_to_name, _ = SpreadsheetParser(excel_path).build_cross_index()
    try:
        df = pd.read_csv(bom_csv_path)
    except Exception as e:
        logging.error("Error reading BOM CSV %s: %s", bom_csv_path, e)
        return 0
    col_map = {str(c).strip(): c for c in df.columns}
    lower = {k.lower() for k in col_map.keys()}
    if {"parent name", "child name"}.issubset(lower):
        parent_col = next((col_map[k] for k in col_map if k.lower() == "parent name"), None)
        child_col = next((col_map[k] for k in col_map if k.lower() == "child name"), None)
        names = df[[parent_col, child_col]].copy()
        names.columns = ["Parent Name", "Child Name"]
        names.to_csv(out_path, index=False)
        logging.info("Generated name-based BOM %s from existing name CSV (%d rows)", out_path, len(names))
        return len(names)
    rows: List[Tuple[str, str]] = []
    unmapped = 0
    edges = parser.parse_bom_csv(bom_csv_path)
    for parent_pn, child_pn in edges:
        p_name = pn_to_name.get(parent_pn, parent_pn)
        c_name = pn_to_name.get(child_pn, child_pn)
        if p_name == parent_pn or c_name == child_pn:
            unmapped += 1
        rows.append((p_name, c_name))
    df_out = pd.DataFrame(rows, columns=["Parent Name", "Child Name"])
    df_out.to_csv(out_path, index=False)
    logging.info("Generated name-based BOM %s with %d rows; fallbacks %d", out_path, len(rows), unmapped)
    return len(rows)


def generate_bom_from_excel(
    excel_path: str,
    out_parent_child_path: str,
    out_name_bom_path: str,
    sheets: Optional[List[str]] = None,
) -> Tuple[int, int]:
    parser = SpreadsheetParser(excel_path)
    parts = parser.parse_parts(sheets)
    if not parts:
        raise RuntimeError("No parts parsed from Excel")
    pn_to_name, _ = parser.build_cross_index(sheets)

    def read_sheet(sheet_name: str) -> Optional[pd.DataFrame]:
        try:
            df = pd.read_excel(excel_path, sheet_name=sheet_name, skiprows=4)
        except Exception:
            try:
                df = pd.read_excel(excel_path, sheet_name=sheet_name)
            except Exception:
                return None
        if len(df) > 0:
            first_vals = list(df.iloc[0].values)
            if 'Action' in str(first_vals) or set(map(str, first_vals)) == set(map(str, df.columns)):
                df.columns = df.iloc[0]
                df = df[1:]
        return df

    edges: List[Tuple[str, str]] = []

    sheet_names = parser.get_sheet_names()
    for sheet_name in sheet_names:
        df = read_sheet(sheet_name)
        if df is None or df.empty:
            continue
        cols = {str(c).strip().lower(): c for c in df.columns}

        # Hierarchical BOM: Number + Level
        if 'number' in cols and 'level' in cols:
            level_stack: Dict[int, str] = {}
            for _, row in df.iterrows():
                num = row.get(cols['number'])
                lvl = row.get(cols['level'])
                if pd.isna(num) or pd.isna(lvl):
                    continue
                try:
                    level = int(lvl)
                except Exception:
                    continue
                part_num = normalize_part_number(num)
                if not part_num:
                    continue
                level_stack[level] = part_num
                if level > 0 and (level - 1) in level_stack:
                    parent = level_stack[level - 1]
                    edges.append((parent, part_num))
                for l in list(level_stack.keys()):
                    if l > level:
                        del level_stack[l]
            continue

        # Simple parent-child BOM: various column pairs
        parent_keys = ['parent number', 'parent', 'number', 'parent part number']
        child_keys = ['child number', 'child', 'component id', 'child part number']
        parent_col = child_col = None
        for pk in parent_keys:
            if pk in cols:
                parent_col = cols[pk]
                break
        for ck in child_keys:
            if ck in cols:
                child_col = cols[ck]
                break
        if parent_col and child_col:
            for _, row in df.iterrows():
                parent = normalize_part_number(row.get(parent_col))
                child = normalize_part_number(row.get(child_col))
                if parent and child:
                    edges.append((parent, child))
            continue

        # Name-based BOM within Excel: Parent Name / Child Name
        if 'parent name' in cols and 'child name' in cols:
            parent_name_col = cols['parent name']
            child_name_col = cols['child name']
            for _, row in df.iterrows():
                p_name = row.get(parent_name_col)
                c_name = row.get(child_name_col)
                if pd.isna(p_name) or pd.isna(c_name):
                    continue
                p_name_s = str(p_name).strip()
                c_name_s = str(c_name).strip()
                parent_pn = next((pn for pn, nm in pn_to_name.items() if nm == p_name_s), None)
                child_pn = next((pn for pn, nm in pn_to_name.items() if nm == c_name_s), None)
                if parent_pn and child_pn:
                    edges.append((parent_pn, child_pn))

    # Deduplicate edges
    edges = list(dict.fromkeys(edges))

    df_pc = pd.DataFrame(edges, columns=['Parent Number','Child Number'])
    df_pc.to_csv(out_parent_child_path, index=False)

    name_rows = []
    unmapped = 0
    for p, c in edges:
        p_name = pn_to_name.get(p, p)
        c_name = pn_to_name.get(c, c)
        if p_name == p or c_name == c:
            unmapped += 1
        name_rows.append((p_name, c_name))
    df_name = pd.DataFrame(name_rows, columns=['Parent Name','Child Name'])
    df_name.to_csv(out_name_bom_path, index=False)

    # Alternate links CSV
    alt_links = parser.parse_alternate_links(excel_path)
    if out_name_bom_path.endswith('_bom_by_name.csv'):
        alt_path = out_name_bom_path.replace('_bom_by_name.csv', '_alternate_links.csv')
    else:
        alt_path = out_name_bom_path + '.alternate_links.csv'
    if alt_links:
        pd.DataFrame(alt_links, columns=['Child Number','Replacement Number','Type']).to_csv(alt_path, index=False)
    else:
        pd.DataFrame(columns=['Child Number','Replacement Number','Type']).to_csv(alt_path, index=False)

    logging.info("Generated BOM from Excel: %d edges across sheets (name fallbacks %d)", len(edges), unmapped)
    return len(edges), (len(name_rows) - unmapped)


def dump_name_index(excel_path: str, out_path: str, sheets: Optional[List[str]] = None) -> int:
    parser = SpreadsheetParser(excel_path)
    pn_to_name, _ = parser.build_cross_index(sheets)
    rows = [(pn, nm) for pn, nm in pn_to_name.items()]
    df = pd.DataFrame(rows, columns=["Part Number", "Name"])
    df.to_csv(out_path, index=False)
    logging.info("Dumped name index to %s (%d entries)", out_path, len(rows))
    return len(rows)


def parse_part_usage(excel_path: str) -> List[Dict[str, Optional[str]]]:
    try:
        df = pd.read_excel(excel_path, sheet_name='BOMSheet1', skiprows=4)
        if df.empty or len(df.columns) == 0:
            df = pd.read_excel(excel_path, sheet_name='BOMSheet1')
        if len(df) > 0:
            first_vals = list(df.iloc[0].values)
            if 'Action' in str(first_vals) or set(map(str, first_vals)) == set(map(str, df.columns)):
                df.columns = df.iloc[0]
                df = df[1:]
    except Exception:
        return []
    cols = {str(c).strip().lower(): c for c in df.columns}
    if 'number' not in cols or 'level' not in cols:
        return []
    usages: List[Dict[str, Optional[str]]] = []
    level_stack: Dict[int, str] = {}
    for _, row in df.iterrows():
        num = row.get(cols['number'])
        lvl = row.get(cols['level'])
        if pd.isna(num) or pd.isna(lvl):
            continue
        try:
            level = int(lvl)
        except Exception:
            continue
        part_num = normalize_part_number(num)
        if not part_num:
            continue
        level_stack[level] = part_num
        if level > 0 and (level - 1) in level_stack:
            parent = level_stack[level - 1]
            child = part_num
            quantity = row.get(cols.get('quantity')) if 'quantity' in cols else None
            uom = str(row.get(cols.get('unit of measure'))).strip() if 'unit of measure' in cols and pd.notna(row.get(cols.get('unit of measure'))) else None
            find_number = str(row.get(cols.get('find number'))).strip() if 'find number' in cols and pd.notna(row.get(cols.get('find number'))) else None
            line_number = str(row.get(cols.get('line number'))).strip() if 'line number' in cols and pd.notna(row.get(cols.get('line number'))) else None
            reference_designators = str(row.get(cols.get('reference designators'))).strip() if 'reference designators' in cols and pd.notna(row.get(cols.get('reference designators'))) else None
            trace_code = str(row.get(cols.get('trace code'))).strip() if 'trace code' in cols and pd.notna(row.get(cols.get('trace code'))) else None
            component_id = str(row.get(cols.get('component id'))).strip() if 'component id' in cols and pd.notna(row.get(cols.get('component id'))) else None
            view = str(row.get(cols.get('view'))).strip() if 'view' in cols and pd.notna(row.get(cols.get('view'))) else None
            usages.append({
                'parent': parent,
                'child': child,
                'quantity': quantity if (quantity is None or pd.notna(quantity)) else None,
                'uom': uom,
                'findNumber': find_number,
                'lineNumber': line_number,
                'referenceDesignators': reference_designators,
                'traceCode': trace_code,
                'componentId': component_id,
                'view': view,
            })
        for l in list(level_stack.keys()):
            if l > level:
                del level_stack[l]
    return usages

def emit_bom_name_candidates(excel_path: str, bom_csv_path: str, out_path: str, sheets: Optional[List[str]] = None) -> int:
    parser = SpreadsheetParser(excel_path)
    pn_to_name, _ = parser.build_cross_index(sheets)
    try:
        df = pd.read_csv(bom_csv_path)
    except Exception as e:
        logging.error("Error reading BOM CSV %s: %s", bom_csv_path, e)
        return 0
    col_map = {str(c).strip(): c for c in df.columns}
    lower = {k.lower() for k in col_map.keys()}
    rows: List[Tuple[str, str, str, str]] = []
    if {"parent name", "child name"}.issubset(lower):
        parent_col = next((col_map[k] for k in col_map if k.lower() == "parent name"), None)
        child_col = next((col_map[k] for k in col_map if k.lower() == "child name"), None)
        for _, row in df.iterrows():
            p_name = str(row.get(parent_col)).strip() if pd.notna(row.get(parent_col)) else ""
            c_name = str(row.get(child_col)).strip() if pd.notna(row.get(child_col)) else ""
            if not p_name or not c_name:
                continue
            # Resolve via precomputed index
            p_num = next((pn for pn, nm in pn_to_name.items() if nm == p_name), None)
            c_num = next((pn for pn, nm in pn_to_name.items() if nm == c_name), None)
            rows.append((p_num or p_name, p_name, c_num or c_name, c_name))
    else:
        edges = parser.parse_bom_csv(bom_csv_path)
        for parent_pn, child_pn in edges:
            p_name = pn_to_name.get(parent_pn, parent_pn)
            c_name = pn_to_name.get(child_pn, child_pn)
            rows.append((parent_pn, p_name, child_pn, c_name))
    df_out = pd.DataFrame(rows, columns=["Parent Number", "Parent Name", "Child Number", "Child Name"])
    df_out.to_csv(out_path, index=False)
    logging.info("Emitted BOM name candidates to %s (%d rows)", out_path, len(rows))
    return len(rows)


def import_data(
    excel_path: str,
    bom_csv_path: Optional[str],
    client: GraphDBClient,
    sheets: Optional[List[str]] = None,
    batch_size: int = 1000,
    dry_run: bool = False,
    bom_by_name: bool = False,
    strict_names: bool = False,
    quiet_missing_sheets: bool = False,
    debug_names: bool = False,
    resolution_report: Optional[str] = None,
    skip_log: Optional[str] = None,
    add_edge_labels: bool = False,
) -> Tuple[int, int]:
    parser = SpreadsheetParser(excel_path, warn_missing_required=not quiet_missing_sheets)
    parts = parser.parse_parts(sheets)
    if not parts:
        raise RuntimeError("No parts parsed from Excel")

    pn_to_name, _ = parser.build_cross_index(sheets)
    if not pn_to_name:
        pn_to_name, _ = build_name_index(parts)
    name_to_pn: Dict[str, List[str]] = {}
    for pn, nm in pn_to_name.items():
        lst = name_to_pn.get(nm)
        if lst is None:
            name_to_pn[nm] = [pn]
        else:
            lst.append(pn)
    edges: List[Tuple[str, str]] = []
    # Conditional routing: choose parser based on flag or header auto-detect
    if bom_csv_path:
        if bom_by_name:
            name_edges = parser.parse_bom_csv_by_name(bom_csv_path)
            if name_edges:
                # Detailed debug report
                report_rows: List[Dict[str, str]] = []
                resolved = []
                unknown = ambiguous = 0
                log_lines: List[str] = []
                for p_name, c_name in name_edges:
                    p_candidates = name_to_pn.get(p_name.strip()) or []
                    c_candidates = name_to_pn.get(c_name.strip()) or []
                    status = ""
                    chosen_p = chosen_c = None
                    if not p_candidates or not c_candidates:
                        status = "unknown"
                        unknown += 1
                        log_lines.append(f"unknown parent_name={p_name} child_name={c_name} parent_candidates={','.join(p_candidates)} child_candidates={','.join(c_candidates)}")
                    elif len(p_candidates) != 1 or len(c_candidates) != 1:
                        status = "ambiguous"
                        ambiguous += 1
                        log_lines.append(f"ambiguous parent_name={p_name} child_name={c_name} parent_candidates={','.join(p_candidates)} child_candidates={','.join(c_candidates)}")
                    else:
                        status = "resolved"
                        chosen_p = p_candidates[0]
                        chosen_c = c_candidates[0]
                        resolved.append((chosen_p, chosen_c))
                    if debug_names or resolution_report:
                        report_rows.append({
                            "parent_name": p_name,
                            "child_name": c_name,
                            "parent_candidates": ",".join(p_candidates) if p_candidates else "",
                            "child_candidates": ",".join(c_candidates) if c_candidates else "",
                            "chosen_parent": chosen_p or "",
                            "chosen_child": chosen_c or "",
                            "status": status,
                        })
                if resolution_report and report_rows:
                    df = pd.DataFrame(report_rows)
                    df.to_csv(resolution_report, index=False)
                    logging.info("Wrote name resolution report to %s (%d rows)", resolution_report, len(report_rows))
                if debug_names:
                    logging.info("Name resolution: %d resolved, %d unknown, %d ambiguous", len(resolved), unknown, ambiguous)
                if skip_log and log_lines:
                    try:
                        with open(skip_log, "w", encoding="utf-8") as f:
                            for line in log_lines:
                                f.write(line + "\n")
                        logging.info("Wrote skipped entries log to %s (%d entries)", skip_log, len(log_lines))
                    except Exception as e:
                        logging.error("Failed to write skip log %s: %s", skip_log, e)
                if strict_names and (unknown > 0 or ambiguous > 0):
                    raise RuntimeError(f"Name resolution failed: {unknown} unknown, {ambiguous} ambiguous")
                edges.extend(resolved)
        else:
            edges = parser.parse_bom_csv(bom_csv_path)

    # Build part triples with all metadata
    part_triples = list(build_part_triples(parts))
    logging.info("Built part triples with metadata for %d parts", len(parts))

    # Build BOM relationship triples
    bom_triples = list(build_bom_triples(edges)) if edges else []
    logging.info("Built %d hasComponent relationships", len(bom_triples))

    # Build usedIn triples (reverse of hasComponent)
    used_in_triples = list(build_used_in_triples(edges)) if edges else []
    logging.info("Built %d usedIn relationships", len(used_in_triples))

    # Build partOfAssembly triples (transitive closure)
    logging.info("Computing transitive closure for partOfAssembly relationships...")
    part_of_assembly_triples = list(build_part_of_assembly_triples(edges)) if edges else []
    logging.info("Built %d partOfAssembly relationships", len(part_of_assembly_triples))

    # Parse and build alternate/replacement link triples
    alternate_links = parser.parse_alternate_links()
    alternate_triples = list(build_alternate_triples(alternate_links)) if alternate_links else []
    logging.info("Built %d hasAlternate relationships", len(alternate_triples))

    describe_links = parser.parse_describe_links(excel_path)
    describe_triples = []
    document_triples = []
    if describe_links:
        describe_triples = list(build_describe_triples([(d, p) for d, p, _, _, _ in describe_links]))
        # Build unique document nodes
        doc_meta: Dict[str, Tuple[Optional[str], Optional[str], Optional[str]]] = {}
        for d, _, dred, dorg, dcont in describe_links:
            doc_meta[d] = (dred, dorg, dcont)
        document_triples = list(build_document_triples([(d, *meta) for d, meta in doc_meta.items()]))
    logging.info("Built %d describes relationships and %d document nodes", len(describe_triples), len(document_triples))

    total_triples = (len(part_triples) + len(bom_triples) + len(used_in_triples) +
                     len(part_of_assembly_triples) + len(alternate_triples) + len(describe_triples) + len(document_triples))
    logging.info("Total triples prepared: %d", total_triples)
    logging.info("  Part metadata: %d", len(part_triples))
    logging.info("  hasComponent: %d", len(bom_triples))
    logging.info("  usedIn: %d", len(used_in_triples))
    logging.info("  partOfAssembly: %d", len(part_of_assembly_triples))
    logging.info("  hasAlternate: %d", len(alternate_triples))
    logging.info("  describes: %d", len(describe_triples))
    logging.info("  documents: %d", len(document_triples))

    if not dry_run:
        if not client.verify_connection():
            raise RuntimeError("GraphDB connection verification failed")

    posted = 0

    if add_edge_labels:
        g_labels = Graph()
        g_labels.add((URIRef("urn:ontology:hasComponent"), RDFS.label, Literal("hasComponent")))
        g_labels.add((URIRef("urn:ontology:usedIn"), RDFS.label, Literal("usedIn")))
        g_labels.add((URIRef("urn:ontology:partOfAssembly"), RDFS.label, Literal("partOfAssembly")))
        g_labels.add((URIRef("urn:ontology:hasAlternate"), RDFS.label, Literal("hasAlternate")))
        g_labels.add((URIRef("urn:ontology:describes"), RDFS.label, Literal("describes")))
        label_bytes = g_labels.serialize(format="ntriples", encoding="utf-8")
        if dry_run:
            logging.info("Dry run: would post edge label chunk of size %d bytes", len(label_bytes))
        else:
            ok = client.post_ntriples(label_bytes)
            if not ok:
                raise RuntimeError("Failed to post edge label chunk to GraphDB")
        posted += 1
        logging.info("Posted chunk %d", posted)

    all_triples = (part_triples + bom_triples + used_in_triples +
                   part_of_assembly_triples + alternate_triples + describe_triples + document_triples)
    for chunk in batch_serialize(all_triples, batch_size=batch_size):
        if dry_run:
            logging.info("Dry run: would post chunk of size %d bytes", len(chunk))
        else:
            ok = client.post_ntriples(chunk)
            if not ok:
                raise RuntimeError("Failed to post chunk to GraphDB")
        posted += 1
        logging.info("Posted chunk %d", posted)

    return total_triples, posted


def import_data_neo4j(
    excel_path: str,
    bom_csv_path: Optional[str],
    client: 'Neo4jClient',
    sheets: Optional[List[str]] = None,
    batch_size: int = 1000,
    bom_by_name: bool = False,
    strict_names: bool = False,
    quiet_missing_sheets: bool = False,
    debug_names: bool = False,
    resolution_report: Optional[str] = None,
    skip_log: Optional[str] = None,
) -> Dict[str, int]:
    """
    Import Excel data into Neo4j as a property graph.

    Returns statistics dictionary with counts of imported entities.
    """

    try:
        logging.info("=" * 60)
        logging.info("Starting Neo4j import process")
        logging.info(f"Excel file: {excel_path}")
        logging.info(f"BOM file: {bom_csv_path}")
        logging.info(f"Batch size: {batch_size}")
        logging.info("=" * 60)

        # Parse parts from Excel
        logging.info("Step 1: Parsing parts from Excel...")
        parser = SpreadsheetParser(excel_path, warn_missing_required=not quiet_missing_sheets)
        parts = parser.parse_parts(sheets)
        if not parts:
            raise RuntimeError("No parts parsed from Excel")

        logging.info(f"✓ Parsed {len(parts)} parts from Excel")
    except Exception as e:
        logging.error(f"Failed during initial parsing: {type(e).__name__}: {e}")
        import traceback
        logging.error(f"Traceback: {traceback.format_exc()}")
        raise

    # Build name index for name-based BOM resolution
    pn_to_name, _ = parser.build_cross_index(sheets)
    if not pn_to_name:
        pn_to_name, _ = build_name_index(parts)

    name_to_pn: Dict[str, List[str]] = {}
    for pn, nm in pn_to_name.items():
        lst = name_to_pn.get(nm)
        if lst is None:
            name_to_pn[nm] = [pn]
        else:
            lst.append(pn)

    # Parse BOM relationships
    edges: List[Tuple[str, str]] = []
    if bom_csv_path:
        if bom_by_name:
            name_edges = parser.parse_bom_csv_by_name(bom_csv_path)
            if name_edges:
                # Detailed debug report
                report_rows: List[Dict[str, str]] = []
                resolved = []
                unknown = ambiguous = 0
                log_lines: List[str] = []

                for p_name, c_name in name_edges:
                    p_candidates = name_to_pn.get(p_name.strip()) or []
                    c_candidates = name_to_pn.get(c_name.strip()) or []
                    status = ""
                    chosen_p = chosen_c = None

                    if not p_candidates or not c_candidates:
                        status = "unknown"
                        unknown += 1
                        log_lines.append(f"unknown parent_name={p_name} child_name={c_name}")
                    elif len(p_candidates) != 1 or len(c_candidates) != 1:
                        status = "ambiguous"
                        ambiguous += 1
                        log_lines.append(f"ambiguous parent_name={p_name} child_name={c_name}")
                    else:
                        status = "resolved"
                        chosen_p = p_candidates[0]
                        chosen_c = c_candidates[0]
                        resolved.append((chosen_p, chosen_c))

                    if debug_names or resolution_report:
                        report_rows.append({
                            "parent_name": p_name,
                            "child_name": c_name,
                            "parent_candidates": ",".join(p_candidates) if p_candidates else "",
                            "child_candidates": ",".join(c_candidates) if c_candidates else "",
                            "chosen_parent": chosen_p or "",
                            "chosen_child": chosen_c or "",
                            "status": status,
                        })

                if resolution_report and report_rows:
                    df = pd.DataFrame(report_rows)
                    df.to_csv(resolution_report, index=False)
                    logging.info("Wrote name resolution report to %s (%d rows)", resolution_report, len(report_rows))

                if debug_names:
                    logging.info("Name resolution: %d resolved, %d unknown, %d ambiguous", len(resolved), unknown, ambiguous)

                if skip_log and log_lines:
                    try:
                        with open(skip_log, "w", encoding="utf-8") as f:
                            for line in log_lines:
                                f.write(line + "\n")
                        logging.info("Wrote skipped entries log to %s (%d entries)", skip_log, len(log_lines))
                    except Exception as e:
                        logging.error("Failed to write skip log %s: %s", skip_log, e)

                if strict_names and (unknown > 0 or ambiguous > 0):
                    raise RuntimeError(f"Name resolution failed: {unknown} unknown, {ambiguous} ambiguous")

                edges.extend(resolved)
        else:
            edges = parser.parse_bom_csv(bom_csv_path)

    logging.info(f"Parsed {len(edges)} BOM relationships")

    # Parse alternate/replacement links
    alternate_links = parser.parse_alternate_links(excel_path)
    logging.info(f"Parsed {len(alternate_links)} alternate/replacement links")

    describe_links = parser.parse_describe_links(excel_path)
    logging.info(f"Parsed {len(describe_links)} describe links")

    # Create constraints and indexes
    client.create_constraints()

    # Import parts
    client.import_parts(parts, batch_size=batch_size)

    # Import BOM relationships
    if edges:
        client.import_bom_relationships(edges, batch_size=batch_size)

    # Import alternate links
    if alternate_links:
        client.import_alternate_links(alternate_links, batch_size=batch_size)

    usage_rows = parse_part_usage(excel_path)
    if usage_rows:
        client.import_part_usage(usage_rows, batch_size=batch_size)

    derived_edges: List[Tuple[str, str]] = []
    if edges:
        derived_edges = edges
    elif usage_rows:
        derived_edges = list({(r['parent'], r['child']) for r in usage_rows if r.get('parent') and r.get('child')})
    if derived_edges:
        client.import_used_in(derived_edges, batch_size=batch_size)
        client.import_part_of_assembly(derived_edges, batch_size=batch_size)

    if describe_links:
        client.import_describe_links(describe_links, batch_size=batch_size)

    # Get statistics
    stats = client.get_statistics()
    logging.info("Import complete: %d parts, %d BOM relationships, %d alternate links",
                 stats['parts'], stats['bom_relationships'], stats['alternate_links'])

    return stats


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Import Excel data into GraphDB or Neo4j")
    parser.add_argument("--excel", default="Snowmobile.xlsx", help="Path to Excel file")
    parser.add_argument("--bom", default=None, help="Optional BOM CSV for relationships")
    parser.add_argument("--url", default="http://127.0.0.1:7200", help="Database URL (GraphDB: http://localhost:7200, Neo4j: bolt://localhost:7687)")
    parser.add_argument("--repo", default="Snowmobile", help="GraphDB repository id or Neo4j database name")
    parser.add_argument("--user", default=None, help="Username for Basic Auth")
    parser.add_argument("--password", default=None, help="Password for Basic Auth")
    parser.add_argument("--batch-size", type=int, default=1000, help="Triples per POST")
    parser.add_argument("--sheets", nargs="*", default=None, help="Specific sheet names to parse")
    parser.add_argument("--dry-run", action="store_true", help="Do not POST, just prepare")
    parser.add_argument("--bom-by-name", action="store_true", help="Parse BOM CSV by part names")
    parser.add_argument("--strict-names", action="store_true", help="Fail on unknown or ambiguous names in name-based BOM")
    parser.add_argument("--generate-bom-by-name", action="store_true", help="Generate name-based BOM CSV from number-based BOM")
    parser.add_argument("--out-bom-name", default="bom_by_name.csv", help="Output path for generated name-based BOM CSV")
    parser.add_argument("--debug-names", action="store_true", help="Enable detailed name resolution diagnostics")
    parser.add_argument("--resolution-report", default=None, help="CSV path to write name resolution report")
    parser.add_argument("--dump-name-index", default=None, help="CSV path to write the number→name index dump")
    parser.add_argument("--emit-bom-name-candidates", default=None, help="CSV path to write BOM name candidates generated from number-based BOM")
    parser.add_argument("--skip-log", default=None, help="Write skipped name-based edges to a log file")
    parser.add_argument("--add-edge-labels", action="store_true", help="Add rdfs:label to predicates for readable relationship labels")
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)")
    parser.add_argument("--quiet-missing-sheets", action="store_true", help="Suppress warnings for sheets missing required columns")
    args = parser.parse_args(argv)

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(message)s")

    try:
        # Always generate intermediate CSVs
        dump_name_index(args.excel, "data/name_index.csv", sheets=args.sheets)
        if args.bom:
            emit_bom_name_candidates(args.excel, args.bom, "data/bom_by_name_candidates.csv", sheets=args.sheets)
            generate_bom_by_name_file(args.excel, args.bom, args.out_bom_name or "data/bom_by_name.csv", sheets=args.sheets)

        if args.dump_name_index:
            dump_name_index(args.excel, args.dump_name_index, sheets=args.sheets)
            return 0

        if args.emit_bom_name_candidates:
            if not args.bom:
                raise RuntimeError("--bom is required to emit BOM name candidates")
            emit_bom_name_candidates(args.excel, args.bom, args.emit_bom_name_candidates, sheets=args.sheets)
            return 0

        if args.generate_bom_by_name:
            if not args.bom:
                raise RuntimeError("--bom is required to generate name-based BOM")
            generate_bom_by_name_file(args.excel, args.bom, args.out_bom_name, sheets=args.sheets)
            return 0

        # Detect database type from URL
        is_neo4j = args.url.startswith('bolt://') or args.url.startswith('neo4j://')

        if is_neo4j:
            # Neo4j import
            if not NEO4J_AVAILABLE:
                raise RuntimeError("Neo4j driver not installed. Install with: pip install neo4j")

            if args.dry_run:
                logging.warning("Dry run not supported for Neo4j imports")

            # Default credentials for Neo4j
            username = args.user or "neo4j"
            password = args.password or "password"

            client = Neo4jClient(
                uri=args.url,
                database=args.repo,
                username=username,
                password=password
            )

            if not client.connect():
                raise RuntimeError("Failed to connect to Neo4j")

            try:
                stats = import_data_neo4j(
                    excel_path=args.excel,
                    bom_csv_path=args.bom,
                    client=client,
                    sheets=args.sheets,
                    batch_size=args.batch_size,
                    bom_by_name=args.bom_by_name,
                    strict_names=args.strict_names,
                    quiet_missing_sheets=args.quiet_missing_sheets,
                    debug_names=True,
                    resolution_report=(args.resolution_report or "data/bom_name_resolution_report.csv"),
                    skip_log=(args.skip_log or "data/skipped_names.log"),
                )
                logging.info("Neo4j import complete: %d parts, %d relationships",
                           stats['parts'], stats['total_relationships'])
            finally:
                client.close()

            return 0
        else:
            # GraphDB import
            client = GraphDBClient(args.url, args.repo, args.user, args.password)
            total, chunks = import_data(
                excel_path=args.excel,
                bom_csv_path=args.bom,
                client=client,
                sheets=args.sheets,
                batch_size=args.batch_size,
                dry_run=args.dry_run,
                bom_by_name=args.bom_by_name,
                strict_names=args.strict_names,
                quiet_missing_sheets=args.quiet_missing_sheets,
                debug_names=True,
                resolution_report=(args.resolution_report or "data/bom_name_resolution_report.csv"),
                skip_log=(args.skip_log or "data/skipped_names.log"),
                add_edge_labels=args.add_edge_labels,
            )
            logging.info("GraphDB import complete: %d triples in %d chunks", total, chunks)
            return 0
    except Exception as e:
        logging.error("Import failed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
