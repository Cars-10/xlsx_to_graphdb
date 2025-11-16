import sys
import os
import argparse
import logging
from typing import Dict, List, Optional, Tuple

import pandas as pd

from src.spreadsheet_loader import SpreadsheetParser, normalize_part_number

class Neo4jClient:
    def __init__(self, uri: str, user: Optional[str] = None, password: Optional[str] = None):
        try:
            from neo4j import GraphDatabase
        except Exception as e:
            raise RuntimeError(f"Neo4j driver not available: {e}")
        auth = (user, password) if user and password else None
        self.driver = GraphDatabase.driver(uri, auth=auth) if auth else GraphDatabase.driver(uri)

    def close(self):
        self.driver.close()

    def load_parts(self, parts: Dict[str, Dict[str, Optional[str]]], batch_size: int = 1000):
        def chunks(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i:i+n]
        items: List[Dict[str, Optional[str]]] = []
        for pn, details in parts.items():
            items.append({
                "number": pn,
                "name": details.get("name") or pn,
                "type": details.get("type"),
                "view": details.get("view"),
                "state": details.get("state"),
                "source": details.get("source"),
                "revision": details.get("revision"),
                "container": details.get("container"),
            })
        cypher = (
            "UNWIND $rows AS row "
            "MERGE (p:Part {number: row.number}) "
            "SET p.name = row.name "
            "FOREACH(ignore IN CASE WHEN row.type IS NULL THEN [] ELSE [1] END | SET p.type = row.type) "
            "FOREACH(ignore IN CASE WHEN row.view IS NULL THEN [] ELSE [1] END | SET p.view = row.view) "
            "FOREACH(ignore IN CASE WHEN row.state IS NULL THEN [] ELSE [1] END | SET p.state = row.state) "
            "FOREACH(ignore IN CASE WHEN row.source IS NULL THEN [] ELSE [1] END | SET p.source = row.source) "
            "FOREACH(ignore IN CASE WHEN row.revision IS NULL THEN [] ELSE [1] END | SET p.revision = row.revision) "
            "FOREACH(ignore IN CASE WHEN row.container IS NULL THEN [] ELSE [1] END | SET p.container = row.container)"
        )
        with self.driver.session() as session:
            for batch in chunks(items, batch_size):
                session.run(cypher, rows=batch)

    def load_relationships(self, edges: List[Tuple[str, str]], pn_to_name: Dict[str, str], batch_size: int = 1000):
        def chunks(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i:i+n]
        rows: List[Dict[str, str]] = []
        for parent, child in edges:
            rows.append({
                "parent": parent,
                "child": child,
                "parentName": pn_to_name.get(parent, parent),
                "childName": pn_to_name.get(child, child),
            })
        cypher = (
            "UNWIND $rows AS row "
            "MERGE (p:Part {number: row.parent}) SET p.name = row.parentName "
            "MERGE (c:Part {number: row.child}) SET c.name = row.childName "
            "MERGE (p)-[:HAS_COMPONENT]->(c)"
        )
        with self.driver.session() as session:
            for batch in chunks(rows, batch_size):
                session.run(cypher, rows=batch)

    def load_describe_links(self, links: List[Tuple[str, str, Optional[str], Optional[str], Optional[str]]], batch_size: int = 1000):
        def chunks(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i:i+n]
        rows: List[Dict[str, Optional[str]]] = []
        for d, p, dred, dorg, dcont in links:
            rows.append({
                "doc": d,
                "part": p,
                "revision": dred,
                "org": dorg,
                "container": dcont,
            })
        cypher = (
            "UNWIND $rows AS row "
            "MERGE (d:Document {number: row.doc}) "
            "FOREACH(ignore IN CASE WHEN row.revision IS NULL THEN [] ELSE [1] END | SET d.revision = row.revision) "
            "FOREACH(ignore IN CASE WHEN row.org IS NULL THEN [] ELSE [1] END | SET d.organization = row.org) "
            "FOREACH(ignore IN CASE WHEN row.container IS NULL THEN [] ELSE [1] END | SET d.container = row.container) "
            "MERGE (p:Part {number: row.part}) "
            "MERGE (d)-[:DESCRIBES]->(p)"
        )
        with self.driver.session() as session:
            for batch in chunks(rows, batch_size):
                session.run(cypher, rows=batch)


def import_to_neo4j(
    excel_path: str,
    bom_csv_path: Optional[str],
    uri: str,
    user: Optional[str] = None,
    password: Optional[str] = None,
    sheets: Optional[List[str]] = None,
    batch_size: int = 1000,
):
    parser = SpreadsheetParser(excel_path)
    parts = parser.parse_parts(sheets)
    if not parts:
        raise RuntimeError("No parts parsed from Excel")
    pn_to_name, _ = parser.build_cross_index(sheets)
    def extract_edges_from_excel() -> List[Tuple[str, str]]:
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
        edges_local: List[Tuple[str, str]] = []
        try:
            sheet_names = parser.get_sheet_names()
        except Exception:
            sheet_names = []
        for sheet_name in sheet_names:
            df = read_sheet(sheet_name)
            if df is None or df.empty:
                continue
            cols = {str(c).strip().lower(): c for c in df.columns}
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
                        edges_local.append((parent, part_num))
                    for l in list(level_stack.keys()):
                        if l > level:
                            del level_stack[l]
                continue
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
                    parent = row.get(parent_col)
                    child = row.get(child_col)
                    p = str(parent).strip() if pd.notna(parent) else ''
                    c = str(child).strip() if pd.notna(child) else ''
                    if not p or not c:
                        continue
                    edges_local.append((p, c))
                continue
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
                        edges_local.append((parent_pn, child_pn))
        # Deduplicate
        if edges_local:
            edges_local = list(dict.fromkeys(edges_local))
        return edges_local
    edges: List[Tuple[str, str]] = []
    if bom_csv_path:
        try:
            df = pd.read_csv(bom_csv_path)
            cols = {str(c).strip().lower() for c in df.columns}
        except Exception:
            cols = set()
        if {"parent name", "child name"}.issubset(cols):
            name_edges = parser.parse_bom_csv_by_name(bom_csv_path)
            name_to_pn: Dict[str, List[str]] = {}
            for pn, nm in pn_to_name.items():
                lst = name_to_pn.get(nm)
                if lst is None:
                    name_to_pn[nm] = [pn]
                else:
                    lst.append(pn)
            from src.spreadsheet_loader import resolve_edges_by_name
            edges = resolve_edges_by_name(name_edges, name_to_pn, strict=False, parts=parts)
            logging.info("Resolved %d name-based BOM relationships from %s", len(edges), bom_csv_path)
        else:
            edges = parser.parse_bom_csv(bom_csv_path)
            logging.info("Parsed %d number-based BOM relationships from %s", len(edges), bom_csv_path)
    if not edges:
        logging.warning("No BOM relationships parsed from CSV; falling back to extracting from Excel")
        edges = extract_edges_from_excel()
        logging.info("Extracted %d relationships from Excel", len(edges))
    client = Neo4jClient(uri, user, password)
    try:
        logging.info("Loading %d parts into Neo4j", len(parts))
        client.load_parts(parts, batch_size=batch_size)
        logging.info("Loading %d relationships into Neo4j", len(edges))
        client.load_relationships(edges, pn_to_name, batch_size=batch_size)
        describe_links = parser.parse_describe_links(excel_path)
        logging.info("Loading %d describe links into Neo4j", len(describe_links))
        if describe_links:
            client.load_describe_links(describe_links, batch_size=batch_size)
    finally:
        client.close()


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Import Snowmobile Excel data into Neo4j")
    parser.add_argument("--excel", default="Snowmobile.xlsx", help="Path to Excel file")
    parser.add_argument("--bom", default="bom.csv", help="BOM CSV with Number/Component Id")
    parser.add_argument("--uri", default="bolt://localhost:7687", help="Neo4j Bolt URI")
    parser.add_argument("--user", default=None, help="Neo4j username")
    parser.add_argument("--password", default=None, help="Neo4j password (or set NEO4J_PASSWORD env)")
    parser.add_argument("--batch-size", type=int, default=1000, help="Rows per Cypher UNWIND")
    parser.add_argument("--sheets", nargs="*", default=None, help="Specific sheet names to parse")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    args = parser.parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
    try:
        # Prefer environment variable when password not provided
        password = args.password or os.environ.get("NEO4J_PASSWORD")
        import_to_neo4j(
            excel_path=args.excel,
            bom_csv_path=args.bom,
            uri=args.uri,
            user=args.user,
            password=password,
            sheets=args.sheets,
            batch_size=args.batch_size,
        )
        logging.info("Neo4j import complete")
        return 0
    except Exception as e:
        logging.error("Neo4j import failed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())