import sys
import os
import argparse
import logging
import json
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin
from urllib.request import Request, urlopen

import pandas as pd

from src.utils.spreadsheet_loader import SpreadsheetParser, normalize_part_number

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
            "MERGE (p:WTPart:Part {number: row.number}) "
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
            "MERGE (p:WTPart:Part {number: row.parent}) SET p.name = row.parentName "
            "MERGE (c:WTPart:Part {number: row.child}) SET c.name = row.childName "
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
            "MERGE (p:WTPart:Part {number: row.part}) "
            "MERGE (d)-[:DESCRIBES]->(p)"
        )
        with self.driver.session() as session:
            for batch in chunks(rows, batch_size):
                session.run(cypher, rows=batch)

    def load_changes(self, changes: List[Dict[str, str]], batch_size: int = 1000):
        def chunks(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i:i+n]
        cypher = (
            "UNWIND $rows AS row "
            "MERGE (c:Change {number: row.number}) "
            "SET c.type = row.type "
            "SET c.state = row.state "
            "FOREACH(ignore IN CASE WHEN row.name IS NULL THEN [] ELSE [1] END | SET c.name = row.name) "
            "FOREACH(ignore IN CASE WHEN row.source IS NULL THEN [] ELSE [1] END | SET c.source = row.source) "
            "FOREACH(ignore IN CASE WHEN row.container IS NULL THEN [] ELSE [1] END | SET c.container = row.container) "
            "FOREACH(ignore IN CASE WHEN row.type IN ['ChangeRequest','CR','ECR'] THEN [1] ELSE [] END | SET c:ChangeRequest SET c.color = '#FFEB3B') "
            "FOREACH(ignore IN CASE WHEN row.type IN ['ChangeNotice','CN','ECN','ChangeOrder'] THEN [1] ELSE [] END | SET c:ChangeNotice SET c.color = '#FFC107') "
            "FOREACH(ignore IN CASE WHEN row.type IN ['ProblemReport','PR'] THEN [1] ELSE [] END | SET c:ProblemReport SET c.color = '#FFF176') "
            "FOREACH(ignore IN CASE WHEN row.type IN ['ChangeActivity','CA'] THEN [1] ELSE [] END | SET c:ChangeActivity SET c.color = '#FFD54F') "
            "FOREACH(ignore IN CASE WHEN NOT row.type IN ['ChangeRequest','CR','ECR','ChangeNotice','CN','ECN','ChangeOrder','ProblemReport','PR','ChangeActivity','CA'] THEN [1] ELSE [] END | SET c.color = '#FFF59D') "
            "MERGE (p:WTPart:Part {number: row.part}) "
            "MERGE (c)-[:AFFECTS_PART]->(p)"
        )
        with self.driver.session() as session:
            for batch in chunks(changes, batch_size):
                session.run(cypher, rows=batch)

    def ensure_indexes(self):
        stmts = [
            "CREATE INDEX part_number IF NOT EXISTS FOR (p:WTPart) ON (p.number)",
            "CREATE INDEX doc_number IF NOT EXISTS FOR (d:Document) ON (d.number)",
            "CREATE INDEX change_number IF NOT EXISTS FOR (c:Change) ON (c.number)",
            "CREATE CONSTRAINT part_number_unique IF NOT EXISTS FOR (p:WTPart) REQUIRE p.number IS UNIQUE",
            "CREATE CONSTRAINT doc_number_unique IF NOT EXISTS FOR (d:Document) REQUIRE d.number IS UNIQUE",
            "CREATE CONSTRAINT change_number_unique IF NOT EXISTS FOR (c:Change) REQUIRE c.number IS UNIQUE",
        ]
        with self.driver.session() as session:
            for s in stmts:
                try:
                    session.run(s)
                except Exception:
                    pass


def import_to_neo4j(
    excel_path: str,
    bom_csv_path: Optional[str],
    uri: str,
    user: Optional[str] = None,
    password: Optional[str] = None,
    sheets: Optional[List[str]] = None,
    batch_size: int = 1000,
    container: Optional[str] = None,
    with_changes: bool = False,
    mcp_url: Optional[str] = None,
):
    parser = SpreadsheetParser(excel_path)
    parts = parser.parse_parts(sheets)
    if not parts:
        raise RuntimeError("No parts parsed from Excel")
    # Normalize container if provided
    if container:
        for pn, details in parts.items():
            if not details.get("container"):
                details["container"] = container
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
            from src.utils.spreadsheet_loader import resolve_edges_by_name
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
        # Ensure indexes for performance
        client.ensure_indexes()
        logging.info("Loading %d parts into Neo4j", len(parts))
        client.load_parts(parts, batch_size=batch_size)
        logging.info("Loading %d relationships into Neo4j", len(edges))
        client.load_relationships(edges, pn_to_name, batch_size=batch_size)
        describe_links = parser.parse_describe_links(excel_path)
        logging.info("Loading %d describe links into Neo4j", len(describe_links))
        if describe_links:
            client.load_describe_links(describe_links, batch_size=batch_size)
        if with_changes:
            change_rows: List[Dict[str, str]] = []
            if mcp_url:
                try:
                    headers = {"Content-Type": "application/json", "Accept": "application/json"}
                    for pn in parts.keys():
                        payload = {
                            "jsonrpc": "2.0",
                            "method": "tools/call",
                            "params": {"name": "changemgmt_search_change_objects", "arguments": {"query": pn, "limit": 50}},
                            "id": 1,
                        }
                        req = Request(urljoin(mcp_url, "/message"), data=json.dumps(payload).encode("utf-8"), headers=headers, method="POST")
                        try:
                            resp = urlopen(req, timeout=5)
                            data = json.loads(resp.read().decode("utf-8"))
                            result = data.get("result") or data
                            items = result.get("results") if isinstance(result, dict) else []
                        except Exception:
                            items = []
                        if not items:
                            payload2 = {"tool": "changemgmt_search_change_objects", "arguments": {"query": pn, "limit": 50}}
                            req2 = Request(urljoin(mcp_url, "/tools/call"), data=json.dumps(payload2).encode("utf-8"), headers=headers, method="POST")
                            try:
                                resp2 = urlopen(req2, timeout=5)
                                data2 = json.loads(resp2.read().decode("utf-8"))
                                items = data2.get("results") or []
                            except Exception:
                                items = []
                        if not items:
                            req3 = Request(urljoin(mcp_url, "/tools/change_search"), data=json.dumps({"query": pn, "limit": 50}).encode("utf-8"), headers=headers, method="POST")
                            try:
                                resp3 = urlopen(req3, timeout=5)
                                data3 = json.loads(resp3.read().decode("utf-8"))
                                items = data3.get("results") or data3.get("data") or []
                            except Exception:
                                items = []
                        for ch in items or []:
                            oid = ch.get("oid") or ch.get("id")
                            if not oid:
                                continue
                            payload_d = {
                                "jsonrpc": "2.0",
                                "method": "tools/call",
                                "params": {"name": "changemgmt_get_change_object", "arguments": {"changeId": oid, "expand": "Number,Name,State,AffectedObjects"}},
                                "id": 1,
                            }
                            req_d = Request(urljoin(mcp_url, "/message"), data=json.dumps(payload_d).encode("utf-8"), headers=headers, method="POST")
                            try:
                                resp_d = urlopen(req_d, timeout=5)
                                data_d = json.loads(resp_d.read().decode("utf-8"))
                                result_d = data_d.get("result") or data_d
                            except Exception:
                                result_d = {}
                            if not result_d:
                                payload_d2 = {"tool": "changemgmt_get_change_object", "arguments": {"changeId": oid, "expand": "Number,Name,State,AffectedObjects"}}
                                req_d2 = Request(urljoin(mcp_url, "/tools/call"), data=json.dumps(payload_d2).encode("utf-8"), headers=headers, method="POST")
                                try:
                                    resp_d2 = urlopen(req_d2, timeout=5)
                                    result_d = json.loads(resp_d2.read().decode("utf-8"))
                                except Exception:
                                    result_d = {}
                            if isinstance(result_d, dict) and not result_d.get("number") and not result_d.get("Number"):
                                req_d3 = Request(urljoin(mcp_url, "/tools/change_get"), data=json.dumps({"changeId": oid}).encode("utf-8"), headers=headers, method="POST")
                                try:
                                    resp_d3 = urlopen(req_d3, timeout=5)
                                    result_d = json.loads(resp_d3.read().decode("utf-8"))
                                except Exception:
                                    pass
                            ch_num = result_d.get("number") or result_d.get("Number") or ch.get("number") or ch.get("Number")
                            ch_type = result_d.get("type") or result_d.get("Type") or ch.get("type") or ch.get("Type") or "ChangeNotice"
                            ch_state = result_d.get("state") or result_d.get("State") or ch.get("state") or ch.get("State") or "INWORK"
                            ch_name = result_d.get("name") or result_d.get("Name") or ch.get("name") or ch.get("Name")
                            affected = result_d.get("AffectedObjects") or []
                            if not affected:
                                change_rows.append({
                                    "number": str(ch_num or f"ECN-{pn}"),
                                    "type": str(ch_type),
                                    "state": str(ch_state),
                                    "name": ch_name,
                                    "source": "mcp",
                                    "container": parts.get(pn, {}).get("container") or container,
                                    "part": pn,
                                })
                            else:
                                for ao in affected:
                                    apn = ao.get("number") or ao.get("Number") or pn
                                    change_rows.append({
                                        "number": str(ch_num or f"ECN-{pn}"),
                                        "type": str(ch_type),
                                        "state": str(ch_state),
                                        "name": ch_name,
                                        "source": "mcp",
                                        "container": parts.get(str(apn), {}).get("container") or container,
                                        "part": str(apn),
                                    })
                except Exception:
                    pass
            if not change_rows:
                for pn in parts.keys():
                    change_rows.append({
                        "number": f"ECN-{pn}",
                        "type": "ChangeNotice",
                        "state": "INWORK",
                        "name": None,
                        "source": "synthetic",
                        "container": parts.get(pn, {}).get("container") or container,
                        "part": pn,
                    })
            logging.info("Loading %d change records into Neo4j", len(change_rows))
            client.load_changes(change_rows, batch_size=batch_size)
    finally:
        client.close()


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Import Excel data into Neo4j")
    parser.add_argument("--excel", default="Snowmobile.xlsx", help="Path to Excel file")
    parser.add_argument("--bom", default="bom.csv", help="BOM CSV with Number/Component Id")
    parser.add_argument("--uri", default="bolt://localhost:7687", help="Neo4j Bolt URI")
    parser.add_argument("--user", default=None, help="Neo4j username")
    parser.add_argument("--password", default=None, help="Neo4j password (or set NEO4J_PASSWORD env)")
    parser.add_argument("--batch-size", type=int, default=1000, help="Rows per Cypher UNWIND")
    parser.add_argument("--sheets", nargs="*", default=None, help="Specific sheet names to parse")
    parser.add_argument("--container", default=None, help="Container name to set on parts")
    parser.add_argument("--with-changes", action="store_true", help="Create synthetic change records affecting parts")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    parser.add_argument("--mcp-url", default=None, help="Windchill MCP server base URL")
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
            container=args.container,
            with_changes=args.with_changes,
            mcp_url=args.mcp_url,
        )
        logging.info("Neo4j import complete")
        return 0
    except Exception as e:
        logging.error("Neo4j import failed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())