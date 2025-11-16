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

warnings.filterwarnings(
    "ignore",
    r"Workbook contains no default style.*",
    UserWarning,
    r"openpyxl\.styles\.stylesheet",
)


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
        if not bom_csv_path:
            return []
        try:
            df = pd.read_csv(bom_csv_path)
        except Exception as e:
            logging.error("Error reading BOM CSV %s: %s", bom_csv_path, e)
            return []
        cols = set(df.columns)
        candidates = [
            ("Number", "Component Id"),
            ("Parent Number", "Child Number"),
        ]
        parent_col = child_col = None
        for p, c in candidates:
            if p in cols and c in cols:
                parent_col, child_col = p, c
                break
        if not parent_col:
            logging.warning("BOM CSV missing expected columns; found: %s", list(cols))
            return []
        edges: List[Tuple[str, str]] = []
        for _, row in df.iterrows():
            parent = normalize_part_number(row.get(parent_col))
            child = normalize_part_number(row.get(child_col))
            if parent and child:
                edges.append((parent, child))
        return edges

    def parse_bom_csv_by_name(self, bom_csv_path: Optional[str]) -> List[Tuple[str, str]]:
        if not bom_csv_path:
            return []
        try:
            df = pd.read_csv(bom_csv_path)
        except Exception as e:
            logging.error("Error reading BOM CSV %s: %s", bom_csv_path, e)
            return []
        col_map = {str(c).strip(): c for c in df.columns}
        lower = {k.lower() for k in col_map.keys()}
        parent_col = child_col = None
        if {"parent name", "child name"}.issubset(lower):
            parent_col = next((col_map[k] for k in col_map if k.lower() == "parent name"), None)
            child_col = next((col_map[k] for k in col_map if k.lower() == "child name"), None)
        elif {"name", "component name"}.issubset(lower):
            parent_col = next((col_map[k] for k in col_map if k.lower() == "name"), None)
            child_col = next((col_map[k] for k in col_map if k.lower() == "component name"), None)
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
    edges = parser.parse_bom_csv(bom_csv_path)
    rows: List[Tuple[str, str]] = []
    unmapped = 0
    for parent_pn, child_pn in edges:
        p_name = pn_to_name.get(parent_pn, parent_pn)
        c_name = pn_to_name.get(child_pn, child_pn)
        if p_name == parent_pn or c_name == child_pn:
            unmapped += 1
        rows.append((p_name, c_name))
    df = pd.DataFrame(rows, columns=["Parent Name", "Child Name"])
    df.to_csv(out_path, index=False)
    logging.info("Generated name-based BOM %s with %d rows; fallbacks %d", out_path, len(rows), unmapped)
    return len(rows)


def dump_name_index(excel_path: str, out_path: str, sheets: Optional[List[str]] = None) -> int:
    parser = SpreadsheetParser(excel_path)
    pn_to_name, _ = parser.build_cross_index(sheets)
    rows = [(pn, nm) for pn, nm in pn_to_name.items()]
    df = pd.DataFrame(rows, columns=["Part Number", "Name"])
    df.to_csv(out_path, index=False)
    logging.info("Dumped name index to %s (%d entries)", out_path, len(rows))
    return len(rows)


def emit_bom_name_candidates(excel_path: str, bom_csv_path: str, out_path: str, sheets: Optional[List[str]] = None) -> int:
    parser = SpreadsheetParser(excel_path)
    pn_to_name, _ = parser.build_cross_index(sheets)
    edges = parser.parse_bom_csv(bom_csv_path)
    rows: List[Tuple[str, str, str, str]] = []
    for parent_pn, child_pn in edges:
        p_name = pn_to_name.get(parent_pn, parent_pn)
        c_name = pn_to_name.get(child_pn, child_pn)
        rows.append((parent_pn, p_name, child_pn, c_name))
    df = pd.DataFrame(rows, columns=["Parent Number", "Parent Name", "Child Number", "Child Name"])
    df.to_csv(out_path, index=False)
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

    total_triples = (len(part_triples) + len(bom_triples) + len(used_in_triples) +
                     len(part_of_assembly_triples) + len(alternate_triples))
    logging.info("Total triples prepared: %d", total_triples)
    logging.info("  Part metadata: %d", len(part_triples))
    logging.info("  hasComponent: %d", len(bom_triples))
    logging.info("  usedIn: %d", len(used_in_triples))
    logging.info("  partOfAssembly: %d", len(part_of_assembly_triples))
    logging.info("  hasAlternate: %d", len(alternate_triples))

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
                   part_of_assembly_triples + alternate_triples)
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


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Import Snowmobile Excel data into GraphDB")
    parser.add_argument("--excel", default="Snowmobile.xlsx", help="Path to Excel file")
    parser.add_argument("--bom", default=None, help="Optional BOM CSV for relationships")
    parser.add_argument("--url", default="http://127.0.0.1:7200", help="GraphDB base URL")
    parser.add_argument("--repo", default="Snowmobile", help="GraphDB repository id")
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
            debug_names=args.debug_names,
            resolution_report=args.resolution_report,
            skip_log=args.skip_log,
            add_edge_labels=args.add_edge_labels,
        )
        logging.info("Import complete: %d triples in %d chunks", total, chunks)
        return 0
    except Exception as e:
        logging.error("Import failed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
