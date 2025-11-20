#!/usr/bin/env python3
import os
import json
import logging
from typing import Dict, Any, List, Optional, Set, Tuple
from pathlib import Path

import importlib.util
_client_path = Path(__file__).parent / "enhanced_windchill_mcp_client.py"
_spec = importlib.util.spec_from_file_location("enhanced_windchill_mcp_client", str(_client_path))
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
EnhancedWindchillMCPClient = _mod.EnhancedWindchillMCPClient
from src.core.logging_config import setup_logging, get_logger
from src.importers.neo4j_importer import Neo4jClient

setup_logging(level="INFO", structured=False, include_console=True)
logger = get_logger(__name__)

class Transporter:
    def __init__(self, mcp_url: str = "http://localhost:3000", out_dir: str = "data/transporter", neo4j_uri: str = "bolt://localhost:7687", neo4j_user: Optional[str] = "neo4j", neo4j_password: Optional[str] = None):
        self.client = EnhancedWindchillMCPClient(base_url=mcp_url)
        self.out_dir = Path(out_dir)
        self.raw_parts_dir = self.out_dir / "raw" / "parts"
        self.raw_boms_dir = self.out_dir / "raw" / "boms"
        self.raw_parts_id_dir = self.out_dir / "raw" / "parts_by_id"
        self.raw_boms_id_dir = self.out_dir / "raw" / "boms_by_id"
        self.raw_docs_by_part_dir = self.out_dir / "raw" / "docs_by_part"
        self.raw_docs_by_id_dir = self.out_dir / "raw" / "docs_by_id"
        self.processed_dir = self.out_dir / "processed"
        self.neo4j_uri = neo4j_uri
        self.neo4j_user = neo4j_user
        self.neo4j_password = neo4j_password
        self.offline = False
        self._stats = {"cache_hits": 0, "cache_misses": 0, "server_calls": 0}
        self.details_strategy = "full"
        self._ensure_dirs()

    def _ensure_dirs(self):
        for d in [self.raw_parts_dir, self.raw_boms_dir, self.raw_parts_id_dir, self.raw_boms_id_dir, self.raw_docs_by_part_dir, self.raw_docs_by_id_dir, self.processed_dir]:
            d.mkdir(parents=True, exist_ok=True)

    def _save_json(self, path: Path, data: Any):
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f, indent=2)

    def _load_json(self, path: Path) -> Optional[Any]:
        try:
            if path.exists():
                with open(path, "r") as f:
                    return json.load(f)
            return None
        except Exception:
            return None

    def _extract_edges_generic(self, structure: Any, current_parent: Optional[str]) -> List[Tuple[str, str]]:
        edges: List[Tuple[str, str]] = []
        def walk(obj: Any, parent: Optional[str]):
            if isinstance(obj, dict):
                num = obj.get("Number") or obj.get("number") or obj.get("PartNumber")
                local_parent = parent
                if parent and num and num != parent:
                    edges.append((parent, str(num)))
                    local_parent = str(num)
                child_candidates: List[Any] = []
                for key in ["children", "Children", "components", "Components", "items", "Items", "value", "Value", "nodes", "relations", "edges", "downstream", "structure", "Parts"]:
                    v = obj.get(key)
                    if isinstance(v, list) and v:
                        child_candidates.append(v)
                for arr in child_candidates:
                    for ch in arr:
                        walk(ch, local_parent)
                for k, v in obj.items():
                    if isinstance(v, (dict, list)):
                        walk(v, local_parent)
            elif isinstance(obj, list):
                for it in obj:
                    walk(it, parent)
        walk(structure, current_parent)
        if edges:
            edges = list(dict.fromkeys(edges))
        return edges

    def _has_nested_components(self, structure: Any) -> bool:
        def check(obj: Any) -> bool:
            if isinstance(obj, dict):
                comps = obj.get("Components")
                if isinstance(comps, list):
                    for it in comps:
                        if isinstance(it, dict) and isinstance(it.get("Components"), list) and it.get("Components"):
                            return True
                    for it in comps:
                        if check(it):
                            return True
                for v in obj.values():
                    if isinstance(v, (dict, list)) and check(v):
                        return True
            elif isinstance(obj, list):
                for it in obj:
                    if check(it):
                        return True
            return False
        return check(structure)

    def _get_part_details_cached(self, part_number: str) -> Optional[Dict[str, Any]]:
        p = self.raw_parts_dir / f"{part_number}.json"
        data = self._load_json(p)
        if data:
            self._stats["cache_hits"] += 1
            return data
        if self.offline:
            return None
        details = self.client.get_part_details(part_number)
        if details:
            self._stats["server_calls"] += 1
            self._save_json(p, details)
            return details
        return None

    def _get_bom_cached(self, part_number: str, levels: int = 3) -> Optional[Dict[str, Any]]:
        p = self.raw_boms_dir / f"{part_number}.json"
        data = self._load_json(p)
        if data:
            self._stats["cache_hits"] += 1
            return data
        if self.offline:
            return None
        bom = self.client.get_bom_structure(part_number, depth=levels)
        if bom:
            self._stats["server_calls"] += 1
            self._save_json(p, bom)
            return bom
        return None

    def _get_details_by_id_cached(self, oid: str) -> Optional[Dict[str, Any]]:
        p = self.raw_parts_id_dir / f"{oid.replace(':','_')}.json"
        data = self._load_json(p)
        if data:
            self._stats["cache_hits"] += 1
            return data
        if self.offline:
            return None
        res = self.client.call_tool("part_get", {"id": oid}) or self.client.call_tool("partmgmt_get_part", {"id": oid})
        if res:
            self._stats["server_calls"] += 1
            self._save_json(p, res)
            return res
        return None

    def _get_bom_by_id_cached(self, oid: str, depth: int = 3) -> Optional[Dict[str, Any]]:
        p = self.raw_boms_id_dir / f"{oid.replace(':','_')}_d{depth}.json"
        data = self._load_json(p)
        if data:
            self._stats["cache_hits"] += 1
            return data
        if self.offline:
            return None
        res = self.client.get_bom_structure_by_id(oid, depth=depth) or self.client.call_tool("part_get_structure", {"id": oid, "depth": depth}) or self.client.call_tool("partmgmt_get_part_structure", {"id": oid, "depth": depth})
        if res:
            self._stats["server_calls"] += 1
            self._save_json(p, res)
            return res
        return None

    def _normalize_part_details(self, details: Dict[str, Any]) -> Dict[str, Optional[str]]:
        def scalar(v: Any) -> Optional[str]:
            if v is None:
                return None
            if isinstance(v, dict):
                if 'Value' in v:
                    return str(v.get('Value'))
                if 'value' in v:
                    return str(v.get('value'))
                return None
            return str(v)
        out: Dict[str, Optional[str]] = {}
        out["name"] = scalar(details.get("Name") or details.get("name"))
        out["type"] = scalar(details.get("Type") or details.get("type") or details.get("@odata.type"))
        out["view"] = scalar(details.get("View") or details.get("view"))
        out["state"] = scalar(details.get("State") or details.get("state"))
        out["source"] = scalar(details.get("Source") or details.get("source"))
        out["revision"] = scalar(details.get("Revision") or details.get("revision"))
        out["container"] = scalar(details.get("Container") or details.get("container") or details.get("OrganizationName"))
        return out

    def _merge_part_details_from_struct(self, struct: Dict[str, Any], parts: Dict[str, Dict[str, Optional[str]]]) -> None:
        def get_num(obj: Dict[str, Any]) -> Optional[str]:
            return obj.get("PartNumber") or obj.get("Number") or obj.get("number")
        def details_from_obj(obj: Dict[str, Any]) -> Dict[str, Optional[str]]:
            d: Dict[str, Any] = {}
            identity = obj.get("Identity") or {}
            d["Name"] = obj.get("Name") or identity.get("Name")
            d["Type"] = obj.get("Type") or obj.get("@odata.type")
            d["View"] = obj.get("View")
            d["State"] = obj.get("State")
            d["Source"] = obj.get("Source")
            d["Revision"] = obj.get("Revision")
            d["Container"] = obj.get("Container") or obj.get("OrganizationName")
            return d
        parent_details = details_from_obj(struct)
        parent_number = get_num(struct)
        if parent_number:
            existing = parts.get(parent_number) or {}
            merged = self._normalize_part_details(parent_details)
            for k, v in merged.items():
                if v is not None and not existing.get(k):
                    existing[k] = v
            parts[parent_number] = existing
        comps = struct.get("Components") or []
        for c in comps:
            child_number = get_num(c)
            if not child_number:
                continue
            existing = parts.get(child_number) or {}
            merged = self._normalize_part_details(details_from_obj(c))
            for k, v in merged.items():
                if v is not None and not existing.get(k):
                    existing[k] = v
            parts[child_number] = existing

    def collect(self, top_part_number: str, max_depth: int = 4) -> Tuple[Dict[str, Dict[str, Optional[str]]], List[Tuple[str, str]]]:
        if not self.client.test_connection():
            raise RuntimeError("Cannot connect to MCP server")
        try:
            tools = self.client.session.get(self.client.base_url + "/tools")
            if tools.status_code == 200:
                logger.info(f"Tools: {tools.json()}")
        except Exception:
            pass
        parts: Dict[str, Dict[str, Optional[str]]] = {}
        edges: List[Tuple[str, str]] = []
        # Initial resolve top OID via single search
        id_index_path = self.processed_dir / "id_index.json"
        id_index = self._load_json(id_index_path) or {}
        pn_to_id: Dict[str, str] = id_index.get("pn_to_id", {})
        top_id = pn_to_id.get(top_part_number)
        if not top_id and not self.offline:
            # exact-number search yields OData value list
            res = self.client.call_tool("part_search", {"number": top_part_number, "limit": 10}) or self.client.call_tool("partmgmt_search_parts", {"query": top_part_number, "limit": 10})
            arr = []
            if isinstance(res, dict):
                arr = res.get("value") or res.get("results") or []
            if arr:
                top_id = arr[0].get("ID") or arr[0].get("id")
                if top_id:
                    pn_to_id[top_part_number] = top_id
        if top_id:
            id_index["pn_to_id"] = pn_to_id
            self._save_json(id_index_path, id_index)
        visited_ids: Set[str] = set()
        queue_ids: List[Tuple[str, int]] = []
        if top_id:
            queue_ids.append((top_id, 0))
        else:
            # fallback to number-based traversal
            queue_ids.append((top_part_number, 0))
        while queue_ids:
            oid_or_pn, depth = queue_ids.pop(0)
            # Determine mode: if looks like OID (has ':'), use id path
            is_oid = isinstance(oid_or_pn, str) and (":" in oid_or_pn)
            if is_oid:
                if oid_or_pn in visited_ids:
                    continue
                visited_ids.add(oid_or_pn)
                details = {}
                if self.details_strategy == "full" or (self.details_strategy == "top" and depth == 0):
                    details = self._get_details_by_id_cached(oid_or_pn) or {}
                # derive part number
                part_number = details.get("Number") or details.get("Identity", {}).get("Number")
                if not part_number:
                    # If no details returned, attempt structure to read PartNumber
                    struct = self._get_bom_by_id_cached(oid_or_pn, depth=3) or {}
                    part_number = struct.get("PartNumber")
                if part_number:
                    pn_to_id[part_number] = oid_or_pn
                    parts.setdefault(part_number, self._normalize_part_details(details if isinstance(details, dict) else {}))
                req_depth = max(1, min(max_depth - depth, 9))
                struct = self._get_bom_by_id_cached(oid_or_pn, depth=req_depth)
                if struct:
                    parent_pn = (struct.get("PartNumber") if isinstance(struct, dict) else None) or part_number or top_part_number
                    use_struct = struct
                    if req_depth > 1 and not self._has_nested_components(use_struct):
                        nb = self._get_bom_cached(parent_pn, levels=req_depth)
                        if nb:
                            use_struct = nb
                    extracted = self._extract_edges_generic(use_struct, parent_pn)
                    for e in extracted:
                        if e not in edges:
                            edges.append(e)
                    self._merge_part_details_from_struct(use_struct, parts)
                    for _, ch in extracted:
                        if ch not in parts:
                            parts.setdefault(ch, {})
                    if depth < max_depth:
                        comps = struct.get("Components") or []
                        for comp in comps:
                            child_id = comp.get("PartId")
                            child_pn = comp.get("PartNumber")
                            if child_pn and child_pn not in parts:
                                parts.setdefault(child_pn, {})
                            if child_id and child_pn:
                                pn_to_id[str(child_pn)] = str(child_id)
                            if child_id:
                                queue_ids.append((str(child_id), depth + 1))
            else:
                # fallback number-based path
                pn = oid_or_pn
                if pn in parts:
                    continue
                details = {}
                if self.details_strategy in ("full", "top"):
                    details = self._get_part_details_cached(pn) or {}
                parts[pn] = self._normalize_part_details(details if isinstance(details, dict) else {})
                req_levels = max(1, min(max_depth - depth, 9))
                bom = self._get_bom_cached(pn, levels=req_levels)
                if not bom:
                    continue
                bom_edges = self._extract_edges_generic(bom, pn)
                for e in bom_edges:
                    if e not in edges:
                        edges.append(e)
                if isinstance(bom, dict):
                    self._merge_part_details_from_struct(bom, parts)
                if req_levels <= 1 and depth < max_depth:
                    for _, ch in bom_edges:
                        if ch not in parts:
                            parts.setdefault(ch, {})
                            queue_ids.append((ch, depth + 1))
        self._save_json(self.processed_dir / "parts.json", parts)
        self._save_json(self.processed_dir / "edges.json", edges)
        index = {
            "top": top_part_number,
            "pn_to_id": pn_to_id,
            "stats": self._stats
        }
        # Heuristic document search by part number (cache-aware)
        doc_links: List[List[Optional[str]]] = []
        for pn in parts.keys():
            pth = self.raw_docs_by_part_dir / f"{pn}.json"
            docs = self._load_json(pth)
            if docs is None and not self.offline:
                docs = self.client.search_documents(number=None, name=pn, limit=50)
                if docs is not None:
                    self._save_json(pth, docs)
            for d in docs or []:
                dnum = d.get("Number") or d.get("number")
                dred = d.get("Revision") or d.get("revision")
                dorg = d.get("OrganizationName") or d.get("organization")
                dcont = d.get("Container") or d.get("container")
                if dnum:
                    doc_links.append([str(dnum), str(pn), dred, dorg, dcont])
        if doc_links:
            self._save_json(self.processed_dir / "doc_links.json", doc_links)
        self._save_json(self.out_dir / "index.json", index)
        return parts, edges

    def load_into_neo4j(self, parts: Dict[str, Dict[str, Optional[str]]], edges: List[Tuple[str, str]], batch_size: int = 1000) -> None:
        client = Neo4jClient(self.neo4j_uri, self.neo4j_user, self.neo4j_password)
        try:
            client.ensure_indexes()
            client.load_parts(parts, batch_size=batch_size)
            pn_to_name = {}
            for pn, det in parts.items():
                nm = det.get("name") or pn
                pn_to_name[pn] = nm
            client.load_relationships(edges, pn_to_name, batch_size=batch_size)
            # Optional document describe links
            doc_links_path = self.processed_dir / "doc_links.json"
            doc_links = self._load_json(doc_links_path) or []
            if doc_links:
                client.load_describe_links([(dl[0], dl[1], dl[2], dl[3], dl[4]) for dl in doc_links], batch_size=batch_size)
        finally:
            client.close()

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Transporter: Windchill → cache → Neo4j")
    parser.add_argument("--part-number", default="101 HELI")
    parser.add_argument("--mcp-url", default="http://localhost:3000")
    parser.add_argument("--out-dir", default="data/transporter/101_HELI")
    parser.add_argument("--neo4j-uri", default="bolt://localhost:7687")
    parser.add_argument("--neo4j-user", default="neo4j")
    parser.add_argument("--neo4j-password", default=os.environ.get("NEO4J_PASSWORD") or "tstpwdpwd")
    parser.add_argument("--max-depth", type=int, default=4)
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--details-strategy", choices=["full","top","none"], default="full")
    parser.add_argument("--hydrate-details", action="store_true")
    args = parser.parse_args()
    t = Transporter(mcp_url=args.mcp_url, out_dir=args.out_dir, neo4j_uri=args.neo4j_uri, neo4j_user=args.neo4j_user, neo4j_password=args.neo4j_password)
    t.offline = bool(args.offline)
    t.details_strategy = args.details_strategy
    parts, edges = t.collect(args.part_number, max_depth=args.max_depth)
    if args.hydrate_details:
        id_index_path = t.processed_dir / "id_index.json"
        id_index = t._load_json(id_index_path) or {}
        pn_to_id_idx: Dict[str, str] = (t._load_json(t.out_dir / "index.json") or {}).get("pn_to_id") or {}
        pn_to_id_proc: Dict[str, str] = id_index.get("pn_to_id") or {}
        pn_to_id: Dict[str, str] = {**pn_to_id_idx, **pn_to_id_proc}
        changed = False
        for pn, oid in pn_to_id.items():
            det = parts.get(pn) or {}
            if det.get("name"):
                continue
            res = t._get_details_by_id_cached(oid) or {}
            norm = t._normalize_part_details(res if isinstance(res, dict) else {})
            if any(v is not None for v in norm.values()):
                parts[pn] = norm
                changed = True
        if changed:
            t._save_json(t.processed_dir / "parts.json", parts)
    t.load_into_neo4j(parts, edges)
    print(json.dumps({"parts": len(parts), "edges": len(edges), "out": str(args.out_dir)}, indent=2))

if __name__ == "__main__":
    main()