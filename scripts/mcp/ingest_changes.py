#!/usr/bin/env python3
import argparse
import logging
from typing import Dict, List, Optional

from neo4j import GraphDatabase, basic_auth
import sys
import os
# Reuse the Enhanced MCP client
try:
    from scripts.mcp.enhanced_windchill_mcp_client import EnhancedWindchillMCPClient
except ImportError:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from mcp.enhanced_windchill_mcp_client import EnhancedWindchillMCPClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


def map_change_type_to_label_and_color(change_type: Optional[str]) -> (str, str):
    t = (change_type or '').lower()
    if any(x in t for x in ['changerequest', 'change request', 'ecr']):
        return 'ChangeRequest', '#FFEB3B'
    if any(x in t for x in ['changenotice', 'change notice', 'ecn', 'changeorder', 'change order']):
        return 'ChangeNotice', '#FFC107'
    if any(x in t for x in ['problemreport', 'problem report', 'pr']):
        return 'ProblemReport', '#FFF176'
    if any(x in t for x in ['changeactivity', 'change activity', 'ca']):
        return 'ChangeActivity', '#FFD54F'
    return 'Change', '#FFF59D'


def fetch_container_part_numbers(driver, container: str) -> List[str]:
    query = """
    MATCH (p:WTPart)
    WHERE toLower(coalesce(p.container,'')) = toLower($container)
    RETURN p.number AS number
    """
    with driver.session() as session:
        res = session.run(query, {"container": container})
        return [r["number"] for r in res]


def ingest_changes_for_container(driver, client: EnhancedWindchillMCPClient, container: str, limit_per_part: Optional[int] = None):
    part_numbers = fetch_container_part_numbers(driver, container)
    if not part_numbers:
        logger.warning(f"No parts found for container '{container}'")
        return
    logger.info(f"Found {len(part_numbers)} parts in container '{container}'")

    rows: List[Dict[str, str]] = []

    for i, pn in enumerate(part_numbers):
        if limit_per_part and i >= limit_per_part:
            break
        changes = client.get_part_changes(pn) or []
        if not changes:
            continue
        for ch in changes:
            ch_num = ch.get('Number') or ch.get('number')
            ch_name = ch.get('Name') or ch.get('name')
            ch_state = ch.get('State') or ch.get('state')
            ch_type = ch.get('Type') or ch.get('type')
            label, color = map_change_type_to_label_and_color(ch_type)
            affected = ch.get('AffectedObjects') or ch.get('affected_objects') or []

            # If MCP didn't include affected list, at minimum link to the queried part
            if not affected:
                rows.append({
                    'number': str(ch_num or f'ECN-{pn}'),
                    'name': ch_name,
                    'state': ch_state or 'INWORK',
                    'type': ch_type or 'ChangeNotice',
                    'label': label,
                    'color': color,
                    'container': container,
                    'source': 'mcp',
                    'part': pn,
                })
            else:
                for ao in affected:
                    apn = ao.get('Number') or ao.get('number') or pn
                    rows.append({
                        'number': str(ch_num or f'ECN-{apn}'),
                        'name': ch_name,
                        'state': ch_state or 'INWORK',
                        'type': ch_type or 'ChangeNotice',
                        'label': label,
                        'color': color,
                        'container': container,
                        'source': 'mcp',
                        'part': str(apn),
                    })

    if not rows:
        logger.warning(f"No change rows collected for container '{container}' via per-part lookup; falling back to global change scan")
        # Fallback: fetch all changes, then filter by container and link to container's root assembly
        years = list(range(2018, 2026))
        all_changes = client.get_all_change_objects_paged(years, limit_per_window=200) or client.get_all_change_objects(limit=2000) or []
        # Determine a representative root assembly part for the container
        fallback_root_query = (
            "MATCH (root:WTPart) "
            "WHERE toLower(coalesce(root.container,'')) = toLower($container) AND NOT ()-[:HAS_COMPONENT]->(root) "
            "RETURN root.number AS number LIMIT 1"
        )
        with driver.session() as session:
            rec = session.run(fallback_root_query, {"container": container}).single()
            root_pn = rec["number"] if rec else None
        part_texts = [pn.lower() for pn in part_numbers if pn]
        part_name_map: Dict[str, str] = {}
        with driver.session() as session:
            recs = session.run(
                "MATCH (p:WTPart) WHERE toLower(coalesce(p.container,'')) = toLower($container) RETURN p.number AS n, toLower(coalesce(p.name,'')) AS nm",
                {"container": container}
            )
            for r in recs:
                n = r["n"]
                nm = r["nm"]
                if n:
                    part_name_map[str(n)] = nm or ''
        for ch in all_changes:
            folder = ch.get('FolderLocation') or ''
            if container.lower() not in folder.lower():
                continue
            ch_num = ch.get('Number') or ch.get('Identity')
            ch_name = ch.get('Name') or ''
            state = ch.get('State')
            ch_state = (state.get('Display') if isinstance(state, dict) else state) or 'INWORK'
            obj_type = ch.get('ObjectType') or ''
            t = obj_type.lower()
            ch_type = 'ChangeNotice' if 'notice' in t else ('ChangeRequest' if 'request' in t else 'Change')
            label, color = map_change_type_to_label_and_color(ch_type)
            txt = (ch.get('Description') or '') + ' ' + ch_name
            txtl = txt.lower()
            matched = []
            for pn in part_numbers:
                pl = pn.lower()
                nm = part_name_map.get(pn, '')
                if pl and pl in txtl:
                    matched.append(pn)
                elif nm and nm in txtl:
                    matched.append(pn)
            if matched:
                for apn in matched[:5]:
                    rows.append({
                        'number': str(ch_num or f'ECN-{apn}'),
                        'name': ch_name,
                        'state': ch_state,
                        'type': ch_type,
                        'label': label,
                        'color': color,
                        'container': container,
                        'source': 'mcp',
                        'part': str(apn),
                    })
            else:
                part_to_link = root_pn or part_numbers[0]
                rows.append({
                        'number': str(ch_num or f'ECN-{part_to_link}'),
                        'name': ch_name,
                        'state': ch_state,
                        'type': ch_type,
                        'label': label,
                        'color': color,
                        'container': container,
                        'source': 'mcp',
                        'part': str(part_to_link),
                    })
        if not rows:
            logger.warning(f"Global change scan produced no rows for '{container}'")
            return

    cypher = (
        "UNWIND $rows AS row "
        "MERGE (c:Change {number: row.number}) "
        "SET c.name = row.name, c.state = row.state, c.type = row.type, c.source = row.source, c.container = row.container, c.color = row.color "
        "FOREACH(_ IN CASE WHEN row.label='ChangeRequest' THEN [1] ELSE [] END | SET c:ChangeRequest) "
        "FOREACH(_ IN CASE WHEN row.label='ChangeNotice' THEN [1] ELSE [] END | SET c:ChangeNotice) "
        "FOREACH(_ IN CASE WHEN row.label='ProblemReport' THEN [1] ELSE [] END | SET c:ProblemReport) "
        "FOREACH(_ IN CASE WHEN row.label='ChangeActivity' THEN [1] ELSE [] END | SET c:ChangeActivity) "
        "MERGE (p:WTPart {number: row.part}) "
        "MERGE (c)-[:AFFECTS_PART]->(p)"
    )

    with driver.session() as session:
        session.run(cypher, {"rows": rows})
    logger.info(f"Ingested {len(rows)} change-part links for container '{container}'")

    # Cleanup synthetic changes when real MCP changes exist for same part
    cleanup = (
        "MATCH (p:WTPart) WHERE toLower(coalesce(p.container,'')) = toLower($container) "
        "MATCH (c:Change {source:'synthetic'})-[:AFFECTS_PART]->(p) "
        "OPTIONAL MATCH (m:Change {source:'mcp'})-[:AFFECTS_PART]->(p) "
        "WITH c, count(m) AS mcps "
        "WHERE mcps > 0 "
        "DETACH DELETE c"
    )
    with driver.session() as session:
        session.run(cleanup, {"container": container})
    logger.info(f"Removed synthetic changes where MCP replacements exist for '{container}'")


def main():
    parser = argparse.ArgumentParser(description='Ingest Windchill changes via MCP into Neo4j')
    parser.add_argument('--uri', default='bolt://localhost:7687')
    parser.add_argument('--user', default='neo4j')
    parser.add_argument('--password', default='tstpwdpwd')
    parser.add_argument('--mcp-url', default='http://localhost:3000')
    parser.add_argument('--containers', nargs='+', required=True, help='Containers to ingest (e.g., Helicopter Snowmobile Mower)')
    parser.add_argument('--limit-per-part', type=int, default=None)
    parser.add_argument('--changes-csv', default=None, help='Optional path to a CSV of change records to ingest')
    parser.add_argument('--csv-container', default=None)
    args = parser.parse_args()

    driver = GraphDatabase.driver(args.uri, auth=basic_auth(args.user, args.password))
    client = EnhancedWindchillMCPClient(base_url=args.mcp_url)

    if args.changes_csv:
        import csv
        rows: List[Dict[str, str]] = []
        csv_container = args.csv_container
        if not csv_container:
            if len(args.containers) == 1:
                csv_container = args.containers[0]
            else:
                fname = os.path.basename(args.changes_csv).lower()
                if 'heli' in fname:
                    csv_container = 'Helicopter'
                elif 'snow' in fname:
                    csv_container = 'Snowmobile'
                elif 'mower' in fname:
                    csv_container = 'Mower'
                else:
                    csv_container = ''
        with open(args.changes_csv, 'r') as f:
            reader = csv.DictReader(f)
            for r in reader:
                part_number = (
                    r.get('affected_part_number') or r.get('Part') or r.get('part') or r.get('_part_number') or r.get('part_number') or r.get('Number')
                )
                part_name = (
                    r.get('affected_part_name') or r.get('PartName') or r.get('_part_name') or r.get('part_name') or r.get('Name')
                )
                ch_type = r.get('type') or r.get('Type') or 'ChangeNotice'
                label, color = map_change_type_to_label_and_color(ch_type)
                ch_num = r.get('number') or r.get('ChangeNumber')
                if not ch_num:
                    rev = r.get('revision') or r.get('Revision') or ''
                    ch_num = f"CSV-{part_number}-{rev}".strip('-')
                ch_name = r.get('name') or r.get('Name') or (part_name and f"Change for {part_name}") or (part_number and f"Change for {part_number}")
                ch_state = r.get('state') or r.get('State') or 'INWORK'
                if part_number and ch_num:
                    rows.append({
                        'number': ch_num,
                        'name': ch_name,
                        'state': ch_state,
                        'type': ch_type,
                        'label': label,
                        'color': color,
                        'container': r.get('container') or r.get('Container') or csv_container,
                        'source': 'csv',
                        'part': str(part_number),
                    })
        cypher = (
            "UNWIND $rows AS row "
            "MERGE (c:Change {number: row.number}) "
            "SET c.name = row.name, c.state = row.state, c.type = row.type, c.source = row.source, c.container = row.container, c.color = row.color "
            "FOREACH(_ IN CASE WHEN row.label='ChangeRequest' THEN [1] ELSE [] END | SET c:ChangeRequest) "
            "FOREACH(_ IN CASE WHEN row.label='ChangeNotice' THEN [1] ELSE [] END | SET c:ChangeNotice) "
            "FOREACH(_ IN CASE WHEN row.label='ProblemReport' THEN [1] ELSE [] END | SET c:ProblemReport) "
            "FOREACH(_ IN CASE WHEN row.label='ChangeActivity' THEN [1] ELSE [] END | SET c:ChangeActivity) "
            "MERGE (p:WTPart {number: row.part}) "
            "MERGE (c)-[:AFFECTS_PART]->(p)"
        )
        with driver.session() as session:
            session.run(cypher, { 'rows': [r for r in rows if r.get('number') and r.get('part')] })
        logger.info(f"Ingested {len([r for r in rows if r.get('number') and r.get('part')])} change records from CSV")

        cleanup_csv = (
            "UNWIND $parts AS pn "
            "MATCH (p:WTPart {number: pn}) "
            "MATCH (s:Change {source:'synthetic'})-[:AFFECTS_PART]->(p) "
            "DETACH DELETE s"
        )
        part_list = list({r.get('part') for r in rows if r.get('part')})
        with driver.session() as session:
            session.run(cleanup_csv, { 'parts': part_list })
        logger.info(f"Removed synthetic changes for {len(part_list)} parts present in CSV")

    if client.test_connection():
        for container in args.containers:
            ingest_changes_for_container(driver, client, container, args.limit_per_part)
    else:
        logger.warning('MCP server not reachable; skipped MCP ingestion')

    driver.close()
    logger.info('MCP change ingestion complete')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())