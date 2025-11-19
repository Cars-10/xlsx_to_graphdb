#!/usr/bin/env python3
import json
import logging
from neo4j import GraphDatabase, basic_auth
from datetime import datetime
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def run_query(driver, query, params=None):
    with driver.session() as session:
        res = session.run(query, params or {})
        return [r.data() for r in res]

def main():
    uri = "bolt://localhost:7687"
    auth = basic_auth("neo4j", "tstpwdpwd")
    driver = GraphDatabase.driver(uri, auth=auth)
    logger.info("Connected to Neo4j")

    def read_numbers(path: str) -> list:
        xl = pd.ExcelFile(path)
        nums = set()
        for sheet in xl.sheet_names:
            df = None
            try:
                df = pd.read_excel(path, sheet_name=sheet, skiprows=4)
            except Exception:
                try:
                    df = pd.read_excel(path, sheet_name=sheet)
                except Exception:
                    df = None
            if df is None or df.empty:
                continue
            first_vals = list(df.iloc[0].values) if len(df) > 0 else []
            if 'Action' in map(str, first_vals):
                df.columns = df.iloc[0]
                df = df[1:]
            cols = {str(c).strip().lower(): c for c in df.columns}
            if 'number' in cols:
                for _, row in df.iterrows():
                    val = row.get(cols['number'])
                    if pd.isna(val):
                        continue
                    s = str(val).strip()
                    if s:
                        nums.add(s)
        return list(nums)

    numbers = read_numbers("data/Mower.xlsx")

    q_parts = """
    UNWIND $numbers AS num
    MATCH (p:WTPart {number: num})
    RETURN count(p) AS count
    """
    q_bom = """
    UNWIND $numbers AS num
    MATCH (p:WTPart {number: num})
    WITH collect(p) AS ps
    MATCH (p)-[r:HAS_COMPONENT]->(c)
    WHERE p IN ps AND c IN ps
    RETURN count(r) AS count
    """
    q_docs = """
    UNWIND $numbers AS num
    MATCH (p:WTPart {number: num})
    MATCH (d:Document)-[r:DESCRIBES]->(p)
    RETURN count(r) AS count
    """
    q_changes = """
    UNWIND $numbers AS num
    MATCH (p:WTPart {number: num})
    MATCH (c:Change)-[r:AFFECTS_PART]->(p)
    RETURN count(DISTINCT c) AS count
    """
    q_change_rels = """
    UNWIND $numbers AS num
    MATCH (p:WTPart {number: num})
    MATCH (:Change)-[r:AFFECTS_PART]->(p)
    RETURN count(r) AS count
    """
    q_changes_by_source = """
    UNWIND $numbers AS num
    MATCH (p:Part {number: num})
    MATCH (c:Change)-[:AFFECTS_PART]->(p)
    RETURN coalesce(c.source,'unknown') AS source, count(DISTINCT c) AS count
    """
    q_change_labels = """
    UNWIND $numbers AS num
    MATCH (p:Part {number: num})
    MATCH (c:Change)-[:AFFECTS_PART]->(p)
    RETURN [label IN labels(c) WHERE label <> 'Change'][0] AS label, count(DISTINCT c) AS count
    """
    q_change_colors = """
    UNWIND $numbers AS num
    MATCH (p:Part {number: num})
    MATCH (c:Change)-[:AFFECTS_PART]->(p)
    RETURN coalesce(c.color,'none') AS color, count(DISTINCT c) AS count
    """
    q_samples = """
    UNWIND $numbers AS num
    MATCH (p:Part {number: num})
    RETURN p.number AS number, p.name AS name
    ORDER BY number
    LIMIT 10
    """

    parts = run_query(driver, q_parts, {"numbers": numbers})[0]["count"]
    bom = run_query(driver, q_bom, {"numbers": numbers})[0]["count"]
    docs = run_query(driver, q_docs, {"numbers": numbers})[0]["count"]
    changes = run_query(driver, q_changes, {"numbers": numbers})[0]["count"]
    change_rels = run_query(driver, q_change_rels, {"numbers": numbers})[0]["count"]
    changes_source_rows = run_query(driver, q_changes_by_source, {"numbers": numbers})
    label_rows = run_query(driver, q_change_labels, {"numbers": numbers})
    color_rows = run_query(driver, q_change_colors, {"numbers": numbers})
    changes_by_source = {row["source"]: row["count"] for row in changes_source_rows}
    labels_breakdown = {row.get("label") or "Change": row["count"] for row in label_rows}
    colors_breakdown = {row.get("color") or "none": row["count"] for row in color_rows}
    samples = run_query(driver, q_samples, {"numbers": numbers})

    q_containers = """
    MATCH (p:WTPart)
    RETURN toLower(coalesce(p.container,'')) AS container, count(p) AS count
    ORDER BY count DESC
    LIMIT 10
    """
    containers = run_query(driver, q_containers)

    report = {
        "timestamp": datetime.now().isoformat(),
        "summary": {
            "container": "Mower",
            "total_parts": parts,
            "bom_relationships": bom,
            "describe_links": docs,
            "changes": changes,
            "change_relationships": change_rels,
            "changes_by_source": changes_by_source,
            "changes_by_label": labels_breakdown,
            "changes_by_color": colors_breakdown,
        },
        "samples": samples,
        "containers": containers,
    }

    out = "data/processed/mower_graph_verification_report.json"
    with open(out, "w") as f:
        json.dump(report, f, indent=2)
    logger.info(f"Saved report to {out}")

    logger.info("=== MOWER IMPORT SUMMARY ===")
    logger.info(f"Parts: {parts}")
    logger.info(f"BOM relationships: {bom}")
    logger.info(f"DESCRIBES links: {docs}")
    logger.info(f"Changes affecting parts: {changes}")
    logger.info(f"AFFECTS_PART relationships: {change_rels}")
    for src, cnt in changes_by_source.items():
        logger.info(f"Changes[{src}]: {cnt}")
    for lbl, cnt in labels_breakdown.items():
        logger.info(f"Changes[label={lbl}]: {cnt}")
    for col, cnt in colors_breakdown.items():
        logger.info(f"Changes[color={col}]: {cnt}")
    for row in containers:
        logger.info(f"Container '{row['container']}': {row['count']} parts")

    driver.close()

if __name__ == "__main__":
    main()