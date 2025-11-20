#!/usr/bin/env python3
"""
Graph visualization script for BOM data from Snowmobile.xlsx

This script builds and visualizes the Bill of Materials (BOM) hierarchy
starting from a specified part number, using part names as node labels.
"""

import sys
import argparse
import logging
from typing import Dict, List, Tuple, Optional, Set
from collections import defaultdict
from pathlib import Path
import json

import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch
from mpl_toolkits.mplot3d import Axes3D  # noqa: F401


def load_name_index(name_index_path: str) -> Dict[str, str]:
    """Load the part number to name mapping from CSV."""
    try:
        df = pd.read_csv(name_index_path)
        # Check if first row is a duplicate header
        if len(df) > 0:
            first_row = df.iloc[0]
            # Check column names - might be 'Part Number' or 'Number'
            col_names = list(df.columns)
            if 'Number' in str(first_row.values):
                # Duplicate header detected, skip it
                df = df.iloc[1:].reset_index(drop=True)

        name_index = {}
        # Handle both possible column name formats
        pn_col = 'Part Number' if 'Part Number' in df.columns else 'Number'
        name_col = 'Name'

        for _, row in df.iterrows():
            pn = str(row[pn_col]).strip()
            name = str(row[name_col]).strip()
            if pn and name and pn != 'nan' and name != 'nan':
                name_index[pn] = name
        logging.info(f"Loaded {len(name_index)} part names from {name_index_path}")
        return name_index
    except FileNotFoundError:
        logging.warning(f"Name index not found at {name_index_path}, will use part numbers as labels")
        return {}


def parse_hierarchical_bom(bom_csv_path: str) -> List[Tuple[str, str, int]]:
    """
    Parse hierarchical BOM CSV and return parent-child-level triples.

    The BOM uses indentation levels to represent hierarchy:
    - Level 0: Root part
    - Level 1: Direct children of root
    - Level 2: Children of most recent Level 1 part
    - etc.

    Returns:
        List of (parent_number, child_number, level) tuples
    """
    df = pd.read_csv(bom_csv_path, skiprows=1)  # Skip duplicate header row

    # Filter to valid rows with Number
    df = df[df['Number'].notna()].copy()
    df['Number'] = df['Number'].astype(str).str.strip()
    df['Level'] = df['Level'].astype(int)

    edges: List[Tuple[str, str, int]] = []
    level_stack: Dict[int, str] = {}  # Maps level to most recent part at that level

    for _, row in df.iterrows():
        part_number = row['Number']
        level = row['Level']

        # Track this part at its level
        level_stack[level] = part_number

        # If not root (level 0), find parent (most recent part at level-1)
        if level > 0:
            parent_level = level - 1
            if parent_level in level_stack:
                parent = level_stack[parent_level]
                edges.append((parent, part_number, level))
            else:
                logging.warning(f"No parent found for {part_number} at level {level}")

        # Clear deeper levels when we encounter a part
        levels_to_clear = [l for l in level_stack.keys() if l > level]
        for l in levels_to_clear:
            del level_stack[l]

    logging.info(f"Parsed {len(edges)} parent-child relationships from BOM")
    return edges


def build_subgraph(
    edges: List[Tuple[str, str, int]],
    root_part: str,
    name_index: Dict[str, str],
    max_depth: Optional[int] = None,
    max_children: Optional[int] = None
) -> nx.DiGraph:
    """
    Build a directed graph starting from root_part.

    Args:
        edges: List of (parent, child, level) tuples
        root_part: Part number to start from
        name_index: Mapping from part numbers to names
        max_depth: Maximum depth to traverse (None = unlimited)
        max_children: Maximum children to include per parent (None = unlimited)

    Returns:
        NetworkX directed graph with 'label' attribute on nodes
    """
    G = nx.DiGraph()

    # Build adjacency list from edges
    children_map: Dict[str, List[str]] = defaultdict(list)
    for parent, child, level in edges:
        children_map[parent].append(child)

    # BFS traversal from root
    visited: Set[str] = set()
    queue: List[Tuple[str, int]] = [(root_part, 0)]

    while queue:
        part, depth = queue.pop(0)

        if part in visited:
            continue
        visited.add(part)

        # Add node with label
        label = name_index.get(part, part)
        G.add_node(part, label=label, depth=depth)

        # Stop if we've reached max depth
        if max_depth is not None and depth >= max_depth:
            continue

        # Add children
        children = children_map.get(part, [])
        if max_children is not None:
            children = children[:max_children]

        for child in children:
            G.add_edge(part, child)
            queue.append((child, depth + 1))

    logging.info(f"Built graph with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges")
    return G


def build_subgraph_from_pairs(
    edges: List[Tuple[str, str]],
    root_part: str,
    name_index: Dict[str, str],
    max_depth: Optional[int] = None,
    max_children: Optional[int] = None
) -> nx.DiGraph:
    G = nx.DiGraph()
    children_map: Dict[str, List[str]] = defaultdict(list)
    for parent, child in edges:
        children_map[str(parent)].append(str(child))
    visited: Set[str] = set()
    queue: List[Tuple[str, int]] = [(root_part, 0)]
    while queue:
        part, depth = queue.pop(0)
        if part in visited:
            continue
        visited.add(part)
        label = name_index.get(part, part)
        G.add_node(part, label=label, depth=depth)
        if max_depth is not None and depth >= max_depth:
            continue
        children = children_map.get(part, [])
        if max_children is not None:
            children = children[:max_children]
        for child in children:
            G.add_edge(part, child)
            queue.append((child, depth + 1))
    logging.info(f"Built graph with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges")
    return G


def visualize_graph(
    G: nx.DiGraph,
    root_part: str,
    output_path: Optional[str] = None,
    layout: str = 'hierarchical',
    figsize: Tuple[int, int] = (20, 12),
    show_labels: bool = True
):
    """
    Visualize the graph using matplotlib.

    Args:
        G: NetworkX graph
        root_part: Root part number for layout reference
        output_path: Path to save figure (None = display only)
        layout: 'hierarchical', 'spring', or 'circular'
        figsize: Figure size in inches
        show_labels: Whether to show node labels
    """
    fig, ax = plt.subplots(figsize=figsize)

    # Choose layout algorithm
    if layout == 'hierarchical':
        # Use graphviz_layout if available, fall back to spring
        try:
            pos = nx.nx_agraph.graphviz_layout(G, prog='dot')
        except:
            logging.warning("Graphviz not available, falling back to spring layout")
            pos = nx.spring_layout(G, k=2, iterations=50, seed=42)
    elif layout == 'spring':
        pos = nx.spring_layout(G, k=2, iterations=50, seed=42)
    elif layout == 'circular':
        pos = nx.circular_layout(G)
    else:
        raise ValueError(f"Unknown layout: {layout}")

    # Color nodes by depth
    depths = nx.get_node_attributes(G, 'depth')
    max_depth = max(depths.values()) if depths else 0
    node_colors = [depths.get(node, 0) for node in G.nodes()]

    # Draw the graph
    nx.draw_networkx_edges(
        G, pos,
        edge_color='#666666',
        arrows=True,
        arrowsize=15,
        arrowstyle='->',
        width=1.5,
        alpha=0.6,
        ax=ax
    )

    # Draw nodes
    nx.draw_networkx_nodes(
        G, pos,
        node_color=node_colors,
        node_size=2000,
        cmap=plt.cm.viridis,
        vmin=0,
        vmax=max(max_depth, 1),
        alpha=0.9,
        edgecolors='white',
        linewidths=2,
        ax=ax
    )

    # Draw labels with part names
    if show_labels:
        labels = nx.get_node_attributes(G, 'label')
        nx.draw_networkx_labels(
            G, pos,
            labels=labels,
            font_size=8,
            font_weight='bold',
            font_color='white',
            ax=ax
        )

    # Highlight root node
    root_label = G.nodes[root_part]['label'] if root_part in G.nodes else root_part
    ax.set_title(
        f'BOM Graph for: {root_label} ({root_part})\n'
        f'{G.number_of_nodes()} parts, {G.number_of_edges()} relationships, '
        f'{max_depth} levels deep',
        fontsize=16,
        fontweight='bold',
        pad=20
    )

    # Add colorbar for depth
    sm = plt.cm.ScalarMappable(
        cmap=plt.cm.viridis,
        norm=plt.Normalize(vmin=0, vmax=max(max_depth, 1))
    )
    sm.set_array([])
    cbar = plt.colorbar(sm, ax=ax, label='Depth Level', shrink=0.5)

    ax.axis('off')
    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
        logging.info(f"Saved visualization to {output_path}")
    else:
        plt.show()


def visualize_graph_3d(
    G: nx.DiGraph,
    root_part: str,
    output_path: Optional[str] = None,
    figsize: Tuple[int, int] = (20, 12),
    show_labels: bool = True
):
    fig = plt.figure(figsize=figsize)
    ax = fig.add_subplot(111, projection='3d')
    pos = nx.spring_layout(G, k=2, iterations=50, seed=42, dim=3)
    depths = nx.get_node_attributes(G, 'depth')
    max_depth = max(depths.values()) if depths else 0
    xs = [pos[n][0] for n in G.nodes()]
    ys = [pos[n][1] for n in G.nodes()]
    zs = [pos[n][2] for n in G.nodes()]
    node_colors = [depths.get(n, 0) for n in G.nodes()]
    ax.scatter(xs, ys, zs, c=node_colors, s=40, cmap=plt.cm.viridis, vmin=0, vmax=max(max_depth, 1))
    for u, v in G.edges():
        x = [pos[u][0], pos[v][0]]
        y = [pos[u][1], pos[v][1]]
        z = [pos[u][2], pos[v][2]]
        ax.plot(x, y, z, color='#666666', linewidth=1, alpha=0.6)
    if show_labels:
        labels = nx.get_node_attributes(G, 'label')
        for n, lbl in labels.items():
            x, y, z = pos[n]
            ax.text(x, y, z, lbl, fontsize=6, color='black')
    root_label = G.nodes[root_part]['label'] if root_part in G.nodes else root_part
    ax.set_title(
        f'BOM 3D Graph for: {root_label} ({root_part})\n{G.number_of_nodes()} parts, {G.number_of_edges()} relationships, {max_depth} levels deep',
        fontsize=14,
        fontweight='bold'
    )
    sm = plt.cm.ScalarMappable(cmap=plt.cm.viridis, norm=plt.Normalize(vmin=0, vmax=max(max_depth, 1)))
    sm.set_array([])
    fig.colorbar(sm, ax=ax, shrink=0.5, pad=0.1, label='Depth Level')
    ax.set_axis_off()
    plt.tight_layout()
    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
        logging.info(f"Saved 3D visualization to {output_path}")
    else:
        plt.show()


def print_graph_stats(G: nx.DiGraph, root_part: str):
    """Print statistics about the graph."""
    print(f"\n{'='*60}")
    print(f"Graph Statistics for {root_part}")
    print(f"{'='*60}")
    print(f"Total nodes: {G.number_of_nodes()}")
    print(f"Total edges: {G.number_of_edges()}")

    depths = nx.get_node_attributes(G, 'depth')
    max_depth = max(depths.values()) if depths else 0
    print(f"Maximum depth: {max_depth}")

    # Parts per level
    level_counts = defaultdict(int)
    for node, depth in depths.items():
        level_counts[depth] += 1

    print(f"\nParts per level:")
    for level in sorted(level_counts.keys()):
        print(f"  Level {level}: {level_counts[level]} parts")

    # Find parts with most children
    out_degrees = dict(G.out_degree())
    if out_degrees:
        max_children = max(out_degrees.values())
        parts_with_most_children = [
            (node, G.nodes[node]['label'], deg)
            for node, deg in out_degrees.items()
            if deg == max_children
        ]
        print(f"\nParts with most children ({max_children}):")
        for pn, name, deg in parts_with_most_children[:5]:
            print(f"  {name} ({pn}): {deg} children")

    # Direct children of root
    if root_part in G:
        direct_children = list(G.successors(root_part))
        print(f"\nDirect children of {root_part}: {len(direct_children)}")
        for child in direct_children[:10]:
            label = G.nodes[child]['label']
            print(f"  - {label} ({child})")
        if len(direct_children) > 10:
            print(f"  ... and {len(direct_children) - 10} more")

    print(f"{'='*60}\n")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description='Visualize BOM graph starting from a specific part',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Visualize full graph for 100 SNOW
  python visualize_graph.py --root "100 SNOW"

  # Limit depth to 3 levels
  python visualize_graph.py --root "100 SNOW" --max-depth 3

  # Save to file instead of displaying
  python visualize_graph.py --root "100 SNOW" --output snowmobile_graph.png

  # Use spring layout with limited children per node
  python visualize_graph.py --root "100 SNOW" --layout spring --max-children 10
        """
    )

    parser.add_argument(
        '--root',
        default='100 SNOW',
        help='Root part number to start visualization from (default: 100 SNOW)'
    )
    parser.add_argument(
        '--bom',
        default='bom.csv',
        help='Path to BOM CSV file (default: bom.csv)'
    )
    parser.add_argument(
        '--name-index',
        default='name_index.csv',
        help='Path to name index CSV file (default: name_index.csv)'
    )
    parser.add_argument(
        '--transporter-dir',
        default=None,
        help='Path to transporter processed directory containing edges.json and parts.json'
    )
    parser.add_argument(
        '--output',
        default=None,
        help='Output path for visualization (default: display only)'
    )
    parser.add_argument(
        '--max-depth',
        type=int,
        default=None,
        help='Maximum depth to traverse (default: unlimited)'
    )
    parser.add_argument(
        '--max-children',
        type=int,
        default=None,
        help='Maximum children per parent (default: unlimited)'
    )
    parser.add_argument(
        '--layout',
        choices=['hierarchical', 'spring', 'circular'],
        default='hierarchical',
        help='Graph layout algorithm (default: hierarchical)'
    )
    parser.add_argument(
        '--plot-3d',
        action='store_true',
        help='Render 3D visualization'
    )
    parser.add_argument(
        '--figsize',
        type=int,
        nargs=2,
        default=[20, 12],
        help='Figure size in inches (width height) (default: 20 12)'
    )
    parser.add_argument(
        '--no-labels',
        action='store_true',
        help='Hide node labels (useful for large graphs)'
    )
    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (default: INFO)'
    )
    parser.add_argument(
        '--stats-only',
        action='store_true',
        help='Print statistics only, do not visualize'
    )

    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s %(levelname)s %(message)s'
    )

    try:
        use_transporter = bool(args.transporter_dir)
        if use_transporter:
            tp_dir = Path(args.transporter_dir)
            edges_path = tp_dir / 'edges.json'
            parts_path = tp_dir / 'parts.json'
            if not edges_path.exists() or not parts_path.exists():
                logging.error('Missing transporter processed files: edges.json or parts.json')
                return 1
            with open(parts_path, 'r') as f:
                parts_obj = json.load(f)
            name_index = {str(k): str(v.get('name') or k) for k, v in parts_obj.items()}
            with open(edges_path, 'r') as f:
                raw_edges = json.load(f)
            edges_pairs: List[Tuple[str, str]] = [(str(a), str(b)) for a, b in raw_edges]
            logging.info("Building graph from transporter data")
            G = build_subgraph_from_pairs(
                edges_pairs,
                args.root,
                name_index,
                max_depth=args.max_depth,
                max_children=args.max_children
            )
        else:
            logging.info("Loading name index...")
            name_index = load_name_index(args.name_index)
            logging.info("Parsing BOM hierarchy...")
            edges = parse_hierarchical_bom(args.bom)
            logging.info(f"Building graph from root: {args.root}")
            G = build_subgraph(
                edges,
                args.root,
                name_index,
                max_depth=args.max_depth,
                max_children=args.max_children
            )

        if G.number_of_nodes() == 0:
            logging.error(f"No nodes found starting from {args.root}. Check that the part number exists.")
            return 1

        # Print statistics
        print_graph_stats(G, args.root)

        # Visualize
        if not args.stats_only:
            logging.info("Creating visualization...")
            if args.plot_3d:
                visualize_graph_3d(
                    G,
                    args.root,
                    output_path=args.output,
                    figsize=tuple(args.figsize),
                    show_labels=not args.no_labels
                )
            else:
                visualize_graph(
                    G,
                    args.root,
                    output_path=args.output,
                    layout=args.layout,
                    figsize=tuple(args.figsize),
                    show_labels=not args.no_labels
                )

        return 0

    except Exception as e:
        logging.error(f"Visualization failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
