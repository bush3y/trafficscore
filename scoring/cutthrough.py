"""
Cut-through risk scoring.

Analyzes the OSM road network graph to identify residential streets
that are structurally attractive for cut-through traffic:
  - Residential/tertiary street that connects two higher-class roads
  - Not a dead end
  - Short enough to be a useful shortcut

Score: 0.0 (no risk) → 1.0 (high cut-through risk)

Usage:
    python -m scoring.cutthrough
"""

import os

import psycopg2
from psycopg2.extras import execute_values
import networkx as nx
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]

# Roads considered "attractors" — roads cut-through traffic is moving between.
# Including tertiary because many Ottawa collectors/arterials are tagged tertiary in OSM.
ARTERIAL_CLASSES = {"primary", "primary_link", "secondary", "secondary_link", "tertiary", "tertiary_link"}
# Roads considered at risk for cut-through use
CUTTHROUGH_CANDIDATE_CLASSES = {"residential", "unclassified", "living_street"}

# Maximum length (metres) of a segment to be considered a useful shortcut
MAX_SHORTCUT_LENGTH_M = 2000


def load_segments(conn) -> list[dict]:
    cur = conn.cursor()
    cur.execute("""
        SELECT
            id,
            road_class,
            ST_AsGeoJSON(geometry)::json AS geom,
            ST_Length(geometry::geography) AS length_m,
            ST_StartPoint(geometry) AS start_pt,
            ST_EndPoint(geometry) AS end_pt
        FROM road_segments
    """)
    rows = cur.fetchall()
    cur.close()

    segments = []
    for row in rows:
        seg_id, road_class, geom, length_m, start_pt, end_pt = row
        segments.append({
            "id": seg_id,
            "road_class": road_class,
            "length_m": length_m,
            "geom": geom,
        })
    return segments


def build_graph(conn) -> nx.Graph:
    """Build a road network graph from road_segments using node coordinates."""
    cur = conn.cursor()
    cur.execute("""
        SELECT
            id,
            name,
            road_class,
            ST_Length(geometry::geography) AS length_m,
            ST_X(ST_StartPoint(geometry)) AS start_lon,
            ST_Y(ST_StartPoint(geometry)) AS start_lat,
            ST_X(ST_EndPoint(geometry))   AS end_lon,
            ST_Y(ST_EndPoint(geometry))   AS end_lat
        FROM road_segments
    """)
    rows = cur.fetchall()
    cur.close()

    G = nx.Graph()
    segment_lookup = {}  # edge (u,v) -> segment info

    for seg_id, name, road_class, length_m, slon, slat, elon, elat in rows:
        # Round to ~10m precision to merge near-identical nodes
        u = (round(slon, 4), round(slat, 4))
        v = (round(elon, 4), round(elat, 4))

        G.add_node(u, road_classes=set())
        G.add_node(v, road_classes=set())
        G.nodes[u]["road_classes"].add(road_class)
        G.nodes[v]["road_classes"].add(road_class)

        G.add_edge(u, v, seg_id=seg_id, road_class=road_class, road_name=name, length_m=length_m or 0)
        segment_lookup[seg_id] = (u, v, road_class, length_m or 0, name)

    print(f"Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    return G, segment_lookup


def node_arterial_names(node, G, road_name: str = None) -> set:
    """Return set of arterial road names connecting to this node.

    If the node has no direct arterial connections but only connects to
    segments of the same road name (i.e. it's an interior junction within
    a multi-segment crescent), follow those same-name segments to their far
    ends and check those for arterials instead.
    """
    names = set()
    for _, neighbour, data in G.edges(node, data=True):
        if data.get("road_class") in ARTERIAL_CLASSES:
            road_name_edge = data.get("road_name")
            if road_name_edge is not None:
                names.add(road_name_edge)
    if names:
        return names

    # No direct arterial — check if this is an interior junction on the same
    # named road (all neighbours share the same road name as the segment being scored).
    if road_name is None:
        return names
    for _, neighbour, data in G.edges(node, data=True):
        if data.get("road_name") == road_name:
            # Follow to the far end of this same-name segment
            for _, _, far_data in G.edges(neighbour, data=True):
                if far_data.get("road_class") in ARTERIAL_CLASSES:
                    far_name = far_data.get("road_name")
                    if far_name is not None:
                        names.add(far_name)
    return names


def score_segment(seg_id: int, u, v, road_class: str, length_m: float, G: nx.Graph, seg_name: str = None) -> float:
    """Compute cut-through risk score for a single segment."""
    if road_class not in CUTTHROUGH_CANDIDATE_CLASSES:
        return 0.0

    # Dead ends have no cut-through risk
    if G.degree(u) <= 1 or G.degree(v) <= 1:
        return 0.0

    # Too long to be a useful shortcut
    if length_m > MAX_SHORTCUT_LENGTH_M:
        return 0.1

    # Which arterials does each end connect to?
    u_arterials = node_arterial_names(u, G, seg_name)
    v_arterials = node_arterial_names(v, G, seg_name)

    if u_arterials and v_arterials:
        # Both ends touch the same arterial — crescent/loop, not a shortcut
        if u_arterials & v_arterials:
            return 0.1
        # Connects two *different* arterials — genuine cut-through risk
        length_factor = max(0.0, 1.0 - (length_m / MAX_SHORTCUT_LENGTH_M))
        return 0.6 + (0.4 * length_factor)
    elif u_arterials or v_arterials:
        # One arterial connection — moderate risk
        return 0.3
    else:
        # Residential interior network — low risk
        return 0.1


def compute_and_save(conn):
    print("Building road network graph...")
    G, segment_lookup = build_graph(conn)

    print("Scoring cut-through risk for each segment...")
    scores = []
    for seg_id, (u, v, road_class, length_m, seg_name) in segment_lookup.items():
        if road_class not in CUTTHROUGH_CANDIDATE_CLASSES:
            # Arterials are attractors, not candidates — leave cutthrough_risk NULL
            # so the composite formula excludes this component for them entirely
            continue
        score = score_segment(seg_id, u, v, road_class, length_m, G, seg_name)
        scores.append((score, seg_id))

    cur = conn.cursor()

    # Ensure non-candidate roads (arterials) have NULL cutthrough_risk so the
    # composite formula excludes this component for them entirely.
    candidate_sql = ", ".join(f"'{c}'" for c in CUTTHROUGH_CANDIDATE_CLASSES)
    cur.execute(f"UPDATE road_segments SET cutthrough_risk = NULL WHERE road_class NOT IN ({candidate_sql})")

    execute_values(
        cur,
        "UPDATE road_segments SET cutthrough_risk = data.score FROM (VALUES %s) AS data(score, id) WHERE road_segments.id = data.id",
        scores,
    )
    conn.commit()
    cur.close()
    print(f"Cut-through risk scores saved for {len(scores)} segments")


def run():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        compute_and_save(conn)
    finally:
        conn.close()
    print("Cut-through scoring complete.")


if __name__ == "__main__":
    run()
