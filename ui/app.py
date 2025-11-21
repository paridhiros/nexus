from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import sqlite3
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel

try:
    from ..src.state.graph_index import GraphIndex
    from ..src._schemas import (
        EntityNotFoundError,
        RelationshipCollisionError,
    )
except ImportError as exc:  # pragma: no cover - fail fast during import issues
    raise RuntimeError(
        "Unable to import GraphIndex. Ensure the cortex package is installed and "
        "PYTHONPATH includes the repository root."
    ) from exc


LOGGER = logging.getLogger("nexus-ui")
if not LOGGER.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    LOGGER.addHandler(handler)
LOGGER.setLevel(logging.INFO)
LOGGER.propagate = False

GRAPH_INDEX_ENV_VAR = "GRAPH_INDEX_PATH"
FRONTEND_DIR = Path(__file__).resolve().parent


class GraphMeta(BaseModel):
    generated_at: str
    node_count: int
    edge_count: int


class GraphNode(BaseModel):
    id: str
    label: str
    entity_type: Optional[str] = None
    claim_count: Optional[int] = None


class GraphEdge(BaseModel):
    id: str
    source: str
    target: str
    strength: Optional[float] = None


class GraphSnapshot(BaseModel):
    meta: GraphMeta
    nodes: List[GraphNode]
    edges: List[GraphEdge]
    adjacency: Dict[str, List[str]]


class Claim(BaseModel):
    content: str
    source: Optional[str] = None
    date_added: Optional[str] = None
    claim_date: Optional[str] = None


class EntityResponse(BaseModel):
    canonical: str
    aliases: List[str]
    claims: List[Claim]
    related_entities: List[str]


class EdgeResponse(BaseModel):
    source: str
    target: str
    claims: List[Claim]


app = FastAPI(
    title="nexus",
    version="0.1.0",
    description="Read-only graph viewer backed by GraphIndex.",
)
app.state.graph_index = None
app.state.graph_index_path = None


def _resolve_index_path() -> Path:
    configured = os.getenv(GRAPH_INDEX_ENV_VAR)
    if not configured:
        raise RuntimeError(
            f"{GRAPH_INDEX_ENV_VAR} is not set. "
            "Set it to the absolute path of your graph.sqlite (see README)."
        )
    resolved = Path(configured).expanduser().resolve()
    if not resolved.exists():
        raise RuntimeError(
            f"Graph index file not found at {resolved}. "
            "Double-check the GRAPH_INDEX_PATH setting."
        )
    if not resolved.is_file():
        raise RuntimeError(f"{resolved} exists but is not a file.")
    return resolved


def _get_graph_index(request: Request) -> GraphIndex:
    index: Optional[GraphIndex] = request.app.state.graph_index
    if index is None:
        raise HTTPException(
            status_code=503,
            detail="Graph index not initialised. Check server configuration.",
        )
    return index


def _fetch_entity_types(index_path: Path) -> Dict[str, Optional[str]]:
    query = "SELECT name, entity_type FROM entities"
    results: Dict[str, Optional[str]] = {}
    with sqlite3.connect(f"file:{index_path}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        for row in conn.execute(query):
            results[row["name"]] = row["entity_type"]
    return results


def _build_graph_snapshot(index: GraphIndex, index_path: Path) -> GraphSnapshot:
    entities = index.list_all_entities()
    entity_types = _fetch_entity_types(index_path)

    canonicals = set()
    for name in entities:
        resolved = index.resolve_alias(name)
        canonicals.add(resolved)
    
    filtered_entities = [name for name in entities if index.resolve_alias(name) == name]

    nodes: List[GraphNode] = []
    adjacency: Dict[str, set[str]] = {name: set() for name in filtered_entities}
    seen_edges: Dict[tuple[str, str], GraphEdge] = {}

    for name in filtered_entities:
        try:
            claims = index.load_entity_claims(name)
        except EntityNotFoundError:
            claims = []
        claim_count = len(claims)

        node = GraphNode(
            id=name,
            label=name,
            entity_type=entity_types.get(name),
            claim_count=claim_count,
        )
        nodes.append(node)

        try:
            relationships = index.load_relationships(name, directed=None)
        except EntityNotFoundError:
            relationships = []

        for rel in relationships:
            src = index.resolve_alias(rel.source_name)
            tgt = index.resolve_alias(rel.target_name)
            if src == tgt:
                continue
            a, b = sorted([src, tgt])
            key = (a, b)
            adjacency.setdefault(a, set()).add(b)
            adjacency.setdefault(b, set()).add(a)
            if key in seen_edges:
                continue

            edge = GraphEdge(
                id=f"{a}||{b}",
                source=a,
                target=b,
                strength=rel.strength if rel.strength is not None else None,
            )
            seen_edges[key] = edge

    sorted_nodes = sorted(nodes, key=lambda n: n.id.lower())
    sorted_edges = sorted(seen_edges.values(), key=lambda e: (e.source.lower(), e.target.lower()))
    adjacency_lists = {
        node: sorted(neighbors, key=str.lower) for node, neighbors in adjacency.items()
    }
    meta = GraphMeta(
        generated_at=datetime.now(timezone.utc).isoformat(),
        node_count=len(sorted_nodes),
        edge_count=len(sorted_edges),
    )
    return GraphSnapshot(
        meta=meta,
        nodes=sorted_nodes,
        edges=sorted_edges,
        adjacency=adjacency_lists,
    )


def _sort_claims(claims: List[Claim]) -> List[Claim]:
    dated = [claim for claim in claims if claim.claim_date]
    undated = [claim for claim in claims if not claim.claim_date]
    dated.sort(key=lambda c: c.claim_date, reverse=True)
    return dated + undated


@app.on_event("startup")
def startup_event() -> None:
    index_path = _resolve_index_path()
    LOGGER.info("Resolved graph index path: %s", index_path)
    app.state.graph_index_path = index_path
    app.state.graph_index = GraphIndex(index_path)
    LOGGER.info("GraphIndex initialised successfully.")


@app.get("/", response_class=FileResponse)
def serve_index() -> FileResponse:
    index_file = FRONTEND_DIR / "index.html"
    if not index_file.exists():
        raise HTTPException(status_code=500, detail="index.html not found.")
    return FileResponse(index_file)


@app.get("/app.js?v=0.1", response_class=FileResponse)
def serve_app_js() -> FileResponse:
    asset = FRONTEND_DIR / "app.js"
    if not asset.exists():
        raise HTTPException(status_code=404, detail="app.js not found.")
    return FileResponse(asset, media_type="application/javascript", headers={"Cache-Control": "no-store"})


@app.get("/styles.css", response_class=FileResponse)
def serve_styles() -> FileResponse:
    asset = FRONTEND_DIR / "styles.css"
    if not asset.exists():
        raise HTTPException(status_code=404, detail="styles.css not found.")
    return FileResponse(asset, media_type="text/css")


@app.get("/favicon.svg", response_class=FileResponse)
def serve_favicon() -> FileResponse:
    asset = FRONTEND_DIR / "favicon.svg"
    if not asset.exists():
        raise HTTPException(status_code=404, detail="favicon.svg not found.")
    return FileResponse(asset, media_type="image/x-icon", headers={"Cache-Control": "no-store"})


@app.get("/api/graph/snapshot", response_model=GraphSnapshot)
def get_graph_snapshot(request: Request) -> GraphSnapshot:
    index = _get_graph_index(request)
    index_path: Path = request.app.state.graph_index_path
    return _build_graph_snapshot(index, index_path)


@app.get("/api/entity/{name}", response_model=EntityResponse)
def get_entity(name: str, request: Request) -> EntityResponse:
    index = _get_graph_index(request)
    canonical = index.resolve_alias(name)
    try:
        claims_data = index.load_entity_claims(canonical)
    except EntityNotFoundError:
        raise HTTPException(status_code=404, detail=f"Entity '{name}' not found.")

    claims = [
        Claim(content=record.content, source=record.source, date_added=record.date_added, claim_date=record.claim_date)
        for record in claims_data
    ]
    claims = _sort_claims(claims)
    aliases = sorted(index.load_aliases(canonical))

    try:
        relationships = index.load_relationships(canonical, directed=None)
    except EntityNotFoundError:
        relationships = []

    related: set[str] = set()
    for rel in relationships:
        src = index.resolve_alias(rel.source_name)
        tgt = index.resolve_alias(rel.target_name)
        if src == canonical and tgt != canonical:
            related.add(tgt)
        elif tgt == canonical and src != canonical:
            related.add(src)
        elif src != canonical and tgt != canonical:  # fallback when canonical not first
            if canonical in (src, tgt):
                related.add(tgt if canonical == src else src)

    return EntityResponse(
        canonical=canonical,
        aliases=aliases,
        claims=claims,
        related_entities=sorted(related, key=str.lower),
    )


@app.get("/api/edge", response_model=EdgeResponse)
def get_edge(
    request: Request,
    src: str = Query(..., description="Source entity name or alias"),
    tgt: str = Query(..., description="Target entity name or alias"),
) -> EdgeResponse:
    if not src or not tgt:
        raise HTTPException(status_code=400, detail="Both src and tgt parameters are required.")

    index = _get_graph_index(request)
    canonical_src = index.resolve_alias(src)
    canonical_tgt = index.resolve_alias(tgt)

    if canonical_src == canonical_tgt:
        raise HTTPException(
            status_code=400,
            detail="Source and target refer to the same canonical entity.",
        )

    # Ensure both entities exist before loading claims.
    for entity_name in (canonical_src, canonical_tgt):
        if not index.entity_exists(entity_name):
            raise HTTPException(status_code=404, detail=f"Entity '{entity_name}' not found.")

    try:
        claims_data = index.load_relationship_claims(
            canonical_src,
            canonical_tgt,
            directed=False,
        )
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=404, detail=f"Entity '{exc}' not found.") from exc
    except RelationshipCollisionError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    claims = [
        Claim(content=record.content, source=record.source, date_added=record.date_added, claim_date=record.claim_date)
        for record in claims_data
    ]
    claims = _sort_claims(claims)

    return EdgeResponse(source=canonical_src, target=canonical_tgt, claims=claims)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the knowledge graph viewer backend.")
    parser.add_argument(
        "--graph-index-path",
        help="Absolute path to graph.sqlite. Overrides GRAPH_INDEX_PATH env var.",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8099)
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload.")
    args = parser.parse_args()

    if args.graph_index_path:
        os.environ[GRAPH_INDEX_ENV_VAR] = args.graph_index_path

    import uvicorn

    uvicorn.run(
        "nexus.ui.app:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        factory=False,
    )


if __name__ == "__main__":  # pragma: no cover - script entrypoint
    main()
