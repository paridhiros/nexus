# nexus/src/fastquery.py
from __future__ import annotations
from collections import defaultdict, deque
from typing import Dict, List, Tuple, Optional, Any

from .state import GraphIndex
from ._schemas import ClaimData  # adjust import path to match your project

class FastQueryEngine:
    """
    In-memory engine that preloads entities, relationships and claims.
    """

    def __init__(self, index: GraphIndex):
        # allow either: pass existing GraphIndex or path
        self.index = index

        # In-memory data structures
        # Map names -> canonical name (GraphIndex.resolve_alias already exists)
        # Map canonical name -> entity_id
        self.entity_name_to_id: Dict[str, int] = {}
        # Map entity_id -> canonical name
        self.entity_id_to_name: Dict[int, str] = {}

        # relationship_id -> relationship record
        self.relationships: Dict[int, Dict[str, Any]] = {}

        # adjacency: canonical_name -> list of (relationship_id, neighbor_name, strength, directed)
        self.adj: Dict[str, List[Tuple[int, str, float, bool]]] = defaultdict(list)

        # claim lookups
        self.claims_for_entity: Dict[int, List[ClaimData]] = defaultdict(list)
        self.claims_for_relationship: Dict[int, List[ClaimData]] = defaultdict(list)

        # Load everything
        self._load_all()

    def _load_all(self):
        # 1) Build entity name/id maps (pull from GraphIndex)
        for name in self.index.list_all_entities():
            # list_all_entities returns canonical names
            with self.index._conn() as con:
                row = con.execute("SELECT id FROM entities WHERE name = ?;", (name,)).fetchone()
                if row:
                    ent_id = row[0]
                    self.entity_name_to_id[name] = ent_id
                    self.entity_id_to_name[ent_id] = name

        # 2) Load relationships (use helper in GraphIndex)
        rel_rows = self.index.dump_all_relationships()
        for r in rel_rows:
            rel_id = int(r["relationship_id"])
            src = r["source_name"]
            tgt = r["target_name"]
            strength = float(r["strength"]) if r["strength"] is not None else 0.0
            directed = bool(r["directed"])

            self.relationships[rel_id] = {
                "relationship_id": rel_id,
                "source": src,
                "target": tgt,
                "strength": strength,
                "directed": directed
            }

            # adjacency: store rel_id so we can map claims later
            self.adj[src].append((rel_id, tgt, strength, directed))
            if not directed:
                # for undirected, also add reverse adjacency referencing same rel_id
                self.adj[tgt].append((rel_id, src, strength, directed))

        # 3) Load claims (use helper)
        claim_rows = self.index.dump_all_claims()
        for c in claim_rows:
            claim = ClaimData(
                content=c["content"],
                source=c["source"],
                date_added=c["date_added"]
            )
            ent_id = c["entity_id"]
            rel_id = c["relationship_id"]
            if ent_id is not None:
                self.claims_for_entity[int(ent_id)].append(claim)
            if rel_id is not None:
                self.claims_for_relationship[int(rel_id)].append(claim)

    # Convenience: get claims for entity name
    def get_entity_claims_by_name(self, entity_name: str) -> List[ClaimData]:
        canonical = self.index.resolve_alias(entity_name)
        ent_id = self.entity_name_to_id.get(canonical)
        if ent_id is None:
            return []
        return self.claims_for_entity.get(ent_id, [])

    # Convenience: get claims for relationship id
    def get_relationship_claims(self, relationship_id: int) -> List[ClaimData]:
        return self.claims_for_relationship.get(relationship_id, [])

    # neighbors returns structured items including relationship id and claims
    def neighbours(self, entity_name: str, depth: int = 1):
        canonical = self.index.resolve_alias(entity_name)
        if canonical not in self.adj and canonical not in self.entity_name_to_id:
            return []  # unknown node

        # Level 0: the node itself with node claims
        base_ent_id = self.entity_name_to_id.get(canonical)
        result = {0: [{
            "entity_name": canonical,
            "entity_id": base_ent_id,
            "relationship_id": None,
            "entity_claims": self.claims_for_entity.get(base_ent_id, []),
            "relationship_claims": []
        }]}

        visited = {canonical}
        frontier = [canonical]

        for d in range(1, depth + 1):
            next_frontier = []
            level_items = []
            for src in frontier:
                for (rel_id, nbr_name, strength, directed) in self.adj.get(src, []):
                    if nbr_name in visited:
                        continue
                    visited.add(nbr_name)
                    next_frontier.append(nbr_name)

                    nbr_ent_id = self.entity_name_to_id.get(nbr_name)
                    level_items.append({
                        "entity_name": nbr_name,
                        "entity_id": nbr_ent_id,
                        "relationship_id": rel_id,
                        "entity_claims": self.claims_for_entity.get(nbr_ent_id, []),
                        "relationship_claims": self.claims_for_relationship.get(rel_id, []),
                        "strength": strength,
                        "directed": directed
                    })
            if not level_items:
                break
            result[d] = level_items
            frontier = next_frontier

        return result

    # shortest_path uses relationship/adjacency as loaded (BFS)
    def shortest_path(self, src_name: str, tgt_name: str) -> Optional[List[str]]:
        src = self.index.resolve_alias(src_name)
        tgt = self.index.resolve_alias(tgt_name)
        if src == tgt:
            return [src]
        q = deque([src])
        parent = {src: None}
        while q:
            node = q.popleft()
            for (_, nbr, _, _) in self.adj.get(node, []):
                if nbr not in parent:
                    parent[nbr] = node
                    if nbr == tgt:
                        # reconstruct
                        path = [tgt]
                        cur = node
                        while cur is not None:
                            path.append(cur)
                            cur = parent[cur]
                        return list(reversed(path))
                    q.append(nbr)
        return None