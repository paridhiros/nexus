from __future__ import annotations
from typing import Dict, List, Tuple, Optional, Set
from .state import GraphIndex

class FastQueryEngine:
    """
    High-performance, in-memory graph engine.
    """

    def __init__(self, index):
        self.index = index  # GraphIndex object
        self.adj: Dict[str, List[Tuple[str, float]]] = {}  # node -> list[(neighbor, weight)]
        self._load_graph()

    # ---------------------------------------------------------
    # LOAD GRAPH INTO MEMORY
    # ---------------------------------------------------------
    def _load_graph(self):
        """
        Loads ALL relationships from SQLite (GraphIndex)
        and builds adjacency list.
        """

        rows = self.index.dump_all_relationships()  # we will create this helper

        adj: Dict[str, List[Tuple[str, float]]] = {}

        for r in rows:
            src = r["src"]
            tgt = r["tgt"]
            w   = r["strength"]
            d   = bool(r["directed"])

            adj.setdefault(src, [])
            adj.setdefault(tgt, [])

            adj[src].append((tgt, w))

            if not d:
                adj[tgt].append((src, w))

        self.adj = adj

    # ---------------------------------------------------------
    # BASIC OPS
    # ---------------------------------------------------------
    def neighbors(self, node: str) -> List[Tuple[str, float]]:
        return self.adj.get(node, [])

    def has_node(self, node: str) -> bool:
        return node in self.adj

    # ---------------------------------------------------------
    # BFS
    # ---------------------------------------------------------
    def bfs(self, start: str, depth: int = 1) -> Set[str]:
        """
        Return set of nodes reachable within `depth`.
        """

        if start not in self.adj:
            return set()

        visited = {start}
        frontier = {start}

        for _ in range(depth):
            next_frontier = set()
            for n in frontier:
                for nbr, _ in self.adj.get(n, []):
                    if nbr not in visited:
                        visited.add(nbr)
                        next_frontier.add(nbr)
            frontier = next_frontier
            if not frontier:
                break

        visited.remove(start)
        return visited

    # ---------------------------------------------------------
    # DFS (optional)
    # ---------------------------------------------------------
    def dfs(self, start: str, max_depth: int = 5):
        """
        Returns list of nodes reachable by DFS (depth-limited).
        """
        result = []
        visited = set()

        def _dfs(node: str, d: int):
            if d > max_depth or node in visited:
                return
            visited.add(node)
            result.append(node)
            for nbr, _ in self.adj.get(node, []):
                _dfs(nbr, d + 1)

        _dfs(start, 0)
        result.remove(start)
        return result

    # ---------------------------------------------------------
    # MULTI-HOP RETRIEVAL WITH WEIGHTS
    # ---------------------------------------------------------
    def walk(self, start: str, depth: int = 3) -> List[Tuple[str, float]]:
        """
        Returns all reachable nodes, with cumulative path score.
        Score = sum(weight) or you can make it multiplicative.
        """

        results: Dict[str, float] = {}
        frontier = [(start, 0.0)]

        for _ in range(depth):
            new_frontier = []
            for node, score in frontier:
                for nbr, w in self.adj.get(node, []):
                    new_score = score + (w or 0.0)
                    if nbr not in results or new_score > results[nbr]:
                        results[nbr] = new_score
                        new_frontier.append((nbr, new_score))
            frontier = new_frontier

        if start in results:
            del results[start]

        return sorted(results.items(), key=lambda x: -x[1])
    
    # ------------------------------
    # CLAIM FETCHING
    # ------------------------------

    def claims_for_entity(self, entity: str):
        return self.index.fetch_claims_for_entity(entity)

    def claims_between(self, src: str, tgt: str):
        return self.index.fetch_claims_between(src, tgt)