"""
Graph domain primitives: Entity and Relationship classes.

- for graph infrastructure, see state/ (GraphIndex)
- for graph construction, see build.py (GraphBuilder)
- for graph querying, see query.py (GraphQueryEngine)
"""

from __future__ import annotations

from typing import Optional, TYPE_CHECKING

from ._schemas import (
    ClaimData,
    RelationshipRecord,
    RelationshipCollisionError,
    RelationshipNotFoundError,
    EntityNotFoundError
)

if TYPE_CHECKING:
    from .state import GraphIndex


class Relationship:
    """Lazy-loading relationship representation"""
    def __init__(self, src: str, tgt: str, strength: Optional[float], state: "GraphIndex", directed: bool = False):
        """
        Load relationship from DB. Raises RelationshipNotFoundError if doesn't exist.
        
        Args:
            src: Source entity name (can be alias).
            tgt: Target entity name (can be alias).
            directed: True => require directed src→tgt;
                      False => require undirected {src, tgt} (either DB order).
            state: GraphIndex instance.
        """
        self._state = state 
        self.input_source_name = src
        self.input_target_name = tgt

        self.canonical_source_name = self._state.resolve_alias(src)
        self.canonical_target_name = self._state.resolve_alias(tgt)
        if self.canonical_source_name == self.canonical_target_name:
            raise RelationshipCollisionError(src, tgt)
        
        found = None

        if directed:
            # exact directed match: src->tgt only
            for r in self._state.load_relationships(self.canonical_source_name, min_strength=None, directed=True):
                if r.directed and r.source_name == self.canonical_source_name and r.target_name == self.canonical_target_name:
                    found = r
                    break
        else:
            # undirected match: accept either DB order without reordering the object
            # (search from both ends to avoid indexing bias)
            seen = set()
            for seed in (self.canonical_source_name, self.canonical_target_name):
                for r in self._state.load_relationships(seed, min_strength=None, directed=False):
                    key = (r.source_name, r.target_name)
                    if key in seen:
                        continue
                    seen.add(key)
                    if not r.directed and {
                        self._state.resolve_alias(r.source_name),
                        self._state.resolve_alias(r.target_name)
                    } == {
                        self.canonical_source_name,
                        self.canonical_target_name
                    }:
                        found = r
                        break
                if found:
                    break
        if found is None:
            raise RelationshipNotFoundError(src, tgt, directed)

        self.directed = found.directed
        self.strength = found.strength

        self._source_entity = None
        self._target_entity = None
        self._claims = None


    @property
    def source(self) -> Entity:
        if self._source_entity is None:
            self._source_entity = Entity(self.canonical_source_name, self._state)
        return self._source_entity


    @property
    def target(self) -> Entity:
        if self._target_entity is None:
            self._target_entity = Entity(self.canonical_target_name, self._state)
        return self._target_entity


    @property
    def claims(self) -> list[ClaimData]:
        """
        NOTE | TODO: We may want to visit turning this into an explicit function;
        as claims grow we can filter by source, adding support
        into load_relationship_claims by sources.
        For now, this is an orchestrator responsibility.
        """
        if self._claims is None:
            self._claims = self._state.load_relationship_claims(
                self.canonical_source_name, 
                self.canonical_target_name,
                self.directed
            )
        return self._claims


    def __eq__(self, other):
        if self.directed:
            return (
                self.canonical_source_name == other.canonical_source_name and
                self.canonical_target_name == other.canonical_target_name
            )
        else:
            return (
                {self.canonical_source_name, self.canonical_target_name} == 
                {other.canonical_source_name, other.canonical_target_name}
            )


    def __hash__(self):
        if self.directed:
            return hash((self.canonical_source_name, self.canonical_target_name))
        return hash(frozenset([self.canonical_source_name, self.canonical_target_name]))


class Entity:
    """Lazy-loading entity representation"""
    def __init__(self, name: str, state: "GraphIndex"):
        """
        Load entity from DB. Raises EntityNotFoundError if doesn't exist.
        
        Args:
            name: Entity name (can be alias, will be resolved)
            state: GraphIndex instance
        """
        self._state = state # graph index

        self.input_name = name # user entry
        self.canonical_name = self._state.resolve_alias(name)

        if not self._state.entity_exists(self.canonical_name):
            raise EntityNotFoundError(name)
        
        self.name = self.canonical_name

        self._aliases = None
        self._claims = None


    @property
    def aliases(self) -> list[str]:
        if self._aliases is None:
            self._aliases = self._state.load_aliases(self.canonical_name)
        return self._aliases


    @property
    def claims(self) -> list[ClaimData]:
        """
        NOTE | TODO: We may want to visit turning this into an explicit function;
        as claims grow we can filter by source, adding support
        into load_entity_claims by sources.
        For now, this is an orchestrator responsibility.
        """
        if self._claims is None:
            self._claims = self._state.load_entity_claims(self.canonical_name)
        return self._claims


    def get_relationships(self, min_strength: Optional[float], directed: Optional[bool]) -> list[Relationship]:
        rels: list[RelationshipRecord] = self._state.load_relationships(
            self.canonical_name, min_strength, directed
        )
        return [
            Relationship(r.source_name, r.target_name, r.strength, self._state, r.directed)
            for r in rels
        ]


    def __eq__(self, other):
        return self.canonical_name == other.canonical_name


    def __hash__(self):
        return hash(self.canonical_name)
