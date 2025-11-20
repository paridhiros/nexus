from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import sqlite3
from datetime import datetime, timezone
from typing import Optional, Literal

from ...config import log
from .._schemas import (
    RelationshipRecord,
    ClaimData,
    AliasConflictError,
    EntityNotFoundError,
    RelationshipCollisionError,
    RelationshipMergeConflict,
    DeletionConflict
)


def debug_only(func):
    """Marks a function as debug/internal use only"""
    func.__debug_only__ = True
    return func


class GraphIndex:
    """Graph Index handler"""

    def __init__(self, index_path: str | Path):
        self.index_path = Path(index_path)
        self._initialize()


    @contextmanager
    def _conn(self):
        """
        Helper to open a SQLite connection with row access by column name.
        """
        con = sqlite3.connect(self.index_path)
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA foreign_keys=ON;")
        con.execute("PRAGMA busy_timeout=5000;")
        con.row_factory = sqlite3.Row
        try:
            yield con
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()


    def _initialize(self) -> None:
        with self._conn() as con:
            con.execute("""
                CREATE TABLE IF NOT EXISTS entities (
                    id INTEGER PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    date_added TIMESTAMP DEFAULT NULL,
                    entity_type TEXT,
                    tags TEXT
                );
            """)
            con.execute("""
                CREATE TABLE IF NOT EXISTS relationships (
                    id INTEGER PRIMARY KEY,
                    source_id INTEGER NOT NULL,
                    target_id INTEGER NOT NULL,
                    strength REAL,
                    directed INTEGER NOT NULL DEFAULT 0 CHECK (directed IN (0,1)),
                    date_added TIMESTAMP DEFAULT NULL,
                    tags TEXT,
                    UNIQUE(source_id, target_id, directed),
                    FOREIGN KEY(source_id) REFERENCES entities(id),
                    FOREIGN KEY(target_id) REFERENCES entities(id)
                );
            """)
            con.execute("""
                CREATE TABLE IF NOT EXISTS aliases (
                    id INTEGER PRIMARY KEY,
                    entity_id INTEGER NOT NULL,
                    alias TEXT UNIQUE NOT NULL,
                    date_added TIMESTAMP DEFAULT NULL,
                    FOREIGN KEY(entity_id) REFERENCES entities(id)
                );
            """)
            con.execute("""
                CREATE TABLE IF NOT EXISTS claims (
                    id INTEGER PRIMARY KEY,
                    entity_id INTEGER,
                    relationship_id INTEGER,
                    content TEXT NOT NULL,
                    source TEXT,
                    date_added TIMESTAMP DEFAULT NULL,
                    claim_date TEXT,
                    tags TEXT,
                    CHECK ((entity_id IS NULL) <> (relationship_id IS NULL)),
                    FOREIGN KEY(entity_id) REFERENCES entities(id),
                    FOREIGN KEY(relationship_id) REFERENCES relationships(id)
                );
            """)

            # indexes
            con.execute("CREATE INDEX IF NOT EXISTS idx_rel_source ON relationships(source_id);")
            con.execute("CREATE INDEX IF NOT EXISTS idx_rel_target ON relationships(target_id);")
            con.execute("CREATE INDEX IF NOT EXISTS idx_claims_entity ON claims(entity_id);")
            con.execute("CREATE INDEX IF NOT EXISTS idx_claims_relationship ON claims(relationship_id);")



    def upsert_entity(self, name: str, entity_type: Optional[str]=None) -> int:
        """Insert or update an entity by name, returning its id."""
        with self._conn() as con:
            cur = con.execute("""
                INSERT INTO entities (name, entity_type, date_added)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(name)
                DO UPDATE SET entity_type = COALESCE(excluded.entity_type, entities.entity_type)
                RETURNING id;
            """, (name, entity_type))
            return cur.fetchone()[0]


    def upsert_relationship(self,
        source_name,
        target_name,
        strength: float = 0.0,
        directed: bool = False
    ) -> int:
        """
        Insert or update a relationship between two entities by name.
        
        Undirected normalization: for directed=False, store the pair in
        ordered form (min_id, max_id, 0). This avoids duplicate undirected rows.
        """
        source_canonical = self.resolve_alias(source_name)
        target_canonical = self.resolve_alias(target_name)
        
        if source_canonical == target_canonical:
            raise RelationshipCollisionError(source_name, target_name)
    
        source_id = self.upsert_entity(source_canonical)
        target_id = self.upsert_entity(target_canonical)

        # normalize for undirected relationships
        source_id, target_id, directed = self._normalize_pair(source_id, target_id, directed)

        with self._conn() as con:
            cur = con.execute("""
                INSERT INTO relationships (source_id, target_id, strength, directed, date_added)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(source_id, target_id, directed)
                DO UPDATE SET strength = excluded.strength
                RETURNING id;
            """, (source_id, target_id, strength, int(bool(directed))))
            return cur.fetchone()[0]


    def upsert_alias(self, entity_name: str, alias: str) -> int:
        """
        Associate an alias with an entity by name.

        NOTE: aliases that already exist as entities **are** allowed:
        - They will be consolidated virtually during query time (i.e. load_X)
        - They can be consolidated physically with merge_alias()
        """
        if self._has_relationship_between(entity_name, alias):
            raise RelationshipCollisionError(
                entity_name, alias,
                message=f"Cannot alias: relationship exists between '{entity_name}' and '{alias}'"
            )
        if entity_name == alias:
            raise AliasConflictError(
                entity_name, alias, alias,
                message=f"Cannot self-alias '{entity_name}' to '{alias}'."
            )
        canonical = self.resolve_alias(entity_name)
        if entity_name != canonical:
            msg = (
                f"Cannot set an alias of '{entity_name}' because "
                f"'{entity_name}' is itself an alias of '{canonical}'. "
                f"Instead, set '{alias}' as an alias of '{canonical}'."
            )
            raise AliasConflictError(entity_name, canonical, alias, message=msg)
        
        entity_id = self.upsert_entity(entity_name)
        
        with self._conn() as con:
            # check: don't upsert an alias that belongs to another entity
            alias_is_existing_alias = con.execute(
                "SELECT entity_id FROM aliases WHERE alias = ?;", 
                (alias,)
            ).fetchone() # check if alias already exists
            if alias_is_existing_alias and alias_is_existing_alias[0] != entity_id:
                existing_entity = con.execute(
                    "SELECT name FROM entities WHERE id = ?;", 
                    (alias_is_existing_alias[0],)
                ).fetchone()[0] # get entity names for error message
                raise AliasConflictError(alias, existing_entity, entity_name)

            con.execute("""
                INSERT INTO aliases (entity_id, alias, date_added)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(alias) DO NOTHING;
            """, (entity_id, alias))
            
            cur = con.execute("SELECT id FROM aliases WHERE alias = ?;", (alias,))
            return cur.fetchone()[0]


    def upsert_claim(self,
        content: str,
        source: Optional[str],
        entity_name: Optional[str] = None,
        relationship: Optional[RelationshipRecord] = None,
        claim_date: Optional[str] = None,
    ) -> int:
        """Insert a claim associated with either an entity or a relationship."""
        if entity_name and relationship:
            raise ValueError("Claim cannot be associated with both entity and relationship")
        if not entity_name and not relationship:
            raise ValueError("Claim must be associated with either entity or relationship")

        entity_id = None
        relationship_id = None
        
        if entity_name:
            entity_id = self.upsert_entity(entity_name)
        elif relationship:
            relationship_id = self.upsert_relationship(
                relationship.source_name,
                relationship.target_name,
                relationship.strength,
                relationship.directed
            )
        
        if claim_date is None:
            claim_date_iso8601 = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        else: # | TODO: kinder parsing
            try:
                dt = datetime.fromisoformat(claim_date)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                claim_date_iso8601 = dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception: # fallback to now
                claim_date_iso8601 = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        
        with self._conn() as con:
            cur = con.execute("""
                INSERT INTO claims (entity_id, relationship_id, content, source, claim_date, date_added)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                RETURNING id;
            """, (entity_id, relationship_id, content, source, claim_date_iso8601))
            return cur.fetchone()[0]


    def drop(self):
        """drop all data from all tables."""
        with self._conn() as con:
            con.execute("DELETE FROM claims;")
            con.execute("DELETE FROM relationships;")
            con.execute("DELETE FROM aliases;")
            con.execute("DELETE FROM entities;")


    def load_aliases(self, name: str) -> list[str]:
        """Load all aliases for an entity (resolve to canonical first)"""
        canonical = self.resolve_alias(name)

        with self._conn() as con:
            entity_row = con.execute(
                "SELECT id FROM entities WHERE name = ?;",
                (canonical,)
            ).fetchone()
            
            if not entity_row:
                raise EntityNotFoundError(canonical)
            
            entity_id = entity_row[0]
            
            rows = con.execute(
                "SELECT alias FROM aliases WHERE entity_id = ?;",
                (entity_id,)
            ).fetchall()
            
            return [row[0] for row in rows]
        

    def resolve_alias(self, name: str) -> str:
        """
        Return the canonical name of an entity.
        NOTE: may want to be more explicit about entity-DNE.
        """
        with self._conn() as con:
            alias_row = con.execute(
                "SELECT entity_id FROM aliases WHERE alias = ?;",
                (name,)
            ).fetchone() # check if name is an alias
            
            if alias_row: # -> get canonical entity name
                entity_row = con.execute(
                    "SELECT name FROM entities WHERE id = ?;",
                    (alias_row[0],)
                ).fetchone()
                return entity_row[0]
            
            entity_row = con.execute(
                "SELECT name FROM entities WHERE name = ?;",
                (name,)
            ).fetchone() # if not alias, check if it's an entity name
            
            if entity_row:
                return entity_row[0]
        
            return name # not an alias, not yet an entity
    

    def load_entity_claims(self, name: str) -> list[ClaimData]:
        """Load claims for entity and all its aliases."""
        canonical = self.resolve_alias(name)

        with self._conn() as con:
            entity_row = con.execute(
                "SELECT id FROM entities WHERE name = ?;",
                (canonical,)
            ).fetchone()
            
            if not entity_row:
                raise EntityNotFoundError(canonical)
            
            canonical_id = entity_row[0]
            
            alias_rows = con.execute(
                "SELECT alias FROM aliases WHERE entity_id = ?;",
                (canonical_id,)
            ).fetchall() # get all alias names for canonical entity
            
            alias_names = [row[0] for row in alias_rows]
            
            # get entity IDs for all names (canonical + aliases that are also entities)
            all_names = [canonical] + alias_names
            placeholders = ','.join('?' * len(all_names))
            
            entity_ids = con.execute(
                f"SELECT id FROM entities WHERE name IN ({placeholders});",
                all_names
            ).fetchall()
            
            entity_id_list = [row[0] for row in entity_ids]
            
            if not entity_id_list:
                return []
            
            # load claims from all these entity IDs
            id_placeholders = ','.join('?' * len(entity_id_list))
            rows = con.execute(f"""
                SELECT content, source, date_added 
                FROM claims 
                WHERE entity_id IN ({id_placeholders});
            """, entity_id_list).fetchall()
            
            return [
                ClaimData(
                    content=row["content"],
                    source=row["source"],
                    date_added=row["date_added"]
                )
                for row in rows
            ]
    

    def load_relationships(self,
        name,
        min_strength: Optional[float] = None,
        directed: Optional[bool] = None
    ) -> list[RelationshipRecord]:
        """Load relationships for an entity and all its aliases."""
        canonical = self.resolve_alias(name)
        with self._conn() as con:
            canonical_row = con.execute(
                "SELECT id FROM entities WHERE name = ?;",
                (canonical,)
            ).fetchone()
            
            if not canonical_row:
                raise EntityNotFoundError(canonical)
            canonical_id = canonical_row[0]

            # collect canonical + alias-entity ids
            entity_ids = self._expand_ids(con, canonical)
            ps = ",".join("?" * len(entity_ids))
            params: list = entity_ids + entity_ids

            where = [f"(r.source_id IN ({ps}) OR r.target_id IN ({ps}))"]
            if min_strength is not None:
                where.append("r.strength >= ?")
                params.append(min_strength)
            if directed is not None:
                where.append("r.directed = ?")
                params.append(int(bool(directed)))

            # prefer rows where the canonical participates for deterministic dedupe
            order = (
                "ORDER BY CASE WHEN r.source_id = ? OR r.target_id = ? THEN 0 ELSE 1 END, "
                "r.directed DESC, r.id"
            )
            params.extend([canonical_id, canonical_id])

            sql = f"""
                SELECT e1.name AS source, e2.name AS target, r.strength, r.source_id, r.target_id, r.directed
                FROM relationships r
                JOIN entities e1 ON r.source_id = e1.id
                JOIN entities e2 ON r.target_id = e2.id
                WHERE {' AND '.join(where)}
                {order};
            """
            rows = con.execute(sql, params).fetchall()

            seen: dict[tuple[int, int, int], sqlite3.Row] = {}
            for row in rows:
                s_id = row["source_id"]
                t_id = row["target_id"]
                d_int = int(row["directed"])
                if d_int == 0:
                    key = (min(s_id, t_id), max(s_id, t_id), 0)
                else:
                    key = (s_id, t_id, 1)
                if key not in seen:
                    seen[key] = row  # first row wins due to ORDER BY

            return [
                RelationshipRecord(
                    source_name=r["source"],
                    target_name=r["target"],
                    strength=r["strength"],
                    directed=bool(r["directed"]),
                )
                for r in seen.values()
            ]


    def load_relationship_claims(self,
        source_name: str,
        target_name: str,
        directed: Optional[bool] = None
    ) -> list[ClaimData]:
        """Load claims for a rel between src and tgt (includes all aliases)"""
        source_canonical = self.resolve_alias(source_name)
        target_canonical = self.resolve_alias(target_name)
        if source_canonical == target_canonical:
            raise RelationshipCollisionError(source_name, target_name)
        
        with self._conn() as con:
            # gather relationship ids per directed setting
            rel_ids: list[int] = []
            if directed is None:
                # both undirected and both directed orientations
                rel_ids.extend(self._relationship_ids_alias_expanded(con, source_canonical, target_canonical, False))
                rel_ids.extend(self._relationship_ids_alias_expanded(con, source_canonical, target_canonical, True))
                rel_ids.extend(self._relationship_ids_alias_expanded(con, target_canonical, source_canonical, True))
            elif directed is True:
                rel_ids.extend(self._relationship_ids_alias_expanded(con, source_canonical, target_canonical, True))
            else:
                rel_ids.extend(self._relationship_ids_alias_expanded(con, source_canonical, target_canonical, False))

            if not rel_ids:
                return []
            
            placeholders = ",".join("?" * len(rel_ids))
            rows = con.execute(f"""
                SELECT content, source, date_added
                FROM claims
                WHERE relationship_id IN ({placeholders});
            """, rel_ids).fetchall()

            return [
                ClaimData(
                    content=row["content"],
                    source=row["source"],
                    date_added=row["date_added"]
                )
                for row in rows
            ]


    def merge_alias(self, canonical_name: str, alias_name: str):
        """
        Physically merge an alias entity into its canonical entity.
    
        Merge rules:
        - If canonical has relationship to X: keep canonical's strength
        - If only alias has relationship to X: inherit alias's strength
        - Claims from both are consolidated
        - Alias entity is deleted (alias mapping remains)
        """
        with self._conn() as con:
            canonical_row = con.execute(
                "SELECT id FROM entities WHERE name = ?;", (canonical_name,)
            ).fetchone()
            if not canonical_row:
                is_an_alias_of = self.resolve_alias(canonical_name)
                if is_an_alias_of and is_an_alias_of != canonical_name:
                    raise EntityNotFoundError(
                        canonical_name,
                        message=(
                            f"Entity '{canonical_name}' not found in graph. "
                            f"It looks like you passed in an alias of {is_an_alias_of}."
                        )
                    )
                else:
                    raise EntityNotFoundError(canonical_name)
            canonical_id = canonical_row[0]
            
            # verify alias_name is actually an alias of canonical_name
            alias_mapping = con.execute(
                "SELECT entity_id FROM aliases WHERE alias = ?;", (alias_name,)
            ).fetchone()
            if not alias_mapping:
                raise ValueError(f"'{alias_name}' is not an alias")
            if alias_mapping[0] != canonical_id:
                raise ValueError(f"'{alias_name}' is not an alias of '{canonical_name}'")
            
            # get alias entity id (if it exists as an entity)
            alias_row = con.execute(
                "SELECT id FROM entities WHERE name = ?;", (alias_name,)
            ).fetchone()
            if not alias_row: # if alias doesn't exist as entity, nothing to merge
                log.info("%s has no entity data to merge", alias_name)
                return
            alias_id = alias_row[0]
            
            # migrate relationships from alias to canonical (normalize undirected)
            alias_relationships = con.execute("""
                SELECT id, source_id, target_id, strength, directed
                FROM relationships
                WHERE source_id = ? OR target_id = ?;
            """, (alias_id, alias_id)).fetchall()
            
            for rel in alias_relationships:
                rel_id, source_id, target_id, strength, directed = rel
                
                # update ids: replace alias_id with canonical_id
                new_source_id = canonical_id if source_id == alias_id else source_id
                new_target_id = canonical_id if target_id == alias_id else target_id
                
                # self-loop sanity check
                if new_source_id == new_target_id:
                    raise RelationshipMergeConflict(canonical_name, alias_name)
                
                new_source_id, new_target_id, directed_int = self._normalize_pair(
                    new_source_id, new_target_id, directed
                )

                # check if canonical already has relationship to this target
                existing_rel = con.execute("""
                    SELECT id FROM relationships
                    WHERE source_id = ? AND target_id = ? AND directed = ?;
                """, (new_source_id, new_target_id, directed_int)).fetchone()
                
                if existing_rel:
                    # move claims to existing; keep existing strength
                    con.execute("""
                        UPDATE claims
                        SET relationship_id = ?
                        WHERE relationship_id = ?;
                    """, (existing_rel[0], rel_id))
                    
                    # delete alias relationship
                    con.execute("DELETE FROM relationships WHERE id = ?;", (rel_id,))
                else:
                    # move relationship from alias to canonical
                    con.execute("""
                        UPDATE relationships
                        SET source_id = ?, target_id = ?, directed = ?
                        WHERE id = ?;
                    """, (new_source_id, new_target_id, directed_int, rel_id))
            
            # migrate entity claims from alias to canonical
            con.execute("""
                UPDATE claims
                SET entity_id = ?
                WHERE entity_id = ?;
            """, (canonical_id, alias_id))
            
            # delete alias entity (alias mapping remains in aliases table!)
            con.execute("DELETE FROM entities WHERE id = ?;", (alias_id,))
            
            log.info("Successfully merged %s into %s", alias_name, canonical_name)


    def merge_all_aliases(self, canonical_name: str, strategy: str = "error_on_conflict"):
        """
        Merge all alias entities for a canonical entity.
        
        Args:
            canonical_name: The canonical entity name
            strategy: How to handle errors:
                - "error_on_conflict": Raise on first error (default)
                - "skip_on_conflict": Log and continue on errors
        """
        aliases = self.load_aliases(canonical_name)

        merged = []
        skipped = []

        for alias in aliases:
            try:
                self.merge_alias(canonical_name, alias)
                merged.append(alias)
            except Exception as e:
                if strategy == "error_on_conflict":
                    raise e
                elif strategy == "skip_on_conflict":
                    log.warning("Skipping merge of %s: %s", alias, e)
                    skipped.append((alias, str(e)))
                else:
                    raise ValueError(f"Unknown strategy: {strategy}")
        
        log.info("Merged %s aliases for %s", len(merged), canonical_name)
        if skipped:
            log.warning(f"Skipped {len(skipped)} aliases: {skipped}")
        
        return {"merged": merged, "skipped": skipped}


    def delete_entity(self, name: str, cascade: bool = True) -> None:
        """
        Delete a canonical entity and associated data, honoring cascade rules.
        
        If 'name' is an alias, refuse and point to the canonical.
        For cascade=False:
        - Block deletion if the canonical or any alias-entity has relationships.
        - Block if the canonical has entity claims.
        For cascade=True:
        - Delete only relationships where the canonical entity's id is an endpoint.
            (Do NOT delete relationships that belong to alias-entities.)
        - Delete canonical entity's claims.
        - Delete alias mappings for this canonical.
        - Delete the canonical entity row.
        """
        canonical = self.resolve_alias(name)

        if name != canonical:
            msg = (
                f"Cannot delete: '{name}' is an alias of '{canonical}'. "
                f"Delete the canonical entity '{canonical}' instead, "
                f"or delete the '{name}' alias from '{canonical}' before proceeding."
            )
            raise DeletionConflict(name, "entities", message=msg)
        
        with self._conn() as con:
            row = con.execute(
                "SELECT id FROM entities WHERE name = ?;", (canonical,)
            ).fetchone()
            if not row:
                raise EntityNotFoundError(canonical)
            canonical_id = row[0]

            try:
                expanded_ids = self._expand_ids(con, canonical)
            except EntityNotFoundError:
                raise EntityNotFoundError(canonical)
            
            # guard: relationships touching canonical or any alias-entity
            if expanded_ids:
                placeholders = ",".join("?" * len(expanded_ids))
                rels_guard = con.execute(
                    f"""
                    SELECT id FROM relationships
                    WHERE source_id IN ({placeholders})
                       OR target_id IN ({placeholders});
                    """,
                    expanded_ids + expanded_ids
                ).fetchall()
            else:
                rels_guard = []

            # guard: claims directly on the canonical entity
            claims_guard = con.execute(
                "SELECT id FROM claims WHERE entity_id = ?;",
                (canonical_id,)
            ).fetchall()

            if not cascade:
                rel_count = len(rels_guard)
                claim_count = len(claims_guard)
                if rels_guard and claims_guard:
                    msg = (
                        f"Entity '{canonical}' has {rel_count} relationships and {claim_count} claims. "
                        f"Use cascade=True or clean up manually."
                    )
                    raise DeletionConflict(canonical, "entities", message=msg)
                elif rels_guard:
                    msg = (
                        f"Entity '{canonical}' has {rel_count} relationships. "
                        f"Use cascade=True or clean up manually."
                    )
                    raise DeletionConflict(canonical, "entities", message=msg)
                elif claims_guard:
                    msg = (
                        f"Entity '{canonical}' has {claim_count} claims. "
                        f"Use cascade=True or clean up manually."
                    )
                    raise DeletionConflict(canonical, "entities", message=msg)

            # delete relationship claims, then relationships
            rels = con.execute(
                "SELECT id FROM relationships WHERE source_id = ? OR target_id = ?;",
                (canonical_id, canonical_id)
            ).fetchall()
            for rel in rels:
                rel_id = rel[0]
                con.execute("DELETE FROM claims WHERE relationship_id = ?;", (rel_id,))
                con.execute("DELETE FROM relationships WHERE id = ?;", (rel_id,))

            # delete entity claims // entity aliases // entity itself
            con.execute("DELETE FROM claims WHERE entity_id = ?;", (canonical_id,))
            con.execute("DELETE FROM aliases WHERE entity_id = ?;", (canonical_id,))
            con.execute("DELETE FROM entities WHERE id = ?;", (canonical_id,))


    def delete_relationship(self,
        source: str,
        target: str,
        directed: Optional[bool] = None,
        cascade: bool = True
    ) -> None:
        """
        Delete all relationships between source and target (including alias-entities).
        
        - Alias-expanded: expand both endpointsd to canonical + alias-entities
        - Undirected behaviour: remove both (src in S, tgt in T, directed=0) and
          (src in T, tgt in S, directed=0)
        - If no matches exist, this is a no-op (idempotent delete).
        - If cascade=False and any matched relationship has claims, raise.
        """
        source_canonical = self.resolve_alias(source)
        target_canonical = self.resolve_alias(target)

        if source_canonical == target_canonical:
            raise RelationshipCollisionError(source, target)

        with self._conn() as con:
            rel_ids: list[int] = []
            if directed is None:
                rel_ids.extend(self._relationship_ids_alias_expanded(con, source_canonical, target_canonical, False))
                rel_ids.extend(self._relationship_ids_alias_expanded(con, source_canonical, target_canonical, True))
                rel_ids.extend(self._relationship_ids_alias_expanded(con, target_canonical, source_canonical, True))
            elif directed is True:
                rel_ids.extend(self._relationship_ids_alias_expanded(con, source_canonical, target_canonical, True))
            else:
                rel_ids.extend(self._relationship_ids_alias_expanded(con, source_canonical, target_canonical, False))
           
            if not rel_ids:
                log.info("No relationship found between %s and %s.", source_canonical, target_canonical)
                return

            # if cascade is off, ensure none of the matched rels have claims
            if not cascade:
                placeholders = ",".join("?" * len(rel_ids))
                claims_guard = con.execute(
                    f"SELECT 1 FROM claims WHERE relationship_id IN ({placeholders}) LIMIT 1;",
                    rel_ids
                ).fetchone()
                if claims_guard:
                    claim_count = len(claims_guard)
                    msg = (
                        f"Relationship between '{source_canonical}' "
                        f"and '{target_canonical}' has {claim_count} claims. "
                        f"Use cascade=True or clean up manually."
                    )
                    raise DeletionConflict(source_canonical, "relationships", message=msg)

            # cascade delete claims for all matched relationships, then the relationships
            placeholders = ",".join("?" * len(rel_ids))
            con.execute(
                f"DELETE FROM claims WHERE relationship_id IN ({placeholders});",
                rel_ids
            )
            con.execute(
                f"DELETE FROM relationships WHERE id IN ({placeholders});",
                rel_ids
            )


    def delete_alias(self, entity_name: str, alias: str) -> None:
        """
        Delete a specific alias mapping, not the alias entity itself.
        - Only deletes the mapping row; if the alias string is an entity too,
            that entity remains untouched.
        """
        canonical = self.resolve_alias(entity_name)
        if canonical != entity_name:
            msg = (f"Cannot delete alias: '{entity_name}' is an alias of '{canonical}'.")
            raise DeletionConflict(alias, "aliases", message=msg)

        with self._conn() as con:
            ent_row = con.execute(
                "SELECT id FROM entities WHERE name = ?;", (entity_name,)
            ).fetchone()
            if not ent_row:
                raise EntityNotFoundError(entity_name)
            entity_id = ent_row[0]

            mapping = con.execute(
                "SELECT entity_id, id FROM aliases WHERE alias = ?;",
                (alias,)
            ).fetchone()
            if not mapping:
                raise AliasConflictError(alias, "<unmapped>", entity_name,
                    message=f"'{alias}' is not an alias of '{entity_name}' (no mapping found).")
            
            if mapping[0] != entity_id:
                other_name = con.execute(
                "SELECT name FROM entities WHERE id = ?;", (mapping[0],)
            ).fetchone()[0]
                raise AliasConflictError(alias, other_name, entity_name,
                    message=f"'{alias}' is mapped to '{other_name}', not '{entity_name}'.")
            
            con.execute("DELETE FROM aliases WHERE id = ?;", (mapping[1],))


    def delete_claim(self,
        content: Optional[str] = None,
        entity_name: Optional[str] = None,
        relationship: Optional[tuple[str, str]] = None,
        source: Optional[str] = None,
        date_range: Optional[tuple[str, str]] = None,
        directed: Optional[bool] = None,
        mode: Literal[
            "exact",
            "by_entity",
            "by_relationship",
            "by_source",
            "by_date",
            "by_content",
        ] = "exact",
    ) -> None:
        """
        Delete claims by various filter modes. No cascade (claims are leaves).
        
        Semantics:
        - `by_entity`: conservative (canonical entity's claims only).
        - `by_relationship`: alias-expanded across both families (undirected).
        - `by_source`/`by_date`/`by_content`: straightforward filters.
        - `exact`: AND logic across provided filters;
                 entity filter conservative; relationship filter alias-expanded.
        - If the filters match nothing, this is a no-op (no raise).
        """
        if not any([content, entity_name, relationship, source, date_range]):
            raise ValueError("Must provide at least one filter criterion")

        with self._conn() as con:
            clauses = []
            params = []

            def add_entity_clause(name: str):
                canonical = self.resolve_alias(name)
                row = con.execute("SELECT id FROM entities WHERE name = ?;", (canonical,)).fetchone()
                if not row:
                    return None
                return ("entity_id = ?", [row[0]])

            if mode == "by_entity" and entity_name:
                clause = add_entity_clause(entity_name)
                if clause is None:
                    return # no-op
                clauses.append(clause[0])
                params.extend(clause[1])

            elif mode == "by_relationship" and relationship:
                src, tgt = relationship
                rel_ids = []
                if directed is None or directed is False:
                    rel_ids.extend(self._relationship_ids_alias_expanded(con, src, tgt, False))
                if directed is True:
                    rel_ids.extend(self._relationship_ids_alias_expanded(con, src, tgt, True))
                if not rel_ids:
                    return
                placeholders = ",".join("?" * len(rel_ids))
                clauses.append(f"relationship_id IN ({placeholders})")
                params.extend(rel_ids)

            elif mode == "by_source" and source:
                clauses.append("source = ?")
                params.append(source)

            elif mode == "by_date" and date_range:
                clauses.append("date_added BETWEEN ? AND ?")
                params.extend(date_range)

            elif mode == "by_content" and content:
                clauses.append("content = ?")
                params.append(content)

            elif mode == "exact":
                if entity_name: # entity filter (conservative)
                    clause = add_entity_clause(entity_name)
                    if clause is None:
                        return  # no-op
                    clauses.append(clause[0])
                    params.extend(clause[1])
                if relationship:
                    src, tgt = relationship
                    rel_ids = []
                    if directed is None or directed is False:
                        rel_ids.extend(self._relationship_ids_alias_expanded(con, src, tgt, False))
                    if directed is True:
                        rel_ids.extend(self._relationship_ids_alias_expanded(con, src, tgt, True))
                    if not rel_ids:
                        return
                    placeholders = ",".join("?" * len(rel_ids))
                    clauses.append(f"relationship_id IN ({placeholders})")
                    params.extend(rel_ids)
                if content:
                    clauses.append("content = ?")
                    params.append(content)
                if source:
                    clauses.append("source = ?")
                    params.append(source)
                if date_range:
                    clauses.append("date_added BETWEEN ? AND ?")
                    params.extend(date_range)
            else:
                raise ValueError(f"Unsupported mode: {mode}")
            
            if not clauses:
                return

            sql = "DELETE FROM claims WHERE " + " AND ".join(clauses)
            con.execute(sql, tuple(params))
    

    def entity_exists(self, name: str) -> bool:
        """Check if entity exists in DB"""
        with self._conn() as con:
            result = con.execute(
                "SELECT 1 FROM entities WHERE name = ? LIMIT 1;",
                (name,)
            ).fetchone()
            return result is not None
    

    def list_all_entities(self) -> list[str]:
        """
        List all canonical entity names in the graph.
        Does not include aliases.
        """
        with self._conn() as con:
            rows = con.execute("SELECT name FROM entities ORDER BY name;").fetchall()
            return [row[0] for row in rows]
    

    def list_all_aliases(self, entity_name: str) -> list[str]:
        """Return all aliases for an entity"""
        canonical = self.resolve_alias(entity_name)
        with self._conn() as con:
            ent_row = con.execute(
                "SELECT id FROM entities WHERE name = ?;",
                (canonical,)
            ).fetchone()
            if not ent_row:
                raise EntityNotFoundError(canonical)
            canonical_id = ent_row[0]

            rows = con.execute(
                "SELECT alias FROM aliases WHERE entity_id = ? ORDER BY alias;",
                (canonical_id,)
            ).fetchall()

            aliases = [r[0] for r in rows]
            return aliases


    def _normalize_pair(self,
        source_id: int,
        target_id: int,
        directed: int | bool
    ) -> tuple[int, int, int]:
        """
        Normalize relationship endpoint ordering for undirected edges.
        Returns (source_id, target_id, directed_int).
        """
        d = int(bool(directed))
        if d == 0 and source_id > target_id:
            return target_id, source_id, 0
        return source_id, target_id, d


    def _expand_ids(self, con, name: str) -> list[int]:
        """Return [canonical_id] plus ids of any alias-entities for this name."""
        canonical = self.resolve_alias(name)
        row = con.execute("SELECT id FROM entities WHERE name = ?;", (canonical,)).fetchone()
        if not row:
            raise EntityNotFoundError(canonical)
        ids = [row[0]]

        alias_rows = con.execute(
            "SELECT alias FROM aliases WHERE entity_id = ?;", (row[0],)
        ).fetchall()
        for alias_row in alias_rows:
            alias_entity = con.execute(
                "SELECT id FROM entities WHERE name = ?;", (alias_row[0],)
            ).fetchone()
            if alias_entity:
                ids.append(alias_entity[0])
        return ids
    

    def _relationship_ids_alias_expanded(self,
        con,
        src_name: str,
        tgt_name: str,
        directed: Optional[bool]
    ) -> list[int]:
        """
        Helper: return relationship IDs between alias-expanded src and tgt families.
        - directed is True: only directed edges source→target.
        - directed is False: only undirected edges (unordered).
        """
        try:
            src_ids = self._expand_ids(con, self.resolve_alias(src_name))
            tgt_ids = self._expand_ids(con, self.resolve_alias(tgt_name))
        except EntityNotFoundError:
            return []

        ps_src = ",".join("?" * len(src_ids))
        ps_tgt = ",".join("?" * len(tgt_ids))

        if directed is True:
            sql = f"""
                SELECT id FROM relationships
                WHERE directed = 1
                AND source_id IN ({ps_src}) AND target_id IN ({ps_tgt});
            """
            params = src_ids + tgt_ids
        else:
            # directed is False (or None in callers that want undirected only)
            sql = f"""
                SELECT id FROM relationships
                WHERE directed = 0 AND (
                    (source_id IN ({ps_src}) AND target_id IN ({ps_tgt}))
                    OR
                    (source_id IN ({ps_tgt}) AND target_id IN ({ps_src}))
                );
            """
            params = src_ids + tgt_ids + tgt_ids + src_ids

        rows = con.execute(sql, params).fetchall()
        return [r[0] for r in rows]


    def _has_relationship_between(self, entity1_name: str, entity2_name: str) -> bool:
        """Check if any relationship exists between two entities (considering aliases)."""
        with self._conn() as con:
            entity1_canonical = self.resolve_alias(entity1_name)
            entity1_row = con.execute(
                "SELECT id FROM entities WHERE name = ?;",
                (entity1_canonical,)
            ).fetchone()
            
            if not entity1_row:
                return False
            
            entity1_ids = [entity1_row[0]]
            
            alias_rows = con.execute(
                "SELECT alias FROM aliases WHERE entity_id = ?;",
                (entity1_row[0],)
            ).fetchall() # add alias entity IDs for entity1
            
            for alias_row in alias_rows:
                alias_entity = con.execute(
                    "SELECT id FROM entities WHERE name = ?;",
                    (alias_row[0],)
                ).fetchone()
                if alias_entity:
                    entity1_ids.append(alias_entity[0])
            
            entity2_canonical = self.resolve_alias(entity2_name)
            entity2_row = con.execute(
                "SELECT id FROM entities WHERE name = ?;",
                (entity2_canonical,)
            ).fetchone() # get all entity IDs for entity2
            
            if not entity2_row:
                return False
            
            entity2_ids = [entity2_row[0]]
            
            alias_rows = con.execute(
                "SELECT alias FROM aliases WHERE entity_id = ?;",
                (entity2_row[0],)
            ).fetchall() # add alias entity IDs for entity2
            
            for alias_row in alias_rows:
                alias_entity = con.execute(
                    "SELECT id FROM entities WHERE name = ?;",
                    (alias_row[0],)
                ).fetchone()
                if alias_entity:
                    entity2_ids.append(alias_entity[0])
            
            # check if any relationship exists between any combination
            placeholders1 = ','.join('?' * len(entity1_ids))
            placeholders2 = ','.join('?' * len(entity2_ids))
            
            result = con.execute(f"""
                SELECT 1 FROM relationships 
                WHERE (source_id IN ({placeholders1}) AND target_id IN ({placeholders2}))
                OR (source_id IN ({placeholders2}) AND target_id IN ({placeholders1}))
                LIMIT 1;
            """, entity1_ids + entity2_ids + entity2_ids + entity1_ids).fetchone()
            
            return result is not None
    

    @debug_only
    def load_entity_claims_raw(self, name: str) -> list[ClaimData]:
        """
        Load claims for exact entity name without alias resolution.
        This is for debugging and inspection. Normal code should use load_entity_claims().
        """
        with self._conn() as con:
            entity_row = con.execute(
                "SELECT id FROM entities WHERE name = ?;",
                (name,)
            ).fetchone()
            
            if not entity_row:
                raise EntityNotFoundError(name)
            
            entity_id = entity_row[0]
            
            rows = con.execute("""
                SELECT content, source, date_added 
                FROM claims 
                WHERE entity_id = ?;
            """, (entity_id,)).fetchall()
            
            return [
                ClaimData(
                    content=row["content"],
                    source=row["source"],
                    date_added=row["date_added"]
                )
                for row in rows
            ]
        

    def dump_all_relationships(self):
        """
        Returns list of all relationships with resolved names.
        """
        with self._conn() as con:
            rows = con.execute("""
                SELECT
                    r.id AS relationship_id,
                    e1.name AS source_name,
                    e2.name AS target_name,
                    r.strength AS strength,
                    r.directed AS directed
                FROM relationships r
                JOIN entities e1 ON e1.id = r.source_id
                JOIN entities e2 ON e2.id = r.target_id;
            """).fetchall()

        return rows
        
    
    def dump_all_claims(self):
        """Return list of dict-like rows for all claims (entity_id or relationship_id present)."""
        with self._conn() as con:
            rows = con.execute("""
                SELECT id AS claim_id,
                    entity_id,
                    relationship_id,
                    content,
                    source,
                    date_added
                FROM claims;
            """).fetchall()
        return rows
