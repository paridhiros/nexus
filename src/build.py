"""
Build orchestrators (vectorDB // graph)
"""

from __future__ import annotations
from typing import Optional
import asyncio
from asyncio import Semaphore

# - local -
from ..config import log, VectorDBConfig, GraphConfig, LLMConfig, HEAD
from .state import VectorIndex, MetaIndex, GraphIndex
from .embed import Embedder
from .llm import SyncLLM, AsyncLLM
from .util import fetch_doc, chunk, print_progress_bar
from ._schemas import (
    ChunkData,
    DocLike, Doc,
    ProcessingStats,
    RelationshipRecord,
    RelationshipCollisionError
)


# === VECTOR DB BUILDER ===

class VectorDBBuilder:
    """Class for building the vector database."""
    
    def __init__(self, cfg: VectorDBConfig):
        self.cfg = cfg
        self.embedder = Embedder(self.cfg.embed_model, self.cfg.batch_size)
        self.vector_index = VectorIndex(self.cfg, self.embedder.dim, self.cfg.rebuild)
        self.meta_index = MetaIndex(self.cfg.meta_index_path)
        self.stats = ProcessingStats()
        
    
    def build(self, docs: list[DocLike]):
        """Build vectorDB from given documents"""
        
        if not docs:
            log.warning("No documents to process")
            return
        else:
            log.info("Preparing %s documents...", len(docs))

        if self.cfg.rebuild:
            self.meta_index.drop()
        for doc in docs:
            try:
                self._process_doc(doc, self.cfg.rebuild)
            except Exception as e:
                log.error("Failed to process %s: %s", doc.filepath, e)
            
        self.vector_index.save()
        log.info("Processing complete: %s", self.stats)


    def _process_doc(self, doc: DocLike, rebuild: bool):
        """Chunk, embed, and upsert a single document file."""
        
        if not rebuild: # don't re-process docs if not rebuilding
            if self.meta_index.has_chunks(doc.document_id):
                return

        # chunk document
        chunks_data, chunks_text = chunk(doc.filepath, doc.document_id,
            self.embedder, self.cfg.max_tokens, self.cfg.overlap)
        if not chunks_data or not chunks_text:
            self.stats.errors += 1
            return

        try: # generate embeddings
            embeddings = self.embedder.embed(chunks_text)
            if embeddings.size == 0:
                log.error("No embeddings generated for %s", doc.filepath)
                self.stats.errors += 1
                return
        except Exception as e:
            log.error("Could not embed chunks for %s: %s", doc.filepath, e)
            self.stats.errors += 1
            return

        embedding_ids = self.vector_index.add_vectors(embeddings)
    
        if len(chunks_data) != len(embedding_ids):
            msg = f"Mismatch between chunks ({len(chunks_data)}) and embeddings ({len(embedding_ids)})"
            log.error(msg)
            raise ValueError(msg)
        
        # upsert batch
        embedding_ids = [int(x) for x in embedding_ids]
        chunks = [
            ChunkData(
                document_id=c.document_id,
                start_token=c.start_token,
                end_token=c.end_token,
                start_char=c.start_char,
                end_char=c.end_char,
                source_path=c.source_path,
                embedding_id=e_id,
            )
            for c, e_id in zip(chunks_data, embedding_ids)
        ]
        self.meta_index.upsert(chunks)

        # update statistics
        self.stats.documents_processed += 1
        self.stats.chunks_created += len(chunks_data)
        self.stats.embeddings_generated += len(embedding_ids)


# === GRAPH BUILDER ===

class GraphBuilder:
    """Class for building the knowledge graph."""
    def __init__(self, debug: bool = False):
        log.info(HEAD)
        self.graph_config = GraphConfig()
        self.llm_config = LLMConfig()

        required = (
            "tuple_delimiter", "record_delimiter", "completion_delimiter",
            "extraction_templates", "entity_templates", "extraction_domains", "extraction_concurrency",
            "graph_index_path"
        )
        missing = [r for r in required if not hasattr(self.graph_config, r)]
        if missing:
            raise ValueError(f"Config missing required fields: {', '.join(missing)}")

        self.graph_index = GraphIndex(self.graph_config.graph_index_path)
        
        self.tuple_delimiter = self.graph_config.tuple_delimiter
        self.record_delimiter = self.graph_config.record_delimiter
        self.completion_delimiter = self.graph_config.completion_delimiter

        self.extraction_domains = self.graph_config.extraction_domains
        self.extraction_templates = self.graph_config.extraction_templates
        self.entity_templates = self.graph_config.entity_templates

        self.extraction_concurrency = self.graph_config.extraction_concurrency

        _backend = self.graph_config.extraction_llm_backend
        _api_key = self.llm_config.api_key
        _url = self.llm_config.local_backend_url
        if self.extraction_concurrency == "sync":
            _model = self.llm_config.sync_model
            self.llm = SyncLLM(backend=_backend, model=_model, api_key=_api_key, url=_url)
        elif self.extraction_concurrency == "async":
            _model = self.llm_config.async_model
            self.llm = AsyncLLM(backend=_backend, model=_model, api_key=_api_key, url=_url)
        else:
            raise ValueError(f"graph_config.extraction_concurrency must be set to sync or async.")
        
        self.llm.set_system(self.graph_config.system_prompt)
        self.semaphore_rate = self.llm_config.semaphore_rate
        self.batch_size = self.graph_config.extraction_batch_size

        self.debug = debug


    def build(self, docs: list[DocLike]):
        """
        Top-level graph builder entrypoint.
        Dispatches to sync or async version.
        """
        total = len(docs)
        if total == 0:
            raise ValueError("No documents provided to build graph")
        log.info(f"Now building graph with {total} document{'s' if total != 1 else ''}")

        docs = [Doc(**vars(doc)) if not isinstance(doc, Doc) else doc for doc in docs]
        if self.extraction_concurrency == "sync":
            self._build_sync(docs)
        else:
            asyncio.run(self._build_async(docs))

    
    def _build_sync(self, docs: list[DocLike]):
        """"""
        total = len(docs)
        print_progress_bar(0, total)
        for i in range(0, len(docs), self.batch_size):
            batch = docs[i : i + self.batch_size]
            entities_batch, relationships_batch = [], []

            for j, doc in enumerate(batch):
                current = i + j + 1
                doc_text = fetch_doc(doc.filepath)
                prompt = self._build_extraction_prompt(
                    document=doc_text,
                    domain=doc.domain,
                    context=doc.context,
                )
                response = self.llm.run(prompt)
                if self.debug:
                    log.info("LLM response: %s", response)
                e, r = self._process_llm_response(response)
                e, r = self._add_metadata(entities=e, relationships=r, date=doc.date, source=doc.source)
                entities_batch.extend(e)
                relationships_batch.extend(r)
                print_progress_bar(current, total)

            self._upsert_entities(entities_batch)
            self._upsert_relationships(relationships_batch)
            

    async def _build_async(self, docs: list[DocLike]):
        """"""
        sem = Semaphore(self.semaphore_rate)
        async def _process_doc(doc):
            async with sem:
                doc_text = await asyncio.to_thread(fetch_doc, doc.filepath)
                prompt = self._build_extraction_prompt(
                    document=doc_text,
                    domain=doc.domain,
                    context=doc.context,
                )
                response = await self.llm.run(prompt)
                if self.debug:
                    log.info("LLM response: %s", response)
                e, r = await asyncio.to_thread(self._process_llm_response, response)
                return await asyncio.to_thread(self._add_metadata,
                    entities=e, relationships=r, date=doc.date, source=doc.source
                )
        
        for i in range(0, len(docs), self.batch_size):
            batch = docs[i : i + self.batch_size]
            results = await asyncio.gather(*[_process_doc(doc) for doc in batch])

            entities_batch, relationships_batch = [], []
            for e, r in results:
                entities_batch.extend(e)
                relationships_batch.extend(r)

            self._upsert_entities(entities_batch)
            self._upsert_relationships(relationships_batch)


    def _upsert_entities(self, entities: list[dict]):
        """Upsert a batch of entities and their claims"""
        for entity in entities:
            entity_name = entity["entity_name"]
            entity_type = entity["entity_type"]
            claim = entity.get("entity_claim")
            claim_date = entity.get("claim_date", None)
            source = entity.get("source", None)

            self.graph_index.upsert_entity(name=entity_name, entity_type=entity_type)
            self.graph_index.upsert_claim(content=claim,source=source, entity_name=entity_name, claim_date=claim_date)


    def _upsert_relationships(self, relationships: list[dict]):
        """Upsert a batch of relationships and their claims"""
        for relationship in relationships:
            source_name = relationship["source_name"]
            target_name = relationship["target_name"]
            claim = relationship["relationship_claim"]
            claim_date = relationship.get("claim_date", None)
            source = relationship.get("source", None)

            rel = RelationshipRecord(
                source_name=source_name,
                target_name=target_name,
                directed=False # | TODO: manipulate directionality at ingest
            )
            
            try:
                self.graph_index.upsert_relationship(source_name=source_name, target_name=target_name)
            except RelationshipCollisionError:
                # RelationshipCollisionError only raises when trying to relate an entity to itself,
                # i.e. resolve_alias(source_name) == resolve_alias(target_name).
                # For the time being we will upsert such a relationship as a claim for the canonical entity,
                # so as to not lose potentially useful content. NOTE that upsert_claim() auto-resolves entity.
                log.info("Self-referential relationship detected between %s and %s: upserting to %s",
                    source_name, target_name, source_name # NOTE: not fully accurate... we upsert to _canon_ of source.
                )
                self.graph_index.upsert_claim(content=claim, source=source, entity_name=source_name, claim_date=claim_date)
                continue

            self.graph_index.upsert_claim(content=claim, source=source, relationship=rel, claim_date=claim_date)


    def _build_extraction_prompt(self,
        document: str,
        domain: Optional[str] = None,
        context: Optional[str] = None,
    ) -> str:
        if domain is None:
            domain = self.extraction_domains[0]
        base_template = self.extraction_templates.get(domain)
        entity_types: list[str] = self.entity_templates.get(domain, [])
        if base_template is None:
            raise ValueError(f"Domain '{domain}' not found in extraction_templates")
        
        optional_context = f"\n**Additional context for this document**: {context}" if context else ""
        return base_template.format(
            tuple_delimiter=self.tuple_delimiter,
            record_delimiter=self.record_delimiter,
            completion_delimiter=self.completion_delimiter,
            entity_types=entity_types,
            document=document,
            context=optional_context
        )


    def _process_llm_response(self, llm_response: str) -> tuple[list[dict], list[dict]]:
        """parse relationships / entities from llm's response"""
        def _clean(s: str) -> str:
            return s.strip().strip('"').strip()
        
        try:
            text = llm_response.strip().rstrip(self.completion_delimiter)
            blocks = text.split(self.record_delimiter)
        except Exception as exc:
            log.exception("Failed to preprocess LLM response: %s", exc)
            return [], []

        entities, relationships = [], []
        for block in blocks:
            block = block.strip().removeprefix("(").removesuffix(")")
            if not block: continue
            
            try:
                kind, *raw_fields = block.split(self.tuple_delimiter)
                kind = _clean(kind).lower()
                fields = [_clean(f) for f in raw_fields]

                if kind == "entity":
                    name, etype, claim = fields
                    entities.append({
                        "entity_name": name,
                        "entity_type": etype,
                        "entity_claim": claim
                    })
                elif kind == "relationship":
                    src, tgt, claim = fields
                    relationships.append({
                        "source_name": src,
                        "target_name": tgt,
                        "relationship_claim": claim
                    })
            except Exception as exc:
                log.error("Failed to parse block: %s | Message: %s", block, exc)
                continue  # skip malformed block
        return entities, relationships
    

    def _add_metadata(self,
        entities: list[dict], relationships: list[dict], date: Optional[str]=None, source: Optional[str]=None,
    ):
        return (
            [{**e, "claim_date": date, "source": source} for e in entities],
            [{**r, "claim_date": date, "source": source} for r in relationships]
        )
