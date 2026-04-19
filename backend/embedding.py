"""
Embedding service for Nocturne Memory — 寫一返三.

SQLite-compatible: stores embeddings as JSON text, computes cosine similarity in Python.
With ~50 nodes, full-table scan is millisecond-level — no vector index needed.
"""

import json
import math
import os
import logging
from typing import Optional

import httpx
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Memory

logger = logging.getLogger(__name__)

NOMIC_API_KEY = os.getenv("NOMIC_API_KEY", "")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
EMBED_DIM = 768


def cosine_similarity(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


async def compute_embedding(content: str) -> Optional[list[float]]:
    """Compute 768d embedding. Nomic API → ollama → None."""
    truncated = content[:8000]

    if NOMIC_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(
                    "https://api-atlas.nomic.ai/v1/embedding/text",
                    headers={"Authorization": f"Bearer {NOMIC_API_KEY}"},
                    json={
                        "model": "nomic-embed-text-v1.5",
                        "texts": [truncated],
                        "task_type": "search_document",
                    },
                )
                resp.raise_for_status()
                return resp.json()["embeddings"][0]
        except Exception as e:
            logger.warning(f"Nomic API failed: {e}")

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{OLLAMA_URL}/api/embeddings",
                json={"model": "nomic-embed-text", "prompt": truncated},
            )
            resp.raise_for_status()
            return resp.json()["embedding"]
    except Exception as e:
        logger.warning(f"Ollama failed: {e}")

    return None


async def store_embedding(session: AsyncSession, memory_id: int, embedding: list[float]) -> None:
    """Store embedding as JSON text."""
    vec_json = json.dumps(embedding)
    await session.execute(
        text("UPDATE memories SET embedding = :vec WHERE id = :mid"),
        {"vec": vec_json, "mid": memory_id},
    )


async def find_similar_memories(
    session: AsyncSession,
    embedding: list[float],
    limit: int = 3,
    exclude_node_uuid: Optional[str] = None,
) -> list[dict]:
    """Find top-k similar memories using Python cosine similarity."""
    query = select(Memory).where(
        Memory.embedding.isnot(None),
        Memory.deprecated == False,
    )
    if exclude_node_uuid:
        query = query.where(Memory.node_uuid != exclude_node_uuid)

    result = await session.execute(query)
    memories = result.scalars().all()

    scored = []
    for mem in memories:
        try:
            stored_vec = json.loads(mem.embedding)
            sim = cosine_similarity(embedding, stored_vec)
            scored.append({
                "node_uuid": mem.node_uuid,
                "content_preview": mem.content[:200] if mem.content else "",
                "similarity": round(sim, 3),
            })
        except (json.JSONDecodeError, TypeError):
            continue

    scored.sort(key=lambda x: x["similarity"], reverse=True)
    return scored[:limit]


async def embed_and_find_related(
    session: AsyncSession,
    content: str,
    memory_id: int,
    node_uuid: str,
) -> str:
    """Compute embedding, store it, find related memories. Returns formatted string."""
    logger.info(f"[EMBED] Start: memory_id={memory_id}, node_uuid={node_uuid[:8]}")

    embedding = await compute_embedding(content)
    if not embedding:
        logger.warning(f"[EMBED] compute_embedding returned None — skipping")
        return ""

    logger.info(f"[EMBED] Got embedding dim={len(embedding)}")

    await store_embedding(session, memory_id, embedding)
    await session.commit()
    logger.info(f"[EMBED] Stored + committed embedding for memory {memory_id}")

    similar = await find_similar_memories(session, embedding, limit=3, exclude_node_uuid=node_uuid)
    logger.info(f"[EMBED] find_similar returned {len(similar)} results")

    if not similar:
        return ""

    lines = ["\n[RELATED MEMORIES — 寫一返三]"]
    for mem in similar:
        lines.append(f"  - {mem['node_uuid'][:8]}... (similarity: {mem['similarity']}) — {mem['content_preview'][:100]}...")

    result = "\n".join(lines)
    logger.info(f"[EMBED] Returning related text, length={len(result)}")
    return result
