"""
Embedding service for Nocturne Memory — 寫一返三.

Computes text embeddings and finds semantically similar memories.
Uses Nomic API (free tier) or local ollama as fallback.
"""

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
EMBED_MODEL_NOMIC = "nomic-embed-text-v1.5"
EMBED_MODEL_OLLAMA = "nomic-embed-text"
EMBED_DIM = 768


async def compute_embedding(content: str) -> Optional[list[float]]:
    """Compute 768d embedding for text content.

    Priority: Nomic API → ollama → None (graceful skip).
    """
    truncated = content[:8000]

    if NOMIC_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(
                    "https://api-atlas.nomic.ai/v1/embedding/text",
                    headers={"Authorization": f"Bearer {NOMIC_API_KEY}"},
                    json={
                        "model": EMBED_MODEL_NOMIC,
                        "texts": [truncated],
                        "task_type": "search_document",
                    },
                )
                resp.raise_for_status()
                return resp.json()["embeddings"][0]
        except Exception as e:
            logger.warning(f"Nomic API embedding failed: {e}")

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{OLLAMA_URL}/api/embeddings",
                json={"model": EMBED_MODEL_OLLAMA, "prompt": truncated},
            )
            resp.raise_for_status()
            return resp.json()["embedding"]
    except Exception as e:
        logger.warning(f"Ollama embedding failed: {e}")

    return None


async def store_embedding(session: AsyncSession, memory_id: int, embedding: list[float]) -> None:
    """Store embedding vector for a memory row."""
    await session.execute(
        text("UPDATE memories SET embedding = :vec WHERE id = :mid"),
        {"vec": str(embedding), "mid": memory_id},
    )


async def find_similar_memories(
    session: AsyncSession,
    embedding: list[float],
    limit: int = 3,
    exclude_node_uuid: Optional[str] = None,
) -> list[dict]:
    """Find top-k semantically similar memories by cosine distance.

    Returns: [{node_uuid, content_preview, similarity}]
    """
    query = """
        SELECT m.node_uuid, LEFT(m.content, 200) as preview,
               1 - (m.embedding <=> :vec::vector) as similarity
        FROM memories m
        WHERE m.embedding IS NOT NULL
          AND m.deprecated = false
    """
    params = {"vec": str(embedding)}

    if exclude_node_uuid:
        query += " AND m.node_uuid != :exclude"
        params["exclude"] = exclude_node_uuid

    query += " ORDER BY m.embedding <=> :vec2::vector LIMIT :lim"
    params["vec2"] = str(embedding)
    params["lim"] = limit

    result = await session.execute(text(query), params)
    rows = result.fetchall()

    return [
        {
            "node_uuid": row[0],
            "content_preview": row[1],
            "similarity": round(float(row[2]), 3) if row[2] else 0,
        }
        for row in rows
    ]


async def embed_and_find_related(
    session: AsyncSession,
    content: str,
    memory_id: int,
    node_uuid: str,
) -> str:
    """Compute embedding, store it, find related memories. Returns formatted string."""
    embedding = await compute_embedding(content)
    if not embedding:
        return ""

    await store_embedding(session, memory_id, embedding)

    similar = await find_similar_memories(session, embedding, limit=3, exclude_node_uuid=node_uuid)
    if not similar:
        return ""

    lines = ["\n[RELATED MEMORIES — 寫一返三]"]
    for mem in similar:
        lines.append(f"  - {mem['node_uuid'][:8]}... (similarity: {mem['similarity']}) — {mem['content_preview'][:100]}...")

    return "\n".join(lines)
