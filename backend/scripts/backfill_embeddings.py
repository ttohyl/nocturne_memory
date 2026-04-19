"""Backfill embeddings for all existing memories that don't have one yet.

Usage (Railway): railway run --service nocturne-cloud -- python backend/scripts/backfill_embeddings.py
"""

import asyncio
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import select
from db import get_db_manager
from db.models import Memory
from embedding import compute_embedding, store_embedding


async def backfill():
    db = get_db_manager()
    await db.init_db()

    async with db.session() as session:
        result = await session.execute(
            select(Memory).where(
                Memory.embedding.is_(None),
                Memory.deprecated == False,
            )
        )
        memories = result.scalars().all()
        total = len(memories)
        print(f"Found {total} memories without embedding", flush=True)

        done = 0
        skipped = 0
        for mem in memories:
            embedding = await compute_embedding(mem.content or "")
            if not embedding:
                skipped += 1
                print(f"[{done + skipped}/{total}] SKIP id={mem.id} (embedding failed)", flush=True)
                continue

            await store_embedding(session, mem.id, embedding)
            await session.commit()
            done += 1
            if done % 10 == 0:
                print(f"[{done + skipped}/{total}] Done {done}, skipped {skipped}", flush=True)

        print(f"\n✅ Backfill complete: {done} embedded, {skipped} skipped, {total} total", flush=True)


if __name__ == "__main__":
    asyncio.run(backfill())
