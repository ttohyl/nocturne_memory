"""One-time migration: add pgvector extension + embedding column to memories table."""

import asyncio
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_db_manager
from sqlalchemy import text


async def migrate():
    db = get_db_manager()
    await db.init_db()

    async with db.session() as session:
        await session.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        await session.execute(
            text("ALTER TABLE memories ADD COLUMN IF NOT EXISTS embedding vector(768)")
        )
        await session.commit()

    print("Migration done: pgvector extension + embedding column added.")


if __name__ == "__main__":
    asyncio.run(migrate())
