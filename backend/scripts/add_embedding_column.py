"""One-time migration: add embedding TEXT column to memories table (SQLite compatible)."""

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
        try:
            await session.execute(
                text("ALTER TABLE memories ADD COLUMN embedding TEXT")
            )
            await session.commit()
            print("Migration done: embedding TEXT column added.")
        except Exception as e:
            if "duplicate column" in str(e).lower() or "already exists" in str(e).lower():
                print("Column already exists, skipping.")
            else:
                raise


if __name__ == "__main__":
    asyncio.run(migrate())
