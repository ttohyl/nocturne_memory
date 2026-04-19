import logging
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger(__name__)

async def up(engine: AsyncEngine):
    """
    Version: v2.4.0
    Add embedding TEXT column to memories table.
    Stores JSON array of floats (768d nomic-embed-text vectors).
    SQLite and PostgreSQL compatible.
    """
    is_postgres = "postgresql" in str(engine.url)

    async with engine.begin() as conn:
        if is_postgres:
            await conn.execute(text(
                "ALTER TABLE memories ADD COLUMN IF NOT EXISTS embedding TEXT NULL"
            ))
        else:
            try:
                await conn.execute(text(
                    "ALTER TABLE memories ADD COLUMN embedding TEXT NULL"
                ))
            except Exception as e:
                if "duplicate column" in str(e).lower():
                    logger.info("embedding column already exists, skipping")
                else:
                    raise

    logger.info("Migration 013: embedding TEXT column added to memories")
