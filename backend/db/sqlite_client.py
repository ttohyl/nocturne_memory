"""
Database Client for Nocturne Memory System

Graph-based memory storage with:
- Node: a conceptual entity (UUID), version-independent
- Memory: a content version of a node
- Edge: parent→child relationship between nodes, carrying metadata
- Path: materialized URI cache (domain://path → edge)

Supports both SQLite (local, single-user) and PostgreSQL (remote, multi-device).
"""

import os
import uuid as uuid_lib
from datetime import datetime
from typing import Optional, Dict, Any, List
from contextlib import asynccontextmanager
from urllib.parse import urlparse

from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    Boolean,
    DateTime,
    ForeignKey,
    UniqueConstraint,
    select,
    update,
    delete,
    func,
    and_,
    or_,
    not_,
    text,
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base, relationship
from dotenv import load_dotenv, find_dotenv

# Load environment variables
_dotenv_path = find_dotenv(usecwd=True)
if _dotenv_path:
    load_dotenv(_dotenv_path)

Base = declarative_base()

# Sentinel root node — parent_uuid of all top-level edges.
# Using a fixed UUID instead of NULL avoids SQLite's NULL != NULL uniqueness quirk.
ROOT_NODE_UUID = "00000000-0000-0000-0000-000000000000"


# =============================================================================
# ORM Models
# =============================================================================


class Node(Base):
    """A conceptual entity whose UUID persists across content versions.

    Edges reference nodes by UUID, so updating a memory's content (which
    creates a new Memory row) never requires touching the graph structure.
    """

    __tablename__ = "nodes"

    uuid = Column(String(36), primary_key=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    memories = relationship("Memory", back_populates="node")
    child_edges = relationship(
        "Edge", foreign_keys="Edge.child_uuid", back_populates="child_node"
    )
    parent_edges = relationship(
        "Edge", foreign_keys="Edge.parent_uuid", back_populates="parent_node"
    )


class Memory(Base):
    """A single content version of a node.

    Version chain: old.migrated_to → new.id.  All versions of the same
    conceptual entity share the same node_uuid.
    """

    __tablename__ = "memories"

    id = Column(Integer, primary_key=True, autoincrement=True)
    node_uuid = Column(String(36), ForeignKey("nodes.uuid"), nullable=True)
    content = Column(Text, nullable=False)
    deprecated = Column(Boolean, default=False)
    migrated_to = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    node = relationship("Node", back_populates="memories")


class Edge(Base):
    """Directed parent→child relationship between two nodes.

    Carries display name, priority, and disclosure.  The (parent_uuid,
    child_uuid) pair is unique — one edge per structural relationship.
    Multiple Path rows can reference the same edge (aliases).
    """

    __tablename__ = "edges"

    id = Column(Integer, primary_key=True, autoincrement=True)
    parent_uuid = Column(String(36), ForeignKey("nodes.uuid"), nullable=False)
    child_uuid = Column(String(36), ForeignKey("nodes.uuid"), nullable=False)
    name = Column(String(256), nullable=False)
    priority = Column(Integer, default=0)
    disclosure = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("parent_uuid", "child_uuid", name="uq_edge_parent_child"),
    )

    parent_node = relationship(
        "Node", foreign_keys=[parent_uuid], back_populates="parent_edges"
    )
    child_node = relationship(
        "Node", foreign_keys=[child_uuid], back_populates="child_edges"
    )
    paths = relationship("Path", back_populates="edge")


class Path(Base):
    """Materialized URI cache: (domain, path_string) → edge.

    The source of truth for tree structure is the edges table.
    Paths are a routing convenience for URI resolution.
    """

    __tablename__ = "paths"

    domain = Column(String(64), primary_key=True, default="core")
    path = Column(String(512), primary_key=True)
    edge_id = Column(Integer, ForeignKey("edges.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    edge = relationship("Edge", back_populates="paths")


# =============================================================================
# SQLite Client
# =============================================================================


class SQLiteClient:
    """
    Async database client for memory operations.

    Supports SQLite (local) and PostgreSQL (remote, multi-device).

    Core operations:
    - read: Get memory by path (Path → Edge → Memory via node_uuid)
    - create: New node + memory + edge + path
    - update: New memory version on same node; update edge metadata
    - add_path: Create alias (new Path, maybe new Edge)
    - remove_path: Delete paths; refuse if children would become unreachable
    - search: Substring search on path and content
    """

    def __init__(self, database_url: str):
        """
        Initialize the database client.

        Args:
            database_url: SQLAlchemy async URL, e.g.
                         SQLite:     "sqlite+aiosqlite:///nocturne_memory.db"
                         PostgreSQL: "postgresql+asyncpg://user:pass@host:5432/dbname"
        """
        self.database_url = database_url
        self.db_type = self._detect_database_type(database_url)

        # PostgreSQL benefits from connection pooling; SQLite doesn't need it
        engine_kwargs = {"echo": False}
        if self.db_type == "postgresql":
            parsed = urlparse(database_url)
            is_local = parsed.hostname in ("localhost", "127.0.0.1", "::1")

            connect_args = {}
            if not is_local:
                # Remote PostgreSQL: enable SSL and disable prepared statement
                # cache for compatibility with PgBouncer-based poolers
                # (e.g. Supabase, Neon).
                connect_args["ssl"] = "require"
                connect_args["statement_cache_size"] = 0

            engine_kwargs.update({
                "pool_size": 10,
                "max_overflow": 20,
                "pool_recycle": 3600,  # Recycle connections after 1 hour
                "pool_pre_ping": True,  # Verify connections before using
                "connect_args": connect_args,
            })

        self.engine = create_async_engine(database_url, **engine_kwargs)
        self.async_session = async_sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )

    def _detect_database_type(self, url: str) -> str:
        """Detect database type from connection URL."""
        if "postgresql" in url:
            return "postgresql"
        elif "sqlite" in url:
            return "sqlite"
        else:
            # Default to sqlite for backward compatibility
            return "sqlite"

    async def init_db(self):
        """Create tables if they don't exist, and run migrations for schema changes."""
        import sys as _sys
        import os as _os

        project_root = _os.path.abspath(_os.path.join(_os.path.dirname(__file__), "..", ".."))
        if project_root not in _sys.path:
            _sys.path.insert(0, project_root)

        from backend.db.migrations.runner import run_migrations

        try:
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)

            await run_migrations(self.engine)
        except Exception as e:
            db_url = self.database_url
            if "@" in db_url and ":" in db_url:
                try:
                    parsed = urlparse(db_url)
                    if parsed.password:
                        db_url = db_url.replace(f":{parsed.password}@", ":***@")
                except Exception:
                    pass
            raise RuntimeError(
                f"Failed to connect to database.\n"
                f"  URL: {db_url}\n"
                f"  Error: {e}\n\n"
                f"Troubleshooting:\n"
                f"  - Check that DATABASE_URL in your .env file is correct\n"
                f"  - For PostgreSQL, ensure the host is reachable and the password has no unescaped special characters (& * # etc.)\n"
                f"  - For SQLite, ensure the file path is absolute and the directory exists"
            ) from e

    async def close(self):
        """Close the database connection."""
        await self.engine.dispose()

    @asynccontextmanager
    async def session(self):
        """Get an async session context manager."""
        async with self.async_session() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    # =========================================================================
    # Read Operations
    # =========================================================================

    async def get_memory_by_path(
        self, path: str, domain: str = "core"
    ) -> Optional[Dict[str, Any]]:
        """
        Get a memory by its path.

        Returns:
            Memory dict with id, node_uuid, content, priority, disclosure,
            created_at, domain, path — or None if not found.
        """
        async with self.session() as session:
            result = await session.execute(
                select(Memory, Edge, Path)
                .select_from(Path)
                .join(Edge, Path.edge_id == Edge.id)
                .join(
                    Memory,
                    and_(
                        Memory.node_uuid == Edge.child_uuid,
                        Memory.deprecated == False,
                    ),
                )
                .where(Path.domain == domain)
                .where(Path.path == path)
                .order_by(Memory.created_at.desc())
                .limit(1)
            )
            row = result.first()

            if not row:
                return None

            memory, edge, path_obj = row
            return {
                "id": memory.id,
                "node_uuid": edge.child_uuid,
                "content": memory.content,
                "priority": edge.priority,
                "disclosure": edge.disclosure,
                "deprecated": memory.deprecated,
                "created_at": memory.created_at.isoformat()
                if memory.created_at
                else None,
                "domain": path_obj.domain,
                "path": path_obj.path,
            }

    async def get_memory_by_node_uuid(
        self, node_uuid: str
    ) -> Optional[Dict[str, Any]]:
        """Get the current active (non-deprecated) memory for a node."""
        async with self.session() as session:
            result = await session.execute(
                select(Memory)
                .where(Memory.node_uuid == node_uuid, Memory.deprecated == False)
                .order_by(Memory.created_at.desc())
                .limit(1)
            )
            memory = result.scalar_one_or_none()

            if not memory:
                return None

            paths_result = await session.execute(
                select(Path.domain, Path.path)
                .select_from(Path)
                .join(Edge, Path.edge_id == Edge.id)
                .where(Edge.child_uuid == node_uuid)
            )
            paths = [f"{r[0]}://{r[1]}" for r in paths_result.all()]

            return {
                "id": memory.id,
                "node_uuid": node_uuid,
                "content": memory.content,
                "deprecated": memory.deprecated,
                "created_at": memory.created_at.isoformat()
                if memory.created_at
                else None,
                "paths": paths,
            }

    async def get_children(
        self,
        node_uuid: str = ROOT_NODE_UUID,
        context_domain: Optional[str] = None,
        context_path: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get direct children of a node via the edges table.

        When *context_domain* / *context_path* are supplied the returned
        ``path`` for each child is chosen with affinity:
          1. Same domain AND path starts with ``context_path/``
          2. Same domain (any path)
          3. Any path at all
        This ensures the browse UI shows paths that match the caller's
        current navigation context rather than an arbitrary alias.
        """
        async with self.session() as session:
            stmt = (
                select(Edge, Memory)
                .join(
                    Memory,
                    and_(
                        Memory.node_uuid == Edge.child_uuid,
                        Memory.deprecated == False,
                    ),
                )
                .where(Edge.parent_uuid == node_uuid)
                .order_by(Edge.priority.asc(), Edge.name)
            )
            result = await session.execute(stmt)

            prefix = f"{context_path}/" if context_path else None

            children = []
            seen = set()
            for edge, memory in result.all():
                if edge.child_uuid in seen:
                    continue
                seen.add(edge.child_uuid)

                path_result = await session.execute(
                    select(Path).where(Path.edge_id == edge.id)
                )
                all_paths = path_result.scalars().all()

                path_obj = self._pick_best_path(
                    all_paths, context_domain, prefix
                )

                children.append(
                    {
                        "node_uuid": edge.child_uuid,
                        "edge_id": edge.id,
                        "name": edge.name,
                        "domain": path_obj.domain if path_obj else "core",
                        "path": path_obj.path if path_obj else edge.name,
                        "content_snippet": memory.content[:100] + "..."
                        if len(memory.content) > 100
                        else memory.content,
                        "priority": edge.priority,
                        "disclosure": edge.disclosure,
                    }
                )

            return children

    @staticmethod
    def _pick_best_path(
        paths: List[Path],
        context_domain: Optional[str],
        prefix: Optional[str],
    ) -> Optional[Path]:
        """Pick the most contextually relevant path from a list of aliases."""
        if not paths:
            return None
        if len(paths) == 1:
            return paths[0]

        # Tier 1: same domain + path is under the caller's current prefix
        if context_domain and prefix:
            for p in paths:
                if p.domain == context_domain and p.path.startswith(prefix):
                    return p

        # Tier 2: same domain, any path
        if context_domain:
            for p in paths:
                if p.domain == context_domain:
                    return p

        # Tier 3: whatever is available
        return paths[0]

    async def get_all_paths(
        self, domain: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all paths with their node/edge info.
        """
        async with self.session() as session:
            stmt = (
                select(Path, Edge, Memory)
                .select_from(Path)
                .join(Edge, Path.edge_id == Edge.id)
                .join(
                    Memory,
                    and_(
                        Memory.node_uuid == Edge.child_uuid,
                        Memory.deprecated == False,
                    ),
                )
            )

            if domain is not None:
                stmt = stmt.where(Path.domain == domain)

            stmt = stmt.order_by(Path.domain, Path.path)
            result = await session.execute(stmt)

            paths = []
            seen = set()
            for path_obj, edge, memory in result.all():
                key = (path_obj.domain, path_obj.path)
                if key in seen:
                    continue
                seen.add(key)
                paths.append(
                    {
                        "domain": path_obj.domain,
                        "path": path_obj.path,
                        "uri": f"{path_obj.domain}://{path_obj.path}",
                        "name": path_obj.path.rsplit("/", 1)[-1],
                        "priority": edge.priority,
                        "memory_id": memory.id,
                        "node_uuid": edge.child_uuid,
                    }
                )

            return paths

    # =========================================================================
    # State Capture (for changeset recording)
    # =========================================================================

    @staticmethod
    def _serialize_row(obj) -> Dict[str, Any]:
        """Convert an ORM model instance to a plain dict for snapshot storage."""
        d = {}
        for col in obj.__table__.columns:
            val = getattr(obj, col.name)
            if isinstance(val, datetime):
                val = val.isoformat()
            d[col.name] = val
        return d

    async def capture_resource_state(
        self,
        path: str,
        domain: str = "core",
        include_subtree: bool = False,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Capture the current raw row state for all tables related to a URI.

        Returns: {"nodes": [...], "memories": [...], "edges": [...], "paths": [...]}
        Each list contains plain dicts of row data.
        """
        async with self.session() as session:
            if include_subtree:
                return await self._capture_subtree(session, path, domain)

            result = await session.execute(
                select(Path, Edge, Node, Memory)
                .select_from(Path)
                .join(Edge, Path.edge_id == Edge.id)
                .join(Node, Edge.child_uuid == Node.uuid)
                .outerjoin(
                    Memory,
                    and_(
                        Memory.node_uuid == Edge.child_uuid,
                        Memory.deprecated == False,
                    ),
                )
                .where(Path.domain == domain, Path.path == path)
            )
            row = result.first()
            if not row:
                return {"nodes": [], "memories": [], "edges": [], "paths": []}

            path_obj, edge, node, memory = row
            state: Dict[str, list] = {
                "nodes": [self._serialize_row(node)],
                "edges": [self._serialize_row(edge)],
                "paths": [self._serialize_row(path_obj)],
                "memories": [],
            }
            if memory:
                state["memories"].append(self._serialize_row(memory))
            return state

    async def _capture_subtree(
        self, session: AsyncSession, path: str, domain: str
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Capture state for a path and all its descendants."""
        safe_path = (
            path.replace("\\", "\\\\")
            .replace("%", "\\%")
            .replace("_", "\\_")
        )
        result = await session.execute(
            select(Path, Edge, Node, Memory)
            .select_from(Path)
            .join(Edge, Path.edge_id == Edge.id)
            .join(Node, Edge.child_uuid == Node.uuid)
            .outerjoin(
                Memory,
                and_(
                    Memory.node_uuid == Edge.child_uuid,
                    Memory.deprecated == False,
                ),
            )
            .where(
                Path.domain == domain,
                or_(
                    Path.path == path,
                    Path.path.like(f"{safe_path}/%", escape="\\"),
                ),
            )
        )
        rows = result.all()
        if not rows:
            return {"nodes": [], "memories": [], "edges": [], "paths": []}

        seen_nodes = set()
        seen_edges = set()
        seen_memories = set()
        state: Dict[str, list] = {"nodes": [], "memories": [], "edges": [], "paths": []}

        for path_obj, edge, node, memory in rows:
            state["paths"].append(self._serialize_row(path_obj))
            if edge.id not in seen_edges:
                seen_edges.add(edge.id)
                state["edges"].append(self._serialize_row(edge))
            if node.uuid not in seen_nodes:
                seen_nodes.add(node.uuid)
                state["nodes"].append(self._serialize_row(node))
            if memory and memory.id not in seen_memories:
                seen_memories.add(memory.id)
                state["memories"].append(self._serialize_row(memory))

        return state

    async def capture_rows(
        self, table: str, pks: List[Any]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Capture specific rows by primary key (for re-querying after mutation).

        Returns: {table: [row_dict, ...]}
        """
        if not pks:
            return {table: []}

        model = {"nodes": Node, "memories": Memory, "edges": Edge, "paths": Path}[table]

        async with self.session() as session:
            if table == "paths":
                # Composite PK (domain, path) — pks is a list of (domain, path) tuples
                conditions = [
                    and_(Path.domain == d, Path.path == p) for d, p in pks
                ]
                result = await session.execute(
                    select(model).where(or_(*conditions))
                )
            else:
                pk_col = {"nodes": Node.uuid, "memories": Memory.id, "edges": Edge.id}[table]
                result = await session.execute(
                    select(model).where(pk_col.in_(pks))
                )
            return {table: [self._serialize_row(r) for r in result.scalars().all()]}

    # =========================================================================
    # Create Operations
    # =========================================================================

    async def create_memory(
        self,
        parent_path: str,
        content: str,
        priority: int,
        title: Optional[str] = None,
        disclosure: Optional[str] = None,
        domain: str = "core",
    ) -> Dict[str, Any]:
        """
        Create a new memory under a parent path.

        Creates: Node → Memory → Edge (parent→child) → Path.
        """
        async with self.session() as session:
            # Resolve parent node UUID
            if parent_path:
                parent_result = await session.execute(
                    select(Edge.child_uuid)
                    .select_from(Path)
                    .join(Edge, Path.edge_id == Edge.id)
                    .where(Path.domain == domain)
                    .where(Path.path == parent_path)
                )
                parent_row = parent_result.first()
                if not parent_row:
                    raise ValueError(
                        f"Parent '{domain}://{parent_path}' does not exist. "
                        f"Create the parent first, or use '{domain}://' as root."
                    )
                parent_uuid = parent_row[0]
            else:
                parent_uuid = ROOT_NODE_UUID

            # Determine final path
            if title:
                final_path = f"{parent_path}/{title}" if parent_path else title
            else:
                next_num = await self._get_next_child_number(session, parent_uuid)
                final_path = (
                    f"{parent_path}/{next_num}" if parent_path else str(next_num)
                )

            # Check path collision
            existing = await session.execute(
                select(Path).where(Path.domain == domain).where(Path.path == final_path)
            )
            if existing.scalar_one_or_none():
                raise ValueError(f"Path '{domain}://{final_path}' already exists")

            # Create node
            new_uuid = str(uuid_lib.uuid4())
            session.add(Node(uuid=new_uuid))

            # Create memory
            memory = Memory(content=content, node_uuid=new_uuid)
            session.add(memory)
            await session.flush()

            # Create edge
            edge_name = title if title else final_path.rsplit("/", 1)[-1]
            edge = Edge(
                parent_uuid=parent_uuid,
                child_uuid=new_uuid,
                name=edge_name,
                priority=priority,
                disclosure=disclosure,
            )
            session.add(edge)
            await session.flush()

            # Create path
            session.add(Path(domain=domain, path=final_path, edge_id=edge.id))

            return {
                "id": memory.id,
                "node_uuid": new_uuid,
                "domain": domain,
                "path": final_path,
                "uri": f"{domain}://{final_path}",
                "priority": priority,
            }

    async def _get_next_child_number(
        self, session: AsyncSession, parent_uuid: str
    ) -> int:
        """Get the next numeric name for auto-naming under a parent node."""
        result = await session.execute(
            select(Edge.name).where(Edge.parent_uuid == parent_uuid)
        )
        max_num = 0
        for (name,) in result.all():
            try:
                num = int(name)
                max_num = max(max_num, num)
            except ValueError:
                pass
        return max_num + 1

    # =========================================================================
    # Update Operations
    # =========================================================================

    async def update_memory(
        self,
        path: str,
        content: Optional[str] = None,
        priority: Optional[int] = None,
        disclosure: Optional[str] = None,
        domain: str = "core",
    ) -> Dict[str, Any]:
        """
        Update a memory.

        Content change → new Memory row with the same node_uuid (no path repointing).
        Metadata change → update the Edge directly.
        """
        if content is None and priority is None and disclosure is None:
            raise ValueError(
                f"No update fields provided for '{domain}://{path}'. "
                "At least one of content, priority, or disclosure must be set."
            )

        async with self.session() as session:
            result = await session.execute(
                select(Memory, Edge, Path)
                .select_from(Path)
                .join(Edge, Path.edge_id == Edge.id)
                .join(
                    Memory,
                    and_(
                        Memory.node_uuid == Edge.child_uuid,
                        Memory.deprecated == False,
                    ),
                )
                .where(Path.domain == domain)
                .where(Path.path == path)
                .order_by(Memory.created_at.desc())
                .limit(1)
            )
            row = result.first()

            if not row:
                raise ValueError(
                    f"Path '{domain}://{path}' not found or memory is deprecated"
                )

            old_memory, edge, path_obj = row
            old_id = old_memory.id
            node_uuid = edge.child_uuid

            # Update edge metadata
            if priority is not None:
                edge.priority = priority
                session.add(edge)
            if disclosure is not None:
                edge.disclosure = disclosure
                session.add(edge)

            new_memory_id = old_id

            if content is not None:
                # New version on the same node — no path repointing needed
                new_memory = Memory(content=content, node_uuid=node_uuid)
                session.add(new_memory)
                await session.flush()
                new_memory_id = new_memory.id

                # Deprecate all currently active old memories for this node (cleans up any dirty data)
                await session.execute(
                    update(Memory)
                    .where(
                        and_(
                            Memory.node_uuid == node_uuid,
                            Memory.deprecated == False,
                            Memory.id != new_memory_id,
                        )
                    )
                    .values(deprecated=True, migrated_to=new_memory_id)
                )

            if content is None:
                session.add(path_obj)

            return {
                "domain": domain,
                "path": path,
                "uri": f"{domain}://{path}",
                "old_memory_id": old_id,
                "new_memory_id": new_memory_id,
                "node_uuid": node_uuid,
            }

    async def rollback_to_memory(
        self, path: str, target_memory_id: int, domain: str = "core"
    ) -> Dict[str, Any]:
        """
        Rollback a path to point to a specific memory version.

        In the graph model this simply deprecates the current Memory row
        and un-deprecates the target — no path repointing needed.
        """
        async with self.session() as session:
            path_node_result = await session.execute(
                select(Edge.child_uuid)
                .select_from(Path)
                .join(Edge, Path.edge_id == Edge.id)
                .where(Path.domain == domain)
                .where(Path.path == path)
            )
            path_node_uuid = path_node_result.scalar_one_or_none()
            if path_node_uuid is None:
                raise ValueError(f"Path '{domain}://{path}' not found")

            target_row = await session.execute(
                select(Memory).where(Memory.id == target_memory_id)
            )
            target_memory = target_row.scalar_one_or_none()
            if not target_memory:
                raise ValueError(f"Target memory ID {target_memory_id} not found")
            if target_memory.node_uuid != path_node_uuid:
                raise ValueError(
                    f"Target memory ID {target_memory_id} does not belong to "
                    f"'{domain}://{path}'"
                )

            await session.execute(
                update(Memory)
                .where(
                    and_(
                        Memory.node_uuid == path_node_uuid,
                        Memory.deprecated == False,
                        Memory.id != target_memory_id,
                    )
                )
                .values(deprecated=True, migrated_to=target_memory_id)
            )

            await session.execute(
                update(Memory)
                .where(Memory.id == target_memory_id)
                .values(deprecated=False, migrated_to=None)
            )

            return {
                "domain": domain,
                "path": path,
                "uri": f"{domain}://{path}",
                "restored_memory_id": target_memory_id,
            }

    # =========================================================================
    # Path / Edge Operations
    # =========================================================================

    async def add_path(
        self,
        new_path: str,
        target_path: str,
        new_domain: str = "core",
        target_domain: str = "core",
        priority: int = 0,
        disclosure: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create an alias path pointing to the same node as target_path.

        Also cascades: automatically creates sub-paths for all descendants.
        """
        async with self.session() as session:
            # Resolve target → node_uuid
            target_result = await session.execute(
                select(Edge.child_uuid)
                .select_from(Path)
                .join(Edge, Path.edge_id == Edge.id)
                .where(Path.domain == target_domain)
                .where(Path.path == target_path)
            )
            target_row = target_result.first()
            if not target_row:
                raise ValueError(
                    f"Target path '{target_domain}://{target_path}' not found"
                )
            target_node_uuid = target_row[0]

            # Resolve new_path's parent → node_uuid
            if "/" in new_path:
                parent_path = new_path.rsplit("/", 1)[0]
                parent_result = await session.execute(
                    select(Edge.child_uuid)
                    .select_from(Path)
                    .join(Edge, Path.edge_id == Edge.id)
                    .where(Path.domain == new_domain)
                    .where(Path.path == parent_path)
                )
                parent_row = parent_result.first()
                if not parent_row:
                    raise ValueError(
                        f"Parent '{new_domain}://{parent_path}' does not exist. "
                        f"Create the parent first, or use a shallower alias path."
                    )
                parent_uuid = parent_row[0]
            else:
                parent_uuid = ROOT_NODE_UUID

            # Check collision
            existing = await session.execute(
                select(Path)
                .where(Path.domain == new_domain)
                .where(Path.path == new_path)
            )
            if existing.scalar_one_or_none():
                raise ValueError(f"Path '{new_domain}://{new_path}' already exists")

            # Cycle check: reject if target_node is an ancestor of parent
            if await self._would_create_cycle(
                session, parent_uuid, target_node_uuid
            ):
                raise ValueError(
                    f"Cannot create alias '{new_domain}://{new_path}': "
                    f"target node is an ancestor of the destination parent, "
                    f"which would create a cycle in the graph."
                )

            # Get or create edge
            edge_name = new_path.rsplit("/", 1)[-1]
            edge_result = await session.execute(
                select(Edge).where(
                    Edge.parent_uuid == parent_uuid,
                    Edge.child_uuid == target_node_uuid,
                )
            )
            edge = edge_result.scalar_one_or_none()
            edge_created = edge is None

            if edge_created:
                edge = Edge(
                    parent_uuid=parent_uuid,
                    child_uuid=target_node_uuid,
                    name=edge_name,
                    priority=priority,
                    disclosure=disclosure,
                )
                session.add(edge)
                await session.flush()

            # Create the primary path
            session.add(Path(domain=new_domain, path=new_path, edge_id=edge.id))

            # Cascade: create sub-paths for all descendants
            await self._cascade_create_paths(
                session, target_node_uuid, new_domain, new_path
            )

            return {
                "new_uri": f"{new_domain}://{new_path}",
                "target_uri": f"{target_domain}://{target_path}",
                "node_uuid": target_node_uuid,
                "edge_id": edge.id,
                "edge_created": edge_created,
            }

    async def _would_create_cycle(
        self,
        session: AsyncSession,
        parent_uuid: str,
        child_uuid: str,
    ) -> bool:
        """Check if adding edge parent_uuid->child_uuid would create a cycle.

        Returns True if child_uuid can already reach parent_uuid by
        following existing edges downward (parent->child direction),
        or if the two UUIDs are identical (self-loop).
        """
        if parent_uuid == ROOT_NODE_UUID:
            return False
        if parent_uuid == child_uuid:
            return True

        visited = {child_uuid}
        queue = [child_uuid]
        while queue:
            current = queue.pop(0)
            result = await session.execute(
                select(Edge.child_uuid).where(Edge.parent_uuid == current)
            )
            for (next_uuid,) in result.all():
                if next_uuid == parent_uuid:
                    return True
                if next_uuid not in visited:
                    visited.add(next_uuid)
                    queue.append(next_uuid)
        return False

    async def _cascade_create_paths(
        self,
        session: AsyncSession,
        node_uuid: str,
        domain: str,
        base_path: str,
        _visited: Optional[set] = None,
    ):
        """Recursively create path entries for all descendants of a node."""
        if _visited is None:
            _visited = set()
        if node_uuid in _visited:
            return
        _visited.add(node_uuid)
        try:
            result = await session.execute(
                select(Edge).where(Edge.parent_uuid == node_uuid)
            )
            child_edges = result.scalars().all()

            for child_edge in child_edges:
                child_path = f"{base_path}/{child_edge.name}"

                existing = await session.execute(
                    select(Path)
                    .where(Path.domain == domain)
                    .where(Path.path == child_path)
                )
                if not existing.scalar_one_or_none():
                    session.add(
                        Path(domain=domain, path=child_path, edge_id=child_edge.id)
                    )

                await self._cascade_create_paths(
                    session, child_edge.child_uuid, domain, child_path, _visited
                )
        finally:
            _visited.remove(node_uuid)

    async def _delete_edge_if_pathless(
        self,
        session: AsyncSession,
        edge: Edge,
        *,
        snapshot: Optional[Dict[str, list]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Delete an edge if it has no remaining path references.

        Returns edge info dict if deleted, None otherwise.
        When *snapshot* is provided, appends the serialized edge row
        before deletion so the caller can record it in the changeset.
        """
        remaining = await session.execute(
            select(func.count())
            .select_from(Path)
            .where(Path.edge_id == edge.id)
        )
        if remaining.scalar() == 0:
            if snapshot is not None:
                snapshot.setdefault("edges", []).append(self._serialize_row(edge))
            info = {
                "edge_id": edge.id,
                "parent_uuid": edge.parent_uuid,
                "child_uuid": edge.child_uuid,
                "name": edge.name,
                "priority": edge.priority,
                "disclosure": edge.disclosure,
            }
            await session.delete(edge)
            return info
        return None

    async def _gc_unreachable_node_edges(
        self,
        session: AsyncSession,
        node_uuid: str,
        *,
        snapshot: Optional[Dict[str, list]] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """GC all edges (and dangling paths) around an unreachable node.

        If the node still has at least one reachable path, returns
        immediately with empty lists.

        Otherwise:
        - Incoming edges are all pathless by definition (if any had a
          path, the node would be reachable), so they are deleted.
        - Outgoing edges are force-deleted along with any remaining
          path references, since those paths are dangling (their
          parent node is unreachable).
        - Active memories on this node are marked deprecated (with
          migrated_to=None to distinguish from version-chain
          deprecation).  restore_path already handles un-deprecating.

        When *snapshot* is provided, serialized row data is appended
        before each deletion for changeset recording.

        Returns {"edges": [...], "paths": [...], "deprecated_memories": [...]}.
        """
        remaining = await session.execute(
            select(func.count())
            .select_from(Path)
            .join(Edge, Path.edge_id == Edge.id)
            .where(Edge.child_uuid == node_uuid)
        )
        if remaining.scalar() > 0:
            return {"edges": [], "paths": [], "deprecated_memories": []}

        deleted_edges: List[Dict[str, Any]] = []
        deleted_paths: List[Dict[str, Any]] = []

        incoming_result = await session.execute(
            select(Edge).where(Edge.child_uuid == node_uuid)
        )
        for edge in incoming_result.scalars().all():
            info = await self._delete_edge_if_pathless(
                session, edge, snapshot=snapshot
            )
            if info:
                deleted_edges.append(info)

        outgoing_result = await session.execute(
            select(Edge).where(Edge.parent_uuid == node_uuid)
        )
        for edge in outgoing_result.scalars().all():
            paths_result = await session.execute(
                select(Path).where(Path.edge_id == edge.id)
            )
            paths = paths_result.scalars().all()
            if paths:
                mem_result = await session.execute(
                    select(Memory)
                    .where(
                        Memory.node_uuid == edge.child_uuid, Memory.deprecated == False
                    )
                )
                memory = mem_result.scalar_one_or_none()
                mem_id = memory.id if memory else None
                if memory and snapshot is not None:
                    snapshot.setdefault("memories", []).append(
                        self._serialize_row(memory)
                    )

                for p in paths:
                    if snapshot is not None:
                        snapshot.setdefault("paths", []).append(
                            self._serialize_row(p)
                        )
                    deleted_paths.append(
                        {
                            "domain": p.domain,
                            "path": p.path,
                            "edge_id": p.edge_id,
                            "uri": f"{p.domain}://{p.path}",
                            "node_uuid": edge.child_uuid,
                            "memory_id": mem_id,
                            "priority": edge.priority,
                            "disclosure": edge.disclosure,
                        }
                    )
                    await session.delete(p)

            if snapshot is not None:
                snapshot.setdefault("edges", []).append(
                    self._serialize_row(edge)
                )
            deleted_edges.append(
                {
                    "edge_id": edge.id,
                    "parent_uuid": edge.parent_uuid,
                    "child_uuid": edge.child_uuid,
                    "name": edge.name,
                    "priority": edge.priority,
                    "disclosure": edge.disclosure,
                }
            )
            await session.delete(edge)

        # Auto-deprecate active memories on this now-unreachable node.
        active_mems = await session.execute(
            select(Memory).where(
                Memory.node_uuid == node_uuid,
                Memory.deprecated == False,
            )
        )
        deprecated_ids = []
        for mem in active_mems.scalars().all():
            if snapshot is not None:
                snapshot.setdefault("memories", []).append(
                    self._serialize_row(mem)
                )
            deprecated_ids.append(mem.id)

        if deprecated_ids:
            await session.execute(
                update(Memory)
                .where(Memory.id.in_(deprecated_ids))
                .values(deprecated=True)
            )

        return {
            "edges": deleted_edges,
            "paths": deleted_paths,
            "deprecated_memories": deprecated_ids,
        }

    async def _gc_empty_node(
        self,
        session: AsyncSession,
        node_uuid: str,
    ) -> Optional[Dict[str, Any]]:
        """GC a node that has lost all its Memory rows.

        Removes any surviving edges (incoming + outgoing), their path
        references, and the Node row itself.  Returns None if the node
        still has memories or is the sentinel root.
        """
        if node_uuid == ROOT_NODE_UUID:
            return None

        remaining = await session.execute(
            select(func.count())
            .select_from(Memory)
            .where(Memory.node_uuid == node_uuid)
        )
        if remaining.scalar() > 0:
            return None

        deleted_edges: List[Dict[str, Any]] = []
        deleted_paths: List[Dict[str, Any]] = []

        edges_result = await session.execute(
            select(Edge).where(
                or_(Edge.parent_uuid == node_uuid, Edge.child_uuid == node_uuid)
            )
        )
        for edge in edges_result.scalars().all():
            paths_result = await session.execute(
                select(Path).where(Path.edge_id == edge.id)
            )
            for p in paths_result.scalars().all():
                deleted_paths.append({
                    "domain": p.domain,
                    "path": p.path,
                    "edge_id": p.edge_id,
                })
                await session.delete(p)

            deleted_edges.append({
                "edge_id": edge.id,
                "parent_uuid": edge.parent_uuid,
                "child_uuid": edge.child_uuid,
                "name": edge.name,
                "priority": edge.priority,
                "disclosure": edge.disclosure,
            })
            await session.delete(edge)

        await session.execute(delete(Node).where(Node.uuid == node_uuid))

        return {
            "deleted_node_uuid": node_uuid,
            "deleted_edges": deleted_edges,
            "deleted_paths": deleted_paths,
        }

    async def remove_path(
        self, path: str, domain: str = "core"
    ) -> Dict[str, Any]:
        """
        Remove a path and its sub-paths with orphan prevention.

        Pre-flight safety: refuses to proceed if any direct child of the
        target node would become unreachable (no surviving paths outside
        the deletion set).

        If the target node loses its last reachable path, pathless incoming/
        outgoing edges around the target node are pruned so dead graph
        fragments do not linger. The target node's memory is preserved but
        becomes an orphan (recoverable via the review interface).

        Raises:
            ValueError: If the path does not exist, or if deletion would
                create unreachable child nodes.
        """
        async with self.session() as session:
            # 1. Find target path + edge
            result = await session.execute(
                select(Path, Edge)
                .join(Edge, Path.edge_id == Edge.id)
                .where(Path.domain == domain)
                .where(Path.path == path)
            )
            row = result.first()
            if not row:
                raise ValueError(f"Path '{domain}://{path}' not found")

            _, target_edge = row
            target_node_uuid = target_edge.child_uuid

            safe_path = (
                path.replace("\\", "\\\\")
                .replace("%", "\\%")
                .replace("_", "\\_")
            )

            # 2. Pre-flight orphan check: every direct child must have
            #    at least one path that would survive this deletion.
            child_edges_result = await session.execute(
                select(Edge).where(Edge.parent_uuid == target_node_uuid)
            )
            child_edges = child_edges_result.scalars().all()

            would_orphan = []
            for child_edge in child_edges:
                surviving = await session.execute(
                    select(func.count())
                    .select_from(Path)
                    .join(Edge, Path.edge_id == Edge.id)
                    .where(Edge.child_uuid == child_edge.child_uuid)
                    .where(
                        not_(
                            and_(
                                Path.domain == domain,
                                Path.path.like(f"{safe_path}/%", escape="\\"),
                            )
                        )
                    )
                )
                if surviving.scalar() == 0:
                    would_orphan.append(child_edge)

            if would_orphan:
                details = ", ".join(
                    f"'{e.name}' (node: {e.child_uuid[:8]}…)"
                    for e in would_orphan
                )
                raise ValueError(
                    f"Cannot remove '{domain}://{path}': "
                    f"the following child node(s) would become unreachable: "
                    f"{details}. "
                    f"Create alternative paths for these children first, "
                    f"or remove them explicitly."
                )

            # 3. Collect subtree paths (target + descendants by prefix)
            subtree_result = await session.execute(
                select(Path, Edge, Memory)
                .join(Edge, Path.edge_id == Edge.id)
                .outerjoin(
                    Memory,
                    and_(
                        Memory.node_uuid == Edge.child_uuid,
                        Memory.deprecated == False,
                    ),
                )
                .where(Path.domain == domain)
                .where(
                    or_(
                        Path.path == path,
                        Path.path.like(f"{safe_path}/%", escape="\\"),
                    )
                )
            )
            subtree_rows = subtree_result.all()

            # 4. Snapshot info before deletion
            deleted_paths_info = []
            snapshot: Dict[str, list] = {"edges": [], "paths": [], "memories": []}
            seen_edges: set = set()
            seen_memories: set = set()

            for p, e, memory in subtree_rows:
                deleted_paths_info.append(
                    {
                        "domain": p.domain,
                        "path": p.path,
                        "edge_id": p.edge_id,
                        "uri": f"{p.domain}://{p.path}",
                        "node_uuid": e.child_uuid,
                        "memory_id": memory.id if memory else None,
                        "priority": e.priority,
                        "disclosure": e.disclosure,
                    }
                )
                snapshot["paths"].append(self._serialize_row(p))
                if e.id not in seen_edges:
                    seen_edges.add(e.id)
                    snapshot["edges"].append(self._serialize_row(e))
                if memory and memory.id not in seen_memories:
                    seen_memories.add(memory.id)
                    snapshot["memories"].append(self._serialize_row(memory))

            # 5. Delete all sub-paths
            for p, _, _ in subtree_rows:
                await session.delete(p)
            await session.flush()

            # 6. Edge cleanup
            #    - Drop the entry edge if it lost all its path references.
            #    - If target node is now completely unreachable, GC all
            #      dangling edges and their orphaned path references.
            #    The snapshot accumulator captures serialized rows
            #    before each deletion for changeset recording.
            deleted_edges = []

            info = await self._delete_edge_if_pathless(
                session, target_edge, snapshot=snapshot
            )
            if info:
                deleted_edges.append(info)

            gc_result = await self._gc_unreachable_node_edges(
                session, target_node_uuid, snapshot=snapshot
            )
            deleted_edges.extend(gc_result["edges"])
            deleted_paths_info.extend(gc_result["paths"])

            return {
                "removed_uri": f"{domain}://{path}",
                "node_uuid": target_node_uuid,
                "deleted_paths": deleted_paths_info,
                "deleted_edges": deleted_edges,
                "snapshot_before": snapshot,
            }

    async def undo_remove_path(
        self,
        deleted_paths: List[Dict[str, Any]],
        deleted_edges: List[Dict[str, Any]],
    ) -> None:
        """
        Manually restore originally deleted paths and edges.
        Used to rollback a remove_path operation.
        """
        async with self.session() as session:
            # 1. Restore edges
            for e_info in deleted_edges:
                edge = Edge(
                    id=e_info["edge_id"],
                    parent_uuid=e_info["parent_uuid"],
                    child_uuid=e_info["child_uuid"],
                    name=e_info["name"],
                    priority=e_info.get("priority", 0),
                    disclosure=e_info.get("disclosure"),
                )
                session.add(edge)
            
            await session.flush()

            # 2. Restore paths
            for p_info in deleted_paths:
                path_obj = Path(
                    domain=p_info["domain"],
                    path=p_info["path"],
                    edge_id=p_info["edge_id"],
                )
                session.add(path_obj)

    async def restore_path(
        self,
        path: str,
        domain: str,
        node_uuid: str,
        parent_uuid: Optional[str] = None,
        priority: int = 0,
        disclosure: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Restore a path pointing to a node (used for rollback of delete).

        Creates/finds the edge from the parent to the target node,
        creates the path entry, and ensures the node has an active memory.
        """
        async with self.session() as session:
            node_result = await session.execute(
                select(Node).where(Node.uuid == node_uuid)
            )
            if not node_result.scalar_one_or_none():
                raise ValueError(f"Node '{node_uuid}' not found")

            active_mem = await session.execute(
                select(Memory)
                .where(Memory.node_uuid == node_uuid, Memory.deprecated == False)
            )
            if not active_mem.scalar_one_or_none():
                latest = await session.execute(
                    select(Memory)
                    .where(Memory.node_uuid == node_uuid)
                    .order_by(Memory.created_at.desc())
                    .limit(1)
                )
                latest_mem = latest.scalar_one_or_none()
                if not latest_mem:
                    raise ValueError(
                        f"Node '{node_uuid}' has no memory versions"
                    )
                await session.execute(
                    update(Memory)
                    .where(Memory.id == latest_mem.id)
                    .values(deprecated=False, migrated_to=None)
                )

            # Check collision
            existing = await session.execute(
                select(Path).where(Path.domain == domain).where(Path.path == path)
            )
            if existing.scalar_one_or_none():
                raise ValueError(f"Path '{domain}://{path}' already exists")

            # Determine parent
            if parent_uuid is None:
                if "/" in path:
                    parent_path_str = path.rsplit("/", 1)[0]
                    parent_result = await session.execute(
                        select(Edge.child_uuid)
                        .select_from(Path)
                        .join(Edge, Path.edge_id == Edge.id)
                        .where(Path.domain == domain)
                        .where(Path.path == parent_path_str)
                    )
                    parent_row = parent_result.first()
                    parent_uuid = parent_row[0] if parent_row else ROOT_NODE_UUID
                else:
                    parent_uuid = ROOT_NODE_UUID

            # Get or create edge
            edge_name = path.rsplit("/", 1)[-1]
            edge_result = await session.execute(
                select(Edge).where(
                    Edge.parent_uuid == parent_uuid,
                    Edge.child_uuid == node_uuid,
                )
            )
            edge = edge_result.scalar_one_or_none()

            if not edge:
                edge = Edge(
                    parent_uuid=parent_uuid,
                    child_uuid=node_uuid,
                    name=edge_name,
                    priority=priority,
                    disclosure=disclosure,
                )
                session.add(edge)
                await session.flush()

            session.add(Path(domain=domain, path=path, edge_id=edge.id))

            return {"uri": f"{domain}://{path}", "node_uuid": node_uuid}

    # =========================================================================
    # Search Operations
    # =========================================================================

    async def search(
        self, query: str, limit: int = 10, domain: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Search memories by path and content.
        """
        async with self.session() as session:
            search_pattern = f"%{query}%"

            stmt = (
                select(Memory, Edge, Path)
                .select_from(Path)
                .join(Edge, Path.edge_id == Edge.id)
                .join(
                    Memory,
                    and_(
                        Memory.node_uuid == Edge.child_uuid,
                        Memory.deprecated == False,
                    ),
                )
                .where(
                    or_(
                        Path.path.like(search_pattern),
                        Memory.content.like(search_pattern),
                    )
                )
            )

            if domain is not None:
                stmt = stmt.where(Path.domain == domain)

            stmt = stmt.order_by(Edge.priority.asc()).limit(limit)
            result = await session.execute(stmt)

            matches = []
            seen_ids = set()

            for memory, edge, path_obj in result.all():
                if memory.id in seen_ids:
                    continue
                seen_ids.add(memory.id)

                content_lower = memory.content.lower()
                query_lower = query.lower()
                pos = content_lower.find(query_lower)

                if pos >= 0:
                    start = max(0, pos - 30)
                    end = min(len(memory.content), pos + len(query) + 30)
                    snippet = "..." + memory.content[start:end] + "..."
                else:
                    snippet = memory.content[:80] + "..."

                matches.append(
                    {
                        "domain": path_obj.domain,
                        "path": path_obj.path,
                        "uri": f"{path_obj.domain}://{path_obj.path}",
                        "name": path_obj.path.rsplit("/", 1)[-1],
                        "snippet": snippet,
                        "priority": edge.priority,
                    }
                )

            return matches

    # =========================================================================
    # Recent Memories
    # =========================================================================

    async def get_recent_memories(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get the most recently created/updated non-deprecated memories
        that have at least one path.
        """
        async with self.session() as session:
            result = await session.execute(
                select(Memory, Edge, Path)
                .select_from(Path)
                .join(Edge, Path.edge_id == Edge.id)
                .join(
                    Memory,
                    and_(
                        Memory.node_uuid == Edge.child_uuid,
                        Memory.deprecated == False,
                    ),
                )
                .order_by(Memory.created_at.desc())
            )

            seen = set()
            memories = []

            for memory, edge, path_obj in result.all():
                if memory.id in seen:
                    continue
                seen.add(memory.id)

                memories.append(
                    {
                        "memory_id": memory.id,
                        "uri": f"{path_obj.domain}://{path_obj.path}",
                        "priority": edge.priority,
                        "disclosure": edge.disclosure,
                        "created_at": memory.created_at.isoformat()
                        if memory.created_at
                        else None,
                    }
                )

                if len(memories) >= limit:
                    break

            return memories

    # =========================================================================
    # Deprecated Memory Operations (for human's review)
    # =========================================================================

    async def get_memory_by_id(self, memory_id: int) -> Optional[Dict[str, Any]]:
        """
        Get a memory by its ID (including deprecated ones).
        """
        async with self.session() as session:
            result = await session.execute(select(Memory).where(Memory.id == memory_id))
            memory = result.scalar_one_or_none()

            if not memory:
                return None

            paths = []
            if memory.node_uuid:
                paths_result = await session.execute(
                    select(Path.domain, Path.path)
                    .select_from(Path)
                    .join(Edge, Path.edge_id == Edge.id)
                    .where(Edge.child_uuid == memory.node_uuid)
                )
                paths = [f"{r[0]}://{r[1]}" for r in paths_result.all()]

            return {
                "memory_id": memory.id,
                "node_uuid": memory.node_uuid,
                "content": memory.content,
                "created_at": memory.created_at.isoformat()
                if memory.created_at
                else None,
                "deprecated": memory.deprecated,
                "migrated_to": memory.migrated_to,
                "paths": paths,
            }

    async def get_deprecated_memories(self) -> List[Dict[str, Any]]:
        """
        Get all deprecated memories for human's review.
        """
        async with self.session() as session:
            result = await session.execute(
                select(Memory)
                .where(Memory.deprecated == True)
                .order_by(Memory.created_at.desc())
            )

            return [
                {
                    "id": m.id,
                    "content_snippet": m.content[:200] + "..."
                    if len(m.content) > 200
                    else m.content,
                    "migrated_to": m.migrated_to,
                    "created_at": m.created_at.isoformat()
                    if m.created_at
                    else None,
                }
                for m in result.scalars().all()
            ]

    async def _resolve_migration_chain(
        self, session: AsyncSession, start_id: int, max_hops: int = 50
    ) -> Optional[Dict[str, Any]]:
        """Follow the migrated_to chain to the final target."""
        current_id = start_id
        for _ in range(max_hops):
            result = await session.execute(
                select(Memory).where(Memory.id == current_id)
            )
            memory = result.scalar_one_or_none()
            if not memory:
                return None
            if memory.migrated_to is None:
                paths = []
                if memory.node_uuid:
                    paths_result = await session.execute(
                        select(Path.domain, Path.path)
                        .select_from(Path)
                        .join(Edge, Path.edge_id == Edge.id)
                        .where(Edge.child_uuid == memory.node_uuid)
                    )
                    paths = [f"{r[0]}://{r[1]}" for r in paths_result.all()]
                return {
                    "id": memory.id,
                    "content": memory.content,
                    "content_snippet": memory.content[:200] + "..."
                    if len(memory.content) > 200
                    else memory.content,
                    "created_at": memory.created_at.isoformat()
                    if memory.created_at
                    else None,
                    "deprecated": memory.deprecated,
                    "paths": paths,
                }
            current_id = memory.migrated_to
        return None

    async def get_all_orphan_memories(self) -> List[Dict[str, Any]]:
        """
        Get all orphan memories (deprecated=True).

        Two sub-categories (distinguished by migrated_to):
        - "deprecated": migrated_to is set — old version replaced by update_memory.
        - "orphaned": migrated_to is NULL — node lost all paths.
        """
        async with self.session() as session:
            orphans = []

            result = await session.execute(
                select(Memory)
                .where(Memory.deprecated == True)
                .order_by(Memory.created_at.desc())
            )

            for memory in result.scalars().all():
                category = "deprecated" if memory.migrated_to else "orphaned"
                item = {
                    "id": memory.id,
                    "content_snippet": memory.content[:200] + "..."
                    if len(memory.content) > 200
                    else memory.content,
                    "created_at": memory.created_at.isoformat()
                    if memory.created_at
                    else None,
                    "deprecated": True,
                    "migrated_to": memory.migrated_to,
                    "category": category,
                    "migration_target": None,
                }

                if memory.migrated_to:
                    target = await self._resolve_migration_chain(
                        session, memory.migrated_to
                    )
                    if target:
                        item["migration_target"] = {
                            "id": target["id"],
                            "paths": target["paths"],
                            "content_snippet": target["content_snippet"],
                        }

                orphans.append(item)

            return orphans

    async def get_orphan_detail(self, memory_id: int) -> Optional[Dict[str, Any]]:
        """
        Get full detail of an orphan memory for content viewing and diff.
        """
        async with self.session() as session:
            result = await session.execute(select(Memory).where(Memory.id == memory_id))
            memory = result.scalar_one_or_none()
            if not memory:
                return None

            if not memory.deprecated:
                category = "active"
            elif memory.migrated_to:
                category = "deprecated"
            else:
                category = "orphaned"

            detail = {
                "id": memory.id,
                "content": memory.content,
                "created_at": memory.created_at.isoformat()
                if memory.created_at
                else None,
                "deprecated": memory.deprecated,
                "migrated_to": memory.migrated_to,
                "category": category,
                "migration_target": None,
            }

            if memory.migrated_to:
                target = await self._resolve_migration_chain(
                    session, memory.migrated_to
                )
                if target:
                    detail["migration_target"] = {
                        "id": target["id"],
                        "content": target["content"],
                        "paths": target["paths"],
                        "created_at": target["created_at"],
                    }

            return detail

    async def permanently_delete_memory(
        self, memory_id: int, *, require_orphan: bool = False
    ) -> Dict[str, Any]:
        """
        Permanently delete a memory version (human only).

        Repairs the version chain by skipping over the deleted node.
        If this was the last memory for the node, GCs the node row
        and any surviving edges/paths (bug-resilient cleanup).
        """
        async with self.session() as session:
            target_result = await session.execute(
                select(Memory).where(Memory.id == memory_id)
            )
            target = target_result.scalar_one_or_none()
            if not target:
                raise ValueError(f"Memory ID {memory_id} not found")

            if require_orphan and not target.deprecated:
                raise PermissionError(
                    f"Memory {memory_id} is not an orphan "
                    f"(deprecated=False). Deletion aborted."
                )

            was_active = not target.deprecated
            node_uuid = target.node_uuid
            successor_id = target.migrated_to

            # Repair chain: predecessors skip over deleted node
            await session.execute(
                update(Memory)
                .where(Memory.migrated_to == memory_id)
                .values(migrated_to=successor_id)
            )

            # Delete the memory row
            result = await session.execute(delete(Memory).where(Memory.id == memory_id))

            if result.rowcount == 0:
                raise ValueError(f"Memory ID {memory_id} not found")

            response: Dict[str, Any] = {
                "deleted_memory_id": memory_id,
                "chain_repaired_to": successor_id,
            }

            if node_uuid:
                if was_active:
                    active_count_result = await session.execute(
                        select(func.count())
                        .select_from(Memory)
                        .where(
                            Memory.node_uuid == node_uuid,
                            Memory.deprecated == False,
                        )
                    )
                    if active_count_result.scalar() == 0:
                        fallback_result = await session.execute(
                            select(Memory.id)
                            .where(
                                Memory.node_uuid == node_uuid,
                                Memory.deprecated == True,
                            )
                            .order_by(Memory.created_at.desc(), Memory.id.desc())
                            .limit(1)
                        )
                        fallback_id = fallback_result.scalar_one_or_none()
                        if fallback_id is not None:
                            await session.execute(
                                update(Memory)
                                .where(Memory.id == fallback_id)
                                .values(deprecated=False, migrated_to=None)
                            )
                            response["fallback_restored_memory_id"] = fallback_id

                gc_result = await self._gc_empty_node(session, node_uuid)
                if gc_result:
                    response["node_gc"] = gc_result

            return response


# =============================================================================
# Global Singleton
# =============================================================================

_db_client: Optional[SQLiteClient] = None


def get_db_client() -> SQLiteClient:
    """Get the global database client instance."""
    global _db_client
    if _db_client is None:
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise ValueError(
                "DATABASE_URL environment variable is not set. Please check your .env file."
            )
        _db_client = SQLiteClient(database_url)
    return _db_client


async def close_db_client():
    """Close the global database client connection."""
    global _db_client
    if _db_client:
        await _db_client.close()
        _db_client = None
