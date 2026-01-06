#########################################
# POSTGRESQL
#########################################

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import text
from contextlib import asynccontextmanager


# #AZURE
# host = os.environ.get("HOST_POSTGRESQL")
# database = 'postgres'
# username = os.environ.get("username_POSTGRESQL")
# password = os.environ.get("password_POSTGRESQL")
# port = "5432"

# DATABASE_URI = f"postgresql+asyncpg://{username}:{password}@{host}:{port}/{database}"


#LOCAL DEV
host = "localhost"
database = "aichatbot"
username = "postgres"
password = "ibm11ibm"
port = "5432"

DATABASE_URI = "postgresql+asyncpg://postgres:ibm11ibm@localhost:5432/postgres"


# Async SQLAlchemy engine
async_engine = create_async_engine(
    DATABASE_URI,
    pool_size=10,
    max_overflow=5,
    pool_pre_ping=True,
    pool_recycle=1800,
    echo=False
)

# Async session factory
AsyncSessionLocal = sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False
)

Base = declarative_base()

# --- Context manager with tenant awareness ---
@asynccontextmanager
async def async_session_scope(org_id: int | None = None):
    """Create an async SQLAlchemy session, scoped to a tenant if provided."""
    async with AsyncSessionLocal() as session:
        try:
            if org_id is not None:
                # This tells PostgreSQL which tenant the session belongs to
                await session.execute(text(f"SET app.current_tenant_id = {org_id}"))

            yield session
            await session.commit()

        except Exception as e:
            await session.rollback()
            print(f"[AsyncSession] Rollback due to error: {e}")
            raise
