from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from backend.utils.config import settings

def _make_engine():
    return create_async_engine(
        settings.DATABASE_URL,
        echo=False,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        pool_recycle=3600
    )

def _meka_session_factory(engine):
    return async_sessionmaker(engine, expire_on_commit=False)

engine = _make_engine()
AssyncSessionLocal = _meka_session_factory(engine)

class Base(DeclarativeBase):
    pass

async def get_db():
    async with AssyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()

def reinit_db_for_worker():
    global engine, AssyncSessionLocal
    engine = _make_engine()
    AssyncSessionLocal = _meka_session_factory(engine)