from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from backend.utils.config import settings

engine = create_async_engine(settings.DATABASE_URL, echo=False)
AssyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

async def get_db():
    async with AssyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()