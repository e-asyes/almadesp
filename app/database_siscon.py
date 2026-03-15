from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from app.config import get_settings

settings = get_settings()

siscon_engine = create_async_engine(
    settings.async_siscon_url,
    echo=True,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
)

siscon_session = async_sessionmaker(
    siscon_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_siscon_db() -> AsyncSession:
    async with siscon_session() as session:
        try:
            yield session
        finally:
            await session.close()
