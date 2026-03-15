from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    azure_pg_url: str
    siscon_pg_url: str

    model_config = {"env_file": ".env", "extra": "ignore"}

    @property
    def async_database_url(self) -> str:
        url = self.azure_pg_url
        if url.startswith("postgresql://"):
            url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
        url = url.replace("?sslmode=require", "").replace("&sslmode=require", "")
        return url

    @property
    def async_siscon_url(self) -> str:
        url = self.siscon_pg_url
        if url.startswith("postgresql://"):
            url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
        return url


@lru_cache
def get_settings() -> Settings:
    return Settings()
