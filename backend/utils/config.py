from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    DATABASE_URL: str 
    SECRET_KEY: str
    BOT_TOKEN: str
    ADMIN_TELEGRAM_IDS: list[int] = []
    IPFOXY_API_BASE: str = "https://api.ipfoxy.com"
    ALLOWED_ORIGINS: list[str] = []
    ENVIRONMENT: str
    REDIS_URL: str
    ENCRYPTION_KEY: str
    POSTGRES_PASSWORD: str

    model_config = SettingsConfigDict(env_file=".env")

settings = Settings()