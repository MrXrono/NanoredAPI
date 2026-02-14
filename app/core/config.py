import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    PROJECT_NAME: str = "NanoredVPN API"
    VERSION: str = "1.5.3"
    API_V1_PREFIX: str = "/api/v1"

    # Database
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://nanored:nanored_secret@nanored-db:5432/nanored_api",
    )

    # Redis
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://nanored-redis:6379/0")

    # JWT
    SECRET_KEY: str = os.getenv("SECRET_KEY", "change-me-in-production-nanored-secret-key-2026")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24  # 24 hours
    ALGORITHM: str = "HS256"

    # Admin credentials (initial)
    ADMIN_USERNAME: str = os.getenv("ADMIN_USERNAME", "admin")
    ADMIN_PASSWORD: str = os.getenv("ADMIN_PASSWORD", "nanored_admin_2026")

    # GeoIP
    GEOIP_DB_PATH: str = "/app/data/GeoLite2-City.mmdb"

    class Config:
        case_sensitive = True


settings = Settings()
