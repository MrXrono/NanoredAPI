import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    PROJECT_NAME: str = "NanoredVPN API"
    VERSION: str = "1.9.0.5"
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

    # Telegram support forum integration (app tickets)
    TELEGRAM_MESSAGE_BOT_TOKEN: str = os.getenv("TELEGRAM_MESSAGE_BOT_TOKEN", "")
    TELEGRAM_SUPPORT_GROUP_ID: int = int(os.getenv("TELEGRAM_SUPPORT_GROUP_ID", "0"))
    TELEGRAM_WEBHOOK_URL: str = os.getenv("TELEGRAM_WEBHOOK_URL", "")
    TELEGRAM_WEBHOOK_SECRET: str = os.getenv("TELEGRAM_WEBHOOK_SECRET", "")
    TELEGRAM_WEBHOOK_DROP_PENDING_UPDATES: bool = os.getenv("TELEGRAM_WEBHOOK_DROP_PENDING_UPDATES", "0").strip() in ("1", "true", "yes", "on")

    # External base URL used to derive webhook URL if TELEGRAM_WEBHOOK_URL is not set.
    # Example: https://api.nanored.top
    PUBLIC_BASE_URL: str = os.getenv("PUBLIC_BASE_URL", "")


    # Remnawave logs ingest
    REMNAWAVE_LOG_INGEST_TOKEN: str = os.getenv("REMNAWAVE_LOG_INGEST_TOKEN", "")
    # GeoIP
    GEOIP_DB_PATH: str = "/app/data/GeoLite2-City.mmdb"

    class Config:
        case_sensitive = True


settings = Settings()
