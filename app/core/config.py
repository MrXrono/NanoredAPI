import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    PROJECT_NAME: str = "NanoredVPN API"
    VERSION: str = "1.16.0.28"
    API_V1_PREFIX: str = "/api/v1"

    # Database
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://nanored:nanored_secret@nanored-db:5432/nanored_api",
    )

    DB_POOL_SIZE: int = max(1, int(os.getenv("DB_POOL_SIZE", "3")))
    DB_MAX_OVERFLOW: int = max(0, int(os.getenv("DB_MAX_OVERFLOW", "1")))
    DB_POOL_TIMEOUT_SECONDS: int = max(5, int(os.getenv("DB_POOL_TIMEOUT_SECONDS", "30")))
    DB_POOL_RECYCLE_SECONDS: int = max(60, int(os.getenv("DB_POOL_RECYCLE_SECONDS", "1800")))
    DB_POOL_PRE_PING: bool = os.getenv("DB_POOL_PRE_PING", "1").strip() in ("1", "true", "yes", "on")

    DB_COMMAND_TIMEOUT_SECONDS: int = max(5, int(os.getenv("DB_COMMAND_TIMEOUT_SECONDS", "60")))
    DB_STATEMENT_TIMEOUT_MS: int = max(1000, int(os.getenv("DB_STATEMENT_TIMEOUT_MS", "30000")))
    DB_LOCK_TIMEOUT_MS: int = max(500, int(os.getenv("DB_LOCK_TIMEOUT_MS", "5000")))
    DB_IDLE_IN_TX_TIMEOUT_MS: int = max(5000, int(os.getenv("DB_IDLE_IN_TX_TIMEOUT_MS", "60000")))


    # Redis
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://nanored-redis:6379/0")
    REDIS_MAX_CONNECTIONS: int = max(5, int(os.getenv("REDIS_MAX_CONNECTIONS", "20")))

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
    REQUEST_LOG_MAX_BODY_BYTES: int = int(os.getenv("REQUEST_LOG_MAX_BODY_BYTES", "4096"))

    LOG_BUFFER_MAXLEN: int = max(100, int(os.getenv("LOG_BUFFER_MAXLEN", "1000")))

    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_DIR: str = os.getenv("LOG_DIR", "/app/data/logs")
    LOG_FILE_NAME: str = os.getenv("LOG_FILE_NAME", "nanored-api.log")
    LOG_FILE_ROTATION_WHEN: str = os.getenv("LOG_FILE_ROTATION_WHEN", "midnight")
    LOG_FILE_ROTATION_INTERVAL: int = max(1, int(os.getenv("LOG_FILE_ROTATION_INTERVAL", "1")))
    LOG_FILE_RETENTION_DAYS: int = max(1, int(os.getenv("LOG_FILE_RETENTION_DAYS", "7")))

    FUNCTION_CALL_LOGGING_ENABLED: bool = os.getenv("FUNCTION_CALL_LOGGING_ENABLED", "0").strip() in ("1", "true", "yes", "on")
    FUNCTION_CALL_LOG_EXCLUDE_MODULES: str = os.getenv("FUNCTION_CALL_LOG_EXCLUDE_MODULES", "app.core.logging_setup")


    # Remnawave logs ingest
    REMNAWAVE_LOG_INGEST_TOKEN: str = os.getenv("REMNAWAVE_LOG_INGEST_TOKEN", "")


    # Remnawave ingest queue
    REMNAWAVE_INGEST_QUEUE_ENABLED: bool = os.getenv("REMNAWAVE_INGEST_QUEUE_ENABLED", "1").strip() in ("1", "true", "yes", "on")
    REMNAWAVE_INGEST_STREAM: str = os.getenv("REMNAWAVE_INGEST_STREAM", "stream:remnawave:ingest")
    REMNAWAVE_INGEST_GROUP: str = os.getenv("REMNAWAVE_INGEST_GROUP", "remnawave_ingest")
    REMNAWAVE_INGEST_CONSUMER: str = os.getenv("REMNAWAVE_INGEST_CONSUMER", "worker-1")
    REMNAWAVE_INGEST_READ_COUNT: int = int(os.getenv("REMNAWAVE_INGEST_READ_COUNT", "10"))
    REMNAWAVE_INGEST_STREAM_MAXLEN: int = int(os.getenv("REMNAWAVE_INGEST_STREAM_MAXLEN", "50000"))
    REMNAWAVE_INGEST_MAX_RETRIES: int = int(os.getenv("REMNAWAVE_INGEST_MAX_RETRIES", "3"))
    REMNAWAVE_INGEST_DEAD_STREAM: str = os.getenv("REMNAWAVE_INGEST_DEAD_STREAM", "stream:remnawave:ingest:dead")
    REMNAWAVE_INGEST_DEAD_MAXLEN: int = int(os.getenv("REMNAWAVE_INGEST_DEAD_MAXLEN", "10000"))
    REMNAWAVE_LOGS_SUMMARY_DAYS: int = max(1, int(os.getenv("REMNAWAVE_LOGS_SUMMARY_DAYS", "7")))

    # GeoIP
    GEOIP_DB_PATH: str = "/app/data/GeoLite2-City.mmdb"

    BG_SESSION_CLEANUP_INTERVAL_SECONDS: int = max(30, int(os.getenv("BG_SESSION_CLEANUP_INTERVAL_SECONDS", "300")))
    BG_SESSION_CLEANUP_BATCH_SIZE: int = max(100, int(os.getenv("BG_SESSION_CLEANUP_BATCH_SIZE", "1000")))
    ADULT_RECHECK_BATCH_LIMIT: int = max(100, int(os.getenv("ADULT_RECHECK_BATCH_LIMIT", "1000")))
    ADULT_RECHECK_MAX_BATCHES_PER_LOOP: int = max(1, int(os.getenv("ADULT_RECHECK_MAX_BATCHES_PER_LOOP", "2")))
    ADULT_RECHECK_LOOP_SLEEP_SECONDS: int = max(5, int(os.getenv("ADULT_RECHECK_LOOP_SLEEP_SECONDS", "30")))

    class Config:
        case_sensitive = True


settings = Settings()
