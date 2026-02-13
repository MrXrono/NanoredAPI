import uuid
from datetime import datetime, timezone

from sqlalchemy import String, Boolean, DateTime, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class Device(Base):
    __tablename__ = "devices"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    android_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    api_key_hash: Mapped[str] = mapped_column(String(128))
    device_model: Mapped[str | None] = mapped_column(String(128))
    manufacturer: Mapped[str | None] = mapped_column(String(128))
    android_version: Mapped[str | None] = mapped_column(String(32))
    api_level: Mapped[int | None] = mapped_column()
    app_version: Mapped[str | None] = mapped_column(String(32))
    screen_resolution: Mapped[str | None] = mapped_column(String(32))
    dpi: Mapped[int | None] = mapped_column()
    language: Mapped[str | None] = mapped_column(String(16))
    timezone: Mapped[str | None] = mapped_column(String(64))
    is_rooted: Mapped[bool | None] = mapped_column(Boolean, default=False)
    carrier: Mapped[str | None] = mapped_column(String(128))
    ram_total_mb: Mapped[int | None] = mapped_column()
    account_id: Mapped[str | None] = mapped_column(String(64), index=True)
    is_blocked: Mapped[bool] = mapped_column(Boolean, default=False)
    note: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    sessions = relationship("Session", back_populates="device", lazy="selectin")
