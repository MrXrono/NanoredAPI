import enum
import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime, Text, Enum, BigInteger, Integer
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class SupportDirection(str, enum.Enum):
    APP_TO_SUPPORT = "app_to_support"
    SUPPORT_TO_APP = "support_to_app"
    SYSTEM = "system"


class SupportMessageType(str, enum.Enum):
    TEXT = "text"
    PHOTO = "photo"
    DOCUMENT = "document"
    VIDEO = "video"
    AUDIO = "audio"
    VOICE = "voice"
    FILE = "file"


class SupportMessage(Base):
    __tablename__ = "support_messages"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    account_id: Mapped[str] = mapped_column(String(64), index=True)
    device_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), index=True)

    direction: Mapped[SupportDirection] = mapped_column(Enum(SupportDirection, name="support_direction"), index=True)
    message_type: Mapped[SupportMessageType] = mapped_column(Enum(SupportMessageType, name="support_message_type"), default=SupportMessageType.TEXT)

    text: Mapped[str | None] = mapped_column(Text)

    file_name: Mapped[str | None] = mapped_column(String(255))
    mime_type: Mapped[str | None] = mapped_column(String(127))
    file_size: Mapped[int | None] = mapped_column(Integer)
    telegram_file_id: Mapped[str | None] = mapped_column(String(255))

    bridge_message_id: Mapped[int | None] = mapped_column(BigInteger, index=True)
    source_bot_message_id: Mapped[int | None] = mapped_column(BigInteger)

    delivered_to_app_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    read_by_app_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)
