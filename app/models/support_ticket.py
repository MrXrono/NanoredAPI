import enum
import uuid
from datetime import datetime, timezone

from sqlalchemy import BigInteger, DateTime, Enum, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class SupportTicketOwner(str, enum.Enum):
    APP = "app"


class SupportTicketMessageFrom(str, enum.Enum):
    INFO = "info"
    APP = "app"
    SUPPORT = "support"


class SupportTicket(Base):
    __tablename__ = "support_tickets"
    __table_args__ = (UniqueConstraint("owner", "thread_id", name="uq_support_tickets_owner_thread"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    owner: Mapped[SupportTicketOwner] = mapped_column(Enum(SupportTicketOwner, name="support_ticket_owner"), index=True)

    account_id: Mapped[str] = mapped_column(String(64), index=True)
    device_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), index=True)

    thread_id: Mapped[int] = mapped_column(BigInteger, index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)


class SupportTicketMessage(Base):
    __tablename__ = "support_ticket_messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ticket_id: Mapped[int] = mapped_column(ForeignKey("support_tickets.id", ondelete="CASCADE"), index=True)

    message_id: Mapped[int] = mapped_column(BigInteger, index=True)
    sender: Mapped[SupportTicketMessageFrom] = mapped_column(Enum(SupportTicketMessageFrom, name="support_ticket_message_from"), index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)


class SupportForumMeta(Base):
    __tablename__ = "support_forum_meta"

    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value: Mapped[str] = mapped_column(String(255))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)

