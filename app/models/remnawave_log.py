import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime, ForeignKey, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class RemnawaveAccount(Base):
    __tablename__ = "remnawave_accounts"

    account_login: Mapped[str] = mapped_column(String(128), primary_key=True)
    last_activity_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    queries = relationship("RemnawaveDNSQuery", back_populates="account", lazy="selectin")


class RemnawaveDNSQuery(Base):
    __tablename__ = "remnawave_dns_queries"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    account_login: Mapped[str] = mapped_column(String(128), ForeignKey("remnawave_accounts.account_login"), index=True)
    dns: Mapped[str] = mapped_column(String(512), index=True)
    resolved_ip: Mapped[str | None] = mapped_column(String(64), index=True)
    node_name: Mapped[str | None] = mapped_column(String(128), index=True)
    requested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    raw_line: Mapped[str | None] = mapped_column(Text)

    account = relationship("RemnawaveAccount", back_populates="queries")
