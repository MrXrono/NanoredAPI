import uuid
from datetime import datetime, timezone

from sqlalchemy import Boolean, BigInteger, DateTime, ForeignKey, Integer, String, Text, Float
from sqlalchemy import JSON, Index, text
from sqlalchemy.dialects.postgresql import ARRAY
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
    dns: Mapped[str] = mapped_column(String(255), index=True)
    node_name: Mapped[str | None] = mapped_column(String(128), index=True)
    requested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

    account = relationship("RemnawaveAccount", back_populates="queries")


class RemnawaveDNSUnique(Base):
    __tablename__ = "remnawave_dns_unique"

    dns_root: Mapped[str] = mapped_column(String(255), primary_key=True)
    is_adult: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    first_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    last_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    last_marked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    mark_source: Mapped[list[str] | None] = mapped_column(ARRAY(String(24)), default=list, nullable=True)
    mark_version: Mapped[str | None] = mapped_column(String(64), nullable=True)
    need_recheck: Mapped[bool] = mapped_column(Boolean, default=False)

    __table_args__ = (
        Index(
            "ix_remnawave_dns_unique_need_recheck_true",
            "need_recheck",
            postgresql_where=text("need_recheck IS TRUE"),
        ),
        Index(
            "ix_remnawave_dns_unique_recheck_last_seen",
            "last_seen",
            postgresql_where=text("need_recheck IS TRUE"),
        ),
    )


class AdultDomainCatalog(Base):
    __tablename__ = "adult_domain_catalog"

    domain: Mapped[str] = mapped_column(String(255), primary_key=True)
    category: Mapped[str] = mapped_column(String(64), default="adult")
    source_mask: Mapped[int] = mapped_column(Integer, default=0, index=True)
    list_version: Mapped[str | None] = mapped_column(String(64), nullable=True)
    checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    is_enabled: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    source_text: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)


class AdultSyncState(Base):
    __tablename__ = "adult_sync_state"

    job_name: Mapped[str] = mapped_column(String(64), primary_key=True)
    last_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_watermark: Mapped[str | None] = mapped_column(String(128), nullable=True)
    status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    stats_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)


class AdultDomainExclusion(Base):
    __tablename__ = "adult_domain_exclusions"

    domain: Mapped[str] = mapped_column(String(255), primary_key=True)
    reason: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))


class AdultTaskRun(Base):
    __tablename__ = "adult_task_runs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    task_key: Mapped[str] = mapped_column(String(32), index=True)
    label: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(32), index=True, default="idle")
    running: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    phase: Mapped[str | None] = mapped_column(String(64), nullable=True)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
    progress_current: Mapped[int] = mapped_column(BigInteger, default=0)
    progress_total: Mapped[int] = mapped_column(BigInteger, default=0)
    progress_percent: Mapped[float] = mapped_column(Float, default=0.0)
    result_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    error_short: Mapped[str | None] = mapped_column(String(512), nullable=True)
    error_full: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
