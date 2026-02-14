import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime, BigInteger, ForeignKey, Integer
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class Session(Base):
    __tablename__ = "sessions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    device_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("devices.id"), index=True)
    server_address: Mapped[str | None] = mapped_column(String(256))
    protocol: Mapped[str | None] = mapped_column(String(32))
    client_ip: Mapped[str | None] = mapped_column(String(64))
    client_country: Mapped[str | None] = mapped_column(String(64))
    client_city: Mapped[str | None] = mapped_column(String(128))
    network_type: Mapped[str | None] = mapped_column(String(32))
    wifi_ssid: Mapped[str | None] = mapped_column(String(128))
    carrier: Mapped[str | None] = mapped_column(String(128))
    bytes_downloaded: Mapped[int] = mapped_column(BigInteger, default=0)
    bytes_uploaded: Mapped[int] = mapped_column(BigInteger, default=0)
    connection_count: Mapped[int] = mapped_column(Integer, default=0)
    reconnect_count: Mapped[int] = mapped_column(Integer, default=0)
    latency_ms: Mapped[int | None] = mapped_column(Integer)
    battery_level: Mapped[int | None] = mapped_column(Integer)
    connected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    disconnected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    device = relationship("Device", back_populates="sessions")
    sni_logs = relationship("SNILog", back_populates="session", lazy="selectin")
    dns_logs = relationship("DNSLog", back_populates="session", lazy="selectin")
    connection_logs = relationship("ConnectionLog", back_populates="session", lazy="selectin")
    errors = relationship("ErrorLog", back_populates="session", lazy="selectin")
