import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime, BigInteger, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class AppTraffic(Base):
    __tablename__ = "app_traffic"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    session_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("sessions.id"), index=True)
    device_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("devices.id"), index=True)
    package_name: Mapped[str] = mapped_column(String(256), index=True)
    app_name: Mapped[str | None] = mapped_column(String(256))
    bytes_downloaded: Mapped[int] = mapped_column(BigInteger, default=0)
    bytes_uploaded: Mapped[int] = mapped_column(BigInteger, default=0)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    session = relationship("Session", back_populates="app_traffic")
