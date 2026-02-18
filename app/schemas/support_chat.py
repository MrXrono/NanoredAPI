from datetime import datetime

from pydantic import BaseModel

from app.models.support_message import SupportDirection, SupportMessageType


class SupportMessageResponse(BaseModel):
    id: str
    account_id: str
    direction: SupportDirection
    message_type: SupportMessageType
    text: str | None = None
    file_name: str | None = None
    mime_type: str | None = None
    file_size: int | None = None
    has_attachment: bool = False
    created_at: datetime
    read_by_app_at: datetime | None = None


class SupportMessagesResponse(BaseModel):
    items: list[SupportMessageResponse]
    unread_count: int


class SupportReadRequest(BaseModel):
    upto_message_id: str | None = None
