import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, UploadFile, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.logging_buffer import logging_buffer
from app.core.security import verify_api_key
from app.models.device import Device
from app.models.support_message import SupportDirection, SupportMessage, SupportMessageType
from app.schemas.support_chat import SupportMessageResponse, SupportMessagesResponse, SupportReadRequest
from app.services.telegram_bridge import telegram_bridge

router = APIRouter(prefix="/client/support", tags=["client-support"])

_MAX_ATTACHMENT_SIZE = 50 * 1024 * 1024


async def _get_device(api_key: str, db: AsyncSession) -> Device:
    if not api_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing X-API-Key")
    parts = api_key.split(":", 1)
    if len(parts) != 2:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key format")

    device_id_str, secret = parts
    try:
        device_id = uuid.UUID(device_id_str)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")

    result = await db.execute(select(Device).where(Device.id == device_id))
    device = result.scalar_one_or_none()
    if not device:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Device not found")
    if not verify_api_key(secret, device.api_key_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
    if device.is_blocked:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Device is blocked")
    if not device.account_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Device account_id is not set")

    return device


def _to_response(msg: SupportMessage) -> SupportMessageResponse:
    return SupportMessageResponse(
        id=str(msg.id),
        account_id=msg.account_id,
        direction=msg.direction,
        message_type=msg.message_type,
        text=msg.text,
        file_name=msg.file_name,
        mime_type=msg.mime_type,
        file_size=msg.file_size,
        has_attachment=bool(msg.telegram_file_id),
        created_at=msg.created_at,
        read_by_app_at=msg.read_by_app_at,
    )


def _guess_type(content_type: str | None) -> SupportMessageType:
    if not content_type:
        return SupportMessageType.FILE
    value = content_type.lower()
    if value.startswith("image/"):
        return SupportMessageType.PHOTO
    if value.startswith("video/"):
        return SupportMessageType.VIDEO
    if value.startswith("audio/"):
        if "ogg" in value or "opus" in value:
            return SupportMessageType.VOICE
        return SupportMessageType.AUDIO
    if value in {"text/plain", "application/json"}:
        return SupportMessageType.DOCUMENT
    return SupportMessageType.DOCUMENT


@router.post("/send", response_model=SupportMessageResponse)
async def send_support_message(
    text: str | None = Form(default=None),
    file: UploadFile | None = File(default=None),
    db: AsyncSession = Depends(get_db),
    x_api_key: str = Header(..., alias="X-API-Key"),
):
    device = await _get_device(x_api_key, db)

    if not text and file is None:
        raise HTTPException(status_code=400, detail="Either text or file must be provided")

    file_bytes: bytes | None = None
    msg_type = SupportMessageType.TEXT
    file_name = None
    mime_type = None
    file_size = None

    if file is not None:
        file_bytes = await file.read()
        if len(file_bytes) > _MAX_ATTACHMENT_SIZE:
            raise HTTPException(status_code=413, detail="Attachment is larger than 50 MB")
        file_name = file.filename
        mime_type = file.content_type
        file_size = len(file_bytes)
        msg_type = _guess_type(file.content_type)

    msg = SupportMessage(
        account_id=device.account_id,
        device_id=device.id,
        direction=SupportDirection.APP_TO_SUPPORT,
        message_type=msg_type,
        text=text.strip() if text else None,
        file_name=file_name,
        mime_type=mime_type,
        file_size=file_size,
    )
    db.add(msg)
    await db.flush()

    try:
        bridge_message_id = await telegram_bridge.send_from_app(
            account_id=device.account_id,
            message_id=str(msg.id),
            message_type=msg_type,
            text=msg.text,
            file_name=file_name,
            mime_type=mime_type,
            file_bytes=file_bytes,
        )
        msg.bridge_message_id = bridge_message_id
    except Exception as e:
        logging_buffer.add(
            "error",
            f"Support bridge send failed: {e} (bridge_chat_id={telegram_bridge.bridge_chat_id})",
        )

    logging_buffer.add("processing", f"Support message from account={device.account_id}, type={msg_type.value}")
    return _to_response(msg)


@router.get("/messages", response_model=SupportMessagesResponse)
async def get_support_messages(
    after_id: str | None = None,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    x_api_key: str = Header(..., alias="X-API-Key"),
):
    device = await _get_device(x_api_key, db)
    limit = max(1, min(limit, 200))

    query = select(SupportMessage).where(SupportMessage.account_id == device.account_id)
    if after_id:
        try:
            after_uuid = uuid.UUID(after_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid after_id")
        after_row = await db.execute(select(SupportMessage.created_at).where(SupportMessage.id == after_uuid))
        after_ts = after_row.scalar_one_or_none()
        if after_ts:
            query = query.where(SupportMessage.created_at > after_ts)

    result = await db.execute(query.order_by(SupportMessage.created_at.asc()).limit(limit))
    items = result.scalars().all()

    unread_count_q = await db.execute(
        select(func.count(SupportMessage.id)).where(
            SupportMessage.account_id == device.account_id,
            SupportMessage.direction == SupportDirection.SUPPORT_TO_APP,
            SupportMessage.read_by_app_at.is_(None),
        )
    )
    unread_count = unread_count_q.scalar() or 0

    return SupportMessagesResponse(items=[_to_response(i) for i in items], unread_count=unread_count)


@router.get("/unread")
async def get_unread_count(
    db: AsyncSession = Depends(get_db),
    x_api_key: str = Header(..., alias="X-API-Key"),
):
    device = await _get_device(x_api_key, db)
    unread_count_q = await db.execute(
        select(func.count(SupportMessage.id)).where(
            SupportMessage.account_id == device.account_id,
            SupportMessage.direction == SupportDirection.SUPPORT_TO_APP,
            SupportMessage.read_by_app_at.is_(None),
        )
    )
    unread_count = unread_count_q.scalar() or 0
    return {"unread_count": unread_count}


@router.post("/read")
async def mark_support_read(
    req: SupportReadRequest,
    db: AsyncSession = Depends(get_db),
    x_api_key: str = Header(..., alias="X-API-Key"),
):
    device = await _get_device(x_api_key, db)
    now = datetime.now(timezone.utc)

    query = select(SupportMessage).where(
        SupportMessage.account_id == device.account_id,
        SupportMessage.direction == SupportDirection.SUPPORT_TO_APP,
        SupportMessage.read_by_app_at.is_(None),
    )

    if req.upto_message_id:
        try:
            upto_uuid = uuid.UUID(req.upto_message_id)
            upto_ts_q = await db.execute(select(SupportMessage.created_at).where(SupportMessage.id == upto_uuid))
            upto_ts = upto_ts_q.scalar_one_or_none()
            if upto_ts:
                query = query.where(SupportMessage.created_at <= upto_ts)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid upto_message_id")

    rows = await db.execute(query)
    messages = rows.scalars().all()
    for row in messages:
        row.read_by_app_at = now
        if row.delivered_to_app_at is None:
            row.delivered_to_app_at = now

    return {"status": "ok", "count": len(messages)}


@router.get("/media/{message_id}")
async def download_media(
    message_id: str,
    db: AsyncSession = Depends(get_db),
    x_api_key: str = Header(..., alias="X-API-Key"),
):
    from fastapi.responses import Response

    device = await _get_device(x_api_key, db)
    try:
        msg_uuid = uuid.UUID(message_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid message_id")

    result = await db.execute(
        select(SupportMessage).where(
            SupportMessage.id == msg_uuid,
            SupportMessage.account_id == device.account_id,
        )
    )
    msg = result.scalar_one_or_none()
    if not msg or not msg.telegram_file_id:
        raise HTTPException(status_code=404, detail="Attachment not found")

    content = await telegram_bridge.download_file(msg.telegram_file_id)
    media_type = msg.mime_type or "application/octet-stream"
    filename = msg.file_name or f"support-{message_id}"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return Response(content=content, media_type=media_type, headers=headers)


@router.post("/telegram/webhook")
async def telegram_webhook(update: dict, db: AsyncSession = Depends(get_db)):
    parsed = telegram_bridge.parse_support_update(update)
    if not parsed:
        return {"status": "ignored"}

    account_id = parsed["account_id"]

    msg = SupportMessage(
        account_id=account_id,
        device_id=None,
        direction=SupportDirection.SUPPORT_TO_APP,
        message_type=parsed["message_type"],
        text=parsed.get("text"),
        file_name=parsed.get("file_name"),
        mime_type=parsed.get("mime_type"),
        file_size=parsed.get("file_size"),
        telegram_file_id=parsed.get("telegram_file_id"),
        bridge_message_id=parsed.get("bridge_message_id"),
        source_bot_message_id=parsed.get("source_bot_message_id"),
        delivered_to_app_at=None,
    )
    db.add(msg)
    await db.flush()

    logging_buffer.add("processing", f"Support reply queued for account={account_id}, type={msg.message_type.value}")
    return {"status": "ok", "message_id": str(msg.id)}
