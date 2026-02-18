import io
import logging
import re
from typing import Any

from aiogram import Bot
from aiogram.types import BufferedInputFile, Message, Update

from app.core.config import settings
from app.models.support_message import SupportMessageType

log = logging.getLogger(__name__)

_META_RE = re.compile(r"^NR_REPLY\|(?P<account>[0-9]{3,32})\|(?P<msg_type>[a-z_]+)")


class TelegramBridge:
    def __init__(self) -> None:
        self._bot: Bot | None = None
        self._bridge_chat_id: int | str | None = None

    @property
    def enabled(self) -> bool:
        return bool(settings.TELEGRAM_MESSAGE_BOT_TOKEN and settings.TELEGRAM_BRIDGE_CHAT_ID)

    @property
    def bridge_chat_id(self) -> int | str:
        if self._bridge_chat_id is None:
            self._bridge_chat_id = self._normalize_chat_id(settings.TELEGRAM_BRIDGE_CHAT_ID)
        return self._bridge_chat_id

    @property
    def bot(self) -> Bot:
        if self._bot is None:
            if not self.enabled:
                raise RuntimeError("Telegram bridge is not configured")
            self._bot = Bot(token=settings.TELEGRAM_MESSAGE_BOT_TOKEN)
        return self._bot

    async def close(self) -> None:
        if self._bot is not None:
            await self._bot.session.close()
            self._bot = None

    async def send_from_app(
        self,
        account_id: str,
        message_id: str,
        message_type: SupportMessageType,
        text: str | None = None,
        file_name: str | None = None,
        mime_type: str | None = None,
        file_bytes: bytes | None = None,
    ) -> int | None:
        if not self.enabled:
            return None

        header = f"NR_APP|{account_id}|{message_id}|{message_type.value}"
        bridge_chat_id = self.bridge_chat_id

        if file_bytes:
            upload_name = file_name or "upload.bin"
            upload = BufferedInputFile(file_bytes, filename=upload_name)
            caption = header
            if text:
                caption = f"{caption}\n{text[:900]}"

            if message_type == SupportMessageType.PHOTO:
                sent = await self.bot.send_photo(chat_id=bridge_chat_id, photo=upload, caption=caption)
            elif message_type == SupportMessageType.VIDEO:
                sent = await self.bot.send_video(chat_id=bridge_chat_id, video=upload, caption=caption)
            elif message_type == SupportMessageType.AUDIO:
                sent = await self.bot.send_audio(chat_id=bridge_chat_id, audio=upload, caption=caption)
            elif message_type == SupportMessageType.VOICE:
                sent = await self.bot.send_voice(chat_id=bridge_chat_id, voice=upload, caption=caption)
            else:
                sent = await self.bot.send_document(chat_id=bridge_chat_id, document=upload, caption=caption)
            return sent.message_id

        body = header if not text else f"{header}\n{text}"
        sent = await self.bot.send_message(chat_id=bridge_chat_id, text=body)
        return sent.message_id

    @staticmethod
    def _normalize_chat_id(raw_chat_id: str | int) -> int | str:
        value = str(raw_chat_id).strip()
        if not value:
            raise ValueError("TELEGRAM_BRIDGE_CHAT_ID is empty")
        if value.startswith("@"):
            return value
        if value.startswith("-"):
            return int(value)
        if value.isdigit():
            # Telegram group IDs are usually exposed without "-100" in some UIs.
            if len(value) >= 10:
                return int(f"-100{value}")
            return int(value)
        return value

    def is_from_support_bridge(self, message: Message) -> bool:
        bridge_chat_id = self.bridge_chat_id
        if isinstance(bridge_chat_id, int):
            if int(message.chat.id) != bridge_chat_id:
                return False
        else:
            if str(message.chat.id) != bridge_chat_id and str(message.chat.username or "") != bridge_chat_id.lstrip("@"):
                return False
        if not message.from_user:
            return False
        if settings.TELEGRAM_SUPPORT_BOT_ID:
            return int(message.from_user.id) == int(settings.TELEGRAM_SUPPORT_BOT_ID)
        return True

    @staticmethod
    def _extract_meta(message: Message) -> tuple[str, SupportMessageType, str | None] | None:
        meta_line = (message.caption or message.text or "").splitlines()[0].strip()
        m = _META_RE.match(meta_line)
        if not m:
            return None
        account_id = m.group("account")
        msg_type_raw = m.group("msg_type")
        try:
            msg_type = SupportMessageType(msg_type_raw)
        except ValueError:
            msg_type = SupportMessageType.TEXT

        body = message.caption if message.caption else message.text
        clean_text = None
        if body:
            parts = body.splitlines()
            clean_text = "\n".join(parts[1:]).strip() if len(parts) > 1 else None
        return account_id, msg_type, clean_text

    @staticmethod
    def _extract_file(message: Message) -> tuple[str | None, str | None, int | None]:
        if message.photo:
            photo = message.photo[-1]
            return photo.file_id, "image/jpeg", photo.file_size
        if message.document:
            d = message.document
            return d.file_id, d.mime_type, d.file_size
        if message.video:
            v = message.video
            return v.file_id, v.mime_type, v.file_size
        if message.audio:
            a = message.audio
            return a.file_id, a.mime_type, a.file_size
        if message.voice:
            v = message.voice
            return v.file_id, "audio/ogg", v.file_size
        return None, None, None

    def parse_support_update(self, update_json: dict[str, Any]) -> dict[str, Any] | None:
        try:
            update = Update.model_validate(update_json)
        except Exception:
            return None

        message = update.message
        if message is None:
            return None
        if not self.is_from_support_bridge(message):
            return None

        parsed = self._extract_meta(message)
        if not parsed:
            return None
        account_id, msg_type, clean_text = parsed

        file_id, mime_type, file_size = self._extract_file(message)
        file_name = message.document.file_name if message.document else None

        return {
            "account_id": account_id,
            "message_type": msg_type,
            "text": clean_text,
            "telegram_file_id": file_id,
            "mime_type": mime_type,
            "file_size": file_size,
            "file_name": file_name,
            "bridge_message_id": message.message_id,
            "source_bot_message_id": message.message_id,
        }

    async def download_file(self, telegram_file_id: str) -> bytes:
        file = await self.bot.get_file(telegram_file_id)
        bio = io.BytesIO()
        await self.bot.download(file, destination=bio)
        return bio.getvalue()


telegram_bridge = TelegramBridge()
