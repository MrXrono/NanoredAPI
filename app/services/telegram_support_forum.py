import asyncio
import io
import json
import logging
import os
from pathlib import Path

from aiogram import Bot
from aiogram.types import BufferedInputFile, Message, Update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.device import Device
from app.models.support_message import SupportMessageType

log = logging.getLogger(__name__)


class TelegramSupportForum:
    def __init__(self) -> None:
        self._bot: Bot | None = None
        self._bot_id: int | None = None
        self._state_lock = asyncio.Lock()
        # Store state on a host-mounted volume by default (docker-compose mounts ./data -> /app/data).
        self._state_file = Path(os.getenv("TELEGRAM_SUPPORT_DATA_FILE", "/app/data/data.json"))
        self._state = self._load_state()

    @staticmethod
    def _default_state() -> dict:
        # Keep the same top-level shape as nanored_support_bot.
        return {
            "next_ticket_id": 1,
            "open_tickets": {},
            "thread_to_user": {},
            "archive_thread_id": None,
            "thread_to_target": {},
        }

    def _load_state(self) -> dict:
        try:
            raw = self._state_file.read_text(encoding="utf-8")
        except FileNotFoundError:
            return self._default_state()
        try:
            data = json.loads(raw)
        except Exception:
            log.warning("TELEGRAM_SUPPORT_DATA_FILE is not valid JSON: %s", self._state_file)
            return self._default_state()

        # Backfill missing keys for forward compatibility.
        base = self._default_state()
        for k, v in base.items():
            if k not in data:
                data[k] = v
        return data

    def _save_state(self) -> None:
        self._state_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._state_file.with_suffix(self._state_file.suffix + ".tmp")
        tmp.write_text(json.dumps(self._state, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self._state_file)

    @property
    def enabled(self) -> bool:
        return bool(settings.TELEGRAM_MESSAGE_BOT_TOKEN and settings.TELEGRAM_SUPPORT_GROUP_ID)

    @property
    def support_group_id(self) -> int:
        return int(settings.TELEGRAM_SUPPORT_GROUP_ID)

    @property
    def bot(self) -> Bot:
        if self._bot is None:
            if not self.enabled:
                raise RuntimeError("Telegram support forum is not configured")
            self._bot = Bot(token=settings.TELEGRAM_MESSAGE_BOT_TOKEN)
        return self._bot

    async def bot_id(self) -> int:
        if self._bot_id is None:
            me = await self.bot.get_me()
            self._bot_id = int(me.id)
        return self._bot_id

    async def close(self) -> None:
        if self._bot is not None:
            await self._bot.session.close()
            self._bot = None
            self._bot_id = None

    async def download_file(self, telegram_file_id: str) -> bytes:
        file = await self.bot.get_file(telegram_file_id)
        bio = io.BytesIO()
        await self.bot.download(file, destination=bio)
        return bio.getvalue()

    def _target_key(self, device: Device) -> str:
        # Stable unique key per app installation/device.
        return f"app:{device.account_id}:{device.id}"

    def _parse_target_key(self, target_key: str) -> tuple[str | None, str | None]:
        # app:<account_id>:<device_uuid>
        try:
            pfx, account_id, device_id = target_key.split(":", 2)
        except ValueError:
            return None, None
        if pfx != "app":
            return None, None
        return account_id, device_id

    async def get_archive_thread_id(self) -> int | None:
        raw = self._state.get("archive_thread_id")
        return int(raw) if isinstance(raw, int) else None

    async def set_archive_thread_id(self, thread_id: int) -> None:
        async with self._state_lock:
            self._state["archive_thread_id"] = int(thread_id)
            self._save_state()

    async def _ensure_ticket(self, device: Device) -> dict:
        target = self._target_key(device)
        async with self._state_lock:
            existing = self._state["open_tickets"].get(target)
            if existing:
                return existing

            ticket_id = int(self._state["next_ticket_id"])
            self._state["next_ticket_id"] = ticket_id + 1
            self._save_state()

        try:
            topic = await self.bot.create_forum_topic(
                chat_id=self.support_group_id,
                name=f"Заявка app №{ticket_id}",
            )
            thread_id = int(topic.message_thread_id)
        except Exception:
            # Roll back ticket counter on failure (keep IDs dense like nanored_support_bot).
            async with self._state_lock:
                if int(self._state.get("next_ticket_id") or 0) == ticket_id + 1:
                    self._state["next_ticket_id"] = ticket_id
                    self._save_state()
            raise

        info = await self.bot.send_message(
            chat_id=self.support_group_id,
            message_thread_id=thread_id,
            text=(
                f"🎫 <b>Заявка app №{ticket_id}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━\n"
                f"🆔 <b>Account ID:</b> <code>{device.account_id}</code>\n"
                f"🔑 <b>App token:</b> <code>{device.id}</code>\n"
                f"━━━━━━━━━━━━━━━━━━━"
            ),
            parse_mode="HTML",
        )

        ticket = {
            "ticket_id": ticket_id,
            "thread_id": thread_id,
            "target": target,
            # Used for archiving.
            "messages": [{"msg_id": int(info.message_id), "from": "info"}],
        }

        async with self._state_lock:
            self._state["open_tickets"][target] = ticket
            self._state["thread_to_target"][str(thread_id)] = target
            self._save_state()

        return ticket

    async def send_from_app(
        self,
        db: AsyncSession,
        device: Device,
        message_type: SupportMessageType,
        text: str | None = None,
        file_name: str | None = None,
        mime_type: str | None = None,
        file_bytes: bytes | None = None,
    ) -> int | None:
        if not self.enabled:
            return None

        ticket = await self._ensure_ticket(device)

        if file_bytes:
            upload_name = file_name or "upload.bin"
            upload = BufferedInputFile(file_bytes, filename=upload_name)
            caption = text[:900] if text else None

            if message_type == SupportMessageType.PHOTO:
                sent = await self.bot.send_photo(
                    chat_id=self.support_group_id,
                    message_thread_id=ticket.thread_id,
                    photo=upload,
                    caption=caption,
                )
            elif message_type == SupportMessageType.VIDEO:
                sent = await self.bot.send_video(
                    chat_id=self.support_group_id,
                    message_thread_id=ticket.thread_id,
                    video=upload,
                    caption=caption,
                )
            elif message_type == SupportMessageType.AUDIO:
                sent = await self.bot.send_audio(
                    chat_id=self.support_group_id,
                    message_thread_id=ticket.thread_id,
                    audio=upload,
                    caption=caption,
                )
            elif message_type == SupportMessageType.VOICE:
                sent = await self.bot.send_voice(
                    chat_id=self.support_group_id,
                    message_thread_id=ticket.thread_id,
                    voice=upload,
                    caption=caption,
                )
            else:
                sent = await self.bot.send_document(
                    chat_id=self.support_group_id,
                    message_thread_id=ticket["thread_id"],
                    document=upload,
                    caption=caption,
                )

            async with self._state_lock:
                ticket["messages"].append({"msg_id": int(sent.message_id), "from": "app"})
                self._save_state()
            return int(sent.message_id)

        body = text or ""
        sent = await self.bot.send_message(
            chat_id=self.support_group_id,
            message_thread_id=ticket["thread_id"],
            text=body,
        )
        async with self._state_lock:
            ticket["messages"].append({"msg_id": int(sent.message_id), "from": "app"})
            self._save_state()
        return int(sent.message_id)

    @staticmethod
    def _infer_type(message: Message) -> SupportMessageType:
        if message.photo:
            return SupportMessageType.PHOTO
        if message.document:
            return SupportMessageType.DOCUMENT
        if message.video:
            return SupportMessageType.VIDEO
        if message.audio:
            return SupportMessageType.AUDIO
        if message.voice:
            return SupportMessageType.VOICE
        return SupportMessageType.TEXT

    @staticmethod
    def _extract_file(message: Message) -> tuple[str | None, str | None, int | None, str | None]:
        if message.photo:
            p = message.photo[-1]
            return p.file_id, "image/jpeg", p.file_size, None
        if message.document:
            d = message.document
            return d.file_id, d.mime_type, d.file_size, d.file_name
        if message.video:
            v = message.video
            return v.file_id, v.mime_type, v.file_size, None
        if message.audio:
            a = message.audio
            return a.file_id, a.mime_type, a.file_size, None
        if message.voice:
            v = message.voice
            return v.file_id, "audio/ogg", v.file_size, None
        return None, None, None, None

    def parse_update(self, update_json: dict) -> Message | None:
        try:
            update = Update.model_validate(update_json)
        except Exception as e:
            log.warning("Telegram update parse failed: %s", e)
            return None
        return update.message or update.edited_message or update.channel_post or update.edited_channel_post

    async def handle_support_message(self, db: AsyncSession, message: Message) -> dict | None:
        if not self.enabled:
            return None
        if int(message.chat.id) != int(self.support_group_id):
            return None
        if not message.message_thread_id:
            return None
        if message.text and message.text.startswith("/"):
            return None

        # Ignore our own messages.
        if message.from_user and int(message.from_user.id) == await self.bot_id():
            return None

        thread_id = int(message.message_thread_id)
        async with self._state_lock:
            target_key = self._state["thread_to_target"].get(str(thread_id))
            ticket = self._state["open_tickets"].get(target_key) if target_key else None
            if ticket:
                ticket["messages"].append({"msg_id": int(message.message_id), "from": "support"})
                self._save_state()

        if not ticket or not isinstance(target_key, str) or not target_key.startswith("app:"):
            return None

        msg_type = self._infer_type(message)
        text = message.text or message.caption
        file_id, mime_type, file_size, file_name = self._extract_file(message)

        account_id, device_id = self._parse_target_key(target_key)
        if not account_id:
            return None

        return {
            "ticket_id": int(ticket["ticket_id"]),
            "account_id": account_id,
            "device_id": device_id,
            "message_type": msg_type,
            "text": text.strip() if text else None,
            "telegram_file_id": file_id,
            "mime_type": mime_type,
            "file_size": file_size,
            "file_name": file_name,
            "source_bot_message_id": int(message.message_id),
        }

    async def archive_and_delete_topic(self, ticket: dict) -> None:
        archive_tid = await self.get_archive_thread_id()
        if not archive_tid:
            log.warning(
                "TELEGRAM archive_thread_id not set; skipping archive for ticket_id=%s (will still delete topic)",
                ticket.get("ticket_id"),
            )
        else:
            await self.bot.send_message(
                chat_id=self.support_group_id,
                message_thread_id=archive_tid,
                text=f"═══ 📂 <b>Начало заявки app №{ticket.get('ticket_id')}</b> ═══",
                parse_mode="HTML",
            )

            for item in ticket.get("messages", []):
                label: str | None
                msg_id = None
                sender = None
                if isinstance(item, dict):
                    msg_id = item.get("msg_id")
                    sender = item.get("from")
                if not isinstance(msg_id, int):
                    continue

                if sender == "info":
                    label = None
                elif sender == "app":
                    label = "📱 <b>От приложения:</b>"
                else:
                    label = "👨‍💻 <b>От техподдержки:</b>"

                try:
                    if label:
                        await self.bot.send_message(
                            chat_id=self.support_group_id,
                            message_thread_id=archive_tid,
                            text=label,
                            parse_mode="HTML",
                        )
                    await self.bot.copy_message(
                        chat_id=self.support_group_id,
                        from_chat_id=self.support_group_id,
                        message_id=int(msg_id),
                        message_thread_id=archive_tid,
                    )
                except Exception as e:
                    log.warning("Failed to archive msg_id=%s ticket_id=%s: %s", msg_id, ticket.get("ticket_id"), e)

            await self.bot.send_message(
                chat_id=self.support_group_id,
                message_thread_id=archive_tid,
                text=f"═══ ✅ <b>Конец заявки app №{ticket.get('ticket_id')}</b> ═══",
                parse_mode="HTML",
            )

        try:
            await self.bot.delete_forum_topic(chat_id=self.support_group_id, message_thread_id=int(ticket.get("thread_id")))
        except Exception as e:
            log.warning("Failed to delete forum topic thread_id=%s: %s", ticket.get("thread_id"), e)

    async def handle_topic_closed(self, db: AsyncSession, thread_id: int) -> dict | None:
        if not self.enabled:
            return None

        async with self._state_lock:
            target_key = self._state["thread_to_target"].get(str(int(thread_id)))
            ticket = self._state["open_tickets"].get(target_key) if target_key else None
            if not ticket or not isinstance(target_key, str) or not target_key.startswith("app:"):
                return None
            # Remove from open lists first to avoid double-processing.
            self._state["open_tickets"].pop(target_key, None)
            self._state["thread_to_target"].pop(str(int(thread_id)), None)
            self._save_state()

        # Best effort: archive and delete forum topic.
        try:
            await self.archive_and_delete_topic(ticket)
        except Exception as e:
            log.warning("archive_and_delete_topic failed for ticket_id=%s: %s", ticket.get("ticket_id"), e)

        account_id, device_id = self._parse_target_key(target_key)
        if not account_id:
            return None

        return {
            "ticket_id": int(ticket["ticket_id"]),
            "account_id": account_id,
            "device_id": device_id,
            "system_text": f"Заявка app №{int(ticket['ticket_id'])} закрыта оператором.",
        }


telegram_support_forum = TelegramSupportForum()
