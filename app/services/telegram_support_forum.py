import io
import logging
from datetime import datetime, timezone

from aiogram import Bot
from aiogram.types import BufferedInputFile, Message, Update
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.device import Device
from app.models.support_message import SupportMessageType
from app.models.support_ticket import (
    SupportForumMeta,
    SupportTicket,
    SupportTicketMessage,
    SupportTicketMessageFrom,
    SupportTicketOwner,
)

log = logging.getLogger(__name__)


class TelegramSupportForum:
    def __init__(self) -> None:
        self._bot: Bot | None = None
        self._bot_id: int | None = None

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

    async def _meta_get(self, db: AsyncSession, key: str) -> str | None:
        row = await db.execute(select(SupportForumMeta).where(SupportForumMeta.key == key))
        item = row.scalar_one_or_none()
        return item.value if item else None

    async def _meta_set(self, db: AsyncSession, key: str, value: str) -> None:
        row = await db.execute(select(SupportForumMeta).where(SupportForumMeta.key == key))
        item = row.scalar_one_or_none()
        now = datetime.now(timezone.utc)
        if item:
            item.value = value
            item.updated_at = now
        else:
            db.add(SupportForumMeta(key=key, value=value, updated_at=now))
        await db.flush()

    async def get_archive_thread_id(self, db: AsyncSession) -> int | None:
        raw = await self._meta_get(db, "archive_thread_id")
        if not raw:
            return None
        try:
            return int(raw)
        except ValueError:
            return None

    async def set_archive_thread_id(self, db: AsyncSession, thread_id: int) -> None:
        await self._meta_set(db, "archive_thread_id", str(int(thread_id)))

    async def _add_ticket_message(self, db: AsyncSession, ticket_id: int, message_id: int, sender: SupportTicketMessageFrom) -> None:
        db.add(SupportTicketMessage(ticket_id=ticket_id, message_id=int(message_id), sender=sender))
        await db.flush()

    async def _get_open_ticket(self, db: AsyncSession, account_id: str) -> SupportTicket | None:
        q = await db.execute(
            select(SupportTicket).where(
                SupportTicket.owner == SupportTicketOwner.APP,
                SupportTicket.account_id == account_id,
                SupportTicket.closed_at.is_(None),
            )
        )
        return q.scalar_one_or_none()

    async def _get_ticket_by_thread(self, db: AsyncSession, thread_id: int) -> SupportTicket | None:
        q = await db.execute(
            select(SupportTicket).where(
                SupportTicket.owner == SupportTicketOwner.APP,
                SupportTicket.thread_id == int(thread_id),
            )
        )
        return q.scalar_one_or_none()

    async def _create_ticket(self, db: AsyncSession, device: Device) -> SupportTicket:
        # Step 1: reserve ticket number via DB autoincrement.
        ticket = SupportTicket(
            owner=SupportTicketOwner.APP,
            account_id=device.account_id,
            device_id=device.id,
            thread_id=0,
        )
        db.add(ticket)
        await db.flush()  # assigns ticket.id

        # Step 2: create topic in support forum.
        try:
            topic = await self.bot.create_forum_topic(
                chat_id=self.support_group_id,
                name=f"Заявка app №{ticket.id}",
            )
            ticket.thread_id = int(topic.message_thread_id)
            await db.flush()
        except Exception:
            # Keep DB clean if Telegram call fails.
            await db.delete(ticket)
            await db.flush()
            raise

        # Step 3: info card inside the thread.
        info = await self.bot.send_message(
            chat_id=self.support_group_id,
            message_thread_id=ticket.thread_id,
            text=(
                f"🎫 <b>Заявка app №{ticket.id}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━\n"
                f"🆔 <b>Account ID:</b> <code>{device.account_id}</code>\n"
                f"🔑 <b>App token:</b> <code>{device.id}</code>\n"
                f"━━━━━━━━━━━━━━━━━━━"
            ),
            parse_mode="HTML",
        )
        await self._add_ticket_message(db, ticket.id, info.message_id, SupportTicketMessageFrom.INFO)
        return ticket

    async def ensure_ticket(self, db: AsyncSession, device: Device) -> SupportTicket:
        existing = await self._get_open_ticket(db, device.account_id)
        if existing:
            return existing
        return await self._create_ticket(db, device)

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

        ticket = await self.ensure_ticket(db, device)

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
                    message_thread_id=ticket.thread_id,
                    document=upload,
                    caption=caption,
                )

            await self._add_ticket_message(db, ticket.id, sent.message_id, SupportTicketMessageFrom.APP)
            return int(sent.message_id)

        body = text or ""
        sent = await self.bot.send_message(
            chat_id=self.support_group_id,
            message_thread_id=ticket.thread_id,
            text=body,
        )
        await self._add_ticket_message(db, ticket.id, sent.message_id, SupportTicketMessageFrom.APP)
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
        except Exception:
            return None
        return update.message

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

        ticket = await self._get_ticket_by_thread(db, int(message.message_thread_id))
        if ticket is None or ticket.closed_at is not None:
            return None

        msg_type = self._infer_type(message)
        text = message.text or message.caption
        file_id, mime_type, file_size, file_name = self._extract_file(message)

        await self._add_ticket_message(db, ticket.id, int(message.message_id), SupportTicketMessageFrom.SUPPORT)

        return {
            "ticket_id": ticket.id,
            "account_id": ticket.account_id,
            "device_id": ticket.device_id,
            "message_type": msg_type,
            "text": text.strip() if text else None,
            "telegram_file_id": file_id,
            "mime_type": mime_type,
            "file_size": file_size,
            "file_name": file_name,
            "source_bot_message_id": int(message.message_id),
        }

    async def archive_and_delete_topic(self, db: AsyncSession, ticket: SupportTicket) -> None:
        archive_tid = await self.get_archive_thread_id(db)
        if not archive_tid:
            log.warning("TELEGRAM archive_thread_id not set; skipping archive for ticket_id=%s", ticket.id)
            return

        await self.bot.send_message(
            chat_id=self.support_group_id,
            message_thread_id=archive_tid,
            text=f"═══ 📂 <b>Начало заявки app №{ticket.id}</b> ═══",
            parse_mode="HTML",
        )

        rows = await db.execute(
            select(SupportTicketMessage).where(SupportTicketMessage.ticket_id == ticket.id).order_by(SupportTicketMessage.id.asc())
        )
        items = rows.scalars().all()
        for item in items:
            label: str | None
            if item.sender == SupportTicketMessageFrom.INFO:
                label = None
            elif item.sender == SupportTicketMessageFrom.APP:
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
                    message_id=int(item.message_id),
                    message_thread_id=archive_tid,
                )
            except Exception as e:
                log.warning("Failed to archive msg_id=%s ticket_id=%s: %s", item.message_id, ticket.id, e)

        await self.bot.send_message(
            chat_id=self.support_group_id,
            message_thread_id=archive_tid,
            text=f"═══ ✅ <b>Конец заявки app №{ticket.id}</b> ═══",
            parse_mode="HTML",
        )

        try:
            await self.bot.delete_forum_topic(chat_id=self.support_group_id, message_thread_id=int(ticket.thread_id))
        except Exception as e:
            log.warning("Failed to delete forum topic thread_id=%s: %s", ticket.thread_id, e)

    async def handle_topic_closed(self, db: AsyncSession, thread_id: int) -> dict | None:
        if not self.enabled:
            return None
        ticket = await self._get_ticket_by_thread(db, int(thread_id))
        if ticket is None or ticket.closed_at is not None:
            return None

        ticket.closed_at = datetime.now(timezone.utc)
        await db.flush()

        # Best effort: archive and delete forum topic.
        try:
            await self.archive_and_delete_topic(db, ticket)
        except Exception as e:
            log.warning("archive_and_delete_topic failed for ticket_id=%s: %s", ticket.id, e)

        return {
            "ticket_id": ticket.id,
            "account_id": ticket.account_id,
            "device_id": ticket.device_id,
            "system_text": f"Заявка app №{ticket.id} закрыта оператором.",
        }


telegram_support_forum = TelegramSupportForum()
