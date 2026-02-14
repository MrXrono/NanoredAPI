import asyncio
import logging
import time
import traceback
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse
from sqlalchemy import select, text

from app.core.config import settings
from app.core.database import engine, Base, async_session
from app.core.redis import close_redis, get_redis
from app.core.security import hash_password
from app.core.logging_buffer import logging_buffer
from app.api.v1.router import api_router
from app.models.admin import Admin
from app.models.device import Device
from app.models.session import Session
from app.models.sni_log import SNILog
from app.models.dns_log import DNSLog
from app.models.connection_log import ConnectionLog
from app.models.error_log import ErrorLog
from app.models.account import Account
from app.models.device_permission import DevicePermission
from app.models.device_log import DeviceLog
from app.models.device_change_log import DeviceChangeLog

logger = logging.getLogger(__name__)


async def _cleanup_stale_sessions():
    """Background task: close sessions whose Redis online key has expired."""
    while True:
        try:
            await asyncio.sleep(180)  # every 3 minutes
            redis = await get_redis()
            async with async_session() as db:
                result = await db.execute(
                    select(Session).where(Session.disconnected_at.is_(None))
                )
                active_sessions = result.scalars().all()
                closed = 0
                for s in active_sessions:
                    online_key = f"online:{s.device_id}"
                    exists = await redis.exists(online_key)
                    if not exists:
                        s.disconnected_at = datetime.now(timezone.utc)
                        closed += 1
                if closed > 0:
                    await db.commit()
                    logging_buffer.add("processing", f"Автозакрытие: {closed} зависших сессий")
                    logger.info(f"Auto-closed {closed} stale sessions")
        except Exception as e:
            logger.error(f"Session cleanup error: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.execute(text("SELECT pg_advisory_lock(1)"))
        try:
            await conn.run_sync(Base.metadata.create_all)
        except Exception as e:
            logger.warning(f"Table creation skipped (already exists): {e}")
        finally:
            await conn.execute(text("SELECT pg_advisory_unlock(1)"))

    try:
        async with async_session() as db:
            result = await db.execute(select(Admin).where(Admin.username == settings.ADMIN_USERNAME))
            if not result.scalar_one_or_none():
                admin = Admin(
                    username=settings.ADMIN_USERNAME,
                    password_hash=hash_password(settings.ADMIN_PASSWORD),
                )
                db.add(admin)
                await db.commit()
    except Exception as e:
        logger.warning(f"Admin creation skipped: {e}")

    cleanup_task = asyncio.create_task(_cleanup_stale_sessions())

    yield

    cleanup_task.cancel()
    await close_redis()
    await engine.dispose()


app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)


@app.middleware("http")
async def logging_middleware(request: Request, call_next):
    if not logging_buffer.enabled:
        return await call_next(request)

    start_time = time.time()
    method = request.method
    path = request.url.path
    client_ip = request.headers.get("X-Real-IP") or (request.client.host if request.client else "unknown")

    # Skip logging for admin log-polling endpoints (prevents recursive log spam)
    if path.startswith("/api/v1/admin/logs"):
        return await call_next(request)

    if path.startswith("/api/"):
        api_key = request.headers.get("X-API-Key", "")
        masked_key = f"{api_key[:8]}...{api_key[-4:]}" if len(api_key) > 12 else "***"

        body_bytes = await request.body()
        body_preview = body_bytes[:2000].decode("utf-8", errors="replace") if body_bytes else ""

        logging_buffer.add("request", f"{method} {path}", {
            "ip": client_ip,
            "api_key": masked_key,
            "body": body_preview,
        })

    try:
        response = await call_next(request)
        duration = round((time.time() - start_time) * 1000, 1)

        if path.startswith("/api/"):
            logging_buffer.add("processing", f"Ответ {response.status_code} за {duration}ms: {method} {path}")

        return response
    except Exception as e:
        duration = round((time.time() - start_time) * 1000, 1)
        logging_buffer.add("error", f"Исключение в {method} {path} ({duration}ms): {str(e)}", {
            "traceback": traceback.format_exc(),
        })
        return JSONResponse(status_code=500, content={"detail": "Internal server error"})


app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

app.include_router(api_router, prefix=settings.API_V1_PREFIX)


@app.get("/")
async def admin_panel(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/health")
async def health():
    return {"status": "ok", "version": settings.VERSION}
