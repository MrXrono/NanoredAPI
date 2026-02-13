from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from sqlalchemy import select

from app.core.config import settings
from app.core.database import engine, Base, async_session
from app.core.redis import close_redis
from app.core.security import hash_password
from app.api.v1.router import api_router
from app.models.admin import Admin
from app.models.device import Device
from app.models.session import Session
from app.models.sni_log import SNILog
from app.models.dns_log import DNSLog
from app.models.app_traffic import AppTraffic
from app.models.connection_log import ConnectionLog
from app.models.error_log import ErrorLog


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: create tables and default admin
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with async_session() as db:
        result = await db.execute(select(Admin).where(Admin.username == settings.ADMIN_USERNAME))
        if not result.scalar_one_or_none():
            admin = Admin(
                username=settings.ADMIN_USERNAME,
                password_hash=hash_password(settings.ADMIN_PASSWORD),
            )
            db.add(admin)
            await db.commit()

    yield

    # Shutdown
    await close_redis()
    await engine.dispose()


app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

app.include_router(api_router, prefix=settings.API_V1_PREFIX)


@app.get("/")
async def admin_panel(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/health")
async def health():
    return {"status": "ok", "version": settings.VERSION}
