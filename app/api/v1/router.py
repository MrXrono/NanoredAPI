from fastapi import APIRouter

from app.api.v1.auth import router as auth_router
from app.api.v1.client import router as client_router
from app.api.v1.admin import router as admin_router

api_router = APIRouter()
api_router.include_router(auth_router)
api_router.include_router(client_router)
api_router.include_router(admin_router)
