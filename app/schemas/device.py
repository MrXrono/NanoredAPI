from pydantic import BaseModel


class DeviceRegisterRequest(BaseModel):
    android_id: str
    device_model: str | None = None
    manufacturer: str | None = None
    android_version: str | None = None
    api_level: int | None = None
    app_version: str | None = None
    screen_resolution: str | None = None
    dpi: int | None = None
    language: str | None = None
    timezone: str | None = None
    is_rooted: bool | None = False
    carrier: str | None = None
    ram_total_mb: int | None = None
    account_id: str | None = None


class DeviceRegisterResponse(BaseModel):
    device_id: str
    api_key: str


class DeviceInfoResponse(BaseModel):
    id: str
    android_id: str
    device_model: str | None
    manufacturer: str | None
    android_version: str | None
    app_version: str | None
    is_rooted: bool | None
    carrier: str | None
    is_blocked: bool
    note: str | None
    created_at: str
    last_seen_at: str | None

    class Config:
        from_attributes = True
