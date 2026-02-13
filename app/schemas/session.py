from pydantic import BaseModel


class SessionStartRequest(BaseModel):
    server_address: str | None = None
    protocol: str | None = None
    network_type: str | None = None  # wifi / mobile
    wifi_ssid: str | None = None
    carrier: str | None = None
    latency_ms: int | None = None
    battery_level: int | None = None


class SessionStartResponse(BaseModel):
    session_id: str


class SessionEndRequest(BaseModel):
    session_id: str
    bytes_downloaded: int = 0
    bytes_uploaded: int = 0
    connection_count: int = 0
    reconnect_count: int = 0


class SessionInfoResponse(BaseModel):
    id: str
    device_id: str
    server_address: str | None
    protocol: str | None
    client_ip: str | None
    client_country: str | None
    client_city: str | None
    network_type: str | None
    bytes_downloaded: int
    bytes_uploaded: int
    connection_count: int
    latency_ms: int | None
    battery_level: int | None
    connected_at: str
    disconnected_at: str | None

    class Config:
        from_attributes = True
