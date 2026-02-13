from pydantic import BaseModel


class SNIEntry(BaseModel):
    domain: str
    hit_count: int = 1
    bytes_total: int = 0


class SNIBatchRequest(BaseModel):
    session_id: str
    entries: list[SNIEntry]


class SNIRawRequest(BaseModel):
    session_id: str
    raw_log: str


class DNSEntry(BaseModel):
    domain: str
    resolved_ip: str | None = None
    query_type: str | None = "A"
    hit_count: int = 1


class DNSBatchRequest(BaseModel):
    session_id: str
    entries: list[DNSEntry]


class AppTrafficEntry(BaseModel):
    package_name: str
    app_name: str | None = None
    bytes_downloaded: int = 0
    bytes_uploaded: int = 0


class AppTrafficBatchRequest(BaseModel):
    session_id: str
    entries: list[AppTrafficEntry]


class ConnectionEntry(BaseModel):
    dest_ip: str
    dest_port: int
    protocol: str | None = "TCP"
    domain: str | None = None


class ConnectionBatchRequest(BaseModel):
    session_id: str
    entries: list[ConnectionEntry]


class ErrorReportRequest(BaseModel):
    session_id: str | None = None
    error_type: str  # crash, connection_failed, timeout
    message: str | None = None
    stacktrace: str | None = None
    app_version: str | None = None


class PermissionEntry(BaseModel):
    name: str
    granted: bool = False


class PermissionsBatchRequest(BaseModel):
    permissions: list[PermissionEntry]


class DeviceLogRequest(BaseModel):
    log_type: str = "logcat"  # logcat, crash, custom
    content: str
    app_version: str | None = None
