from pydantic import BaseModel, Field


class SNIEntry(BaseModel):
    domain: str
    hit_count: int = 1
    bytes_total: int = 0


class SNIBatchRequest(BaseModel):
    session_id: str
    entries: list[SNIEntry]


class SNIRawRequest(BaseModel):
    session_id: str
    raw_log: str = ""
    dns_log: str = ""


class DNSEntry(BaseModel):
    domain: str
    resolved_ip: str | None = None
    query_type: str | None = "A"
    hit_count: int = 1


class DNSBatchRequest(BaseModel):
    session_id: str
    entries: list[DNSEntry]



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


class FileSessionHeartbeatRequest(BaseModel):
    session_id: str



class FileBrowserEntry(BaseModel):
    name: str
    path: str
    is_directory: bool
    size_bytes: int | None = None
    mime_type: str | None = None
    modified_at: str | None = None
    is_image: bool = False
    thumbnail_base64: str | None = None


class FileBrowserSnapshotRequest(BaseModel):
    session_id: str
    path: str
    has_parent: bool = False
    entries: list[FileBrowserEntry] = Field(default_factory=list)
