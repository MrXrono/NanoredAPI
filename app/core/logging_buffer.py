import threading
from collections import deque
from datetime import datetime, timezone


class LoggingBuffer:
    def __init__(self, maxlen: int = 5000):
        self._buffer: deque = deque(maxlen=maxlen)
        self._lock = threading.Lock()
        self.enabled: bool = False

    def add(self, log_type: str, message: str, details: dict | None = None):
        if not self.enabled:
            return
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "type": log_type,
            "message": message,
            "details": details or {},
        }
        with self._lock:
            self._buffer.append(entry)

    TELEMETRY_KEYWORDS = (
        "Регистрация", "перерегистрировано", "Новое устройство",
        "Сессия начата", "Сессия завершена", "Автозакрытие",
        "SNI batch", "DNS batch", "App traffic batch", "Connections batch",
        "Permissions", "Лог от устройства", "Ошибка от устройства",
        "Создан аккаунт",
    )

    def get_logs(self, log_type: str | None = None, limit: int = 200, offset: int = 0) -> list[dict]:
        with self._lock:
            items = list(self._buffer)
        if log_type and log_type != "all":
            if log_type == "telemetry":
                items = [i for i in items if any(kw in i["message"] for kw in self.TELEMETRY_KEYWORDS)]
            else:
                items = [i for i in items if i["type"] == log_type]
        items.reverse()
        return items[offset:offset + limit]

    def clear(self):
        with self._lock:
            self._buffer.clear()

    def start(self):
        self.enabled = True

    def stop(self):
        self.enabled = False


logging_buffer = LoggingBuffer()
