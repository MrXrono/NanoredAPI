import logging
import sys
import threading
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from app.core.config import settings


_PROJECT_ROOT = Path(__file__).resolve().parents[1]


class _ProjectFunctionProfiler:
    def __init__(self):
        self._logger = logging.getLogger("app.function_trace")
        self._guard = threading.local()
        raw_excludes = (settings.FUNCTION_CALL_LOG_EXCLUDE_MODULES or "").strip()
        self._exclude_prefixes = tuple(
            item.strip() for item in raw_excludes.split(",") if item.strip()
        )

    def _is_allowed(self, frame) -> bool:
        module = frame.f_globals.get("__name__", "")
        if not module.startswith("app"):
            return False
        if self._exclude_prefixes and any(module.startswith(prefix) for prefix in self._exclude_prefixes):
            return False
        filename = frame.f_code.co_filename
        if not filename:
            return False
        try:
            file_path = Path(filename).resolve()
        except Exception:
            return False
        return str(file_path).startswith(str(_PROJECT_ROOT))

    def callback(self, frame, event, arg):
        if event not in ("call", "exception"):
            return
        if not self._is_allowed(frame):
            return

        if getattr(self._guard, "active", False):
            return

        self._guard.active = True
        try:
            module = frame.f_globals.get("__name__", "unknown")
            function_name = frame.f_code.co_name
            line_no = frame.f_lineno
            if event == "call":
                self._logger.info("CALL %s.%s:%s", module, function_name, line_no)
            else:
                exc_type = arg[0].__name__ if isinstance(arg, tuple) and arg and arg[0] else "Exception"
                self._logger.error("EXCEPTION %s.%s:%s %s", module, function_name, line_no, exc_type)
        finally:
            self._guard.active = False


def _get_log_level() -> int:
    level = (settings.LOG_LEVEL or "INFO").strip().upper()
    return getattr(logging, level, logging.INFO)


def setup_logging() -> None:
    root_logger = logging.getLogger()
    if getattr(root_logger, "_nanored_logging_configured", False):
        return

    log_level = _get_log_level()
    root_logger.setLevel(log_level)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(log_level)
    stream_handler.setFormatter(formatter)

    root_logger.addHandler(stream_handler)

    try:
        log_dir = Path(settings.LOG_DIR)
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file_path = log_dir / settings.LOG_FILE_NAME

        file_handler = TimedRotatingFileHandler(
            filename=str(log_file_path),
            when=settings.LOG_FILE_ROTATION_WHEN,
            interval=max(1, settings.LOG_FILE_ROTATION_INTERVAL),
            backupCount=max(1, settings.LOG_FILE_RETENTION_DAYS),
            encoding="utf-8",
            utc=True,
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    except Exception as exc:
        logging.getLogger(__name__).warning("Failed to initialize file logger: %s", exc)

    for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access", "fastapi"):
        log = logging.getLogger(logger_name)
        log.setLevel(log_level)

    root_logger._nanored_logging_configured = True


def setup_function_call_logging() -> None:
    if not settings.FUNCTION_CALL_LOGGING_ENABLED:
        return

    root_logger = logging.getLogger()
    if getattr(root_logger, "_nanored_profiler_configured", False):
        return

    profiler = _ProjectFunctionProfiler()
    sys.setprofile(profiler.callback)
    threading.setprofile(profiler.callback)

    root_logger._nanored_profiler_configured = True
    logging.getLogger(__name__).info("Function call profiler enabled for project modules")
