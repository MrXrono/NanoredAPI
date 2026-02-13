import geoip2.database
import geoip2.errors

from app.core.config import settings

_reader = None


def _get_reader():
    global _reader
    if _reader is None:
        try:
            _reader = geoip2.database.Reader(settings.GEOIP_DB_PATH)
        except Exception:
            return None
    return _reader


def lookup_ip(ip: str) -> dict:
    reader = _get_reader()
    if not reader:
        return {"country": None, "city": None}
    try:
        resp = reader.city(ip)
        return {
            "country": resp.country.name,
            "city": resp.city.name,
        }
    except (geoip2.errors.AddressNotFoundError, ValueError):
        return {"country": None, "city": None}
