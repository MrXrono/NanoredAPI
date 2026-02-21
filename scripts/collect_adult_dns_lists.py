#!/usr/bin/env python3
"""Collect DNS names from three sources and merge into one unique TXT.

Sources:
- blocklistproject (porn-nl.txt)
- oisd/nsfw page (auto-discover .txt links)
- v2fly category-porn (recursively process include:<name>)
"""

from __future__ import annotations

import argparse
import os
import re
from collections import defaultdict
from pathlib import Path
from typing import Set
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


TXT_SOURCES = {
    "blocklistproject": "https://blocklistproject.github.io/Lists/alt-version/porn-nl.txt",
    "v2fly_root": "https://raw.githubusercontent.com/v2fly/domain-list-community/refs/heads/master/data/category-porn",
    "oisd_root": "https://oisd.nl/includedlists/nsfw",
}


def make_session(timeout: int) -> requests.Session:
    s = requests.Session()
    retry = Retry(total=4, connect=4, read=4, backoff_factor=0.5, status_forcelist=(429, 500, 502, 503, 504))
    adapter = HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=16)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers["User-Agent"] = "Mozilla/5.0 (compatible; DNSMerge/1.0)"
    s.request_timeout = timeout
    return s


def fetch_text(session: requests.Session, url: str, timeout: int) -> str:
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    enc = r.encoding or "utf-8"
    return r.content.decode(enc, errors="replace")


def normalize_domain(token: str) -> str | None:
    if not token:
        return None
    t = token.strip().lower()
    if not t:
        return None
    if t[0] in {"#", "!", ";"}:
        return None

    # remove common list syntax
    if t.startswith("@@||"):
        t = t[3:]
    if t.startswith("||"):
        t = t[2:]
    if t.startswith("*."):
        t = t[2:]
    if t.startswith("www."):
        t = t[4:]

    # comments inline
    for sep in (" #", "\t#", " //", " ;"):
        if sep in t:
            t = t.split(sep, 1)[0].strip()
    for sep in ("#", ";"):
        if t.startswith(sep):
            return None

    t = t.split("^")[0]
    t = t.replace("[.]", ".")
    t = t.split("?")[0].split("/")[0].split(":")[0]

    if t.startswith("http://") or t.startswith("https://"):
        try:
            u = urlparse(t)
            t = (u.hostname or "").lower()
        except Exception:
            t = t.replace("https://", "").replace("http://", "")
            t = t.split("/")[0]

    if not t:
        return None
    if t.startswith(".") or t.endswith("."):
        return None
    if ".." in t:
        return None

    # ignore ipv4
    if re.fullmatch(r"\d{1,3}(?:\.\d{1,3}){3}", t):
        return None

    # keep only domain-looking tokens
    if "." not in t or t.startswith("-") or t.endswith("-"):
        return None

    parts = t.split(".")
    for part in parts:
        if not part or len(part) > 63 or part.startswith("-") or part.endswith("-"):
            return None
        if not re.fullmatch(r"[a-z0-9-]+", part) and not re.fullmatch(r"xn--[a-z0-9-]+", part):
            return None

    return t


def parse_domains(text: str) -> Set[str]:
    domains: Set[str] = set()
    for line in text.splitlines():
        raw = line.strip()
        if not raw:
            continue
        if raw.startswith(("#", "!", ";")):
            continue

        # split host-file rows: ip domain domain
        if re.match(r"^(?:0\.0\.0\.0|127\.0\.0\.1|::1)\s+", raw):
            for token in re.split(r"\s+", raw)[1:]:
                token = normalize_domain(token)
                if token:
                    domains.add(token)
            continue

        # split by whitespace and comma for most text lists
        for token in re.split(r"[\s,]+", raw):
            if not token:
                continue
            token = normalize_domain(token)
            if token:
                domains.add(token)
    return domains


def extract_v2fly_includes(text: str) -> list[str]:
    includes: list[str] = []
    for line in text.splitlines():
        s = line.strip()
        if not s or not s.lower().startswith("include:"):
            continue
        val = s.split(":", 1)[1].strip()
        if not val:
            continue
        # keep raw http(s) includes
        if val.startswith("http://") or val.startswith("https://"):
            includes.append(val)
        else:
            # v2fly uses sibling files in data/
            includes.append(f"https://raw.githubusercontent.com/v2fly/domain-list-community/master/data/{val}")
    return includes


def collect_txt_urls_oisd(html: str, base_url: str) -> list[str]:
    hrefs = set(re.findall(r"href=[\"']([^\"']+)[\"']", html, flags=re.I))
    out = []
    for h in hrefs:
        token = h.strip()
        if token.endswith("</a>"):
            token = token[:-4]
        if ".txt" not in token.lower():
            continue
        full = urljoin(base_url, token)
        parsed = urlparse(full)
        if parsed.path.lower().endswith(".txt"):
            out.append(full)
    return sorted(set(out))


def collect_all(session: requests.Session, timeout: int):
    domain_set: Set[str] = set()
    source_stats = defaultdict(int)
    source_files = 0

    # BlocklistProject direct
    try:
        txt = fetch_text(session, TXT_SOURCES["blocklistproject"], timeout)
        domains = parse_domains(txt)
        domain_set |= domains
        source_stats[TXT_SOURCES["blocklistproject"]] = len(domains)
        source_files += 1
    except Exception as exc:
        print(f"[warn] blocklistproject failed: {exc}")

    # v2fly with recursive include:
    try:
        v2_queue = [TXT_SOURCES["v2fly_root"]]
        seen_v2 = set(v2_queue)
        while v2_queue:
            url = v2_queue.pop(0)
            txt = fetch_text(session, url, timeout)
            source_files += 1
            domains = parse_domains(txt)
            source_stats[url] = len(domains)
            domain_set |= domains

            for inc in extract_v2fly_includes(txt):
                if inc not in seen_v2:
                    seen_v2.add(inc)
                    v2_queue.append(inc)
    except Exception as exc:
        print(f"[warn] v2fly failed: {exc}")

    # oisd: discover all linked txt and collect
    try:
        oisd_html = fetch_text(session, TXT_SOURCES["oisd_root"], timeout)
        for u in collect_txt_urls_oisd(oisd_html, TXT_SOURCES["oisd_root"]):
            try:
                txt = fetch_text(session, u, timeout)
                source_files += 1
                domains = parse_domains(txt)
                source_stats[u] = len(domains)
                domain_set |= domains
            except Exception as exc:
                print(f"[warn] oisd source failed: {u} -> {exc}")
    except Exception as exc:
        print(f"[warn] oisd failed: {exc}")

    return domain_set, source_stats, source_files


def main() -> int:
    default_output = os.getenv("ADULT_DOMAINS_OUTPUT")
    if not default_output:
        if Path("/app/data").exists():
            default_output = "/app/data/artifacts/adult_domains_merged.txt"
        else:
            default_output = "data/artifacts/adult_domains_merged.txt"

    p = argparse.ArgumentParser()
    p.add_argument(
        "--output",
        default=default_output,
        help="Output txt path (default: ADULT_DOMAINS_OUTPUT or /app/data/artifacts/... in Docker)",
    )
    p.add_argument("--timeout", type=int, default=30, help="HTTP timeout")
    args = p.parse_args()

    session = make_session(args.timeout)
    domains, source_stats, files = collect_all(session, args.timeout)

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as f:
        for domain in sorted(domains):
            f.write(domain + "\n")

    total_unique = len(domains)
    size = out.stat().st_size

    print("\nРезультат:")
    print(f"  Источников файлов обработано: {files}")
    print(f"  Всего уникальных DNS: {total_unique}")
    print(f"  Выходной файл: {out.resolve()}")
    print(f"  Размер файла: {size} байт (~{size / 1024:.2f} KB, ~{size / (1024*1024):.2f} MB)")
    print("\nTOP источники по количеству извлеченных записей:")
    for src, cnt in sorted(source_stats.items(), key=lambda x: x[1], reverse=True)[:30]:
        print(f"  {cnt:9d}  {src}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
