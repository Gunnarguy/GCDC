from __future__ import annotations

import logging
import re
import shutil
import subprocess
import time
from collections.abc import Iterable, Mapping
from typing import TypeVar

import requests

from ..paths import RAW_DATA_DIR
from ..settings import RuntimeSettings


LOGGER = logging.getLogger(__name__)
RowT = TypeVar("RowT", bound=Mapping[str, object])


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def normalize_name(value: str) -> str:
    return normalize_text(re.sub(r"\[[^\]]+\]", "", value))


def normalize_tier(value: str) -> str:
    tokens = re.findall(r"[A-Z+]+", normalize_text(value).upper())
    for token in tokens:
        if token == "S+":
            return "SS"
        if token in {"SS", "S", "A", "B", "C"}:
            return token
    return ""


def dedupe_rows(rows: Iterable[RowT], keys: tuple[str, ...]) -> list[RowT]:
    seen: set[tuple[object, ...]] = set()
    unique_rows: list[RowT] = []
    for row in rows:
        marker = tuple(row.get(key) for key in keys)
        if marker in seen:
            continue
        seen.add(marker)
        unique_rows.append(row)
    return unique_rows


def _persist_snapshot(html: str, snapshot_name: str, settings: RuntimeSettings) -> None:
    if settings.scraping.get("persist_html_snapshots", False):
        snapshot_path = RAW_DATA_DIR / f"{snapshot_name}.html"
        snapshot_path.write_text(html, encoding="utf-8")
        LOGGER.info("Saved HTML snapshot to %s", snapshot_path)


def _fetch_with_curl(url: str, timeout: int | float) -> str:
    curl_path = shutil.which("curl")
    if curl_path is None:
        raise RuntimeError("curl is not available for fallback fetches")

    result = subprocess.run(
        [
            curl_path,
            "-L",
            "--fail",
            "--silent",
            "--show-error",
            "--max-time",
            str(int(timeout)),
            "-A",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "-H",
            "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "-H",
            "Accept-Language: en-US,en;q=0.9",
            url,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def fetch_html(url: str, snapshot_name: str, settings: RuntimeSettings) -> str:
    headers = {
        "User-Agent": settings.scraping["user_agent"],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": url.rsplit("/", 1)[0],
    }
    timeout = settings.scraping["request_timeout_seconds"]
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        html = response.text
    except requests.RequestException as error:
        LOGGER.warning("requests fetch failed for %s: %s", url, error)
        html = _fetch_with_curl(url, timeout)

    _persist_snapshot(html, snapshot_name, settings)

    time.sleep(float(settings.scraping["delay_between_requests_seconds"]))
    return html
