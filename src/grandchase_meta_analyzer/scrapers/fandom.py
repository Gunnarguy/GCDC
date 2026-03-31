from __future__ import annotations

import json
import logging
from io import StringIO
from urllib.parse import urlparse

import pandas as pd
from bs4 import BeautifulSoup

from ..settings import RuntimeSettings
from .common import dedupe_rows, fetch_html, normalize_text


LOGGER = logging.getLogger(__name__)
SKILL_KEYWORDS = {
    "damage",
    "heal",
    "buff",
    "debuff",
    "cooldown",
    "mana",
    "shield",
    "stun",
    "summon",
    "effect",
}


def _api_html_from_page_url(
    url: str, snapshot_name: str, settings: RuntimeSettings
) -> str:
    parsed = urlparse(url)
    page_name = parsed.path.removeprefix("/wiki/")
    api_url = (
        f"{parsed.scheme}://{parsed.netloc}/api.php"
        f"?action=parse&page={page_name}&prop=text&formatversion=2&format=json"
    )
    payload = json.loads(fetch_html(api_url, snapshot_name, settings))
    return payload["parse"]["text"]


def scrape_chaser_traits(settings: RuntimeSettings) -> list[dict[str, str]]:
    url = settings.config["sources"]["fandom_chaser"]
    html = _api_html_from_page_url(url, "fandom_chaser_api", settings)
    tables = pd.read_html(StringIO(html))

    rows: list[dict[str, str]] = []
    for table in tables:
        if table.shape[1] < 2:
            continue
        normalized = table.fillna("")
        for raw_row in normalized.itertuples(index=False):
            cells = [normalize_text(str(value)) for value in raw_row]
            if len(cells) < 2:
                continue
            trait_name = cells[0]
            description = cells[1]
            if (
                not trait_name
                or not description
                or trait_name.lower() in {"trait", "name"}
            ):
                continue
            if len(description) < 12:
                continue
            rows.append(
                {
                    "trait_name": trait_name,
                    "description": description,
                    "rank": cells[2] if len(cells) > 2 else "",
                    "source_page": url,
                }
            )

    unique_rows = dedupe_rows(rows, ("trait_name", "description"))
    LOGGER.info("Fandom chaser scraper extracted %s traits", len(unique_rows))
    return unique_rows


def scrape_skill_snippets(settings: RuntimeSettings) -> list[dict[str, str]]:
    url = settings.config["sources"]["fandom_skills"]
    html = _api_html_from_page_url(url, "fandom_skills_api", settings)
    soup = BeautifulSoup(html, "lxml")

    rows: list[dict[str, str]] = []
    for node in soup.select("p, li"):
        text = normalize_text(node.get_text(" ", strip=True))
        if len(text) < 40:
            continue
        lowered = text.lower()
        if not any(keyword in lowered for keyword in SKILL_KEYWORDS):
            continue
        skill_name = text.split(":", 1)[0][:80]
        rows.append(
            {
                "skill_name": skill_name,
                "description": text,
                "source_page": url,
            }
        )

    unique_rows = dedupe_rows(rows, ("description",))
    LOGGER.info("Fandom skill scraper extracted %s skill snippets", len(unique_rows))
    return unique_rows
