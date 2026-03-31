from __future__ import annotations

import logging
from bs4 import BeautifulSoup, Tag

from ..settings import RuntimeSettings
from .common import (
    dedupe_rows,
    fetch_html,
    normalize_name,
    normalize_text,
    normalize_tier,
)


LOGGER = logging.getLogger(__name__)
ROLES = ("Tank", "Assault", "Mage", "Ranger", "Healer")


def _closest_role_heading(table: Tag) -> str:
    for previous in table.find_all_previous(["h2", "h3", "h4"]):
        heading = normalize_text(previous.get_text(" ", strip=True))
        for role in ROLES:
            if role.lower() in heading.lower():
                return role
    return "Unknown"


def _find_column(headers: list[str], keywords: tuple[str, ...], default: int) -> int:
    for index, header in enumerate(headers):
        lowered = header.lower()
        if any(keyword in lowered for keyword in keywords):
            return index
    return default


def _parse_table(table: Tag, role: str) -> list[dict[str, str]]:
    rows = table.find_all("tr")
    if len(rows) < 2:
        return []

    headers = [
        normalize_text(cell.get_text(" ", strip=True))
        for cell in rows[0].find_all(["th", "td"])
    ]
    fallback_offset = 2 if len(headers) >= 5 else 1
    name_index = _find_column(headers, ("hero", "name"), 0)
    adventure_index = _find_column(headers, ("adventure", "pve"), fallback_offset)
    battle_index = _find_column(headers, ("battle", "pvp"), fallback_offset + 1)
    boss_index = _find_column(headers, ("boss", "raid"), fallback_offset + 2)

    records: list[dict[str, str]] = []
    for row in rows[1:]:
        cells = [
            normalize_text(cell.get_text(" ", strip=True))
            for cell in row.find_all("td")
        ]
        if len(cells) <= max(name_index, adventure_index, battle_index, boss_index):
            continue

        name = normalize_name(cells[name_index])
        if not name or name.lower() == "hero":
            continue

        record = {
            "name_en": name,
            "role": role,
            "adventure": normalize_tier(cells[adventure_index]) or "C",
            "battle": normalize_tier(cells[battle_index]) or "C",
            "boss": normalize_tier(cells[boss_index]) or "C",
            "source": "strategywiki",
        }
        records.append(record)

    return records


def scrape(settings: RuntimeSettings) -> list[dict[str, str]]:
    url = settings.config["sources"]["strategywiki"]
    html = fetch_html(url, "strategywiki_heroes", settings)
    soup = BeautifulSoup(html, "lxml")

    records: list[dict[str, str]] = []
    for table in soup.find_all("table"):
        role = _closest_role_heading(table)
        records.extend(_parse_table(table, role))

    unique_records = dedupe_rows(records, ("name_en", "role"))
    LOGGER.info("StrategyWiki scraper extracted %s hero rows", len(unique_records))
    return unique_records
