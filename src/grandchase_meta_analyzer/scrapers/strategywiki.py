from __future__ import annotations

import logging
import re
from io import StringIO

import pandas as pd
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
SECTION_HEADING_TAGS = ("h2", "h3", "h4")
REFERENCE_TRUST_TIER = "community_wiki"
LEGACY_GAME_ERA = "legacy_pre_2024"
HERO_GROWTH_SECTION_KEYS = {"upgrade", "evolve", "prestige", "awakening"}
IGNORED_SECTION_TITLES = {"navigation menu", "additional links", "search"}
IGNORED_REFERENCE_KEYS = {
    "namespaces",
    "views",
    "navigation",
    "tools",
    "personal_tools",
    "table_of_contents",
}
FOOTER_MARKERS = (
    "Go to top",
    "NewPP limit report",
    "Saved in parser cache",
    "Transclusion expansion time report",
)


def _closest_role_heading(table: Tag) -> str:
    for previous in table.find_all_previous(["h2", "h3", "h4"]):
        heading = _clean_heading_text(previous.get_text(" ", strip=True))
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


def _clean_heading_text(text: str) -> str:
    cleaned = re.sub(r"\[\s*edit.*$", "", str(text), flags=re.IGNORECASE)
    return normalize_text(cleaned)


def _section_key(text: str) -> str:
    cleaned = _clean_heading_text(text).lower()
    cleaned = cleaned.replace("&", "and")
    cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned).strip("_")
    return cleaned


def _collect_section_text(heading: Tag) -> str:
    parts: list[str] = []
    for sibling in heading.next_siblings:
        if isinstance(sibling, Tag) and sibling.name in SECTION_HEADING_TAGS:
            break
        if isinstance(sibling, Tag):
            text = normalize_text(sibling.get_text(" ", strip=True))
        else:
            text = normalize_text(str(sibling))
        if text:
            parts.append(text)
    content = "\n".join(parts).strip()
    for marker in FOOTER_MARKERS:
        if marker in content:
            content = content.split(marker, 1)[0].rstrip()
    return content


def _flatten_header(column: object) -> str:
    if isinstance(column, tuple):
        return normalize_text(" ".join(str(part) for part in column if str(part)))
    return normalize_text(str(column))


def _find_tables_within_section(heading: Tag) -> list[Tag]:
    tables: list[Tag] = []
    seen_ids: set[int] = set()
    for element in heading.next_elements:
        if element is heading:
            continue
        if isinstance(element, Tag) and element.name in SECTION_HEADING_TAGS:
            break
        if isinstance(element, Tag) and element.name == "table":
            marker = id(element)
            if marker in seen_ids:
                continue
            seen_ids.add(marker)
            tables.append(element)
    return tables


def _strategywiki_source_url(settings: RuntimeSettings, source_name: str) -> str:
    sources = settings.config["sources"]
    if source_name == "heroes":
        return sources.get("strategywiki_heroes") or sources["strategywiki"]
    if source_name == "hero_growth":
        return sources["strategywiki_hero_growth"]
    if source_name == "material":
        return sources["strategywiki_material"]
    raise KeyError(f"Unsupported StrategyWiki source {source_name}")


def _load_strategywiki_page(
    settings: RuntimeSettings,
    source_name: str,
    snapshot_name: str,
) -> tuple[str, BeautifulSoup]:
    url = _strategywiki_source_url(settings, source_name)
    html = fetch_html(url, snapshot_name, settings)
    return url, BeautifulSoup(html, "lxml")


def _parse_hero_table(table: Tag, role: str) -> list[dict[str, str]]:
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

        records.append(
            {
                "name_en": name,
                "role": role,
                "adventure": normalize_tier(cells[adventure_index]) or "C",
                "battle": normalize_tier(cells[battle_index]) or "C",
                "boss": normalize_tier(cells[boss_index]) or "C",
                "source": "strategywiki",
            }
        )

    return records


def _iter_reference_sections(
    soup: BeautifulSoup,
    source: str,
    source_page: str,
    allowed_keys: set[str] | None = None,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    path_by_level: dict[int, str] = {}

    for heading in soup.find_all(list(SECTION_HEADING_TAGS)):
        title = _clean_heading_text(heading.get_text(" ", strip=True))
        if not title:
            continue
        if title.lower() in IGNORED_SECTION_TITLES:
            continue
        reference_key = _section_key(title)
        if reference_key in IGNORED_REFERENCE_KEYS:
            continue
        if allowed_keys is not None and reference_key not in allowed_keys:
            continue

        heading_level = int(heading.name[1]) if heading.name[1:].isdigit() else 0
        path_by_level = {
            level: value
            for level, value in path_by_level.items()
            if level < heading_level
        }
        path_by_level[heading_level] = title

        content = _collect_section_text(heading)
        if not content:
            continue
        if "This page was last edited" in content:
            continue

        rows.append(
            {
                "source": source,
                "reference_key": reference_key,
                "title": title,
                "section_path": " > ".join(
                    path_by_level[level] for level in sorted(path_by_level)
                ),
                "content": content,
                "source_page": source_page,
                "game_era": LEGACY_GAME_ERA,
                "is_legacy_system": "1",
                "trust_tier": REFERENCE_TRUST_TIER,
            }
        )

    return rows


def scrape(settings: RuntimeSettings) -> list[dict[str, str]]:
    url, soup = _load_strategywiki_page(settings, "heroes", "strategywiki_heroes")

    records: list[dict[str, str]] = []
    for table in soup.find_all("table"):
        role = _closest_role_heading(table)
        records.extend(_parse_hero_table(table, role))

    unique_records = dedupe_rows(records, ("name_en", "role"))
    LOGGER.info(
        "StrategyWiki scraper extracted %s hero rows from %s",
        len(unique_records),
        url,
    )
    return unique_records


def scrape_reference_notes(settings: RuntimeSettings) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    hero_growth_url, hero_growth_soup = _load_strategywiki_page(
        settings, "hero_growth", "strategywiki_hero_growth"
    )
    rows.extend(
        _iter_reference_sections(
            hero_growth_soup,
            "strategywiki_hero_growth",
            hero_growth_url,
            allowed_keys=HERO_GROWTH_SECTION_KEYS,
        )
    )

    material_url, material_soup = _load_strategywiki_page(
        settings, "material", "strategywiki_material"
    )
    rows.extend(
        _iter_reference_sections(
            material_soup,
            "strategywiki_material",
            material_url,
        )
    )

    unique_rows = dedupe_rows(rows, ("source", "reference_key", "section_path"))
    LOGGER.info(
        "StrategyWiki scraper extracted %s reference-note rows",
        len(unique_rows),
    )
    return unique_rows


def scrape_hero_growth_values(settings: RuntimeSettings) -> list[dict[str, str]]:
    url, soup = _load_strategywiki_page(
        settings, "hero_growth", "strategywiki_hero_growth"
    )
    rows = _extract_hero_growth_value_rows(soup, url)
    LOGGER.info(
        "StrategyWiki scraper extracted %s hero-growth table cells",
        len(rows),
    )
    return rows


def _extract_hero_growth_value_rows(
    soup: BeautifulSoup,
    source_page: str,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []

    for heading in soup.find_all(list(SECTION_HEADING_TAGS)):
        title = _clean_heading_text(heading.get_text(" ", strip=True))
        reference_key = _section_key(title)
        if reference_key not in HERO_GROWTH_SECTION_KEYS:
            continue

        for table in _find_tables_within_section(heading):
            try:
                tables = pd.read_html(StringIO(str(table)))
            except ValueError:
                continue
            if not tables:
                continue

            frame = tables[0].fillna("")
            frame.columns = [
                _flatten_header(column) for column in frame.columns.tolist()
            ]
            headers = [str(column) for column in frame.columns.tolist()]
            row_label_index = 0
            first_column_values = [
                normalize_text(str(value))
                for value in frame.iloc[:, 0].tolist()
                if normalize_text(str(value))
            ]
            if (
                len(headers) > 1
                and headers[0].lower().startswith("unnamed")
                and headers[1].lower().startswith("unnamed")
                and first_column_values
                and len(set(first_column_values)) == 1
            ):
                row_label_index = 1
            row_header = headers[row_label_index] if headers else "row"

            for raw_row in frame.itertuples(index=False):
                cells = [normalize_text(str(value)) for value in raw_row]
                if not any(cells):
                    continue

                row_label = cells[row_label_index]
                if not row_label or row_label.lower() == row_header.lower():
                    continue
                if row_label.lower() in {"hero", "evolve", reference_key}:
                    continue

                for index, value in enumerate(cells):
                    if index <= row_label_index:
                        continue
                    if not value or value == "-":
                        continue
                    rows.append(
                        {
                            "source": "strategywiki_hero_growth",
                            "reference_key": reference_key,
                            "title": title,
                            "row_label": row_label,
                            "column_label": (
                                headers[index]
                                if index < len(headers)
                                else f"column_{index}"
                            ),
                            "value_text": value,
                            "source_page": source_page,
                            "game_era": LEGACY_GAME_ERA,
                            "is_legacy_system": "1",
                            "trust_tier": REFERENCE_TRUST_TIER,
                        }
                    )

    unique_rows = dedupe_rows(
        rows,
        ("source", "reference_key", "row_label", "column_label", "value_text"),
    )
    return unique_rows
