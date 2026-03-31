from __future__ import annotations

import hashlib
import logging
import re
from urllib.parse import unquote, urljoin

from bs4 import BeautifulSoup, Tag

from ..settings import RuntimeSettings, load_aliases
from .common import dedupe_rows, fetch_html, normalize_text


LOGGER = logging.getLogger(__name__)
SECTION_HEADING_TAGS = ("h2", "h3", "h4")
DETAIL_SECTION_LABELS = {
    "skill": "Skill",
    "soul_imprint": "Soul Imprint",
    "transcendence": "Transcendence",
}
NOTABLE_NOTE_KEYS = {
    "ex": ("former_hero", "Former Hero"),
    "soul imprint": ("soul_imprint", "Soul Imprint"),
    "transcendental awakening": (
        "transcendental_awakening",
        "Transcendental Awakening",
    ),
    "special hero": ("special_hero", "Special Hero"),
}
FEATURE_PATTERNS = {
    "characteristics": (re.compile(r"Characteristic selectable", re.IGNORECASE),),
    "chaser": (
        re.compile(r"Chaser can grow", re.IGNORECASE),
        re.compile(r"Chaser can be opened or grown", re.IGNORECASE),
        re.compile(r"Chaser attribute points not used", re.IGNORECASE),
    ),
    "transcendental_awakening": (
        re.compile(r"transcendental awakening possible", re.IGNORECASE),
        re.compile(r"transcendence awakening possible", re.IGNORECASE),
    ),
    "soul_imprint": (
        re.compile(
            r"Soul imprint growth possible[^\n]*",
            re.IGNORECASE,
        ),
        re.compile(
            r"Soul imprint[^\n]*remaining points (?:available|not used)",
            re.IGNORECASE,
        ),
    ),
}


def _attribute_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return " ".join(str(item) for item in value)
    return str(value)


def _extract_excerpt(page_text: str, alias: str, span: int = 80) -> str:
    position = page_text.find(alias)
    if position < 0:
        return ""
    start = max(0, position - span)
    end = min(len(page_text), position + len(alias) + span)
    return normalize_text(page_text[start:end])


def _is_hero_anchor(anchor: Tag) -> bool:
    href = _attribute_text(anchor.get("href", ""))
    title = _attribute_text(anchor.get("title", ""))
    lowered_title = title.lower()
    return (
        href.startswith("/w/")
        and "grand chase" in lowered_title
        and "kakao" in lowered_title
    )


def _decoded_href(href: str) -> str:
    return unquote(href)


def _extract_title_base(title: str) -> str:
    primary = title.split("/", 1)[0]
    return normalize_text(primary.split("(", 1)[0])


def _extract_href_base(decoded_href: str) -> str:
    base = decoded_href.removeprefix("/w/").split("/", 1)[0]
    return normalize_text(base.split("(", 1)[0])


def _resolve_canonical_name(
    title_base: str,
    href_base: str,
    alias_map: dict[str, list[str]],
) -> tuple[str, str]:
    normalized_title = normalize_text(title_base)
    normalized_href = normalize_text(href_base)

    for english_name, aliases in alias_map.items():
        if normalized_title.lower() == english_name.lower():
            matched_alias = next(
                (alias for alias in aliases if alias and alias in normalized_href),
                aliases[0] if aliases else normalized_href,
            )
            return english_name, matched_alias

        for alias in aliases:
            if alias and alias in normalized_href:
                return english_name, alias

    return normalized_title or normalized_href, normalized_href


def _extract_marker(container: Tag) -> str:
    for sup in container.find_all("sup"):
        text = normalize_text(sup.get_text(" ", strip=True)).strip("()")
        if re.fullmatch(r"[A-Z]", text) or re.fullmatch(r"\d+", text):
            return text
    return ""


def _classify_variant(title: str, decoded_href: str, marker: str) -> tuple[str, str]:
    lowered = f"{title} {decoded_href}".lower()
    if "former hero" in lowered or "전직 영웅" in decoded_href:
        return "former", "T"
    if "special hero" in lowered or "스페셜 영웅" in decoded_href:
        return "special", "S"
    if marker in {"T", "S", "X"}:
        if marker == "T":
            return "former", marker
        if marker == "S":
            return "special", marker
        return "base", marker
    return "base", ""


def _extract_variant_rows(
    soup: BeautifulSoup,
    alias_map: dict[str, list[str]],
) -> list[dict[str, str]]:
    rows_by_href: dict[str, dict[str, str]] = {}

    for container in soup.select("div[style*='width:calc(100% / 6)']"):
        anchors = [
            anchor
            for anchor in container.find_all("a", href=True, title=True)
            if _is_hero_anchor(anchor)
        ]
        if not anchors:
            continue

        primary = anchors[0]
        href = _attribute_text(primary.get("href", ""))
        title = normalize_text(_attribute_text(primary.get("title", "")))
        decoded_href = _decoded_href(href)
        title_base = _extract_title_base(title)
        href_base = _extract_href_base(decoded_href)
        canonical_name, matched_alias = _resolve_canonical_name(
            title_base, href_base, alias_map
        )
        marker = _extract_marker(container)
        variant_kind, variant_suffix = _classify_variant(title, decoded_href, marker)
        note_excerpt = normalize_text(container.get_text(" ", strip=True))[:240]

        row = {
            "name_ko": matched_alias,
            "name_en_guess": canonical_name,
            "variant_name_en": title_base or canonical_name,
            "rarity": "SS",
            "variant_kind": variant_kind,
            "variant_suffix": variant_suffix,
            "availability_marker": marker,
            "variant_title": title,
            "variant_href": href,
            "note_excerpt": note_excerpt,
            "source": "namuwiki",
        }

        existing = rows_by_href.get(href)
        if existing is None:
            rows_by_href[href] = row
            continue
        if not existing["availability_marker"] and row["availability_marker"]:
            rows_by_href[href] = row
            continue
        if len(row["note_excerpt"]) > len(existing["note_excerpt"]):
            rows_by_href[href] = row

    return dedupe_rows(
        rows_by_href.values(), ("name_en_guess", "variant_kind", "variant_href")
    )


def _extract_source_notes(soup: BeautifulSoup, url: str) -> list[dict[str, str]]:
    notes: list[dict[str, str]] = []
    legend_candidates = [
        tag
        for tag in soup.find_all(["div", "td"])
        if "Event limited edition special hero" in tag.get_text(" ", strip=True)
        and "former hero" in tag.get_text(" ", strip=True)
        and "A hero who cannot grow" in tag.get_text(" ", strip=True)
    ]
    legend_node = (
        min(
            legend_candidates,
            key=lambda tag: len(normalize_text(tag.get_text(" ", strip=True))),
        )
        if legend_candidates
        else None
    )
    if legend_node is not None:
        notes.append(
            {
                "source": "namuwiki",
                "note_key": "legend",
                "title": "Legend",
                "content": normalize_text(legend_node.get_text(" ", strip=True)),
                "source_page": url,
            }
        )

    for heading in soup.find_all(["h3", "h4"]):
        heading_text = normalize_text(heading.get_text(" ", strip=True)).lower()
        key_and_title = None
        for candidate, candidate_value in NOTABLE_NOTE_KEYS.items():
            if candidate in heading_text:
                key_and_title = candidate_value
                break
        if key_and_title is None:
            continue

        content_node = heading.find_next_sibling("div")
        if content_node is None:
            continue
        content = normalize_text(content_node.get_text(" ", strip=True))
        if not content:
            continue
        note_key, title = key_and_title
        notes.append(
            {
                "source": "namuwiki",
                "note_key": note_key,
                "title": title,
                "content": content,
                "source_page": url,
            }
        )

    return dedupe_rows(notes, ("source", "note_key"))


def _absolute_variant_url(settings: RuntimeSettings, href: str) -> str:
    base_url = settings.config["sources"]["namuwiki_ss"]
    return urljoin(base_url, href)


def _variant_snapshot_name(variant: dict[str, str]) -> str:
    href = variant.get("variant_href", "")
    digest = hashlib.sha1(href.encode("utf-8")).hexdigest()[:10]
    title = re.sub(r"[^a-z0-9]+", "_", variant.get("variant_title", "").lower())
    title = title.strip("_") or variant.get("name_en_guess", "variant").lower()
    title = title[:40]
    return f"namuwiki_variant_{title}_{digest}"


def _heading_text(heading: Tag) -> str:
    return normalize_text(heading.get_text(" ", strip=True))


def _heading_level(heading: Tag) -> int:
    try:
        return int(heading.name[1])
    except (IndexError, ValueError):
        return 0


def _strip_heading_prefix(text: str) -> str:
    return normalize_text(re.sub(r"^\d+(?:\.\d+)*\.\s*", "", text))


def _classify_detail_section(text: str) -> tuple[str, str] | None:
    cleaned = _strip_heading_prefix(text)
    lowered = cleaned.lower()
    if lowered == "skill":
        return "skill", DETAIL_SECTION_LABELS["skill"]
    if lowered.startswith("soul imprint"):
        return "soul_imprint", cleaned
    if "transcend" in lowered:
        return "transcendence", cleaned
    return None


def _heading_anchor_id(heading: Tag) -> str:
    anchor = heading.find("a", id=True)
    if anchor is None:
        return ""
    return normalize_text(_attribute_text(anchor.get("id", "")))


def _skill_entry_from_heading(
    heading_text: str,
    section_key: str,
) -> tuple[str, str, str] | None:
    cleaned = _strip_heading_prefix(heading_text)
    stage = "imprint" if section_key == "soul_imprint" else "base"

    enhancement_match = re.match(
        r"^\[Enhancement\s+([IVX]+)\]\s*Chaser\s*:\s*(.+)$",
        cleaned,
        re.IGNORECASE,
    )
    if enhancement_match:
        numeral = enhancement_match.group(1).lower()
        return f"enhancement_{numeral}", "chaser", enhancement_match.group(2).strip()

    passive_match = re.match(r"^Passive\s*-\s*(.+)$", cleaned, re.IGNORECASE)
    if passive_match:
        return stage, "passive", passive_match.group(1).strip()

    active_match = re.match(
        r"^(?:Skill\s*(\d+)|(\d+)\s*Skill)\s*-\s*(.+)$",
        cleaned,
        re.IGNORECASE,
    )
    if active_match:
        return stage, "active", active_match.group(3).strip()

    chaser_match = re.match(r"^Chaser\s*:\s*(.+)$", cleaned, re.IGNORECASE)
    if chaser_match:
        return stage, "chaser", chaser_match.group(1).strip()

    return None


def _collect_heading_block_text(heading: Tag) -> str:
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

    description = "\n".join(parts)
    description = re.sub(
        r"^(active|passive|chaser)\s+", "", description, flags=re.IGNORECASE
    )
    return description.strip()


def _extract_variant_page_sections(
    soup: BeautifulSoup,
    variant: dict[str, str],
    source_page: str,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    path_by_level: dict[int, str] = {}

    for heading in soup.find_all(list(SECTION_HEADING_TAGS)):
        heading_title = _strip_heading_prefix(_heading_text(heading))
        if not heading_title:
            continue

        heading_level = _heading_level(heading)
        path_by_level = {
            level: title
            for level, title in path_by_level.items()
            if level < heading_level
        }
        path_by_level[heading_level] = heading_title

        content = _collect_heading_block_text(heading)
        if not content:
            continue

        rows.append(
            {
                "variant_href": variant.get("variant_href", ""),
                "name_en_guess": variant.get("name_en_guess", ""),
                "variant_kind": variant.get("variant_kind", ""),
                "heading_level": str(heading_level),
                "heading_id": _heading_anchor_id(heading),
                "heading_title": heading_title,
                "section_path": " > ".join(
                    path_by_level[level] for level in sorted(path_by_level)
                ),
                "content": content,
                "source_page": source_page,
            }
        )

    return dedupe_rows(
        rows,
        ("variant_href", "heading_id", "heading_title"),
    )


def _extract_variant_page_skills(
    soup: BeautifulSoup,
    variant: dict[str, str],
    source_page: str,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    current_section_key: str | None = None
    current_section_title = ""

    for heading in soup.find_all(["h2", "h3", "h4"]):
        heading_text = _heading_text(heading)
        if heading.name == "h2":
            section = _classify_detail_section(heading_text)
            if section is None:
                current_section_key = None
                current_section_title = ""
            else:
                current_section_key, current_section_title = section
            continue

        if current_section_key is None:
            continue

        parsed = _skill_entry_from_heading(heading_text, current_section_key)
        if parsed is None:
            continue

        skill_stage, skill_type, skill_name = parsed
        description = _collect_heading_block_text(heading)
        if not description:
            continue

        rows.append(
            {
                "variant_href": variant.get("variant_href", ""),
                "name_en_guess": variant.get("name_en_guess", ""),
                "variant_kind": variant.get("variant_kind", ""),
                "section_key": current_section_key,
                "section_title": current_section_title,
                "heading_id": _heading_anchor_id(heading),
                "skill_stage": skill_stage,
                "skill_type": skill_type,
                "skill_name": skill_name,
                "description": description,
                "source_page": source_page,
            }
        )

    return dedupe_rows(
        rows,
        ("variant_href", "heading_id", "section_key", "skill_name"),
    )


def _extract_variant_page_features(
    soup: BeautifulSoup,
    variant: dict[str, str],
    source_page: str,
) -> list[dict[str, str]]:
    candidate_texts = [
        normalize_text(node.get_text(" ", strip=True))
        for node in soup.select("div.wiki-paragraph, td, span, strong")
    ]
    candidate_texts = [text for text in candidate_texts if text]

    rows: list[dict[str, str]] = []
    for feature_key, patterns in FEATURE_PATTERNS.items():
        matched_text = next(
            (
                text
                for text in candidate_texts
                if any(pattern.search(text) for pattern in patterns)
            ),
            "",
        )
        if not matched_text:
            continue
        rows.append(
            {
                "variant_href": variant.get("variant_href", ""),
                "feature_key": feature_key,
                "feature_value": matched_text,
                "source_page": source_page,
            }
        )

    return dedupe_rows(rows, ("variant_href", "feature_key"))


def scrape_variant_details(
    settings: RuntimeSettings,
    variants: list[dict[str, str]] | None = None,
) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    variant_rows = variants or scrape(settings)
    section_rows: list[dict[str, str]] = []
    skill_rows: list[dict[str, str]] = []
    feature_rows: list[dict[str, str]] = []

    for variant in variant_rows:
        href = normalize_text(variant.get("variant_href", ""))
        if not href:
            continue
        url = _absolute_variant_url(settings, href)
        snapshot_name = _variant_snapshot_name(variant)
        try:
            html = fetch_html(url, snapshot_name, settings)
            soup = BeautifulSoup(html, "lxml")
            section_rows.extend(_extract_variant_page_sections(soup, variant, url))
            skill_rows.extend(_extract_variant_page_skills(soup, variant, url))
            feature_rows.extend(_extract_variant_page_features(soup, variant, url))
        except Exception as error:  # noqa: BLE001
            LOGGER.warning("NamuWiki detail scrape failed for %s: %s", url, error)

    unique_sections = dedupe_rows(
        section_rows,
        ("variant_href", "heading_id", "heading_title"),
    )
    unique_skills = dedupe_rows(
        skill_rows,
        ("variant_href", "heading_id", "section_key", "skill_name"),
    )
    unique_features = dedupe_rows(feature_rows, ("variant_href", "feature_key"))
    LOGGER.info(
        "NamuWiki detail scraper extracted %s section rows, %s skill rows and %s feature rows",
        len(unique_sections),
        len(unique_skills),
        len(unique_features),
    )
    return unique_sections, unique_skills, unique_features


def scrape(settings: RuntimeSettings) -> list[dict[str, str]]:
    alias_map = load_aliases()
    url = settings.config["sources"]["namuwiki_ss"]
    html = fetch_html(url, "namuwiki_ss_heroes", settings)
    soup = BeautifulSoup(html, "lxml")

    unique_records = _extract_variant_rows(soup, alias_map)
    LOGGER.info(
        "NamuWiki scraper extracted %s hero and variant rows", len(unique_records)
    )
    return unique_records


def scrape_notes(settings: RuntimeSettings) -> list[dict[str, str]]:
    url = settings.config["sources"]["namuwiki_ss"]
    html = fetch_html(url, "namuwiki_ss_heroes", settings)
    soup = BeautifulSoup(html, "lxml")
    notes = _extract_source_notes(soup, url)
    LOGGER.info("NamuWiki scraper extracted %s source notes", len(notes))
    return notes
