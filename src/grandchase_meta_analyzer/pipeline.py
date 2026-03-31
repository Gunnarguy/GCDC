from __future__ import annotations

import logging
from pathlib import Path

from . import normalize
from .llm import tag_skill_snippets
from .paths import LOG_DIR, RAW_DATA_DIR, ensure_runtime_directories
from .scrapers import fandom, namuwiki, strategywiki
from .settings import RuntimeSettings
from .storage import write_csv


LOGGER = logging.getLogger(__name__)


def _run_scrape_step(
    label: str,
    fetcher,
    output_name: str,
    columns: list[str],
) -> int:
    try:
        rows = fetcher()
    except Exception as error:  # noqa: BLE001
        LOGGER.warning(
            "Scrape step %s failed; continuing with empty output: %s", label, error
        )
        rows = []
    write_csv(RAW_DATA_DIR / output_name, rows, columns)
    return len(rows)


def configure_logging(level: str = "INFO") -> None:
    ensure_runtime_directories()
    log_path = LOG_DIR / "pipeline.log"
    handlers: list[logging.Handler] = [
        logging.StreamHandler(),
        logging.FileHandler(log_path, encoding="utf-8"),
    ]
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=handlers,
        force=True,
    )


def run_scrape(settings: RuntimeSettings, source: str | None = None) -> dict[str, int]:
    ensure_runtime_directories()
    results: dict[str, int] = {}

    if source in {None, "strategywiki"}:
        results["strategywiki"] = _run_scrape_step(
            "strategywiki",
            lambda: strategywiki.scrape(settings),
            "strategywiki_heroes.csv",
            ["name_en", "role", "adventure", "battle", "boss", "source"],
        )

    if source in {None, "namuwiki"}:
        try:
            namuwiki_rows = namuwiki.scrape(settings)
        except Exception as error:  # noqa: BLE001
            LOGGER.warning(
                "Scrape step namuwiki failed; continuing with empty output: %s",
                error,
            )
            namuwiki_rows = []
        write_csv(
            RAW_DATA_DIR / "namuwiki_heroes.csv",
            namuwiki_rows,
            [
                "name_ko",
                "name_en_guess",
                "variant_name_en",
                "rarity",
                "variant_kind",
                "variant_suffix",
                "availability_marker",
                "variant_title",
                "variant_href",
                "note_excerpt",
                "source",
            ],
        )
        results["namuwiki"] = len(namuwiki_rows)

        results["namuwiki_notes"] = _run_scrape_step(
            "namuwiki_notes",
            lambda: namuwiki.scrape_notes(settings),
            "namuwiki_notes.csv",
            ["source", "note_key", "title", "content", "source_page"],
        )

        try:
            section_rows, skill_rows, feature_rows = namuwiki.scrape_variant_details(
                settings, namuwiki_rows
            )
        except Exception as error:  # noqa: BLE001
            LOGGER.warning(
                "Scrape step namuwiki variant details failed; continuing with empty output: %s",
                error,
            )
            section_rows, skill_rows, feature_rows = [], [], []
        write_csv(
            RAW_DATA_DIR / "namuwiki_variant_sections.csv",
            section_rows,
            [
                "variant_href",
                "name_en_guess",
                "variant_kind",
                "heading_level",
                "heading_id",
                "heading_title",
                "section_path",
                "content",
                "source_page",
            ],
        )
        write_csv(
            RAW_DATA_DIR / "namuwiki_variant_skills.csv",
            skill_rows,
            [
                "variant_href",
                "name_en_guess",
                "variant_kind",
                "section_key",
                "section_title",
                "heading_id",
                "skill_stage",
                "skill_type",
                "skill_name",
                "description",
                "source_page",
            ],
        )
        write_csv(
            RAW_DATA_DIR / "namuwiki_variant_features.csv",
            feature_rows,
            ["variant_href", "feature_key", "feature_value", "source_page"],
        )
        results["namuwiki_variant_sections"] = len(section_rows)
        results["namuwiki_variant_skills"] = len(skill_rows)
        results["namuwiki_variant_features"] = len(feature_rows)

    if source in {None, "fandom"}:
        results["fandom_traits"] = _run_scrape_step(
            "fandom_traits",
            lambda: fandom.scrape_chaser_traits(settings),
            "fandom_chaser_traits.csv",
            ["trait_name", "description", "rank", "source_page"],
        )
        results["fandom_skills"] = _run_scrape_step(
            "fandom_skills",
            lambda: fandom.scrape_skill_snippets(settings),
            "fandom_skills.csv",
            ["skill_name", "description", "source_page"],
        )

    return results


def run_normalize(settings: RuntimeSettings) -> dict[str, int]:
    return normalize.run(settings)


def run_pipeline(settings: RuntimeSettings) -> dict[str, dict[str, int]]:
    scrape_summary = run_scrape(settings)
    normalize_summary = run_normalize(settings)
    summary: dict[str, dict[str, int]] = {
        "scrape": scrape_summary,
        "normalize": normalize_summary,
    }
    llm_rows = tag_skill_snippets(settings)
    if llm_rows:
        summary["llm"] = {"skill_tags": len(llm_rows)}
    return summary


def pipeline_log_path() -> Path:
    return LOG_DIR / "pipeline.log"
