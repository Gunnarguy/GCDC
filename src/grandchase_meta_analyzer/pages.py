from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from .explorer_skill_details import parse_patch_entries, split_patch_details
from .paths import PROJECT_ROOT
from .settings import RuntimeSettings


DOCS_DIR = PROJECT_ROOT / "docs"
DOCS_DATA_DIR = DOCS_DIR / "data"
ATLAS_JSON_PATH = DOCS_DATA_DIR / "atlas.json"
NOJEKYLL_PATH = DOCS_DIR / ".nojekyll"
VARIANT_KIND_LABELS = {
    "base": "Base",
    "former": "Former",
    "special": "Special",
}
PATCH_COLUMNS = [
    "name_en",
    "name_ko",
    "variant_id",
    "variant_title",
    "variant_kind",
    "variant_label",
    "heading_title",
    "section_path",
    "source_name",
    "source_page",
    "patch_block_key",
    "patch_entry_index",
    "patch_entry_count_in_block",
    "patch_date",
    "patch_date_iso",
    "patch_change_type",
    "patch_change",
    "body_excerpt",
]


def _read_sql(connection: sqlite3.Connection, query: str) -> pd.DataFrame:
    return pd.read_sql_query(query, connection).fillna("")


def _preview_series(series: pd.Series, length: int) -> pd.Series:
    return series.str.replace(r"\s+", " ", regex=True).str.strip().str.slice(0, length)


def _preview_text(text: str, length: int) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()[:length]


def _format_variant_kind_label(variant_kind: str, variant_suffix: str = "") -> str:
    kind_label = VARIANT_KIND_LABELS.get(
        str(variant_kind).strip(),
        str(variant_kind).replace("_", " ").title() or "Variant",
    )
    suffix = str(variant_suffix).strip()
    if suffix:
        return f"{kind_label} ({suffix})"
    return kind_label


def _apply_variant_display_columns(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "variant_kind" not in frame.columns:
        return frame

    result = frame.copy()
    if "variant_name_en" not in result.columns:
        result["variant_name_en"] = result.get("variant_title", "")
    if "variant_title" not in result.columns:
        result["variant_title"] = result["variant_name_en"]
    if "variant_suffix" not in result.columns:
        result["variant_suffix"] = ""

    result["variant_kind_label"] = result.apply(
        lambda row: _format_variant_kind_label(
            str(row.get("variant_kind", "")),
            str(row.get("variant_suffix", "")),
        ),
        axis=1,
    )
    result["variant_label"] = result.apply(
        lambda row: (
            f"{str(row.get('variant_name_en', '')).strip() or str(row.get('variant_title', '')).strip() or 'Variant'}"
            f" · {row['variant_kind_label']}"
        ),
        axis=1,
    )
    return result


def _to_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return frame.to_dict(orient="records")


def _json_default(value: Any) -> Any:
    if hasattr(value, "item"):
        return value.item()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _parse_patch_date_iso(date_text: str) -> str:
    cleaned = str(date_text or "").strip()
    if not cleaned:
        return ""
    try:
        return datetime.strptime(cleaned, "%B %d, %Y").date().isoformat()
    except ValueError:
        return ""


def _classify_patch_change_type(change: str) -> str:
    lowered = str(change or "").strip().lower()
    if not lowered:
        return "Change"

    has_buff = bool(re.search(r"\bbuff\b", lowered))
    has_nerf = bool(re.search(r"\bnerf\b", lowered))
    has_hotfix = bool(re.search(r"\bhot\s*fix\b", lowered))
    has_fix = bool(re.search(r"\bfix(?:ed)?\b", lowered))
    has_remake = bool(re.search(r"\b(remake|rework|renewal)\b", lowered))
    has_adjustment = bool(re.search(r"\b(other|others|adjust|adjustment)\b", lowered))

    if has_buff and has_nerf:
        return "Mixed"
    if has_hotfix:
        return "Hotfix"
    if has_fix:
        return "Fix"
    if has_remake:
        return "Remake"
    if has_buff:
        return "Buff"
    if has_nerf:
        return "Nerf"
    if has_adjustment:
        return "Adjustment"
    return "Change"


def _extract_patch_entries_from_sections(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=PATCH_COLUMNS)

    rows: list[dict[str, Any]] = []
    for row in frame.itertuples(index=False):
        content = str(getattr(row, "content", "") or "")
        body_text, patch_text = split_patch_details(content)
        if not patch_text:
            continue

        patch_entries = parse_patch_entries(patch_text)
        if not patch_entries:
            continue

        heading_title = str(getattr(row, "heading_title", "") or "")
        section_path = str(getattr(row, "section_path", "") or "")
        source_name = (
            heading_title
            or section_path
            or str(getattr(row, "variant_title", "") or "Variant")
        )
        patch_block_key = "::".join(
            [
                str(getattr(row, "variant_id", "")),
                heading_title,
                section_path,
            ]
        )

        for index, entry in enumerate(patch_entries, start=1):
            rows.append(
                {
                    "name_en": str(getattr(row, "name_en", "") or ""),
                    "name_ko": str(getattr(row, "name_ko", "") or ""),
                    "variant_id": getattr(row, "variant_id", ""),
                    "variant_title": str(getattr(row, "variant_title", "") or ""),
                    "variant_kind": str(getattr(row, "variant_kind", "") or ""),
                    "variant_label": str(getattr(row, "variant_label", "") or ""),
                    "heading_title": heading_title,
                    "section_path": section_path,
                    "source_name": source_name,
                    "source_page": str(getattr(row, "source_page", "") or ""),
                    "patch_block_key": patch_block_key,
                    "patch_entry_index": index,
                    "patch_entry_count_in_block": len(patch_entries),
                    "patch_date": entry.date or "Undated",
                    "patch_date_iso": _parse_patch_date_iso(entry.date),
                    "patch_change_type": _classify_patch_change_type(entry.change),
                    "patch_change": str(entry.change or "").strip(),
                    "body_excerpt": _preview_text(body_text or source_name, 220),
                }
            )

    if not rows:
        return pd.DataFrame(columns=PATCH_COLUMNS)

    patch_entries_df = pd.DataFrame(rows)
    patch_entries_df = patch_entries_df.drop_duplicates(
        subset=[
            "patch_block_key",
            "patch_date",
            "patch_change",
        ]
    ).reset_index(drop=True)
    patch_entries_df = patch_entries_df.sort_values(
        [
            "patch_date_iso",
            "patch_date",
            "name_en",
            "variant_title",
            "source_name",
            "patch_entry_index",
        ],
        ascending=[False, False, True, True, True, True],
        na_position="last",
    ).reset_index(drop=True)
    return patch_entries_df


def _summarize_patch_coverage(patch_entries_df: pd.DataFrame) -> pd.DataFrame:
    if patch_entries_df.empty:
        return pd.DataFrame(
            columns=[
                "name_en",
                "patch_entries",
                "patch_blocks",
                "variants",
                "latest_patch_date",
                "latest_patch_type",
            ]
        )

    latest_rows = patch_entries_df.drop_duplicates(subset=["name_en"])[
        ["name_en", "patch_date", "patch_change_type"]
    ].rename(
        columns={
            "patch_date": "latest_patch_date",
            "patch_change_type": "latest_patch_type",
        }
    )
    coverage_df = (
        patch_entries_df.groupby("name_en", dropna=False)
        .agg(
            patch_entries=("patch_change", "count"),
            patch_blocks=("patch_block_key", "nunique"),
            variants=("variant_title", "nunique"),
        )
        .reset_index()
        .merge(latest_rows, on="name_en", how="left")
        .sort_values(
            ["patch_entries", "patch_blocks", "name_en"],
            ascending=[False, False, True],
        )
        .reset_index(drop=True)
    )
    return coverage_df


def build_pages_payload(database_path: Path) -> dict[str, Any]:
    if not database_path.exists():
        raise FileNotFoundError(
            f"Missing {database_path}. Run bash scripts/run_pipeline.sh first."
        )

    with sqlite3.connect(database_path) as connection:
        heroes_df = _read_sql(
            connection,
            """
            WITH mode_pivot AS (
                SELECT
                    hero_id,
                    MAX(CASE WHEN mode = 'adventure' THEN tier_letter END) AS adventure_tier,
                    MAX(CASE WHEN mode = 'battle' THEN tier_letter END) AS battle_tier,
                    MAX(CASE WHEN mode = 'boss' THEN tier_letter END) AS boss_tier
                FROM hero_modes
                GROUP BY hero_id
            )
            SELECT
                h.hero_id,
                h.name_en,
                h.name_ko,
                h.role,
                h.rarity,
                h.sources,
                mp.adventure_tier,
                mp.battle_tier,
                mp.boss_tier,
                s.base_score,
                s.rarity_adjusted,
                s.final_meta_score,
                s.meta_rank
            FROM heroes h
            LEFT JOIN mode_pivot mp USING (hero_id)
            LEFT JOIN hero_meta_scores s USING (hero_id)
            ORDER BY s.meta_rank, h.name_en
            """,
        )

        variants_df = _apply_variant_display_columns(
            _read_sql(
                connection,
                """
                SELECT
                    hv.variant_id,
                    h.name_en,
                    h.name_ko,
                    h.role,
                    hv.variant_name_en,
                    hv.variant_kind,
                    hv.variant_suffix,
                    hv.availability_marker,
                    hv.source_title AS variant_title,
                    hv.note_excerpt
                FROM hero_variants hv
                JOIN heroes h ON h.hero_id = hv.hero_id
                ORDER BY h.name_en, hv.variant_kind, hv.variant_name_en
                """,
            )
        )

        sections_df = _apply_variant_display_columns(
            _read_sql(
                connection,
                """
                SELECT
                    h.name_en,
                    h.name_ko,
                    hv.variant_id,
                    hv.variant_kind,
                    hv.variant_name_en,
                    hv.variant_suffix,
                    hv.source_title AS variant_title,
                    hvs.heading_level,
                    hvs.heading_title,
                    hvs.section_path,
                    hvs.content,
                    hvs.source_page
                FROM hero_variant_sections hvs
                JOIN hero_variants hv ON hv.variant_id = hvs.variant_id
                JOIN heroes h ON h.hero_id = hv.hero_id
                ORDER BY h.name_en, hv.variant_kind, hvs.heading_level, hvs.heading_title
                """,
            )
        )
        sections_df["content_preview"] = _preview_series(sections_df["content"], 320)
        patch_entries_df = _extract_patch_entries_from_sections(sections_df)

        skills_df = _apply_variant_display_columns(
            _read_sql(
                connection,
                """
                SELECT
                    h.name_en,
                    h.name_ko,
                    hv.variant_id,
                    hv.variant_kind,
                    hv.variant_name_en,
                    hv.variant_suffix,
                    hv.source_title AS variant_title,
                    hvs.section_title,
                    hvs.skill_stage,
                    hvs.skill_type,
                    hvs.skill_name,
                    hvs.description
                FROM hero_variant_skills hvs
                JOIN hero_variants hv ON hv.variant_id = hvs.variant_id
                JOIN heroes h ON h.hero_id = hv.hero_id
                ORDER BY h.name_en, hv.variant_kind, hvs.skill_stage, hvs.skill_name
                """,
            )
        )
        skills_df["description_preview"] = _preview_series(
            skills_df["description"], 220
        )

        features_df = _apply_variant_display_columns(
            _read_sql(
                connection,
                """
                SELECT
                    h.name_en,
                    h.name_ko,
                    hv.variant_id,
                    hv.variant_kind,
                    hv.variant_name_en,
                    hv.variant_suffix,
                    hv.source_title AS variant_title,
                    hvf.feature_key,
                    hvf.feature_value
                FROM hero_variant_features hvf
                JOIN hero_variants hv ON hv.variant_id = hvf.variant_id
                JOIN heroes h ON h.hero_id = hv.hero_id
                ORDER BY h.name_en, hv.variant_kind, hvf.feature_key
                """,
            )
        )

    patch_blocks_df = (
        patch_entries_df.drop_duplicates(subset=["patch_block_key"]).copy()
        if not patch_entries_df.empty
        else patch_entries_df.copy()
    )
    patch_coverage_df = _summarize_patch_coverage(patch_entries_df)
    patch_counts_by_hero_df = (
        patch_coverage_df[["name_en", "patch_blocks", "patch_entries"]].copy()
        if not patch_coverage_df.empty
        else pd.DataFrame(columns=["name_en", "patch_blocks", "patch_entries"])
    )

    top_heroes_df = (
        heroes_df[
            ["meta_rank", "name_en", "name_ko", "role", "rarity", "final_meta_score"]
        ]
        .head(15)
        .copy()
    )

    role_summary_df = (
        heroes_df.groupby("role", dropna=False)
        .agg(
            hero_count=("hero_id", "count"),
            avg_meta_score=("final_meta_score", "mean"),
            best_meta_score=("final_meta_score", "max"),
        )
        .reset_index()
        .sort_values("avg_meta_score", ascending=False)
        .reset_index(drop=True)
    )
    role_summary_df[["avg_meta_score", "best_meta_score"]] = role_summary_df[
        ["avg_meta_score", "best_meta_score"]
    ].round(2)

    variant_mix_df = (
        variants_df.groupby(["variant_kind", "variant_kind_label"], dropna=False)
        .size()
        .reset_index(name="variant_count")
        .sort_values("variant_count", ascending=False)
        .reset_index(drop=True)
    )

    section_coverage_df = (
        sections_df.groupby("name_en", dropna=False)
        .agg(
            variants=("variant_title", "nunique"),
            section_blocks=("heading_title", "count"),
        )
        .reset_index()
        .merge(patch_counts_by_hero_df, on="name_en", how="left")
        .fillna({"patch_blocks": 0, "patch_entries": 0})
        .sort_values(["section_blocks", "variants"], ascending=[False, False])
        .reset_index(drop=True)
        .head(20)
    )
    if not section_coverage_df.empty:
        section_coverage_df[["patch_blocks", "patch_entries"]] = section_coverage_df[
            ["patch_blocks", "patch_entries"]
        ].astype(int)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "database_path": str(database_path.relative_to(PROJECT_ROOT)),
            "site_kind": "github-pages-static-atlas",
        },
        "summary": {
            "hero_count": int(heroes_df["name_en"].nunique()),
            "variant_count": int(variants_df["variant_id"].nunique()),
            "section_count": int(len(sections_df.index)),
            "skill_count": int(len(skills_df.index)),
            "feature_count": int(len(features_df.index)),
            "patch_block_count": int(len(patch_blocks_df.index)),
            "patch_entry_count": int(len(patch_entries_df.index)),
        },
        "top_heroes": _to_records(top_heroes_df),
        "role_summary": _to_records(role_summary_df),
        "variant_mix": _to_records(variant_mix_df),
        "section_coverage": _to_records(section_coverage_df),
        "patch_coverage": _to_records(patch_coverage_df.head(20)),
        "heroes": _to_records(heroes_df),
        "variants": _to_records(variants_df),
        "sections": _to_records(sections_df),
        "skills": _to_records(skills_df),
        "features": _to_records(features_df),
        "patches": _to_records(patch_entries_df),
    }


def export_pages_site(
    settings: RuntimeSettings,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    docs_dir = output_dir or DOCS_DIR
    docs_data_dir = docs_dir / "data"
    docs_dir.mkdir(parents=True, exist_ok=True)
    docs_data_dir.mkdir(parents=True, exist_ok=True)

    payload = build_pages_payload(settings.database_path)
    atlas_json_path = docs_data_dir / "atlas.json"
    atlas_json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )
    (docs_dir / ".nojekyll").write_text("", encoding="utf-8")

    return {
        "atlas_json": str(atlas_json_path.relative_to(PROJECT_ROOT)),
        "docs_dir": str(docs_dir.relative_to(PROJECT_ROOT)),
        "hero_count": payload["summary"]["hero_count"],
        "variant_count": payload["summary"]["variant_count"],
        "section_count": payload["summary"]["section_count"],
        "skill_count": payload["summary"]["skill_count"],
        "feature_count": payload["summary"]["feature_count"],
        "patch_block_count": payload["summary"]["patch_block_count"],
        "patch_entry_count": payload["summary"]["patch_entry_count"],
    }
