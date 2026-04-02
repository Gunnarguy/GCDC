from __future__ import annotations

import html
import re
import sqlite3

import pandas as pd
import streamlit as st

try:
    from .explorer_skill_details import extract_skill_insight
    from .paths import PROCESSED_DATA_DIR
    from .team_analysis import (
        build_default_team_variant_ids,
        build_team_defense_evidence_frame,
        build_team_defense_summary,
        build_team_member_snapshot,
        build_team_skill_cost_frame,
        build_team_source_frame,
        build_team_sp_evidence_frame,
        build_team_sp_summary,
    )
except ImportError:  # pragma: no cover - Streamlit runs this file as a script
    from grandchase_meta_analyzer.explorer_skill_details import extract_skill_insight
    from grandchase_meta_analyzer.paths import PROCESSED_DATA_DIR
    from grandchase_meta_analyzer.team_analysis import (
        build_default_team_variant_ids,
        build_team_defense_evidence_frame,
        build_team_defense_summary,
        build_team_member_snapshot,
        build_team_skill_cost_frame,
        build_team_source_frame,
        build_team_sp_evidence_frame,
        build_team_sp_summary,
    )


DB_PATH = PROCESSED_DATA_DIR / "grandchase.db"
DEFAULT_HERO = "Ronan"
PROGRESSION_STAGE_METADATA = {
    "feature": {"label": "System Availability", "order": 0},
    "base_skill": {"label": "Base Kit / Level 1+", "order": 1},
    "gear": {"label": "Dedicated Equipment", "order": 2},
    "pet": {"label": "Pet Support", "order": 3},
    "base_chaser": {"label": "Chaser Progression", "order": 4},
    "enhancement_i": {"label": "Transcendence / Enhancement I", "order": 5},
    "enhancement_ii": {"label": "Transcendence / Enhancement II", "order": 6},
    "imprint": {"label": "Soul Imprint Progression", "order": 7},
    "advent": {"label": "Advent Growth", "order": 8},
    "other": {"label": "Other Systems", "order": 9},
}
FEATURE_LABELS = {
    "characteristics": "Traits / Characteristics",
    "chaser": "Chaser System",
    "soul_imprint": "Soul Imprint System",
    "transcendental_awakening": "Transcendence System",
}
VARIANT_KIND_LABELS = {
    "base": "Base",
    "former": "Former",
    "special": "Special",
}
RELATION_TYPE_LABELS = {
    "does_not_stack_with": "Does not stack with",
    "does_not_overlap_with": "Does not overlap with",
    "mutually_exclusive_with": "Mutually exclusive with",
    "overwrites": "Overwrites",
    "overwritten_by": "Overwritten by",
}
BRACKETED_PREFIX_PATTERN = re.compile(r"^(?:\[[^\]]+\]\s*)+")


st.set_page_config(
    page_title="GrandChase Atlas",
    layout="wide",
    initial_sidebar_state="expanded",
)


def apply_styles() -> None:
    st.markdown(
        """
        <style>
            .stApp {
                background:
                    radial-gradient(circle at top left, rgba(240, 180, 76, 0.16), transparent 28%),
                    radial-gradient(circle at top right, rgba(15, 108, 116, 0.14), transparent 24%),
                    linear-gradient(180deg, #f6f1e7 0%, #f1eadc 100%);
                color: #203040;
            }
            section[data-testid="stSidebar"] {
                background: linear-gradient(180deg, #17324d 0%, #12304a 100%);
            }
            section[data-testid="stSidebar"] * {
                color: #f8f3e8;
            }
            .gc-hero {
                background: linear-gradient(135deg, #17324d 0%, #0f6c74 48%, #f0b44c 100%);
                color: #f9f4ea;
                padding: 1.4rem 1.6rem;
                border-radius: 24px;
                box-shadow: 0 22px 52px rgba(23, 50, 77, 0.18);
                margin-bottom: 1rem;
            }
            .gc-hero h1 {
                margin: 0 0 0.35rem 0;
                font-size: 2.2rem;
                line-height: 1.05;
            }
            .gc-hero p {
                margin: 0;
                max-width: 78ch;
                color: rgba(249, 244, 234, 0.92);
                line-height: 1.55;
            }
            div[data-testid="metric-container"] {
                background: linear-gradient(180deg, #fffaf1 0%, #f6edde 100%);
                border: 1px solid #e7d8c1;
                padding: 0.9rem 1rem;
                border-radius: 18px;
                box-shadow: 0 10px 24px rgba(98, 71, 45, 0.08);
            }
            div[data-testid="metric-container"] label {
                color: #8a6542;
                font-weight: 700;
                letter-spacing: 0.04em;
            }
            .gc-section-card {
                background: rgba(255, 250, 241, 0.92);
                border: 1px solid #e7d8c1;
                border-radius: 18px;
                padding: 1rem 1.1rem;
                margin-bottom: 0.9rem;
            }
            .gc-pill {
                display: inline-block;
                padding: 0.2rem 0.55rem;
                border-radius: 999px;
                background: #17324d;
                color: #f8f3e8;
                font-size: 0.78rem;
                font-weight: 700;
                margin-right: 0.4rem;
            }
            .gc-patch-entry {
                background: rgba(255, 250, 241, 0.92);
                border: 1px solid #e7d8c1;
                border-radius: 18px;
                padding: 0.9rem 1rem;
                margin-bottom: 0.8rem;
            }
            .gc-patch-meta {
                display: flex;
                align-items: center;
                flex-wrap: wrap;
                gap: 0.55rem;
                margin-bottom: 0.45rem;
            }
            .gc-badge {
                display: inline-block;
                padding: 0.18rem 0.55rem;
                border-radius: 999px;
                font-size: 0.75rem;
                font-weight: 700;
                border: 1px solid transparent;
            }
            .gc-badge-buff {
                background: #d8f3e3;
                border-color: #9fd0af;
                color: #1d6a43;
            }
            .gc-badge-nerf {
                background: #fde1df;
                border-color: #efb1ad;
                color: #9a3b34;
            }
            .gc-badge-hotfix,
            .gc-badge-fix {
                background: #deeffc;
                border-color: #a9cbed;
                color: #1b5f95;
            }
            .gc-badge-remake,
            .gc-badge-adjustment,
            .gc-badge-mixed,
            .gc-badge-change {
                background: #f3e8d2;
                border-color: #dcc4a0;
                color: #785528;
            }
            .gc-patch-date {
                color: #5d4a38;
                font-weight: 700;
            }
            .gc-patch-change {
                margin: 0;
                color: #203040;
                line-height: 1.5;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_data(show_spinner="Loading GrandChase Atlas...")
def load_atlas() -> dict[str, pd.DataFrame]:
    if not DB_PATH.exists():
        raise FileNotFoundError(
            "Missing data/processed/grandchase.db. Run bash scripts/run_pipeline.sh first."
        )

    with sqlite3.connect(DB_PATH) as connection:
        table_names = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }

        def read_optional_sql(
            query: str,
            columns: list[str],
            required_tables: set[str],
        ) -> pd.DataFrame:
            if not required_tables.issubset(table_names):
                return pd.DataFrame(columns=columns)
            return pd.read_sql_query(query, connection).fillna("")

        heroes_df = pd.read_sql_query(
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
            connection,
        ).fillna("")

        variants_df = pd.read_sql_query(
            """
            SELECT
                hv.variant_id,
                h.name_en,
                h.name_ko,
                hv.variant_role AS role,
                hv.variant_rarity AS rarity,
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
            connection,
        ).fillna("")
        variants_df = apply_variant_display_columns(variants_df)

        variant_leaderboard_df = read_optional_sql(
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
                hv.variant_id,
                hv.hero_id,
                h.name_en,
                COALESCE(hv.name_ko, h.name_ko) AS name_ko,
                hv.variant_name_en,
                hv.variant_kind,
                hv.variant_suffix,
                hv.availability_marker,
                hv.variant_role AS role,
                hv.variant_rarity AS rarity,
                hv.source_title AS variant_title,
                hv.note_excerpt,
                mp.adventure_tier,
                mp.battle_tier,
                mp.boss_tier,
                vms.base_score,
                vms.rarity_adjusted,
                vms.final_meta_score,
                vms.meta_rank,
                vms.score_basis
            FROM hero_variants hv
            JOIN heroes h ON h.hero_id = hv.hero_id
            LEFT JOIN mode_pivot mp ON mp.hero_id = hv.hero_id
            LEFT JOIN variant_meta_scores vms ON vms.variant_id = hv.variant_id
            ORDER BY vms.meta_rank, h.name_en, hv.variant_kind, hv.variant_name_en
            """,
            [
                "variant_id",
                "hero_id",
                "name_en",
                "name_ko",
                "variant_name_en",
                "variant_kind",
                "variant_suffix",
                "availability_marker",
                "role",
                "rarity",
                "variant_title",
                "note_excerpt",
                "adventure_tier",
                "battle_tier",
                "boss_tier",
                "base_score",
                "rarity_adjusted",
                "final_meta_score",
                "meta_rank",
                "score_basis",
            ],
            {"hero_variants", "variant_meta_scores", "heroes", "hero_modes"},
        )
        variant_leaderboard_df = apply_variant_display_columns(variant_leaderboard_df)

        sections_df = pd.read_sql_query(
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
            connection,
        ).fillna("")
        sections_df = apply_variant_display_columns(sections_df)

        skills_df = pd.read_sql_query(
            """
            SELECT
                h.name_en,
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
            connection,
        ).fillna("")
        skills_df = apply_variant_display_columns(skills_df)

        features_df = pd.read_sql_query(
            """
            SELECT
                h.name_en,
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
            connection,
        ).fillna("")
        features_df = apply_variant_display_columns(features_df)

        progression_rows_df = read_optional_sql(
            """
            SELECT
                h.name_en,
                h.name_ko,
                pr.progression_key,
                pr.variant_id,
                pr.variant_title,
                pr.variant_kind,
                hv.variant_name_en,
                hv.variant_suffix,
                pr.source_kind,
                pr.source_name,
                pr.skill_family,
                pr.progression_stage_key,
                pr.progression_stage_label,
                pr.stage_order,
                pr.skill_stage,
                pr.skill_type,
                pr.progression_tracks_summary,
                pr.modifiers_summary,
                pr.mechanics_summary,
                pr.stats_summary,
                pr.top_coefficient,
                pr.excerpt,
                pr.source_page
            FROM hero_progression_rows pr
            JOIN hero_variants hv ON hv.variant_id = pr.variant_id
            JOIN heroes h ON h.hero_id = pr.hero_id
            ORDER BY h.name_en, pr.stage_order, pr.variant_title, pr.source_kind, pr.source_name
            """,
            [
                "name_en",
                "name_ko",
                "progression_key",
                "variant_id",
                "variant_title",
                "variant_kind",
                "variant_name_en",
                "variant_suffix",
                "source_kind",
                "source_name",
                "skill_family",
                "progression_stage_key",
                "progression_stage_label",
                "stage_order",
                "skill_stage",
                "skill_type",
                "progression_tracks_summary",
                "modifiers_summary",
                "mechanics_summary",
                "stats_summary",
                "top_coefficient",
                "excerpt",
                "source_page",
            ],
            {"hero_progression_rows", "hero_variants", "heroes"},
        )
        progression_rows_df = apply_variant_display_columns(progression_rows_df)

        progression_values_df = read_optional_sql(
            """
            SELECT
                h.name_en,
                pr.variant_id,
                hv.variant_name_en,
                pr.variant_title,
                hv.variant_kind,
                hv.variant_suffix,
                pr.source_name,
                pr.skill_family,
                pr.progression_stage_label,
                pv.value_kind,
                pv.category,
                pv.value_text,
                pv.numeric_value,
                pv.unit,
                pv.context
            FROM hero_progression_values pv
            JOIN hero_progression_rows pr ON pr.progression_key = pv.progression_key
            JOIN hero_variants hv ON hv.variant_id = pr.variant_id
            JOIN heroes h ON h.hero_id = pr.hero_id
            ORDER BY h.name_en, pr.stage_order, pr.variant_title, pr.source_name, pv.value_kind, pv.numeric_value DESC
            """,
            [
                "name_en",
                "variant_id",
                "variant_name_en",
                "variant_title",
                "variant_kind",
                "variant_suffix",
                "source_name",
                "skill_family",
                "progression_stage_label",
                "value_kind",
                "category",
                "value_text",
                "numeric_value",
                "unit",
                "context",
            ],
            {
                "hero_progression_values",
                "hero_progression_rows",
                "hero_variants",
                "heroes",
            },
        )
        progression_values_df = apply_variant_display_columns(progression_values_df)

        progression_tracks_df = read_optional_sql(
            """
            SELECT
                h.name_en,
                pr.variant_id,
                hv.variant_name_en,
                pr.variant_title,
                hv.variant_kind,
                hv.variant_suffix,
                pr.source_name,
                pr.skill_family,
                pr.progression_stage_label,
                pt.track_label,
                pt.step_index,
                pt.step_value,
                pt.numeric_value,
                pt.unit,
                pt.context
            FROM hero_progression_tracks pt
            JOIN hero_progression_rows pr ON pr.progression_key = pt.progression_key
            JOIN hero_variants hv ON hv.variant_id = pr.variant_id
            JOIN heroes h ON h.hero_id = pr.hero_id
            ORDER BY h.name_en, pr.stage_order, pr.variant_title, pr.source_name, pt.track_label, pt.step_index
            """,
            [
                "name_en",
                "variant_id",
                "variant_name_en",
                "variant_title",
                "variant_kind",
                "variant_suffix",
                "source_name",
                "skill_family",
                "progression_stage_label",
                "track_label",
                "step_index",
                "step_value",
                "numeric_value",
                "unit",
                "context",
            ],
            {
                "hero_progression_tracks",
                "hero_progression_rows",
                "hero_variants",
                "heroes",
            },
        )
        progression_tracks_df = apply_variant_display_columns(progression_tracks_df)

        equipment_stats_df = read_optional_sql(
            """
            SELECT
                h.name_en,
                pr.variant_id,
                hv.variant_name_en,
                pr.variant_title,
                hv.variant_kind,
                hv.variant_suffix,
                pes.equipment_name,
                pes.equipment_level,
                pes.physical_attack,
                pes.vitality,
                pes.physical_defense,
                pes.magic_defense
            FROM hero_progression_equipment_stats pes
            JOIN hero_progression_rows pr ON pr.progression_key = pes.progression_key
            JOIN hero_variants hv ON hv.variant_id = pr.variant_id
            JOIN heroes h ON h.hero_id = pr.hero_id
            ORDER BY h.name_en, pr.variant_title, pes.equipment_name, pes.equipment_level
            """,
            [
                "name_en",
                "variant_id",
                "variant_name_en",
                "variant_title",
                "variant_kind",
                "variant_suffix",
                "equipment_name",
                "equipment_level",
                "physical_attack",
                "vitality",
                "physical_defense",
                "magic_defense",
            ],
            {
                "hero_progression_equipment_stats",
                "hero_progression_rows",
                "hero_variants",
                "heroes",
            },
        )
        equipment_stats_df = apply_variant_display_columns(equipment_stats_df)

        progression_relationships_df = read_optional_sql(
            """
            SELECT
                h.name_en,
                pr.progression_key,
                pr.variant_id,
                pr.variant_title,
                pr.variant_kind,
                hv.variant_name_en,
                hv.variant_suffix,
                pr.source_kind,
                pr.source_name,
                pr.skill_family,
                pr.progression_stage_label,
                pr.stage_order,
                rel.relation_type,
                rel.relation_scope,
                rel.target_source_name,
                rel.target_skill_family,
                rel.evidence_text,
                rel.source_page,
                rel.confidence_source
            FROM hero_progression_relationships rel
            JOIN hero_progression_rows pr ON pr.progression_key = rel.progression_key
            JOIN hero_variants hv ON hv.variant_id = pr.variant_id
            JOIN heroes h ON h.hero_id = pr.hero_id
            ORDER BY h.name_en, pr.stage_order, pr.variant_title, pr.source_name, rel.relation_type, rel.target_source_name
            """,
            [
                "name_en",
                "progression_key",
                "variant_id",
                "variant_title",
                "variant_kind",
                "variant_name_en",
                "variant_suffix",
                "source_kind",
                "source_name",
                "skill_family",
                "progression_stage_label",
                "stage_order",
                "relation_type",
                "relation_scope",
                "target_source_name",
                "target_skill_family",
                "evidence_text",
                "source_page",
                "confidence_source",
            ],
            {
                "hero_progression_relationships",
                "hero_progression_rows",
                "hero_variants",
                "heroes",
            },
        )
        progression_relationships_df = apply_variant_display_columns(
            progression_relationships_df
        )

    patch_df = sections_df[
        sections_df["section_path"].str.contains("Patch Details", case=False, na=False)
    ].copy()
    role_summary_df = (
        variant_leaderboard_df.groupby("role", dropna=False)
        .agg(
            unit_count=("variant_id", "count"),
            avg_meta_score=("final_meta_score", "mean"),
            best_meta_score=("final_meta_score", "max"),
        )
        .reset_index()
        .sort_values("avg_meta_score", ascending=False)
        .reset_index(drop=True)
    )
    role_summary_df[["avg_meta_score", "best_meta_score"]] = role_summary_df[
        [
            "avg_meta_score",
            "best_meta_score",
        ]
    ].round(2)
    variant_mix_df = (
        variants_df.groupby("variant_kind", dropna=False)
        .size()
        .reset_index(name="variant_count")
        .sort_values("variant_count", ascending=False)
        .reset_index(drop=True)
    )
    coverage_df = (
        sections_df.groupby("name_en", dropna=False)
        .agg(
            variants=("variant_title", "nunique"),
            section_blocks=("heading_title", "count"),
            patch_blocks=(
                "section_path",
                lambda values: values.str.contains(
                    "Patch Details", case=False, na=False
                ).sum(),
            ),
        )
        .reset_index()
        .sort_values(["section_blocks", "variants"], ascending=[False, False])
        .reset_index(drop=True)
    )
    return {
        "heroes": heroes_df,
        "variants": variants_df,
        "variant_leaderboard": variant_leaderboard_df,
        "sections": sections_df,
        "skills": skills_df,
        "features": features_df,
        "patches": patch_df,
        "role_summary": role_summary_df,
        "variant_mix": variant_mix_df,
        "coverage": coverage_df,
        "progression_rows": progression_rows_df,
        "progression_values": progression_values_df,
        "progression_tracks": progression_tracks_df,
        "equipment_stats": equipment_stats_df,
        "progression_relationships": progression_relationships_df,
    }


def preview_text(series: pd.Series, length: int = 240) -> pd.Series:
    return series.str.replace(r"\s+", " ", regex=True).str.slice(0, length)


def preview_value(text: str, length: int = 240) -> str:
    return re.sub(r"\s+", " ", text).strip()[:length]


def join_or_dash(items: list[str], limit: int = 8) -> str:
    return ", ".join(items[:limit]) if items else "-"


def format_metric_value(value: str, suffix: str = "") -> str:
    return f"{value}{suffix}" if value else "-"


def format_variant_kind_label(
    variant_kind: str,
    variant_suffix: str = "",
) -> str:
    kind_label = VARIANT_KIND_LABELS.get(
        str(variant_kind).strip(),
        str(variant_kind).replace("_", " ").title() or "Variant",
    )
    suffix = str(variant_suffix).strip()
    if suffix:
        return f"{kind_label} ({suffix})"
    return kind_label


def build_variant_label(
    variant_name_en: str,
    variant_title: str,
    variant_kind: str,
    variant_suffix: str = "",
) -> str:
    display_name = (
        str(variant_name_en).strip() or str(variant_title).strip() or "Variant"
    )
    return f"{display_name} · {format_variant_kind_label(variant_kind, variant_suffix)}"


def apply_variant_display_columns(frame: pd.DataFrame) -> pd.DataFrame:
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
        lambda row: format_variant_kind_label(
            str(row.get("variant_kind", "")),
            str(row.get("variant_suffix", "")),
        ),
        axis=1,
    )
    result["variant_label"] = result.apply(
        lambda row: build_variant_label(
            str(row.get("variant_name_en", "")),
            str(row.get("variant_title", "")),
            str(row.get("variant_kind", "")),
            str(row.get("variant_suffix", "")),
        ),
        axis=1,
    )
    return result


def build_variant_kind_label_map(variants_df: pd.DataFrame) -> dict[str, str]:
    if variants_df.empty:
        return {}
    labels: dict[str, str] = {}
    for _, row in (
        variants_df[["variant_kind", "variant_suffix"]].drop_duplicates().iterrows()
    ):
        kind = str(row["variant_kind"])
        labels.setdefault(
            kind,
            format_variant_kind_label(kind, str(row.get("variant_suffix", ""))),
        )
    return labels


def max_numeric_token(values: list[str]) -> str:
    numeric_pairs: list[tuple[float, str]] = []
    for value in values:
        match = (
            pd.Series([value])
            .str.extract(r"([0-9]+(?:\.[0-9]+)?)", expand=False)
            .iloc[0]
        )
        if not match:
            continue
        numeric_pairs.append((float(match), value))
    if not numeric_pairs:
        return "-"
    return max(numeric_pairs, key=lambda item: item[0])[1]


def render_pills(label: str, items: list[str]) -> None:
    if not items:
        return
    pills = " ".join(f"<span class='gc-pill'>{item}</span>" for item in items)
    st.markdown(f"**{label}**<br>{pills}", unsafe_allow_html=True)


PATCH_TYPE_BADGE_CLASSES = {
    "Buff": "gc-badge-buff",
    "Nerf": "gc-badge-nerf",
    "Hotfix": "gc-badge-hotfix",
    "Fix": "gc-badge-fix",
    "Remake": "gc-badge-remake",
    "Adjustment": "gc-badge-adjustment",
    "Mixed": "gc-badge-mixed",
    "Change": "gc-badge-change",
}


def patch_type_badge_class(change_type: str) -> str:
    return PATCH_TYPE_BADGE_CLASSES.get(change_type, "gc-badge-change")


def render_patch_history_entries(entries: list[object]) -> None:
    for entry in entries:
        change_type = str(getattr(entry, "change_type", "Change") or "Change")
        badge_class = patch_type_badge_class(change_type)
        date_text = html.escape(str(getattr(entry, "date", "") or "Undated"))
        change_text = html.escape(str(getattr(entry, "change", "") or ""))
        st.markdown(
            (
                "<div class='gc-patch-entry'>"
                "<div class='gc-patch-meta'>"
                f"<span class='gc-badge {badge_class}'>{html.escape(change_type)}</span>"
                f"<span class='gc-patch-date'>{date_text}</span>"
                "</div>"
                f"<p class='gc-patch-change'>{change_text}</p>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )


def normalize_skill_family_name(skill_name: str) -> str:
    normalized = BRACKETED_PREFIX_PATTERN.sub("", skill_name).strip()
    normalized = re.sub(
        r"^(?:imprint of |imprint |engraving |stamp )",
        "",
        normalized,
        flags=re.IGNORECASE,
    )
    return normalized.strip() or skill_name


def progression_stage_metadata(stage_key: str) -> dict[str, object]:
    return PROGRESSION_STAGE_METADATA.get(
        stage_key, PROGRESSION_STAGE_METADATA["other"]
    )


def classify_skill_progression_row(skill_stage: str, skill_type: str) -> str:
    if skill_stage == "enhancement_i":
        return "enhancement_i"
    if skill_stage == "enhancement_ii":
        return "enhancement_ii"
    if skill_stage == "imprint":
        return "imprint"
    if skill_type == "chaser":
        return "base_chaser"
    return "base_skill"


def classify_section_progression(heading_title: str) -> str | None:
    lowered = heading_title.lower()
    if "dedicated equipment" in lowered:
        return "gear"
    if lowered == "pet" or lowered.startswith("pet"):
        return "pet"
    if "advent skill" in lowered or "growth effect" in lowered:
        return "advent"
    if lowered.startswith("soul imprint:"):
        return "imprint"
    return None


def format_progression_tracks(tracks: list[object]) -> str:
    if not tracks:
        return "-"
    return "; ".join(f"{track.label}: {' / '.join(track.values)}" for track in tracks)


def insight_progression_tracks(insight: object) -> list[object]:
    # Streamlit hot reload can briefly mix older SkillInsight instances with newer app code.
    return list(getattr(insight, "progression_tracks", []) or [])


def extract_equipment_rows(text: str) -> list[dict[str, str]]:
    normalized = re.sub(r"\s+", " ", text)
    match = re.search(
        r"level physical attack power vitality physical defense magic defense (.+?)(?: Encyclopedia Story|$)",
        normalized,
        re.IGNORECASE,
    )
    if match is None:
        return []
    values = re.findall(r"\d[\d,]*", match.group(1))
    if len(values) < 5 or len(values) % 5 != 0:
        return []
    rows: list[dict[str, str]] = []
    for index in range(0, len(values), 5):
        level, physical_attack, vitality, physical_defense, magic_defense = values[
            index : index + 5
        ]
        rows.append(
            {
                "level": level,
                "physical_attack": physical_attack,
                "vitality": vitality,
                "physical_defense": physical_defense,
                "magic_defense": magic_defense,
            }
        )
    return rows


def format_equipment_rows(rows: list[dict[str, str]]) -> str:
    if not rows:
        return "-"
    return " | ".join(
        (
            f"Lv {row['level']}: PATK {row['physical_attack']}, HP {row['vitality']}, "
            f"PDEF {row['physical_defense']}, MDEF {row['magic_defense']}"
        )
        for row in rows
    )


def default_column_label(column_name: str) -> str:
    return column_name.replace("_", " ").title()


def summarize_unique_text(
    values: pd.Series | list[object],
    limit: int = 4,
    separator: str = ", ",
    preview_length: int | None = None,
) -> str:
    seen: set[str] = set()
    items: list[str] = []
    for raw_value in values:
        value = str(raw_value).strip()
        if not value or value == "-":
            continue
        lowered = value.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        items.append(preview_value(value, preview_length) if preview_length else value)
        if len(items) >= limit:
            break
    return separator.join(items) if items else "-"


def build_column_config(
    frame: pd.DataFrame,
    large_columns: tuple[str, ...] = (),
    medium_columns: tuple[str, ...] = (),
    number_formats: dict[str, str] | None = None,
    column_labels: dict[str, str] | None = None,
) -> dict[str, object]:
    large_set = set(large_columns)
    medium_set = set(medium_columns)
    number_formats = number_formats or {}
    column_labels = column_labels or {}
    config: dict[str, object] = {}
    for column in frame.columns:
        label = column_labels.get(column, default_column_label(column))
        if column in number_formats:
            config[column] = st.column_config.NumberColumn(
                label,
                format=number_formats[column],
                width="small",
            )
            continue
        if pd.api.types.is_numeric_dtype(frame[column]):
            config[column] = st.column_config.NumberColumn(label, width="small")
            continue
        width = (
            "large"
            if column in large_set
            else "medium" if column in medium_set else "small"
        )
        config[column] = st.column_config.TextColumn(label, width=width)
    return config


def render_readable_dataframe(
    frame: pd.DataFrame,
    *,
    height: int,
    large_columns: tuple[str, ...] = (),
    medium_columns: tuple[str, ...] = (),
    number_formats: dict[str, str] | None = None,
    column_labels: dict[str, str] | None = None,
) -> None:
    st.dataframe(
        frame,
        width="stretch",
        hide_index=True,
        height=height,
        column_config=build_column_config(
            frame,
            large_columns=large_columns,
            medium_columns=medium_columns,
            number_formats=number_formats,
            column_labels=column_labels,
        ),
    )


def format_relation_type(relation_type: str) -> str:
    return RELATION_TYPE_LABELS.get(
        relation_type, relation_type.replace("_", " ").title()
    )


def build_relationship_frame(
    relationships_df: pd.DataFrame,
    *,
    hero_name: str | None = None,
    variant_title: str | None = None,
    source_name: str | None = None,
    skill_family: str | None = None,
) -> pd.DataFrame:
    filtered = relationships_df.copy()
    if hero_name is not None:
        filtered = filtered[filtered["name_en"] == hero_name]
    if variant_title is not None:
        filtered = filtered[filtered["variant_title"] == variant_title]
    if source_name is not None:
        filtered = filtered[filtered["source_name"] == source_name]
    if skill_family is not None:
        filtered = filtered[
            (filtered["skill_family"] == skill_family)
            | (filtered["target_skill_family"] == skill_family)
        ]
    if filtered.empty:
        return filtered
    frame = filtered[
        [
            "variant_label",
            "progression_stage_label",
            "source_name",
            "relation_type",
            "relation_scope",
            "target_source_name",
            "evidence_text",
            "source_page",
            "confidence_source",
        ]
    ].copy()
    frame["relation_type"] = frame["relation_type"].map(format_relation_type)
    frame["evidence_text"] = frame["evidence_text"].map(
        lambda value: preview_value(str(value), 220)
    )
    return frame.sort_values(
        [
            "progression_stage_label",
            "variant_label",
            "source_name",
            "relation_type",
            "target_source_name",
        ]
    ).reset_index(drop=True)


def relationship_counts_by_progression(
    relationships_df: pd.DataFrame,
) -> pd.DataFrame:
    if relationships_df.empty:
        return pd.DataFrame(columns=["progression_key", "explicit_relationship_count"])
    counts = (
        relationships_df.groupby("progression_key", dropna=False)
        .size()
        .reset_index(name="explicit_relationship_count")
    )
    return counts


def prepare_progression_rows_for_comparison(
    progression_rows_df: pd.DataFrame,
    relationships_df: pd.DataFrame,
    variant_leaderboard_df: pd.DataFrame,
) -> pd.DataFrame:
    if progression_rows_df.empty:
        return progression_rows_df.copy()
    relation_counts_df = relationship_counts_by_progression(relationships_df)
    variant_meta_df = variant_leaderboard_df[
        ["variant_id", "role", "meta_rank", "final_meta_score"]
    ].copy()
    frame = progression_rows_df.merge(variant_meta_df, on="variant_id", how="left")
    frame = frame.merge(relation_counts_df, on="progression_key", how="left")
    frame["explicit_relationship_count"] = (
        frame["explicit_relationship_count"].fillna(0).astype(int)
    )
    return frame


def build_hero_vs_hero_frame(
    comparison_rows_df: pd.DataFrame,
    left_hero: str,
    right_hero: str,
    variant_kinds: list[str],
    stage_labels: list[str],
    shared_only: bool,
) -> pd.DataFrame:
    filtered = comparison_rows_df[
        comparison_rows_df["name_en"].isin([left_hero, right_hero])
    ].copy()
    if variant_kinds:
        filtered = filtered[filtered["variant_kind"].isin(variant_kinds)]
    if stage_labels:
        filtered = filtered[filtered["progression_stage_label"].isin(stage_labels)]
    if filtered.empty:
        return filtered

    summary_df = (
        filtered.groupby(
            [
                "name_en",
                "skill_family",
                "progression_stage_label",
                "source_kind",
                "stage_order",
            ],
            dropna=False,
        )
        .agg(
            variant_titles=(
                "variant_label",
                lambda values: summarize_unique_text(values, limit=3),
            ),
            source_names=(
                "source_name",
                lambda values: summarize_unique_text(values, limit=3),
            ),
            top_coefficient=(
                "top_coefficient",
                lambda values: max_numeric_token(
                    [
                        str(value)
                        for value in values
                        if str(value).strip() and str(value) != "-"
                    ]
                ),
            ),
            modifiers_summary=(
                "modifiers_summary",
                lambda values: summarize_unique_text(
                    values, limit=1, preview_length=150
                ),
            ),
            mechanics_summary=(
                "mechanics_summary",
                lambda values: summarize_unique_text(
                    values, limit=1, preview_length=120
                ),
            ),
            stats_summary=(
                "stats_summary",
                lambda values: summarize_unique_text(
                    values, limit=1, preview_length=120
                ),
            ),
            explicit_relationship_count=("explicit_relationship_count", "sum"),
            coverage_rows=("progression_key", "count"),
        )
        .reset_index()
    )

    left_df = summary_df[summary_df["name_en"] == left_hero].drop(columns=["name_en"])
    right_df = summary_df[summary_df["name_en"] == right_hero].drop(columns=["name_en"])
    merge_how = "inner" if shared_only else "outer"
    merged = left_df.merge(
        right_df,
        on=["skill_family", "progression_stage_label", "source_kind", "stage_order"],
        how=merge_how,
        suffixes=(f"_{left_hero}", f"_{right_hero}"),
    ).fillna("-")
    return merged.sort_values(
        ["stage_order", "skill_family", "source_kind"]
    ).reset_index(drop=True)


def build_skill_family_comparison_frame(
    comparison_rows_df: pd.DataFrame,
    skill_family: str,
    roles: list[str],
    variant_kinds: list[str],
    stage_labels: list[str],
) -> pd.DataFrame:
    filtered = comparison_rows_df[
        (comparison_rows_df["source_kind"] == "skill")
        & (comparison_rows_df["skill_family"] == skill_family)
    ].copy()
    if roles:
        filtered = filtered[filtered["role"].isin(roles)]
    if variant_kinds:
        filtered = filtered[filtered["variant_kind"].isin(variant_kinds)]
    if stage_labels:
        filtered = filtered[filtered["progression_stage_label"].isin(stage_labels)]
    return filtered.sort_values(
        ["stage_order", "meta_rank", "name_en", "variant_label", "source_name"]
    ).reset_index(drop=True)


def build_stage_comparison_frame(
    comparison_rows_df: pd.DataFrame,
    stage_label: str,
    roles: list[str],
    variant_kinds: list[str],
    source_kinds: list[str],
) -> pd.DataFrame:
    filtered = comparison_rows_df[
        comparison_rows_df["progression_stage_label"] == stage_label
    ].copy()
    if roles:
        filtered = filtered[filtered["role"].isin(roles)]
    if variant_kinds:
        filtered = filtered[filtered["variant_kind"].isin(variant_kinds)]
    if source_kinds:
        filtered = filtered[filtered["source_kind"].isin(source_kinds)]
    return filtered.sort_values(
        ["meta_rank", "name_en", "variant_label", "source_kind", "source_name"]
    ).reset_index(drop=True)


def build_role_stage_coverage_frame(
    comparison_rows_df: pd.DataFrame,
    roles: list[str],
    variant_kinds: list[str],
    stage_labels: list[str],
) -> pd.DataFrame:
    filtered = comparison_rows_df.copy()
    if roles:
        filtered = filtered[filtered["role"].isin(roles)]
    if variant_kinds:
        filtered = filtered[filtered["variant_kind"].isin(variant_kinds)]
    if stage_labels:
        filtered = filtered[filtered["progression_stage_label"].isin(stage_labels)]
    if filtered.empty:
        return filtered
    return (
        filtered.groupby(
            ["role", "progression_stage_label", "stage_order"], dropna=False
        )
        .agg(
            unit_count=("variant_id", "nunique"),
            row_count=("progression_key", "count"),
            explicit_relationships=("explicit_relationship_count", "sum"),
        )
        .reset_index()
        .sort_values(["role", "stage_order"])
        .reset_index(drop=True)
    )


def build_role_family_frame(
    comparison_rows_df: pd.DataFrame,
    roles: list[str],
    variant_kinds: list[str],
    stage_labels: list[str],
) -> pd.DataFrame:
    filtered = comparison_rows_df[comparison_rows_df["source_kind"] == "skill"].copy()
    if roles:
        filtered = filtered[filtered["role"].isin(roles)]
    if variant_kinds:
        filtered = filtered[filtered["variant_kind"].isin(variant_kinds)]
    if stage_labels:
        filtered = filtered[filtered["progression_stage_label"].isin(stage_labels)]
    if filtered.empty:
        return filtered
    return (
        filtered.groupby(["role", "skill_family"], dropna=False)
        .agg(
            unit_count=("variant_id", "nunique"),
            row_count=("progression_key", "count"),
            stage_coverage=(
                "progression_stage_label",
                lambda values: summarize_unique_text(values, limit=5),
            ),
            top_coefficient=(
                "top_coefficient",
                lambda values: max_numeric_token(
                    [
                        str(value)
                        for value in values
                        if str(value).strip() and str(value) != "-"
                    ]
                ),
            ),
            explicit_relationships=("explicit_relationship_count", "sum"),
        )
        .reset_index()
        .sort_values(
            ["role", "unit_count", "row_count", "skill_family"],
            ascending=[True, False, False, True],
        )
        .reset_index(drop=True)
    )


def build_skill_label(row: pd.Series) -> str:
    return " · ".join(
        part
        for part in (
            str(row["skill_stage"]).replace("_", " ").title(),
            str(row["skill_type"]).title(),
            str(row["skill_name"]),
        )
        if part
    )


def build_skill_mechanics_frame(
    skills_df: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, object]]:
    if skills_df.empty:
        return pd.DataFrame(), {}

    unique_skills = skills_df[
        [
            "variant_title",
            "skill_stage",
            "skill_type",
            "skill_name",
            "description",
        ]
    ].drop_duplicates()

    insight_by_key: dict[str, object] = {}
    rows: list[dict[str, object]] = []
    for _, row in unique_skills.iterrows():
        skill_label = build_skill_label(row)
        skill_key = f"{row['variant_title']}::{skill_label}"
        insight = extract_skill_insight(str(row["description"]))
        stage_key = classify_skill_progression_row(
            str(row["skill_stage"]),
            str(row["skill_type"]),
        )
        stage_metadata = progression_stage_metadata(stage_key)
        insight_by_key[skill_key] = insight
        rows.append(
            {
                "skill_key": skill_key,
                "variant_title": str(row["variant_title"]),
                "variant_label": str(row.get("variant_label", row["variant_title"])),
                "skill_stage": str(row["skill_stage"]),
                "skill_type": str(row["skill_type"]),
                "skill_name": str(row["skill_name"]),
                "skill_label": skill_label,
                "skill_family": normalize_skill_family_name(str(row["skill_name"])),
                "progression_stage_key": stage_key,
                "progression_stage": str(stage_metadata["label"]),
                "stage_order": int(stage_metadata["order"]),
                "progression_tracks": format_progression_tracks(
                    insight_progression_tracks(insight)
                ),
                "cooldown": format_metric_value(insight.cooldown_seconds, " s"),
                "sp": format_metric_value(insight.sp_cost),
                "top_coefficient": max_numeric_token(insight.coefficients),
                "durations": join_or_dash(insight.durations, 6),
                "stacks": join_or_dash(insight.stack_mentions, 4),
                "chances": join_or_dash(insight.chance_mentions, 4),
                "thresholds": join_or_dash(insight.threshold_mentions, 4),
                "targets": join_or_dash(insight.target_mentions, 4),
                "mechanics": join_or_dash(insight.mechanic_tags, 8),
                "stats": join_or_dash(insight.stat_tags, 8),
                "patch_entries": len(insight.patch_entries),
                "scaling_ladders": len(insight.scaling_series),
                "stat_bonuses": len(insight.stat_bonuses),
                "trigger_count": len(insight.trigger_clauses),
                "current_effect": insight.body_text,
                "raw_description": str(row["description"]),
            }
        )

    mechanics_df = pd.DataFrame.from_records(rows).sort_values(
        ["variant_label", "stage_order", "skill_type", "skill_name"]
    )
    return mechanics_df, insight_by_key


def build_progression_roadmap(
    hero_skill_mechanics_df: pd.DataFrame,
    hero_insights: dict[str, object],
    hero_sections: pd.DataFrame,
    hero_features: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    rows: list[dict[str, object]] = []
    equipment_tables: dict[str, pd.DataFrame] = {}

    if not hero_features.empty:
        feature_metadata = progression_stage_metadata("feature")
        for _, row in (
            hero_features[
                ["variant_title", "variant_label", "feature_key", "feature_value"]
            ]
            .drop_duplicates()
            .iterrows()
        ):
            feature_key = str(row["feature_key"])
            rows.append(
                {
                    "variant_title": str(row["variant_title"]),
                    "variant_label": str(
                        row.get("variant_label", row["variant_title"])
                    ),
                    "stage_label": str(feature_metadata["label"]),
                    "stage_order": int(feature_metadata["order"]),
                    "source_kind": "feature",
                    "source_name": FEATURE_LABELS.get(
                        feature_key, feature_key.replace("_", " ").title()
                    ),
                    "family": "System",
                    "progression_tracks": "-",
                    "modifiers": str(row["feature_value"]),
                    "mechanics": FEATURE_LABELS.get(feature_key, feature_key),
                    "stats": "-",
                    "top_coefficient": "-",
                    "excerpt": str(row["feature_value"]),
                    "table_key": "",
                }
            )

    if not hero_skill_mechanics_df.empty:
        for _, row in hero_skill_mechanics_df.iterrows():
            rows.append(
                {
                    "variant_title": str(row["variant_title"]),
                    "variant_label": str(
                        row.get("variant_label", row["variant_title"])
                    ),
                    "stage_label": str(row["progression_stage"]),
                    "stage_order": int(row["stage_order"]),
                    "source_kind": "skill",
                    "source_name": str(row["skill_name"]),
                    "family": str(row["skill_family"]),
                    "progression_tracks": str(row["progression_tracks"]),
                    "modifiers": "; ".join(
                        value
                        for value in (
                            (
                                f"Cooldown {row['cooldown']}"
                                if row["cooldown"] != "-"
                                else ""
                            ),
                            f"SP {row['sp']}" if row["sp"] != "-" else "",
                            (
                                f"Top damage {row['top_coefficient']}"
                                if row["top_coefficient"] != "-"
                                else ""
                            ),
                            (
                                f"Durations {row['durations']}"
                                if row["durations"] != "-"
                                else ""
                            ),
                            f"Stacks {row['stacks']}" if row["stacks"] != "-" else "",
                            (
                                f"Chances {row['chances']}"
                                if row["chances"] != "-"
                                else ""
                            ),
                            (
                                f"Thresholds {row['thresholds']}"
                                if row["thresholds"] != "-"
                                else ""
                            ),
                        )
                        if value
                    )
                    or "-",
                    "mechanics": str(row["mechanics"]),
                    "stats": str(row["stats"]),
                    "top_coefficient": str(row["top_coefficient"]),
                    "excerpt": preview_value(str(row["current_effect"]), 220),
                    "table_key": "",
                }
            )

    if not hero_sections.empty:
        section_columns = ["variant_title", "variant_label", "heading_title", "content"]
        for _, row in hero_sections[section_columns].drop_duplicates().iterrows():
            stage_key = classify_section_progression(str(row["heading_title"]))
            if stage_key is None:
                continue
            stage_metadata = progression_stage_metadata(stage_key)
            content = str(row["content"])
            insight = extract_skill_insight(content)
            tracks = insight_progression_tracks(insight)
            equipment_rows = (
                extract_equipment_rows(content) if stage_key == "gear" else []
            )
            table_key = ""
            modifiers = (
                "; ".join(
                    value
                    for value in (
                        format_equipment_rows(equipment_rows),
                        (
                            f"Top damage {max_numeric_token(insight.coefficients)}"
                            if insight.coefficients
                            else ""
                        ),
                        (
                            f"Durations {join_or_dash(insight.durations, 6)}"
                            if insight.durations
                            else ""
                        ),
                        (
                            f"Scaling {format_progression_tracks(tracks)}"
                            if tracks
                            else ""
                        ),
                    )
                    if value and value != "-"
                )
                or "-"
            )
            if equipment_rows:
                table_key = f"{row['variant_title']}::{row['heading_title']}"
                equipment_tables[table_key] = pd.DataFrame(equipment_rows)
            rows.append(
                {
                    "variant_title": str(row["variant_title"]),
                    "variant_label": str(
                        row.get("variant_label", row["variant_title"])
                    ),
                    "stage_label": str(stage_metadata["label"]),
                    "stage_order": int(stage_metadata["order"]),
                    "source_kind": "section",
                    "source_name": str(row["heading_title"]),
                    "family": str(row["heading_title"]),
                    "progression_tracks": format_progression_tracks(tracks),
                    "modifiers": modifiers,
                    "mechanics": join_or_dash(insight.mechanic_tags, 8),
                    "stats": join_or_dash(insight.stat_tags, 8),
                    "top_coefficient": max_numeric_token(insight.coefficients),
                    "excerpt": preview_value(content, 220),
                    "table_key": table_key,
                }
            )

    roadmap_df = pd.DataFrame.from_records(rows)
    if roadmap_df.empty:
        return roadmap_df, equipment_tables
    roadmap_df = roadmap_df.sort_values(
        ["stage_order", "variant_label", "source_kind", "family", "source_name"]
    ).reset_index(drop=True)
    return roadmap_df, equipment_tables


def build_skill_family_progression_frame(
    variant_mechanics_df: pd.DataFrame,
    hero_insights: dict[str, object],
    selected_skill_row: pd.Series,
) -> pd.DataFrame:
    family = str(selected_skill_row["skill_family"])
    family_df = variant_mechanics_df[
        variant_mechanics_df["skill_family"] == family
    ].sort_values(["stage_order", "skill_stage", "skill_type"])
    rows: list[dict[str, object]] = []
    previous_values: set[str] = set()
    previous_mechanics: set[str] = set()
    for _, row in family_df.iterrows():
        insight = hero_insights[str(row["skill_key"])]
        current_values = (
            {mention.value for mention in insight.numeric_mentions}
            | set(insight.durations)
            | set(insight.chance_mentions)
            | set(insight.stack_mentions)
        )
        current_mechanics = set(insight.mechanic_tags)
        added_values = sorted(current_values - previous_values)
        removed_values = sorted(previous_values - current_values)
        added_mechanics = sorted(current_mechanics - previous_mechanics)
        rows.append(
            {
                "stage": str(row["progression_stage"]),
                "source": str(row["skill_name"]),
                "captured_ladder": str(row["progression_tracks"]),
                "cooldown": str(row["cooldown"]),
                "sp": str(row["sp"]),
                "top_coefficient": str(row["top_coefficient"]),
                "added_modifiers": join_or_dash(added_values, 8),
                "removed_modifiers": join_or_dash(removed_values, 8),
                "added_mechanics": join_or_dash(added_mechanics, 8),
                "excerpt": preview_value(str(row["current_effect"]), 180),
            }
        )
        previous_values = current_values
        previous_mechanics = current_mechanics
    return pd.DataFrame.from_records(rows)


def render_header() -> None:
    st.markdown(
        """
        <div class="gc-hero">
            <h1>GrandChase Atlas</h1>
            <p>
                Search the captured GrandChase database without fighting raw wiki pages.
                Browse heroes, variants, section blocks, parsed skills, feature flags, and patch notes
                from the stored local data only.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_overview(data: dict[str, pd.DataFrame]) -> None:
    heroes_df = data["heroes"]
    variants_df = data["variants"]
    variant_leaderboard_df = data["variant_leaderboard"]
    sections_df = data["sections"]
    skills_df = data["skills"]
    features_df = data["features"]
    patches_df = data["patches"]
    role_summary_df = data["role_summary"]
    variant_mix_df = data["variant_mix"]
    coverage_df = data["coverage"]

    metric_columns = st.columns(6)
    metric_columns[0].metric("Heroes", heroes_df["name_en"].nunique())
    metric_columns[1].metric("Variants", len(variants_df))
    metric_columns[2].metric("Sections", len(sections_df))
    metric_columns[3].metric("Skills", len(skills_df))
    metric_columns[4].metric("Patch Blocks", len(patches_df))
    metric_columns[5].metric("Feature Flags", len(features_df))

    left, right = st.columns([1.1, 0.9])
    with left:
        st.subheader("Top Units")
        st.dataframe(
            variant_leaderboard_df[
                [
                    "meta_rank",
                    "variant_label",
                    "role",
                    "rarity",
                    "final_meta_score",
                ]
            ].head(15),
            width="stretch",
            hide_index=True,
            height=560,
        )
    with right:
        st.subheader("Role Summary")
        st.dataframe(role_summary_df, width="stretch", hide_index=True, height=280)
        st.subheader("Variant Mix")
        st.bar_chart(variant_mix_df.set_index("variant_kind"))

    st.subheader("Coverage Leaders")
    st.dataframe(coverage_df.head(18), width="stretch", hide_index=True, height=460)


def render_search(
    data: dict[str, pd.DataFrame],
    hero_query: str,
    text_query: str,
    section_query: str,
    kind_filter: list[str],
    row_limit: int,
    show_section_expanders: bool,
) -> None:
    sections_df = data["sections"].copy()
    skills_df = data["skills"].copy()
    features_df = data["features"].copy()

    if hero_query.strip():
        section_mask = sections_df["name_en"].str.contains(
            hero_query, case=False, na=False
        )
        section_mask = section_mask | sections_df["name_ko"].str.contains(
            hero_query, case=False, na=False
        )
        sections_df = sections_df[section_mask]
        skills_df = skills_df[
            skills_df["name_en"].str.contains(hero_query, case=False, na=False)
        ]
        features_df = features_df[
            features_df["name_en"].str.contains(hero_query, case=False, na=False)
        ]

    if kind_filter:
        sections_df = sections_df[sections_df["variant_kind"].isin(kind_filter)]
        skills_df = skills_df[skills_df["variant_kind"].isin(kind_filter)]
        features_df = features_df[features_df["variant_kind"].isin(kind_filter)]

    if section_query.strip():
        section_mask = sections_df["section_path"].str.contains(
            section_query, case=False, na=False
        )
        section_mask = section_mask | sections_df["heading_title"].str.contains(
            section_query, case=False, na=False
        )
        sections_df = sections_df[section_mask]

    if text_query.strip():
        text_mask = sections_df["content"].str.contains(
            text_query, case=False, na=False
        )
        text_mask = text_mask | sections_df["heading_title"].str.contains(
            text_query, case=False, na=False
        )
        sections_df = sections_df[text_mask]

        skill_mask = skills_df["skill_name"].str.contains(
            text_query, case=False, na=False
        )
        skill_mask = skill_mask | skills_df["description"].str.contains(
            text_query, case=False, na=False
        )
        skills_df = skills_df[skill_mask]

        feature_mask = features_df["feature_key"].str.contains(
            text_query, case=False, na=False
        )
        feature_mask = feature_mask | features_df["feature_value"].str.contains(
            text_query, case=False, na=False
        )
        features_df = features_df[feature_mask]

    metrics = st.columns(4)
    metrics[0].metric("Section Matches", len(sections_df))
    metrics[1].metric("Skill Matches", len(skills_df))
    metrics[2].metric("Feature Matches", len(features_df))
    metrics[3].metric(
        "Patch Matches",
        int(
            sections_df["section_path"]
            .str.contains("Patch Details", case=False, na=False)
            .sum()
        ),
    )

    section_tab, skill_tab, feature_tab = st.tabs(["Sections", "Skills", "Features"])

    with section_tab:
        if sections_df.empty:
            st.info("No matching section blocks. Relax the filters and try again.")
        else:
            preview_df = sections_df[
                ["name_en", "variant_label", "heading_title", "section_path", "content"]
            ].copy()
            preview_df["content"] = preview_text(preview_df["content"], 320)
            st.dataframe(
                preview_df.head(row_limit),
                width="stretch",
                hide_index=True,
                height=520,
            )
            if show_section_expanders:
                for row in sections_df.head(min(row_limit, 8)).itertuples(index=False):
                    with st.expander(
                        f"{row.name_en} · {row.variant_label} · {row.heading_title}"
                    ):
                        st.caption(row.section_path)
                        st.write(row.content)
                        st.caption(row.source_page)

    with skill_tab:
        if skills_df.empty:
            st.info("No matching parsed skill rows.")
        else:
            preview_df = skills_df[
                [
                    "name_en",
                    "variant_label",
                    "skill_stage",
                    "skill_type",
                    "skill_name",
                    "description",
                ]
            ].copy()
            preview_df["description"] = preview_text(preview_df["description"], 220)
            st.dataframe(
                preview_df.head(row_limit),
                width="stretch",
                hide_index=True,
                height=520,
            )

    with feature_tab:
        if features_df.empty:
            st.info("No matching feature flags.")
        else:
            preview_df = features_df[
                ["name_en", "variant_label", "feature_key", "feature_value"]
            ].copy()
            st.dataframe(
                preview_df.head(row_limit),
                width="stretch",
                hide_index=True,
                height=420,
            )


def render_dossier(data: dict[str, pd.DataFrame], hero_name: str) -> None:
    heroes_df = data["heroes"]
    variants_df = data["variants"]
    variant_leaderboard_df = data["variant_leaderboard"]
    sections_df = data["sections"]
    skills_df = data["skills"]
    features_df = data["features"]
    progression_rows_df = data["progression_rows"]
    progression_values_df = data["progression_values"]
    progression_tracks_df = data["progression_tracks"]
    equipment_stats_df = data["equipment_stats"]
    progression_relationships_df = data["progression_relationships"]

    hero_row = heroes_df[heroes_df["name_en"] == hero_name]
    hero_sections = sections_df[sections_df["name_en"] == hero_name].copy()
    hero_skills = skills_df[skills_df["name_en"] == hero_name].copy()
    hero_features = features_df[features_df["name_en"] == hero_name].copy()
    hero_progression_rows_df = progression_rows_df[
        progression_rows_df["name_en"] == hero_name
    ].copy()
    hero_progression_values_df = progression_values_df[
        progression_values_df["name_en"] == hero_name
    ].copy()
    hero_progression_tracks_df = progression_tracks_df[
        progression_tracks_df["name_en"] == hero_name
    ].copy()
    hero_equipment_stats_df = equipment_stats_df[
        equipment_stats_df["name_en"] == hero_name
    ].copy()
    hero_relationships_df = progression_relationships_df[
        progression_relationships_df["name_en"] == hero_name
    ].copy()
    hero_skill_mechanics_df, hero_insights = build_skill_mechanics_frame(hero_skills)
    hero_progression_df, equipment_tables = build_progression_roadmap(
        hero_skill_mechanics_df,
        hero_insights,
        hero_sections,
        hero_features,
    )

    if hero_row.empty or hero_sections.empty:
        st.warning(f"No dossier data found for {hero_name}.")
        return

    hero_record = hero_row.iloc[0]
    hero_variant_records = variants_df[variants_df["name_en"] == hero_name].copy()
    hero_variant_scores_df = variant_leaderboard_df[
        variant_leaderboard_df["name_en"] == hero_name
    ].copy()
    st.subheader(f"Hero Dossier: {hero_name}")
    st.caption(
        "Use the sidebar hero picker to switch to any other hero in the database."
    )
    variant_options_df = hero_sections[
        ["variant_title", "variant_label"]
    ].drop_duplicates()
    variants = variant_options_df["variant_title"].tolist()
    variant_label_by_title = dict(
        zip(variant_options_df["variant_title"], variant_options_df["variant_label"])
    )
    default_variant = variants[0] if variants else ""
    selected_variant = st.selectbox(
        "Variant",
        variants,
        index=variants.index(default_variant),
        format_func=lambda variant_title: variant_label_by_title.get(
            variant_title, variant_title
        ),
    )
    selected_variant_label = variant_label_by_title.get(
        selected_variant, selected_variant
    )
    st.caption(f"Current variant: {selected_variant_label}")

    selected_variant_record = hero_variant_records[
        hero_variant_records["variant_title"] == selected_variant
    ].head(1)
    selected_variant_score_record = hero_variant_scores_df[
        hero_variant_scores_df["variant_title"] == selected_variant
    ].head(1)
    variant_record = (
        selected_variant_record.iloc[0]
        if not selected_variant_record.empty
        else hero_record
    )
    variant_score_record = (
        selected_variant_score_record.iloc[0]
        if not selected_variant_score_record.empty
        else hero_record
    )

    variant_sections = hero_sections[
        hero_sections["variant_title"] == selected_variant
    ].copy()
    variant_features = hero_features[
        hero_features["variant_title"] == selected_variant
    ].copy()
    variant_patches = variant_sections[
        variant_sections["section_path"].str.contains(
            "Patch Details", case=False, na=False
        )
    ].copy()

    metric_columns = st.columns(6)
    metric_columns[0].metric("Role", variant_record["role"])
    metric_columns[1].metric("Rarity", variant_record["rarity"])
    metric_columns[2].metric("Meta Rank", int(variant_score_record["meta_rank"]))
    metric_columns[3].metric(
        "Meta Score", round(float(variant_score_record["final_meta_score"]), 2)
    )
    metric_columns[4].metric(
        "Variants", hero_variant_records["variant_title"].nunique()
    )
    metric_columns[5].metric("Patch Blocks", len(variant_patches))

    hero_mechanic_tags = sorted(
        {tag for insight in hero_insights.values() for tag in insight.mechanic_tags}
    )
    hero_stat_tags = sorted(
        {tag for insight in hero_insights.values() for tag in insight.stat_tags}
    )
    hero_patch_entry_count = sum(
        len(insight.patch_entries) for insight in hero_insights.values()
    )
    hero_scaling_count = sum(
        len(insight.scaling_series) for insight in hero_insights.values()
    )
    hero_numeric_count = sum(
        len(insight.numeric_mentions) for insight in hero_insights.values()
    )
    hero_top_coefficients = [
        mention.value
        for insight in hero_insights.values()
        for mention in insight.numeric_mentions
        if mention.category == "damage"
    ]

    systems_columns = st.columns(7)
    systems_columns[0].metric("Skill Rows", len(hero_skill_mechanics_df))
    systems_columns[1].metric("Mechanic Tags", len(hero_mechanic_tags))
    systems_columns[2].metric("Stat Tags", len(hero_stat_tags))
    systems_columns[3].metric("Numeric Mentions", hero_numeric_count)
    systems_columns[4].metric("Scaling Ladders", hero_scaling_count)
    systems_columns[5].metric("Patch Entries", hero_patch_entry_count)
    systems_columns[6].metric("Top Damage", max_numeric_token(hero_top_coefficients))

    render_pills("Hero Mechanics Inventory", hero_mechanic_tags)
    render_pills("Hero Stats Inventory", hero_stat_tags)

    roadmap_scope = st.radio(
        "Evolution roadmap scope",
        [
            "All variants for hero",
            f"Selected variant only ({selected_variant_label})",
        ],
        horizontal=True,
    )
    if roadmap_scope.startswith("Selected variant only"):
        roadmap_view_df = hero_progression_df[
            hero_progression_df["variant_title"] == selected_variant
        ].copy()
        normalized_rows_view_df = hero_progression_rows_df[
            hero_progression_rows_df["variant_title"] == selected_variant
        ].copy()
        normalized_values_view_df = hero_progression_values_df[
            hero_progression_values_df["variant_title"] == selected_variant
        ].copy()
        normalized_tracks_view_df = hero_progression_tracks_df[
            hero_progression_tracks_df["variant_title"] == selected_variant
        ].copy()
        normalized_equipment_view_df = hero_equipment_stats_df[
            hero_equipment_stats_df["variant_title"] == selected_variant
        ].copy()
        normalized_relationships_view_df = hero_relationships_df[
            hero_relationships_df["variant_title"] == selected_variant
        ].copy()
    else:
        roadmap_view_df = hero_progression_df.copy()
        normalized_rows_view_df = hero_progression_rows_df.copy()
        normalized_values_view_df = hero_progression_values_df.copy()
        normalized_tracks_view_df = hero_progression_tracks_df.copy()
        normalized_equipment_view_df = hero_equipment_stats_df.copy()
        normalized_relationships_view_df = hero_relationships_df.copy()

    st.subheader("Hero Evolution Roadmap")
    st.caption(
        "This roadmap is organized by the captured source ladders: base kit, chaser rows, Enhancement I and II, "
        "Soul Imprint rows, plus dedicated equipment, pet, and advent growth sections when they exist. "
        "The source usually exposes Chaser as 0/1/2/3 and Soul Imprint as 1/2/3/4/5 ladders rather than a literal 25/25 or 15/15 point sheet."
    )
    if roadmap_view_df.empty:
        st.info("No progression roadmap rows are available for this hero.")
    else:
        roadmap_columns = [
            "variant_label",
            "stage_label",
            "source_kind",
            "family",
            "source_name",
            "progression_tracks",
            "modifiers",
            "mechanics",
            "stats",
            "excerpt",
        ]
        st.dataframe(
            roadmap_view_df[roadmap_columns],
            height=420,
            width="stretch",
            hide_index=True,
        )
        visible_tables = {
            key: frame
            for key, frame in equipment_tables.items()
            if roadmap_scope == "All variants for hero"
            or key.startswith(f"{selected_variant}::")
        }
        if visible_tables:
            st.markdown("**Dedicated Equipment Stat Tables**")
            for table_key, frame in visible_tables.items():
                with st.expander(table_key):
                    st.dataframe(frame, width="stretch", hide_index=True)

    st.subheader("Exact Progression Ledger")
    if normalized_rows_view_df.empty:
        st.info(
            "No normalized progression tables are loaded yet. Run the normalize step once to persist exact stage rows into SQLite."
        )
    else:
        (
            ledger_tab,
            value_tab,
            track_tab,
            relationship_tab,
            equipment_tab,
        ) = st.tabs(
            [
                "Ledger Rows",
                "Exact Modifier Values",
                "Track Steps",
                "Explicit Relationships",
                "Equipment Rows",
            ]
        )

        with ledger_tab:
            render_readable_dataframe(
                normalized_rows_view_df[
                    [
                        "variant_label",
                        "progression_stage_label",
                        "source_kind",
                        "source_name",
                        "skill_family",
                        "progression_tracks_summary",
                        "modifiers_summary",
                        "mechanics_summary",
                        "stats_summary",
                        "top_coefficient",
                        "excerpt",
                    ]
                ],
                height=320,
                large_columns=(
                    "modifiers_summary",
                    "mechanics_summary",
                    "stats_summary",
                    "excerpt",
                ),
                medium_columns=(
                    "variant_label",
                    "source_name",
                    "skill_family",
                    "progression_tracks_summary",
                ),
            )

        with value_tab:
            render_readable_dataframe(
                normalized_values_view_df[
                    [
                        "variant_label",
                        "progression_stage_label",
                        "source_name",
                        "value_kind",
                        "category",
                        "value_text",
                        "numeric_value",
                        "unit",
                        "context",
                    ]
                ],
                height=280,
                large_columns=("context",),
                medium_columns=("variant_label", "source_name", "value_text"),
                number_formats={"numeric_value": "%.2f"},
            )

        with track_tab:
            render_readable_dataframe(
                normalized_tracks_view_df[
                    [
                        "variant_label",
                        "progression_stage_label",
                        "source_name",
                        "track_label",
                        "step_index",
                        "step_value",
                        "numeric_value",
                        "unit",
                    ]
                ],
                height=240,
                medium_columns=("variant_label", "source_name", "track_label"),
                number_formats={"numeric_value": "%.2f"},
            )

        with relationship_tab:
            if normalized_relationships_view_df.empty:
                st.info(
                    "No explicit non-stacking, overwrite, or exclusivity rules were found in the captured sources for this scope."
                )
            else:
                render_readable_dataframe(
                    build_relationship_frame(
                        normalized_relationships_view_df,
                    ),
                    height=240,
                    large_columns=("evidence_text", "source_page"),
                    medium_columns=(
                        "variant_label",
                        "source_name",
                        "target_source_name",
                    ),
                )

        with equipment_tab:
            if normalized_equipment_view_df.empty:
                st.info("No normalized equipment rows are available for this scope.")
            else:
                render_readable_dataframe(
                    normalized_equipment_view_df,
                    height=220,
                    medium_columns=("variant_label", "equipment_name"),
                )

    st.subheader("Hero Mechanics Matrix")
    if hero_skill_mechanics_df.empty:
        st.info("No parsed mechanics matrix is available for this hero.")
    else:
        matrix_columns = [
            "variant_label",
            "progression_stage",
            "skill_stage",
            "skill_type",
            "skill_family",
            "skill_name",
            "progression_tracks",
            "cooldown",
            "sp",
            "top_coefficient",
            "durations",
            "stacks",
            "chances",
            "targets",
            "mechanics",
            "stats",
            "patch_entries",
        ]
        st.dataframe(
            hero_skill_mechanics_df[matrix_columns],
            width="stretch",
            hide_index=True,
            height=420,
        )

    st.subheader("Variant Drilldown")

    top_left, top_right = st.columns([1.1, 0.9])
    with top_left:
        st.subheader("Variant Skills")
        variant_mechanics_df = hero_skill_mechanics_df[
            hero_skill_mechanics_df["variant_title"] == selected_variant
        ].copy()
        if variant_mechanics_df.empty:
            st.info("No parsed skill rows for this variant.")
        else:
            preview_df = variant_mechanics_df[
                [
                    "skill_stage",
                    "skill_type",
                    "skill_name",
                    "cooldown",
                    "sp",
                    "top_coefficient",
                    "durations",
                    "mechanics",
                ]
            ].copy()
            st.dataframe(preview_df, width="stretch", hide_index=True, height=420)

            st.subheader("Skill Inspector")
            selected_skill_label = st.selectbox(
                "Skill detail",
                variant_mechanics_df["skill_label"].tolist(),
                key=f"skill-detail-{hero_name}-{selected_variant}",
            )
            selected_skill_row = variant_mechanics_df[
                variant_mechanics_df["skill_label"] == selected_skill_label
            ].iloc[0]
            skill_insight = hero_insights[str(selected_skill_row["skill_key"])]
            family_progression_df = build_skill_family_progression_frame(
                variant_mechanics_df,
                hero_insights,
                selected_skill_row,
            )
            selected_relationships_df = build_relationship_frame(
                hero_relationships_df,
                variant_title=selected_variant,
                source_name=str(selected_skill_row["skill_name"]),
            )
            family_relationships_df = build_relationship_frame(
                hero_relationships_df,
                variant_title=selected_variant,
                skill_family=str(selected_skill_row["skill_family"]),
            )

            detail_metrics = st.columns(7)
            detail_metrics[0].metric(
                "Type", str(selected_skill_row["skill_type"]).title()
            )
            detail_metrics[1].metric(
                "Stage",
                str(selected_skill_row["skill_stage"]).replace("_", " ").title(),
            )
            detail_metrics[2].metric(
                "Cooldown", format_metric_value(skill_insight.cooldown_seconds, " s")
            )
            detail_metrics[3].metric("SP", format_metric_value(skill_insight.sp_cost))
            detail_metrics[4].metric(
                "Top Coefficient", max_numeric_token(skill_insight.coefficients)
            )
            detail_metrics[5].metric("Patch Entries", len(skill_insight.patch_entries))
            detail_metrics[6].metric(
                "Explicit Relations",
                len(selected_relationships_df.index),
            )

            render_pills("Mechanic Tags", skill_insight.mechanic_tags)
            render_pills("Stat Tags", skill_insight.stat_tags)
            render_pills("Mode Tags", skill_insight.mode_tags)
            render_pills("Durations", skill_insight.durations[:8])
            render_pills("Coefficients", skill_insight.coefficients[:10])
            render_pills(
                "Captured Progression Ladders",
                [
                    f"{track.label}: {' / '.join(track.values)}"
                    for track in insight_progression_tracks(skill_insight)
                ],
            )

            (
                summary_tab,
                numbers_tab,
                mechanics_tab,
                relationship_tab,
                evolution_tab,
                patch_tab,
                raw_tab,
            ) = st.tabs(
                [
                    "Summary",
                    "Numbers",
                    "Mechanics",
                    "Relationships",
                    "Evolution Path",
                    "Patch History",
                    "Raw Block",
                ]
            )

            with summary_tab:
                summary_rows = pd.DataFrame(
                    [
                        {
                            "field": "Cooldown",
                            "value": format_metric_value(
                                skill_insight.cooldown_seconds, " seconds"
                            ),
                        },
                        {
                            "field": "SP Cost",
                            "value": format_metric_value(skill_insight.sp_cost),
                        },
                        {
                            "field": "Duration Mentions",
                            "value": ", ".join(skill_insight.durations[:8]) or "-",
                        },
                        {
                            "field": "Coefficient Mentions",
                            "value": ", ".join(skill_insight.coefficients[:10]) or "-",
                        },
                        {
                            "field": "Mode Tags",
                            "value": ", ".join(skill_insight.mode_tags) or "-",
                        },
                        {
                            "field": "Mechanic Tags",
                            "value": ", ".join(skill_insight.mechanic_tags) or "-",
                        },
                        {
                            "field": "Stat Tags",
                            "value": ", ".join(skill_insight.stat_tags) or "-",
                        },
                        {
                            "field": "Captured Ladders",
                            "value": format_progression_tracks(
                                insight_progression_tracks(skill_insight)
                            ),
                        },
                    ]
                )
                st.dataframe(summary_rows, width="stretch", hide_index=True)
                st.write(skill_insight.body_text or "No current effect text captured.")
                if selected_relationships_df.empty:
                    st.caption(
                        "No explicit non-stacking, overwrite, or exclusivity rules were found in captured source text for this exact skill row."
                    )

            with numbers_tab:
                numeric_rows = pd.DataFrame(
                    [
                        {
                            "value": mention.value,
                            "category": mention.category,
                            "context": mention.context,
                        }
                        for mention in skill_insight.numeric_mentions
                    ]
                )
                scaling_rows = pd.DataFrame(
                    [
                        {
                            "label": row.label,
                            "category": row.category,
                            "values": ", ".join(row.values),
                            "context": row.context,
                        }
                        for row in skill_insight.scaling_series
                    ]
                )
                stat_bonus_rows = pd.DataFrame(
                    [
                        {"stat": bonus.stat, "value": bonus.value}
                        for bonus in skill_insight.stat_bonuses
                    ]
                )

                st.markdown("**Categorized Numeric Mentions**")
                if numeric_rows.empty:
                    st.info(
                        "No categorized numeric mentions were parsed for this skill."
                    )
                else:
                    st.dataframe(
                        numeric_rows,
                        width="stretch",
                        hide_index=True,
                        height=280,
                    )

                st.markdown("**Scaling Ladders**")
                if scaling_rows.empty:
                    st.info("No multi-step scaling ladders were parsed for this skill.")
                else:
                    st.dataframe(
                        scaling_rows,
                        width="stretch",
                        hide_index=True,
                        height=220,
                    )

                st.markdown("**Flat Stat Bonuses**")
                if stat_bonus_rows.empty:
                    st.info("No flat stat bonuses were parsed for this skill.")
                else:
                    st.dataframe(
                        stat_bonus_rows,
                        width="stretch",
                        hide_index=True,
                        height=180,
                    )

            with mechanics_tab:
                mechanics_rows = pd.DataFrame(
                    [
                        {"bucket": "Stack rules", "value": value}
                        for value in skill_insight.stack_mentions
                    ]
                    + [
                        {"bucket": "Proc chances", "value": value}
                        for value in skill_insight.chance_mentions
                    ]
                    + [
                        {"bucket": "Thresholds", "value": value}
                        for value in skill_insight.threshold_mentions
                    ]
                    + [
                        {"bucket": "Targets", "value": value}
                        for value in skill_insight.target_mentions
                    ]
                )
                trigger_rows = pd.DataFrame(
                    [
                        {"trigger clause": value}
                        for value in skill_insight.trigger_clauses
                    ]
                )

                if not mechanics_rows.empty:
                    st.dataframe(
                        mechanics_rows,
                        width="stretch",
                        hide_index=True,
                        height=220,
                    )
                else:
                    st.info("No stack, chance, threshold, or target rules were parsed.")

                st.markdown("**Trigger Clauses**")
                if trigger_rows.empty:
                    st.info("No explicit trigger clauses were parsed for this skill.")
                else:
                    st.dataframe(
                        trigger_rows,
                        width="stretch",
                        hide_index=True,
                        height=220,
                    )

            with relationship_tab:
                st.caption(
                    "Only explicit relationship facts are shown here. If the source text does not say a skill conflicts, overwrites, or fails to stack with something, the table stays empty rather than guessing."
                )
                if selected_relationships_df.empty:
                    st.info(
                        "No explicit relationship rows were captured for this exact skill entry."
                    )
                else:
                    render_readable_dataframe(
                        selected_relationships_df,
                        height=220,
                        large_columns=("evidence_text", "source_page"),
                        medium_columns=(
                            "variant_label",
                            "source_name",
                            "target_source_name",
                        ),
                    )
                if not family_relationships_df.empty and len(
                    family_relationships_df.index
                ) > len(selected_relationships_df.index):
                    st.markdown(
                        "**Other relationships found in this skill family across the current variant**"
                    )
                    render_readable_dataframe(
                        family_relationships_df,
                        height=220,
                        large_columns=("evidence_text", "source_page"),
                        medium_columns=(
                            "variant_label",
                            "source_name",
                            "target_source_name",
                        ),
                    )

            with evolution_tab:
                st.caption(
                    "This upgrade path compares the same skill family across the captured stages for the current variant. "
                    "Added and removed modifiers are derived from the parsed numeric mentions, durations, stack rules, and proc chances in each stage row."
                )
                if family_progression_df.empty:
                    st.info(
                        "No stage-to-stage evolution rows were found for this skill family."
                    )
                else:
                    render_readable_dataframe(
                        family_progression_df,
                        height=280,
                        large_columns=(
                            "added_modifiers",
                            "removed_modifiers",
                            "added_mechanics",
                            "excerpt",
                        ),
                        medium_columns=("source", "captured_ladder"),
                    )

            with patch_tab:
                if not skill_insight.patch_entries:
                    st.info("No patch history parsed for this skill block.")
                else:
                    render_patch_history_entries(skill_insight.patch_entries)

            with raw_tab:
                st.write(str(selected_skill_row["raw_description"]))

    with top_right:
        st.subheader("Feature Flags")
        if variant_features.empty:
            st.info("No feature flags for this variant.")
        else:
            st.dataframe(
                variant_features[["feature_key", "feature_value"]],
                width="stretch",
                hide_index=True,
                height=220,
            )

        st.subheader("Patch Notes")
        if variant_patches.empty:
            st.info("No patch-note blocks for this variant.")
        else:
            for row in variant_patches.head(8).itertuples(index=False):
                with st.expander(row.heading_title):
                    st.write(row.content)

    st.subheader("Captured Sections")
    section_preview_df = variant_sections[
        ["heading_title", "section_path", "content"]
    ].copy()
    section_preview_df["content"] = preview_text(section_preview_df["content"], 320)
    render_readable_dataframe(
        section_preview_df,
        height=480,
        large_columns=("section_path", "content"),
        medium_columns=("heading_title",),
    )


def render_comparisons(data: dict[str, pd.DataFrame]) -> None:
    heroes_df = data["heroes"]
    variants_df = data["variants"]
    variant_leaderboard_df = data["variant_leaderboard"]
    progression_rows_df = data["progression_rows"]
    progression_values_df = data["progression_values"]
    progression_tracks_df = data["progression_tracks"]
    progression_relationships_df = data["progression_relationships"]

    comparison_rows_df = prepare_progression_rows_for_comparison(
        progression_rows_df,
        progression_relationships_df,
        variant_leaderboard_df,
    )
    if comparison_rows_df.empty:
        st.warning(
            "No normalized progression rows are available. Run the normalize step before opening comparisons."
        )
        return

    hero_options = sorted(
        name for name in heroes_df["name_en"].dropna().unique() if name
    )
    stage_options = (
        comparison_rows_df[["progression_stage_label", "stage_order"]]
        .drop_duplicates()
        .sort_values("stage_order")["progression_stage_label"]
        .tolist()
    )
    role_options = sorted(role for role in heroes_df["role"].dropna().unique() if role)
    variant_kind_options = sorted(
        kind for kind in variants_df["variant_kind"].dropna().unique() if kind
    )
    variant_kind_label_map = build_variant_kind_label_map(variants_df)
    skill_family_options = sorted(
        skill_family
        for skill_family in comparison_rows_df[
            comparison_rows_df["source_kind"] == "skill"
        ]["skill_family"]
        .dropna()
        .unique()
        if skill_family
    )

    st.subheader("Comparisons")
    st.caption(
        "These comparison views are read-only and explicit-only. They use normalized progression rows, exact modifier values, captured stage ladders, and explicit relationship facts from source text. Missing data means the source did not provide it or it was not captured."
    )

    top_metrics = st.columns(5)
    top_metrics[0].metric("Heroes", heroes_df["name_en"].nunique())
    top_metrics[1].metric("Progression Rows", len(comparison_rows_df))
    top_metrics[2].metric("Exact Values", len(progression_values_df))
    top_metrics[3].metric("Track Steps", len(progression_tracks_df))
    top_metrics[4].metric(
        "Explicit Relations",
        len(progression_relationships_df),
    )

    hero_tab, family_tab, stage_tab, role_tab = st.tabs(
        [
            "Hero vs Hero",
            "Skill Family Across Heroes",
            "Progression Stage",
            "Role / Archetype",
        ]
    )

    with hero_tab:
        left_col, right_col, shared_col = st.columns([1, 1, 0.9])
        left_hero = left_col.selectbox("Left hero", hero_options, index=0)
        right_default_index = 1 if len(hero_options) > 1 else 0
        right_hero = right_col.selectbox(
            "Right hero", hero_options, index=right_default_index
        )
        shared_only = shared_col.checkbox("Shared families only", value=True)

        stage_filter = st.multiselect("Stages", stage_options, default=[])
        kind_filter = st.multiselect(
            "Variant kinds",
            variant_kind_options,
            default=[],
            format_func=lambda kind: variant_kind_label_map.get(kind, kind),
        )

        hero_compare_df = build_hero_vs_hero_frame(
            comparison_rows_df,
            left_hero,
            right_hero,
            kind_filter,
            stage_filter,
            shared_only,
        )
        left_record = heroes_df[heroes_df["name_en"] == left_hero].iloc[0]
        right_record = heroes_df[heroes_df["name_en"] == right_hero].iloc[0]
        left_relations = progression_relationships_df[
            progression_relationships_df["name_en"] == left_hero
        ]
        right_relations = progression_relationships_df[
            progression_relationships_df["name_en"] == right_hero
        ]
        hero_metrics = st.columns(6)
        hero_metrics[0].metric(f"{left_hero} Rank", int(left_record["meta_rank"]))
        hero_metrics[1].metric(
            f"{left_hero} Score", round(float(left_record["final_meta_score"]), 2)
        )
        hero_metrics[2].metric(f"{left_hero} Relations", len(left_relations.index))
        hero_metrics[3].metric(f"{right_hero} Rank", int(right_record["meta_rank"]))
        hero_metrics[4].metric(
            f"{right_hero} Score", round(float(right_record["final_meta_score"]), 2)
        )
        hero_metrics[5].metric(f"{right_hero} Relations", len(right_relations.index))

        if hero_compare_df.empty:
            st.info("No normalized comparison rows matched the current filters.")
        else:
            display_df = hero_compare_df.rename(
                columns={
                    f"variant_titles_{left_hero}": f"{left_hero} variants",
                    f"source_names_{left_hero}": f"{left_hero} rows",
                    f"top_coefficient_{left_hero}": f"{left_hero} top damage",
                    f"modifiers_summary_{left_hero}": f"{left_hero} modifiers",
                    f"mechanics_summary_{left_hero}": f"{left_hero} mechanics",
                    f"stats_summary_{left_hero}": f"{left_hero} stats",
                    f"explicit_relationship_count_{left_hero}": f"{left_hero} relations",
                    f"coverage_rows_{left_hero}": f"{left_hero} rows captured",
                    f"variant_titles_{right_hero}": f"{right_hero} variants",
                    f"source_names_{right_hero}": f"{right_hero} rows",
                    f"top_coefficient_{right_hero}": f"{right_hero} top damage",
                    f"modifiers_summary_{right_hero}": f"{right_hero} modifiers",
                    f"mechanics_summary_{right_hero}": f"{right_hero} mechanics",
                    f"stats_summary_{right_hero}": f"{right_hero} stats",
                    f"explicit_relationship_count_{right_hero}": f"{right_hero} relations",
                    f"coverage_rows_{right_hero}": f"{right_hero} rows captured",
                }
            )
            render_readable_dataframe(
                display_df,
                height=420,
                large_columns=(
                    f"{left_hero} modifiers",
                    f"{left_hero} mechanics",
                    f"{left_hero} stats",
                    f"{right_hero} modifiers",
                    f"{right_hero} mechanics",
                    f"{right_hero} stats",
                ),
                medium_columns=(
                    "skill_family",
                    "progression_stage_label",
                    f"{left_hero} variants",
                    f"{right_hero} variants",
                    f"{left_hero} rows",
                    f"{right_hero} rows",
                ),
            )

        relationship_compare_df = progression_relationships_df[
            progression_relationships_df["name_en"].isin([left_hero, right_hero])
        ].copy()
        if not relationship_compare_df.empty:
            st.markdown("**Explicit Relationship Coverage**")
            render_readable_dataframe(
                build_relationship_frame(relationship_compare_df),
                height=240,
                large_columns=("evidence_text", "source_page"),
                medium_columns=("variant_label", "source_name", "target_source_name"),
            )
        else:
            st.caption(
                "No explicit non-stacking, overwrite, or exclusivity rules were captured for either selected hero."
            )

    with family_tab:
        selected_family = st.selectbox("Skill family", skill_family_options)
        role_filter = st.multiselect("Roles", role_options, default=[])
        kind_filter = st.multiselect(
            "Variant kinds",
            variant_kind_options,
            default=[],
            format_func=lambda kind: variant_kind_label_map.get(kind, kind),
        )
        stage_filter = st.multiselect("Stages", stage_options, default=[])

        family_rows_df = build_skill_family_comparison_frame(
            comparison_rows_df,
            selected_family,
            role_filter,
            kind_filter,
            stage_filter,
        )
        family_values_df = progression_values_df[
            progression_values_df["skill_family"] == selected_family
        ].copy()
        if role_filter:
            family_values_df = family_values_df[
                family_values_df["name_en"].isin(family_rows_df["name_en"].unique())
            ]
        if kind_filter:
            family_values_df = family_values_df[
                family_values_df["variant_title"].isin(
                    family_rows_df["variant_title"].unique()
                )
            ]
        if stage_filter:
            family_values_df = family_values_df[
                family_values_df["progression_stage_label"].isin(stage_filter)
            ]
        family_track_df = progression_tracks_df[
            progression_tracks_df["skill_family"] == selected_family
        ].copy()
        if role_filter:
            family_track_df = family_track_df[
                family_track_df["name_en"].isin(family_rows_df["name_en"].unique())
            ]
        if kind_filter:
            family_track_df = family_track_df[
                family_track_df["variant_title"].isin(
                    family_rows_df["variant_title"].unique()
                )
            ]
        if stage_filter:
            family_track_df = family_track_df[
                family_track_df["progression_stage_label"].isin(stage_filter)
            ]
        family_relationships_df = build_relationship_frame(
            progression_relationships_df,
            skill_family=selected_family,
        )
        if role_filter or kind_filter or stage_filter:
            allowed_variants = set(family_rows_df["variant_label"].tolist())
            allowed_sources = set(family_rows_df["source_name"].tolist())
            family_relationships_df = family_relationships_df[
                family_relationships_df["variant_label"].isin(allowed_variants)
                | family_relationships_df["source_name"].isin(allowed_sources)
            ].copy()

        family_metrics = st.columns(4)
        family_metrics[0].metric("Heroes", family_rows_df["name_en"].nunique())
        family_metrics[1].metric("Rows", len(family_rows_df.index))
        family_metrics[2].metric("Track Steps", len(family_track_df.index))
        family_metrics[3].metric(
            "Explicit Relations", len(family_relationships_df.index)
        )

        render_readable_dataframe(
            family_rows_df[
                [
                    "name_en",
                    "role",
                    "meta_rank",
                    "variant_kind_label",
                    "variant_label",
                    "progression_stage_label",
                    "source_name",
                    "skill_type",
                    "top_coefficient",
                    "modifiers_summary",
                    "mechanics_summary",
                    "stats_summary",
                    "explicit_relationship_count",
                    "excerpt",
                ]
            ],
            height=360,
            large_columns=(
                "modifiers_summary",
                "mechanics_summary",
                "stats_summary",
                "excerpt",
            ),
            medium_columns=("name_en", "variant_label", "source_name"),
        )

        family_detail_tab, family_track_tab, family_relation_tab = st.tabs(
            ["Exact Values", "Track Steps", "Explicit Relationships"]
        )
        with family_detail_tab:
            if family_values_df.empty:
                st.info("No exact modifier rows matched the current family filters.")
            else:
                render_readable_dataframe(
                    family_values_df[
                        [
                            "name_en",
                            "variant_label",
                            "progression_stage_label",
                            "source_name",
                            "value_kind",
                            "category",
                            "value_text",
                            "numeric_value",
                            "unit",
                            "context",
                        ]
                    ],
                    height=260,
                    large_columns=("context",),
                    medium_columns=(
                        "name_en",
                        "variant_label",
                        "source_name",
                        "value_text",
                    ),
                    number_formats={"numeric_value": "%.2f"},
                )
        with family_track_tab:
            if family_track_df.empty:
                st.info(
                    "No captured progression track steps matched the current family filters."
                )
            else:
                render_readable_dataframe(
                    family_track_df[
                        [
                            "name_en",
                            "variant_label",
                            "progression_stage_label",
                            "source_name",
                            "track_label",
                            "step_index",
                            "step_value",
                            "numeric_value",
                            "unit",
                        ]
                    ],
                    height=240,
                    medium_columns=(
                        "name_en",
                        "variant_label",
                        "source_name",
                        "track_label",
                    ),
                    number_formats={"numeric_value": "%.2f"},
                )
        with family_relation_tab:
            if family_relationships_df.empty:
                st.info(
                    "No explicit non-stacking, overwrite, or exclusivity rules were found for this skill family in captured sources."
                )
            else:
                render_readable_dataframe(
                    family_relationships_df,
                    height=220,
                    large_columns=("evidence_text", "source_page"),
                    medium_columns=(
                        "variant_label",
                        "source_name",
                        "target_source_name",
                    ),
                )

    with stage_tab:
        selected_stage = st.selectbox("Stage", stage_options)
        role_filter = st.multiselect("Roles", role_options, default=[])
        kind_filter = st.multiselect(
            "Variant kinds",
            variant_kind_options,
            default=[],
            format_func=lambda kind: variant_kind_label_map.get(kind, kind),
        )
        source_kind_filter = st.multiselect(
            "Source kinds",
            sorted(
                kind
                for kind in comparison_rows_df["source_kind"].dropna().unique()
                if kind
            ),
            default=[],
        )

        stage_rows_df = build_stage_comparison_frame(
            comparison_rows_df,
            selected_stage,
            role_filter,
            kind_filter,
            source_kind_filter,
        )
        stage_relationships_df = build_relationship_frame(
            progression_relationships_df[
                progression_relationships_df["progression_stage_label"]
                == selected_stage
            ]
        )
        if role_filter:
            stage_relationships_df = stage_relationships_df[
                stage_relationships_df["source_name"].isin(
                    stage_rows_df["source_name"].tolist()
                )
                | stage_relationships_df["variant_label"].isin(
                    stage_rows_df["variant_label"].tolist()
                )
            ]

        stage_metrics = st.columns(4)
        stage_metrics[0].metric("Heroes", stage_rows_df["name_en"].nunique())
        stage_metrics[1].metric("Rows", len(stage_rows_df.index))
        stage_metrics[2].metric(
            "Skill Families", stage_rows_df["skill_family"].nunique()
        )
        stage_metrics[3].metric("Explicit Relations", len(stage_relationships_df.index))

        if stage_rows_df.empty:
            st.info("No progression rows matched the current stage filters.")
        else:
            render_readable_dataframe(
                stage_rows_df[
                    [
                        "name_en",
                        "role",
                        "meta_rank",
                        "variant_kind_label",
                        "variant_label",
                        "source_kind",
                        "source_name",
                        "skill_family",
                        "top_coefficient",
                        "modifiers_summary",
                        "mechanics_summary",
                        "stats_summary",
                        "explicit_relationship_count",
                        "excerpt",
                    ]
                ],
                height=360,
                large_columns=(
                    "modifiers_summary",
                    "mechanics_summary",
                    "stats_summary",
                    "excerpt",
                ),
                medium_columns=(
                    "name_en",
                    "variant_label",
                    "source_name",
                    "skill_family",
                ),
            )
        if not stage_relationships_df.empty:
            st.markdown("**Explicit relationships for this stage**")
            render_readable_dataframe(
                stage_relationships_df,
                height=220,
                large_columns=("evidence_text", "source_page"),
                medium_columns=("variant_label", "source_name", "target_source_name"),
            )

    with role_tab:
        selected_roles = st.multiselect(
            "Roles",
            role_options,
            default=role_options[:2] if len(role_options) >= 2 else role_options,
        )
        kind_filter = st.multiselect(
            "Variant kinds",
            variant_kind_options,
            default=[],
            format_func=lambda kind: variant_kind_label_map.get(kind, kind),
        )
        stage_filter = st.multiselect("Stages", stage_options, default=[])

        role_coverage_df = build_role_stage_coverage_frame(
            comparison_rows_df,
            selected_roles,
            kind_filter,
            stage_filter,
        )
        role_family_df = build_role_family_frame(
            comparison_rows_df,
            selected_roles,
            kind_filter,
            stage_filter,
        )
        role_hero_df = variant_leaderboard_df[
            variant_leaderboard_df["role"].isin(selected_roles)
        ].copy()
        role_relationship_df = progression_relationships_df.merge(
            variant_leaderboard_df[["variant_id", "role"]],
            on="variant_id",
            how="left",
        )
        role_relationship_df = role_relationship_df[
            role_relationship_df["role"].isin(selected_roles)
        ].copy()
        if kind_filter:
            role_relationship_df = role_relationship_df[
                role_relationship_df["variant_kind"].isin(kind_filter)
            ]
        if stage_filter:
            role_relationship_df = role_relationship_df[
                role_relationship_df["progression_stage_label"].isin(stage_filter)
            ]

        role_metrics = st.columns(4)
        role_metrics[0].metric("Units", role_hero_df["variant_id"].nunique())
        role_metrics[1].metric(
            "Progression Rows",
            len(role_coverage_df.index)
            and int(role_coverage_df["row_count"].sum())
            or 0,
        )
        role_metrics[2].metric(
            "Skill Families", role_family_df["skill_family"].nunique()
        )
        role_metrics[3].metric("Explicit Relations", len(role_relationship_df.index))

        role_overview_tab, role_stage_tab, role_family_tab, role_relation_tab = st.tabs(
            [
                "Roster Overview",
                "Stage Coverage",
                "Skill Families",
                "Explicit Relationships",
            ]
        )
        with role_overview_tab:
            render_readable_dataframe(
                role_hero_df[
                    [
                        "meta_rank",
                        "name_en",
                        "variant_label",
                        "name_ko",
                        "role",
                        "rarity",
                        "adventure_tier",
                        "battle_tier",
                        "boss_tier",
                        "final_meta_score",
                    ]
                ].sort_values(["role", "meta_rank", "variant_label"]),
                height=280,
                medium_columns=("name_en", "variant_label", "name_ko", "role"),
                number_formats={"final_meta_score": "%.2f"},
            )
        with role_stage_tab:
            if role_coverage_df.empty:
                st.info("No stage coverage rows matched the current role filters.")
            else:
                render_readable_dataframe(
                    role_coverage_df,
                    height=260,
                    medium_columns=("role", "progression_stage_label"),
                )
        with role_family_tab:
            if role_family_df.empty:
                st.info(
                    "No skill-family comparison rows matched the current role filters."
                )
            else:
                render_readable_dataframe(
                    role_family_df,
                    height=320,
                    large_columns=("stage_coverage",),
                    medium_columns=("role", "skill_family"),
                )
        with role_relation_tab:
            if role_relationship_df.empty:
                st.info(
                    "No explicit non-stacking, overwrite, or exclusivity rules were captured for the current role filters."
                )
            else:
                relationship_preview_df = role_relationship_df[
                    [
                        "name_en",
                        "variant_label",
                        "progression_stage_label",
                        "source_name",
                        "relation_type",
                        "target_source_name",
                        "evidence_text",
                        "source_page",
                    ]
                ].copy()
                relationship_preview_df["relation_type"] = relationship_preview_df[
                    "relation_type"
                ].map(format_relation_type)
                relationship_preview_df["evidence_text"] = relationship_preview_df[
                    "evidence_text"
                ].map(lambda value: preview_value(str(value), 220))
                render_readable_dataframe(
                    relationship_preview_df,
                    height=260,
                    large_columns=("evidence_text", "source_page"),
                    medium_columns=(
                        "name_en",
                        "variant_label",
                        "source_name",
                        "target_source_name",
                    ),
                )


def render_team_lab(data: dict[str, pd.DataFrame]) -> None:
    variant_leaderboard_df = data["variant_leaderboard"]
    skills_df = data["skills"]
    sections_df = data["sections"]
    features_df = data["features"]
    progression_values_df = data["progression_values"]
    progression_tracks_df = data["progression_tracks"]
    progression_relationships_df = data["progression_relationships"]

    st.subheader("Team Lab")
    st.caption(
        "This view is explicit-only. It surfaces captured SP economy and defensive coverage from real source text and stored progression rows, but it does not simulate rotations, uptime, or true combat throughput."
    )

    roster_df = (
        variant_leaderboard_df[
            [
                "variant_id",
                "variant_label",
                "name_en",
                "role",
                "rarity",
                "meta_rank",
                "final_meta_score",
            ]
        ]
        .drop_duplicates(subset=["variant_id"])
        .sort_values(["meta_rank", "variant_label"])
        .reset_index(drop=True)
    )
    option_label_map = {
        int(row["variant_id"]): (
            f"#{int(row['meta_rank'])} {row['variant_label']} · {row['role']}"
        )
        for _, row in roster_df.iterrows()
    }
    default_team_ids = build_default_team_variant_ids(variant_leaderboard_df, size=4)
    selected_variant_ids = st.multiselect(
        "Units",
        roster_df["variant_id"].tolist(),
        default=default_team_ids,
        format_func=lambda variant_id: option_label_map.get(
            int(variant_id), str(variant_id)
        ),
        help="Select up to 4 units. If you choose more than 4, only the first 4 are analyzed.",
    )
    if len(selected_variant_ids) > 4:
        st.warning("Only the first 4 selected units are analyzed in Team Lab.")
        selected_variant_ids = selected_variant_ids[:4]
    if not selected_variant_ids:
        st.info("Select up to 4 units to inspect SP economy and defensive coverage.")
        return

    selected_roster_df = roster_df[
        roster_df["variant_id"].isin(selected_variant_ids)
    ].copy()
    selected_roster_df["selection_order"] = pd.Categorical(
        selected_roster_df["variant_id"],
        categories=selected_variant_ids,
        ordered=True,
    )
    selected_roster_df = selected_roster_df.sort_values("selection_order").drop(
        columns=["selection_order"]
    )

    team_sources_df = build_team_source_frame(
        skills_df,
        sections_df,
        features_df,
        selected_variant_ids,
    )
    sp_summary_df = build_team_sp_summary(team_sources_df)
    sp_evidence_df = build_team_sp_evidence_frame(team_sources_df)
    skill_cost_df = build_team_skill_cost_frame(team_sources_df)
    defense_evidence_df = build_team_defense_evidence_frame(team_sources_df)
    defense_summary_df = build_team_defense_summary(defense_evidence_df)
    member_snapshot_df = build_team_member_snapshot(selected_roster_df, team_sources_df)
    team_values_df = progression_values_df[
        progression_values_df["variant_id"].isin(selected_variant_ids)
    ].copy()
    team_tracks_df = progression_tracks_df[
        progression_tracks_df["variant_id"].isin(selected_variant_ids)
    ].copy()
    team_relationships_df = build_relationship_frame(
        progression_relationships_df[
            progression_relationships_df["variant_id"].isin(selected_variant_ids)
        ].copy()
    )

    metric_columns = st.columns(6)
    metric_columns[0].metric("Units", len(selected_variant_ids))
    metric_columns[1].metric("SP Clauses", len(sp_evidence_df.index))
    metric_columns[2].metric("Defensive Rows", len(defense_evidence_df.index))
    metric_columns[3].metric("Exact Values", len(team_values_df.index))
    metric_columns[4].metric("Track Steps", len(team_tracks_df.index))
    metric_columns[5].metric("Overlap Rules", len(team_relationships_df.index))

    summary_tab, sp_tab, defense_tab, values_tab, overlap_tab = st.tabs(
        [
            "Summary",
            "SP Economy",
            "Defense",
            "Exact Values",
            "Overlap Rules",
        ]
    )

    with summary_tab:
        render_readable_dataframe(
            selected_roster_df[
                [
                    "meta_rank",
                    "variant_label",
                    "name_en",
                    "role",
                    "rarity",
                    "final_meta_score",
                ]
            ],
            height=180,
            medium_columns=("variant_label", "name_en", "role"),
            number_formats={"final_meta_score": "%.2f"},
        )
        render_readable_dataframe(
            member_snapshot_df,
            height=260,
            medium_columns=("variant_label", "name_en", "role"),
            number_formats={
                "final_meta_score": "%.2f",
                "highest_sp_cost": "%.2f",
                "flat_sp_gain": "%.2f",
                "sp_per_second": "%.2f",
            },
        )
        left_col, right_col = st.columns(2)
        with left_col:
            if sp_summary_df.empty:
                st.info(
                    "No explicit SP generation or SP cost-management clauses were found for the selected units."
                )
            else:
                render_readable_dataframe(
                    sp_summary_df,
                    height=240,
                    large_columns=("examples",),
                    medium_columns=("economy_type",),
                )
        with right_col:
            if defense_summary_df.empty:
                st.info(
                    "No explicit defensive clauses were found for the selected units."
                )
            else:
                render_readable_dataframe(
                    defense_summary_df,
                    height=240,
                    large_columns=("examples",),
                    medium_columns=("defense_type",),
                )

    with sp_tab:
        if skill_cost_df.empty:
            st.info("No skill SP costs were captured for the selected units.")
        else:
            render_readable_dataframe(
                skill_cost_df,
                height=240,
                large_columns=("source_preview",),
                medium_columns=("variant_label", "source_name", "stage_label"),
                number_formats={"sp_cost": "%.2f", "cooldown_seconds": "%.2f"},
            )
        if sp_evidence_df.empty:
            st.info(
                "No explicit SP generation, discount, or free-cast clauses were captured for the selected units."
            )
        else:
            render_readable_dataframe(
                sp_evidence_df,
                height=320,
                large_columns=("context",),
                medium_columns=(
                    "variant_label",
                    "source_name",
                    "economy_type",
                    "value_text",
                ),
                number_formats={
                    "numeric_value": "%.2f",
                    "sp_cost": "%.2f",
                    "cooldown_seconds": "%.2f",
                },
            )

    with defense_tab:
        if defense_summary_df.empty:
            st.info(
                "No explicit defensive toolkit rows were captured for the selected units."
            )
        else:
            render_readable_dataframe(
                defense_summary_df,
                height=220,
                large_columns=("examples",),
                medium_columns=("defense_type",),
            )
        if defense_evidence_df.empty:
            st.info("No defensive evidence rows matched the selected units.")
        else:
            render_readable_dataframe(
                defense_evidence_df[
                    [
                        "variant_label",
                        "stage_label",
                        "source_kind",
                        "source_name",
                        "defense_type",
                        "strongest_value",
                        "duration_summary",
                        "target_summary",
                        "mechanics",
                        "context",
                    ]
                ],
                height=360,
                large_columns=("mechanics", "context"),
                medium_columns=(
                    "variant_label",
                    "source_name",
                    "defense_type",
                ),
            )

    with values_tab:
        st.caption(
            "These tables include all stored exact-value rows and progression ladders for the selected units. SP gain clauses are parsed live from source text above because the older normalized exact-value tables only index percentage and ladder captures."
        )
        value_tab, track_tab = st.tabs(["Exact Values", "Track Steps"])
        with value_tab:
            if team_values_df.empty:
                st.info(
                    "No stored exact-value rows are available for the selected units."
                )
            else:
                render_readable_dataframe(
                    team_values_df[
                        [
                            "variant_label",
                            "progression_stage_label",
                            "source_name",
                            "value_kind",
                            "category",
                            "value_text",
                            "numeric_value",
                            "unit",
                            "context",
                        ]
                    ],
                    height=360,
                    large_columns=("context",),
                    medium_columns=(
                        "variant_label",
                        "source_name",
                        "value_text",
                    ),
                    number_formats={"numeric_value": "%.2f"},
                )
        with track_tab:
            if team_tracks_df.empty:
                st.info(
                    "No stored progression-track rows are available for the selected units."
                )
            else:
                render_readable_dataframe(
                    team_tracks_df[
                        [
                            "variant_label",
                            "progression_stage_label",
                            "source_name",
                            "track_label",
                            "step_index",
                            "step_value",
                            "numeric_value",
                            "unit",
                            "context",
                        ]
                    ],
                    height=320,
                    large_columns=("context",),
                    medium_columns=(
                        "variant_label",
                        "source_name",
                        "track_label",
                    ),
                    number_formats={"numeric_value": "%.2f"},
                )

    with overlap_tab:
        if team_relationships_df.empty:
            st.info(
                "No explicit non-stacking, overwrite, or exclusivity rules were captured for the selected units."
            )
        else:
            render_readable_dataframe(
                team_relationships_df,
                height=320,
                large_columns=("evidence_text", "source_page"),
                medium_columns=(
                    "variant_label",
                    "source_name",
                    "target_source_name",
                ),
            )


def main() -> None:
    apply_styles()
    render_header()
    data = load_atlas()

    heroes_df = data["heroes"]
    variants_df = data["variants"]
    available_kinds = sorted(
        kind for kind in variants_df["variant_kind"].dropna().unique() if kind
    )
    variant_kind_label_map = build_variant_kind_label_map(variants_df)
    hero_options = sorted(
        name for name in heroes_df["name_en"].dropna().unique() if name
    )
    default_hero = DEFAULT_HERO if DEFAULT_HERO in hero_options else hero_options[0]

    with st.sidebar:
        st.header("Atlas Controls")
        st.caption("Search the captured database without touching raw files.")
        page = st.radio(
            "View",
            ["Overview", "Search", "Hero Dossier", "Comparisons", "Team Lab"],
            index=0,
        )

        hero_query = ""
        text_query = ""
        section_query = ""
        kind_filter: list[str] = []
        row_limit = 30
        show_section_expanders = False
        focus_hero = default_hero

        if page == "Search":
            hero_query = st.text_input("Hero filter", value="")
            text_query = st.text_input("Keyword", value="")
            section_query = st.text_input("Section filter", value="")
            kind_filter = st.multiselect(
                "Variant kinds",
                available_kinds,
                format_func=lambda kind: variant_kind_label_map.get(kind, kind),
            )
            row_limit = st.slider(
                "Result rows", min_value=10, max_value=100, value=30, step=5
            )
            show_section_expanders = st.checkbox("Show section previews", value=False)

        if page == "Hero Dossier":
            focus_hero = st.selectbox(
                "Choose hero", hero_options, index=hero_options.index(default_hero)
            )
            st.caption(
                f"Showing {len(hero_options)} heroes. Default is {default_hero}."
            )

        st.caption(f"Database: {DB_PATH}")

    if page == "Overview":
        render_overview(data)
    elif page == "Search":
        render_search(
            data,
            hero_query,
            text_query,
            section_query,
            kind_filter,
            row_limit,
            show_section_expanders,
        )
    elif page == "Comparisons":
        render_comparisons(data)
    elif page == "Team Lab":
        render_team_lab(data)
    else:
        render_dossier(data, focus_hero)


if __name__ == "__main__":
    main()
