from __future__ import annotations

import logging
import re
import sqlite3

import pandas as pd

from .explorer_skill_details import extract_skill_insight
from .ingest_spreadsheet import ingest_all as ingest_spreadsheet
from .paths import PROCESSED_DATA_DIR, RAW_DATA_DIR
from .scrapers.common import normalize_text
from .settings import RuntimeSettings
from .storage import read_csv


LOGGER = logging.getLogger(__name__)
RAW_STRATEGYWIKI = RAW_DATA_DIR / "strategywiki_heroes.csv"
RAW_STRATEGYWIKI_REFERENCE_NOTES = RAW_DATA_DIR / "strategywiki_reference_notes.csv"
RAW_STRATEGYWIKI_HERO_GROWTH_VALUES = (
    RAW_DATA_DIR / "strategywiki_hero_growth_values.csv"
)
RAW_NAMUWIKI = RAW_DATA_DIR / "namuwiki_heroes.csv"
RAW_NAMUWIKI_NOTES = RAW_DATA_DIR / "namuwiki_notes.csv"
RAW_NAMUWIKI_SYSTEM_REFERENCES = RAW_DATA_DIR / "namuwiki_system_references.csv"
RAW_NAMUWIKI_RELEASE_HISTORY = RAW_DATA_DIR / "namuwiki_release_history.csv"
RAW_NAMUWIKI_VARIANT_SECTIONS = RAW_DATA_DIR / "namuwiki_variant_sections.csv"
RAW_NAMUWIKI_VARIANT_SKILLS = RAW_DATA_DIR / "namuwiki_variant_skills.csv"
RAW_NAMUWIKI_VARIANT_FEATURES = RAW_DATA_DIR / "namuwiki_variant_features.csv"
RAW_CHASER = RAW_DATA_DIR / "fandom_chaser_traits.csv"
RAW_SKILLS = RAW_DATA_DIR / "fandom_skills.csv"
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
BRACKETED_PREFIX_PATTERN = re.compile(r"^(?:\[[^\]]+\]\s*)+")
NUMERIC_VALUE_PATTERN = re.compile(r"-?\d+(?:,\d{3})*(?:\.\d+)?")
VARIANT_KIND_SORT_ORDER = {"base": 0, "former": 1, "special": 2}
SPREADSHEET_ALIAS_PATTERN = re.compile(r"[^a-z0-9]+")
SPREADSHEET_VARIANT_ALIAS_FIXUPS = {
    "ercnard": ("Esnar", "base"),
    "ercnardt": ("Esnar", "former"),
}
SPREADSHEET_ADVENTURE_CONTENT_MODES = {
    "Raids",
    "Raid (17)",
    "GoC",
    "Chasm",
    "AoT",
    "AoT (2)",
    "AoT (3)",
    "AoT (4)",
    "AH (1)",
    "AH (2)",
    "AH (3)",
    "Merc",
}
SPREADSHEET_BATTLE_CONTENT_MODES = {"PvP ATK", "PvP DEF", "Arena"}
SPREADSHEET_BOSS_CONTENT_MODES = {
    "Berkas",
    "Void (R)",
    "Void (G)",
    "Void (B)",
    "WB (1)",
    "WB (2)",
    "WB (3)",
    "WB (4)",
    "WB (5)",
    "WB (6)",
    "Boss",
    "GB",
    "FC",
    "FC 2",
    "FC 3",
}
SPREADSHEET_ADVENTURE_TEAM_CONTENTS = {
    "Raids",
    "Aernasis Hammer",
    "Altar of Time",
    "Hell's Furnace Balance",
    "Hell's Furnace Life",
    "Hell's Furnace Retribution",
}
SPREADSHEET_BOSS_TEAM_CONTENTS = {
    "World Boss",
    "World Boss Season 2",
    "Guild Boss",
    "Final Core",
    "Berkas Lair",
}
VARIANT_ROLE_PATTERNS = (
    (re.compile(r"\b(?:guardian|tank|defen(?:se|sive))\b", re.IGNORECASE), "Tank"),
    (re.compile(r"\b(?:assault|warrior)\b", re.IGNORECASE), "Assault"),
    (re.compile(r"\b(?:magic|magical|mage)\b", re.IGNORECASE), "Mage"),
    (re.compile(r"\b(?:sniper|ranger|archer)\b", re.IGNORECASE), "Ranger"),
    (re.compile(r"\b(?:healing|healer|support)\b", re.IGNORECASE), "Healer"),
)
VARIANT_RARITY_PATTERN = re.compile(
    r"\b(SS|SR|S|A|B)\s*[- ]?(?:grade|rank)\b",
    re.IGNORECASE,
)


def resolve_hero_identities(
    strategy_df: pd.DataFrame, namuwiki_df: pd.DataFrame
) -> pd.DataFrame:
    namu_rows = [
        {str(key): str(value) for key, value in raw_row.items()}
        for raw_row in namuwiki_df.to_dict(orient="records")
        if str(raw_row.get("name_en_guess", "")).strip()
    ]
    rows_by_name: dict[str, list[dict[str, str]]] = {}
    for row in namu_rows:
        rows_by_name.setdefault(str(row["name_en_guess"]), []).append(row)

    resolved_rows: list[dict[str, object]] = []
    seen_names: set[str] = set()

    for raw_row in strategy_df.to_dict(orient="records"):
        name_en = str(raw_row.get("name_en", "")).strip()
        if not name_en:
            continue
        namu_candidates = rows_by_name.get(name_en, [])
        namu_row = next(
            (
                row
                for row in namu_candidates
                if row.get("variant_kind", "base") == "base"
            ),
            namu_candidates[0] if namu_candidates else None,
        )
        seen_names.add(name_en)

        resolved_rows.append(
            {
                "name_en": name_en,
                "name_ko": (namu_row or {}).get("name_ko", ""),
                "role": str(raw_row.get("role", "Unknown")) or "Unknown",
                "rarity": "SS" if namu_row else "S",
                "adventure_tier": str(raw_row.get("adventure", "B")) or "B",
                "battle_tier": str(raw_row.get("battle", "B")) or "B",
                "boss_tier": str(raw_row.get("boss", "B")) or "B",
                "sources": "strategywiki,namuwiki" if namu_row else "strategywiki",
            }
        )

    for name_en, namu_candidates in rows_by_name.items():
        if name_en in seen_names:
            continue
        representative_row = next(
            (
                row
                for row in namu_candidates
                if row.get("variant_kind", "base") == "base"
            ),
            namu_candidates[0],
        )
        resolved_rows.append(
            {
                "name_en": name_en,
                "name_ko": str(representative_row.get("name_ko", "")).strip(),
                "role": "Unknown",
                "rarity": str(representative_row.get("rarity", "SS")) or "SS",
                "adventure_tier": "B",
                "battle_tier": "B",
                "boss_tier": "B",
                "sources": "namuwiki",
            }
        )

    resolved_df = pd.DataFrame.from_records(resolved_rows)
    resolved_df.insert(0, "hero_id", range(1, len(resolved_df.index) + 1))
    LOGGER.info("Resolved %s heroes into the unified dataset", len(resolved_df.index))
    return resolved_df


def _score_components(
    adventure_tier: str,
    battle_tier: str,
    boss_tier: str,
    rarity: str,
    settings: RuntimeSettings,
) -> tuple[float, float, float]:
    tier_scores = settings.scoring["tier_scores"]
    mode_weights = settings.scoring["mode_weights"]
    rarity_multipliers = settings.scoring["rarity_multipliers"]
    chaser_multiplier = float(settings.scoring["chaser_multiplier"])

    adventure_score = tier_scores.get(str(adventure_tier), 2)
    battle_score = tier_scores.get(str(battle_tier), 2)
    boss_score = tier_scores.get(str(boss_tier), 2)

    base_score = (
        mode_weights["adventure"] * adventure_score
        + mode_weights["battle"] * battle_score
        + mode_weights["boss"] * boss_score
    )
    rarity_adjusted = base_score * rarity_multipliers.get(str(rarity), 1.0)
    final_meta_score = rarity_adjusted * chaser_multiplier
    return base_score, rarity_adjusted, final_meta_score


def _score_components_from_numeric(
    adventure_score: float,
    battle_score: float,
    boss_score: float,
    rarity: str,
    settings: RuntimeSettings,
) -> tuple[float, float, float]:
    mode_weights = settings.scoring["mode_weights"]
    rarity_multipliers = settings.scoring["rarity_multipliers"]
    chaser_multiplier = float(settings.scoring["chaser_multiplier"])

    base_score = (
        mode_weights["adventure"] * adventure_score
        + mode_weights["battle"] * battle_score
        + mode_weights["boss"] * boss_score
    )
    rarity_adjusted = base_score * rarity_multipliers.get(str(rarity), 1.0)
    final_meta_score = rarity_adjusted * chaser_multiplier
    return base_score, rarity_adjusted, final_meta_score


def _variant_kind_sort_key(variant_kind: str) -> int:
    return VARIANT_KIND_SORT_ORDER.get(str(variant_kind).strip(), 99)


def _build_variant_outline_lookup(variant_sections_df: pd.DataFrame) -> dict[str, str]:
    if variant_sections_df.empty or "variant_href" not in variant_sections_df.columns:
        return {}
    outline_rows = variant_sections_df[
        variant_sections_df["heading_title"]
        .astype(str)
        .str.strip()
        .str.lower()
        .eq("outline")
    ].copy()
    if outline_rows.empty:
        return {}
    outline_rows = outline_rows.drop_duplicates(subset=["variant_href"])
    return {
        str(raw_row["variant_href"]): str(raw_row.get("content", ""))
        for raw_row in outline_rows.to_dict(orient="records")
        if str(raw_row.get("variant_href", "")).strip()
    }


def _infer_variant_role(outline_text: str, fallback_role: str) -> str:
    cleaned = normalize_text(outline_text)
    for pattern, role in VARIANT_ROLE_PATTERNS:
        if pattern.search(cleaned):
            return role
    return str(fallback_role or "Unknown")


def _infer_variant_rarity(outline_text: str, fallback_rarity: str) -> str:
    match = VARIANT_RARITY_PATTERN.search(normalize_text(outline_text))
    if match is not None:
        return match.group(1).upper()
    return str(fallback_rarity or "SS")


def _synthetic_base_variant_href(hero_id: int) -> str:
    return f"synthetic://hero/{hero_id}/base"


def _normalize_spreadsheet_alias(value: str) -> str:
    cleaned = normalize_text(value).lower().strip()
    return SPREADSHEET_ALIAS_PATTERN.sub("", cleaned)


def _parse_spreadsheet_variant_identity(name: str) -> tuple[str, str]:
    cleaned = str(name).strip()
    if cleaned.endswith("(T)"):
        return cleaned[:-3].strip(), "former"
    if cleaned.endswith("(S)"):
        return cleaned[:-3].strip(), "special"
    if cleaned.endswith(" T"):
        return cleaned[:-2].strip(), "former"
    if cleaned.endswith(" S"):
        return cleaned[:-2].strip(), "special"
    return cleaned, "base"


def _build_spreadsheet_variant_alias_map(
    unit_data_df: pd.DataFrame,
) -> dict[str, tuple[str, str]]:
    alias_map: dict[str, tuple[str, str]] = {}
    if unit_data_df.empty:
        return alias_map

    for raw_row in unit_data_df.to_dict(orient="records"):
        base_name, variant_kind = _parse_spreadsheet_variant_identity(
            str(raw_row.get("name", ""))
        )
        if not base_name:
            continue
        canonical_key = (base_name, variant_kind)
        alias_candidates = {
            str(raw_row.get("name", "")),
            str(raw_row.get("longname", "")),
            str(raw_row.get("shortname", "")),
            str(raw_row.get("keysname", "")),
            base_name,
        }
        if variant_kind == "former":
            alias_candidates.update(
                {f"{base_name} T", f"{base_name}(T)", f"{base_name} (T)"}
            )
        elif variant_kind == "special":
            alias_candidates.update(
                {f"{base_name} S", f"{base_name}(S)", f"{base_name} (S)"}
            )

        for alias in alias_candidates:
            normalized_alias = _normalize_spreadsheet_alias(alias)
            if normalized_alias and normalized_alias not in alias_map:
                alias_map[normalized_alias] = canonical_key

    alias_map.update(SPREADSHEET_VARIANT_ALIAS_FIXUPS)
    return alias_map


def _resolve_spreadsheet_variant_key(
    name: str,
    alias_map: dict[str, tuple[str, str]],
    valid_variant_keys: set[tuple[str, str]],
) -> tuple[str, str] | None:
    normalized_name = _normalize_spreadsheet_alias(name)
    resolved = alias_map.get(normalized_name)
    if resolved in valid_variant_keys:
        return resolved

    parsed = _parse_spreadsheet_variant_identity(str(name))
    if parsed in valid_variant_keys:
        return parsed
    return None


def _split_variant_members(value: str) -> list[str]:
    return [item.strip() for item in str(value).split(",") if item.strip()]


def _content_mode_category(mode_name: str) -> str | None:
    if mode_name in SPREADSHEET_ADVENTURE_CONTENT_MODES:
        return "adventure"
    if mode_name in SPREADSHEET_BATTLE_CONTENT_MODES:
        return "battle"
    if mode_name in SPREADSHEET_BOSS_CONTENT_MODES:
        return "boss"
    return None


def _content_label_category(content_label: str) -> str | None:
    if content_label in SPREADSHEET_ADVENTURE_TEAM_CONTENTS:
        return "adventure"
    if content_label in SPREADSHEET_BOSS_TEAM_CONTENTS:
        return "boss"
    return None


def _legacy_tier_numeric(tier_letter: str, settings: RuntimeSettings) -> float:
    return float(settings.scoring["tier_scores"].get(str(tier_letter), 2))


def _mode_score_to_tier(score: float) -> str:
    if score >= 4.5:
        return "SS"
    if score >= 3.5:
        return "S"
    if score >= 2.5:
        return "A"
    if score >= 1.5:
        return "B"
    return "C"


def _signal_to_mode_score(signal: float) -> float:
    clamped = max(0.0, min(float(signal), 1.0))
    return 1.0 + (clamped * 4.0)


def _build_variant_signal_lookup(
    variant_profiles_df: pd.DataFrame,
    spreadsheet_sheets: dict[str, pd.DataFrame],
    settings: RuntimeSettings,
) -> dict[tuple[str, str], dict[str, object]]:
    if variant_profiles_df.empty:
        return {}

    unit_data_df = spreadsheet_sheets.get("unit_data", pd.DataFrame())
    if unit_data_df.empty:
        return {}

    alias_map = _build_spreadsheet_variant_alias_map(unit_data_df)
    valid_variant_keys = {
        (str(raw_row.get("name_en", "")), str(raw_row.get("variant_kind", "base")))
        for raw_row in variant_profiles_df.to_dict(orient="records")
    }
    metric_names = [
        "pve_rank_points",
        "adventure_usage",
        "battle_usage",
        "boss_usage",
        "adventure_mentions",
        "battle_mentions",
        "boss_mentions",
    ]
    metrics: dict[tuple[str, str], dict[str, float]] = {
        key: {metric_name: 0.0 for metric_name in metric_names}
        for key in valid_variant_keys
    }
    evidence: dict[tuple[str, str], dict[str, int]] = {
        key: {"adventure": 0, "battle": 0, "boss": 0} for key in valid_variant_keys
    }

    for raw_row in spreadsheet_sheets.get("pve_meta", pd.DataFrame()).to_dict(
        orient="records"
    ):
        variant_key = _resolve_spreadsheet_variant_key(
            str(raw_row.get("hero_name", "")),
            alias_map,
            valid_variant_keys,
        )
        if variant_key is None:
            continue
        tier_rank = int(raw_row.get("tier_rank", 5) or 5)
        if str(raw_row.get("meta_type", "PvE")) == "PvE":
            metrics[variant_key]["pve_rank_points"] = max(
                metrics[variant_key]["pve_rank_points"],
                max(1.0, 6.0 - float(tier_rank)),
            )
            evidence[variant_key]["adventure"] += 1
        else:
            metrics[variant_key]["battle_mentions"] += 0.5
            evidence[variant_key]["battle"] += 1

    for raw_row in spreadsheet_sheets.get("pvp_meta", pd.DataFrame()).to_dict(
        orient="records"
    ):
        for member_name in _split_variant_members(str(raw_row.get("members", ""))):
            variant_key = _resolve_spreadsheet_variant_key(
                member_name,
                alias_map,
                valid_variant_keys,
            )
            if variant_key is None:
                continue
            metrics[variant_key]["battle_mentions"] += 1.0
            evidence[variant_key]["battle"] += 1

    for raw_row in spreadsheet_sheets.get("content_usage", pd.DataFrame()).to_dict(
        orient="records"
    ):
        variant_key = _resolve_spreadsheet_variant_key(
            str(raw_row.get("hero_name", "")),
            alias_map,
            valid_variant_keys,
        )
        if variant_key is None:
            continue
        category = _content_mode_category(str(raw_row.get("content_mode", "")))
        if category is None:
            continue
        evidence[variant_key][category] += 1
        if bool(raw_row.get("is_viable", False)):
            metrics[variant_key][f"{category}_usage"] += 1.0

    for raw_row in spreadsheet_sheets.get("content_teams", pd.DataFrame()).to_dict(
        orient="records"
    ):
        category = _content_label_category(str(raw_row.get("content", "")))
        if category is None:
            continue
        mention_weight = (
            0.5 if str(raw_row.get("team_type", "main")) == "off_meta" else 1.0
        )
        for member_name in _split_variant_members(str(raw_row.get("members", ""))):
            variant_key = _resolve_spreadsheet_variant_key(
                member_name,
                alias_map,
                valid_variant_keys,
            )
            if variant_key is None:
                continue
            metrics[variant_key][f"{category}_mentions"] += mention_weight
            evidence[variant_key][category] += 1

    maxima = {
        metric_name: max(
            (metrics[key][metric_name] for key in valid_variant_keys),
            default=0.0,
        )
        for metric_name in metric_names
    }
    variant_weights = settings.scoring.get("variant_signal_weights", {})
    adventure_weights = variant_weights.get("adventure", {})
    battle_weights = variant_weights.get("battle", {})
    boss_weights = variant_weights.get("boss", {})
    spreadsheet_blend = float(variant_weights.get("spreadsheet_blend", 0.6))
    legacy_blend = float(variant_weights.get("legacy_blend", 0.4))

    signal_lookup: dict[tuple[str, str], dict[str, object]] = {}
    for raw_row in variant_profiles_df.to_dict(orient="records"):
        variant_key = (
            str(raw_row.get("name_en", "")),
            str(raw_row.get("variant_kind", "base")),
        )
        legacy_scores = {
            "adventure": _legacy_tier_numeric(
                str(raw_row.get("adventure_tier", "B")),
                settings,
            ),
            "battle": _legacy_tier_numeric(
                str(raw_row.get("battle_tier", "B")),
                settings,
            ),
            "boss": _legacy_tier_numeric(
                str(raw_row.get("boss_tier", "B")),
                settings,
            ),
        }
        row_signals: dict[str, float | None] = {
            "adventure": None,
            "battle": None,
            "boss": None,
        }

        if evidence[variant_key]["adventure"] > 0:
            pve_meta_norm = (
                metrics[variant_key]["pve_rank_points"] / maxima["pve_rank_points"]
                if maxima["pve_rank_points"]
                else 0.0
            )
            adventure_usage_norm = (
                metrics[variant_key]["adventure_usage"] / maxima["adventure_usage"]
                if maxima["adventure_usage"]
                else 0.0
            )
            adventure_mentions_norm = (
                metrics[variant_key]["adventure_mentions"]
                / maxima["adventure_mentions"]
                if maxima["adventure_mentions"]
                else 0.0
            )
            row_signals["adventure"] = (
                float(adventure_weights.get("pve_meta", 0.5)) * pve_meta_norm
                + float(adventure_weights.get("usage", 0.3)) * adventure_usage_norm
                + float(adventure_weights.get("teams", 0.2)) * adventure_mentions_norm
            )

        if evidence[variant_key]["battle"] > 0:
            battle_usage_norm = (
                metrics[variant_key]["battle_usage"] / maxima["battle_usage"]
                if maxima["battle_usage"]
                else 0.0
            )
            battle_mentions_norm = (
                metrics[variant_key]["battle_mentions"] / maxima["battle_mentions"]
                if maxima["battle_mentions"]
                else 0.0
            )
            row_signals["battle"] = (
                float(battle_weights.get("usage", 0.45)) * battle_usage_norm
                + float(battle_weights.get("teams", 0.55)) * battle_mentions_norm
            )

        if evidence[variant_key]["boss"] > 0:
            boss_usage_norm = (
                metrics[variant_key]["boss_usage"] / maxima["boss_usage"]
                if maxima["boss_usage"]
                else 0.0
            )
            boss_mentions_norm = (
                metrics[variant_key]["boss_mentions"] / maxima["boss_mentions"]
                if maxima["boss_mentions"]
                else 0.0
            )
            row_signals["boss"] = (
                float(boss_weights.get("usage", 0.6)) * boss_usage_norm
                + float(boss_weights.get("teams", 0.4)) * boss_mentions_norm
            )

        mode_scores: dict[str, float] = {}
        mode_tiers: dict[str, str] = {}
        uses_spreadsheet_signals = False
        for mode_name in ("adventure", "battle", "boss"):
            signal = row_signals[mode_name]
            if signal is None:
                mode_scores[mode_name] = legacy_scores[mode_name]
                mode_tiers[mode_name] = _mode_score_to_tier(legacy_scores[mode_name])
                continue
            uses_spreadsheet_signals = True
            spreadsheet_score = _signal_to_mode_score(signal)
            blended_score = (
                spreadsheet_blend * spreadsheet_score
                + legacy_blend * legacy_scores[mode_name]
            )
            mode_scores[mode_name] = round(blended_score, 4)
            mode_tiers[mode_name] = _mode_score_to_tier(blended_score)

        signal_lookup[variant_key] = {
            "adventure_mode_score": mode_scores["adventure"],
            "battle_mode_score": mode_scores["battle"],
            "boss_mode_score": mode_scores["boss"],
            "adventure_tier": mode_tiers["adventure"],
            "battle_tier": mode_tiers["battle"],
            "boss_tier": mode_tiers["boss"],
            "score_basis": (
                "spreadsheet_variant_signals"
                if uses_spreadsheet_signals
                else str(raw_row.get("score_basis", "inherited_hero_modes"))
            ),
        }

    return signal_lookup


def _apply_variant_signal_profiles(
    variant_profiles_df: pd.DataFrame,
    spreadsheet_sheets: dict[str, pd.DataFrame] | None,
    settings: RuntimeSettings | None,
) -> pd.DataFrame:
    if variant_profiles_df.empty or spreadsheet_sheets is None or settings is None:
        return variant_profiles_df

    result = variant_profiles_df.copy()
    result["adventure_mode_score"] = result["adventure_tier"].map(
        lambda value: _legacy_tier_numeric(str(value), settings)
    )
    result["battle_mode_score"] = result["battle_tier"].map(
        lambda value: _legacy_tier_numeric(str(value), settings)
    )
    result["boss_mode_score"] = result["boss_tier"].map(
        lambda value: _legacy_tier_numeric(str(value), settings)
    )

    signal_lookup = _build_variant_signal_lookup(result, spreadsheet_sheets, settings)
    if not signal_lookup:
        return result

    for index, raw_row in result.iterrows():
        variant_key = (
            str(raw_row.get("name_en", "")),
            str(raw_row.get("variant_kind", "base")),
        )
        signal_row = signal_lookup.get(variant_key)
        if signal_row is None:
            continue
        result.at[index, "adventure_mode_score"] = float(
            str(signal_row["adventure_mode_score"])
        )
        result.at[index, "battle_mode_score"] = float(
            str(signal_row["battle_mode_score"])
        )
        result.at[index, "boss_mode_score"] = float(str(signal_row["boss_mode_score"]))
        result.at[index, "adventure_tier"] = str(signal_row["adventure_tier"])
        result.at[index, "battle_tier"] = str(signal_row["battle_tier"])
        result.at[index, "boss_tier"] = str(signal_row["boss_tier"])
        result.at[index, "score_basis"] = str(signal_row["score_basis"])
    return result


def build_variant_profiles(
    heroes_df: pd.DataFrame,
    variants_df: pd.DataFrame,
    variant_sections_df: pd.DataFrame,
    settings: RuntimeSettings | None = None,
    spreadsheet_sheets: dict[str, pd.DataFrame] | None = None,
) -> pd.DataFrame:
    columns = [
        "hero_id",
        "name_en",
        "name_ko",
        "variant_name_en",
        "variant_kind",
        "variant_suffix",
        "availability_marker",
        "variant_title",
        "variant_href",
        "note_excerpt",
        "source",
        "variant_role",
        "variant_rarity",
        "adventure_tier",
        "battle_tier",
        "boss_tier",
        "score_basis",
    ]
    if heroes_df.empty:
        return pd.DataFrame(columns=columns)

    hero_rows_by_name = {
        str(raw_row["name_en"]): raw_row
        for raw_row in heroes_df.to_dict(orient="records")
    }
    outline_by_href = _build_variant_outline_lookup(variant_sections_df)
    rows: list[dict[str, object]] = []
    hero_ids_with_base_variant: set[int] = set()

    for raw_row in variants_df.to_dict(orient="records"):
        name_en = str(raw_row.get("name_en_guess", "")).strip()
        variant_href = str(raw_row.get("variant_href", "")).strip()
        if not name_en or name_en not in hero_rows_by_name or not variant_href:
            continue

        hero_row = hero_rows_by_name[name_en]
        outline_text = outline_by_href.get(variant_href, "")
        variant_kind = str(raw_row.get("variant_kind", "base") or "base")
        variant_rarity = str(raw_row.get("rarity", "") or hero_row.get("rarity", "SS"))
        rows.append(
            {
                "hero_id": int(hero_row["hero_id"]),
                "name_en": name_en,
                "name_ko": str(
                    raw_row.get("name_ko", "") or hero_row.get("name_ko", "")
                ),
                "variant_name_en": str(raw_row.get("variant_name_en", "") or name_en),
                "variant_kind": variant_kind,
                "variant_suffix": str(raw_row.get("variant_suffix", "")),
                "availability_marker": str(raw_row.get("availability_marker", "")),
                "variant_title": str(raw_row.get("variant_title", "") or name_en),
                "variant_href": variant_href,
                "note_excerpt": str(raw_row.get("note_excerpt", "")),
                "source": str(raw_row.get("source", "namuwiki")),
                "variant_role": _infer_variant_role(
                    outline_text, str(hero_row.get("role", "Unknown"))
                ),
                "variant_rarity": _infer_variant_rarity(outline_text, variant_rarity),
                "adventure_tier": str(hero_row.get("adventure_tier", "B")),
                "battle_tier": str(hero_row.get("battle_tier", "B")),
                "boss_tier": str(hero_row.get("boss_tier", "B")),
                "score_basis": "inherited_hero_modes",
            }
        )
        if variant_kind == "base":
            hero_ids_with_base_variant.add(int(hero_row["hero_id"]))

    for hero_row in heroes_df.to_dict(orient="records"):
        hero_id = int(hero_row["hero_id"])
        if hero_id in hero_ids_with_base_variant:
            continue
        rows.append(
            {
                "hero_id": hero_id,
                "name_en": str(hero_row["name_en"]),
                "name_ko": str(hero_row.get("name_ko", "")),
                "variant_name_en": str(hero_row["name_en"]),
                "variant_kind": "base",
                "variant_suffix": "",
                "availability_marker": "",
                "variant_title": str(hero_row["name_en"]),
                "variant_href": _synthetic_base_variant_href(hero_id),
                "note_excerpt": "",
                "source": "synthetic_base",
                "variant_role": str(hero_row.get("role", "Unknown")),
                "variant_rarity": str(hero_row.get("rarity", "SS")),
                "adventure_tier": str(hero_row.get("adventure_tier", "B")),
                "battle_tier": str(hero_row.get("battle_tier", "B")),
                "boss_tier": str(hero_row.get("boss_tier", "B")),
                "score_basis": "inherited_hero_modes",
            }
        )

    variant_profiles_df = pd.DataFrame.from_records(rows)
    if variant_profiles_df.empty:
        return pd.DataFrame(columns=columns)
    variant_profiles_df = _apply_variant_signal_profiles(
        variant_profiles_df,
        spreadsheet_sheets,
        settings,
    )
    variant_profiles_df = variant_profiles_df.drop_duplicates(subset=["variant_href"])
    variant_profiles_df["variant_sort_order"] = variant_profiles_df["variant_kind"].map(
        _variant_kind_sort_key
    )
    variant_profiles_df = variant_profiles_df.sort_values(
        ["name_en", "variant_sort_order", "variant_name_en", "variant_title"],
        ascending=[True, True, True, True],
        ignore_index=True,
    )
    return variant_profiles_df.drop(columns=["variant_sort_order"])


def compute_variant_meta_scores(
    variant_profiles_df: pd.DataFrame,
    settings: RuntimeSettings,
) -> pd.DataFrame:
    if variant_profiles_df.empty:
        return pd.DataFrame(
            columns=[
                "variant_id",
                "hero_id",
                "base_score",
                "rarity_adjusted",
                "final_meta_score",
                "meta_rank",
                "score_basis",
            ]
        )

    score_rows: list[dict[str, object]] = []
    for raw_row in variant_profiles_df.to_dict(orient="records"):
        if all(
            raw_row.get(column_name, "") != ""
            for column_name in (
                "adventure_mode_score",
                "battle_mode_score",
                "boss_mode_score",
            )
        ):
            base_score, rarity_adjusted, final_meta_score = (
                _score_components_from_numeric(
                    float(raw_row.get("adventure_mode_score", 2.0)),
                    float(raw_row.get("battle_mode_score", 2.0)),
                    float(raw_row.get("boss_mode_score", 2.0)),
                    str(raw_row.get("variant_rarity", "SS")),
                    settings,
                )
            )
        else:
            base_score, rarity_adjusted, final_meta_score = _score_components(
                str(raw_row.get("adventure_tier", "B")),
                str(raw_row.get("battle_tier", "B")),
                str(raw_row.get("boss_tier", "B")),
                str(raw_row.get("variant_rarity", "SS")),
                settings,
            )
        score_rows.append(
            {
                "variant_id": int(raw_row["variant_id"]),
                "hero_id": int(raw_row["hero_id"]),
                "base_score": round(base_score, 2),
                "rarity_adjusted": round(rarity_adjusted, 2),
                "final_meta_score": round(final_meta_score, 2),
                "score_basis": str(raw_row.get("score_basis", "inherited_hero_modes")),
                "name_en": str(raw_row.get("name_en", "")),
                "variant_kind": str(raw_row.get("variant_kind", "base")),
                "variant_title": str(raw_row.get("variant_title", "")),
            }
        )

    score_df = pd.DataFrame.from_records(score_rows)
    score_df["variant_sort_order"] = score_df["variant_kind"].map(
        _variant_kind_sort_key
    )
    score_df = score_df.sort_values(
        ["final_meta_score", "name_en", "variant_sort_order", "variant_title"],
        ascending=[False, True, True, True],
        ignore_index=True,
    )
    score_df["meta_rank"] = score_df.index + 1
    return score_df[
        [
            "variant_id",
            "hero_id",
            "base_score",
            "rarity_adjusted",
            "final_meta_score",
            "meta_rank",
            "score_basis",
        ]
    ]


def compute_meta_scores(
    heroes_df: pd.DataFrame, settings: RuntimeSettings
) -> pd.DataFrame:
    score_rows: list[dict[str, object]] = []
    for raw_row in heroes_df.to_dict(orient="records"):
        base_score, rarity_adjusted, final_meta_score = _score_components(
            str(raw_row["adventure_tier"]),
            str(raw_row["battle_tier"]),
            str(raw_row["boss_tier"]),
            str(raw_row["rarity"]),
            settings,
        )
        score_rows.append(
            {
                "hero_id": int(raw_row["hero_id"]),
                "base_score": round(base_score, 2),
                "rarity_adjusted": round(rarity_adjusted, 2),
                "final_meta_score": round(final_meta_score, 2),
            }
        )

    score_df = pd.DataFrame.from_records(score_rows).sort_values(
        "final_meta_score", ascending=False, ignore_index=True
    )
    score_df["meta_rank"] = score_df.index + 1
    LOGGER.info("Computed meta scores for %s heroes", len(score_df.index))
    return score_df


def _build_mode_rows(
    heroes_df: pd.DataFrame, settings: RuntimeSettings
) -> list[tuple[int, str, str, int]]:
    tier_scores = settings.scoring["tier_scores"]
    rows: list[tuple[int, str, str, int]] = []
    for raw_row in heroes_df.to_dict(orient="records"):
        for mode, column in (
            ("adventure", "adventure_tier"),
            ("battle", "battle_tier"),
            ("boss", "boss_tier"),
        ):
            tier_letter = str(raw_row[column]) or "B"
            rows.append(
                (
                    int(raw_row["hero_id"]),
                    mode,
                    tier_letter,
                    int(tier_scores.get(tier_letter, 2)),
                )
            )
    return rows


def _join_or_dash(items: list[str], limit: int = 8) -> str:
    return ", ".join(items[:limit]) if items else "-"


def _preview_text(text: str, length: int = 240) -> str:
    return re.sub(r"\s+", " ", text).strip()[:length]


def _as_optional_int(value: object) -> int | None:
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _extract_numeric_value(value: object) -> float | None:
    match = NUMERIC_VALUE_PATTERN.search(str(value))
    if match is None:
        return None
    return float(match.group(0).replace(",", ""))


def _max_numeric_token(values: list[str]) -> str:
    numeric_pairs: list[tuple[float, str]] = []
    for value in values:
        match = NUMERIC_VALUE_PATTERN.search(str(value))
        if match is None:
            continue
        numeric_pairs.append((float(match.group(0).replace(",", "")), str(value)))
    if not numeric_pairs:
        return "-"
    return max(numeric_pairs, key=lambda item: item[0])[1]


def _normalize_skill_family_name(skill_name: str) -> str:
    normalized = BRACKETED_PREFIX_PATTERN.sub("", skill_name).strip()
    normalized = re.sub(
        r"^(?:imprint of |imprint |engraving |stamp )",
        "",
        normalized,
        flags=re.IGNORECASE,
    )
    return normalized.strip() or skill_name


def _progression_stage_metadata(stage_key: str) -> dict[str, object]:
    return PROGRESSION_STAGE_METADATA.get(
        stage_key, PROGRESSION_STAGE_METADATA["other"]
    )


def _classify_skill_progression_row(skill_stage: str, skill_type: str) -> str:
    if skill_stage == "enhancement_i":
        return "enhancement_i"
    if skill_stage == "enhancement_ii":
        return "enhancement_ii"
    if skill_stage == "imprint":
        return "imprint"
    if skill_type == "chaser":
        return "base_chaser"
    return "base_skill"


def _classify_section_progression(heading_title: str) -> str | None:
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


def _format_progression_tracks(tracks: list[object]) -> str:
    if not tracks:
        return "-"
    return "; ".join(f"{track.label}: {' / '.join(track.values)}" for track in tracks)


def _insight_progression_tracks(insight: object) -> list[object]:
    return list(getattr(insight, "progression_tracks", []) or [])


def _insight_explicit_relationships(insight: object) -> list[object]:
    return list(getattr(insight, "explicit_relationships", []) or [])


def _summarize_progression_modifiers(insight) -> str:
    tracks = _insight_progression_tracks(insight)
    return (
        "; ".join(
            value
            for value in (
                (
                    f"Cooldown {insight.cooldown_seconds} s"
                    if insight.cooldown_seconds
                    else ""
                ),
                f"SP {insight.sp_cost}" if insight.sp_cost else "",
                (
                    f"Top damage {_max_numeric_token(insight.coefficients)}"
                    if insight.coefficients
                    else ""
                ),
                (
                    f"Durations {_join_or_dash(insight.durations, 6)}"
                    if insight.durations
                    else ""
                ),
                (
                    f"Stacks {_join_or_dash(insight.stack_mentions, 4)}"
                    if insight.stack_mentions
                    else ""
                ),
                (
                    f"Chances {_join_or_dash(insight.chance_mentions, 4)}"
                    if insight.chance_mentions
                    else ""
                ),
                (
                    f"Thresholds {_join_or_dash(insight.threshold_mentions, 4)}"
                    if insight.threshold_mentions
                    else ""
                ),
                (f"Ladders {_format_progression_tracks(tracks)}" if tracks else ""),
            )
            if value
        )
        or "-"
    )


def _parse_numeric_value(value_text: str) -> tuple[float | None, str]:
    match = NUMERIC_VALUE_PATTERN.search(str(value_text))
    if match is None:
        return None, "text"
    numeric_value = float(match.group(0).replace(",", ""))
    lowered = str(value_text).lower()
    if "%" in str(value_text):
        return numeric_value, "percent"
    if "second" in lowered:
        return numeric_value, "seconds"
    if "stack" in lowered:
        return numeric_value, "stacks"
    if "chance" in lowered:
        return numeric_value, "chance"
    if any(
        keyword in lowered
        for keyword in (
            "enemy",
            "enemies",
            "ally",
            "allies",
            "hero",
            "heroes",
            "party members",
        )
    ):
        return numeric_value, "targets"
    return numeric_value, "flat"


def _extract_equipment_rows(text: str) -> list[dict[str, int]]:
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
    rows: list[dict[str, int]] = []
    for index in range(0, len(values), 5):
        level, physical_attack, vitality, physical_defense, magic_defense = values[
            index : index + 5
        ]
        rows.append(
            {
                "level": int(level.replace(",", "")),
                "physical_attack": int(physical_attack.replace(",", "")),
                "vitality": int(vitality.replace(",", "")),
                "physical_defense": int(physical_defense.replace(",", "")),
                "magic_defense": int(magic_defense.replace(",", "")),
            }
        )
    return rows


def build_progression_records(
    variants_df: pd.DataFrame,
    variant_sections_df: pd.DataFrame,
    variant_skills_df: pd.DataFrame,
    variant_features_df: pd.DataFrame,
    hero_id_by_name: dict[str, int],
    variant_id_by_href: dict[str, int],
) -> dict[str, list[tuple[object, ...]]]:
    records: dict[str, list[tuple[object, ...]]] = {
        "rows": [],
        "values": [],
        "tracks": [],
        "tags": [],
        "relationships": [],
        "equipment": [],
    }
    variant_meta_by_href = {
        str(raw_row.get("variant_href", "")): {
            "hero_id": hero_id_by_name[str(raw_row.get("name_en_guess", ""))],
            "variant_id": variant_id_by_href[str(raw_row.get("variant_href", ""))],
            "variant_title": str(raw_row.get("variant_title", "")),
            "variant_kind": str(raw_row.get("variant_kind", "base")),
        }
        for raw_row in variants_df.to_dict(orient="records")
        if str(raw_row.get("name_en_guess", "")) in hero_id_by_name
        and str(raw_row.get("variant_href", "")) in variant_id_by_href
    }

    def add_child_records(progression_key: str, insight, source_page: str) -> None:
        for mention in insight.numeric_mentions:
            numeric_value, unit = _parse_numeric_value(mention.value)
            records["values"].append(
                (
                    progression_key,
                    "numeric_mention",
                    mention.category,
                    mention.value,
                    numeric_value,
                    unit,
                    mention.context,
                )
            )
        for duration in insight.durations:
            numeric_value, unit = _parse_numeric_value(duration)
            records["values"].append(
                (
                    progression_key,
                    "duration",
                    "duration",
                    duration,
                    numeric_value,
                    unit,
                    duration,
                )
            )
        for stack in insight.stack_mentions:
            numeric_value, unit = _parse_numeric_value(stack)
            records["values"].append(
                (progression_key, "stack", "stack", stack, numeric_value, unit, stack)
            )
        for chance in insight.chance_mentions:
            numeric_value, unit = _parse_numeric_value(chance)
            records["values"].append(
                (
                    progression_key,
                    "chance",
                    "chance",
                    chance,
                    numeric_value,
                    unit,
                    chance,
                )
            )
        for threshold in insight.threshold_mentions:
            numeric_value, unit = _parse_numeric_value(threshold)
            records["values"].append(
                (
                    progression_key,
                    "threshold",
                    "threshold",
                    threshold,
                    numeric_value,
                    unit,
                    threshold,
                )
            )
        for target in insight.target_mentions:
            numeric_value, unit = _parse_numeric_value(target)
            records["values"].append(
                (
                    progression_key,
                    "target",
                    "target",
                    target,
                    numeric_value,
                    unit,
                    target,
                )
            )
        for stat_bonus in insight.stat_bonuses:
            numeric_value, unit = _parse_numeric_value(stat_bonus.value)
            records["values"].append(
                (
                    progression_key,
                    "stat_bonus",
                    stat_bonus.stat,
                    stat_bonus.value,
                    numeric_value,
                    unit,
                    stat_bonus.stat,
                )
            )
        for track in _insight_progression_tracks(insight):
            for step_index, step_value in enumerate(track.values):
                numeric_value, unit = _parse_numeric_value(step_value)
                records["tracks"].append(
                    (
                        progression_key,
                        track.label,
                        step_index,
                        step_value,
                        numeric_value,
                        unit,
                        track.context,
                    )
                )
        for tag in insight.mechanic_tags:
            records["tags"].append((progression_key, "mechanic", tag))
        for tag in insight.stat_tags:
            records["tags"].append((progression_key, "stat", tag))
        for tag in insight.mode_tags:
            records["tags"].append((progression_key, "mode", tag))
        for relationship in _insight_explicit_relationships(insight):
            records["relationships"].append(
                (
                    progression_key,
                    relationship.relation_type,
                    relationship.relation_scope,
                    relationship.target_text,
                    _normalize_skill_family_name(relationship.target_text),
                    relationship.evidence_text,
                    source_page,
                    "explicit",
                )
            )

    for raw_row in variant_skills_df.to_dict(orient="records"):
        variant_href = str(raw_row.get("variant_href", ""))
        meta = variant_meta_by_href.get(variant_href)
        if meta is None:
            continue
        insight = extract_skill_insight(str(raw_row.get("description", "")))
        tracks = _insight_progression_tracks(insight)
        stage_key = _classify_skill_progression_row(
            str(raw_row.get("skill_stage", "")),
            str(raw_row.get("skill_type", "")),
        )
        stage_metadata = _progression_stage_metadata(stage_key)
        progression_key = f"skill:{meta['variant_id']}:{raw_row.get('heading_id', '')}"
        records["rows"].append(
            (
                progression_key,
                int(meta["hero_id"]),
                int(meta["variant_id"]),
                str(meta["variant_title"]),
                str(meta["variant_kind"]),
                "skill",
                str(raw_row.get("skill_name", "")),
                _normalize_skill_family_name(str(raw_row.get("skill_name", ""))),
                stage_key,
                str(stage_metadata["label"]),
                int(stage_metadata["order"]),
                str(raw_row.get("skill_stage", "")),
                str(raw_row.get("skill_type", "")),
                _format_progression_tracks(tracks),
                _summarize_progression_modifiers(insight),
                _join_or_dash(insight.mechanic_tags, 8),
                _join_or_dash(insight.stat_tags, 8),
                _max_numeric_token(insight.coefficients),
                _preview_text(insight.body_text, 220),
                str(raw_row.get("source_page", "")),
            )
        )
        add_child_records(progression_key, insight, str(raw_row.get("source_page", "")))

    for raw_row in variant_sections_df.to_dict(orient="records"):
        variant_href = str(raw_row.get("variant_href", ""))
        meta = variant_meta_by_href.get(variant_href)
        if meta is None:
            continue
        stage_key = _classify_section_progression(str(raw_row.get("heading_title", "")))
        if stage_key is None:
            continue
        insight = extract_skill_insight(str(raw_row.get("content", "")))
        tracks = _insight_progression_tracks(insight)
        stage_metadata = _progression_stage_metadata(stage_key)
        progression_key = f"section:{meta['variant_id']}:{raw_row.get('heading_id', '')}:{raw_row.get('heading_title', '')}"
        records["rows"].append(
            (
                progression_key,
                int(meta["hero_id"]),
                int(meta["variant_id"]),
                str(meta["variant_title"]),
                str(meta["variant_kind"]),
                "section",
                str(raw_row.get("heading_title", "")),
                str(raw_row.get("heading_title", "")),
                stage_key,
                str(stage_metadata["label"]),
                int(stage_metadata["order"]),
                "",
                "",
                _format_progression_tracks(tracks),
                _summarize_progression_modifiers(insight),
                _join_or_dash(insight.mechanic_tags, 8),
                _join_or_dash(insight.stat_tags, 8),
                _max_numeric_token(insight.coefficients),
                _preview_text(str(raw_row.get("content", "")), 220),
                str(raw_row.get("source_page", "")),
            )
        )
        add_child_records(progression_key, insight, str(raw_row.get("source_page", "")))
        if stage_key == "gear":
            for equipment_row in _extract_equipment_rows(
                str(raw_row.get("content", ""))
            ):
                records["equipment"].append(
                    (
                        progression_key,
                        str(raw_row.get("heading_title", "")),
                        int(equipment_row["level"]),
                        int(equipment_row["physical_attack"]),
                        int(equipment_row["vitality"]),
                        int(equipment_row["physical_defense"]),
                        int(equipment_row["magic_defense"]),
                    )
                )

    for raw_row in variant_features_df.to_dict(orient="records"):
        variant_href = str(raw_row.get("variant_href", ""))
        meta = variant_meta_by_href.get(variant_href)
        if meta is None:
            continue
        feature_key = str(raw_row.get("feature_key", ""))
        feature_value = str(raw_row.get("feature_value", ""))
        progression_key = f"feature:{meta['variant_id']}:{feature_key}"
        stage_metadata = _progression_stage_metadata("feature")
        insight = extract_skill_insight(feature_value)
        tracks = _insight_progression_tracks(insight)
        records["rows"].append(
            (
                progression_key,
                int(meta["hero_id"]),
                int(meta["variant_id"]),
                str(meta["variant_title"]),
                str(meta["variant_kind"]),
                "feature",
                FEATURE_LABELS.get(feature_key, feature_key.replace("_", " ").title()),
                "System",
                "feature",
                str(stage_metadata["label"]),
                int(stage_metadata["order"]),
                "",
                "",
                _format_progression_tracks(tracks),
                feature_value or "-",
                _join_or_dash(insight.mechanic_tags, 8),
                _join_or_dash(insight.stat_tags, 8),
                _max_numeric_token(insight.coefficients),
                _preview_text(feature_value, 220),
                str(raw_row.get("source_page", "")),
            )
        )
        add_child_records(progression_key, insight, str(raw_row.get("source_page", "")))
        records["tags"].append((progression_key, "feature", feature_key))

    return records


def _populate_spreadsheet_tables(
    cursor: sqlite3.Cursor,
    sheets: dict[str, pd.DataFrame],
) -> None:
    """Insert parsed spreadsheet DataFrames into the meta_* tables."""

    def _insert(table: str, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        cols = [c for c in df.columns if not c.endswith("_label")]
        placeholders = ", ".join("?" for _ in cols)
        col_list = ", ".join(cols)
        rows = [
            tuple(
                (int(v) if isinstance(v, bool) else v) for v in (row[c] for c in cols)
            )
            for _, row in df.iterrows()
        ]
        cursor.executemany(
            f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})",  # noqa: S608
            rows,
        )
        return len(rows)

    counts: dict[str, int] = {}

    # Unit data
    ud = sheets.get("unit_data", pd.DataFrame())
    if not ud.empty:
        keep = [
            "name",
            "longname",
            "shortname",
            "keysname",
            "attribute",
            "color",
            "unit_class",
            "job_type",
            "kr_release_date",
            "released",
            "is_pve",
            "is_pvp",
            "is_support",
            "ht1",
            "hp1",
            "ht2",
            "hp2",
            "ht3",
            "hp3",
            "ht4",
            "hp4",
            "ht5",
            "hp5",
            "ct1",
            "cp1",
            "ct2",
            "cp2",
            "ct3",
            "cp3",
            "ct4",
            "cp4",
            "ct5",
            "cp5",
            "cs_level",
            "rn1",
            "rn2",
            "artifact",
            "ac1",
            "ac2",
            "ac3",
            "equip_set",
            "tc1",
            "mt1",
            "tt1",
            "tt2",
            "tc2",
            "mt2",
            "tt3",
            "tt4",
            "si_build_label",
            "ps",
            "s1",
            "s2",
            "ss",
            "cs1",
            "cs2",
            "si_ps",
            "si_s1",
            "si_s2",
            "si_cs",
            "descent",
        ]
        existing = [c for c in keep if c in ud.columns]
        # map si_build_label → si_build for DB
        ud_db = ud[existing].copy()
        if "si_build_label" in ud_db.columns:
            ud_db = ud_db.rename(columns={"si_build_label": "si_build"})
        for bcol in ("released", "is_pve", "is_pvp", "is_support"):
            if bcol in ud_db.columns:
                ud_db[bcol] = ud_db[bcol].apply(
                    lambda v: 1 if v is True else (0 if v is False else None)
                )
        counts["meta_unit_data"] = _insert("meta_unit_data", ud_db)

    # Builds
    counts["meta_builds"] = _insert("meta_builds", sheets.get("builds", pd.DataFrame()))

    # PvE Meta
    counts["meta_pve_meta"] = _insert(
        "meta_pve_meta", sheets.get("pve_meta", pd.DataFrame())
    )

    # PvP Meta
    counts["meta_pvp_meta"] = _insert(
        "meta_pvp_meta", sheets.get("pvp_meta", pd.DataFrame())
    )

    # Content Usage
    cu = sheets.get("content_usage", pd.DataFrame())
    if not cu.empty:
        cu_db = cu.copy()
        cu_db["is_viable"] = cu_db["is_viable"].apply(lambda v: 1 if v else 0)
        counts["meta_content_usage"] = _insert("meta_content_usage", cu_db)

    # Content Teams
    counts["meta_content_teams"] = _insert(
        "meta_content_teams", sheets.get("content_teams", pd.DataFrame())
    )

    # Equipment Presets
    counts["meta_equipment_presets"] = _insert(
        "meta_equipment_presets", sheets.get("equipment_presets", pd.DataFrame())
    )

    # Soul Imprint
    counts["meta_soul_imprint"] = _insert(
        "meta_soul_imprint", sheets.get("soul_imprint", pd.DataFrame())
    )

    # Changelog
    counts["meta_changelog"] = _insert(
        "meta_changelog", sheets.get("changelog", pd.DataFrame())
    )

    # Release Order
    counts["meta_release_order"] = _insert(
        "meta_release_order", sheets.get("release_order", pd.DataFrame())
    )

    # Content Keys
    counts["meta_content_keys"] = _insert(
        "meta_content_keys", sheets.get("content_keys", pd.DataFrame())
    )

    # Beginners Guide
    counts["meta_beginners_guide"] = _insert(
        "meta_beginners_guide", sheets.get("beginners_guide", pd.DataFrame())
    )

    for table, n in counts.items():
        if n:
            LOGGER.info("Inserted %d rows into %s", n, table)


def build_database(
    heroes_df: pd.DataFrame,
    scores_df: pd.DataFrame,
    variants_df: pd.DataFrame,
    notes_df: pd.DataFrame,
    system_references_df: pd.DataFrame,
    system_reference_values_df: pd.DataFrame,
    release_history_df: pd.DataFrame,
    variant_sections_df: pd.DataFrame,
    variant_skills_df: pd.DataFrame,
    variant_features_df: pd.DataFrame,
    chaser_df: pd.DataFrame,
    skills_df: pd.DataFrame,
    settings: RuntimeSettings,
) -> None:
    settings.database_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(settings.database_path) as connection:
        cursor = connection.cursor()
        cursor.executescript(
            """
            PRAGMA foreign_keys = OFF;

            DROP TABLE IF EXISTS meta_beginners_guide;
            DROP TABLE IF EXISTS meta_content_keys;
            DROP TABLE IF EXISTS meta_release_order;
            DROP TABLE IF EXISTS meta_changelog;
            DROP TABLE IF EXISTS meta_soul_imprint;
            DROP TABLE IF EXISTS meta_equipment_presets;
            DROP TABLE IF EXISTS meta_content_teams;
            DROP TABLE IF EXISTS meta_content_usage;
            DROP TABLE IF EXISTS meta_pvp_meta;
            DROP TABLE IF EXISTS meta_pve_meta;
            DROP TABLE IF EXISTS meta_builds;
            DROP TABLE IF EXISTS meta_unit_data;
            DROP TABLE IF EXISTS hero_progression_equipment_stats;
            DROP TABLE IF EXISTS hero_progression_relationships;
            DROP TABLE IF EXISTS hero_progression_tags;
            DROP TABLE IF EXISTS hero_progression_tracks;
            DROP TABLE IF EXISTS hero_progression_values;
            DROP TABLE IF EXISTS hero_progression_rows;
            DROP TABLE IF EXISTS hero_release_history;
            DROP TABLE IF EXISTS system_reference_values;
            DROP TABLE IF EXISTS game_system_references;
            DROP TABLE IF EXISTS source_notes;
            DROP TABLE IF EXISTS hero_variant_features;
            DROP TABLE IF EXISTS hero_variant_skills;
            DROP TABLE IF EXISTS hero_variant_sections;
            DROP TABLE IF EXISTS variant_meta_scores;
            DROP TABLE IF EXISTS skill_tags;
            DROP TABLE IF EXISTS skill_snippets;
            DROP TABLE IF EXISTS chaser_traits;
            DROP TABLE IF EXISTS hero_variants;
            DROP TABLE IF EXISTS hero_meta_scores;
            DROP TABLE IF EXISTS hero_modes;
            DROP TABLE IF EXISTS heroes;

            CREATE TABLE heroes (
                hero_id INTEGER PRIMARY KEY,
                name_en TEXT NOT NULL,
                name_ko TEXT,
                role TEXT NOT NULL,
                rarity TEXT NOT NULL,
                sources TEXT NOT NULL
            );

            CREATE TABLE hero_modes (
                hero_id INTEGER NOT NULL,
                mode TEXT NOT NULL,
                tier_letter TEXT NOT NULL,
                tier_numeric INTEGER NOT NULL,
                PRIMARY KEY (hero_id, mode),
                FOREIGN KEY (hero_id) REFERENCES heroes(hero_id)
            );

            CREATE TABLE hero_meta_scores (
                hero_id INTEGER PRIMARY KEY,
                base_score REAL NOT NULL,
                rarity_adjusted REAL NOT NULL,
                final_meta_score REAL NOT NULL,
                meta_rank INTEGER NOT NULL,
                FOREIGN KEY (hero_id) REFERENCES heroes(hero_id)
            );

            CREATE TABLE hero_variants (
                variant_id INTEGER PRIMARY KEY AUTOINCREMENT,
                hero_id INTEGER NOT NULL,
                variant_name_en TEXT NOT NULL,
                name_ko TEXT,
                variant_kind TEXT NOT NULL,
                variant_suffix TEXT,
                availability_marker TEXT,
                variant_role TEXT NOT NULL,
                variant_rarity TEXT NOT NULL,
                adventure_tier TEXT NOT NULL,
                battle_tier TEXT NOT NULL,
                boss_tier TEXT NOT NULL,
                source_title TEXT NOT NULL,
                source_href TEXT NOT NULL,
                note_excerpt TEXT,
                source TEXT NOT NULL,
                UNIQUE(hero_id, source_href),
                FOREIGN KEY (hero_id) REFERENCES heroes(hero_id)
            );

            CREATE TABLE variant_meta_scores (
                variant_id INTEGER PRIMARY KEY,
                hero_id INTEGER NOT NULL,
                base_score REAL NOT NULL,
                rarity_adjusted REAL NOT NULL,
                final_meta_score REAL NOT NULL,
                meta_rank INTEGER NOT NULL,
                score_basis TEXT NOT NULL,
                FOREIGN KEY (variant_id) REFERENCES hero_variants(variant_id),
                FOREIGN KEY (hero_id) REFERENCES heroes(hero_id)
            );

            CREATE TABLE hero_variant_skills (
                variant_skill_id INTEGER PRIMARY KEY AUTOINCREMENT,
                variant_id INTEGER NOT NULL,
                section_key TEXT NOT NULL,
                section_title TEXT NOT NULL,
                heading_id TEXT NOT NULL,
                skill_stage TEXT NOT NULL,
                skill_type TEXT NOT NULL,
                skill_name TEXT NOT NULL,
                description TEXT NOT NULL,
                source_page TEXT NOT NULL,
                UNIQUE(variant_id, heading_id),
                FOREIGN KEY (variant_id) REFERENCES hero_variants(variant_id)
            );

            CREATE TABLE hero_variant_sections (
                variant_section_id INTEGER PRIMARY KEY AUTOINCREMENT,
                variant_id INTEGER NOT NULL,
                heading_level INTEGER NOT NULL,
                heading_id TEXT NOT NULL,
                heading_title TEXT NOT NULL,
                section_path TEXT NOT NULL,
                content TEXT NOT NULL,
                source_page TEXT NOT NULL,
                UNIQUE(variant_id, heading_id, heading_title),
                FOREIGN KEY (variant_id) REFERENCES hero_variants(variant_id)
            );

            CREATE TABLE hero_variant_features (
                variant_id INTEGER NOT NULL,
                feature_key TEXT NOT NULL,
                feature_value TEXT NOT NULL,
                source_page TEXT NOT NULL,
                PRIMARY KEY (variant_id, feature_key),
                FOREIGN KEY (variant_id) REFERENCES hero_variants(variant_id)
            );

            CREATE TABLE hero_progression_rows (
                progression_key TEXT PRIMARY KEY,
                hero_id INTEGER NOT NULL,
                variant_id INTEGER NOT NULL,
                variant_title TEXT NOT NULL,
                variant_kind TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                source_name TEXT NOT NULL,
                skill_family TEXT NOT NULL,
                progression_stage_key TEXT NOT NULL,
                progression_stage_label TEXT NOT NULL,
                stage_order INTEGER NOT NULL,
                skill_stage TEXT NOT NULL,
                skill_type TEXT NOT NULL,
                progression_tracks_summary TEXT NOT NULL,
                modifiers_summary TEXT NOT NULL,
                mechanics_summary TEXT NOT NULL,
                stats_summary TEXT NOT NULL,
                top_coefficient TEXT NOT NULL,
                excerpt TEXT NOT NULL,
                source_page TEXT NOT NULL,
                FOREIGN KEY (hero_id) REFERENCES heroes(hero_id),
                FOREIGN KEY (variant_id) REFERENCES hero_variants(variant_id)
            );

            CREATE TABLE hero_progression_values (
                progression_value_id INTEGER PRIMARY KEY AUTOINCREMENT,
                progression_key TEXT NOT NULL,
                value_kind TEXT NOT NULL,
                category TEXT NOT NULL,
                value_text TEXT NOT NULL,
                numeric_value REAL,
                unit TEXT NOT NULL,
                context TEXT NOT NULL,
                FOREIGN KEY (progression_key) REFERENCES hero_progression_rows(progression_key)
            );

            CREATE TABLE hero_progression_tracks (
                progression_track_id INTEGER PRIMARY KEY AUTOINCREMENT,
                progression_key TEXT NOT NULL,
                track_label TEXT NOT NULL,
                step_index INTEGER NOT NULL,
                step_value TEXT NOT NULL,
                numeric_value REAL,
                unit TEXT NOT NULL,
                context TEXT NOT NULL,
                FOREIGN KEY (progression_key) REFERENCES hero_progression_rows(progression_key)
            );

            CREATE TABLE hero_progression_tags (
                progression_key TEXT NOT NULL,
                tag_type TEXT NOT NULL,
                tag TEXT NOT NULL,
                PRIMARY KEY (progression_key, tag_type, tag),
                FOREIGN KEY (progression_key) REFERENCES hero_progression_rows(progression_key)
            );

            CREATE TABLE hero_progression_relationships (
                relationship_id INTEGER PRIMARY KEY AUTOINCREMENT,
                progression_key TEXT NOT NULL,
                relation_type TEXT NOT NULL,
                relation_scope TEXT NOT NULL,
                target_source_name TEXT NOT NULL,
                target_skill_family TEXT NOT NULL,
                evidence_text TEXT NOT NULL,
                source_page TEXT NOT NULL,
                confidence_source TEXT NOT NULL,
                FOREIGN KEY (progression_key) REFERENCES hero_progression_rows(progression_key)
            );

            CREATE TABLE hero_progression_equipment_stats (
                equipment_stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
                progression_key TEXT NOT NULL,
                equipment_name TEXT NOT NULL,
                equipment_level INTEGER NOT NULL,
                physical_attack INTEGER NOT NULL,
                vitality INTEGER NOT NULL,
                physical_defense INTEGER NOT NULL,
                magic_defense INTEGER NOT NULL,
                FOREIGN KEY (progression_key) REFERENCES hero_progression_rows(progression_key)
            );

            CREATE TABLE chaser_traits (
                trait_id INTEGER PRIMARY KEY AUTOINCREMENT,
                trait_name TEXT NOT NULL,
                description TEXT NOT NULL,
                rank TEXT,
                source_page TEXT NOT NULL
            );

            CREATE TABLE skill_snippets (
                skill_id INTEGER PRIMARY KEY,
                skill_name TEXT NOT NULL,
                description TEXT NOT NULL,
                source_page TEXT NOT NULL
            );

            CREATE TABLE skill_tags (
                skill_id INTEGER NOT NULL,
                tag TEXT NOT NULL,
                confidence REAL NOT NULL,
                rationale TEXT,
                model_name TEXT NOT NULL,
                PRIMARY KEY (skill_id, tag),
                FOREIGN KEY (skill_id) REFERENCES skill_snippets(skill_id)
            );

            CREATE TABLE source_notes (
                note_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                note_key TEXT NOT NULL,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                source_page TEXT NOT NULL,
                UNIQUE(source, note_key)
            );

            CREATE TABLE game_system_references (
                reference_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                reference_key TEXT NOT NULL,
                title TEXT NOT NULL,
                section_path TEXT NOT NULL,
                content TEXT NOT NULL,
                source_page TEXT NOT NULL,
                game_era TEXT NOT NULL,
                is_legacy_system INTEGER NOT NULL,
                trust_tier TEXT NOT NULL,
                UNIQUE(source, reference_key, section_path)
            );

            CREATE TABLE hero_release_history (
                release_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                release_order_label TEXT NOT NULL,
                release_order_numeric INTEGER,
                release_year INTEGER,
                hero_name_raw TEXT NOT NULL,
                release_date_text TEXT NOT NULL,
                release_date_iso TEXT,
                release_batch_note TEXT NOT NULL,
                source_page TEXT NOT NULL,
                trust_tier TEXT NOT NULL
            );

            CREATE TABLE system_reference_values (
                reference_value_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                reference_key TEXT NOT NULL,
                title TEXT NOT NULL,
                row_label TEXT NOT NULL,
                column_label TEXT NOT NULL,
                value_text TEXT NOT NULL,
                numeric_value REAL,
                source_page TEXT NOT NULL,
                game_era TEXT NOT NULL,
                is_legacy_system INTEGER NOT NULL,
                trust_tier TEXT NOT NULL
            );

            -- ── Spreadsheet meta tables ──────────────────────────

            CREATE TABLE meta_unit_data (
                unit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                longname TEXT,
                shortname TEXT,
                keysname TEXT,
                attribute TEXT,
                color TEXT,
                unit_class TEXT,
                job_type TEXT,
                kr_release_date TEXT,
                released INTEGER,
                is_pve INTEGER,
                is_pvp INTEGER,
                is_support INTEGER,
                ht1 TEXT, hp1 TEXT, ht2 TEXT, hp2 TEXT, ht3 TEXT, hp3 TEXT,
                ht4 TEXT, hp4 TEXT, ht5 TEXT, hp5 TEXT,
                ct1 TEXT, cp1 TEXT, ct2 TEXT, cp2 TEXT, ct3 TEXT, cp3 TEXT,
                ct4 TEXT, cp4 TEXT, ct5 TEXT, cp5 TEXT,
                cs_level TEXT,
                rn1 TEXT, rn2 TEXT, artifact TEXT,
                ac1 TEXT, ac2 TEXT, ac3 TEXT, equip_set TEXT,
                tc1 TEXT, mt1 TEXT, tt1 TEXT, tt2 TEXT,
                tc2 TEXT, mt2 TEXT, tt3 TEXT, tt4 TEXT,
                si_build TEXT,
                ps TEXT, s1 TEXT, s2 TEXT, ss TEXT,
                cs1 TEXT, cs2 TEXT,
                si_ps TEXT, si_s1 TEXT, si_s2 TEXT, si_cs TEXT,
                descent TEXT
            );

            CREATE TABLE meta_builds (
                build_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                header_name TEXT,
                attribute TEXT,
                unit_class TEXT,
                content_tag TEXT,
                hero_trait_1 TEXT, hero_trait_2 TEXT, hero_trait_3 TEXT,
                hero_trait_4 TEXT, hero_trait_5 TEXT,
                chaser_trait_1 TEXT, chaser_trait_2 TEXT, chaser_trait_3 TEXT,
                chaser_trait_4 TEXT, chaser_trait_5 TEXT,
                cs_level TEXT,
                rune_normal TEXT, rune_special TEXT,
                acc_ring TEXT, acc_necklace TEXT, acc_earring TEXT,
                trans_main_mode TEXT, trans_main_t3 TEXT, trans_main_t6 TEXT
            );

            CREATE TABLE meta_pve_meta (
                pve_meta_id INTEGER PRIMARY KEY AUTOINCREMENT,
                meta_type TEXT NOT NULL,
                tier_group TEXT,
                tier_rank INTEGER,
                hero_name TEXT NOT NULL,
                attribute TEXT
            );

            CREATE TABLE meta_pvp_meta (
                pvp_meta_id INTEGER PRIMARY KEY AUTOINCREMENT,
                section TEXT NOT NULL,
                team_variant INTEGER,
                members TEXT NOT NULL,
                attributes TEXT,
                member_count INTEGER
            );

            CREATE TABLE meta_content_usage (
                usage_id INTEGER PRIMARY KEY AUTOINCREMENT,
                hero_name TEXT NOT NULL,
                content_mode TEXT NOT NULL,
                is_viable INTEGER NOT NULL
            );

            CREATE TABLE meta_content_teams (
                team_id INTEGER PRIMARY KEY AUTOINCREMENT,
                content TEXT NOT NULL,
                phase TEXT,
                team_type TEXT,
                members TEXT NOT NULL,
                attributes TEXT,
                member_count INTEGER,
                notes TEXT
            );

            CREATE TABLE meta_equipment_presets (
                preset_id INTEGER PRIMARY KEY AUTOINCREMENT,
                equipment_class TEXT,
                preset_name TEXT NOT NULL,
                set_color TEXT,
                stat_first_line TEXT,
                weapon_second_line TEXT,
                supp_weapon_second_line TEXT,
                armor_second_line TEXT,
                enchant_1 TEXT,
                enchant_2 TEXT,
                enchant_3 TEXT
            );

            CREATE TABLE meta_soul_imprint (
                si_id INTEGER PRIMARY KEY AUTOINCREMENT,
                hero_name TEXT NOT NULL,
                column_index INTEGER
            );

            CREATE TABLE meta_changelog (
                changelog_id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                entry TEXT NOT NULL
            );

            CREATE TABLE meta_release_order (
                release_order_id INTEGER PRIMARY KEY AUTOINCREMENT,
                release_type TEXT NOT NULL,
                batch TEXT,
                attribute TEXT,
                hero_name TEXT NOT NULL
            );

            CREATE TABLE meta_content_keys (
                key_id INTEGER PRIMARY KEY AUTOINCREMENT,
                content TEXT NOT NULL,
                team_key TEXT,
                team_members TEXT
            );

            CREATE TABLE meta_beginners_guide (
                guide_id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic TEXT,
                content TEXT NOT NULL
            );

            -- ── Indexes ──────────────────────────────────────────

            CREATE INDEX idx_meta_score_rank ON hero_meta_scores(final_meta_score DESC);
            CREATE INDEX idx_hero_variants_hero_id ON hero_variants(hero_id);
            CREATE INDEX idx_variant_meta_score_rank ON variant_meta_scores(final_meta_score DESC, meta_rank);
            CREATE INDEX idx_hero_variant_sections_variant_id ON hero_variant_sections(variant_id);
            CREATE INDEX idx_hero_variant_skills_variant_id ON hero_variant_skills(variant_id);
            CREATE INDEX idx_progression_rows_hero_stage ON hero_progression_rows(hero_id, stage_order, variant_id);
            CREATE INDEX idx_progression_values_kind ON hero_progression_values(value_kind, category, numeric_value);
            CREATE INDEX idx_progression_tracks_label ON hero_progression_tracks(track_label, step_index);
            CREATE INDEX idx_progression_relationships_target ON hero_progression_relationships(target_skill_family, relation_type);
            CREATE INDEX idx_progression_equipment_level ON hero_progression_equipment_stats(equipment_level);
            CREATE INDEX idx_game_system_references_key ON game_system_references(reference_key, game_era);
            CREATE INDEX idx_release_history_year_order ON hero_release_history(release_year, release_order_numeric);
            CREATE INDEX idx_system_reference_values_key ON system_reference_values(reference_key, row_label, column_label);
            CREATE INDEX idx_meta_unit_data_name ON meta_unit_data(name);
            CREATE INDEX idx_meta_unit_data_attr ON meta_unit_data(attribute);
            CREATE INDEX idx_meta_builds_name ON meta_builds(name);
            CREATE INDEX idx_meta_content_usage_hero ON meta_content_usage(hero_name);
            CREATE INDEX idx_meta_content_teams_content ON meta_content_teams(content);
            CREATE INDEX idx_meta_changelog_date ON meta_changelog(date);

            PRAGMA foreign_keys = ON;
            """
        )

        hero_rows = [
            (
                int(raw_row["hero_id"]),
                str(raw_row["name_en"]),
                str(raw_row["name_ko"]),
                str(raw_row["role"]),
                str(raw_row["rarity"]),
                str(raw_row["sources"]),
            )
            for raw_row in heroes_df.to_dict(orient="records")
        ]
        cursor.executemany(
            "INSERT INTO heroes (hero_id, name_en, name_ko, role, rarity, sources) VALUES (?, ?, ?, ?, ?, ?)",
            hero_rows,
        )

        cursor.executemany(
            "INSERT INTO hero_modes (hero_id, mode, tier_letter, tier_numeric) VALUES (?, ?, ?, ?)",
            _build_mode_rows(heroes_df, settings),
        )

        score_rows = [
            (
                int(raw_row["hero_id"]),
                float(raw_row["base_score"]),
                float(raw_row["rarity_adjusted"]),
                float(raw_row["final_meta_score"]),
                int(raw_row["meta_rank"]),
            )
            for raw_row in scores_df.to_dict(orient="records")
        ]
        cursor.executemany(
            "INSERT INTO hero_meta_scores (hero_id, base_score, rarity_adjusted, final_meta_score, meta_rank) VALUES (?, ?, ?, ?, ?)",
            score_rows,
        )

        sheets = ingest_spreadsheet()

        variant_profiles_df = build_variant_profiles(
            heroes_df,
            variants_df,
            variant_sections_df,
            settings=settings,
            spreadsheet_sheets=sheets,
        )
        hero_id_by_name = {
            str(raw_row["name_en"]): int(raw_row["hero_id"])
            for raw_row in heroes_df.to_dict(orient="records")
        }
        variant_rows = [
            (
                int(raw_row["hero_id"]),
                str(raw_row.get("variant_name_en", raw_row["name_en"])),
                str(raw_row.get("name_ko", "")),
                str(raw_row.get("variant_kind", "base")),
                str(raw_row.get("variant_suffix", "")),
                str(raw_row.get("availability_marker", "")),
                str(raw_row.get("variant_role", "Unknown")),
                str(raw_row.get("variant_rarity", "SS")),
                str(raw_row.get("adventure_tier", "B")),
                str(raw_row.get("battle_tier", "B")),
                str(raw_row.get("boss_tier", "B")),
                str(raw_row.get("variant_title", raw_row["name_en"])),
                str(raw_row.get("variant_href", "")),
                str(raw_row.get("note_excerpt", "")),
                str(raw_row.get("source", "namuwiki")),
            )
            for raw_row in variant_profiles_df.to_dict(orient="records")
            if str(raw_row.get("variant_href", "")).strip()
        ]
        if variant_rows:
            cursor.executemany(
                "INSERT INTO hero_variants (hero_id, variant_name_en, name_ko, variant_kind, variant_suffix, availability_marker, variant_role, variant_rarity, adventure_tier, battle_tier, boss_tier, source_title, source_href, note_excerpt, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                variant_rows,
            )

        variant_id_by_href = {
            str(source_href): int(variant_id)
            for variant_id, source_href in cursor.execute(
                "SELECT variant_id, source_href FROM hero_variants"
            )
        }

        variant_score_profiles_df = variant_profiles_df.copy()
        if not variant_score_profiles_df.empty:
            variant_score_profiles_df["variant_id"] = variant_score_profiles_df[
                "variant_href"
            ].map(variant_id_by_href)
            variant_score_profiles_df = variant_score_profiles_df[
                variant_score_profiles_df["variant_id"].notna()
            ].copy()
            variant_score_profiles_df["variant_id"] = variant_score_profiles_df[
                "variant_id"
            ].astype(int)

        variant_score_rows = [
            (
                int(raw_row["variant_id"]),
                int(raw_row["hero_id"]),
                float(raw_row["base_score"]),
                float(raw_row["rarity_adjusted"]),
                float(raw_row["final_meta_score"]),
                int(raw_row["meta_rank"]),
                str(raw_row.get("score_basis", "inherited_hero_modes")),
            )
            for raw_row in compute_variant_meta_scores(
                variant_score_profiles_df,
                settings,
            ).to_dict(orient="records")
        ]
        if variant_score_rows:
            cursor.executemany(
                "INSERT INTO variant_meta_scores (variant_id, hero_id, base_score, rarity_adjusted, final_meta_score, meta_rank, score_basis) VALUES (?, ?, ?, ?, ?, ?, ?)",
                variant_score_rows,
            )

        if not variant_sections_df.empty:
            variant_section_rows = [
                (
                    variant_id_by_href[str(raw_row["variant_href"])],
                    int(raw_row["heading_level"]),
                    str(raw_row["heading_id"]),
                    str(raw_row["heading_title"]),
                    str(raw_row["section_path"]),
                    str(raw_row["content"]),
                    str(raw_row["source_page"]),
                )
                for raw_row in variant_sections_df.to_dict(orient="records")
                if str(raw_row.get("variant_href", "")) in variant_id_by_href
            ]
            if variant_section_rows:
                cursor.executemany(
                    "INSERT INTO hero_variant_sections (variant_id, heading_level, heading_id, heading_title, section_path, content, source_page) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    variant_section_rows,
                )

        if not variant_skills_df.empty:
            variant_skill_rows = [
                (
                    variant_id_by_href[str(raw_row["variant_href"])],
                    str(raw_row["section_key"]),
                    str(raw_row["section_title"]),
                    str(raw_row["heading_id"]),
                    str(raw_row["skill_stage"]),
                    str(raw_row["skill_type"]),
                    str(raw_row["skill_name"]),
                    str(raw_row["description"]),
                    str(raw_row["source_page"]),
                )
                for raw_row in variant_skills_df.to_dict(orient="records")
                if str(raw_row.get("variant_href", "")) in variant_id_by_href
            ]
            if variant_skill_rows:
                cursor.executemany(
                    "INSERT INTO hero_variant_skills (variant_id, section_key, section_title, heading_id, skill_stage, skill_type, skill_name, description, source_page) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    variant_skill_rows,
                )

        if not variant_features_df.empty:
            variant_feature_rows = [
                (
                    variant_id_by_href[str(raw_row["variant_href"])],
                    str(raw_row["feature_key"]),
                    str(raw_row["feature_value"]),
                    str(raw_row["source_page"]),
                )
                for raw_row in variant_features_df.to_dict(orient="records")
                if str(raw_row.get("variant_href", "")) in variant_id_by_href
            ]
            if variant_feature_rows:
                cursor.executemany(
                    "INSERT INTO hero_variant_features (variant_id, feature_key, feature_value, source_page) VALUES (?, ?, ?, ?)",
                    variant_feature_rows,
                )

        progression_records = build_progression_records(
            variants_df,
            variant_sections_df,
            variant_skills_df,
            variant_features_df,
            hero_id_by_name,
            variant_id_by_href,
        )
        if progression_records["rows"]:
            cursor.executemany(
                "INSERT INTO hero_progression_rows (progression_key, hero_id, variant_id, variant_title, variant_kind, source_kind, source_name, skill_family, progression_stage_key, progression_stage_label, stage_order, skill_stage, skill_type, progression_tracks_summary, modifiers_summary, mechanics_summary, stats_summary, top_coefficient, excerpt, source_page) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                progression_records["rows"],
            )
        if progression_records["values"]:
            cursor.executemany(
                "INSERT INTO hero_progression_values (progression_key, value_kind, category, value_text, numeric_value, unit, context) VALUES (?, ?, ?, ?, ?, ?, ?)",
                progression_records["values"],
            )
        if progression_records["tracks"]:
            cursor.executemany(
                "INSERT INTO hero_progression_tracks (progression_key, track_label, step_index, step_value, numeric_value, unit, context) VALUES (?, ?, ?, ?, ?, ?, ?)",
                progression_records["tracks"],
            )
        if progression_records["tags"]:
            cursor.executemany(
                "INSERT INTO hero_progression_tags (progression_key, tag_type, tag) VALUES (?, ?, ?)",
                progression_records["tags"],
            )
        if progression_records["relationships"]:
            cursor.executemany(
                "INSERT INTO hero_progression_relationships (progression_key, relation_type, relation_scope, target_source_name, target_skill_family, evidence_text, source_page, confidence_source) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                progression_records["relationships"],
            )
        if progression_records["equipment"]:
            cursor.executemany(
                "INSERT INTO hero_progression_equipment_stats (progression_key, equipment_name, equipment_level, physical_attack, vitality, physical_defense, magic_defense) VALUES (?, ?, ?, ?, ?, ?, ?)",
                progression_records["equipment"],
            )

        if not notes_df.empty:
            note_rows = [
                (
                    str(raw_row["source"]),
                    str(raw_row["note_key"]),
                    str(raw_row["title"]),
                    str(raw_row["content"]),
                    str(raw_row["source_page"]),
                )
                for raw_row in notes_df.to_dict(orient="records")
            ]
            cursor.executemany(
                "INSERT INTO source_notes (source, note_key, title, content, source_page) VALUES (?, ?, ?, ?, ?)",
                note_rows,
            )

        if not system_references_df.empty:
            system_reference_rows = [
                (
                    str(raw_row["source"]),
                    str(raw_row["reference_key"]),
                    str(raw_row["title"]),
                    str(raw_row["section_path"]),
                    str(raw_row["content"]),
                    str(raw_row["source_page"]),
                    str(raw_row.get("game_era", "current_reference")),
                    (
                        1
                        if str(raw_row.get("is_legacy_system", "0")).strip()
                        in {"1", "true", "True"}
                        else 0
                    ),
                    str(raw_row.get("trust_tier", "community_wiki")),
                )
                for raw_row in system_references_df.to_dict(orient="records")
            ]
            cursor.executemany(
                "INSERT INTO game_system_references (source, reference_key, title, section_path, content, source_page, game_era, is_legacy_system, trust_tier) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                system_reference_rows,
            )

        if not system_reference_values_df.empty:
            system_reference_value_rows = [
                (
                    str(raw_row["source"]),
                    str(raw_row["reference_key"]),
                    str(raw_row["title"]),
                    str(raw_row.get("row_label", "")),
                    str(raw_row.get("column_label", "")),
                    str(raw_row.get("value_text", "")),
                    _extract_numeric_value(raw_row.get("value_text", "")),
                    str(raw_row.get("source_page", "")),
                    str(raw_row.get("game_era", "legacy_pre_2024")),
                    (
                        1
                        if str(raw_row.get("is_legacy_system", "1")).strip()
                        in {"1", "true", "True"}
                        else 0
                    ),
                    str(raw_row.get("trust_tier", "community_wiki")),
                )
                for raw_row in system_reference_values_df.to_dict(orient="records")
                if str(raw_row.get("value_text", "")).strip()
            ]
            cursor.executemany(
                "INSERT INTO system_reference_values (source, reference_key, title, row_label, column_label, value_text, numeric_value, source_page, game_era, is_legacy_system, trust_tier) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                system_reference_value_rows,
            )

        if not release_history_df.empty:
            release_history_rows = [
                (
                    str(raw_row["source"]),
                    str(raw_row.get("release_order_label", "")),
                    _as_optional_int(raw_row.get("release_order_numeric", "")),
                    _as_optional_int(raw_row.get("release_year", "")),
                    str(raw_row.get("hero_name_raw", "")),
                    str(raw_row.get("release_date_text", "")),
                    str(raw_row.get("release_date_iso", "")),
                    str(raw_row.get("release_batch_note", "")),
                    str(raw_row.get("source_page", "")),
                    str(raw_row.get("trust_tier", "community_wiki")),
                )
                for raw_row in release_history_df.to_dict(orient="records")
                if str(raw_row.get("hero_name_raw", "")).strip()
            ]
            cursor.executemany(
                "INSERT INTO hero_release_history (source, release_order_label, release_order_numeric, release_year, hero_name_raw, release_date_text, release_date_iso, release_batch_note, source_page, trust_tier) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                release_history_rows,
            )

        if not chaser_df.empty:
            trait_rows = [
                (
                    str(raw_row["trait_name"]),
                    str(raw_row["description"]),
                    str(raw_row["rank"]),
                    str(raw_row["source_page"]),
                )
                for raw_row in chaser_df.to_dict(orient="records")
            ]
            cursor.executemany(
                "INSERT INTO chaser_traits (trait_name, description, rank, source_page) VALUES (?, ?, ?, ?)",
                trait_rows,
            )

        if not skills_df.empty:
            skill_rows = [
                (
                    index + 1,
                    str(raw_row["skill_name"]),
                    str(raw_row["description"]),
                    str(raw_row["source_page"]),
                )
                for index, raw_row in enumerate(skills_df.to_dict(orient="records"))
            ]
            cursor.executemany(
                "INSERT INTO skill_snippets (skill_id, skill_name, description, source_page) VALUES (?, ?, ?, ?)",
                skill_rows,
            )
        _populate_spreadsheet_tables(cursor, sheets)

        connection.commit()
    LOGGER.info("Built SQLite database at %s", settings.database_path)


def run(settings: RuntimeSettings) -> dict[str, int]:
    strategy_df = read_csv(
        RAW_STRATEGYWIKI,
        ["name_en", "role", "adventure", "battle", "boss", "source"],
    )
    strategywiki_reference_notes_df = read_csv(
        RAW_STRATEGYWIKI_REFERENCE_NOTES,
        [
            "source",
            "reference_key",
            "title",
            "section_path",
            "content",
            "source_page",
            "game_era",
            "is_legacy_system",
            "trust_tier",
        ],
    )
    strategywiki_hero_growth_values_df = read_csv(
        RAW_STRATEGYWIKI_HERO_GROWTH_VALUES,
        [
            "source",
            "reference_key",
            "title",
            "row_label",
            "column_label",
            "value_text",
            "source_page",
            "game_era",
            "is_legacy_system",
            "trust_tier",
        ],
    )
    namuwiki_df = read_csv(
        RAW_NAMUWIKI,
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
    namuwiki_notes_df = read_csv(
        RAW_NAMUWIKI_NOTES,
        ["source", "note_key", "title", "content", "source_page"],
    )
    namuwiki_system_references_df = read_csv(
        RAW_NAMUWIKI_SYSTEM_REFERENCES,
        [
            "source",
            "reference_key",
            "title",
            "section_path",
            "content",
            "source_page",
            "game_era",
            "is_legacy_system",
            "trust_tier",
        ],
    )
    namuwiki_release_history_df = read_csv(
        RAW_NAMUWIKI_RELEASE_HISTORY,
        [
            "source",
            "release_order_label",
            "release_order_numeric",
            "release_year",
            "hero_name_raw",
            "release_date_text",
            "release_date_iso",
            "release_batch_note",
            "source_page",
            "trust_tier",
        ],
    )
    variant_sections_df = read_csv(
        RAW_NAMUWIKI_VARIANT_SECTIONS,
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
    variant_skills_df = read_csv(
        RAW_NAMUWIKI_VARIANT_SKILLS,
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
    variant_features_df = read_csv(
        RAW_NAMUWIKI_VARIANT_FEATURES,
        ["variant_href", "feature_key", "feature_value", "source_page"],
    )
    chaser_df = read_csv(
        RAW_CHASER, ["trait_name", "description", "rank", "source_page"]
    )
    skills_df = read_csv(RAW_SKILLS, ["skill_name", "description", "source_page"])

    combined_system_references_df = pd.concat(
        [namuwiki_system_references_df, strategywiki_reference_notes_df],
        ignore_index=True,
    ).fillna("")

    heroes_df = resolve_hero_identities(strategy_df, namuwiki_df)
    scores_df = compute_meta_scores(heroes_df, settings)
    build_database(
        heroes_df,
        scores_df,
        namuwiki_df,
        namuwiki_notes_df,
        combined_system_references_df,
        strategywiki_hero_growth_values_df,
        namuwiki_release_history_df,
        variant_sections_df,
        variant_skills_df,
        variant_features_df,
        chaser_df,
        skills_df,
        settings,
    )

    leaderboard_df = heroes_df.merge(scores_df, on="hero_id").sort_values(
        "final_meta_score", ascending=False
    )
    leaderboard_path = PROCESSED_DATA_DIR / "hero_leaderboard.csv"
    leaderboard_df.to_csv(leaderboard_path, index=False)
    LOGGER.info("Wrote leaderboard to %s", leaderboard_path)

    with sqlite3.connect(settings.database_path) as connection:
        variant_leaderboard_df = pd.read_sql_query(
            """
            SELECT
                hv.variant_id,
                hv.hero_id,
                h.name_en,
                COALESCE(hv.name_ko, h.name_ko) AS name_ko,
                hv.variant_name_en,
                hv.variant_kind,
                hv.variant_suffix,
                hv.availability_marker,
                hv.variant_role,
                hv.variant_rarity,
                hv.source_title AS variant_title,
                hv.note_excerpt,
                hv.adventure_tier,
                hv.battle_tier,
                hv.boss_tier,
                vms.base_score,
                vms.rarity_adjusted,
                vms.final_meta_score,
                vms.meta_rank,
                vms.score_basis
            FROM hero_variants hv
            JOIN heroes h ON h.hero_id = hv.hero_id
            LEFT JOIN variant_meta_scores vms ON vms.variant_id = hv.variant_id
            ORDER BY vms.meta_rank, h.name_en, hv.variant_kind, hv.variant_name_en
            """,
            connection,
        ).fillna("")
        variant_leaderboard_path = PROCESSED_DATA_DIR / "variant_leaderboard.csv"
        variant_leaderboard_df.to_csv(variant_leaderboard_path, index=False)
        LOGGER.info("Wrote variant leaderboard to %s", variant_leaderboard_path)

        progression_counts = {
            table_name: int(
                connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            )
            for table_name in (
                "hero_progression_rows",
                "hero_progression_values",
                "hero_progression_tracks",
                "hero_progression_tags",
                "hero_progression_relationships",
                "hero_progression_equipment_stats",
            )
        }

        spreadsheet_counts = {
            table_name: int(
                connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            )
            for table_name in (
                "meta_unit_data",
                "meta_builds",
                "meta_pve_meta",
                "meta_pvp_meta",
                "meta_content_usage",
                "meta_content_teams",
                "meta_equipment_presets",
                "meta_soul_imprint",
                "meta_changelog",
                "meta_release_order",
                "meta_content_keys",
                "meta_beginners_guide",
            )
        }

    return {
        "heroes": int(len(heroes_df.index)),
        "variants": int(len(namuwiki_df.index)),
        "ranked_variants": int(len(variant_leaderboard_df.index)),
        "notes": int(len(namuwiki_notes_df.index)),
        "system_references": int(len(combined_system_references_df.index)),
        "system_reference_values": int(len(strategywiki_hero_growth_values_df.index)),
        "release_history": int(len(namuwiki_release_history_df.index)),
        "variant_sections": int(len(variant_sections_df.index)),
        "variant_skills": int(len(variant_skills_df.index)),
        "variant_features": int(len(variant_features_df.index)),
        "progression_rows": progression_counts["hero_progression_rows"],
        "progression_values": progression_counts["hero_progression_values"],
        "progression_tracks": progression_counts["hero_progression_tracks"],
        "progression_tags": progression_counts["hero_progression_tags"],
        "progression_relationships": progression_counts[
            "hero_progression_relationships"
        ],
        "equipment_stats": progression_counts["hero_progression_equipment_stats"],
        "traits": int(len(chaser_df.index)),
        "skills": int(len(skills_df.index)),
        **spreadsheet_counts,
    }
