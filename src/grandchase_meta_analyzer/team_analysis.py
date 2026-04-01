from __future__ import annotations

import re

import pandas as pd

from .explorer_skill_details import extract_skill_insight


TEAM_SP_CATEGORY_LABELS = {
    "sp_gain": "Flat SP Gains",
    "sp_gain_rate": "SP Per Second",
    "sp_cost_discount": "SP Cost Discounts",
    "sp_free_cast": "Free Cast Clauses",
}
TEAM_SP_CATEGORY_ORDER = {
    "Flat SP Gains": 0,
    "SP Per Second": 1,
    "SP Cost Discounts": 2,
    "Free Cast Clauses": 3,
}
TEAM_DEFENSE_CATEGORY_ORDER = {
    "Shield": 0,
    "Damage Reduction": 1,
    "Invincibility": 2,
    "Healing": 3,
    "Cleanse": 4,
    "Resurrection": 5,
    "Defense Stats": 6,
}
DEFENSE_CATEGORY_BY_MECHANIC = {
    "shield": "Shield",
    "damage reduction": "Damage Reduction",
    "invincibility": "Invincibility",
    "healing": "Healing",
    "cleanse": "Cleanse",
    "resurrection": "Resurrection",
}
DEFENSE_STAT_NAMES = {"life", "health", "physical defense", "magic defense"}
FEATURE_LABELS = {
    "characteristics": "Traits / Characteristics",
    "chaser": "Chaser System",
    "soul_imprint": "Soul Imprint System",
    "transcendental_awakening": "Transcendence System",
}
PROGRESSION_STAGE_METADATA = {
    "feature": ("System Availability", 0),
    "base_skill": ("Base Kit / Level 1+", 1),
    "gear": ("Dedicated Equipment", 2),
    "pet": ("Pet Support", 3),
    "base_chaser": ("Chaser Progression", 4),
    "enhancement_i": ("Transcendence / Enhancement I", 5),
    "enhancement_ii": ("Transcendence / Enhancement II", 6),
    "imprint": ("Soul Imprint Progression", 7),
    "advent": ("Advent Growth", 8),
    "other": ("Other Systems", 9),
}


def preview_value(text: str, length: int = 220) -> str:
    return re.sub(r"\s+", " ", str(text)).strip()[:length]


def join_or_dash(items: list[str], limit: int = 6) -> str:
    seen: set[str] = set()
    unique_items: list[str] = []
    for item in items:
        normalized = str(item).strip()
        if not normalized or normalized == "-":
            continue
        lowered = normalized.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        unique_items.append(normalized)
        if len(unique_items) >= limit:
            break
    return ", ".join(unique_items) if unique_items else "-"


def _to_float(value: object) -> float | None:
    text = str(value).strip()
    if not text or text == "-":
        return None
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)", text)
    if match is None:
        return None
    return float(match.group(1).replace(",", ""))


def _max_text_by_numeric(values: list[str]) -> tuple[str, float | None]:
    best_text = "-"
    best_numeric: float | None = None
    for value in values:
        numeric_value = _to_float(value)
        if numeric_value is None:
            continue
        if best_numeric is None or numeric_value > best_numeric:
            best_numeric = numeric_value
            best_text = value
    return best_text, best_numeric


def _format_progression_tracks(tracks: list[object]) -> str:
    if not tracks:
        return "-"
    return "; ".join(f"{track.label}: {' / '.join(track.values)}" for track in tracks)


def _max_numeric_token(values: list[str]) -> str:
    best_text, _ = _max_text_by_numeric([str(value) for value in values])
    return best_text


def _skill_stage_key(skill_stage: str, skill_type: str) -> str:
    if skill_stage == "enhancement_i":
        return "enhancement_i"
    if skill_stage == "enhancement_ii":
        return "enhancement_ii"
    if skill_stage == "imprint":
        return "imprint"
    if skill_type == "chaser":
        return "base_chaser"
    return "base_skill"


def _section_stage_key(heading_title: str) -> str | None:
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


def _stage_label_and_order(stage_key: str) -> tuple[str, int]:
    return PROGRESSION_STAGE_METADATA.get(
        stage_key, PROGRESSION_STAGE_METADATA["other"]
    )


def _base_source_row(
    row: pd.Series,
    *,
    source_kind: str,
    source_name: str,
    stage_label: str,
    stage_order: int,
    effect_text: str,
) -> dict[str, object]:
    insight = extract_skill_insight(effect_text)
    return {
        "variant_id": int(row["variant_id"]),
        "name_en": str(row["name_en"]),
        "variant_label": str(row["variant_label"]),
        "variant_kind_label": str(row["variant_kind_label"]),
        "source_kind": source_kind,
        "source_name": source_name,
        "skill_type": str(row.get("skill_type", "-")).title() or "-",
        "stage_label": stage_label,
        "stage_order": stage_order,
        "sp_cost": _to_float(insight.sp_cost),
        "cooldown_seconds": _to_float(insight.cooldown_seconds),
        "progression_tracks": _format_progression_tracks(
            list(insight.progression_tracks)
        ),
        "mechanic_tags": list(insight.mechanic_tags),
        "stat_tags": list(insight.stat_tags),
        "numeric_mentions": list(insight.numeric_mentions),
        "stat_bonuses": list(insight.stat_bonuses),
        "durations": list(insight.durations),
        "target_mentions": list(insight.target_mentions),
        "economy_mentions": list(insight.economy_mentions),
        "current_effect": insight.body_text,
        "source_preview": preview_value(insight.body_text),
        "top_coefficient": _max_numeric_token(list(insight.coefficients)),
    }


def build_team_source_frame(
    skills_df: pd.DataFrame,
    sections_df: pd.DataFrame,
    features_df: pd.DataFrame,
    selected_variant_ids: list[int],
) -> pd.DataFrame:
    selected_set = {int(variant_id) for variant_id in selected_variant_ids}
    rows: list[dict[str, object]] = []

    if not skills_df.empty:
        skill_columns = [
            "name_en",
            "variant_id",
            "variant_label",
            "variant_kind_label",
            "skill_stage",
            "skill_type",
            "skill_name",
            "description",
        ]
        selected_skills = skills_df[skills_df["variant_id"].isin(selected_set)][
            skill_columns
        ]
        for _, row in selected_skills.drop_duplicates().iterrows():
            stage_key = _skill_stage_key(
                str(row["skill_stage"]), str(row["skill_type"])
            )
            stage_label, stage_order = _stage_label_and_order(stage_key)
            rows.append(
                _base_source_row(
                    row,
                    source_kind="skill",
                    source_name=str(row["skill_name"]),
                    stage_label=stage_label,
                    stage_order=stage_order,
                    effect_text=str(row["description"]),
                )
            )

    if not sections_df.empty:
        section_columns = [
            "name_en",
            "variant_id",
            "variant_label",
            "variant_kind_label",
            "heading_title",
            "content",
        ]
        selected_sections = sections_df[sections_df["variant_id"].isin(selected_set)][
            section_columns
        ]
        for _, row in selected_sections.drop_duplicates().iterrows():
            stage_key = _section_stage_key(str(row["heading_title"]))
            if stage_key is None:
                continue
            stage_label, stage_order = _stage_label_and_order(stage_key)
            rows.append(
                _base_source_row(
                    row,
                    source_kind="section",
                    source_name=str(row["heading_title"]),
                    stage_label=stage_label,
                    stage_order=stage_order,
                    effect_text=str(row["content"]),
                )
            )

    if not features_df.empty:
        feature_columns = [
            "name_en",
            "variant_id",
            "variant_label",
            "variant_kind_label",
            "feature_key",
            "feature_value",
        ]
        selected_features = features_df[features_df["variant_id"].isin(selected_set)][
            feature_columns
        ]
        for _, row in selected_features.drop_duplicates().iterrows():
            stage_label, stage_order = _stage_label_and_order("feature")
            feature_key = str(row["feature_key"])
            rows.append(
                _base_source_row(
                    row,
                    source_kind="feature",
                    source_name=FEATURE_LABELS.get(
                        feature_key, feature_key.replace("_", " ").title()
                    ),
                    stage_label=stage_label,
                    stage_order=stage_order,
                    effect_text=str(row["feature_value"]),
                )
            )

    if not rows:
        return pd.DataFrame(
            columns=[
                "variant_id",
                "name_en",
                "variant_label",
                "variant_kind_label",
                "source_kind",
                "source_name",
                "skill_type",
                "stage_label",
                "stage_order",
                "sp_cost",
                "cooldown_seconds",
                "progression_tracks",
                "mechanic_tags",
                "stat_tags",
                "numeric_mentions",
                "stat_bonuses",
                "durations",
                "target_mentions",
                "economy_mentions",
                "current_effect",
                "source_preview",
                "top_coefficient",
            ]
        )
    return (
        pd.DataFrame.from_records(rows)
        .sort_values(["stage_order", "variant_label", "source_kind", "source_name"])
        .reset_index(drop=True)
    )


def build_default_team_variant_ids(
    variant_leaderboard_df: pd.DataFrame,
    *,
    size: int = 4,
) -> list[int]:
    if variant_leaderboard_df.empty:
        return []

    ranked_df = variant_leaderboard_df.sort_values("meta_rank").drop_duplicates(
        subset=["variant_id"]
    )
    selected_ids: list[int] = []
    used_roles: set[str] = set()
    used_heroes: set[str] = set()

    for _, row in ranked_df.iterrows():
        role = str(row.get("role", "")).strip()
        hero_name = str(row.get("name_en", ""))
        if not role or role in used_roles:
            continue
        if hero_name in used_heroes:
            continue
        selected_ids.append(int(row["variant_id"]))
        used_roles.add(role)
        used_heroes.add(hero_name)
        if len(selected_ids) >= size:
            return selected_ids[:size]

    for _, row in ranked_df.iterrows():
        hero_name = str(row.get("name_en", ""))
        variant_id = int(row["variant_id"])
        if variant_id in selected_ids or hero_name in used_heroes:
            continue
        selected_ids.append(variant_id)
        used_heroes.add(hero_name)
        if len(selected_ids) >= size:
            break

    return selected_ids[:size]


def build_team_sp_evidence_frame(team_sources_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for source_row in team_sources_df.to_dict(orient="records"):
        for mention in source_row.get("economy_mentions", []):
            label = TEAM_SP_CATEGORY_LABELS.get(str(mention.category))
            if label is None:
                continue
            rows.append(
                {
                    "variant_id": int(source_row["variant_id"]),
                    "variant_label": str(source_row["variant_label"]),
                    "stage_label": str(source_row["stage_label"]),
                    "source_kind": str(source_row["source_kind"]),
                    "source_name": str(source_row["source_name"]),
                    "economy_type": label,
                    "value_text": str(mention.value_text),
                    "numeric_value": mention.numeric_value,
                    "unit": str(mention.unit),
                    "sp_cost": source_row.get("sp_cost"),
                    "cooldown_seconds": source_row.get("cooldown_seconds"),
                    "context": preview_value(str(mention.context)),
                }
            )
    if not rows:
        return pd.DataFrame(
            columns=[
                "variant_id",
                "variant_label",
                "stage_label",
                "source_kind",
                "source_name",
                "economy_type",
                "value_text",
                "numeric_value",
                "unit",
                "sp_cost",
                "cooldown_seconds",
                "context",
            ]
        )
    frame = pd.DataFrame.from_records(rows)
    frame["sort_order"] = frame["economy_type"].map(TEAM_SP_CATEGORY_ORDER)
    return (
        frame.sort_values(["sort_order", "variant_label", "stage_label", "source_name"])
        .drop(columns=["sort_order"])
        .reset_index(drop=True)
    )


def build_team_sp_summary(team_sources_df: pd.DataFrame) -> pd.DataFrame:
    evidence_df = build_team_sp_evidence_frame(team_sources_df)
    if evidence_df.empty:
        return pd.DataFrame(
            columns=[
                "economy_type",
                "providers",
                "clauses",
                "captured_total",
                "best_value",
                "examples",
            ]
        )

    rows: list[dict[str, object]] = []
    for economy_type, group in evidence_df.groupby("economy_type", dropna=False):
        numeric_values = group["numeric_value"].dropna()
        captured_total = "-"
        best_value = "-"
        if economy_type == "Flat SP Gains" and not numeric_values.empty:
            captured_total = f"{numeric_values.sum():.2f} SP"
            best_value = group.sort_values("numeric_value", ascending=False).iloc[0][
                "value_text"
            ]
        elif economy_type == "SP Per Second" and not numeric_values.empty:
            captured_total = f"{numeric_values.sum():.2f} SP/s"
            best_value = group.sort_values("numeric_value", ascending=False).iloc[0][
                "value_text"
            ]
        elif economy_type == "SP Cost Discounts" and not numeric_values.empty:
            best_value = group.sort_values("numeric_value", ascending=False).iloc[0][
                "value_text"
            ]
        rows.append(
            {
                "economy_type": economy_type,
                "providers": int(group["variant_id"].nunique()),
                "clauses": int(len(group.index)),
                "captured_total": captured_total,
                "best_value": best_value,
                "examples": join_or_dash(group["context"].tolist(), limit=2),
            }
        )

    frame = pd.DataFrame.from_records(rows)
    frame["sort_order"] = frame["economy_type"].map(TEAM_SP_CATEGORY_ORDER)
    return (
        frame.sort_values("sort_order")
        .drop(columns=["sort_order"])
        .reset_index(drop=True)
    )


def build_team_skill_cost_frame(team_sources_df: pd.DataFrame) -> pd.DataFrame:
    if team_sources_df.empty:
        return pd.DataFrame(
            columns=[
                "variant_label",
                "stage_label",
                "skill_type",
                "source_name",
                "sp_cost",
                "cooldown_seconds",
                "source_preview",
            ]
        )

    frame = team_sources_df[
        (team_sources_df["source_kind"] == "skill") & team_sources_df["sp_cost"].notna()
    ][
        [
            "variant_label",
            "stage_label",
            "skill_type",
            "source_name",
            "sp_cost",
            "cooldown_seconds",
            "source_preview",
        ]
    ].copy()
    return frame.sort_values(
        ["sp_cost", "cooldown_seconds", "variant_label", "source_name"],
        ascending=[False, False, True, True],
    ).reset_index(drop=True)


def _best_numeric_value(
    numeric_mentions: list[object],
    category: str,
) -> tuple[str, float | None]:
    relevant_values = [
        str(mention.value)
        for mention in numeric_mentions
        if str(getattr(mention, "category", "")) == category
    ]
    return _max_text_by_numeric(relevant_values)


def _best_duration_value(durations: list[str]) -> tuple[str, float | None]:
    return _max_text_by_numeric([str(duration) for duration in durations])


def _best_defense_stat_value(stat_bonuses: list[object]) -> tuple[str, float | None]:
    relevant = [
        f"{stat_bonus.stat} +{stat_bonus.value}"
        for stat_bonus in stat_bonuses
        if str(getattr(stat_bonus, "stat", "")).strip().lower() in DEFENSE_STAT_NAMES
    ]
    return _max_text_by_numeric(relevant)


def build_team_defense_evidence_frame(team_sources_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for source_row in team_sources_df.to_dict(orient="records"):
        mechanic_tags = set(source_row.get("mechanic_tags", []))
        numeric_mentions = list(source_row.get("numeric_mentions", []))
        stat_bonuses = list(source_row.get("stat_bonuses", []))
        durations = list(source_row.get("durations", []))

        for mechanic, defense_type in DEFENSE_CATEGORY_BY_MECHANIC.items():
            if mechanic not in mechanic_tags:
                continue
            if defense_type == "Shield":
                strongest_value, sort_numeric = _best_numeric_value(
                    numeric_mentions, "shield"
                )
            elif defense_type == "Damage Reduction":
                strongest_value, sort_numeric = _best_numeric_value(
                    numeric_mentions, "damage reduction"
                )
            elif defense_type == "Healing":
                strongest_value, sort_numeric = _best_numeric_value(
                    numeric_mentions, "healing"
                )
            elif defense_type == "Invincibility":
                strongest_value, sort_numeric = _best_duration_value(durations)
            else:
                strongest_value, sort_numeric = "-", None

            rows.append(
                {
                    "variant_id": int(source_row["variant_id"]),
                    "variant_label": str(source_row["variant_label"]),
                    "stage_label": str(source_row["stage_label"]),
                    "source_kind": str(source_row["source_kind"]),
                    "source_name": str(source_row["source_name"]),
                    "defense_type": defense_type,
                    "strongest_value": strongest_value,
                    "sort_numeric": sort_numeric,
                    "duration_summary": join_or_dash(
                        [str(duration) for duration in durations], limit=3
                    ),
                    "target_summary": join_or_dash(
                        [
                            str(target)
                            for target in source_row.get("target_mentions", [])
                        ],
                        limit=3,
                    ),
                    "mechanics": join_or_dash(
                        [str(tag) for tag in source_row.get("mechanic_tags", [])],
                        limit=6,
                    ),
                    "context": preview_value(str(source_row["current_effect"])),
                }
            )

        strongest_value, sort_numeric = _best_defense_stat_value(stat_bonuses)
        if strongest_value != "-":
            rows.append(
                {
                    "variant_id": int(source_row["variant_id"]),
                    "variant_label": str(source_row["variant_label"]),
                    "stage_label": str(source_row["stage_label"]),
                    "source_kind": str(source_row["source_kind"]),
                    "source_name": str(source_row["source_name"]),
                    "defense_type": "Defense Stats",
                    "strongest_value": strongest_value,
                    "sort_numeric": sort_numeric,
                    "duration_summary": join_or_dash(
                        [str(duration) for duration in durations], limit=3
                    ),
                    "target_summary": join_or_dash(
                        [
                            str(target)
                            for target in source_row.get("target_mentions", [])
                        ],
                        limit=3,
                    ),
                    "mechanics": join_or_dash(
                        [str(tag) for tag in source_row.get("mechanic_tags", [])],
                        limit=6,
                    ),
                    "context": preview_value(str(source_row["current_effect"])),
                }
            )

    if not rows:
        return pd.DataFrame(
            columns=[
                "variant_id",
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
        )
    frame = pd.DataFrame.from_records(rows)
    frame["sort_order"] = frame["defense_type"].map(TEAM_DEFENSE_CATEGORY_ORDER)
    return (
        frame.sort_values(["sort_order", "variant_label", "stage_label", "source_name"])
        .drop(columns=["sort_order"])
        .reset_index(drop=True)
    )


def build_team_defense_summary(defense_evidence_df: pd.DataFrame) -> pd.DataFrame:
    if defense_evidence_df.empty:
        return pd.DataFrame(
            columns=[
                "defense_type",
                "providers",
                "source_rows",
                "strongest_value",
                "durations",
                "examples",
            ]
        )

    rows: list[dict[str, object]] = []
    for defense_type, group in defense_evidence_df.groupby(
        "defense_type", dropna=False
    ):
        strongest_value = "-"
        sorted_group = group.sort_values(
            "sort_numeric", ascending=False, na_position="last"
        )
        if not sorted_group.empty:
            strongest_value = str(sorted_group.iloc[0]["strongest_value"])
        rows.append(
            {
                "defense_type": defense_type,
                "providers": int(group["variant_id"].nunique()),
                "source_rows": int(len(group.index)),
                "strongest_value": strongest_value,
                "durations": join_or_dash(group["duration_summary"].tolist(), limit=2),
                "examples": join_or_dash(group["context"].tolist(), limit=2),
            }
        )

    frame = pd.DataFrame.from_records(rows)
    frame["sort_order"] = frame["defense_type"].map(TEAM_DEFENSE_CATEGORY_ORDER)
    return (
        frame.sort_values("sort_order")
        .drop(columns=["sort_order"])
        .reset_index(drop=True)
    )


def build_team_member_snapshot(
    variant_roster_df: pd.DataFrame,
    team_sources_df: pd.DataFrame,
) -> pd.DataFrame:
    if variant_roster_df.empty:
        return pd.DataFrame()

    skill_cost_df = build_team_skill_cost_frame(team_sources_df)
    sp_evidence_df = build_team_sp_evidence_frame(team_sources_df)
    defense_evidence_df = build_team_defense_evidence_frame(team_sources_df)

    snapshot_df = (
        variant_roster_df[
            [
                "variant_id",
                "variant_label",
                "name_en",
                "role",
                "meta_rank",
                "final_meta_score",
            ]
        ]
        .drop_duplicates(subset=["variant_id"])
        .copy()
    )

    if not skill_cost_df.empty:
        cost_metrics_df = (
            team_sources_df[
                (team_sources_df["source_kind"] == "skill")
                & team_sources_df["sp_cost"].notna()
            ]
            .groupby("variant_id", dropna=False)
            .agg(
                sp_cost_skills=("source_name", "count"),
                highest_sp_cost=("sp_cost", "max"),
            )
            .reset_index()
        )
        snapshot_df = snapshot_df.merge(cost_metrics_df, on="variant_id", how="left")
    else:
        snapshot_df["sp_cost_skills"] = 0
        snapshot_df["highest_sp_cost"] = 0.0

    if not sp_evidence_df.empty:
        flat_gain_df = (
            sp_evidence_df[sp_evidence_df["economy_type"] == "Flat SP Gains"]
            .groupby("variant_id", dropna=False)["numeric_value"]
            .sum()
            .reset_index(name="flat_sp_gain")
        )
        rate_gain_df = (
            sp_evidence_df[sp_evidence_df["economy_type"] == "SP Per Second"]
            .groupby("variant_id", dropna=False)["numeric_value"]
            .sum()
            .reset_index(name="sp_per_second")
        )
        discount_df = (
            sp_evidence_df[sp_evidence_df["economy_type"] == "SP Cost Discounts"]
            .groupby("variant_id", dropna=False)
            .size()
            .reset_index(name="sp_discount_clauses")
        )
        free_cast_df = (
            sp_evidence_df[sp_evidence_df["economy_type"] == "Free Cast Clauses"]
            .groupby("variant_id", dropna=False)
            .size()
            .reset_index(name="free_cast_clauses")
        )
        snapshot_df = snapshot_df.merge(flat_gain_df, on="variant_id", how="left")
        snapshot_df = snapshot_df.merge(rate_gain_df, on="variant_id", how="left")
        snapshot_df = snapshot_df.merge(discount_df, on="variant_id", how="left")
        snapshot_df = snapshot_df.merge(free_cast_df, on="variant_id", how="left")
    else:
        snapshot_df["flat_sp_gain"] = 0.0
        snapshot_df["sp_per_second"] = 0.0
        snapshot_df["sp_discount_clauses"] = 0
        snapshot_df["free_cast_clauses"] = 0

    for defense_type, column_name in (
        ("Shield", "shield_sources"),
        ("Damage Reduction", "damage_reduction_sources"),
        ("Invincibility", "invincibility_sources"),
        ("Healing", "healing_sources"),
        ("Cleanse", "cleanse_sources"),
        ("Resurrection", "resurrection_sources"),
        ("Defense Stats", "defense_stat_rows"),
    ):
        if defense_evidence_df.empty:
            snapshot_df[column_name] = 0
            continue
        metric_df = (
            defense_evidence_df[defense_evidence_df["defense_type"] == defense_type]
            .groupby("variant_id", dropna=False)
            .size()
            .reset_index(name=column_name)
        )
        snapshot_df = snapshot_df.merge(metric_df, on="variant_id", how="left")

    snapshot_df = snapshot_df.fillna(
        {
            "sp_cost_skills": 0,
            "highest_sp_cost": 0.0,
            "flat_sp_gain": 0.0,
            "sp_per_second": 0.0,
            "sp_discount_clauses": 0,
            "free_cast_clauses": 0,
            "shield_sources": 0,
            "damage_reduction_sources": 0,
            "invincibility_sources": 0,
            "healing_sources": 0,
            "cleanse_sources": 0,
            "resurrection_sources": 0,
            "defense_stat_rows": 0,
        }
    )
    snapshot_df[
        [
            "sp_cost_skills",
            "sp_discount_clauses",
            "free_cast_clauses",
            "shield_sources",
            "damage_reduction_sources",
            "invincibility_sources",
            "healing_sources",
            "cleanse_sources",
            "resurrection_sources",
            "defense_stat_rows",
        ]
    ] = snapshot_df[
        [
            "sp_cost_skills",
            "sp_discount_clauses",
            "free_cast_clauses",
            "shield_sources",
            "damage_reduction_sources",
            "invincibility_sources",
            "healing_sources",
            "cleanse_sources",
            "resurrection_sources",
            "defense_stat_rows",
        ]
    ].astype(
        int
    )
    return snapshot_df.sort_values(["meta_rank", "variant_label"]).reset_index(
        drop=True
    )
