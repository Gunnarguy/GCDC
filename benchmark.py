import time
import pandas as pd
import sys

# Create dummy dataframe to test iterrows vs itertuples
df = pd.DataFrame({
    "variant_title": ["Title"] * 1000,
    "variant_label": ["Label"] * 1000,
    "progression_stage": ["Stage"] * 1000,
    "stage_order": [1] * 1000,
    "skill_name": ["Skill"] * 1000,
    "skill_family": ["Family"] * 1000,
    "progression_tracks": ["Tracks"] * 1000,
    "cooldown": ["-"] * 1000,
    "sp": ["-"] * 1000,
    "top_coefficient": ["-"] * 1000,
    "durations": ["-"] * 1000,
    "stacks": ["-"] * 1000,
    "chances": ["-"] * 1000,
    "thresholds": ["-"] * 1000,
    "mechanics": ["-"] * 1000,
    "stats": ["-"] * 1000,
    "current_effect": ["-"] * 1000,
})

def test_iterrows():
    rows = []
    for _, row in df.iterrows():
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
                "excerpt": str(row["current_effect"]),
                "table_key": "",
            }
        )

def test_itertuples():
    rows = []
    for row in df.itertuples(index=False):
        variant_label = getattr(row, "variant_label", row.variant_title)
        rows.append(
            {
                "variant_title": str(row.variant_title),
                "variant_label": str(variant_label),
                "stage_label": str(row.progression_stage),
                "stage_order": int(row.stage_order),
                "source_kind": "skill",
                "source_name": str(row.skill_name),
                "family": str(row.skill_family),
                "progression_tracks": str(row.progression_tracks),
                "modifiers": "; ".join(
                    value
                    for value in (
                        (
                            f"Cooldown {row.cooldown}"
                            if row.cooldown != "-"
                            else ""
                        ),
                        f"SP {row.sp}" if row.sp != "-" else "",
                        (
                            f"Top damage {row.top_coefficient}"
                            if row.top_coefficient != "-"
                            else ""
                        ),
                        (
                            f"Durations {row.durations}"
                            if row.durations != "-"
                            else ""
                        ),
                        f"Stacks {row.stacks}" if row.stacks != "-" else "",
                        (
                            f"Chances {row.chances}"
                            if row.chances != "-"
                            else ""
                        ),
                        (
                            f"Thresholds {row.thresholds}"
                            if row.thresholds != "-"
                            else ""
                        ),
                    )
                    if value
                )
                or "-",
                "mechanics": str(row.mechanics),
                "stats": str(row.stats),
                "top_coefficient": str(row.top_coefficient),
                "excerpt": str(row.current_effect),
                "table_key": "",
            }
        )

import timeit
print(f"iterrows: {timeit.timeit(test_iterrows, number=100)}")
print(f"itertuples: {timeit.timeit(test_itertuples, number=100)}")
