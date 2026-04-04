"""Ingest the GCDC community meta spreadsheet into structured DataFrames.

Parses all sheets from ``data/raw/gcdc_meta_spreadsheet/`` and returns
normalised DataFrames ready for SQLite insertion.
"""

from __future__ import annotations

import csv
import logging
import re

import pandas as pd

from .paths import RAW_DATA_DIR

LOGGER = logging.getLogger(__name__)
SHEET_DIR = RAW_DATA_DIR / "gcdc_meta_spreadsheet"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_WHITESPACE = re.compile(r"\s+")


def _clean(val: object) -> str:
    """Stringify and collapse whitespace."""
    if val is None or (isinstance(val, float) and val != val):  # NaN
        return ""
    return _WHITESPACE.sub(" ", str(val)).strip()


def _read_sheet(name: str) -> list[list[str]]:
    """Return raw rows from a CSV sheet file."""
    path = SHEET_DIR / f"{name}.csv"
    if not path.exists():
        LOGGER.warning("Sheet %s not found at %s", name, path)
        return []
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.reader(f))


def _safe_bool(val: str) -> bool | None:
    v = _clean(val).lower()
    if v in ("true", "1", "yes"):
        return True
    if v in ("false", "0", "no"):
        return False
    return None


def _parse_date(val: str) -> str:
    """Return ISO date string or empty."""
    v = _clean(val)
    if not v:
        return ""
    # "2023-07-04 00:00:00" → "2023-07-04"
    return v[:10] if len(v) >= 10 else v


# ---------------------------------------------------------------------------
# 1. Unit Data  —  the master hero/variant roster
# ---------------------------------------------------------------------------

UNITDATA_COLUMNS = [
    "icon1",
    "icon2",
    "fluff",
    "name",
    "longname",
    "shortname",
    "length",
    "keysname",
    "attribute",
    "color",
    "unit_class",
    "job_type",
    "sort",
    "sort2",
    "released",
    "kr_release_date",
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
    "hero_traits_label",
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
    "chaser_traits_label",
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
    "runes_label",
    "rn1",
    "rn2",
    "artifact",
    "accessories_label",
    "ac1",
    "ac2",
    "ac3",
    "equip_set",
    "trans_traits_label",
    "tc1",
    "mt1",
    "tt1",
    "tt2",
    "prev1",
    "pre2",
    "tc2",
    "mt2",
    "tt3",
    "tt4",
    "pre3",
    "pre4",
    "equip_preset_label",
    "eq_green_cdr",
    "eq_green_aspd",
    "eq_green_crit",
    "eq_orange_cdr",
    "eq_orange_crit",
    "eq_purple_cdr",
    "eq_purple_crit",
    "eq_mix_cdr",
    "eq_mix_ss",
    "eq_red_cdr",
    "si_build_label",
    "is_pve",
    "is_pvp",
    "is_support",
    "unused1",
    "temp",
]


def parse_unit_data() -> pd.DataFrame:
    """Parse ``unitdata.csv`` into a DataFrame with one row per unit."""
    rows = _read_sheet("unitdata")
    if len(rows) < 2:
        return pd.DataFrame()

    records: list[dict] = []
    for raw_row in rows[1:]:
        if len(raw_row) < 15:
            continue
        name = _clean(raw_row[3])
        if not name:
            continue

        rec: dict = {}
        for i, col in enumerate(UNITDATA_COLUMNS):
            rec[col] = _clean(raw_row[i]) if i < len(raw_row) else ""

        # Normalise key fields
        rec["name"] = name
        rec["attribute"] = _clean(rec.get("attribute", ""))
        rec["color"] = _clean(rec.get("color", ""))
        rec["unit_class"] = _clean(rec.get("unit_class", ""))
        rec["job_type"] = _clean(rec.get("job_type", ""))
        rec["kr_release_date"] = _parse_date(rec.get("kr_release_date", ""))
        rec["is_pve"] = _safe_bool(rec.get("is_pve", ""))
        rec["is_pvp"] = _safe_bool(rec.get("is_pvp", ""))
        rec["is_support"] = _safe_bool(rec.get("is_support", ""))
        rec["released"] = _safe_bool(rec.get("released", ""))
        records.append(rec)

    df = pd.DataFrame(records)
    LOGGER.info("Parsed %d units from unitdata sheet", len(df))
    return df


# ---------------------------------------------------------------------------
# 2. Builds  —  detailed build specs per hero
# ---------------------------------------------------------------------------


def parse_builds() -> pd.DataFrame:
    """Parse ``builds.csv``.

    The builds sheet has alternating "header" rows (Icon, Class, Name …)
    and "detail" rows with trait values.  We extract pairs.
    """
    rows = _read_sheet("builds")
    if len(rows) < 4:
        return pd.DataFrame()

    records: list[dict] = []
    i = 3  # skip the first 3 header rows
    while i < len(rows):
        row = rows[i]
        # A header row has the hero name in column 2 and the next-hero name in col 3
        name = _clean(row[2]) if len(row) > 2 else ""
        if not name:
            i += 1
            continue

        attribute = _clean(row[6]) if len(row) > 6 else ""
        unit_class = _clean(row[7]) if len(row) > 7 else ""
        content_tag = _clean(row[8]) if len(row) > 8 else ""

        # Next row should be the detail row with trait values
        detail = rows[i + 1] if i + 1 < len(rows) else []
        hero_detail_name = _clean(detail[3]) if len(detail) > 3 else ""

        # Hero traits: cols 10-14 (5 slots)
        hero_traits = [
            _clean(detail[j]) if len(detail) > j else "" for j in range(10, 15)
        ]
        # Chaser traits: cols 16-20 (5 slots)
        chaser_traits = [
            _clean(detail[j]) if len(detail) > j else "" for j in range(16, 21)
        ]
        # CS level col 22
        cs_level = _clean(detail[22]) if len(detail) > 22 else ""
        # Runes: cols 23-24 (Normal, Special)
        rune_normal = _clean(detail[23]) if len(detail) > 23 else ""
        rune_special = _clean(detail[24]) if len(detail) > 24 else ""
        # Accessories: cols 26-28 (Ring, Necklace, Earring)
        acc_ring = _clean(detail[26]) if len(detail) > 26 else ""
        acc_necklace = _clean(detail[27]) if len(detail) > 27 else ""
        acc_earring = _clean(detail[28]) if len(detail) > 28 else ""
        # Trans traits main: col 30 = TC1, 31 = T3, 32 = T6
        trans_main_mode = _clean(detail[30]) if len(detail) > 30 else ""
        trans_main_t3 = _clean(detail[31]) if len(detail) > 31 else ""
        trans_main_t6 = _clean(detail[32]) if len(detail) > 32 else ""

        records.append(
            {
                "name": hero_detail_name or name,
                "header_name": name,
                "attribute": attribute,
                "unit_class": unit_class,
                "content_tag": content_tag,
                "hero_trait_1": hero_traits[0],
                "hero_trait_2": hero_traits[1],
                "hero_trait_3": hero_traits[2],
                "hero_trait_4": hero_traits[3],
                "hero_trait_5": hero_traits[4],
                "chaser_trait_1": chaser_traits[0],
                "chaser_trait_2": chaser_traits[1],
                "chaser_trait_3": chaser_traits[2],
                "chaser_trait_4": chaser_traits[3],
                "chaser_trait_5": chaser_traits[4],
                "cs_level": cs_level,
                "rune_normal": rune_normal,
                "rune_special": rune_special,
                "acc_ring": acc_ring,
                "acc_necklace": acc_necklace,
                "acc_earring": acc_earring,
                "trans_main_mode": trans_main_mode,
                "trans_main_t3": trans_main_t3,
                "trans_main_t6": trans_main_t6,
            }
        )
        i += 2  # skip the pair

    df = pd.DataFrame(records)
    # Drop rows that are just empty or header artifacts
    df = df[df["name"].str.len() > 0].reset_index(drop=True)
    LOGGER.info("Parsed %d build entries from builds sheet", len(df))
    return df


# ---------------------------------------------------------------------------
# 3. PvE Meta tiers
# ---------------------------------------------------------------------------


def parse_pve_meta() -> pd.DataFrame:
    """Parse ``pve_meta.csv`` into attribute-grouped tier rows."""
    rows = _read_sheet("pve_meta")
    if len(rows) < 7:
        return pd.DataFrame()

    records: list[dict] = []

    # The sheet uses a column-grouped layout:
    # Cols 0-5 = Retribution, 7-12 = Life, 13-17 = Balance, 18-19 = PvP Usable
    # Rows come in pairs: attribute row + name row
    tier_groups = [
        ("Retribution", 1, 5),
        ("Life", 7, 11),
        ("Balance", 13, 17),
    ]

    # Parse row-pairs starting from row 6 (after headers)
    row_idx = 6
    tier_rank = 1
    while row_idx + 1 < len(rows):
        attr_row = rows[row_idx]
        name_row = rows[row_idx + 1]

        found_any = False
        for group_attr, col_start, col_end in tier_groups:
            for c in range(col_start, min(col_end + 1, len(name_row))):
                hero_name = _clean(name_row[c])
                hero_attr = _clean(attr_row[c]) if c < len(attr_row) else ""
                if hero_name and hero_name != group_attr:
                    records.append(
                        {
                            "meta_type": "PvE",
                            "tier_group": group_attr,
                            "tier_rank": tier_rank,
                            "hero_name": hero_name,
                            "attribute": hero_attr or group_attr,
                        }
                    )
                    found_any = True

        # PvP usable column (18-19)
        for c in range(18, min(20, len(name_row))):
            hero_name = _clean(name_row[c])
            if hero_name:
                records.append(
                    {
                        "meta_type": "PvE_PvP_Usable",
                        "tier_group": "PvP Usable",
                        "tier_rank": tier_rank,
                        "hero_name": hero_name,
                        "attribute": _clean(attr_row[c]) if c < len(attr_row) else "",
                    }
                )
                found_any = True

        if found_any:
            tier_rank += 1
        row_idx += 2
        # Skip blank separator rows
        while row_idx < len(rows) and all(not _clean(c) for c in rows[row_idx]):
            row_idx += 1

    df = pd.DataFrame(records)
    LOGGER.info("Parsed %d PvE meta entries", len(df))
    return df


# ---------------------------------------------------------------------------
# 4. PvP Meta compositions
# ---------------------------------------------------------------------------


def parse_pvp_meta() -> pd.DataFrame:
    """Parse ``pvp_meta.csv`` into team compositions."""
    rows = _read_sheet("pvp_meta")
    if len(rows) < 4:
        return pd.DataFrame()

    records: list[dict] = []
    current_section = "Attack"
    i = 0
    while i < len(rows):
        row = rows[i]
        first = _clean(row[0]) if row else ""

        # Detect section headers
        if "attack meta" in first.lower():
            current_section = "Attack"
            i += 2  # skip header line
            continue
        if "defense meta" in first.lower():
            current_section = "Defense"
            i += 1
            continue

        # Attribute rows followed by name rows
        # Look for rows with attribute names (Ruin, Cycle, Life, etc.)
        if first in ("Ruin", "Cycle", "Life", "Balance", "Retribution", "Spirit"):
            attr_row = row
            name_row = rows[i + 1] if i + 1 < len(rows) else []
            team_members = []
            for c in range(min(len(attr_row), len(name_row))):
                attr = _clean(attr_row[c])
                name = _clean(name_row[c])
                if name and attr in (
                    "Ruin",
                    "Cycle",
                    "Life",
                    "Balance",
                    "Retribution",
                    "Spirit",
                ):
                    team_members.append({"name": name, "attribute": attr})

            if team_members:
                # Split into teams (left side = main, right side = alt, separated by empty cols)
                # Find split point: consecutive empty columns
                team_a = []
                team_b = []
                in_gap = False
                for c in range(min(len(attr_row), len(name_row))):
                    a = _clean(attr_row[c])
                    n = _clean(name_row[c])
                    if not a and not n:
                        if team_a and not in_gap:
                            in_gap = True
                        continue
                    if in_gap and n:
                        team_b.append({"name": n, "attribute": a})
                    elif n and a:
                        team_a.append({"name": n, "attribute": a})

                for team_idx, team in enumerate([team_a, team_b]):
                    if not team:
                        continue
                    members_str = ", ".join(m["name"] for m in team)
                    attrs_str = ", ".join(m["attribute"] for m in team)
                    records.append(
                        {
                            "section": current_section,
                            "team_variant": team_idx + 1,
                            "members": members_str,
                            "attributes": attrs_str,
                            "member_count": len(team),
                        }
                    )
            i += 2
        else:
            # Check for defense hero list (single-row hero names)
            if current_section == "Defense":
                names = [_clean(c) for c in row if _clean(c)]
                # Filter out section headers
                candidate_names = [
                    n
                    for n in names
                    if n
                    and not any(
                        kw in n.lower()
                        for kw in (
                            "defense",
                            "hide",
                            "meta",
                            "guild",
                            "tourney",
                            "combination",
                            "expected",
                            "win",
                            "condition",
                            "fuck",
                            "lime",
                        )
                    )
                ]
                for name in candidate_names:
                    records.append(
                        {
                            "section": "Defense",
                            "team_variant": 0,
                            "members": name,
                            "attributes": "",
                            "member_count": 1,
                        }
                    )
            i += 1

    df = pd.DataFrame(records)
    LOGGER.info("Parsed %d PvP meta entries", len(df))
    return df


# ---------------------------------------------------------------------------
# 5. Content Usage matrix  —  which heroes work in which content
# ---------------------------------------------------------------------------

CONTENT_MODES = [
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
    "Raids",
    "Raid (17)",
    "GoC",
    "Chasm",
    "Boss",
    "AoT",
    "AoT (2)",
    "AoT (3)",
    "AoT (4)",
    "AH (1)",
    "AH (2)",
    "AH (3)",
    "Merc",
    "GB",
    "FC",
    "FC 2",
    "FC 3",
    "PvP ATK",
    "PvP DEF",
    "Arena",
]


def parse_content_usage() -> pd.DataFrame:
    """Parse ``content_usage_wip.csv`` into hero × content viability matrix."""
    rows = _read_sheet("content_usage_wip")
    if len(rows) < 3:
        return pd.DataFrame()

    records: list[dict] = []
    for raw_row in rows[2:]:  # skip header rows
        name = _clean(raw_row[2]) if len(raw_row) > 2 else ""
        if not name:
            continue

        for mode_idx, mode_name in enumerate(CONTENT_MODES):
            col = mode_idx + 3  # content columns start at col 3
            cell = _clean(raw_row[col]) if col < len(raw_row) else ""
            # Empty = usable/recommended, ❌ = not usable
            is_viable = cell != "❌" if cell else None
            if is_viable is not None:
                records.append(
                    {
                        "hero_name": name,
                        "content_mode": mode_name,
                        "is_viable": is_viable,
                    }
                )

    df = pd.DataFrame(records)
    LOGGER.info("Parsed %d content usage entries", len(df))
    return df


# ---------------------------------------------------------------------------
# 6. Content Teams  —  team compositions for raids, bosses, etc.
# ---------------------------------------------------------------------------

CONTENT_SHEETS = [
    ("raids", "Raids"),
    ("world_boss", "World Boss"),
    ("world_boss_season_2", "World Boss Season 2"),
    ("guild_boss", "Guild Boss"),
    ("hells_furnace_balance", "Hell's Furnace Balance"),
    ("hells_furnace_life", "Hell's Furnace Life"),
    ("hells_furnace_retribution", "Hell's Furnace Retribution"),
    ("berkas_lair", "Berkas Lair"),
    ("aernasis_hammer", "Aernasis Hammer"),
    ("altar_of_time", "Altar of Time"),
    ("final_core", "Final Core"),
    ("assembly", "Assembly"),
    ("support_party", "Support Party"),
]

_ATTRIBUTE_SET = {"Ruin", "Cycle", "Life", "Balance", "Retribution", "Spirit"}


def _parse_content_teams_sheet(sheet_name: str, content_label: str) -> list[dict]:
    """Parse a single content team sheet into records."""
    rows = _read_sheet(sheet_name)
    if not rows:
        return []

    records: list[dict] = []
    current_phase = ""
    i = 0
    while i < len(rows):
        row = rows[i]
        first = _clean(row[0]) if row else ""

        # Detect phase/section headers
        phase_match = re.match(
            r"^(Phase \d+|Raid \d+|World \d+|Season \d+)",
            first,
            re.IGNORECASE,
        )
        if phase_match:
            current_phase = phase_match.group(1)
            i += 1
            continue

        # Also pick up named sections like "Raid 18", "Phase 1" etc.
        if first and not any(first.startswith(a) for a in _ATTRIBUTE_SET):
            # Could be a section header if it contains descriptive text
            if any(
                kw in first.lower()
                for kw in (
                    "raid",
                    "phase",
                    "world",
                    "video",
                    "playlist",
                    "leave",
                    "note",
                )
            ):
                if (
                    "raid" in first.lower()
                    or "phase" in first.lower()
                    or "world" in first.lower()
                ):
                    current_phase = first
                i += 1
                continue

        # Check if this is an attribute row
        if first in _ATTRIBUTE_SET:
            attr_row = row
            name_row = rows[i + 1] if i + 1 < len(rows) else []

            # Determine notes from surrounding rows
            notes = ""
            # Check for notes column (usually col 5 of nearby rows)
            for check_row in rows[max(0, i - 2) : i]:
                for c in range(4, min(7, len(check_row))):
                    v = _clean(check_row[c])
                    if v and len(v) > 5 and not v.startswith("Off"):
                        notes = v
                        break

            # Parse main team (left side) and off-meta (right side)
            for team_type in ("main", "off_meta"):
                if team_type == "main":
                    col_range = range(0, min(5, len(name_row)))
                else:
                    # Off-meta typically starts after column 6
                    col_range = range(6, min(len(name_row), 16))

                members = []
                for c in col_range:
                    attr = _clean(attr_row[c]) if c < len(attr_row) else ""
                    name = _clean(name_row[c]) if c < len(name_row) else ""
                    if name and attr in _ATTRIBUTE_SET:
                        members.append({"name": name, "attribute": attr})

                if members:
                    records.append(
                        {
                            "content": content_label,
                            "phase": current_phase,
                            "team_type": team_type,
                            "members": ", ".join(m["name"] for m in members),
                            "attributes": ", ".join(m["attribute"] for m in members),
                            "member_count": len(members),
                            "notes": notes,
                        }
                    )

            i += 2
        else:
            i += 1

    return records


def parse_content_teams() -> pd.DataFrame:
    """Parse all content team sheets into a single DataFrame."""
    all_records: list[dict] = []
    for sheet_name, label in CONTENT_SHEETS:
        records = _parse_content_teams_sheet(sheet_name, label)
        all_records.extend(records)
        if records:
            LOGGER.info("Parsed %d teams from %s", len(records), sheet_name)

    df = pd.DataFrame(all_records)
    LOGGER.info("Parsed %d total content team entries", len(df))
    return df


# ---------------------------------------------------------------------------
# 7. Equipment Presets
# ---------------------------------------------------------------------------


def parse_equipment_presets() -> pd.DataFrame:
    """Parse ``equipment_presets.csv`` into preset definitions."""
    rows = _read_sheet("equipment_presets")
    if len(rows) < 4:
        return pd.DataFrame()

    records: list[dict] = []
    current_class = ""

    for raw_row in rows[2:]:  # skip 2 header rows
        cls = _clean(raw_row[0]) if raw_row else ""
        if cls:
            current_class = cls
        preset_name = _clean(raw_row[1]) if len(raw_row) > 1 else ""
        if not preset_name:
            continue
        set_color = _clean(raw_row[2]) if len(raw_row) > 2 else ""

        # Additional stats
        stat_first_line = _clean(raw_row[5]) if len(raw_row) > 5 else ""
        weapon_2nd = _clean(raw_row[6]) if len(raw_row) > 6 else ""
        supp_weapon_2nd = _clean(raw_row[7]) if len(raw_row) > 7 else ""
        armor_2nd = _clean(raw_row[8]) if len(raw_row) > 8 else ""

        # Magic enchants
        enchant_1 = _clean(raw_row[11]) if len(raw_row) > 11 else ""
        enchant_2 = _clean(raw_row[12]) if len(raw_row) > 12 else ""
        enchant_3 = _clean(raw_row[13]) if len(raw_row) > 13 else ""

        records.append(
            {
                "equipment_class": current_class,
                "preset_name": preset_name,
                "set_color": set_color,
                "stat_first_line": stat_first_line,
                "weapon_second_line": weapon_2nd,
                "supp_weapon_second_line": supp_weapon_2nd,
                "armor_second_line": armor_2nd,
                "enchant_1": enchant_1,
                "enchant_2": enchant_2,
                "enchant_3": enchant_3,
            }
        )

    df = pd.DataFrame(records)
    LOGGER.info("Parsed %d equipment presets", len(df))
    return df


# ---------------------------------------------------------------------------
# 8. Soul Imprint presets
# ---------------------------------------------------------------------------


def parse_soul_imprint() -> pd.DataFrame:
    """Parse ``soul_imprint.csv`` into preset → hero mappings."""
    rows = _read_sheet("soul_imprint")
    if len(rows) < 4:
        return pd.DataFrame()

    records: list[dict] = []
    # The sheet has preset definitions in top rows, then hero assignment rows
    # Row structure: preset info cols 0-14, then hero attribute row + hero name row
    # Heroes start at column 16+

    for raw_row in rows[3:]:
        # Look for rows with hero names in attribute+name pairs
        if len(raw_row) < 17:
            continue
        for c in range(16, len(raw_row)):
            name = _clean(raw_row[c])
            if name and name not in ("", "0"):
                # Get attribute from rows above
                records.append(
                    {
                        "hero_name": name,
                        "column_index": c,
                    }
                )

    df = pd.DataFrame(records)
    LOGGER.info("Parsed %d soul imprint entries", len(df))
    return df


# ---------------------------------------------------------------------------
# 9. Changelog
# ---------------------------------------------------------------------------


def parse_changelog() -> pd.DataFrame:
    """Parse ``changelog.csv`` into date + description entries."""
    rows = _read_sheet("changelog")
    if len(rows) < 3:
        return pd.DataFrame()

    records: list[dict] = []
    current_date = ""
    for raw_row in rows[1:]:  # skip header
        date_val = _clean(raw_row[0]) if raw_row else ""
        entry_val = _clean(raw_row[1]) if len(raw_row) > 1 else ""

        if date_val:
            current_date = _parse_date(date_val)
        if entry_val:
            records.append(
                {
                    "date": current_date,
                    "entry": entry_val,
                }
            )

    df = pd.DataFrame(records)
    LOGGER.info("Parsed %d changelog entries", len(df))
    return df


# ---------------------------------------------------------------------------
# 10. Release Order batches
# ---------------------------------------------------------------------------


def parse_release_order() -> pd.DataFrame:
    """Parse ``release_order.csv`` into release batch entries."""
    rows = _read_sheet("release_order")
    if len(rows) < 4:
        return pd.DataFrame()

    records: list[dict] = []
    # Layout: 4 column groups (Soul Imprint, Job Change, Special, Spirit/Descent)
    groups = [
        ("Soul Imprint", 0, 4),
        ("Job Change", 6, 10),
        ("Special", 12, 14),
        ("Spirit/Descent", 16, 18),
    ]

    current_batch: dict[str, str] = {}
    i = 2  # Skip first 2 rows (header)
    while i < len(rows):
        row = rows[i]
        first = _clean(row[0]) if row else ""

        # Batch header detection
        if first.lower().startswith("batch"):
            for group_name, _, _ in groups:
                # Find batch label in each group
                for col_start in [0, 6, 12, 16]:
                    if col_start < len(row):
                        val = _clean(row[col_start])
                        if val.lower().startswith("batch"):
                            current_batch[group_name] = val
            i += 1
            # Skip empty row after batch header
            if i < len(rows) and all(not _clean(c) for c in rows[i]):
                i += 1
            continue

        # Check for attribute row + name row pair
        if first in _ATTRIBUTE_SET or any(
            _clean(row[c]) in _ATTRIBUTE_SET for c in range(len(row)) if c < len(row)
        ):
            attr_row = row
            name_row = rows[i + 1] if i + 1 < len(rows) else []

            for group_name, col_start, col_end in groups:
                batch_label = current_batch.get(group_name, "")
                for c in range(col_start, min(col_end + 1, len(name_row))):
                    attr = _clean(attr_row[c]) if c < len(attr_row) else ""
                    name = _clean(name_row[c]) if c < len(name_row) else ""
                    if name and attr in _ATTRIBUTE_SET:
                        records.append(
                            {
                                "release_type": group_name,
                                "batch": batch_label,
                                "attribute": attr,
                                "hero_name": name,
                            }
                        )
            i += 2
        else:
            i += 1

    df = pd.DataFrame(records)
    LOGGER.info("Parsed %d release order entries", len(df))
    return df


# ---------------------------------------------------------------------------
# 11. Content Keys (div.csv)  —  team composition key mappings
# ---------------------------------------------------------------------------

_TEAM_KEY_PATTERN = re.compile(
    r"^([A-Za-z\s()]+(?:_[A-Za-z\s()]+)+)_V\d+_[A-F0-9]+", re.IGNORECASE
)


def parse_content_keys() -> pd.DataFrame:
    """Parse ``div.csv`` into content → team-key mappings."""
    rows = _read_sheet("div")
    if len(rows) < 2:
        return pd.DataFrame()

    records: list[dict] = []
    for raw_row in rows[1:]:  # skip header
        content = _clean(raw_row[0]) if raw_row else ""
        key = _clean(raw_row[1]) if len(raw_row) > 1 else ""
        if not content or not key:
            continue

        # Extract hero names from key pattern: Hero1_Hero2(T)_Hero3_V1_<hash>
        m = _TEAM_KEY_PATTERN.match(key)
        team_names = ""
        if m:
            names_part = m.group(1)
            team_names = ", ".join(
                n.strip() for n in names_part.split("_") if n.strip()
            )

        records.append(
            {
                "content": content,
                "team_key": key[:200],  # truncate extremely long keys
                "team_members": team_names,
            }
        )

    df = pd.DataFrame(records)
    LOGGER.info("Parsed %d content key entries from div sheet", len(df))
    return df


# ---------------------------------------------------------------------------
# 12. Beginners Guide  —  game system info
# ---------------------------------------------------------------------------


def parse_beginners_guide() -> pd.DataFrame:
    """Parse ``beginners_guide.csv`` into topic → info rows."""
    rows = _read_sheet("beginners_guide")
    if len(rows) < 3:
        return pd.DataFrame()

    records: list[dict] = []
    current_topic = ""
    for raw_row in rows:
        first = _clean(raw_row[0]) if raw_row else ""
        if not first:
            continue

        # Topic headers are usually single-cell with descriptive names
        if len(first) > 3 and not any(first.startswith(a) for a in _ATTRIBUTE_SET):
            rest = " ".join(_clean(c) for c in raw_row[1:] if _clean(c))
            if rest:
                records.append(
                    {
                        "topic": current_topic or first,
                        "content": f"{first} {rest}".strip(),
                    }
                )
            else:
                current_topic = first

    df = pd.DataFrame(records)
    LOGGER.info("Parsed %d beginners guide entries", len(df))
    return df


# ---------------------------------------------------------------------------
# Master ingest function
# ---------------------------------------------------------------------------


def ingest_all() -> dict[str, pd.DataFrame]:
    """Parse every spreadsheet sheet and return named DataFrames."""
    if not SHEET_DIR.exists():
        LOGGER.warning("Spreadsheet directory not found: %s", SHEET_DIR)
        return {}

    result = {
        "unit_data": parse_unit_data(),
        "builds": parse_builds(),
        "pve_meta": parse_pve_meta(),
        "pvp_meta": parse_pvp_meta(),
        "content_usage": parse_content_usage(),
        "content_teams": parse_content_teams(),
        "equipment_presets": parse_equipment_presets(),
        "soul_imprint": parse_soul_imprint(),
        "changelog": parse_changelog(),
        "release_order": parse_release_order(),
        "content_keys": parse_content_keys(),
        "beginners_guide": parse_beginners_guide(),
    }

    total = sum(len(df) for df in result.values())
    non_empty = sum(1 for df in result.values() if len(df) > 0)
    LOGGER.info(
        "Spreadsheet ingest complete: %d tables, %d total rows",
        non_empty,
        total,
    )
    return result
