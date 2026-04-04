import sqlite3

import pandas as pd

import grandchase_meta_analyzer.pages as pages_module
from grandchase_meta_analyzer.pages import (
    _extract_patch_entries_from_sections,
    _summarize_patch_coverage,
    build_pages_payload,
)


def test_extract_patch_entries_from_sections_reads_dated_blocks() -> None:
    sections_df = pd.DataFrame(
        [
            {
                "name_en": "Ronan",
                "name_ko": "로난",
                "variant_id": 1,
                "variant_title": "Ronan (Grand Chase for kakao)",
                "variant_kind": "base",
                "variant_label": "Ronan · Base",
                "heading_title": "Guardian Oath",
                "section_path": "Skills > Guardian Oath",
                "content": (
                    "Dungeon Daejeon 〉 Guardian Oath Reduces incoming damage by 30%. "
                    "Patch Details 【Expand/Collapse】 February 27, 2018 Buff : Add 10% shield "
                    "January 8, 2019 Nerf : Damage reduction 40% → 30%"
                ),
                "source_page": "https://example.test/ronan",
            },
            {
                "name_en": "Ronan",
                "name_ko": "로난",
                "variant_id": 1,
                "variant_title": "Ronan (Grand Chase for kakao)",
                "variant_kind": "base",
                "variant_label": "Ronan · Base",
                "heading_title": "Trivia",
                "section_path": "Notes > Trivia",
                "content": "Ronan is a royal guardian.",
                "source_page": "https://example.test/ronan",
            },
            {
                "name_en": "Asin",
                "name_ko": "아신",
                "variant_id": 2,
                "variant_title": "Asin (Grand Chase for kakao)",
                "variant_kind": "base",
                "variant_label": "Asin · Base",
                "heading_title": "Patch Details",
                "section_path": "Patch Details",
                "content": (
                    "Patch Details 【Expand/Collapse】 March 4, 2025: Hero remake "
                    "Asin's attributes change from life to destruction"
                ),
                "source_page": "https://example.test/asin",
            },
        ]
    )

    patch_entries_df = _extract_patch_entries_from_sections(sections_df)

    assert len(patch_entries_df.index) == 3
    assert patch_entries_df.iloc[0]["name_en"] == "Asin"
    assert patch_entries_df.iloc[0]["patch_date"] == "March 4, 2025"
    assert patch_entries_df.iloc[0]["patch_change_type"] == "Remake"
    entries_by_date = {
        row["patch_date"]: row["patch_change_type"]
        for row in patch_entries_df.to_dict(orient="records")
    }
    assert entries_by_date["February 27, 2018"] == "Buff"
    assert entries_by_date["January 8, 2019"] == "Nerf"
    assert (
        patch_entries_df[patch_entries_df["name_en"] == "Ronan"]["patch_block_key"]
        .nunique()
        == 1
    )


def test_variant_kind_label_uses_job_change_wording() -> None:
    assert pages_module._format_variant_kind_label("former", "T") == "Job Change (T)"


def test_summarize_patch_coverage_counts_entries_and_blocks() -> None:
    patch_entries_df = pd.DataFrame(
        [
            {
                "name_en": "Ronan",
                "variant_title": "Ronan (Grand Chase for kakao)",
                "patch_block_key": "1::Guardian Oath::Skills > Guardian Oath",
                "patch_date": "January 8, 2019",
                "patch_change_type": "Nerf",
                "patch_change": "Damage reduction 40% → 30%",
            },
            {
                "name_en": "Ronan",
                "variant_title": "Ronan (Grand Chase for kakao)",
                "patch_block_key": "1::Guardian Oath::Skills > Guardian Oath",
                "patch_date": "February 27, 2018",
                "patch_change_type": "Buff",
                "patch_change": "Add 10% shield",
            },
            {
                "name_en": "Asin",
                "variant_title": "Asin (Grand Chase for kakao)",
                "patch_block_key": "2::Patch Details::Patch Details",
                "patch_date": "March 4, 2025",
                "patch_change_type": "Remake",
                "patch_change": "Hero remake",
            },
        ]
    )

    coverage_df = _summarize_patch_coverage(patch_entries_df)

    ronan_row = coverage_df[coverage_df["name_en"] == "Ronan"].iloc[0]
    asin_row = coverage_df[coverage_df["name_en"] == "Asin"].iloc[0]

    assert int(ronan_row["patch_entries"]) == 2
    assert int(ronan_row["patch_blocks"]) == 1
    assert ronan_row["latest_patch_date"] == "January 8, 2019"
    assert asin_row["latest_patch_type"] == "Remake"


def test_build_pages_payload_includes_system_references_and_release_history(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setattr(pages_module, "PROJECT_ROOT", tmp_path)
    database_path = tmp_path / "atlas-test.db"

    with sqlite3.connect(database_path) as connection:
        connection.executescript(
            """
            CREATE TABLE heroes (
                hero_id INTEGER PRIMARY KEY,
                name_en TEXT,
                name_ko TEXT,
                role TEXT,
                rarity TEXT,
                sources TEXT
            );
            CREATE TABLE hero_modes (
                hero_id INTEGER,
                mode TEXT,
                tier_letter TEXT,
                tier_numeric INTEGER
            );
            CREATE TABLE hero_meta_scores (
                hero_id INTEGER PRIMARY KEY,
                base_score REAL,
                rarity_adjusted REAL,
                final_meta_score REAL,
                meta_rank INTEGER
            );
            CREATE TABLE hero_variants (
                variant_id INTEGER PRIMARY KEY,
                hero_id INTEGER,
                variant_name_en TEXT,
                name_ko TEXT,
                variant_kind TEXT,
                variant_suffix TEXT,
                availability_marker TEXT,
                variant_role TEXT,
                variant_rarity TEXT,
                adventure_tier TEXT,
                battle_tier TEXT,
                boss_tier TEXT,
                source_title TEXT,
                source_href TEXT,
                note_excerpt TEXT,
                source TEXT
            );
            CREATE TABLE variant_meta_scores (
                variant_id INTEGER PRIMARY KEY,
                hero_id INTEGER,
                base_score REAL,
                rarity_adjusted REAL,
                final_meta_score REAL,
                meta_rank INTEGER,
                score_basis TEXT
            );
            CREATE TABLE hero_variant_sections (
                variant_section_id INTEGER PRIMARY KEY,
                variant_id INTEGER,
                heading_level INTEGER,
                heading_id TEXT,
                heading_title TEXT,
                section_path TEXT,
                content TEXT,
                source_page TEXT
            );
            CREATE TABLE hero_variant_skills (
                variant_skill_id INTEGER PRIMARY KEY,
                variant_id INTEGER,
                section_title TEXT,
                skill_stage TEXT,
                skill_type TEXT,
                skill_name TEXT,
                description TEXT
            );
            CREATE TABLE hero_variant_features (
                variant_id INTEGER,
                feature_key TEXT,
                feature_value TEXT
            );
            CREATE TABLE game_system_references (
                reference_id INTEGER PRIMARY KEY,
                source TEXT,
                reference_key TEXT,
                title TEXT,
                section_path TEXT,
                content TEXT,
                source_page TEXT,
                game_era TEXT,
                is_legacy_system INTEGER,
                trust_tier TEXT
            );
            CREATE TABLE hero_release_history (
                release_id INTEGER PRIMARY KEY,
                source TEXT,
                release_order_label TEXT,
                release_order_numeric INTEGER,
                release_year INTEGER,
                hero_name_raw TEXT,
                release_date_text TEXT,
                release_date_iso TEXT,
                release_batch_note TEXT,
                source_page TEXT,
                trust_tier TEXT
            );
            CREATE TABLE system_reference_values (
                reference_value_id INTEGER PRIMARY KEY,
                source TEXT,
                reference_key TEXT,
                title TEXT,
                row_label TEXT,
                column_label TEXT,
                value_text TEXT,
                numeric_value REAL,
                source_page TEXT,
                game_era TEXT,
                is_legacy_system INTEGER,
                trust_tier TEXT
            );
            """
        )
        connection.execute(
            "INSERT INTO heroes VALUES (1, 'Elesis', '엘리시스', 'Assault', 'SS', 'strategywiki,namuwiki')"
        )
        connection.executemany(
            "INSERT INTO hero_modes VALUES (?, ?, ?, ?)",
            [
                (1, "adventure", "SS", 5),
                (1, "battle", "S", 4),
                (1, "boss", "S", 4),
            ],
        )
        connection.execute("INSERT INTO hero_meta_scores VALUES (1, 4.5, 5.4, 6.21, 1)")
        connection.executemany(
            "INSERT INTO hero_variants VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    10,
                    1,
                    "Elesis",
                    "엘리시스",
                    "base",
                    "",
                    "",
                    "Assault",
                    "SS",
                    "SS",
                    "S",
                    "S",
                    "Elesis (Grand Chase for kakao)",
                    "/w/elesis",
                    "",
                    "namuwiki",
                ),
                (
                    11,
                    1,
                    "Elesis",
                    "엘리시스",
                    "special",
                    "S",
                    "S",
                    "Assault",
                    "SS",
                    "SS",
                    "S",
                    "S",
                    "Elesis (Grand Chase for kakao)/special hero",
                    "/w/elesis-s",
                    "",
                    "namuwiki",
                ),
            ],
        )
        connection.executemany(
            "INSERT INTO variant_meta_scores VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                (10, 1, 4.5, 5.4, 6.21, 1, "inherited_hero_modes"),
                (11, 1, 4.5, 5.4, 6.21, 2, "inherited_hero_modes"),
            ],
        )
        connection.execute(
            "INSERT INTO hero_variant_sections VALUES (100, 10, 2, 's-1', 'Overview', 'Overview', 'General overview text.', 'https://example.com/elesis')"
        )
        connection.execute(
            "INSERT INTO hero_variant_skills VALUES (200, 10, 'Skill', 'base', 'active', 'Critical X', 'Deals damage.')"
        )
        connection.execute(
            "INSERT INTO hero_variant_features VALUES (10, 'chaser', 'Chaser can be opened or grown')"
        )
        connection.execute(
            "INSERT INTO game_system_references VALUES (1, 'namuwiki', 'soul_imprint', 'Soul Imprint', 'growth stage > soul imprint', 'Soul imprint grants engraved skills.', 'https://example.com/namu', 'current_reference', 0, 'community_wiki')"
        )
        connection.execute(
            "INSERT INTO hero_release_history VALUES (1, 'namuwiki', '62nd', 62, 2024, 'Elesis (S)', 'January 23', '2024-01-23', '', 'https://example.com/namu', 'community_wiki')"
        )
        connection.execute(
            "INSERT INTO system_reference_values VALUES (1, 'strategywiki_hero_growth', 'upgrade', 'Upgrade', '3☆', 'Monster Card 1☆', '25%', 25, 'https://example.com/growth', 'legacy_pre_2024', 1, 'community_wiki')"
        )
        connection.commit()

    payload = build_pages_payload(database_path)

    assert payload["summary"]["system_reference_count"] == 1
    assert payload["summary"]["system_reference_value_count"] == 1
    assert payload["summary"]["release_history_count"] == 1
    assert payload["top_heroes"][0]["variant_label"] == "Elesis · Base"
    assert payload["top_heroes"][1]["variant_label"] == "Elesis · Special (S)"
    assert payload["role_summary"][0]["unit_count"] == 2
    assert payload["system_references"][0]["reference_key"] == "soul_imprint"
    assert payload["system_reference_values"][0]["reference_key"] == "upgrade"
    assert payload["release_history"][0]["hero_name_raw"] == "Elesis (S)"
