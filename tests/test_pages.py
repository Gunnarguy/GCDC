import pandas as pd

from grandchase_meta_analyzer.pages import (
    _extract_patch_entries_from_sections,
    _summarize_patch_coverage,
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
