import pandas as pd

from grandchase_meta_analyzer.normalize import (
    build_progression_records,
    compute_meta_scores,
    resolve_hero_identities,
)
from grandchase_meta_analyzer.settings import load_settings


def test_resolve_hero_identities_matches_aliases() -> None:
    strategy_df = pd.DataFrame(
        [
            {
                "name_en": "Ronan",
                "role": "Tank",
                "adventure": "SS",
                "battle": "S",
                "boss": "B",
                "source": "strategywiki",
            }
        ]
    )
    namuwiki_df = pd.DataFrame(
        [
            {
                "name_ko": "로난",
                "name_en_guess": "Ronan",
                "variant_name_en": "Ronan",
                "rarity": "SS",
                "variant_kind": "base",
                "variant_suffix": "",
                "availability_marker": "",
                "variant_title": "Ronan (Grand Chase for kakao)",
                "variant_href": "/w/%EB%A1%9C%EB%82%9C(%EA%B7%B8%EB%9E%9C%EB%93%9C%EC%B2%B4%EC%9D%B4%EC%8A%A4%20for%20kakao)",
                "note_excerpt": "",
                "source": "namuwiki",
            }
        ]
    )

    heroes_df = resolve_hero_identities(strategy_df, namuwiki_df)

    assert len(heroes_df.index) == 1
    assert heroes_df.iloc[0]["name_ko"] == "로난"
    assert heroes_df.iloc[0]["rarity"] == "SS"


def test_resolve_hero_identities_ignores_variant_duplicates() -> None:
    strategy_df = pd.DataFrame(
        [
            {
                "name_en": "Ronan",
                "role": "Tank",
                "adventure": "SS",
                "battle": "S",
                "boss": "B",
                "source": "strategywiki",
            }
        ]
    )
    namuwiki_df = pd.DataFrame(
        [
            {
                "name_ko": "로난",
                "name_en_guess": "Ronan",
                "variant_name_en": "Ronan",
                "rarity": "SS",
                "variant_kind": "base",
                "variant_suffix": "",
                "availability_marker": "",
                "variant_title": "Ronan (Grand Chase for kakao)",
                "variant_href": "/w/base",
                "note_excerpt": "",
                "source": "namuwiki",
            },
            {
                "name_ko": "로난",
                "name_en_guess": "Ronan",
                "variant_name_en": "Ronan",
                "rarity": "SS",
                "variant_kind": "former",
                "variant_suffix": "T",
                "availability_marker": "T",
                "variant_title": "Ronan (Grand Chase for kakao)/former hero",
                "variant_href": "/w/former",
                "note_excerpt": "",
                "source": "namuwiki",
            },
        ]
    )

    heroes_df = resolve_hero_identities(strategy_df, namuwiki_df)

    assert len(heroes_df.index) == 1
    assert heroes_df.iloc[0]["name_en"] == "Ronan"


def test_compute_meta_scores_uses_config_weights() -> None:
    heroes_df = pd.DataFrame(
        [
            {
                "hero_id": 1,
                "name_en": "Ronan",
                "name_ko": "로난",
                "role": "Tank",
                "rarity": "SS",
                "adventure_tier": "SS",
                "battle_tier": "S",
                "boss_tier": "B",
                "sources": "strategywiki,namuwiki",
            }
        ]
    )

    score_df = compute_meta_scores(heroes_df, load_settings())

    assert score_df.iloc[0]["final_meta_score"] == 5.66
    assert score_df.iloc[0]["meta_rank"] == 1


def test_build_progression_records_normalizes_skill_ladders_and_equipment() -> None:
    variants_df = pd.DataFrame(
        [
            {
                "name_en_guess": "Elesis",
                "variant_kind": "base",
                "variant_title": "Elesis (Grand Chase for kakao)",
                "variant_href": "/w/elesis",
            }
        ]
    )
    variant_skills_df = pd.DataFrame(
        [
            {
                "variant_href": "/w/elesis",
                "heading_id": "skill-crit-x",
                "skill_stage": "imprint",
                "skill_type": "active",
                "skill_name": "[Engraving] Critical X",
                "description": "⏰ 15 seconds SP 2 Dungeon Daejeon 〉 [Imprint] Critical X Standard 1/2/3/4/5 The party causes 12%/24%/36%/48%/60% damage for 10 seconds.",
                "source_page": "https://example.com/elesis",
            }
        ]
    )
    variant_sections_df = pd.DataFrame(
        [
            {
                "variant_href": "/w/elesis",
                "heading_id": "gear",
                "heading_title": "dedicated equipment",
                "content": "Sword level physical attack power vitality physical defense magic defense 1 1,040 2,560 540 290 8 3,159 7,813 1,636 893 Encyclopedia Story",
                "source_page": "https://example.com/elesis",
            }
        ]
    )
    variant_features_df = pd.DataFrame(
        [
            {
                "variant_href": "/w/elesis",
                "feature_key": "chaser",
                "feature_value": "Chaser can be opened or grown",
                "source_page": "https://example.com/elesis",
            }
        ]
    )

    records = build_progression_records(
        variants_df,
        variant_sections_df,
        variant_skills_df,
        variant_features_df,
        {"Elesis": 1},
        {"/w/elesis": 10},
    )

    assert len(records["rows"]) == 3
    assert any(row[8] == "imprint" for row in records["rows"])
    assert any(row[8] == "gear" for row in records["rows"])
    assert any(row[8] == "feature" for row in records["rows"])
    assert any(
        track[1] == "Standard" and track[3] == "1" for track in records["tracks"]
    )
    assert any(
        value[1] == "numeric_mention" and value[3] == "12%"
        for value in records["values"]
    )
    assert any(tag[1] == "feature" and tag[2] == "chaser" for tag in records["tags"])
    assert records["equipment"][0][2:] == (1, 1040, 2560, 540, 290)


def test_build_progression_records_captures_explicit_relationships() -> None:
    variants_df = pd.DataFrame(
        [
            {
                "name_en_guess": "Elesis",
                "variant_kind": "base",
                "variant_title": "Elesis (Grand Chase for kakao)",
                "variant_href": "/w/elesis",
            }
        ]
    )
    variant_skills_df = pd.DataFrame(
        [
            {
                "variant_href": "/w/elesis",
                "heading_id": "skill-shield-aura",
                "skill_stage": "base",
                "skill_type": "active",
                "skill_name": "Shield Aura",
                "description": "Shield Aura does not stack with Barrier Song. Overwrites Guardian Oath.",
                "source_page": "https://example.com/elesis",
            }
        ]
    )

    records = build_progression_records(
        variants_df,
        pd.DataFrame(
            columns=[
                "variant_href",
                "heading_id",
                "heading_title",
                "content",
                "source_page",
            ]
        ),
        variant_skills_df,
        pd.DataFrame(
            columns=["variant_href", "feature_key", "feature_value", "source_page"]
        ),
        {"Elesis": 1},
        {"/w/elesis": 10},
    )

    assert len(records["relationships"]) == 2
    assert records["relationships"][0][1] == "does_not_stack_with"
    assert records["relationships"][0][3] == "Barrier Song"
    assert records["relationships"][0][4] == "Barrier Song"
    assert records["relationships"][1][1] == "overwrites"
    assert records["relationships"][1][3] == "Guardian Oath"
