import pandas as pd

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


def test_build_default_team_variant_ids_prefers_role_spread() -> None:
    variant_leaderboard_df = pd.DataFrame(
        [
            {"variant_id": 10, "name_en": "Amy", "role": "Healer", "meta_rank": 1},
            {"variant_id": 11, "name_en": "Amy", "role": "Assault", "meta_rank": 2},
            {"variant_id": 12, "name_en": "Jin", "role": "Tank", "meta_rank": 3},
            {"variant_id": 13, "name_en": "Ley", "role": "Mage", "meta_rank": 4},
            {"variant_id": 14, "name_en": "Lire", "role": "Ranger", "meta_rank": 5},
        ]
    )

    selected_ids = build_default_team_variant_ids(variant_leaderboard_df, size=4)

    assert selected_ids == [10, 12, 13, 14]


def test_team_analysis_builders_surface_sp_and_defense_rows() -> None:
    variant_roster_df = pd.DataFrame(
        [
            {
                "variant_id": 1,
                "variant_label": "Amy · Base",
                "name_en": "Amy",
                "role": "Healer",
                "rarity": "SS",
                "meta_rank": 1,
                "final_meta_score": 6.9,
            },
            {
                "variant_id": 2,
                "variant_label": "Ronan · Base",
                "name_en": "Ronan",
                "role": "Tank",
                "rarity": "SS",
                "meta_rank": 20,
                "final_meta_score": 5.38,
            },
        ]
    )
    skills_df = pd.DataFrame(
        [
            {
                "name_en": "Amy",
                "variant_id": 1,
                "variant_label": "Amy · Base",
                "variant_kind_label": "Base",
                "skill_stage": "base",
                "skill_type": "active",
                "skill_name": "Loving You",
                "description": (
                    "⏰ 15 seconds SP 1 The party members restore vitality by 249.9% of magic attack power for 10 seconds, "
                    "and SP 0.1 is obtained whenever a critical hit occurs. Holly Dance can be used without SP consumption."
                ),
            },
            {
                "name_en": "Ronan",
                "variant_id": 2,
                "variant_label": "Ronan · Base",
                "variant_kind_label": "Base",
                "skill_stage": "base",
                "skill_type": "active",
                "skill_name": "Tempest Barrier",
                "description": (
                    "⏰ 20 seconds SP 1 The party members are granted a shield equal to 45% of maximum health and reduce damage by 60% for 10 seconds."
                ),
            },
        ]
    )
    sections_df = pd.DataFrame(
        [
            {
                "name_en": "Amy",
                "variant_id": 1,
                "variant_label": "Amy · Base",
                "variant_kind_label": "Base",
                "heading_title": "Soul Imprint: Superstar Amy",
                "content": (
                    "Gains 0.5 SP during basic attack. The party hero acquires a shield of 20% of maximum vitality for 5 seconds."
                ),
            },
            {
                "name_en": "Ronan",
                "variant_id": 2,
                "variant_label": "Ronan · Base",
                "variant_kind_label": "Base",
                "heading_title": "Soul Imprint: Savior Ronan",
                "content": "It instantly cancels harmful effects on party members and makes them invincible for 3 seconds.",
            },
        ]
    )
    features_df = pd.DataFrame(
        [
            {
                "name_en": "Amy",
                "variant_id": 1,
                "variant_label": "Amy · Base",
                "variant_kind_label": "Base",
                "feature_key": "soul_imprint",
                "feature_value": "The recovered party hero reduces SP consumption by 30% for 10 seconds.",
            }
        ]
    )

    team_sources_df = build_team_source_frame(
        skills_df, sections_df, features_df, [1, 2]
    )
    sp_evidence_df = build_team_sp_evidence_frame(team_sources_df)
    sp_summary_df = build_team_sp_summary(team_sources_df)
    cost_df = build_team_skill_cost_frame(team_sources_df)
    defense_evidence_df = build_team_defense_evidence_frame(team_sources_df)
    defense_summary_df = build_team_defense_summary(defense_evidence_df)
    member_snapshot_df = build_team_member_snapshot(variant_roster_df, team_sources_df)

    assert len(team_sources_df.index) == 5
    assert set(sp_evidence_df["economy_type"]) == {
        "Flat SP Gains",
        "SP Cost Discounts",
        "Free Cast Clauses",
    }
    assert (
        sp_summary_df.loc[
            sp_summary_df["economy_type"] == "Flat SP Gains", "captured_total"
        ].iloc[0]
        == "0.60 SP"
    )
    assert (
        sp_summary_df.loc[
            sp_summary_df["economy_type"] == "SP Cost Discounts", "best_value"
        ].iloc[0]
        == "30%"
    )
    assert len(cost_df.index) == 2
    assert set(defense_evidence_df["defense_type"]) == {
        "Shield",
        "Damage Reduction",
        "Healing",
        "Invincibility",
        "Cleanse",
    }
    assert (
        defense_summary_df.loc[
            defense_summary_df["defense_type"] == "Shield", "strongest_value"
        ].iloc[0]
        == "45%"
    )
    amy_row = member_snapshot_df[
        member_snapshot_df["variant_label"] == "Amy · Base"
    ].iloc[0]
    ronan_row = member_snapshot_df[
        member_snapshot_df["variant_label"] == "Ronan · Base"
    ].iloc[0]
    assert amy_row["flat_sp_gain"] == 0.6
    assert amy_row["free_cast_clauses"] == 1
    assert ronan_row["shield_sources"] == 1
    assert ronan_row["damage_reduction_sources"] == 1
    assert ronan_row["invincibility_sources"] == 1
