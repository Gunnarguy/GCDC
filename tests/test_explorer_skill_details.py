from grandchase_meta_analyzer.explorer_skill_details import extract_skill_insight


def test_extract_skill_insight_reads_active_skill_metrics_and_patch_history() -> None:
    text = (
        "⏰ 15 seconds SP 1 Dungeon Daejeon 〉 Sword Lancer "
        "The party members gain a shield equal to 45% of max health and reduce damage by 60% for 10 seconds, "
        "then deal 246% damage and trigger Holly Spear for 7 seconds with 147.6% damage. "
        "Patch Details 【Expand/Collapse】 February 27, 2018 Buff : Add debuffs that increase the magic damage of 25% "
        "January 8, 2019 Buff : SP consumption 2 → 1"
    )

    insight = extract_skill_insight(text)

    assert insight.cooldown_seconds == "15"
    assert insight.sp_cost == "1"
    assert insight.mode_tags == ["Dungeon", "Daejeon"]
    assert insight.durations[:3] == ["15 seconds", "10 seconds", "7 seconds"]
    assert insight.coefficients[:4] == ["45%", "60%", "246%", "147.6%"]
    assert "shield" in insight.mechanic_tags
    assert "damage reduction" in insight.mechanic_tags
    assert "health" in insight.stat_tags
    assert "damage" in insight.stat_tags
    assert insight.target_mentions == []
    assert any(
        mention.category == "shield" and mention.value == "45%"
        for mention in insight.numeric_mentions
    )
    assert any(
        mention.category == "damage reduction" and mention.value == "60%"
        for mention in insight.numeric_mentions
    )
    assert any(
        mention.category == "damage" and mention.value == "246%"
        for mention in insight.numeric_mentions
    )
    assert insight.progression_tracks == []
    assert len(insight.patch_entries) == 2
    assert insight.patch_entries[0].date == "February 27, 2018"
    assert "SP consumption 2 → 1" in insight.patch_entries[1].change


def test_extract_skill_insight_handles_passives_without_cooldown_or_sp() -> None:
    text = (
        "Dungeon Daejeon 〉 Guardian Whenever Ronan receives damage from an enemy, "
        "he reduces damage by 5% for 5 seconds, up to a maximum of 40%. "
        "Summon 1 when damaged and 2 when using skills (up to 8 stacks). "
        "Patch Details 【Expand/Collapse】 July 12, 2022 Others : The description changed"
    )

    insight = extract_skill_insight(text)

    assert insight.cooldown_seconds == ""
    assert insight.sp_cost == ""
    assert insight.mode_tags == ["Dungeon", "Daejeon"]
    assert insight.durations == ["5 seconds"]
    assert insight.coefficients == ["5%", "40%"]
    assert "stacking" in insight.mechanic_tags
    assert insight.stack_mentions == ["up to 8 stacks"]
    assert any(
        "Whenever Ronan receives damage" in clause for clause in insight.trigger_clauses
    )
    assert len(insight.patch_entries) == 1
    assert insight.patch_entries[0].date == "July 12, 2022"


def test_extract_skill_insight_reads_scaling_series_and_stat_bonuses() -> None:
    text = (
        "Dungeon Daejeon 〉 Erudon's sword Reinforcement Level 0/1/2/3 During battle, summons a magic sword. "
        "It causes 450%/526.4%/598.4%/675.2% physical damage and stays open for 15 seconds/20 seconds/25 seconds/30 seconds. "
        "The 40% chance of being hit creates a 3% protective film. "
        "[engrave] Enhancement: Live +12,307, Physical Attack +2,769, Physical Defense +2,153, Magic Defense +2,615"
    )

    insight = extract_skill_insight(text)

    assert "summon" in insight.mechanic_tags
    assert "shield" in insight.mechanic_tags
    assert insight.chance_mentions == ["40% chance"]
    assert len(insight.scaling_series) == 2
    assert insight.scaling_series[0].values == ["450%", "526.4%", "598.4%", "675.2%"]
    assert insight.scaling_series[1].values == [
        "15 seconds",
        "20 seconds",
        "25 seconds",
        "30 seconds",
    ]
    assert insight.stat_bonuses[0].stat in {"Life", "Live"}
    assert insight.stat_bonuses[1].stat == "Physical Attack"
    assert insight.progression_tracks[0].label == "Reinforcement Level"
    assert insight.progression_tracks[0].values == ["0", "1", "2", "3"]


def test_extract_skill_insight_reads_explicit_relationships_only() -> None:
    text = (
        "Dungeon Daejeon 〉 Shield Aura This effect does not stack with Barrier Song. "
        "Overwrites Guardian Oath. Up to 5 stacks."
    )

    insight = extract_skill_insight(text)

    assert len(insight.explicit_relationships) == 2
    assert insight.explicit_relationships[0].relation_type == "does_not_stack_with"
    assert insight.explicit_relationships[0].target_text == "Barrier Song"
    assert insight.explicit_relationships[0].relation_scope == "unspecified"
    assert insight.explicit_relationships[1].relation_type == "overwrites"
    assert insight.explicit_relationships[1].target_text == "Guardian Oath"
    assert insight.stack_mentions == ["Up to 5 stacks"]
