from grandchase_meta_analyzer.scrapers.common import normalize_tier


def test_normalize_tier_maps_s_plus_to_ss() -> None:
    assert normalize_tier("S+") == "SS"


def test_normalize_tier_extracts_letter_grade() -> None:
    assert normalize_tier("Adventure Rank: B") == "B"
