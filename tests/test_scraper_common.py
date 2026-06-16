from grandchase_meta_analyzer.scrapers.common import normalize_text, normalize_tier


def test_normalize_text_removes_extra_whitespace() -> None:
    assert normalize_text("  hello   world  ") == "hello world"


def test_normalize_text_handles_tabs_and_newlines() -> None:
    assert normalize_text("hello\tworld\n") == "hello world"


def test_normalize_text_handles_empty_string() -> None:
    assert normalize_text("") == ""


def test_normalize_text_handles_normal_string() -> None:
    assert normalize_text("hello world") == "hello world"


def test_normalize_tier_maps_s_plus_to_ss() -> None:
    assert normalize_tier("S+") == "SS"


def test_normalize_tier_extracts_letter_grade() -> None:
    assert normalize_tier("Adventure Rank: B") == "B"
