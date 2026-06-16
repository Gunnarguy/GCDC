from grandchase_meta_analyzer.scrapers.common import dedupe_rows, normalize_tier


def test_normalize_tier_maps_s_plus_to_ss() -> None:
    assert normalize_tier("S+") == "SS"


def test_normalize_tier_extracts_letter_grade() -> None:
    assert normalize_tier("Adventure Rank: B") == "B"


def test_dedupe_rows_no_duplicates() -> None:
    rows = [{"a": 1}, {"a": 2}]
    assert dedupe_rows(rows, ("a",)) == [{"a": 1}, {"a": 2}]


def test_dedupe_rows_single_key() -> None:
    rows = [{"a": 1, "b": 2}, {"a": 1, "b": 3}]
    assert dedupe_rows(rows, ("a",)) == [{"a": 1, "b": 2}]


def test_dedupe_rows_multi_key() -> None:
    rows = [{"a": 1, "b": 2}, {"a": 1, "b": 2}, {"a": 1, "b": 3}]
    assert dedupe_rows(rows, ("a", "b")) == [{"a": 1, "b": 2}, {"a": 1, "b": 3}]


def test_dedupe_rows_empty_input() -> None:
    assert dedupe_rows([], ("a",)) == []


def test_dedupe_rows_missing_keys() -> None:
    rows = [{"a": 1}, {"b": 2}, {"c": 3}]
    # The first row has missing 'b', marker is (None,)
    # The second row has missing 'b' mapped to 2, marker is (2,)
    # The third row has missing 'b', marker is (None,)
    assert dedupe_rows(rows, ("b",)) == [{"a": 1}, {"b": 2}]
