from bs4 import BeautifulSoup

from grandchase_meta_analyzer.scrapers.strategywiki import (
    _extract_hero_growth_value_rows,
    _iter_reference_sections,
)


def test_iter_reference_sections_collects_legacy_notes() -> None:
    html = """
    <html>
      <body>
        <h2>Resource</h2>
        <p>Shared currencies and item references.</p>
        <h3>Gem</h3>
        <p>Currency required to purchase items or summon Heroes.</p>
        <ul>
          <li>Trail Tower : First Clear Reward</li>
          <li>PVP : Weekly League Reward</li>
        </ul>
      </body>
    </html>
    """
    soup = BeautifulSoup(html, "lxml")

    rows = _iter_reference_sections(
        soup,
        "strategywiki_material",
        "https://example.com/material",
    )
    rows_by_key = {row["reference_key"]: row for row in rows}

    assert "resource" in rows_by_key
    assert "gem" in rows_by_key
    assert rows_by_key["gem"]["section_path"] == "Resource > Gem"
    assert rows_by_key["gem"]["is_legacy_system"] == "1"
    assert rows_by_key["gem"]["trust_tier"] == "community_wiki"


def test_extract_hero_growth_value_rows_flattens_tables() -> None:
    html = """
    <html>
      <body>
        <h2>Upgrade</h2>
        <p>Via Monster Cards.</p>
        <table>
          <tr>
            <th>Hero</th>
            <th>1☆</th>
            <th>2☆</th>
          </tr>
          <tr>
            <td>3☆</td>
            <td>25%</td>
            <td>50%</td>
          </tr>
        </table>
      </body>
    </html>
    """
    soup = BeautifulSoup(html, "lxml")

    rows = _extract_hero_growth_value_rows(
        soup,
        "https://example.com/hero_growth",
    )

    assert len(rows) == 2
    assert rows[0]["reference_key"] == "upgrade"
    assert rows[0]["row_label"] == "3☆"
    assert rows[0]["column_label"] == "1☆"
    assert rows[0]["value_text"] == "25%"
    assert rows[1]["column_label"] == "2☆"


def test_extract_hero_growth_value_rows_uses_second_column_when_first_is_group_label() -> (
    None
):
    html = """
    <html>
      <body>
        <h2>Upgrade</h2>
        <table>
          <tr>
            <th></th>
            <th></th>
            <th>Monster Card 1☆</th>
            <th>Monster Card 2☆</th>
          </tr>
          <tr>
            <td>Hero</td>
            <td>3☆</td>
            <td>25%</td>
            <td>50%</td>
          </tr>
          <tr>
            <td>Hero</td>
            <td>4☆</td>
            <td>10%</td>
            <td>25%</td>
          </tr>
        </table>
      </body>
    </html>
    """
    soup = BeautifulSoup(html, "lxml")

    rows = _extract_hero_growth_value_rows(
        soup,
        "https://example.com/hero_growth",
    )

    assert len(rows) == 4
    assert rows[0]["row_label"] == "3☆"
    assert rows[0]["value_text"] == "25%"
    assert rows[2]["row_label"] == "4☆"
