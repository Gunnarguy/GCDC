from bs4 import BeautifulSoup

from grandchase_meta_analyzer.scrapers.namuwiki import (
    _extract_variant_page_features,
    _extract_variant_page_sections,
    _extract_variant_page_skills,
    _extract_source_notes,
    _extract_variant_rows,
)


def test_extract_variant_rows_recognizes_base_former_and_special() -> None:
    html = """
    <html>
      <body>
        <div style='width:calc(100% / 6)'>
          <a href='/w/%EB%A1%9C%EB%82%9C(%EA%B7%B8%EB%9E%9C%EB%93%9C%EC%B2%B4%EC%9D%B4%EC%8A%A4%20for%20kakao)' title='Ronan (Grand Chase for kakao)'>Ronan</a>
          <sup>4</sup>
        </div>
        <div style='width:calc(100% / 6)'>
          <a href='/w/%EB%A1%9C%EB%82%9C(%EA%B7%B8%EB%9E%9C%EB%93%9C%EC%B2%B4%EC%9D%B4%EC%8A%A4%20for%20kakao)/%EC%A0%84%EC%A7%81%20%EC%98%81%EC%9B%85' title='Ronan (Grand Chase for kakao)/former hero'>Ronan</a>
          <sup>(T)</sup>
        </div>
        <div style='width:calc(100% / 6)'>
          <a href='/w/%EC%97%98%EB%A6%AC%EC%8B%9C%EC%8A%A4(%EA%B7%B8%EB%9E%9C%EB%93%9C%EC%B2%B4%EC%9D%B4%EC%8A%A4%20for%20kakao)/%EC%8A%A4%ED%8E%98%EC%85%9C%20%EC%98%81%EC%9B%85' title='Elesis (Grand Chase for Kakao)/Special Hero'>Elesis</a>
          <sup>(S)</sup>
        </div>
      </body>
    </html>
    """
    soup = BeautifulSoup(html, "lxml")
    alias_map = {"Ronan": ["로난"], "Elesis": ["엘리시스"]}

    rows = _extract_variant_rows(soup, alias_map)

    assert len(rows) == 3
    assert rows[0]["variant_kind"] == "base"
    assert rows[0]["availability_marker"] == "4"
    assert rows[1]["variant_kind"] == "former"
    assert rows[1]["variant_suffix"] == "T"
    assert rows[2]["variant_kind"] == "special"
    assert rows[2]["variant_suffix"] == "S"


def test_extract_variant_page_skills_collects_base_and_imprint_rows() -> None:
    html = """
    <html>
      <body>
        <h2><a id='s-5' href='#toc'>5.</a> <span>skill</span></h2>
        <h3><a id='s-5.1' href='#toc'>5.1.</a> <span>Passive - Guardian</span></h3>
        <div class='wiki-paragraph'>Protect party allies.</div>
        <h3><a id='s-5.2' href='#toc'>5.2.</a> <span>Skill 1 - Sword Lancer</span></h3>
        <div class='wiki-paragraph'>Deal basic damage.</div>
        <h4><a id='s-5.5.1' href='#toc'>5.5.1.</a> <span>[Enhancement I] Chaser: Blade Beam</span></h4>
        <div class='wiki-paragraph'>Enhance chaser damage.</div>
        <h2><a id='s-6' href='#toc'>6.</a> <span>Soul Imprint: Savior Ronan</span></h2>
        <h3><a id='s-6.1' href='#toc'>6.1.</a> <span>Passive - imprint of memory</span></h3>
        <div class='wiki-paragraph'>Gain engraving power.</div>
      </body>
    </html>
    """
    soup = BeautifulSoup(html, "lxml")
    variant = {
        "variant_href": "/w/ronan",
        "name_en_guess": "Ronan",
        "variant_kind": "base",
    }

    rows = _extract_variant_page_skills(soup, variant, "https://example.com/ronan")

    assert len(rows) == 4
    assert rows[0]["section_key"] == "skill"
    assert rows[0]["skill_type"] == "passive"
    assert rows[1]["skill_type"] == "active"
    assert rows[2]["skill_stage"] == "enhancement_i"
    assert rows[3]["section_key"] == "soul_imprint"
    assert rows[3]["skill_stage"] == "imprint"


def test_extract_variant_page_sections_collects_heading_blocks() -> None:
    html = """
    <html>
      <body>
        <h2><a id='s-5' href='#toc'>5.</a> <span>Skill</span></h2>
        <h3><a id='s-5.2' href='#toc'>5.2.</a> <span>1 Skill - Sword Lancer</span></h3>
        <div class='wiki-paragraph'>active</div>
        <div class='wiki-paragraph'>⏰ 15 seconds</div>
        <div class='wiki-paragraph'>SP 1</div>
        <dl><dt>Patch Details</dt><dd><div>Buff: SP consumption 2 → 1</div></dd></dl>
      </body>
    </html>
    """
    soup = BeautifulSoup(html, "lxml")
    variant = {
        "variant_href": "/w/ronan",
        "name_en_guess": "Ronan",
        "variant_kind": "base",
    }

    rows = _extract_variant_page_sections(soup, variant, "https://example.com/ronan")

    assert len(rows) == 1
    assert rows[0]["heading_title"] == "1 Skill - Sword Lancer"
    assert rows[0]["section_path"] == "Skill > 1 Skill - Sword Lancer"
    assert "15 seconds" in rows[0]["content"]
    assert "Patch Details" in rows[0]["content"]


def test_extract_variant_page_features_collects_page_capabilities() -> None:
    html = """
    <html>
      <body>
        <div class='wiki-paragraph'>Characteristic selectable</div>
        <div class='wiki-paragraph'>Chaser can grow</div>
        <div class='wiki-paragraph'>transcendence awakening possible</div>
        <div class='wiki-paragraph'>Soul imprint, remaining points available</div>
      </body>
    </html>
    """
    soup = BeautifulSoup(html, "lxml")
    variant = {"variant_href": "/w/ronan"}

    rows = _extract_variant_page_features(soup, variant, "https://example.com/ronan")
    features = {row["feature_key"]: row["feature_value"] for row in rows}

    assert features["characteristics"] == "Characteristic selectable"
    assert features["chaser"] == "Chaser can grow"
    assert features["transcendental_awakening"] == "transcendence awakening possible"
    assert features["soul_imprint"] == "Soul imprint, remaining points available"


def test_extract_source_notes_collects_legend_and_variant_explanations() -> None:
    html = """
    <html>
      <body>
        <div>
          Number: Heroes you can obtain by clearing Adventure Acts 1-5
          S: Event limited edition special hero
          T: former hero
          X: A hero who cannot grow
        </div>
        <h4>2.4.1. ex</h4>
        <div>Former heroes are derived from SS heroes and use the T marker.</div>
        <h4>2.7.1. special hero</h4>
        <div>Special heroes are event-limited and use the S marker.</div>
      </body>
    </html>
    """
    soup = BeautifulSoup(html, "lxml")

    notes = _extract_source_notes(soup, "https://example.com/namu")
    notes_by_key = {note["note_key"]: note for note in notes}

    assert "legend" in notes_by_key
    assert "former_hero" in notes_by_key
    assert "special_hero" in notes_by_key
