from __future__ import annotations

import re
from dataclasses import dataclass


COOLDOWN_PATTERN = re.compile(r"⏰\s*([0-9]+(?:\.[0-9]+)?)\s*seconds?", re.IGNORECASE)
SP_PATTERN = re.compile(r"\bSP\s*([0-9]+(?:\.[0-9]+)?)\b", re.IGNORECASE)
SP_VALUE_PATTERN = re.compile(
    r"\bSP\s*([0-9]+(?:\.[0-9]+)?)\b|([0-9]+(?:\.[0-9]+)?)\s*SP\b",
    re.IGNORECASE,
)
DURATION_PATTERN = re.compile(r"([0-9]+(?:\.[0-9]+)?\s*seconds?)", re.IGNORECASE)
COEFFICIENT_PATTERN = re.compile(r"([0-9]+(?:\.[0-9]+)?%)")
PATCH_SPLIT_PATTERN = re.compile(r"Patch Details", re.IGNORECASE)
PATCH_DATE_PATTERN = re.compile(
    r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}"
)
STACK_PATTERN = re.compile(
    r"((?:up to(?: a maximum of)?|maximum of|up to)\s*\d+\s*(?:stacks?|times?)|\d+\s*stacks?)",
    re.IGNORECASE,
)
CHANCE_PATTERN = re.compile(r"([0-9]+(?:\.[0-9]+)?%\s*chance)", re.IGNORECASE)
THRESHOLD_PATTERN = re.compile(
    r"((?:less than|below|over|under|more than)\s*[0-9]+(?:\.[0-9]+)?%|up to\s*[0-9]+\s*stacks?)",
    re.IGNORECASE,
)
TARGET_PATTERN = re.compile(
    r"(\b\d+\s+(?:enemy|enemies|ally|allies|party members?|heroes?)\b)",
    re.IGNORECASE,
)
SCALING_SERIES_PATTERN = re.compile(
    r"([0-9]+(?:\.[0-9]+)?%(?:\s*/\s*[0-9]+(?:\.[0-9]+)?%){2,}|[0-9]+(?:\.[0-9]+)?\s*seconds?(?:\s*/\s*[0-9]+(?:\.[0-9]+)?\s*seconds?){2,})",
    re.IGNORECASE,
)
STAT_BONUS_PATTERN = re.compile(
    r"(Life|Live|Physical Attack|Magic Attack|Physical Defense|Magic Defense|Vitality|Attack Speed|Critical Hit Chance|Critical Hit Damage|Skill Damage|Basic Damage)\s*\+\s*([0-9,]+(?:\.[0-9]+)?)",
    re.IGNORECASE,
)
MODE_KEYWORDS = ("Dungeon", "Daejeon", "Raid", "Adventure", "PvP")
TRIGGER_KEYWORDS = (
    "whenever",
    "when ",
    "every time",
    "during battle",
    "when using",
    "at the same time",
    "if ",
    "upon",
)
SP_GAIN_VERB_PATTERN = re.compile(
    r"\b(?:gain(?:s|ed)?|generate(?:s|d)?|obtain(?:s|ed)?|acquire(?:s|d)?|recover(?:s|ed)?|restore(?:s|d)?|get(?:s|ting|ted)?)\b",
    re.IGNORECASE,
)
SP_GAIN_CONTEXT_PATTERN = re.compile(
    r"(?:gain(?:s|ed)?|generate(?:s|d)?|obtain(?:s|ed)?|acquire(?:s|d)?|get(?:s|ting|ted)?|recover(?:s|ed)?|restore(?:s|d)?)\b.{0,24}\bSP\b|\bSP\b.{0,24}(?:is|are|was|were)?\s*(?:acquired|obtained|generated|gained|recovered|restored)|\bSP\b.{0,24}(?:per second|every second)|automatic acquisition|automatically acquires|continuously acquired",
    re.IGNORECASE,
)
SP_FREE_CAST_PATTERN = re.compile(r"(?:without|no)\s+SP\s+consumption", re.IGNORECASE)
SP_COST_DISCOUNT_VERB_PATTERN = re.compile(
    r"\b(?:reduce(?:s|d)?|decrease(?:s|d)?|lower(?:s|ed)?)\b",
    re.IGNORECASE,
)
MECHANIC_TAG_PATTERNS = {
    "shield": re.compile(r"shield|barrier|protective film", re.IGNORECASE),
    "invincibility": re.compile(
        r"invincible|invincibility|immune to enemy damage",
        re.IGNORECASE,
    ),
    "summon": re.compile(r"summon|totem|magic sword|tempest blade", re.IGNORECASE),
    "damage reduction": re.compile(
        r"reduce(?:s|d)?(?: the)?(?: amount of)? damage|damage reduction|reduction effect",
        re.IGNORECASE,
    ),
    "healing": re.compile(r"recover|restor|healing", re.IGNORECASE),
    "healing reduction": re.compile(r"life recovery", re.IGNORECASE),
    "attack speed control": re.compile(r"attack speed", re.IGNORECASE),
    "cooldown control": re.compile(
        r"skill cooldown|skill reuse|cool time", re.IGNORECASE
    ),
    "cleanse": re.compile(r"harmful effects|cancel harmful effects", re.IGNORECASE),
    "resurrection": re.compile(r"resurrection|revive", re.IGNORECASE),
    "stacking": re.compile(r"stack", re.IGNORECASE),
}
STAT_TAG_PATTERNS = {
    "health": re.compile(r"max(?:imum)? health|health|life", re.IGNORECASE),
    "physical attack": re.compile(r"physical attack", re.IGNORECASE),
    "magic attack": re.compile(r"magic attack", re.IGNORECASE),
    "physical defense": re.compile(r"physical defense", re.IGNORECASE),
    "magic defense": re.compile(r"magic defense", re.IGNORECASE),
    "attack speed": re.compile(r"attack speed", re.IGNORECASE),
    "healing": re.compile(r"healing|recover|restore", re.IGNORECASE),
    "damage": re.compile(r"damage", re.IGNORECASE),
}
RELATION_PATTERN_DEFINITIONS = (
    (
        "does_not_stack_with",
        re.compile(
            r"(?:does not stack with|do not stack with|cannot stack with|can't stack with)\s+(?P<target>[^.;:()\]]+)",
            re.IGNORECASE,
        ),
    ),
    (
        "does_not_overlap_with",
        re.compile(
            r"(?:does not overlap with|do not overlap with|cannot overlap with|can't overlap with)\s+(?P<target>[^.;:()\]]+)",
            re.IGNORECASE,
        ),
    ),
    (
        "mutually_exclusive_with",
        re.compile(
            r"(?:mutually exclusive with|exclusive with)\s+(?P<target>[^.;:()\]]+)",
            re.IGNORECASE,
        ),
    ),
    (
        "overwrites",
        re.compile(
            r"(?:overwrites?|overrides?)\s+(?P<target>[^.;:()\]]+)",
            re.IGNORECASE,
        ),
    ),
    (
        "overwritten_by",
        re.compile(
            r"(?:is overwritten by|is overridden by|overridden by)\s+(?P<target>[^.;:()\]]+)",
            re.IGNORECASE,
        ),
    ),
)


@dataclass(frozen=True)
class PatchEntry:
    date: str
    change: str


@dataclass(frozen=True)
class NumericMention:
    value: str
    category: str
    context: str


@dataclass(frozen=True)
class ScalingSeries:
    label: str
    values: list[str]
    category: str
    context: str


@dataclass(frozen=True)
class StatBonus:
    stat: str
    value: str


@dataclass(frozen=True)
class ProgressionTrack:
    label: str
    values: list[str]
    context: str


@dataclass(frozen=True)
class EconomyMention:
    category: str
    value_text: str
    numeric_value: float | None
    unit: str
    context: str


@dataclass(frozen=True)
class ExplicitRelationship:
    relation_type: str
    relation_scope: str
    target_text: str
    evidence_text: str


@dataclass(frozen=True)
class SkillInsight:
    body_text: str
    cooldown_seconds: str
    sp_cost: str
    durations: list[str]
    coefficients: list[str]
    mode_tags: list[str]
    patch_entries: list[PatchEntry]
    mechanic_tags: list[str]
    stat_tags: list[str]
    trigger_clauses: list[str]
    stack_mentions: list[str]
    chance_mentions: list[str]
    threshold_mentions: list[str]
    target_mentions: list[str]
    numeric_mentions: list[NumericMention]
    scaling_series: list[ScalingSeries]
    stat_bonuses: list[StatBonus]
    progression_tracks: list[ProgressionTrack]
    economy_mentions: list[EconomyMention]
    explicit_relationships: list[ExplicitRelationship]


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _unique_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    unique_items: list[str] = []
    for item in items:
        lowered = item.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        unique_items.append(item)
    return unique_items


def _context_slice(text: str, start: int, end: int, radius: int = 56) -> str:
    slice_start = max(0, start - radius)
    slice_end = min(len(text), end + radius)
    return _normalize_text(text[slice_start:slice_end])


def _unique_dataclasses(items: list[object]) -> list[object]:
    seen: set[str] = set()
    unique_items: list[object] = []
    for item in items:
        marker = repr(item)
        if marker in seen:
            continue
        seen.add(marker)
        unique_items.append(item)
    return unique_items


def _classify_numeric_context(context: str) -> str:
    lowered = context.lower()
    if "chance" in lowered:
        return "proc chance"
    if "shield" in lowered or "protective film" in lowered:
        return "shield"
    if "life recovery" in lowered:
        return "healing reduction"
    if "reduce" in lowered and "damage" in lowered or "reduction effect" in lowered:
        return "damage reduction"
    if "recover" in lowered or "restor" in lowered or "healing" in lowered:
        return "healing"
    if "attack speed" in lowered:
        return "attack speed"
    if "magic attack" in lowered:
        return "magic attack"
    if "physical attack" in lowered:
        return "physical attack"
    if "magic defense" in lowered:
        return "magic defense"
    if "physical defense" in lowered:
        return "physical defense"
    if "health" in lowered or "life" in lowered:
        return "health"
    if "damage" in lowered:
        return "damage"
    return "other"


def _extract_keyword_tags(text: str, patterns: dict[str, re.Pattern[str]]) -> list[str]:
    return [label for label, pattern in patterns.items() if pattern.search(text)]


def _extract_trigger_clauses(text: str) -> list[str]:
    normalized = _normalize_text(text)
    clauses = re.split(r"(?<=[.!?])\s+|\s*\([^)]*\)\s*", normalized)
    filtered = [
        clause
        for clause in clauses
        if clause and any(keyword in clause.lower() for keyword in TRIGGER_KEYWORDS)
    ]
    return _unique_preserve_order(filtered)


def _extract_stat_bonuses(text: str) -> list[StatBonus]:
    bonuses = [
        StatBonus(
            stat=_normalize_text(match.group(1)),
            value=match.group(2).rstrip(","),
        )
        for match in STAT_BONUS_PATTERN.finditer(text)
    ]
    return _unique_dataclasses(bonuses)


def _extract_numeric_mentions(text: str) -> list[NumericMention]:
    mentions: list[NumericMention] = []
    for match in COEFFICIENT_PATTERN.finditer(text):
        context = _context_slice(text, match.start(), match.end(), radius=28)
        mentions.append(
            NumericMention(
                value=match.group(1),
                category=_classify_numeric_context(context),
                context=context,
            )
        )
    return _unique_dataclasses(mentions)


def _extract_scaling_series(text: str) -> list[ScalingSeries]:
    series_rows: list[ScalingSeries] = []
    for match in SCALING_SERIES_PATTERN.finditer(text):
        context = _context_slice(text, match.start(), match.end())
        values = [
            _normalize_text(value) for value in re.split(r"\s*/\s*", match.group(1))
        ]
        category = _classify_numeric_context(context)
        label = (
            f"{category.title()} scaling" if category != "other" else "Value scaling"
        )
        series_rows.append(
            ScalingSeries(
                label=label,
                values=values,
                category=category,
                context=context,
            )
        )
    return _unique_dataclasses(series_rows)


def _extract_progression_tracks(text: str) -> list[ProgressionTrack]:
    patterns = [
        re.compile(r"(Reinforcement Level)\s*([0-9]+(?:/[0-9]+)+)", re.IGNORECASE),
        re.compile(r"(Advent stage)\s*([0-9]+(?:/[0-9]+)+)", re.IGNORECASE),
        re.compile(r"(Traffic Standard)\s*([0-9]+(?:/[0-9]+)+)", re.IGNORECASE),
        re.compile(
            r"(Standards of the enhancement level of the soul)\s*,?\s*([0-9]+(?:/[0-9]+)+)",
            re.IGNORECASE,
        ),
        re.compile(r"(Standard)\s*([0-9]+(?:/[0-9]+)+)", re.IGNORECASE),
    ]
    tracks: list[ProgressionTrack] = []
    for pattern in patterns:
        for match in pattern.finditer(text):
            tracks.append(
                ProgressionTrack(
                    label=_normalize_text(match.group(1)),
                    values=[
                        _normalize_text(value) for value in match.group(2).split("/")
                    ],
                    context=_context_slice(text, match.start(), match.end(), radius=36),
                )
            )
    return _unique_dataclasses(tracks)


def _sp_numeric_value(match: re.Match[str]) -> float:
    return float(next(group for group in match.groups() if group is not None))


def _sp_value_text(match: re.Match[str], *, is_rate: bool) -> str:
    numeric_text = next(group for group in match.groups() if group is not None)
    return f"{numeric_text} SP/s" if is_rate else f"{numeric_text} SP"


def _extract_economy_mentions(text: str) -> list[EconomyMention]:
    normalized = _normalize_text(text)
    if not normalized:
        return []

    clauses = re.split(r"(?<=[.!?;])\s+", normalized)
    if not clauses:
        clauses = [normalized]

    mentions: list[EconomyMention] = []
    for clause in clauses:
        lowered = clause.lower()
        if "sp" not in lowered:
            continue

        if SP_FREE_CAST_PATTERN.search(clause):
            mentions.append(
                EconomyMention(
                    category="sp_free_cast",
                    value_text="Without SP consumption",
                    numeric_value=None,
                    unit="clause",
                    context=_normalize_text(clause),
                )
            )

        gain_clause = SP_GAIN_VERB_PATTERN.search(clause) is not None or any(
            marker in lowered
            for marker in (
                "is acquired",
                "is obtained",
                "is restored",
                "is recovered",
                "is generated",
                "automatic acquisition",
                "automatically acquires",
                "continuously acquired",
            )
        )
        if gain_clause:
            for match in SP_VALUE_PATTERN.finditer(clause):
                match_context = _context_slice(
                    clause, match.start(), match.end(), radius=48
                ).lower()
                local_gain_clause = SP_GAIN_CONTEXT_PATTERN.search(match_context)
                is_rate = (
                    "per second" in match_context or "every second" in match_context
                )
                if not local_gain_clause and not is_rate:
                    continue
                mentions.append(
                    EconomyMention(
                        category="sp_gain_rate" if is_rate else "sp_gain",
                        value_text=_sp_value_text(match, is_rate=is_rate),
                        numeric_value=_sp_numeric_value(match),
                        unit="sp_per_second" if is_rate else "sp",
                        context=_normalize_text(clause),
                    )
                )

        if "sp consumption" in lowered and "enemy" not in lowered:
            for match in COEFFICIENT_PATTERN.finditer(clause):
                if SP_COST_DISCOUNT_VERB_PATTERN.search(clause) is None:
                    continue
                mentions.append(
                    EconomyMention(
                        category="sp_cost_discount",
                        value_text=match.group(1),
                        numeric_value=float(match.group(1).rstrip("%")),
                        unit="percent",
                        context=_normalize_text(clause),
                    )
                )

    return _unique_dataclasses(mentions)


def _clean_relation_target(target_text: str) -> str:
    cleaned = _normalize_text(target_text).strip(" .,:;\"'")
    cleaned = re.sub(
        r"^(?:the effect of|effect of|the skill|skill|the buff|buff|the debuff|debuff)\s+",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    return cleaned.strip(" .,:;\"'")


def _detect_relation_scope(evidence_text: str) -> str:
    lowered = evidence_text.lower()
    if any(keyword in lowered for keyword in ("party", "ally", "allies", "team")):
        return "party"
    if any(
        keyword in lowered
        for keyword in ("same skill", "this skill", "own skill", "self")
    ):
        return "self"
    return "unspecified"


def _extract_explicit_relationships(text: str) -> list[ExplicitRelationship]:
    normalized = _normalize_text(text)
    if not normalized:
        return []
    clauses = re.split(r"(?<=[.!?;])\s+", normalized)
    if not clauses:
        clauses = [normalized]
    relationships: list[ExplicitRelationship] = []
    for clause in clauses:
        if not clause:
            continue
        for relation_type, pattern in RELATION_PATTERN_DEFINITIONS:
            for match in pattern.finditer(clause):
                target_text = _clean_relation_target(match.group("target"))
                if not target_text:
                    continue
                relationships.append(
                    ExplicitRelationship(
                        relation_type=relation_type,
                        relation_scope=_detect_relation_scope(clause),
                        target_text=target_text,
                        evidence_text=_normalize_text(clause),
                    )
                )
    return _unique_dataclasses(relationships)


def split_patch_details(text: str) -> tuple[str, str]:
    normalized = _normalize_text(text)
    match = PATCH_SPLIT_PATTERN.search(normalized)
    if match is None:
        return normalized, ""
    body_text = normalized[: match.start()].strip()
    patch_text = normalized[match.end() :].replace("【Expand/Collapse】", "").strip()
    return body_text, patch_text


def parse_patch_entries(text: str) -> list[PatchEntry]:
    normalized = _normalize_text(text)
    if not normalized:
        return []

    matches = list(PATCH_DATE_PATTERN.finditer(normalized))
    if not matches:
        return [PatchEntry(date="", change=normalized)]

    entries: list[PatchEntry] = []
    for index, match in enumerate(matches):
        start = match.end()
        end = (
            matches[index + 1].start() if index + 1 < len(matches) else len(normalized)
        )
        change = normalized[start:end].strip(" :-")
        entries.append(PatchEntry(date=match.group(0), change=change))
    return entries


def extract_skill_insight(text: str) -> SkillInsight:
    body_text, patch_text = split_patch_details(text)
    cooldown_match = COOLDOWN_PATTERN.search(body_text)
    sp_match = SP_PATTERN.search(body_text)
    durations = _unique_preserve_order(
        [match.group(1) for match in DURATION_PATTERN.finditer(body_text)]
    )
    coefficients = _unique_preserve_order(
        [match.group(1) for match in COEFFICIENT_PATTERN.finditer(body_text)]
    )
    mode_tags = [
        keyword for keyword in MODE_KEYWORDS if re.search(rf"\b{keyword}\b", body_text)
    ]
    patch_entries = parse_patch_entries(patch_text)
    mechanic_tags = _extract_keyword_tags(body_text, MECHANIC_TAG_PATTERNS)
    stat_tags = _extract_keyword_tags(body_text, STAT_TAG_PATTERNS)
    trigger_clauses = _extract_trigger_clauses(body_text)
    stack_mentions = _unique_preserve_order(STACK_PATTERN.findall(body_text))
    chance_mentions = _unique_preserve_order(CHANCE_PATTERN.findall(body_text))
    threshold_mentions = _unique_preserve_order(THRESHOLD_PATTERN.findall(body_text))
    target_mentions = _unique_preserve_order(TARGET_PATTERN.findall(body_text))
    numeric_mentions = _extract_numeric_mentions(body_text)
    scaling_series = _extract_scaling_series(body_text)
    stat_bonuses = _extract_stat_bonuses(body_text)
    progression_tracks = _extract_progression_tracks(body_text)
    economy_mentions = _extract_economy_mentions(body_text)
    explicit_relationships = _extract_explicit_relationships(body_text)
    return SkillInsight(
        body_text=body_text,
        cooldown_seconds=cooldown_match.group(1) if cooldown_match else "",
        sp_cost=sp_match.group(1) if sp_match else "",
        durations=durations,
        coefficients=coefficients,
        mode_tags=mode_tags,
        patch_entries=patch_entries,
        mechanic_tags=mechanic_tags,
        stat_tags=stat_tags,
        trigger_clauses=trigger_clauses,
        stack_mentions=stack_mentions,
        chance_mentions=chance_mentions,
        threshold_mentions=threshold_mentions,
        target_mentions=target_mentions,
        numeric_mentions=numeric_mentions,
        scaling_series=scaling_series,
        stat_bonuses=stat_bonuses,
        progression_tracks=progression_tracks,
        economy_mentions=economy_mentions,
        explicit_relationships=explicit_relationships,
    )
