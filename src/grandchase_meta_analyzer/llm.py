from __future__ import annotations

import json
import logging
import re
import sqlite3

import requests

from .paths import PROCESSED_DATA_DIR, RAW_DATA_DIR
from .settings import RuntimeSettings
from .storage import read_csv, write_csv


LOGGER = logging.getLogger(__name__)


def _coerce_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float, str)):
        return int(value)
    raise TypeError(f"Unsupported integer value: {value!r}")


def _coerce_float(value: object) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float, str)):
        return float(value)
    raise TypeError(f"Unsupported float value: {value!r}")


def _completion_endpoint(base_url: str) -> str:
    return f"{base_url.rstrip('/')}/chat/completions"


def _extract_json_blob(content: str) -> dict[str, object]:
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", content, flags=re.DOTALL)
        if not match:
            raise
        return json.loads(match.group(0))


def _tag_single_skill(
    description: str,
    settings: RuntimeSettings,
    allowed_tags: list[str],
) -> dict[str, object]:
    payload = {
        "model": settings.llm_model,
        "temperature": 0.1,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You classify GrandChase skill text. Return strict JSON with keys "
                    "tags (array of strings), confidence (0-1 number), rationale (string). "
                    f"Allowed tags: {', '.join(allowed_tags)}."
                ),
            },
            {"role": "user", "content": description},
        ],
    }
    response = requests.post(
        _completion_endpoint(settings.llm_url or ""),
        json=payload,
        timeout=settings.config["llm"]["request_timeout_seconds"],
    )
    response.raise_for_status()
    content = response.json()["choices"][0]["message"]["content"]
    return _extract_json_blob(content)


def _persist_tags(settings: RuntimeSettings, rows: list[dict[str, object]]) -> None:
    with sqlite3.connect(settings.database_path) as connection:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM skill_tags")
        cursor.executemany(
            "INSERT OR REPLACE INTO skill_tags (skill_id, tag, confidence, rationale, model_name) VALUES (?, ?, ?, ?, ?)",
            [
                (
                    _coerce_int(row["skill_id"]),
                    str(row["tag"]),
                    _coerce_float(row["confidence"]),
                    str(row["rationale"]),
                    str(row["model_name"]),
                )
                for row in rows
            ],
        )
        connection.commit()


def tag_skill_snippets(settings: RuntimeSettings) -> list[dict[str, object]]:
    if not settings.enable_llm_tagging:
        LOGGER.info("Skipping skill tagging because ENABLE_LLM_TAGGING is disabled")
        return []
    if not settings.llm_url or not settings.llm_model:
        raise RuntimeError(
            "LLM tagging is enabled but LOCAL_LLM_URL or LOCAL_LLM_MODEL is missing"
        )

    allowed_tags = settings.config.get("llm", {}).get("default_tags", [])
    skills_df = read_csv(
        RAW_DATA_DIR / "fandom_skills.csv", ["skill_name", "description", "source_page"]
    )
    if skills_df.empty:
        LOGGER.warning("No skill snippets available for LLM tagging")
        return []

    rows: list[dict[str, object]] = []
    for index, skill_row in enumerate(skills_df.to_dict(orient="records"), start=1):
        try:
            result = _tag_single_skill(
                str(skill_row["description"]), settings, allowed_tags
            )
        except Exception as error:  # noqa: BLE001
            LOGGER.warning(
                "Skipping skill %s after LLM failure: %s",
                skill_row["skill_name"],
                error,
            )
            continue

        tags_value = result.get("tags", [])
        tags = tags_value if isinstance(tags_value, list) else []
        confidence = _coerce_float(result.get("confidence", 0.0))
        rationale = str(result.get("rationale", ""))
        for tag in tags:
            rows.append(
                {
                    "skill_id": index,
                    "skill_name": skill_row["skill_name"],
                    "tag": str(tag),
                    "confidence": confidence,
                    "rationale": rationale,
                    "model_name": settings.llm_model,
                }
            )

    write_csv(
        PROCESSED_DATA_DIR / "skill_tags.csv",
        rows,
        ["skill_id", "skill_name", "tag", "confidence", "rationale", "model_name"],
    )
    if rows:
        _persist_tags(settings, rows)
    LOGGER.info("Tagged %s skill-label pairs", len(rows))
    return rows
