from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import cast
from typing import Any

from dotenv import load_dotenv

from .paths import CONFIG_DIR, PROJECT_ROOT


def _as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class RuntimeSettings:
    config: dict[str, Any]
    llm_url: str | None
    llm_model: str | None
    enable_llm_tagging: bool

    @property
    def database_path(self) -> Path:
        return PROJECT_ROOT / self.config["database"]["path"]

    @property
    def scraping(self) -> dict[str, Any]:
        return self.config["scraping"]

    @property
    def scoring(self) -> dict[str, Any]:
        return self.config["meta_scoring"]

    @property
    def explorer(self) -> dict[str, Any]:
        return cast(dict[str, Any], self.config.get("explorer", {}))

    @property
    def explorer_preferred_ports(self) -> list[int]:
        ports = self.explorer.get("preferred_ports", [8506])
        normalized_ports: list[int] = []
        seen_ports: set[int] = set()
        for port in ports:
            normalized_port = int(port)
            if normalized_port in seen_ports:
                continue
            seen_ports.add(normalized_port)
            normalized_ports.append(normalized_port)
        return normalized_ports


def load_settings() -> RuntimeSettings:
    load_dotenv(PROJECT_ROOT / ".env", override=False)
    with (CONFIG_DIR / "config.json").open("r", encoding="utf-8") as handle:
        config = json.load(handle)

    return RuntimeSettings(
        config=config,
        llm_url=os.getenv("LOCAL_LLM_URL"),
        llm_model=os.getenv("LOCAL_LLM_MODEL"),
        enable_llm_tagging=_as_bool(
            os.getenv("ENABLE_LLM_TAGGING"),
            default=bool(config.get("llm", {}).get("enabled", False)),
        ),
    )


def load_aliases() -> dict[str, list[str]]:
    with (CONFIG_DIR / "hero_aliases.json").open("r", encoding="utf-8") as handle:
        alias_payload = json.load(handle)
    return alias_payload["aliases"]
