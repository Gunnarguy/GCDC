from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from pathlib import Path

import pandas as pd


LOGGER = logging.getLogger(__name__)


def write_csv(
    path: Path,
    rows: Sequence[Mapping[str, object]],
    columns: list[str] | None = None,
) -> pd.DataFrame:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame.from_records(list(rows))
    if columns is not None:
        if frame.empty:
            frame = pd.DataFrame(columns=columns)
        else:
            frame = frame.reindex(columns=columns)
    frame.to_csv(path, index=False)
    LOGGER.info("Wrote %s rows to %s", len(frame.index), path)
    return frame


def read_csv(path: Path, columns: list[str] | None = None) -> pd.DataFrame:
    if not path.exists():
        LOGGER.warning("Input file not found: %s", path)
        return pd.DataFrame(columns=columns)
    frame = pd.read_csv(path)
    if columns is not None:
        frame = frame.reindex(columns=columns)
    return frame.fillna("")
