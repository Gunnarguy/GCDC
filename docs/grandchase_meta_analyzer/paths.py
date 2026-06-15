from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
LOG_DIR = PROJECT_ROOT / "logs"
NOTEBOOK_DIR = PROJECT_ROOT / "notebooks"


def ensure_runtime_directories() -> None:
    for directory in (
        CONFIG_DIR,
        RAW_DATA_DIR,
        PROCESSED_DATA_DIR,
        LOG_DIR,
        NOTEBOOK_DIR,
    ):
        directory.mkdir(parents=True, exist_ok=True)
