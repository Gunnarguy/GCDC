"""Download each sheet from the GCDC meta spreadsheet as individual CSVs."""

import os
import re
import subprocess
import time
import urllib.parse

SPREADSHEET_ID = "1FU4RI2MMvSQkO0k4c4IxgwY-hx2YFKNBIfhsXT4uC-I"
OUT_DIR = "data/raw/gcdc_meta_spreadsheet"

# Sheet names from the Table of Contents
SHEETS = [
    "Table of Contents",
    "Changelog",
    "Beginner's Guide",
    "Assembly",
    "Support Party",
    "Builds",
    "Equipment Presets",
    "Equipment",
    "PvE Meta",
    "PvP Meta",
    "Content Keys",
    "World Boss",
    "World Boss (Season 2)",
    "Final Core",
    "Raids",
    "Aernasis Hammer",
    "Altar of Time",
    "Hell's Furnace: Retribution",
    "Hell's Furnace: Balance",
    "Hell's Furnace: Life",
    "Guild Boss",
    "Berkas' Lair",
    "Soul Imprint",
    "Release Order",
]


def slugify(name: str) -> str:
    s = name.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def download_sheet_csv(sheet_name: str, out_path: str) -> bool:
    encoded = urllib.parse.quote(sheet_name)
    url = (
        f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"
        f"/gviz/tq?tqx=out:csv&sheet={encoded}"
    )
    result = subprocess.run(
        ["curl", "--http1.1", "-L", "-s", "-o", out_path, "-w", "%{http_code}", url],
        capture_output=True,
        text=True,
        timeout=120,
    )
    http_code = result.stdout.strip()
    return http_code == "200" and os.path.getsize(out_path) > 0


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    success = 0
    failed = []

    for i, name in enumerate(SHEETS):
        slug = slugify(name)
        csv_path = os.path.join(OUT_DIR, f"{slug}.csv")
        print(f"  [{i+1:2d}/{len(SHEETS)}] {name:40s} -> ", end="", flush=True)

        ok = download_sheet_csv(name, csv_path)
        if ok:
            size = os.path.getsize(csv_path)
            # Count rows
            with open(csv_path, encoding="utf-8") as f:
                rows = sum(1 for _ in f)
            print(f"OK  ({rows} rows, {size:,} bytes)")
            success += 1
        else:
            print("FAILED")
            failed.append(name)

        time.sleep(0.5)  # be polite to Google

    print(f"\n{success}/{len(SHEETS)} sheets downloaded.")
    if failed:
        print(f"Failed: {failed}")


if __name__ == "__main__":
    main()
