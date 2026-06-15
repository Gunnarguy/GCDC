from __future__ import annotations

import argparse
import json
import logging
from typing import Sequence

from .explorer import launch_explorer
from .llm import tag_skill_snippets
from .pages import export_pages_site
from .pipeline import configure_logging, run_normalize, run_pipeline, run_scrape
from .settings import load_settings


LOGGER = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="GrandChase meta analyzer CLI")
    parser.add_argument("--log-level", default="INFO", help="Python logging level")

    subparsers = parser.add_subparsers(dest="command", required=True)

    scrape_parser = subparsers.add_parser(
        "scrape", help="Scrape supported community sources"
    )
    scrape_parser.add_argument(
        "--source",
        choices=["all", "strategywiki", "namuwiki", "fandom"],
        default="all",
        help="Restrict scraping to one source",
    )

    subparsers.add_parser(
        "normalize", help="Normalize raw source files and rebuild SQLite"
    )
    subparsers.add_parser(
        "pipeline", help="Run scraping, normalization, and optional LLM tagging"
    )
    explorer_parser = subparsers.add_parser(
        "explorer", help="Launch the local browser UI"
    )
    explorer_parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Override the configured local Streamlit browser port",
    )
    explorer_parser.add_argument(
        "--headless",
        action="store_true",
        help="Run the local browser server without attempting to open a tab",
    )
    subparsers.add_parser(
        "pages", help="Build the static GitHub Pages atlas under docs/"
    )
    subparsers.add_parser(
        "tag-skills", help="Tag Fandom skill snippets with the configured local LLM"
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.log_level)
    settings = load_settings()

    if args.command == "scrape":
        source = None if args.source == "all" else args.source
        summary = run_scrape(settings, source)
    elif args.command == "normalize":
        summary = run_normalize(settings)
    elif args.command == "pipeline":
        summary = run_pipeline(settings)
    elif args.command == "explorer":
        summary = launch_explorer(
            port=args.port,
            headless=args.headless,
            preferred_ports=settings.explorer_preferred_ports,
        )
    elif args.command == "pages":
        summary = export_pages_site(settings)
    elif args.command == "tag-skills":
        summary = {"skill_tags": len(tag_skill_snippets(settings))}
    else:
        parser.error(f"Unsupported command: {args.command}")

    LOGGER.info("Completed command %s", args.command)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
