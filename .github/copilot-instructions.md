# GrandChase Meta Analyzer Workspace Instructions

- Prefer the package entrypoint over ad hoc scripts: `python -m grandchase_meta_analyzer.cli ...`.
- Keep source under `src/grandchase_meta_analyzer/` and thin wrappers under `scripts/`.
- Treat `config/config.json` as the source of truth for URLs, scoring weights, and database paths.
- Scrapers should degrade gracefully when markup shifts: capture raw snapshots, log warnings, and keep partial results rather than hard-failing on one malformed row.
- Persist derived outputs in `data/raw/` and `data/processed/`; never hardcode absolute local paths.
- Prefer small, composable functions with deterministic inputs so tests can cover parsing and scoring without network access.
- Use `make bootstrap`, `make pipeline`, `make lint`, and `make test` as the default collaborator commands.
- Keep notebook changes additive and analysis-focused; do not move business logic into notebooks.
