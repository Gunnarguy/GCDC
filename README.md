# GrandChase Meta Analyzer

Offline tooling to scrape GrandChase community sources, normalize hero rankings, score the meta in SQLite, and analyze the results in Jupyter.

## What This Repo Includes

- A Python package under `src/grandchase_meta_analyzer/`.
- Scrapers for StrategyWiki, NamuWiki, and Fandom.
- SQLite database build with hero, mode, score, trait, and skill tables.
- Optional local-LLM skill tagging through an OpenAI-compatible endpoint.
- VS Code tasks, settings, extension recommendations, and Copilot workspace instructions.

## Quick Start

1. Bootstrap the local environment:

```bash
bash scripts/bootstrap.sh
```

2. Run the full unattended pipeline:

```bash
bash scripts/run_pipeline.sh
```

3. Verify the setup or rerun pieces individually:

```bash
bash scripts/verify_setup.sh
.venv/bin/python -m grandchase_meta_analyzer.cli scrape --source strategywiki
.venv/bin/python -m grandchase_meta_analyzer.cli normalize
```

4. Open the notebook:

```bash
.venv/bin/python -m jupyter notebook notebooks/analysis.ipynb
```

5. Open the local browser app:

```bash
bash scripts/run_explorer.sh
```

## Default Commands

```bash
make bootstrap
make pipeline
make pages
make explorer
make lint
make test
```

## Fastest Way To Browse Data

If you just want to search heroes, cooldowns, coefficients, patch notes, and skill text without digging through notebook cells, use the local browser app:

```bash
bash scripts/run_explorer.sh
```

You can also launch it through the package entrypoint:

```bash
.venv/bin/python -m grandchase_meta_analyzer.cli explorer
```

The browser reads from `data/processed/grandchase.db` and the current stored Namu captures only. It does not trigger live scraping.

By default, the explorer now uses the configured port in `config/config.json`: `8506`. It will reuse an existing GrandChase Atlas server on that port and fail clearly if something else is already occupying it.

## Free Hosted Deployment

There are now two free hosted paths in the repo.

### GitHub Pages Static Atlas

This is the path if you want a phone-friendly site on GitHub Pages with no server running on your Mac.

1. Commit `data/processed/grandchase.db` whenever you want the hosted site refreshed.
2. Build the static site locally if you want to preview it:

```bash
make pages
```

3. Push to GitHub.
4. In the repository settings, enable GitHub Pages and choose the GitHub Actions source.
5. The workflow in `.github/workflows/pages.yml` will build `docs/data/atlas.json` and publish the static site.

The GitHub Pages atlas keeps the overview, search, and hero dossier views. It does not yet mirror the full Streamlit comparisons workspace.

### Streamlit Community Cloud

If you want the fuller Streamlit app instead of the static Pages export:

1. Push the repository to GitHub.
2. Keep `data/processed/grandchase.db` committed whenever you want the hosted app refreshed.
3. In Streamlit Community Cloud, create a new app from the repo.
4. Set the main file path to `streamlit_app.py`.
5. Deploy.

No secrets are required for either hosted path. Both use the checked-in SQLite database.

## Data Outputs

- `data/raw/strategywiki_heroes.csv`
- `data/raw/namuwiki_heroes.csv`
- `data/raw/fandom_chaser_traits.csv`
- `data/raw/fandom_skills.csv`
- `data/processed/grandchase.db`
- `data/processed/hero_leaderboard.csv`
- `data/processed/skill_tags.csv` when LLM tagging is enabled

## Local LLM Tagging

The repo supports optional skill tagging through a local OpenAI-compatible endpoint.

1. Update `.env` if your endpoint or model differs.
2. Set `ENABLE_LLM_TAGGING=1`.
3. Run either `bash scripts/run_pipeline.sh` or `.venv/bin/python -m grandchase_meta_analyzer.cli tag-skills`.

## Project Layout

```text
.
├── config/
├── data/
├── logs/
├── notebooks/
├── scripts/
├── src/grandchase_meta_analyzer/
├── tests/
├── Makefile
├── pyproject.toml
└── requirements.txt
```
