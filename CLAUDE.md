# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python crawler for extracting public data from the Brazilian Receita Federal's Nextcloud (CAFIR, CNO, CNPJ, SISEN datasets). It runs periodically, downloads files in streaming chunks, and maintains a data catalog and audit logs.

## Environment Setup

```bash
# Activate venv (Windows)
venv\Scripts\activate

# Activate venv (bash/Git Bash on Windows)
source venv/Scripts/activate

# Install dependencies after adding to requirements.txt
pip install -r requirements.txt
```

Python 3.14 is used (venv already created at `venv/`).

## Running the Crawler

```bash
# Run all datasets
python -m crawler.main

# Or directly
python crawler/main.py
```

To run a single dataset programmatically:

```python
from crawler.main import run
run(datasets=["CNPJ"])
```

## Architecture

The portal at `https://dadosabertos.rfb.gov.br/` serves Apache-style directory listings (static HTML, no JavaScript needed). The codebase follows **Single Responsibility Principle** with three core layers:

| Module | Responsibility |
|--------|---------------|
| `crawler/network.py` | `NetworkSession`: HTTP session with retries + `stream_download()` in 8 MB chunks |
| `crawler/scraper.py` | `DirectoryScraper`: parse Apache directory HTML (BeautifulSoup), DFS recursion |
| `crawler/filesystem.py` | `DataGovernance`: manifest, catalog, retention policy, audit log |
| `crawler/main.py` | Orchestrator: `ThreadPoolExecutor` (3 workers), health-check summary, webhook |
| `crawler/config.py` | All tuneable constants (`BASE_URL`, `DATASETS`, `MAX_WORKERS`, etc.) |
| `crawler/exceptions.py` | `RFBConnectionError`, `DiskFullError`, `SchemaChangeError` |

### Key Classes

- **`DataGovernance`** — tracks every downloaded file by SHA-256 hash in `manifest.json`. Deduplicates by hash (same content under different names is skipped). Enforces retention by deleting the oldest competência subdirs, keeping the last `RETENTION_COUNT` (default 3).
- **`DirectoryScraper.crawl()`** — DFS generator; each `DirectoryEntry` carries `hierarchy` (e.g. `["CNPJ", "2024-10"]`), `parent_folder`, `source_url`, `modified`, `size_bytes`.
- **`NetworkSession.stream_download()`** — writes to a `.part` temp file and atomically renames on completion; raises `DiskFullError` when free space drops below chunk size.

### Output Files (under `data/`)

- `manifest.json` — internal SHA-256 → metadata map; source of truth for deduplication.
- `catalog.json` — public index regenerated each run; datasets as top-level keys.
- `execution_log.csv` — appended each run: `run_id`, `status` (new/skipped/error), file metadata, timestamps.

## Code Conventions

- PEP 8; use `pathlib.Path` for all paths.
- Never load full file content into memory — always stream in `CHUNK_SIZE` chunks.
- Custom exceptions only — no bare `except` or `except Exception: pass`.
- `config.py` is the single place to tune constants; no magic numbers elsewhere.

## PRD

The full product requirements are in `.llm/prd.md`.
The crawler skill reference is in `.llm/skill.md`.
