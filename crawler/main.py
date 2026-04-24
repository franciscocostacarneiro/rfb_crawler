"""Main orchestrator: crawl all RFB datasets, download new files, emit health report."""

from __future__ import annotations

import hashlib
import json
import logging
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests

from crawler.config import (
    APACHE_BASE_URL,
    BASE_URL,
    DATASETS,
    DOWNLOAD_DIR,
    MAX_WORKERS,
    NC_BASE_URL,
    NC_SHARE_ROOT,
    NC_SHARE_TOKEN,
    PORTAL,
    WEBHOOK_URL,
)
from crawler.exceptions import DiskFullError, RFBConnectionError, SchemaChangeError
from crawler.filesystem import DataGovernance
from crawler.network import NetworkSession
from crawler.scraper import DirectoryEntry, DirectoryScraper, NextcloudScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-file download task (runs inside thread pool)
# ---------------------------------------------------------------------------

def _download_file(
    entry: DirectoryEntry,
    session: NetworkSession,
    governance: DataGovernance,
    run_id: str,
    auth: tuple[str, str] | None = None,
) -> dict:
    """Download one file and return a log-row dict."""
    log_row = {
        "run_id": run_id,
        "dataset": entry.hierarchy[0] if entry.hierarchy else "unknown",
        "file_name": entry.name,
        "source_url": entry.url,
        "parent_folder": entry.parent_folder,
        "size_bytes": entry.size_bytes,
        "file_hash": "",
        "download_timestamp": "",
        "status": "error",
        "error_message": "",
    }

    local_path = governance.resolve_local_path(entry)

    # Fast incremental check: compare WebDAV metadata (size + modified) against
    # the manifest — no local hashing needed when the remote file is unchanged.
    if governance.is_unchanged(entry):
        stored = governance._url_index[entry.url]
        log_row["status"] = "skipped"
        log_row["file_hash"] = stored.get("file_hash", "")
        logger.debug("Skipped (unchanged): %s", entry.name)
        return log_row

    # Fallback: if the file exists locally but metadata is missing/mismatched,
    # hash the local copy before re-downloading.
    if local_path.exists():
        existing_hash = _sha256_file(local_path)
        if governance.is_known(existing_hash):
            log_row["status"] = "skipped"
            log_row["file_hash"] = existing_hash
            logger.debug("Skipped (hash match): %s", entry.name)
            return log_row

    try:
        bytes_written, file_hash = session.stream_download(entry.url, local_path, auth=auth)
    except RFBConnectionError as exc:
        log_row["error_message"] = str(exc)
        logger.error("Connection error downloading %s: %s", entry.name, exc)
        return log_row
    except DiskFullError as exc:
        log_row["error_message"] = str(exc)
        logger.critical("Disk full while downloading %s: %s", entry.name, exc)
        return log_row

    if governance.is_known(file_hash):
        # Same content under a different name/path — skip duplicate
        local_path.unlink(missing_ok=True)
        log_row["status"] = "skipped"
        log_row["file_hash"] = file_hash
        logger.info("Skipped duplicate (same hash): %s", entry.name)
        return log_row

    governance.register_file(entry, local_path, file_hash, bytes_written)
    log_row.update(
        status="new",
        file_hash=file_hash,
        size_bytes=bytes_written,
        download_timestamp=datetime.now(timezone.utc).isoformat(),
    )
    logger.info("Downloaded: %s (%s MB)", entry.name, f"{bytes_written / 1e6:.1f}")
    return log_row


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Dataset-level orchestration
# ---------------------------------------------------------------------------

def _process_dataset(
    dataset: str,
    scraper: DirectoryScraper | NextcloudScraper,
    session: NetworkSession,
    governance: DataGovernance,
    run_id: str,
    portal: str = "apache",
) -> tuple[list[dict], dict]:
    """Crawl one dataset and download all new files. Returns (log_rows, stats)."""
    if portal == "nextcloud":
        crawl_path = f"{NC_SHARE_ROOT}/{dataset}"
        entries_iter = scraper.crawl(crawl_path, dataset)  # type: ignore[union-attr]
    else:
        url = f"{APACHE_BASE_URL}/{dataset}/"
        entries_iter = scraper.crawl(url, dataset)  # type: ignore[union-attr]

    stats = {"dataset": dataset, "new": 0, "skipped": 0, "errors": 0, "bytes": 0}
    log_rows: list[dict] = []

    try:
        entries = list(entries_iter)
    except SchemaChangeError as exc:
        logger.error("Schema change detected for %s: %s", dataset, exc)
        stats["errors"] += 1
        return log_rows, stats
    except RFBConnectionError as exc:
        logger.error("Cannot crawl %s: %s", dataset, exc)
        stats["errors"] += 1
        return log_rows, stats

    logger.info("Dataset %s: %d files found", dataset, len(entries))

    download_auth: tuple[str, str] | None = None
    if portal == "nextcloud":
        download_auth = (NC_SHARE_TOKEN, "")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(_download_file, entry, session, governance, run_id, download_auth): entry
            for entry in entries
        }
        for future in as_completed(futures):
            row = future.result()
            log_rows.append(row)
            if row["status"] == "new":
                stats["new"] += 1
                stats["bytes"] += row.get("size_bytes") or 0
            elif row["status"] == "skipped":
                stats["skipped"] += 1
            else:
                stats["errors"] += 1

    return log_rows, stats


# ---------------------------------------------------------------------------
# Webhook notification
# ---------------------------------------------------------------------------

def _notify_webhook(message: str) -> None:
    if not WEBHOOK_URL:
        return
    try:
        requests.post(WEBHOOK_URL, json={"text": message}, timeout=10)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Webhook notification failed: %s", exc)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(
    datasets: list[str] | None = None,
    download_dir: Path = DOWNLOAD_DIR,
    portal: str = PORTAL,
) -> None:
    run_id = uuid.uuid4().hex[:8]
    started_at = time.monotonic()
    logger.info("=== RFB Crawler started | run_id=%s portal=%s ===", run_id, portal)

    governance = DataGovernance(root=download_dir)
    all_log_rows: list[dict] = []
    all_stats: list[dict] = []

    target_datasets = datasets or DATASETS

    with NetworkSession() as session:
        if portal == "nextcloud":
            scraper: DirectoryScraper | NextcloudScraper = NextcloudScraper(
                session=session,
                base_url=NC_BASE_URL,
                share_token=NC_SHARE_TOKEN,
                share_root=NC_SHARE_ROOT,
            )
        else:
            scraper = DirectoryScraper(session)

        for dataset in target_datasets:
            logger.info("--- Processing dataset: %s ---", dataset)
            log_rows, stats = _process_dataset(
                dataset, scraper, session, governance, run_id, portal=portal
            )
            all_log_rows.extend(log_rows)
            all_stats.append(stats)

            if stats["new"] > 0:
                _notify_webhook(
                    f"[RFB Crawler] New data found in {dataset}: "
                    f"{stats['new']} file(s), "
                    f"{stats['bytes'] / 1e9:.2f} GB"
                )

            # Apply retention policy after each dataset
            governance.apply_retention(dataset)

    # Persist governance state
    governance.save_manifest()
    governance.update_catalog()
    log_path = governance.write_execution_log(run_id, all_log_rows)

    elapsed = time.monotonic() - started_at
    total_bytes = sum(s["bytes"] for s in all_stats)
    total_new = sum(s["new"] for s in all_stats)
    total_skipped = sum(s["skipped"] for s in all_stats)
    total_errors = sum(s["errors"] for s in all_stats)

    # Health-check summary
    print("\n" + "=" * 60)
    print(f"  RFB Crawler — run_id={run_id}  |  {elapsed:.1f}s")
    print("=" * 60)
    print(f"  Total downloaded : {total_bytes / 1e9:.3f} GB  ({total_new} files)")
    print(f"  Skipped (cached) : {total_skipped}")
    print(f"  Errors           : {total_errors}")
    print()
    for s in all_stats:
        icon = "OK" if s["errors"] == 0 else "FAIL"
        print(
            f"  [{icon:4s}] {s['dataset']:<10}  "
            f"new={s['new']}  skipped={s['skipped']}  "
            f"errors={s['errors']}  "
            f"{s['bytes'] / 1e6:.1f} MB"
        )
    print("=" * 60)
    print(f"  Execution log → {log_path}")
    print(f"  Catalog       → {governance._catalog_path}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    run()
