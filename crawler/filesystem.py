"""Filesystem / persistence layer: DataGovernance, catalog, manifest, audit log."""

from __future__ import annotations

import csv
import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from crawler.config import DOWNLOAD_DIR, RETENTION_COUNT
from crawler.scraper import DirectoryEntry

logger = logging.getLogger(__name__)

_MANIFEST_FILE = "manifest.json"
_CATALOG_FILE = "catalog.json"
_LOG_FILE = "execution_log.csv"

_LOG_FIELDS = [
    "run_id",
    "dataset",
    "file_name",
    "status",          # new | skipped | error
    "source_url",
    "parent_folder",
    "size_bytes",
    "file_hash",
    "download_timestamp",
    "error_message",
]


class DataGovernance:
    """Manages the download manifest, catalog, and retention policy.

    The manifest tracks every file ever downloaded (keyed by SHA-256 hash)
    so we never re-download the same content twice.
    """

    def __init__(self, root: Path = DOWNLOAD_DIR) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self._manifest_path = self.root / _MANIFEST_FILE
        self._catalog_path = self.root / _CATALOG_FILE
        self._manifest: dict[str, dict[str, Any]] = self._load_manifest()
        # Secondary index: source_url → manifest record (for fast incremental checks)
        self._url_index: dict[str, dict[str, Any]] = self._build_url_index()

    # ------------------------------------------------------------------
    # Manifest helpers
    # ------------------------------------------------------------------

    def _load_manifest(self) -> dict[str, dict[str, Any]]:
        if self._manifest_path.exists():
            try:
                return json.loads(self._manifest_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                logger.warning("Manifest corrupted, starting fresh.")
        return {}

    def save_manifest(self) -> None:
        self._manifest_path.write_text(
            json.dumps(self._manifest, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    def _build_url_index(self) -> dict[str, dict[str, Any]]:
        """Build a secondary index: source_url → manifest record."""
        return {
            rec["source_url"]: rec
            for rec in self._manifest.values()
            if "source_url" in rec
        }

    def is_known(self, file_hash: str) -> bool:
        """Return True if a file with this SHA-256 hash was already downloaded."""
        return file_hash in self._manifest

    def is_unchanged(self, entry: "DirectoryEntry") -> bool:
        """Return True when the remote file metadata matches the manifest record.

        Uses WebDAV size + last-modified as a lightweight fingerprint to avoid
        re-downloading and re-hashing files that haven't changed on the server.
        Requires the local file to still exist on disk.
        """
        rec = self._url_index.get(entry.url)
        if rec is None:
            return False
        # Both metadata fields must be present and match
        if entry.size_bytes is None or rec.get("remote_size_bytes") != entry.size_bytes:
            return False
        if entry.modified is None or rec.get("remote_modified") != entry.modified:
            return False
        local_path = self.root / rec["local_path"]
        return local_path.exists()

    def register_file(
        self,
        entry: DirectoryEntry,
        local_path: Path,
        file_hash: str,
        bytes_written: int,
    ) -> None:
        """Record a successfully downloaded file in the manifest."""
        record: dict[str, Any] = {
            "name": entry.name,
            "source_url": entry.url,
            "parent_folder": entry.parent_folder,
            "hierarchy": entry.hierarchy,
            "local_path": str(local_path.relative_to(self.root)),
            "size_bytes": bytes_written,
            "remote_size_bytes": entry.size_bytes,
            "remote_modified": entry.modified,
            "file_hash": file_hash,
            "download_timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self._manifest[file_hash] = record
        self._url_index[entry.url] = record

    # ------------------------------------------------------------------
    # Catalog
    # ------------------------------------------------------------------

    def update_catalog(self) -> None:
        """Rewrite catalog.json from the current manifest state."""
        catalog: dict[str, list[dict]] = {}
        for record in self._manifest.values():
            dataset = record["hierarchy"][0] if record["hierarchy"] else "unknown"
            catalog.setdefault(dataset, []).append(record)

        self._catalog_path.write_text(
            json.dumps(
                {"generated_at": datetime.now(timezone.utc).isoformat(),
                 "datasets": catalog},
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        logger.info("Catalog updated → %s", self._catalog_path)

    # ------------------------------------------------------------------
    # Execution log
    # ------------------------------------------------------------------

    def write_execution_log(self, run_id: str, log_entries: list[dict]) -> Path:
        """Append *log_entries* to the execution CSV log."""
        log_path = self.root / _LOG_FILE
        write_header = not log_path.exists()
        with log_path.open("a", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=_LOG_FIELDS, extrasaction="ignore")
            if write_header:
                writer.writeheader()
            for row in log_entries:
                row.setdefault("run_id", run_id)
                writer.writerow(row)
        return log_path

    # ------------------------------------------------------------------
    # Retention policy
    # ------------------------------------------------------------------

    def apply_retention(
        self,
        dataset: str,
        keep: int = RETENTION_COUNT,
    ) -> list[Path]:
        """Remove the oldest competência folders for *dataset*, keeping the last *keep*.

        Returns a list of paths that were deleted.
        """
        dataset_dir = self.root / dataset
        if not dataset_dir.is_dir():
            return []

        # Competência sub-dirs are immediate children (e.g. "2024-01", "2024-02")
        subdirs = sorted(
            [d for d in dataset_dir.iterdir() if d.is_dir()],
            key=lambda d: d.name,
        )
        to_remove = subdirs[:-keep] if len(subdirs) > keep else []

        removed = []
        for old_dir in to_remove:
            logger.info("Retention: removing %s", old_dir)
            shutil.rmtree(old_dir, ignore_errors=True)
            removed.append(old_dir)
            # Remove from manifest
            self._manifest = {
                h: rec
                for h, rec in self._manifest.items()
                if not rec.get("local_path", "").startswith(
                    str(old_dir.relative_to(self.root))
                )
            }

        return removed

    # ------------------------------------------------------------------
    # Local path resolver
    # ------------------------------------------------------------------

    def resolve_local_path(self, entry: DirectoryEntry) -> Path:
        """Build the local destination path for *entry*, mirroring the hierarchy."""
        parts = entry.hierarchy + [entry.name]
        return self.root.joinpath(*parts)
