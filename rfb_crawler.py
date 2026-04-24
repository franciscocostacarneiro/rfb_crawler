"""
RFB Crawler — single-file version.
Acessa dados públicos da Receita Federal via Nextcloud (arquivos.receitafederal.gov.br).
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Standard library
# ---------------------------------------------------------------------------
import csv
import hashlib
import json
import logging
import shutil
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Third-party
# ---------------------------------------------------------------------------
import xml.etree.ElementTree as ET

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===========================================================================
# CONFIG
# ===========================================================================

# --- Portal: Nextcloud Public Share ---
NC_BASE_URL = "https://arquivos.receitafederal.gov.br"
NC_SHARE_TOKEN = "gn672Ad4CF8N6TK"
NC_SHARE_ROOT = ""   # raiz do compartilhamento público (listagem completa)

DOWNLOAD_DIR = Path("data")
MAX_WORKERS = 3
CHUNK_SIZE = 8 * 1024 * 1024   # 8 MB
TIMEOUT = (10, 60)              # (connect, read) seconds
MAX_RETRIES = 3
RETENTION_COUNT = 3             # keep last N competência folders per dataset
WEBHOOK_URL = None              # e.g. "https://hooks.slack.com/..."

# ===========================================================================
# EXCEPTIONS
# ===========================================================================

class RFBConnectionError(IOError):
    """Network failure reaching the RFB portal."""

class DiskFullError(OSError):
    """Not enough disk space to write a downloaded file."""

class SchemaChangeError(RuntimeError):
    """Portal directory structure no longer matches expectations."""

# ===========================================================================
# NETWORK
# ===========================================================================

def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=2,
        status_forcelist={500, 502, 503, 504},
        allowed_methods={"GET", "HEAD"},
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers["User-Agent"] = "Mozilla/5.0 (compatible; RFBCrawler/1.0)"
    return session


class NetworkSession:
    def __init__(self) -> None:
        self._session = _build_session()

    def get(self, url: str, **kwargs) -> requests.Response:
        try:
            resp = self._session.get(url, timeout=TIMEOUT, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.exceptions.Timeout as exc:
            raise RFBConnectionError(f"Timeout: {url}") from exc
        except requests.exceptions.ConnectionError as exc:
            raise RFBConnectionError(f"Connection failed: {url}") from exc
        except requests.exceptions.HTTPError as exc:
            raise RFBConnectionError(
                f"HTTP {exc.response.status_code}: {url}"
            ) from exc

    def stream_download(
        self,
        url: str,
        dest: Path,
        auth: tuple[str, str] | None = None,
    ) -> tuple[int, str]:
        """Download url → dest in streaming chunks. Returns (bytes, sha256)."""
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".part")

        try:
            kwargs: dict = {"stream": True, "timeout": TIMEOUT}
            if auth:
                kwargs["auth"] = auth
            resp = self._session.get(url, **kwargs)
            resp.raise_for_status()
        except requests.exceptions.Timeout as exc:
            raise RFBConnectionError(f"Timeout starting download: {url}") from exc
        except requests.exceptions.RequestException as exc:
            raise RFBConnectionError(f"Download failed: {url} — {exc}") from exc

        sha256 = hashlib.sha256()
        bytes_written = 0
        try:
            with tmp.open("wb") as fh:
                for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                    if not chunk:
                        continue
                    try:
                        fh.write(chunk)
                    except OSError as exc:
                        usage = shutil.disk_usage(dest.parent)
                        if usage.free < CHUNK_SIZE:
                            raise DiskFullError(
                                f"Disk full: {usage.free / 1e6:.1f} MB free"
                            ) from exc
                        raise
                    sha256.update(chunk)
                    bytes_written += len(chunk)
        except Exception:
            tmp.unlink(missing_ok=True)
            raise

        tmp.replace(dest)
        return bytes_written, sha256.hexdigest()

    def propfind(
        self,
        url: str,
        body: str,
        auth: tuple[str, str] | None = None,
    ) -> requests.Response:
        """Send a WebDAV PROPFIND request and return the response."""
        kwargs: dict = {
            "headers": {"Depth": "1", "Content-Type": "application/xml"},
            "data": body.encode("utf-8"),
            "timeout": TIMEOUT,
        }
        if auth:
            kwargs["auth"] = auth
        try:
            resp = self._session.request("PROPFIND", url, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.exceptions.Timeout as exc:
            raise RFBConnectionError(f"Timeout PROPFIND: {url}") from exc
        except requests.exceptions.ConnectionError as exc:
            raise RFBConnectionError(f"Connection failed PROPFIND: {url}") from exc
        except requests.exceptions.HTTPError as exc:
            raise RFBConnectionError(
                f"HTTP {exc.response.status_code} PROPFIND: {url}"
            ) from exc

    def close(self) -> None:
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

# ===========================================================================
# SCRAPER
# ===========================================================================


@dataclass
class DirectoryEntry:
    name: str
    url: str
    is_dir: bool
    parent_folder: str
    size_bytes: int | None = None
    modified: str | None = None
    hierarchy: list[str] = field(default_factory=list)


# ===========================================================================
# DATA GOVERNANCE (filesystem layer)
# ===========================================================================

_LOG_FIELDS = [
    "run_id", "dataset", "file_name", "status",
    "source_url", "parent_folder", "size_bytes",
    "file_hash", "download_timestamp", "error_message",
]


class DataGovernance:
    def __init__(self, root: Path = DOWNLOAD_DIR) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self._manifest_path = root / "manifest.json"
        self._catalog_path = root / "catalog.json"
        self._log_path = root / "execution_log.csv"
        self._manifest: dict = self._load_manifest()
        # Secondary index: source_url → manifest record (for fast incremental checks)
        self._url_index: dict = self._build_url_index()

    def _load_manifest(self) -> dict:
        if self._manifest_path.exists():
            try:
                return json.loads(self._manifest_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                logger.warning("Manifest corrupted — starting fresh.")
        return {}

    def _build_url_index(self) -> dict:
        """Build a secondary index: source_url → manifest record."""
        return {
            rec["source_url"]: rec
            for rec in self._manifest.values()
            if "source_url" in rec
        }

    def save_manifest(self) -> None:
        self._manifest_path.write_text(
            json.dumps(self._manifest, indent=2, ensure_ascii=False), encoding="utf-8"
        )

    def is_known(self, file_hash: str) -> bool:
        return file_hash in self._manifest

    def is_unchanged(self, entry: DirectoryEntry) -> bool:
        """Return True when the remote file metadata matches the manifest record.

        Uses WebDAV size + last-modified as a lightweight fingerprint to avoid
        re-downloading and re-hashing files that haven't changed on the server.
        Requires the local file to still exist on disk.
        """
        rec = self._url_index.get(entry.url)
        if rec is None:
            return False
        if entry.size_bytes is None or rec.get("remote_size_bytes") != entry.size_bytes:
            return False
        if entry.modified is None or rec.get("remote_modified") != entry.modified:
            return False
        local_path = self.root / rec["local_path"]
        return local_path.exists()

    def register_file(self, entry: DirectoryEntry, local_path: Path,
                      file_hash: str, bytes_written: int) -> None:
        record = {
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

    def update_catalog(self) -> None:
        catalog: dict = {}
        for rec in self._manifest.values():
            ds = rec["hierarchy"][0] if rec["hierarchy"] else "unknown"
            catalog.setdefault(ds, []).append(rec)
        self._catalog_path.write_text(
            json.dumps(
                {"generated_at": datetime.now(timezone.utc).isoformat(),
                 "datasets": catalog},
                indent=2, ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        logger.info("Catalog updated → %s", self._catalog_path)

    def write_execution_log(self, run_id: str, rows: list[dict]) -> Path:
        write_header = not self._log_path.exists()
        with self._log_path.open("a", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=_LOG_FIELDS, extrasaction="ignore")
            if write_header:
                writer.writeheader()
            for row in rows:
                row.setdefault("run_id", run_id)
                writer.writerow(row)
        return self._log_path

    def apply_retention(self, dataset: str, keep: int = RETENTION_COUNT) -> None:
        dataset_dir = self.root / dataset
        if not dataset_dir.is_dir():
            return
        subdirs = sorted(d for d in dataset_dir.iterdir() if d.is_dir())
        for old in subdirs[:-keep] if len(subdirs) > keep else []:
            logger.info("Retention: removing %s", old)
            shutil.rmtree(old, ignore_errors=True)
            prefix = str(old.relative_to(self.root))
            self._manifest = {
                h: r for h, r in self._manifest.items()
                if not r.get("local_path", "").startswith(prefix)
            }

    def resolve_local_path(self, entry: DirectoryEntry) -> Path:
        return self.root.joinpath(*entry.hierarchy, entry.name)

# ===========================================================================
# NEXTCLOUD SCRAPER (WebDAV-based)
# ===========================================================================

_PROPFIND_BODY = (
    '<?xml version="1.0" encoding="utf-8"?>'
    '<d:propfind xmlns:d="DAV:">'
    '<d:prop>'
    '<d:displayname/>'
    '<d:resourcetype/>'
    '<d:getcontentlength/>'
    '<d:getlastmodified/>'
    '</d:prop>'
    '</d:propfind>'
)

_NS = {"d": "DAV:"}


class NextcloudScraper:
    """Scraper for Nextcloud public-share portals via WebDAV PROPFIND.

    Authentication uses the share token as username with an empty password,
    which is the standard Nextcloud mechanism for public shares.
    Download URLs use the Nextcloud share-download endpoint.
    """

    def __init__(
        self,
        session: NetworkSession,
        base_url: str = NC_BASE_URL,
        share_token: str = NC_SHARE_TOKEN,
    ) -> None:
        self._session = session
        self._base = base_url.rstrip("/")
        self._token = share_token
        self._webdav_root = f"{self._base}/public.php/webdav"
        self._auth = (share_token, "")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _propfind(self, nc_path: str) -> list[dict]:
        """PROPFIND on *nc_path* (e.g. '/Dados/Cadastros'). Returns raw entries."""
        url = self._webdav_root + nc_path.rstrip("/") + "/"
        try:
            resp = self._session.propfind(url, _PROPFIND_BODY, auth=self._auth)
        except RFBConnectionError:
            raise
        return self._parse_propfind_xml(resp.text, nc_path)

    def _parse_propfind_xml(self, xml_text: str, base_path: str) -> list[dict]:
        """Parse WebDAV PROPFIND response. Returns list of child entries."""
        try:
            root_el = ET.fromstring(xml_text)
        except ET.ParseError as exc:
            raise SchemaChangeError(f"Cannot parse WebDAV XML: {exc}") from exc

        prefix = "/public.php/webdav"
        entries = []

        for response in root_el.findall("d:response", _NS):
            href_el = response.find("d:href", _NS)
            if href_el is None or not href_el.text:
                continue
            href = href_el.text

            # Extract the path relative to the share root
            if href.startswith(prefix):
                item_path = href[len(prefix):]
            else:
                item_path = href

            item_path = item_path.rstrip("/")

            # Skip the parent directory itself
            if item_path == base_path.rstrip("/"):
                continue

            propstat = response.find("d:propstat", _NS)
            if propstat is None:
                continue
            prop = propstat.find("d:prop", _NS)
            if prop is None:
                continue

            resourcetype = prop.find("d:resourcetype", _NS)
            is_dir = (
                resourcetype is not None
                and resourcetype.find("d:collection", _NS) is not None
            )

            name_el = prop.find("d:displayname", _NS)
            name = (name_el.text or "").strip() if name_el is not None else ""
            if not name:
                name = item_path.rstrip("/").split("/")[-1]
            if not name:
                continue

            size_el = prop.find("d:getcontentlength", _NS)
            size_bytes: int | None = None
            if size_el is not None and size_el.text:
                try:
                    size_bytes = int(size_el.text)
                except ValueError:
                    pass

            modified_el = prop.find("d:getlastmodified", _NS)
            modified = modified_el.text if modified_el is not None else None

            entries.append(
                {
                    "name": name,
                    "path": item_path,
                    "is_dir": is_dir,
                    "size_bytes": size_bytes,
                    "modified": modified,
                }
            )

        return entries

    def _download_url(self, nc_path: str) -> str:
        """Build the WebDAV direct-download URL for a file path."""
        from urllib.parse import quote
        return f"{self._base}/public.php/webdav{quote(nc_path)}"

    # ------------------------------------------------------------------
    # Public interface (mirrors DirectoryScraper)
    # ------------------------------------------------------------------

    def list_directory(self, nc_path: str) -> list[dict]:
        """List *nc_path* and return entries in the same format as DirectoryScraper."""
        entries = self._propfind(nc_path)
        return [
            {
                "name": e["name"],
                "url": self._download_url(e["path"]) if not e["is_dir"] else None,
                "is_dir": e["is_dir"],
                "modified": e["modified"],
                "size_bytes": e["size_bytes"],
                "path": e["path"],
            }
            for e in entries
        ]

    def crawl(
        self,
        nc_path: str,
        dataset: str,
        hierarchy: list[str] | None = None,
    ):
        """DFS crawl starting at *nc_path*. Yields DirectoryEntry for each file."""
        if hierarchy is None:
            hierarchy = [dataset]

        logger.info("Scanning (Nextcloud WebDAV) %s", nc_path)
        try:
            entries = self._propfind(nc_path)
        except RFBConnectionError as exc:
            logger.error("Cannot list %s: %s", nc_path, exc)
            return

        for e in entries:
            if e["is_dir"]:
                yield from self.crawl(
                    e["path"], dataset, hierarchy + [e["name"]]
                )
            else:
                yield DirectoryEntry(
                    name=e["name"],
                    url=self._download_url(e["path"]),
                    is_dir=False,
                    parent_folder=hierarchy[-1] if len(hierarchy) > 1 else dataset,
                    size_bytes=e["size_bytes"],
                    modified=e["modified"],
                    hierarchy=list(hierarchy),
                )

# ===========================================================================
# ORCHESTRATOR
# ===========================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(CHUNK_SIZE), b""):
            h.update(chunk)
    return h.hexdigest()


def _download_file(entry: DirectoryEntry, session: NetworkSession,
                   governance: DataGovernance, run_id: str,
                   auth: tuple[str, str] | None = None) -> dict:
    row = {
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

    local = governance.resolve_local_path(entry)

    # Fast incremental check: compare WebDAV metadata (size + modified) against
    # the manifest — no local hashing needed when the remote file is unchanged.
    if governance.is_unchanged(entry):
        stored = governance._url_index[entry.url]
        row.update(status="skipped", file_hash=stored.get("file_hash", ""))
        logger.debug("Skipped (unchanged): %s", entry.name)
        return row

    # Fallback: if the file exists locally but metadata is missing/mismatched,
    # hash the local copy before re-downloading.
    if local.exists():
        h = _sha256_file(local)
        if governance.is_known(h):
            row.update(status="skipped", file_hash=h)
            logger.debug("Skipped (hash match): %s", entry.name)
            return row

    try:
        bytes_written, file_hash = session.stream_download(entry.url, local, auth=auth)
    except RFBConnectionError as exc:
        row["error_message"] = str(exc)
        logger.error("Connection error: %s — %s", entry.name, exc)
        return row
    except DiskFullError as exc:
        row["error_message"] = str(exc)
        logger.critical("Disk full: %s", exc)
        return row

    if governance.is_known(file_hash):
        local.unlink(missing_ok=True)
        row.update(status="skipped", file_hash=file_hash)
        logger.info("Skipped duplicate (same hash): %s", entry.name)
        return row

    governance.register_file(entry, local, file_hash, bytes_written)
    row.update(
        status="new",
        file_hash=file_hash,
        size_bytes=bytes_written,
        download_timestamp=datetime.now(timezone.utc).isoformat(),
    )
    logger.info("Downloaded: %s (%.1f MB)", entry.name, bytes_written / 1e6)
    return row


def _notify_webhook(message: str) -> None:
    if not WEBHOOK_URL:
        return
    try:
        requests.post(WEBHOOK_URL, json={"text": message}, timeout=10)
    except Exception as exc:
        logger.warning("Webhook failed: %s", exc)


# Prefix stripped when converting a server NC path to a local hierarchy.
# Keeps backward-compatible folder layout: /Dados/Cadastros/CNPJ → ["CNPJ"].
_NC_KNOWN_PREFIX = "/Dados/Cadastros"


def _nc_hierarchy(nc_path: str) -> list[str]:
    """Map a server NC path to the initial local hierarchy list for DataGovernance.

    Strips the known /Dados/Cadastros prefix so that e.g.
    /Dados/Cadastros/CNPJ/2026-04  →  ['CNPJ', '2026-04'],
    maintaining backward-compatible local folder structure.
    For paths outside this prefix all components are used.
    """
    path = nc_path.rstrip("/")
    if path.startswith(_NC_KNOWN_PREFIX):
        path = path[len(_NC_KNOWN_PREFIX):]
    parts = [p for p in path.split("/") if p]
    if not parts:
        # Fallback: use the last component of the original path
        orig_parts = [p for p in nc_path.strip("/").split("/") if p]
        parts = [orig_parts[-1]] if orig_parts else ["dados"]
    return parts


def run(
    nc_paths: list[dict | str] | None = None,
    download_dir: Path = DOWNLOAD_DIR,
) -> None:
    """Run the crawler with Nextcloud paths chosen via the folder browser."""
    run_id = uuid.uuid4().hex[:8]
    t0 = time.monotonic()
    logger.info("=== RFB Crawler started | run_id=%s ===", run_id)

    governance = DataGovernance(root=download_dir)
    all_rows: list[dict] = []
    all_stats: list[dict] = []

    with NetworkSession() as session:
        scraper = NextcloudScraper(session)
        download_auth: tuple[str, str] = (NC_SHARE_TOKEN, "")

        if nc_paths is not None:
            # ── Free-path mode: crawl each NC path chosen in the browser ──────────
            for nc_item in nc_paths:
                # Support both plain strings (legacy) and dicts from the browser
                if isinstance(nc_item, dict):
                    nc_path = nc_item["path"]
                    is_dir = nc_item["is_dir"]
                else:
                    nc_path = nc_item
                    is_dir = True  # assume directory for plain-string paths
                hier = _nc_hierarchy(nc_path)
                dataset_name = hier[0]
                logger.info("--- NC path: %s  (é pasta: %s)  →  local: %s ---", nc_path, is_dir, "/".join(hier))
                stats = {"dataset": dataset_name, "new": 0, "skipped": 0, "errors": 0, "bytes": 0}

                entries: list[DirectoryEntry] = []
                if not is_dir:
                    # Direct file — build a single DirectoryEntry without PROPFIND crawl
                    file_name = hier[-1] if hier else nc_path.split("/")[-1]
                    file_hier = hier[:-1] if len(hier) > 1 else hier
                    parent = file_hier[-1] if file_hier else dataset_name
                    entry_size = nc_item.get("size_bytes") if isinstance(nc_item, dict) else None
                    entry_mod  = nc_item.get("modified")   if isinstance(nc_item, dict) else None
                    entries = [DirectoryEntry(
                        name=file_name,
                        url=f"{NC_BASE_URL}/public.php/webdav{nc_path}",
                        is_dir=False,
                        parent_folder=parent,
                        size_bytes=entry_size,
                        modified=entry_mod,
                        hierarchy=file_hier,
                    )]
                else:
                    try:
                        entries = list(scraper.crawl(nc_path, dataset_name, hier))
                    except SchemaChangeError as exc:
                        logger.error("Schema change at %s: %s", nc_path, exc)
                        stats["errors"] += 1
                        all_stats.append(stats)
                        continue
                    except RFBConnectionError as exc:
                        logger.error("Cannot crawl %s: %s", nc_path, exc)
                        stats["errors"] += 1
                        all_stats.append(stats)
                        continue

                logger.info("%s: %d files found", nc_path, len(entries))

                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                    futures = {
                        pool.submit(_download_file, e, session, governance, run_id, download_auth): e
                        for e in entries
                    }
                    for future in as_completed(futures):
                        row = future.result()
                        all_rows.append(row)
                        if row["status"] == "new":
                            stats["new"] += 1
                            stats["bytes"] += row.get("size_bytes") or 0
                        elif row["status"] == "skipped":
                            stats["skipped"] += 1
                        else:
                            stats["errors"] += 1

                if stats["new"] > 0:
                    _notify_webhook(
                        f"[RFB Crawler] {dataset_name}: {stats['new']} new file(s), "
                        f"{stats['bytes'] / 1e9:.2f} GB"
                    )
                governance.apply_retention(dataset_name)
                all_stats.append(stats)

    governance.save_manifest()
    governance.update_catalog()
    log_path = governance.write_execution_log(run_id, all_rows)

    elapsed = time.monotonic() - t0
    total_gb = sum(s["bytes"] for s in all_stats) / 1e9
    total_new = sum(s["new"] for s in all_stats)
    total_skip = sum(s["skipped"] for s in all_stats)
    total_err = sum(s["errors"] for s in all_stats)

    print("\n" + "=" * 60)
    print(f"  RFB Crawler  run_id={run_id}  {elapsed:.1f}s")
    print("=" * 60)
    print(f"  Downloaded : {total_gb:.3f} GB  ({total_new} new files)")
    print(f"  Skipped    : {total_skip}  (already cached)")
    print(f"  Errors     : {total_err}")
    print()
    for s in all_stats:
        icon = "OK  " if s["errors"] == 0 else "FAIL"
        print(
            f"  [{icon}] {s['dataset']:<8}  "
            f"new={s['new']}  skipped={s['skipped']}  "
            f"errors={s['errors']}  {s['bytes'] / 1e6:.1f} MB"
        )
    print("=" * 60)
    print(f"  Log     → {log_path}")
    print(f"  Catalog → {governance._catalog_path}")
    print("=" * 60 + "\n")

    # GUI notification when no new files were found (base already up to date)
    if total_new == 0 and total_err == 0:
        try:
            from tkinter import messagebox
            root = _gui_root()
            messagebox.showinfo(
                "RFB Crawler — Base já atualizada",
                "Nenhum arquivo novo foi encontrado para os datasets selecionados.\n\n"
                f"Arquivos verificados: {total_skip}\n"
                "Todos os arquivos locais já estão completos e atualizados.\n\n"
                "Não há nada para incrementar.",
                parent=root,
            )
            root.destroy()
        except Exception:
            pass  # Non-GUI context; the console summary is sufficient


# ===========================================================================
# GUI helpers (tkinter — stdlib only)
# ===========================================================================

def _gui_root():
    import tkinter as tk
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    return root


def _ask_save_directory() -> Path:
    """Show a folder-picker dialog and return the chosen path."""
    from tkinter import filedialog, messagebox

    root = _gui_root()
    messagebox.showinfo(
        "RFB Crawler — Receita Federal",
        "Selecione a pasta onde os arquivos extraídos serão salvos.\n\n"
        "Os dados serão organizados em subpastas por dataset "
        "(CNPJ, CAFIR, CNO, SISEN).",
        parent=root,
    )
    folder = filedialog.askdirectory(
        title="Selecione a pasta de destino para os dados da RFB",
        mustexist=False,
        parent=root,
    )
    root.destroy()

    if not folder:
        print("Nenhuma pasta selecionada. Encerrando.")
        sys.exit(0)

    chosen = Path(folder)
    chosen.mkdir(parents=True, exist_ok=True)
    return chosen


def _check_connectivity(url: str, timeout: int = 8) -> tuple[bool, str]:
    """Quick reachability check. Returns (ok, message)."""
    try:
        resp = requests.head(
            url,
            timeout=(timeout, timeout),
            allow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; RFBCrawler/1.0)"},
        )
        # 401 on Nextcloud = server reachable, requires auth (expected for WebDAV)
        if resp.status_code < 500 or resp.status_code == 401:
            return True, f"HTTP {resp.status_code}"
        return False, f"HTTP {resp.status_code}"
    except requests.exceptions.Timeout:
        return False, "Timeout (servidor não respondeu)"
    except requests.exceptions.ConnectionError as exc:
        return False, f"Conexão recusada/bloqueada: {exc.__class__.__name__}"
    except requests.exceptions.RequestException as exc:
        return False, f"Erro: {exc}"


def _ask_proxy(portal_url: str) -> str | None:
    """Ask the user for an optional proxy URL. Returns None if skipped."""
    import os
    from tkinter import simpledialog, messagebox

    root = _gui_root()
    answer = messagebox.askyesno(
        "RFB Crawler — Sem conectividade",
        "Não foi possível conectar ao servidor da Receita Federal\n"
        f"({portal_url}).\n\n"
        "Causas comuns:\n"
        "  • Firewall corporativo ou antivírus bloqueando o destino\n"
        "  • Rede exige um servidor proxy\n"
        "  • Servidor da RFB temporariamente fora do ar\n\n"
        "Deseja informar um servidor proxy e tentar novamente?",
        parent=root,
    )
    if not answer:
        root.destroy()
        return None

    proxy = simpledialog.askstring(
        "Configurar proxy",
        "Informe o endereço do proxy\n"
        "(ex.: http://usuario:senha@proxy.empresa.com:8080):",
        parent=root,
    )
    root.destroy()

    if proxy:
        proxy = proxy.strip()
        os.environ["HTTP_PROXY"] = proxy
        os.environ["HTTPS_PROXY"] = proxy
        return proxy
    return None


def _show_error(title: str, message: str) -> None:
    try:
        from tkinter import messagebox
        root = _gui_root()
        messagebox.showerror(title, message, parent=root)
        root.destroy()
    except Exception:
        print(f"[{title}] {message}")



def _ensure_connectivity() -> None:
    """Pre-flight check for the Nextcloud portal. Aborts with GUI message if unreachable."""
    check_url = NC_BASE_URL + "/"
    domain = "arquivos.receitafederal.gov.br"

    print(f"Verificando conectividade com {check_url} ...")
    ok, info = _check_connectivity(check_url)
    if ok:
        print(f"  Conectividade OK ({info}).")
        return

    print(f"  FALHA: {info}")
    proxy = _ask_proxy(check_url)
    if proxy:
        print(f"Tentando novamente via proxy: {proxy}")
        ok, info = _check_connectivity(check_url, timeout=15)
        if ok:
            print(f"  Conectividade via proxy OK ({info}).")
            return
        print(f"  Ainda sem conexão ({info}).")

    _show_error(
        "RFB Crawler — Sem conexão",
        "Não foi possível conectar ao servidor da Receita Federal:\n"
        f"  {check_url}\n\n"
        f"Detalhe técnico: {info}\n\n"
        "Verifique com a equipe de TI:\n"
        f"  • Liberação de acesso ao domínio {domain} (porta 443/HTTPS)\n"
        "  • Configuração de proxy corporativo (se houver)\n"
        "  • Regras de firewall/antivírus\n\n"
        "O programa será encerrado.",
    )
    sys.exit(2)


# ===========================================================================
# NEXTCLOUD FOLDER BROWSER
# ===========================================================================

def _browse_nextcloud(session: NetworkSession) -> list[dict]:
    """Interactive two-pane folder browser for the Nextcloud share.

    Left pane: navigable directory listing (double-click or "Entrar" to descend).
    Right pane: download queue the user builds up by clicking "+ Adicionar à fila".

    Returns list of dicts {path, is_dir, name, size_bytes, modified} for each
    queued item, or [] if cancelled.
    """
    import tkinter as tk
    from tkinter import ttk, messagebox

    scraper = NextcloudScraper(session)

    # ── Mutable state ────────────────────────────────────────────────────────
    nav_stack: list[str] = []              # history for "Back"
    state: dict = {"path": "", "entries": []}
    queue: list[dict] = []                 # items selected for download
    result: dict = {"paths": None}

    # ── Window ───────────────────────────────────────────────────────────────
    win = tk.Tk()
    win.title("RFB Crawler — Seleção de pastas para download")
    win.geometry("880x580")
    win.minsize(640, 420)
    win.resizable(True, True)
    win.attributes("-topmost", True)

    # ── Top bar: current path + Back button ──────────────────────────────────
    top_bar = ttk.Frame(win)
    top_bar.pack(fill="x", padx=10, pady=(10, 2))
    ttk.Label(top_bar, text="Pasta atual:", font=("Segoe UI", 9, "bold")).pack(side="left")
    path_var = tk.StringVar()
    ttk.Label(top_bar, textvariable=path_var, foreground="#1a4b9e",
              font=("Segoe UI", 9)).pack(side="left", padx=(6, 0))
    btn_back = ttk.Button(top_bar, text="← Voltar", state="disabled")
    btn_back.pack(side="right")

    ttk.Label(
        win,
        text=(
            "Dica: dê duplo clique em uma pasta para entrar nela. "
            "Selecione um ou mais itens e clique em \"+ Adicionar à fila\" para programar o download."
        ),
        foreground="#555", wraplength=840, font=("Segoe UI", 8),
    ).pack(padx=10, anchor="w")

    # ── Two-pane layout ──────────────────────────────────────────────────────
    pw = ttk.PanedWindow(win, orient="horizontal")
    pw.pack(fill="both", expand=True, padx=10, pady=6)

    # Left: directory listing
    lf = ttk.LabelFrame(pw, text="Conteúdo da pasta")
    pw.add(lf, weight=3)
    dir_sb = ttk.Scrollbar(lf, orient="vertical")
    dir_lb = tk.Listbox(lf, selectmode="extended", yscrollcommand=dir_sb.set,
                        font=("Consolas", 9), activestyle="dotbox")
    dir_sb.config(command=dir_lb.yview)
    dir_sb.pack(side="right", fill="y")
    dir_lb.pack(fill="both", expand=True)

    # Right: download queue
    rf = ttk.LabelFrame(pw, text="Fila de download (0 itens)")
    pw.add(rf, weight=2)
    q_sb = ttk.Scrollbar(rf, orient="vertical")
    q_lb = tk.Listbox(rf, yscrollcommand=q_sb.set, font=("Consolas", 8),
                      activestyle="dotbox")
    q_sb.config(command=q_lb.yview)
    q_sb.pack(side="right", fill="y")
    q_lb.pack(fill="both", expand=True)
    ttk.Button(rf, text="✕ Remover da fila",
               command=lambda: _remove()).pack(padx=4, pady=(4, 2), fill="x")

    # ── Action buttons ───────────────────────────────────────────────────────
    bf = ttk.Frame(win)
    bf.pack(fill="x", padx=10, pady=(0, 8))
    btn_enter = ttk.Button(bf, text="↓ Entrar na pasta")
    btn_enter.pack(side="left")
    btn_add = ttk.Button(bf, text="+ Adicionar à fila")
    btn_add.pack(side="left", padx=6)
    ttk.Button(bf, text="Cancelar",
               command=lambda: win.destroy()).pack(side="right")
    ttk.Button(bf, text="✓ Confirmar download",
               command=lambda: _confirm()).pack(side="right", padx=6)

    # ── Helper functions ─────────────────────────────────────────────────────
    def _entry_label(e: dict) -> str:
        icon = "📁" if e["is_dir"] else "📄"
        label = f"{icon}  {e['name']}"
        if e.get("modified"):
            label += f"   [{e['modified'][:16]}]"
        if not e["is_dir"] and e.get("size_bytes"):
            mb = e["size_bytes"] / 1e6
            label += f"   ({mb:.1f} MB)" if mb < 1024 else f"   ({mb / 1024:.2f} GB)"
        return label

    def _update_queue_label() -> None:
        n = len(queue)
        rf.config(text=f"Fila de download ({n} iten{'s' if n != 1 else ''})")

    def _load(nc_path: str) -> None:
        """Fetch and display the contents of *nc_path*."""
        win.config(cursor="watch")
        win.update()
        try:
            entries = scraper.list_directory(nc_path)
        except Exception as exc:
            messagebox.showerror(
                "Erro ao listar pasta",
                f"Não foi possível listar:\n{nc_path or '(raiz)'}\n\n{exc}",
                parent=win,
            )
            return
        finally:
            win.config(cursor="")
        state["path"] = nc_path
        state["entries"] = entries
        path_var.set(nc_path if nc_path else "/  (raiz do compartilhamento)")
        btn_back.config(state="normal" if nav_stack else "disabled")
        dir_lb.delete(0, "end")
        for e in entries:
            dir_lb.insert("end", _entry_label(e))

    def _go_back() -> None:
        if nav_stack:
            _load(nav_stack.pop())

    def _enter_selected() -> None:
        """Enter a folder on double-click/button. If a file is double-clicked, add it to queue."""
        idxs = dir_lb.curselection()
        if len(idxs) != 1:
            messagebox.showwarning(
                "Selecione uma pasta",
                "Selecione exatamente uma pasta (📁) para entrar.",
                parent=win,
            )
            return
        e = state["entries"][idxs[0]]
        if not e["is_dir"]:
            # Double-click on a file → add it to the queue directly
            _add_to_queue()
            return
        nav_stack.append(state["path"])
        _load(e["path"])

    def _add_to_queue() -> None:
        idxs = dir_lb.curselection()
        if not idxs:
            messagebox.showwarning(
                "Nenhum item selecionado",
                "Selecione pelo menos um item antes de adicionar à fila.",
                parent=win,
            )
            return
        queue_paths = {item["path"] for item in queue}
        added = 0
        for i in idxs:
            e = state["entries"][i]
            if e["path"] not in queue_paths:
                queue.append({
                    "path": e["path"],
                    "is_dir": e["is_dir"],
                    "name": e["name"],
                    "size_bytes": e.get("size_bytes"),
                    "modified": e.get("modified"),
                })
                icon = "📁" if e["is_dir"] else "📄"
                q_lb.insert("end", f"{icon}  {e['path']}")
                added += 1
        _update_queue_label()
        if added == 0:
            messagebox.showinfo(
                "Já na fila",
                "Os itens selecionados já estão na fila de download.",
                parent=win,
            )

    def _remove() -> None:
        idxs = q_lb.curselection()
        for i in reversed(idxs):
            queue.pop(i)
            q_lb.delete(i)
        _update_queue_label()

    def _confirm() -> None:
        if not queue:
            messagebox.showwarning(
                "Fila vazia",
                "Adicione pelo menos um item à fila de download antes de confirmar.",
                parent=win,
            )
            return
        lines = ["Itens selecionados para download:\n"]
        for item in queue:
            icon = "📁" if item["is_dir"] else "📄"
            lines.append(f"  {icon} {item['path']}")
        lines.append(f"\n{len(queue)} item(ns) no total.\nDeseja iniciar o download?")
        if messagebox.askyesno("Confirmar download", "\n".join(lines), parent=win):
            result["paths"] = list(queue)
            win.destroy()

    btn_back.config(command=_go_back)
    btn_enter.config(command=_enter_selected)
    btn_add.config(command=_add_to_queue)
    dir_lb.bind("<Double-Button-1>", lambda _: _enter_selected())
    win.protocol("WM_DELETE_WINDOW", lambda: win.destroy())

    _load("")       # start at the share root
    win.mainloop()
    return result["paths"] or []


if __name__ == "__main__":
    print("Solução desenvolvida por Francisco Costa Carneiro")
    save_dir = _ask_save_directory()
    print(f"Pasta de destino: {save_dir}")
    _ensure_connectivity()

    try:
        with NetworkSession() as _sel_session:
            selected_paths = _browse_nextcloud(_sel_session)
            if not selected_paths:
                print("Nenhum item selecionado. Encerrando.")
                sys.exit(0)
            run(nc_paths=selected_paths, download_dir=save_dir)
    except SystemExit:
        raise
    except Exception as exc:
        logger.exception("Falha inesperada na execução")
        _show_error("RFB Crawler — Erro", f"Erro durante a execução:\n\n{exc}")
        sys.exit(1)
    finally:
        # Keep terminal open so the user can read the summary
        try:
            input("\nPressione ENTER para encerrar...")
        except EOFError:
            pass
