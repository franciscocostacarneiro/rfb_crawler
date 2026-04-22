"""
RFB Crawler — single-file version.
Crawls CAFIR, CNO, CNPJ and SISEN from https://dadosabertos.rfb.gov.br/
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Standard library
# ---------------------------------------------------------------------------
import csv
import hashlib
import json
import logging
import re
import shutil
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

# ---------------------------------------------------------------------------
# Third-party
# ---------------------------------------------------------------------------
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===========================================================================
# CONFIG
# ===========================================================================

BASE_URL = "https://dadosabertos.rfb.gov.br"
DATASETS = ["CNPJ", "CAFIR", "CNO", "SISEN"]
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

    def stream_download(self, url: str, dest: Path) -> tuple[int, str]:
        """Download url → dest in streaming chunks. Returns (bytes, sha256)."""
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".part")

        try:
            resp = self._session.get(url, stream=True, timeout=TIMEOUT)
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

    def close(self) -> None:
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

# ===========================================================================
# SCRAPER
# ===========================================================================

_SKIP_HREFS = {"../", "/", "?C=N;O=D", "?C=M;O=A", "?C=S;O=A", "?C=D;O=A"}


def _parse_size(raw: str) -> int | None:
    raw = raw.strip()
    if not raw or raw == "-":
        return None
    units = {"K": 1024, "M": 1024 ** 2, "G": 1024 ** 3, "T": 1024 ** 4}
    m = re.fullmatch(r"([\d.]+)\s*([KMGT]?)", raw, re.IGNORECASE)
    if not m:
        return None
    return int(float(m.group(1)) * units.get(m.group(2).upper(), 1))


def _parse_listing(html: str, base_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    raw = []

    rows = soup.select("table tr")
    if rows:
        for row in rows:
            cells = row.find_all("td")
            anchor = (cells[0].find("a") if cells else None) or row.find("a")
            if not anchor:
                continue
            href = anchor.get("href", "")
            if href in _SKIP_HREFS or href.startswith(("?", "/")):
                continue
            raw.append({
                "name": anchor.get_text(strip=True),
                "href": href,
                "modified": cells[1].get_text(strip=True) if len(cells) > 1 else None,
                "size_raw": cells[2].get_text(strip=True) if len(cells) > 2 else None,
            })
    else:
        pre = soup.find("pre")
        anchors = pre.find_all("a", href=True) if pre else soup.find_all("a", href=True)
        for anchor in anchors:
            href = anchor["href"]
            if href in _SKIP_HREFS or href.startswith(("?", "/")):
                continue
            sibling_text = "".join(
                str(s) for s in anchor.next_siblings if not getattr(s, "name", None)
            )
            parts = sibling_text.split()
            raw.append({
                "name": anchor.get_text(strip=True),
                "href": href,
                "modified": " ".join(parts[:2]) if len(parts) >= 2 else None,
                "size_raw": parts[2] if len(parts) >= 3 else None,
            })

    return [
        {
            "name": e["name"].rstrip("/"),
            "url": urljoin(base_url, e["href"]),
            "is_dir": e["href"].endswith("/"),
            "modified": e.get("modified"),
            "size_bytes": _parse_size(e.get("size_raw") or ""),
        }
        for e in raw
    ]


@dataclass
class DirectoryEntry:
    name: str
    url: str
    is_dir: bool
    parent_folder: str
    size_bytes: int | None = None
    modified: str | None = None
    hierarchy: list[str] = field(default_factory=list)


class DirectoryScraper:
    def __init__(self, session: NetworkSession) -> None:
        self._session = session

    def list_directory(self, url: str) -> list[dict]:
        return _parse_listing(self._session.get(url).text, url)

    def crawl(self, base_url: str, dataset: str, hierarchy: list[str] | None = None):
        if hierarchy is None:
            hierarchy = [dataset]

        logger.info("Scanning %s", base_url)
        try:
            entries = self.list_directory(base_url)
        except RFBConnectionError as exc:
            logger.error("Cannot list %s: %s", base_url, exc)
            return

        if not entries and hierarchy == [dataset]:
            raise SchemaChangeError(
                f"No entries at {base_url} — portal structure may have changed."
            )

        for e in entries:
            if e["is_dir"]:
                yield from self.crawl(e["url"], dataset, hierarchy + [e["name"]])
            else:
                yield DirectoryEntry(
                    name=e["name"],
                    url=e["url"],
                    is_dir=False,
                    parent_folder=hierarchy[-1] if len(hierarchy) > 1 else dataset,
                    size_bytes=e["size_bytes"],
                    modified=e["modified"],
                    hierarchy=list(hierarchy),
                )

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

    def _load_manifest(self) -> dict:
        if self._manifest_path.exists():
            try:
                return json.loads(self._manifest_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                logger.warning("Manifest corrupted — starting fresh.")
        return {}

    def save_manifest(self) -> None:
        self._manifest_path.write_text(
            json.dumps(self._manifest, indent=2, ensure_ascii=False), encoding="utf-8"
        )

    def is_known(self, file_hash: str) -> bool:
        return file_hash in self._manifest

    def register_file(self, entry: DirectoryEntry, local_path: Path,
                      file_hash: str, bytes_written: int) -> None:
        self._manifest[file_hash] = {
            "name": entry.name,
            "source_url": entry.url,
            "parent_folder": entry.parent_folder,
            "hierarchy": entry.hierarchy,
            "local_path": str(local_path.relative_to(self.root)),
            "size_bytes": bytes_written,
            "file_hash": file_hash,
            "download_timestamp": datetime.now(timezone.utc).isoformat(),
        }

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
                   governance: DataGovernance, run_id: str) -> dict:
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

    if local.exists():
        h = _sha256_file(local)
        if governance.is_known(h):
            row.update(status="skipped", file_hash=h)
            logger.debug("Skipped (cached): %s", entry.name)
            return row

    try:
        bytes_written, file_hash = session.stream_download(entry.url, local)
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


def run(datasets: list[str] | None = None, download_dir: Path = DOWNLOAD_DIR) -> None:
    run_id = uuid.uuid4().hex[:8]
    t0 = time.monotonic()
    logger.info("=== RFB Crawler started | run_id=%s ===", run_id)

    governance = DataGovernance(root=download_dir)
    all_rows: list[dict] = []
    all_stats: list[dict] = []

    with NetworkSession() as session:
        scraper = DirectoryScraper(session)

        for dataset in (datasets or DATASETS):
            logger.info("--- Dataset: %s ---", dataset)
            stats = {"dataset": dataset, "new": 0, "skipped": 0, "errors": 0, "bytes": 0}
            url = f"{BASE_URL}/{dataset}/"

            try:
                entries = list(scraper.crawl(url, dataset))
            except SchemaChangeError as exc:
                logger.error("Schema change in %s: %s", dataset, exc)
                stats["errors"] += 1
                all_stats.append(stats)
                continue
            except RFBConnectionError as exc:
                logger.error("Cannot crawl %s: %s", dataset, exc)
                stats["errors"] += 1
                all_stats.append(stats)
                continue

            logger.info("%s: %d files found", dataset, len(entries))

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                futures = {
                    pool.submit(_download_file, e, session, governance, run_id): e
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
                    f"[RFB Crawler] {dataset}: {stats['new']} new file(s), "
                    f"{stats['bytes'] / 1e9:.2f} GB"
                )

            governance.apply_retention(dataset)
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


def _check_connectivity(timeout: int = 8) -> tuple[bool, str]:
    """Quick reachability check to the RFB portal.

    Returns (ok, message). Uses os.environ proxy settings (HTTP_PROXY,
    HTTPS_PROXY) automatically via requests.
    """
    try:
        resp = requests.head(
            BASE_URL + "/",
            timeout=(timeout, timeout),
            allow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; RFBCrawler/1.0)"},
        )
        if resp.status_code < 500:
            return True, f"HTTP {resp.status_code}"
        return False, f"HTTP {resp.status_code}"
    except requests.exceptions.Timeout:
        return False, "Timeout (servidor não respondeu)"
    except requests.exceptions.ConnectionError as exc:
        return False, f"Conexão recusada/bloqueada: {exc.__class__.__name__}"
    except requests.exceptions.RequestException as exc:
        return False, f"Erro: {exc}"


def _ask_proxy() -> str | None:
    """Ask the user for an optional proxy URL. Returns None if skipped."""
    import os
    from tkinter import simpledialog, messagebox

    root = _gui_root()
    answer = messagebox.askyesno(
        "RFB Crawler — Sem conectividade",
        "Não foi possível conectar ao servidor da Receita Federal\n"
        f"({BASE_URL}).\n\n"
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
    """Pre-flight check. Aborts (with GUI message) if RFB is unreachable."""
    print(f"Verificando conectividade com {BASE_URL} ...")
    ok, info = _check_connectivity()
    if ok:
        print(f"  Conectividade OK ({info}).")
        return

    print(f"  FALHA: {info}")
    proxy = _ask_proxy()
    if proxy:
        print(f"Tentando novamente via proxy: {proxy}")
        ok, info = _check_connectivity(timeout=15)
        if ok:
            print(f"  Conectividade via proxy OK ({info}).")
            return
        print(f"  Ainda sem conexão ({info}).")

    _show_error(
        "RFB Crawler — Sem conexão",
        "Não foi possível conectar ao servidor da Receita Federal:\n"
        f"  {BASE_URL}\n\n"
        f"Detalhe técnico: {info}\n\n"
        "Verifique com a equipe de TI:\n"
        "  • Liberação de acesso ao IP/domínio dadosabertos.rfb.gov.br "
        "(porta 443/HTTPS)\n"
        "  • Configuração de proxy corporativo (se houver)\n"
        "  • Regras de firewall/antivírus\n\n"
        "O programa será encerrado.",
    )
    sys.exit(2)


if __name__ == "__main__":
    save_dir = _ask_save_directory()
    print(f"Pasta de destino: {save_dir}")
    _ensure_connectivity()
    try:
        run(download_dir=save_dir)
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
