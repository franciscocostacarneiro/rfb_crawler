"""Network layer: HTTP session management and streaming file downloads."""

import hashlib
import logging
import shutil
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from crawler.config import CHUNK_SIZE, MAX_RETRIES, TIMEOUT
from crawler.exceptions import DiskFullError, RFBConnectionError

logger = logging.getLogger(__name__)


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
    session.headers["User-Agent"] = (
        "Mozilla/5.0 (compatible; RFBCrawler/1.0; +https://github.com)"
    )
    return session


class NetworkSession:
    """Reusable HTTP session with automatic retries and streaming support."""

    def __init__(self) -> None:
        self._session = _build_session()

    def get(self, url: str, **kwargs) -> requests.Response:
        try:
            response = self._session.get(url, timeout=TIMEOUT, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.Timeout as exc:
            raise RFBConnectionError(f"Timeout connecting to {url}") from exc
        except requests.exceptions.ConnectionError as exc:
            raise RFBConnectionError(f"Connection failed for {url}") from exc
        except requests.exceptions.HTTPError as exc:
            raise RFBConnectionError(
                f"HTTP {exc.response.status_code} for {url}"
            ) from exc

    def stream_download(self, url: str, dest: Path) -> tuple[int, str]:
        """Download *url* to *dest* in streaming chunks.

        Returns:
            (bytes_written, sha256_hex) tuple.

        Raises:
            RFBConnectionError: on network failure.
            DiskFullError: when the disk runs out of space during write.
        """
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".part")

        try:
            response = self._session.get(url, stream=True, timeout=TIMEOUT)
            response.raise_for_status()
        except requests.exceptions.Timeout as exc:
            raise RFBConnectionError(f"Timeout starting download: {url}") from exc
        except requests.exceptions.RequestException as exc:
            raise RFBConnectionError(f"Download failed for {url}: {exc}") from exc

        sha256 = hashlib.sha256()
        bytes_written = 0

        try:
            with tmp.open("wb") as fh:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if not chunk:
                        continue
                    try:
                        fh.write(chunk)
                    except OSError as exc:
                        _check_disk_space(dest.parent, exc)
                    sha256.update(chunk)
                    bytes_written += len(chunk)
        except Exception:
            tmp.unlink(missing_ok=True)
            raise

        tmp.replace(dest)
        logger.debug("Downloaded %s → %s (%d bytes)", url, dest, bytes_written)
        return bytes_written, sha256.hexdigest()

    def close(self) -> None:
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def _check_disk_space(directory: Path, original_exc: OSError) -> None:
    usage = shutil.disk_usage(directory)
    if usage.free < CHUNK_SIZE:
        raise DiskFullError(
            f"Disk full: only {usage.free / 1e6:.1f} MB free in {directory}"
        ) from original_exc
    raise original_exc
