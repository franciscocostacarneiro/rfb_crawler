"""Scraper layer: parse Apache/Nginx directory listings and DFS-crawl datasets."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from crawler.exceptions import RFBConnectionError, SchemaChangeError
from crawler.network import NetworkSession

logger = logging.getLogger(__name__)

# Anchors that are never real entries
_SKIP_HREFS = {"../", "/", "?C=N;O=D", "?C=M;O=A", "?C=S;O=A", "?C=D;O=A"}
_SKIP_PREFIXES = ("?", "/")


@dataclass
class DirectoryEntry:
    name: str
    url: str
    is_dir: bool
    parent_folder: str
    size_bytes: int | None = None
    modified: str | None = None
    hierarchy: list[str] = field(default_factory=list)


def _parse_size(raw: str) -> int | None:
    """Convert human-readable size strings like '123M', '4.5G', '789K' to bytes."""
    raw = raw.strip()
    if not raw or raw == "-":
        return None
    units = {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
    match = re.fullmatch(r"([\d.]+)\s*([KMGT]?)", raw, re.IGNORECASE)
    if not match:
        return None
    value = float(match.group(1))
    unit = match.group(2).upper()
    return int(value * units.get(unit, 1))


def _parse_listing(html: str, base_url: str) -> list[dict]:
    """Extract entries from an Apache/Nginx autoindex HTML page."""
    soup = BeautifulSoup(html, "lxml")

    entries = []
    # Apache wraps entries in <pre> or <table>; try table rows first
    rows = soup.select("table tr") or []
    if rows:
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            anchor = cells[0].find("a") or row.find("a")
            if not anchor:
                continue
            href = anchor.get("href", "")
            if href in _SKIP_HREFS or href.startswith(_SKIP_PREFIXES):
                continue
            modified = cells[1].get_text(strip=True) if len(cells) > 1 else None
            size_raw = cells[2].get_text(strip=True) if len(cells) > 2 else None
            entries.append(
                {
                    "name": anchor.get_text(strip=True),
                    "href": href,
                    "modified": modified,
                    "size_raw": size_raw,
                }
            )
    else:
        # Fallback: parse <pre> block (classic Apache)
        pre = soup.find("pre")
        if pre is None:
            # Try all anchors as last resort
            for anchor in soup.find_all("a", href=True):
                href = anchor["href"]
                if href in _SKIP_HREFS or href.startswith(_SKIP_PREFIXES):
                    continue
                entries.append(
                    {"name": anchor.get_text(strip=True), "href": href,
                     "modified": None, "size_raw": None}
                )
        else:
            for anchor in pre.find_all("a", href=True):
                href = anchor["href"]
                if href in _SKIP_HREFS or href.startswith(_SKIP_PREFIXES):
                    continue
                # Text after the anchor on the same line contains date and size
                sibling_text = ""
                for sibling in anchor.next_siblings:
                    if sibling.name:
                        break
                    sibling_text += str(sibling)
                parts = sibling_text.split()
                modified = " ".join(parts[:2]) if len(parts) >= 2 else None
                size_raw = parts[2] if len(parts) >= 3 else None
                entries.append(
                    {"name": anchor.get_text(strip=True), "href": href,
                     "modified": modified, "size_raw": size_raw}
                )

    if not entries:
        logger.warning("No entries found at %s — possible schema change", base_url)

    result = []
    for e in entries:
        url = urljoin(base_url, e["href"])
        is_dir = e["href"].endswith("/")
        result.append(
            {
                "name": e["name"].rstrip("/"),
                "url": url,
                "is_dir": is_dir,
                "modified": e.get("modified"),
                "size_bytes": _parse_size(e.get("size_raw") or ""),
            }
        )
    return result


class DirectoryScraper:
    """DFS crawler over RFB directory listings."""

    def __init__(self, session: NetworkSession) -> None:
        self._session = session

    def list_directory(self, url: str) -> list[dict]:
        """Fetch and parse one directory page. Returns list of entry dicts."""
        try:
            response = self._session.get(url)
        except RFBConnectionError:
            raise
        html = response.text
        entries = _parse_listing(html, url)
        return entries

    def crawl(
        self,
        base_url: str,
        dataset_name: str,
        hierarchy: list[str] | None = None,
    ):
        """Recursively yield :class:`DirectoryEntry` objects via DFS.

        Directories are entered depth-first; their path is recorded in
        ``hierarchy`` so downstream code knows the competência context.
        """
        if hierarchy is None:
            hierarchy = [dataset_name]

        logger.info("Scanning %s", base_url)
        try:
            entries = self.list_directory(base_url)
        except RFBConnectionError as exc:
            logger.error("Cannot list %s: %s", base_url, exc)
            return

        if not entries and hierarchy == [dataset_name]:
            raise SchemaChangeError(
                f"Dataset '{dataset_name}' returned no entries at {base_url}. "
                "The portal structure may have changed."
            )

        for entry in entries:
            if entry["is_dir"]:
                child_hierarchy = hierarchy + [entry["name"]]
                yield from self.crawl(entry["url"], dataset_name, child_hierarchy)
            else:
                parent_folder = hierarchy[-1] if len(hierarchy) > 1 else dataset_name
                yield DirectoryEntry(
                    name=entry["name"],
                    url=entry["url"],
                    is_dir=False,
                    parent_folder=parent_folder,
                    size_bytes=entry["size_bytes"],
                    modified=entry["modified"],
                    hierarchy=list(hierarchy),
                )
