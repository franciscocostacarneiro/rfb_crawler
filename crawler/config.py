from pathlib import Path

# ---------------------------------------------------------------------------
# Portal 1: Apache directory listing (dadosabertos.rfb.gov.br)
# ---------------------------------------------------------------------------
APACHE_BASE_URL = "https://dadosabertos.rfb.gov.br"

# ---------------------------------------------------------------------------
# Portal 2: Nextcloud public share (arquivos.receitafederal.gov.br)
# ---------------------------------------------------------------------------
NC_BASE_URL = "https://arquivos.receitafederal.gov.br"
NC_SHARE_TOKEN = "gn672Ad4CF8N6TK"
NC_SHARE_ROOT = "/Dados/Cadastros"   # path inside the share

# Active portal — "nextcloud" is the current working portal.
# Switch to "apache" when dadosabertos.rfb.gov.br is available again.
PORTAL = "nextcloud"

# Backwards-compatible alias (used by scraper/main for the Apache portal)
BASE_URL = APACHE_BASE_URL

# Top-level datasets to crawl (same names in both portals)
DATASETS = ["CNPJ", "CAFIR", "CNO", "SISEN"]

# Local download root
DOWNLOAD_DIR = Path("data")

# Parallel download workers (keep low to avoid being blocked)
MAX_WORKERS = 3

# Streaming chunk size: 8 MB
CHUNK_SIZE = 8 * 1024 * 1024

# HTTP timeouts (connect, read) in seconds
TIMEOUT = (10, 60)

# Retry attempts on transient HTTP errors
MAX_RETRIES = 3

# Number of competência periods to retain per dataset (older ones are removed)
RETENTION_COUNT = 3

# Optional webhook URL for new-competência notifications (set to None to disable)
WEBHOOK_URL = None
