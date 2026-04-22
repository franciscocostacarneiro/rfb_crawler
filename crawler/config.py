from pathlib import Path

# Base URL of the RFB open data portal
BASE_URL = "https://dadosabertos.rfb.gov.br"

# Top-level datasets to crawl
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
