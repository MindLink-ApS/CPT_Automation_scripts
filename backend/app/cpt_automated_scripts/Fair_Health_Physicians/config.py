import os

# Geozip batches - each will be processed separately
GEOZIP_BATCHES = [
    ["070"],  # Batch 1: New Jersey area
    ["usa"]   # Batch 2: National data
]

# Fair Health credentials - loaded from environment variables
FAIRHEALTH_URL = "https://fhonline.fairhealth.org/login"
EMAIL = os.getenv("FAIRHEALTH_EMAIL")
PASSWORD = os.getenv("FAIRHEALTH_PASSWORD")

# Proxy configuration - loaded from environment variables
PROXY_SERVER = os.getenv("PROXY_SERVER")
PROXY_USERNAME = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")

# Validate required credentials
if not EMAIL or not PASSWORD:
    raise ValueError(
        "Missing required FairHealth credentials. "
        "Please set FAIRHEALTH_EMAIL and FAIRHEALTH_PASSWORD environment variables."
    )

# Product selection
PRODUCT_CATEGORY = "FH Benchmarks"
PRODUCT_NAME = "Charge Medical"

# Download settings
DOWNLOAD_DIR_NAME = "downloads_physicians"
HEADLESS = True  # Set to True for production