"""
    download.config
    ~~~~~~~~~~~~~~~

    Default configuration module for download application. Update these
    settings with a file specified by DOWNLOAD_SERVICE_SETTINGS environment
    variable.
"""

from dotenv import load_dotenv, dotenv_values
import os
import pprint

load_dotenv()

# Volume mounts
DOWNLOAD_CACHE_DIR = os.environ.get("DOWNLOAD_CACHE_DIR", "/mnt/download-service-cache")
IDA_DATA_ROOT = os.environ.get("IDA_DATA_ROOT", "/mnt/download-ida-storage")

# Database
DATABASE_FILE = os.environ.get("DATABASE_FILE", f"{DOWNLOAD_CACHE_DIR}/download.db")

# Message queue
MQ_HOST = os.environ.get("MQ_HOST", "0.0.0.0")
MQ_VHOST = os.environ.get("MQ_VHOST", "download")
MQ_USER = os.environ.get("MQ_USER", "download")
MQ_PASS = os.environ.get("MQ_PASS", "download")

# JWT
JWT_SECRET = os.environ.get("JWT_SECRET", "secret")
JWT_ALGORITHM = os.environ.get("JWT_ALGORITHM", "HS256")
JWT_TTL = os.environ.get("JWT_TTL", 60)

# Metax API
METAX_URL = os.environ.get("METAX_URL", "https://metax.fd-dev.csc.fi/")
METAX_USER = os.environ.get("METAX_USER", "download")
METAX_PASS = os.environ.get("METAX_PASS", "download")

DEBUG = os.environ.get("DEBUG", False)

if DEBUG:
    pprint.pprint(dotenv_values(".env"))

