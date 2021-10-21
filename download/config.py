"""
    download.config
    ~~~~~~~~~~~~~~~

    Default configuration module for download application. Update these
    settings with a file specified by DOWNLOAD_SERVICE_SETTINGS environment
    variable.
"""
# Volume mounts
DOWNLOAD_CACHE_DIR = '/mnt/download-service-cache'
IDA_DATA_ROOT = '/mnt/download-ida-storage'

# Database
DATABASE_FILE = '%s/download.db' % DOWNLOAD_CACHE_DIR

# Cache
# ~~~~~
# Variables affecting the automated cache management
GB = 1073741824
CACHE_PURGE_THRESHOLD = GB  # Default to 1GB
CACHE_PURGE_TARGET = GB * 0.75  # Default to 750MB
ENABLE_CACHE_FILE_DELETION = False
ALWAYS_CALCULATE_CACHE_RANKING = False

# Message queue
MQ_HOST = 'download-rabbitmq'
MQ_VHOST = 'download'
MQ_USER = 'download'
MQ_PASS = 'download'

# JWT
JWT_SECRET = 'secret'
JWT_ALGORITHM = 'HS256'
JWT_TTL = 60

# Metax API
METAX_URL = 'https://metax.fd-dev.csc.fi/'
METAX_USER = 'download'
METAX_PASS = 'download'
