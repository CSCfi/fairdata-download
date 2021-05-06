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

# Message queue
MQ_HOST = '0.0.0.0'
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
