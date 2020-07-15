"""
    download.config
    ~~~~~~~~~~~~~~~

    Default configuration module for download application. Update these
    settings with a file specified by DOWNLOAD_SERVICE_SETTINGS environment
    variable.
"""
# Volume mounts
DOWNLOAD_CACHE_DIR = 'download-cache'

# Database
DATABASE_FILE = 'download.db'

# Message queue
MQ_HOST = 'localhost'
MQ_VHOST = 'download'
MQ_USER = 'download'
MQ_PASS = 'download'
