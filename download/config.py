# download.config
#
# Default configuration module for download application. Update these
# settings with a file specified by DOWNLOAD_SETTINGS environment
# variable.
# 
# Any values defined in this file will be overwritten by values defined
# in DOWNLOAD_SETTINGS, if an equivalent value is defined there.
# 
# If an equivalent value is not defined in DOWNLOAD_SETTINGS,
# the value defined in this default config.py template configurtion will
# be used instead.
# 
# The values can be retrieved via e.g. current_app.config('{{ VALUE }}')
# 
# NOTE: Care should be taken to define values in this file in a manner
# which is compatible with both bash environment variable definitions,
# and python modules, so that the file can be utilized both by python
# and bash utilities.

# Volume mounts
DOWNLOAD_CACHE_DIR='/mnt/download-cache'
IDA_DATA_ROOT='/mnt/download-ida-storage'

# Database
DATABASE_FILE='/mnt/download-cache/download.db'

# Cache
# Variables affecting the automated cache management
CACHE_PURGE_THRESHOLD=1073741824 # Default to 1GB
CACHE_PURGE_TARGET=786432000     # Default to 750MB

# Message queue
MQ_HOST='download-rabbitmq'
MQ_VHOST='download'
MQ_USER='download'
MQ_PASS='download'

# JWT
JWT_SECRET='secret'
JWT_ALGORITHM='HS256'
JWT_TTL=4320 # 72 hours

# Metax API
METAX_URL='https://metax.fd-dev.csc.fi/'
METAX_USER='download'
METAX_PASS='download'

# Trusted services
TRUSTED_SERVICE_TOKEN='secret'
