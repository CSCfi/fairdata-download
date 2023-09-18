#!/usr/bin/env bash
#--------------------------------------------------------------------------------
# This script is only used for Docker development
#--------------------------------------------------------------------------------

TZ="UTC"

source /opt/fairdata/fairdata-download-service/dev_config/fairdata-download-server.env

export TZ
export FLASK_APP
export FLASK_DEBUG
export DOWNLOAD_SERVICE_SETTINGS

if [ "$DEBUG" = "true" ]; then
    echo "FLASK_APP:                 $FLASK_APP"
    echo "FLASK_DEBUG:               $FLASK_DEBUG"
    echo "DOWNLOAD_SERVICE_SETTINGS: $DOWNLOAD_SERVICE_SETTINGS"
    echo "GUNICORN_BIN:              $GUNICORN_BIN"
    echo "GUNICORN_CONF:             $GUNICORN_CONF"
    echo "GUNICORN_LOGGING_CONF:     $GUNICORN_LOGGING_CONF"
fi

cd /opt/fairdata/fairdata-download-service

source venv/bin/activate

${GUNICORN_BIN} --config ${GUNICORN_CONF} --certfile=/etc/pki/tls/certs/ssl.crt.pem --keyfile=/etc/pki/tls/private/ssl.key.pem --bind 0.0.0.0:4431 download:flask_app
