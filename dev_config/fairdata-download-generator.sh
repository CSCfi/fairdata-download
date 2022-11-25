#!/usr/bin/env bash
#--------------------------------------------------------------------------------
# This script is only used for local and Docker development
#--------------------------------------------------------------------------------

source /opt/fairdata/fairdata-download-service/dev_config/fairdata-download-generator.env

export FLASK_APP
export FLASK_DEBUG
export DOWNLOAD_SERVICE_SETTINGS

if [ "$DEBUG" = "true" ]; then
    echo "FLASK_APP:                 $FLASK_APP"
    echo "FLASK_DEBUG:               $FLASK_DEBUG"
    echo "DOWNLOAD_SERVICE_SETTINGS: $DOWNLOAD_SERVICE_SETTINGS"
    echo "CELERY_BIN:                $CELERY_BIN"
    echo "CELERY_APP:                $CELERY_APP"
    echo "CELERYD_NODES:             $CELERYD_NODES"
    echo "CELERYD_PID_FILE:          $CELERYD_PID_FILE"
    echo "CELERYD_LOG_FILE:          $CELERYD_LOG_FILE"
    echo "CELERYD_LOG_LEVEL:         $CELERYD_LOG_LEVEL"
    echo "CELERYD_OPTS:              $CELERYD_OPTS"
fi

cd /opt/fairdata/fairdata-download-service

source venv/bin/activate

${CELERY_BIN} multi start ${CELERYD_NODES} \
    -A ${CELERY_APP} --pidfile=${CELERYD_PID_FILE} \
    --logfile=${CELERYD_LOG_FILE} --loglevel=${CELERYD_LOG_LEVEL} \
    ${CELERYD_OPTS}
