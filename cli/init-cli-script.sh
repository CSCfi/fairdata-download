#!/usr/bin/env bash
#--------------------------------------------------------------------------------
# This script is used to configure and manage CLI scripts which may be executed
# both in production and non-production environments. 
#--------------------------------------------------------------------------------

ID=`id -u -n`
if [ "$ID" != "root" ]; then
    echo "You must execute this script as root" >&2
    exit 1
fi

SCRIPT="$(realpath $0)"
CLI=`dirname "$SCRIPT"`
ROOT=`dirname "$CLI"`

if [ -z "$DOWNLOAD_SERVICE_SETTINGS" ]; then
    DOWNLOAD_SERVICE_SETTINGS="$ROOT/dev_config/settings.cfg"
fi

if [ -z "$DOWNLOAD_SERVICE_VENV" ]; then
    DOWNLOAD_SERVICE_VENV="$ROOT/venv"
fi

if [ ! -f "$DOWNLOAD_SERVICE_SETTINGS" ]; then
    echo "Could not find configuration file $DOWNLOAD_SERVICE_SETTINGS" >&2
    exit 1
fi

if [ ! -d "$DOWNLOAD_SERVICE_VENV" ]; then
    echo "Could not find python3 virtual environment $DOWNLOAD_SERVICE_VENV" >&2
    exit 1
fi

source $DOWNLOAD_SERVICE_SETTINGS

FLASK_APP="download"

if [ "$DEBUG" = "true" ]; then
    echo "ENVIRONMENT: $ENVIRONMENT"
    echo "SCRIPT:      $SCRIPT"
    echo "ROOT:        $ROOT"
    echo "CLI:         $CLI"
    echo "SETTINGS:    $DOWNLOAD_SERVICE_SETTINGS"
    echo "VENV:        $DOWNLOAD_SERVICE_VENV"
    echo "FLASK_APP:   $FLASK_APP"
fi

export FLASK_APP
export DOWNLOAD_SERVICE_SETTINGS
export DOWNLOAD_SERVICE_VENV
export TRUSTED_SERVICE_TOKEN
