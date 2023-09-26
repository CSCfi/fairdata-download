#!/usr/bin/env bash
#--------------------------------------------------------------------------------
# This script is used to configure and manage a local development instance of
# the download service. It should NOT be used in production.
#--------------------------------------------------------------------------------

ID=`id -u -n`
if [ "$ID" != "root" ]; then
    echo "You must execute this script as root" >&2
    exit 1
fi

umask 007

#DEBUG="true" # TEMP HACK

SCRIPT="$(realpath $0)"
UTILS=`dirname "$SCRIPT"`
DEV=`dirname "$UTILS"`
ROOT=`dirname "$DEV"`

if [ -z "$DOWNLOAD_SERVICE_SETTINGS" ]; then
    if [ -d "$ROOT/config" ]; then
        DOWNLOAD_SERVICE_SETTINGS="$ROOT/config/settings.cfg"
    else
        DOWNLOAD_SERVICE_SETTINGS="$DEV/settings.cfg"
    fi
fi

if [ -z "$DOWNLOAD_SERVICE_VENV" ]; then
    DOWNLOAD_SERVICE_VENV="$ROOT/venv"
fi

if [ ! -f "$DOWNLOAD_SERVICE_SETTINGS" ]; then
    echo "Could not find configuration file $DOWNLOAD_SERVICE_SETTINGS" >&2
    exit 1
fi

source $DOWNLOAD_SERVICE_SETTINGS

if [ "$ENVIRONMENT" = "" ]; then
    echo "No ENVIRONMENT defined" >&2
    exit 1
fi

if [ "$ENVIRONMENT" != "DEV" ]; then
    if [ "$ENVIRONMENT" != "TEST" ]; then
        echo "This script should only be executed in a development or test environment!" >&2
        exit 1
    fi
fi

METAX_VERSION=$(echo "$METAX_URL" | grep '/v3/')
if [ -n "$METAX_VERSION" ]; then
    METAX_VERSION=3
else
    METAX_VERSION=1
fi

if [ "$DEBUG" = "true" ]; then
    echo "ENVIRONMENT:   $ENVIRONMENT"
    echo "SETTINGS:      $DOWNLOAD_SERVICE_SETTINGS"
    echo "SCRIPT:        $SCRIPT"
    echo "ROOT:          $ROOT"
    echo "UTILS:         $UTILS"
    echo "METAX_URL:     $METAX_URL"
    echo "METAX_VERSION: $METAX_VERSION"
fi

export METAX_URL
export METAX_VERSION
export DOWNLOAD_SERVICE_SETTINGS
export TRUSTED_SERVICE_TOKEN
