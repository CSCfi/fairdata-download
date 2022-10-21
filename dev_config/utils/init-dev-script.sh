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

SCRIPT="$(realpath $0)"
UTILS=`dirname "$SCRIPT"`
DEV=`dirname "$UTILS"`
ROOT=`dirname "$DEV"`

DOWNLOAD_SERVICE_SETTINGS="$DEV/settings.cfg"

if [ ! -f "$DOWNLOAD_SERVICE_SETTINGS" ]; then
    echo "Could not find environment variable configuration file $DOWNLOAD_SERVICE_SETTINGS" >&2
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

if [ "$DEBUG" = "true" ]; then
    echo "ENVIRONMENT: $ENVIRONMENT"
    echo "SETTINGS:    $DOWNLOAD_SERVICE_SETTINGS"
    echo "SCRIPT:      $SCRIPT"
    echo "ROOT:        $ROOT"
    echo "UTILS:       $UTILS"
fi
