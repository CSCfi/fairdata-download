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
ROOT=`dirname "$UTILS"`

if [ -z "$DOWNLOAD_SETTINGS" ]; then
    if [ -d "$ROOT/config" ]; then
        DOWNLOAD_SETTINGS="$ROOT/config/settings.cfg"
    else
        DOWNLOAD_SETTINGS="$ROOT/dev_config/settings.cfg"
    fi
fi

if [ -z "$DOWNLOAD_SERVICE_VENV" ]; then
    DOWNLOAD_SERVICE_VENV="$ROOT/venv"
fi

if [ ! -f "$DOWNLOAD_SETTINGS" ]; then
    echo "Could not find configuration file $DOWNLOAD_SETTINGS" >&2
    exit 1
fi

source $DOWNLOAD_SETTINGS

if [ "$ENVIRONMENT" = "" ]; then
    echo "No ENVIRONMENT defined" >&2
    exit 1
fi

if [ "$ENVIRONMENT" = "PRODUCTION" ]; then
    echo "This script should NOT be executed in production!" >&2
    exit 1
fi

METAX_URL=$(echo "$METAX_URL" | sed -e 's/\/$//')
IDA_URL=$(echo "$IDA_URL" | sed -e 's/\/$//')

METAX_VERSION=$(echo "$METAX_URL" | grep '/v3')
if [ -n "$METAX_VERSION" ]; then
    METAX_VERSION=3
else
    METAX_VERSION=1
fi

if [ -d /etc/httpd ]; then
    HTTPD_USER="apache"
else
    HTTPD_USER="www-data"
fi

if [ "$DEBUG" = "true" ]; then
    echo "ENVIRONMENT:           $ENVIRONMENT"
    echo "SCRIPT:                $SCRIPT"
    echo "ROOT:                  $ROOT"
    echo "UTILS:                 $UTILS"
    echo "HTTPD_USER:            $HTTPD_USER"
    echo "TRUSTED_SERVICE_TOKEN: $TRUSTED_SERVICE_TOKEN"
    echo "DOWNLOAD_HOST:         $DOWNLOAD_HOST"
    echo "DOWNLOAD_SETTINGS:     $DOWNLOAD_SETTINGS"
    echo "DOWNLOAD_CACHE_DIR:    $DOWNLOAD_CACHE_DIR"
    echo "METAX_URL:             $METAX_URL"
    echo "METAX_VERSION:         $METAX_VERSION"
    echo "METAX_USER:            $METAX_USER"
    echo "IDA_DATA_ROOT:         $IDA_DATA_ROOT"
    echo "IDA_URL:               $IDA_URL"
    echo "IDA_TEST_PROJECT:      $IDA_TEST_PROJECT"
    echo "IDA_TEST_USER:         $IDA_TEST_USER"
    echo "IDA_ADMIN_USER:        $IDA_ADMIN_USER"
fi

export ROOT
export HTTPD_USER
export DOWNLOAD_HOST
export DOWNLOAD_SETTINGS
export DOWNLOAD_CACHE_DIR
export TRUSTED_SERVICE_TOKEN
export METAX_URL
export METAX_VERSION
export METAX_USER
export METAX_PASS
export IDA_DATA_ROOT
export IDA_URL
export IDA_TEST_PROJECT
export IDA_TEST_USER
export IDA_TEST_PASS
export IDA_ADMIN_USER
export IDA_ADMIN_PASS
