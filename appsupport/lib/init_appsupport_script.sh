#!/usr/bin/env bash
#--------------------------------------------------------------------------------
# Provides configuration initialization to scripts located one directory level
# down from the repository root.
#--------------------------------------------------------------------------------

ID=`id -u -n`
if [ "$ID" != "download" ]; then
    echo "You must execute this script as download" >&2
    exit 1
fi

umask 007

# /CLONED_REPOSITORY_ROOT/lib/init_script.sh
INIT_SCRIPT_PATHNAME=$(realpath "${BASH_SOURCE[0]}")
INIT_SCRIPT_DIR=$(dirname "$INIT_SCRIPT_PATHNAME")
INIT_SCRIPT=$(basename "$INIT_SCRIPT_PATHNAME")
APPSUPPORT_DIR=$(dirname "$INIT_SCRIPT_DIR")
ROOT=$(dirname "$APPSUPPORT_DIR")

if [ -z "$SCRIPT" ]; then
    SCRIPT_PATHNAME=$(realpath "$0")
    SCRIPT_DIR=$(dirname "$SCRIPT_PATHNAME")
    SCRIPT=$(basename "$SCRIPT_PATHNAME")
fi

if [ -z "$USAGE" ]; then
    USAGE="
Usage: $SCRIPT [-h]
"
fi

if [ "$1" = "-h" ]; then
    echo "$USAGE"
    exit 0
fi

#--------------------------------------------------------------------------------

if [ -z "$DOWNLOAD_SETTINGS" ]; then
    if [ -f "$ROOT/settings.cfg" ]; then
        DOWNLOAD_SETTINGS="$ROOT/settings.cfg"
    elif [ -d "$ROOT/config" ]; then
        DOWNLOAD_SETTINGS="$ROOT/config/settings.cfg"
    else
        DOWNLOAD_SETTINGS="$ROOT/dev_config/settings.cfg"
    fi
fi

if [ ! -f "$DOWNLOAD_SETTINGS" ]; then
    echo "Could not find configuration file $DOWNLOAD_SETTINGS" >&2
    exit 1
fi

source "$DOWNLOAD_SETTINGS"

#--------------------------------------------------------------------------------

verify_required_value() {

    local VARNAME="$1"

    if [[ -z "$VARNAME" ]]; then
        echo "Missing name parameter" >&2
        exit 1
    fi

    local VALUE="${!VARNAME}"

    if [[ -z "$VALUE" ]]; then
        echo "Error: $VARNAME is missing or empty" >&2
        exit 1
    fi
}

#--------------------------------------------------------------------------------

verify_required_value "ENVIRONMENT"
verify_required_value "TRUSTED_SERVICE_TOKEN"
verify_required_value "DOWNLOAD_HOST"
verify_required_value "DOWNLOAD_SETTINGS"
verify_required_value "DOWNLOAD_CACHE_DIR"
verify_required_value "METAX_URL"
verify_required_value "METAX_PASS"
verify_required_value "IDA_DATA_ROOT"
verify_required_value "IDA_URL"

FLASK_APP="download"
METAX_URL=$(echo "$METAX_URL" | sed -e 's/\/$//')
IDA_URL=$(echo "$IDA_URL" | sed -e 's/\/$//')

if [ -z "$DOWNLOAD_VENV" ]; then
    DOWNLOAD_VENV="$ROOT/venv"
fi

if [ ! -d "$DOWNLOAD_VENV" ]; then
    echo "Could not find python virtual environment $DOWNLOAD_VENV" >&2
    exit 1
fi

if [ ! -d "$DOWNLOAD_CACHE_DIR" ]; then
    echo "The specified download cache directory does not exist" >&2
    exit 1
fi

if [ ! -d "$IDA_DATA_ROOT" ]; then
    echo "The specified IDA data root directory does not exist" >&2
    exit 1
fi

if [ "$DEBUG" = "true" ]; then
    echo "ENVIRONMENT:           $ENVIRONMENT"
    echo "FLASK_APP:             $FLASK_APP"
    echo "ROOT:                  $ROOT"
    echo "INIT_SCRIPT_PATHNAME:  $INIT_SCRIPT_PATHNAME"
    echo "INIT_SCRIPT_DIR:       $INIT_SCRIPT_DIR"
    echo "INIT_SCRIPT:           $INIT_SCRIPT"
    echo "SCRIPT_PATHNAME:       $SCRIPT_PATHNAME"
    echo "SCRIPT_DIR:            $SCRIPT_DIR"
    echo "SCRIPT:                $SCRIPT"
    echo "DOWNLOAD_HOST:         $DOWNLOAD_HOST"
    echo "DOWNLOAD_SETTINGS:     $DOWNLOAD_SETTINGS"
    echo "DOWNLOAD_CACHE_DIR:    $DOWNLOAD_CACHE_DIR"
    echo "DOWNLOAD_VENV:         $DOWNLOAD_VENV"
    echo "IDA_DATA_ROOT:         $IDA_DATA_ROOT"
    echo "IDA_URL:               $IDA_URL"
    echo "IDA_ADMIN_USER:        $IDA_ADMIN_USER"
    echo "IDA_TEST_PROJECT:      $IDA_TEST_PROJECT"
    echo "IDA_TEST_USER:         $IDA_TEST_USER"
    echo "METAX_URL:             $METAX_URL"
    echo "SIMULATE_FILE_LINKS:   $SIMULATE_DATASET_SHARE_FILE_LINKS"
fi

export FLASK_APP
export ROOT
export HTTPD_USER
export DOWNLOAD_HOST
export DOWNLOAD_SETTINGS
export DOWNLOAD_CACHE_DIR
export DOWNLOAD_VENV
export TRUSTED_SERVICE_TOKEN
export METAX_URL
export METAX_USER
export METAX_PASS
export IDA_DATA_ROOT
export IDA_URL
export IDA_TEST_PROJECT
export IDA_TEST_USER
export IDA_TEST_PASS
export IDA_ADMIN_USER
export IDA_ADMIN_PASS
export SIMULATE_DATASET_SHARE_FILE_LINKS

