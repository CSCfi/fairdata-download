# Configuration

Production configuration is handled via ansible playbooks maintained in a separate 
repository.

Configuration for local development is taken care of when installing
the download service components as systemd services, as documented in
[installation instructions](/docs/installation.md) If the script
`dev_config/utils/initialize-service` is used, this guide is not relevant.

The following guidance is for configuring the service in other environments, or for
other purposes.

## Environment variables

In order to run development server, some environment variables need to be
specified.

Environment variables can be set on command line or by including a file called
`.env` at the root of the repository. Sample environment file below:
```
FLASK_APP=download
FLASK_DEBUG=1
FLASK_RUN_HOST=0.0.0.0
FLASK_RUN_PORT=5000

DOWNLOAD_SERVICE_SETTINGS=./settings.cfg.dev
```

## Download Settings

Download Service settings can be specified with a settings file referenced by
`DOWNLOAD_SERVICE_SETTINGS` environment variable. (see
[default configuration](/download/config.py) for reference)

Sample configuration below:

```
MQ_HOST = 'rabbitmq'

DOWNLOAD_CACHE_DIR = '/app/data/download-cache'
IDA_DATA_ROOT = '/app/data/ida-data'

DATABASE_FILE = '/app/data/download.db'

METAX_URL = 'https://metax.fairdata.fi/'
METAX_USER = 'download'
METAX_PASS = '<download-password>'
```

## Download E2E settings

End-to-End tests for Download Service can be configured with a separate file
pointed to by `DOWNLOAD_SERVICE_TEST_DATA` environment variable.

Sample confguration below:
```
[testdata]
DOWNLOAD_URL = https://download.fd-dev.csc.fi:4431
TEST_DATASET_PID = 73a365ee-fff4-4be0-b788-17662e035658
TEST_DATASET_FILE = /same_name_test/test_doc_1
```

## Docker Swarm

Note that the download service is incorporated into the IDA service Docker environment, which 
should be the Docker environment used for development of the download service itself. The following
instructions are for deploying a basic, static instance of the service for use in a Docker swarm
with other services which depend on having a running download service.

The files used for Docker configuration are managed in the download/config subdirectory of the
fairdata-secrets repository.

Docker Swarm configurations can be created from existing configuration files.
Use the following configuration names with the templates included in this
repository.

```
docker config create download-settings <settings-file-path>
docker config create download-e2e-settings <e2e-settings-file-path>
docker config create download-nginx-config <nginx-config-file-path>
docker config create fairdata-nginx-config <nginx-base-config-file-path>
docker config create fairdata-ssl-certificate <ssl-certificate-file-path>
docker config create fairdata-ssl-certificate-key <ssl-certificate-key-file-path>
```
