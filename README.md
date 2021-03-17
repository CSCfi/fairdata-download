# fairdata-download-service

[![pipeline status](https://gitlab.ci.csc.fi/fairdata/fairdata-download-service/badges/test/pipeline.svg)](https://gitlab.ci.csc.fi/fairdata/fairdata-download-service/-/commits/test)
[![coverage report](https://gitlab.ci.csc.fi/fairdata/fairdata-download-service/badges/test/coverage.svg)](https://gitlab.ci.csc.fi/fairdata/fairdata-download-service/-/commits/test)

The Fairdata download server (v2) which provides for packaging and download of
published datasets, or subsets of datasets, available via Etsin.

## Installation

### Local Development

Requirements for development server and generator can be installed with pip:

```
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### Docker

Repository includes Dockerfiles for server and generator processes for
development environment setup. Images can be
built by running:

```
docker build . --no-cache -t fairdata-download-server
docker build . --no-cache -t fairdata-download-generator -f Dockerfile.generator
```

## Configuration

### Local Development

In order to run development server, some environment variables need to be
specified. There variables can be set on command line or by including a file
called `.env` at the root of the repository. Sample content of `.env` file
below:

```
FLASK_APP=download
FLASK_ENV=development
FLASK_RUN_HOST=0.0.0.0
FLASK_RUN_PORT=5000

DOWNLOAD_SERVICE_SETTINGS=./settings.cfg.dev
```

Sample content for a settings.cfg.dev file is:

```
MQ_HOST = 'rabbitmq'

DOWNLOAD_CACHE_DIR = '/app/data/download-cache'
IDA_DATA_ROOT = '/app/data/ida-data'

DATABASE_FILE = '/app/data/download.db'

METAX_URL = 'https://metax.fairdata.fi/'
METAX_USER = 'download'
METAX_PASS = '<download-password>'
```

#### Docker Swarm

Docker Swarm configurations can be created from existing configuration files.
Use the following configuration names with the templates included in this
repository.

```
docker config create download-settings <settings-file-path>
docker config create download-e2e-settings <e2e-settings-file-path>
docker config create download-nginx-config <nginx-config-file-path>
docker config create fairdata-ssl-certificate <ssl-certificate-file-path>
docker config create fairdata-ssl-certificate-key <ssl-certificate-key-file-path>
```

## Run applications

### Local Development

Local development server can be started with Flask command:

```
flask run
```

In order to generate compressed files to download, worker application has to be
started with celery:

```
celery -A download.celery worker
```

### Docker Swarm

Ensure that Docker Swarm is initialized, and finally deploy the stack with
template provided in the repository:

```
docker swarm init
docker stack deploy --with-registry-auth -c docker-compose.yml fairdata-download
```

## Run unit tests

### Local Development

Unit tests can be run locally with coverage:

```
coverage run -m pytest
```

After running tests, coverage report can be displayed with:

```
coverage report -m
```
