# fairdata-download-service

The Fairdata download server (v2) which provides for packaging and download of
published datasets, or subsets of datasets, available via Etsin.

## Development

### Install

Requirements for development server including can be installed with pip:

```
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### Environment variables

In order to run development server some environment variables need to be
specified. There variables can be set on command line or by including a file
called `.env` at the root of the repository. Sample content of `.env` file
below:

```
FLASK_APP=download
FLASK_ENV=development
FLASK_RUN_HOST=0.0.0.0
FLASK_RUN_PORT=80

DOWNLOAD_SERVICE_SETTINGS=/etc/fairdata-download-service/custom-settings.cfg
```

### Run development server

Development server can be run with Flask:

```
flask run
```

### Run tests

Tests can be run with coverage:

```
coverage run -m pytest
```

After running tests, coverage report can be displayed with:

```
coverage report -m
```
