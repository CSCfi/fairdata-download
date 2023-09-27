# fairdata-download

[![pipeline status](https://gitlab.ci.csc.fi/fairdata/fairdata-download/badges/test/pipeline.svg)](https://gitlab.ci.csc.fi/fairdata/fairdata-download/-/commits/test)
[![coverage report](https://gitlab.ci.csc.fi/fairdata/fairdata-download/badges/test/coverage.svg)](https://gitlab.ci.csc.fi/fairdata/fairdata-download/-/commits/test)

`fairdata-download` provides packaging and download of published datasets, or subsets of datasets, available via Etsin.

## Installation

See [installation instructions](/docs/installation.md).

## Configuration

See [configuration instructions](/docs/configuration.md).

## Deployment

See [deployment instructions](/docs/deployment.md).

## Testing

See [testing instructions](/docs/testing.md).

## Dependency management: Poetry

Python dependencies are managed with [Poetry](https://python-poetry.org/docs/)

### Poetry setup instructions 

```
# Setup part 1: Install pipx: 
https://github.com/pypa/pipx

# Setup part 2: Then, with pipx install poetry
pipx install poetry

# Installing dependencies
poetry install
```

### Updating Python dependencies with Poetry

```
# Add dependency
poetry add {{ dependency }}

# Add dev dependency
poetry add --dev {{ dependency }}

# Edit dependency (or dev dependency) version
{{ edit_command }} pyproject.toml

# Update requirements.txt
poetry export --without-hashes -o requirements.txt

# Update requirements-dev.txt
poetry export --dev --without-hashes -o requirements-dev.txt
```

License
-------
Copyright (c) 2018-2022 Ministry of Education and Culture, Finland

Licensed under [MIT License](LICENSE)