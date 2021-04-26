# Installation

Instructions for installing the download service software dependencies are
below.

## Local Development

Local development server requires a RabbitMQ server instance to be available
with the credentials being used for the service (see
[default configuration](/download/config.py) for reference).

Python requirements for development server and generator can be installed with
pip:

```
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

## Development with Docker

Repository includes Dockerfiles for server and generator processes for
development environment setup. Images can be built by running:

```
docker build . --no-cache -t fairdata-docker.artifactory.ci.csc.fi/fairdata-download-server
docker build . --no-cache -f Dockerfile.generator -t fairdata-docker.artifactory.ci.csc.fi/fairdata-download-generator
```

