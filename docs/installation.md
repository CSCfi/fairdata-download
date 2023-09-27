# Installation

Production installation is handled via ansible playbooks maintained in a separate 
repository.

## Local Development

Local development is expected to be done by configuring and executing the service
components as systemd services on a Centos server.

Ideally, the service is being configured on a host which is also running
the IDA service, in which case, the IDA data storage root will be used.

Copy the file `dev_config/settings.cfg.example` to `dev_config/settings.cfg` and edit
accordingly. The example configuration file should be usable as-is except for defining
the relevant secrets.

Local development requires both nginx and a RabbitMQ server instance to
be running and available, with the relevant credentials defined in
`dev_config/settings.cfg`

Once the settings are defined, a utility script exists to fully configure and deploy
the fairdata-download-server and fairdata-download-generator services locally on a
Centos host where nginx and RabbitMQ are running.

To install and deploy the service, execute the script `dev_config/utils/initialize-service`, which will:

 * load the configuration settings from `dev_config/settings.cfg`
 * create the necessary symbolic links from `/usr/lib/systemd/system/` to the relevant files in the root of the repository
 * initialize the `data/` directory with subdirectories for database, cache, and either a link to the IDA data storage root or an empty placeholder directory
 * copy the `dev_config/fairdata-download.nginx.conf` to `/etc/nginx/conf.d/`
 * create the needed user account and vhost in RabbitMQ
 * initialize the python3 virtual environment `venv/`, installing all components in `requirements.txt` and `requirements-dev.txt`
 * start and enable the services

The configured and running services can be easily restarted as needed using
the utility script `dev_config/utils/restart`

You can generate, as needed, new `requirements.txt` and/or `requirements-dev.txt` files from `pyproject.toml` with Poetry:

```bash
# no development deps
poetry export --without-hashes -o requirements.txt

# with development dependencies
poetry export --without-hashes --dev -o requirements-dev.txt
```

## Docker Development

Note that the download service is incorporated into the IDA service Docker environment, which 
should be the Docker environment used for development of the download service itself. The following
instructions are for deploying a basic, static instance of the service for use in a Docker swarm
with other services which depend on having a running download service.

The Docker images provided in this repository are not really suitable for development of the
download service itself, but are provided for those developing other services in a Docker swarm
environment (e.g. Etsin) which require a running instance of the download service for
integration testing.

The basic Docker images can be built by executing:

```bash
docker build . --no-cache -t fairdata-docker.artifactory.ci.csc.fi/fairdata-download-server
docker build . --no-cache -f Dockerfile.generator -t fairdata-docker.artifactory.ci.csc.fi/fairdata-download-generator
```
