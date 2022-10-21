# Deployment

Production deployment is handled via ansible playbooks maintained in a separate 
repository.

Deployment for local development is taken care of when installing
the download service components as systemd services, as documented in
[installation instructions](/docs/installation.md) If the script
`dev_config/utils/initialize-service` is used, this guide is not relevant.

The following guidance is for deployment of the service in other environments, or for
other purposes.

## Basic Local Deployment without Services

Basic local development, without installing the service components as systemd services, can
be done by simply initializing and activating the python3 virtual environment and 
explicitly running the server and generator components using Flask or Celery directly.

To initialize the python3 virtual environment, execute the script
```
utils/initialize-venv
```

The download service server component can be started with Flask command:

```
source venv/bin/activate
flask run
```

The download service package generator component can be started with Celery command:

```
source venv/bin/activate
celery -A download.tasks worker
```

## Deployment with Docker Swarm

Ensure that Docker Swarm is initialized, and finally deploy the stack with
template provided in the repository:

```
docker swarm init
docker stack deploy --with-registry-auth --resolve-image always -c docker-compose.yml fairdata-download
```
