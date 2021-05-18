# Deployment

Once installed (see [installation instructions](/docs/installation.md)) the
development environment can be set up by following the instructions below.

## Local Development

Local development server can be started with Flask command:

```
flask run
```

In order to generate compressed files to download, worker application has to be
started with celery:

```
celery -A download.tasks worker
```

## Development with Docker Swarm

Ensure that Docker Swarm is initialized, and finally deploy the stack with
template provided in the repository:

```
docker swarm init
docker stack deploy --with-registry-auth --resolve-image always -c docker-compose.yml fairdata-download
```
