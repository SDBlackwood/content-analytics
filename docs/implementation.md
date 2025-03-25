# Content Analytics Pipeline - Storage Implementation

This implementation focuses on the storage component of the content analytics pipeline, which processes media events from Kafka and stores them as Parquet files in S3-compatible object storage.

## Overview

1. **Kafka**: Used to ingest data from user events.
2. **MinIO**: An S3-compatible object storage service for storing processed Parquet files. Minio is used during local
development but any object storage can be used in production.
3. **Airflow**: Used for orchestrating storage processing.

## Development Setup

1. Start the services using Docker Compose:

```bash
docker-compose up
```

### Initialization Containers

This uses init containers to set up the development environment automatically when Docker Compose starts:

1. **Kafka Init Container**: Creates required Kafka topics automatically on startup to ensure the messaging infrastructure is ready for development. This eliminates the need to manually create topics via CLI commands.
2. **MinIO Setup Container**: Creates required buckets in the S3-compatible storage when the system starts. This ensures consistent storage configuration across development environments.
3. **Airflow Init Container**: Handles database initialization and user creation for the Airflow webserver. This container runs only once at startup to ensure the Airflow environment is properly configured.


## Set up

1. Generate sample events and send them to Kafka:

```bash
# Run in batch mode to generate 1M events (this was done as a init container due to local connection issues)
docker compose -f docker-compose.yml up -d event-generator
```

## Service Access

- **Kafka UI**: http://localhost:8080
- **MinIO Console**: http://localhost:9001 (Login: minioadmin / minioadmin)
- **Airflow Web UI**: http://localhost:8081 (Login: airflow / airflow)

## Env variables

This project uses Pydantic Settings for type-safe configuration management. This document explains how the configuration system works and how to use it in your code.  Each setting can be set via environment variables with the same name as the field:

```
KAFKA_BOOTSTRAP_SERVERS=localhost:29092
S3_BUCKET=content-analytics-dev
APP_DEBUG=true
```
## Configuration Files

- **`.env.dist`**: Template with development-focused settings
- **`.env`**: Your local configuration


## Docker
There are 2 docker files: one for the airflow workflow and 1 for the data generation script.  
To allow developers to have a local venv and run command locally/have dependecies picked up by an LSP, depenencies are in the pyproject toml in groups and are then exported to
requirements files for each docker image with:

poetry export --with=airflow -f requirements.txt --output docker/workflow/requirements.txt
poetry export --with=generate -f requirements.txt --output docker/workflow/requirements.txt
