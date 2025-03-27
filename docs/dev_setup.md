# Dev Setup

## Local Development

1. Install dependencies using Poetry:

```bash
poetry install
```

2. Start the services using Docker Compose:

```bash
docker-compose up
```

3. Generate sample events and send them to Kafka:

```bash
poetry run python content_analytics/bin/generate_events.py
```

4. Run the storage script:

```bash
poetry run python content_analytics/storage/storage.py
```

5. Run the tests:

These are integration tests which analyse the results of the storage script.

```bash
poetry run pytest
```

There is a single integration test which can insert events into kafka and then runs the storage script and then asserts that the number of records in S3 is correct. There are 2 flags here:

- `--integration` - runs the test as an integration test
- `--add-events` - adds events to kafka before running the test

To run the test with both flags:

```bash
 poetry run pytest tests/test_e2e.py -s --integration --add-events
``` 

> [!note] The test is a work in progress and hasn't been verified to pass as of 27/03/2025 12:00
> Records can be manually viewed in Kafka UI at `http://localhost:8080/ui/clusters/local/all-topics/media-events`
> and Parquet files can be viewed in the MinIO UI at `http://localhost:9001/browser/content-analytics`
> but the test doesn't assert that the number of records is correct yet.  

## Env variables

This project uses Pydantic Settings for type-safe configuration management. This document explains how the configuration system works and how to use it in your code.  Each setting can be set via environment variables with the same name as the field:

```
KAFKA_BOOTSTRAP_SERVERS=localhost:29092
S3_BUCKET=content-analytics
```
## Configuration Files

- **`.env.dist`**: Template with development-focused settings
- **`.env`**: Your local configuration

### Initialization Containers

This uses init containers to set up the development environment automatically when Docker Compose starts:

1. **Kafka Init Container**: Creates required Kafka topics automatically on startup to ensure the messaging infrastructure is ready for development. This eliminates the need to manually create topics via CLI commands.
2. **MinIO Setup Container**: Creates required buckets in the S3-compatible storage when the system starts. This ensures consistent storage configuration across development environments.