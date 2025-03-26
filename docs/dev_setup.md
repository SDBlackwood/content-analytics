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
poetry run python content_analytics/bin/storage.py
```

5. Run the tests:

These are integration tests which analyse the results of the storage script.

```bash
poetry run pytest
```

