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

