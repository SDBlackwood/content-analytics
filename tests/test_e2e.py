import pytest
from content_analytics.bin.generate_events import send_events_to_kafka
from content_analytics.storage.store_events_simple import store_events
import boto3
from content_analytics.utils.config import settings
from kafka import KafkaClient
from kafka.consumer import KafkaConsumer
from kafka.structs import TopicPartition
import pandas as pd
import json
import time


# S3 client fixture
@pytest.fixture(scope="module")
def s3_client():
    client = boto3.client(
        "s3",
        aws_access_key_id=settings.s3_access_key_id,
        aws_secret_access_key=settings.s3_secret_access_key,
        endpoint_url=settings.s3_endpoint_url,  # None for actual AWS S3
        region_name=settings.s3_region,
        # For MinIO compatibility, setting addressing style to path
        config=boto3.session.Config(
            signature_version="s3v4", s3={"addressing_style": "path"}
        ),
    )
    # Rememove everything from the bucket
    objects = client.list_objects_v2(Bucket=settings.s3_bucket)
    if "Contents" in objects:
        for obj in client.list_objects_v2(Bucket=settings.s3_bucket)["Contents"]:
            client.delete_object(Bucket=settings.s3_bucket, Key=obj["Key"])
        # Assert that the bucket is empty
        time.sleep(5)
        if "Contents" in client.list_objects_v2(Bucket=settings.s3_bucket):
            assert (
                len(client.list_objects_v2(Bucket=settings.s3_bucket)["Contents"]) == 0
            )

    yield client
    client.close()


@pytest.fixture(scope="module")
def consumer():
    client = KafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers="localhost:29092",  # Use external listener address
        auto_offset_reset=settings.kafka_auto_offset_reset,
        # Consume valyes as JSON https://kafka-python.readthedocs.io/en/master/usage.html
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        max_poll_records=settings.batch_size,
        group_id=settings.kafka_topic,
    )
    yield client

    client.close()


# Set to integration test so it is not run by default
# Add --integration flag to run the test https://jwodder.github.io/kbits/posts/pytest-mark-off/
@pytest.mark.skipif(
    "not config.getoption('--integration')",
    reason="Only run when --integration is given",
)
def test_e2e(request, s3_client, consumer):
    batch_size = 5000000
    topic = "media-events"
    bootstrap_servers = "localhost:29092"
    add_events = request.config.getoption(
        "--add-events"
    )  # Used for manual/development and testing

    if add_events:
        print("Adding events to kafka")
        # Generate 5M events in kafka
        send_events_to_kafka(
            batch_size=batch_size, topic=topic, bootstrap_servers=bootstrap_servers
        )

    # Assert that the events are in kafka
    # TODO: Implement this - issues during development getting kafka lag

    # Run the store_events_simple function to store events in S3
    store_events(consumer, s3_client, poll_duration_seconds=60)

    # Assert that the events are in S3
    records = s3_client.list_objects_v2(Bucket=settings.s3_bucket)
    print(f"Number of records in S3: {len(records)}")

    # Download all the parquet files and assert that the number of records is correct
    files = []
    for obj in records["Contents"]:
        file = s3_client.download_file(
            Bucket=settings.s3_bucket, Key=obj["Key"], Filename=obj["Key"]
        )
        files.append(file)

    # Using Pandas, read all the parquet files and assert that the number of records is correct
    df = pd.read_parquet(files)
    print(len(df))
    assert len(df) == batch_size


def check_consumer_group(topic, group_id):
    # Initialize Kafka client
    client = KafkaClient(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id="consumer-group-checker",
    )

    print(f"Checking consumer group: {group_id}")
    print(f"Topic: {topic}")

    # Get consumer group offsets
    consumer = KafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    consumer.poll(timeout_ms=10)

    # Get topic partitions
    partitions = consumer.partitions_for_topic(topic)

    total_lag = 0
    for partition in partitions:
        current_offset = consumer.position(
            TopicPartition(topic=topic, partition=partition)
        )
        total_lag += current_offset
    print(f"Total lag: {total_lag}")

    consumer.close()
    return total_lag
