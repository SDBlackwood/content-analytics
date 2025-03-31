import pytest
import pyarrow as pa
from content_analytics.bin.generate_events import send_events_to_kafka
from content_analytics.storage.store_events_simple import store_events
import boto3
from kafka import KafkaClient
from kafka.consumer import KafkaConsumer
from kafka.structs import TopicPartition
import pandas as pd
import json
import time
from s3fs.core import S3FileSystem
from content_analytics.utils.config import settings
import asyncio


# S3 client fixture
@pytest.fixture(scope="module")
def s3_client():
    client = boto3.client(
        "s3",
        aws_access_key_id=settings.s3_access_key_id,
        aws_secret_access_key=settings.s3_secret_access_key,
        endpoint_url=settings.s3_endpoint_url,
        region_name=settings.s3_region,
        config=boto3.session.Config(
            signature_version="s3v4", s3={"addressing_style": "path"}
        ),
    )
    # Remove everything from the bucket
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
    try:
        # Create a consumer that subscribes directly to the topic
        client = KafkaConsumer(
            bootstrap_servers="localhost:29092",
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            enable_auto_commit=False,
            max_poll_records=settings.batch_size,
            consumer_timeout_ms=60000,
            fetch_max_bytes=1024 * 1024 * 1024,  # 1GB
        )

        client.subscribe([settings.kafka_topic])

        # Wait for consumer to be ready
        time.sleep(5)

        yield client

    finally:
        client.close()


# Set to integration test so it is not run by default
# Add --integration flag to run the test https://jwodder.github.io/kbits/posts/pytest-mark-off/
@pytest.mark.skipif(
    "not config.getoption('--integration')",
    reason="Only run when --integration is given",
)
def test_e2e(request, s3_client, consumer):
    batch_size = 50000 # Reduced for testing
    topic = "media-events"
    bootstrap_servers = "localhost:29092"
    add_events = request.config.getoption(
        "--add-events"
    )  # Used for manual/development and testing

    if add_events:
        # Generate events in kafka
        asyncio.run(
            send_events_to_kafka(
                batch_size=batch_size, topic=topic, bootstrap_servers=bootstrap_servers
            )
        )

    # Get a count of records currently in Kafka
    try:
        kafka_record_count = count_kafka_messages(topic)

        # Make sure we have records to process
        assert kafka_record_count > 0, "No records found in Kafka topic"

        # Run the store_events_simple function to store events in S3
        try:
            # Use a shorter poll duration for testing
            store_events(consumer, s3_client, poll_duration_seconds=60)
        except Exception as e:
            print(f"Error in store_events: {e}")
            raise

        # Give S3 a moment to finalize all uploads
        time.sleep(5)

        # Assert that the events are in S3
        records = s3_client.list_objects_v2(Bucket=settings.s3_bucket)
        assert "Contents" in records, "No files were uploaded to S3"

        # Using Pandas, read all the parquet files and count the total records
        df = pd.read_parquet(
            f"s3://{settings.s3_bucket}/",
            storage_options={
                "key": settings.s3_access_key_id,
                "secret": settings.s3_secret_access_key,
                "client_kwargs": {"endpoint_url": settings.s3_endpoint_url},
            },
            engine="fastparquet",
        )
        s3_record_count = len(df)

        # Assert
        msg = f"Record mismatch: Kafka has {kafka_record_count}, S3 has {s3_record_count} records"
        assert s3_record_count == kafka_record_count, msg
    except Exception as e:
        print(f"Test failure: {e}")
        raise


def count_kafka_messages(topic):
    """Count the total number of messages in a Kafka topic"""
    # Create a consumer that starts from the beginning
    temp_consumer = KafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )

    # Get the partitions for the topic
    partitions = temp_consumer.partitions_for_topic(topic)
    if not partitions:
        temp_consumer.close()
        return 0

    # Create TopicPartition objects
    topic_partitions = [TopicPartition(topic, p) for p in partitions]

    # Assign the consumer to these partitions
    temp_consumer.assign(topic_partitions)

    # Seek to the beginning
    temp_consumer.seek_to_beginning()

    # Get the beginning offsets
    beginning_offsets = temp_consumer.beginning_offsets(topic_partitions)

    # Get the end offsets
    end_offsets = temp_consumer.end_offsets(topic_partitions)

    # Calculate the total number of messages
    total_messages = sum(
        end_offsets[tp] - beginning_offsets[tp] for tp in topic_partitions
    )

    temp_consumer.close()
    return total_messages
