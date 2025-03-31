from datetime import datetime, timedelta
import json
import uuid
from io import BytesIO
import pydantic
import time

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer
import boto3

# Import settings parsed from env
from content_analytics.utils.config import settings
from content_analytics.utils.data_model import MediaEvent


def store_events(consumer, s3_client, poll_duration_seconds=None):
    """
    Consumes a batch of 1M messages from Kafka and stores as Parquet files in Object Storage every 10s
    """
    print(f"Starting batch processing at {datetime.now()}")

    total_messages_processed = 0
    start_time = time.time()

    print(f"Polling Kafka for up to {settings.batch_size} messages")

    # Poll forever so that we keep consuming messages every batch_poll_interval_seconds
    while True:
        # For testing, allow us to stop polling after a certain duration
        if poll_duration_seconds is not None:
            elapsed_time = time.time() - start_time
            if elapsed_time >= poll_duration_seconds:
                print(f"Reached polling duration of {poll_duration_seconds} seconds")
                break

        records = consumer.poll(
            timeout_ms=settings.batch_poll_interval_seconds * 1000,
            max_records=settings.batch_size,
        )

        if not records:
            print("No new records, continuing to poll...")
            continue

        # Process the batch of records
        batch_time = time.time()
        messages = []
        for _, partition_messages in records.items():
            for message in partition_messages:
                try:
                    # Validate the incoming message
                    event = MediaEvent.model_validate(message.value)
                    # Append validated event to messages
                    messages.append(event)
                except pydantic.ValidationError as e:
                    print(f"Pydantic validation error processing message: {e}")
                    continue
                except Exception as e:
                    print(f"Error processing message: {e}")
                    continue

                if len(message) >= settings.batch_size:
                    break

            if len(messages) >= settings.batch_size:
                break

        if not messages:
            print("No messages received from Kafka.")
            continue

        # Convert Pydantic models to dictionaries before creating DataFrame
        messages_dict = [message.model_dump() for message in messages]
        # Call helper function to organize and store events
        __store_as_parquet(pd.DataFrame(messages_dict), s3_client)

        # Update the total count
        total_messages_processed += len(messages)

        # Wait for batch_poll_interval_seconds (total time taken to process the batch - batch_poll_interval_seconds)
        batch_processing_time = time.time() - batch_time  # time in seconds
        print(f"\nTime taken to process the batch: {batch_processing_time:.1f}s")
        print(f"TOTAL Time taken: {time.time() - start_time:.1f}s")
        print(f"Records processed in this batch: {len(messages)}")
        print(f"TOTAL records processed: {total_messages_processed}")

        # Calculate wait time, if needed
        is_batch_size_reached = len(messages) >= settings.batch_size
        if (
            batch_processing_time < settings.batch_poll_interval_seconds
            and is_batch_size_reached
        ):
            wait_time = settings.batch_poll_interval_seconds - batch_processing_time
            print(f"Waiting for {wait_time:.1f}s before polling again")
            time.sleep(wait_time)


def __store_as_parquet(df, s3_client):
    # Add a date column for partitioning
    df["date"] = pd.to_datetime(df["timestamp"]).dt.date.astype(str)

    # Organize by event_type and date for partitioning
    for event_type in df["event_type"].unique():
        # Filter the DataFrame for this event type
        event_df = df[df["event_type"] == event_type]

        # Group by date for partitioning
        for date, date_df in event_df.groupby("date"):
            print(f"Processing {len(date_df)} '{event_type}' events for date {date}")

            # Get current timestamp for the filename
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

            # Create Arrow Table from pandas DataFrame
            table = pa.Table.from_pandas(date_df)

            # Create a BytesIO object to hold the Parquet file
            parquet_buffer = BytesIO()

            # Write the Parquet file to the buffer
            pq.write_table(
                table, parquet_buffer, compression=settings.storage_compression
            )

            # Define the S3 key with partitioning
            s3_key = f"{settings.storage_base_path}/event_type={event_type}/date={date}/{settings.storage_file_prefix}_{timestamp}_{uuid.uuid4().hex}.parquet"

            # Upload the Parquet file to S3
            print(f"Uploading {s3_key} to S3 to {settings.s3_bucket}")
            try:
                s3_client.upload_fileobj(
                    Fileobj=parquet_buffer, Bucket="content-analytics", Key=s3_key
                )
            except Exception as e:
                print(f"Error uploading file to S3: {e}")
                return

            print(f"Successfully uploaded {s3_key} to S3")

    print(f"Batch processing completed at {datetime.now()}")


if __name__ == "__main__":
    # Create Kafka consumer
    consumer = KafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers="localhost:29092",  # Use external listener address
        auto_offset_reset=settings.kafka_auto_offset_reset,
        # Consume valyes as JSON https://kafka-python.readthedocs.io/en/master/usage.html
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        max_poll_records=settings.batch_size,
        group_id=settings.kafka_topic,
    )

    # Object storage client - works with both AWS S3 and minio
    s3_client = boto3.client(
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

    try:
        store_events(consumer, s3_client)
    finally:
        if consumer:
            consumer.close()
        if s3_client:
            s3_client.close()
