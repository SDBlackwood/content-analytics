from datetime import datetime, timedelta
import json
import uuid
from io import BytesIO
import pydantic

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer
import boto3

# Import settings parsed from env
from content_analytics.utils.config import settings
from content_analytics.utils.data_model import MediaEvent


def store_events(consumer, s3_client):
    """
    Consumes a batch of messages from Kafka and stores as Parquet files in Object Storage
    """
    print(f"Starting batch processing at {datetime.now()}")
 
    messages = []
    message_count = 0
    start_time = datetime.now()

    # Poll for BATCH_SIZE messages or until timeout
    end_time = start_time + timedelta(seconds=settings.batch_poll_timeout_seconds)

    print(
        f"Polling Kafka for up to {settings.batch_size} messages or {settings.batch_poll_timeout_seconds} seconds..."
    )

    while datetime.now() < end_time and message_count < settings.batch_size:
        records = consumer.poll(timeout_ms=settings.batch_poll_interval_ms)

        if not records:
            print("No new records, continuing to poll...")
            continue

        # Process the batch of records
        for _, partition_messages in records.items():
            for message in partition_messages:
                try:
                    # Validate the incoming message
                    event = MediaEvent.model_validate(message.value)
                    # Append validated event to messages
                    messages.append(event)
                    message_count += 1
                except pydantic.ValidationError as e:
                    print(f"Pydantic validation error processing message: {e}")
                    continue
                except Exception as e:
                    print(f"Error processing message: {e}")
                    continue

                if message_count >= settings.batch_size:
                    break

            if message_count >= settings.batch_size:
                break

    if not messages:
        print("No messages received from Kafka. Exiting.")
        return

    # Convert Pydantic models to dictionaries before creating DataFrame
    messages_dict = [message.model_dump() for message in messages]
    # Call helper function to organize and store events
    __store_as_parquet(pd.DataFrame(messages_dict), s3_client)


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

            # Reset buffer position
            parquet_buffer.seek(0)

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
        max_poll_interval_ms=settings.batch_max_poll_interval_ms,
        group_id=settings.kafka_topic,
    )

    # Object storage client - works with both AWS S3 and minio
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=settings.s3_access_key_id,
        aws_secret_access_key=settings.s3_secret_access_key,
        endpoint_url=settings.s3_endpoint_url,  # None for actual AWS S3
        region_name=settings.s3_region,
        use_ssl=settings.s3_use_ssl,
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
