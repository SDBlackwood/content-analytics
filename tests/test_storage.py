import json
import pytest
from unittest.mock import patch, MagicMock, call
from datetime import datetime, timedelta
import pandas as pd
from io import BytesIO
import uuid

# Import the function to test
from content_analytics.storage.store_events_dag import store_events
from content_analytics.utils.data_model import MediaEvent

# Sample valid event that matches the MediaEvent model
SAMPLE_EVENT = {
    "event_id": "ev_123456789",
    "event_type": "play",
    "timestamp": datetime.now().isoformat(),
    "user_id": "user_12345",
    "device_id": "device_67890",
    "ip_address": "192.168.1.1",
    "content_id": "content_54321",
    "content_type": "movie",
    "title": "Test Movie",
    "genre": ["action", "drama"],
    "duration_seconds": 7200,
    "language": "en",
    "current_timestamp": 120.5,
    "schema_version": "1.0",
    "event_source": "web_app"
}


@pytest.fixture
def mock_kafka_consumer():
    """
    Fixture to create a mock Kafka consumer that returns predefined messages
    """
    with patch('content_analytics.dags.storage.KafkaConsumer') as mock_consumer_class:
        # Create the mock consumer instance
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer
        
        # Setup the poll method to return our sample events
        def mock_poll(timeout_ms):
            # This creates a structure like what KafkaConsumer.poll() would return
            # with topic partitions and messages
            topic_partition = MagicMock()
            
            # Create message objects
            message1 = MagicMock()
            message1.value = json.dumps(SAMPLE_EVENT)
            
            # Second event with different type
            event2 = SAMPLE_EVENT.copy()
            event2["event_id"] = "ev_987654321"
            event2["event_type"] = "pause"
            message2 = MagicMock()
            message2.value = json.dumps(event2)
            
            # Return dictionary structure that matches KafkaConsumer.poll()
            return {topic_partition: [message1, message2]}
        
        mock_consumer.poll.side_effect = [mock_poll(0), {}]  # Return messages once, then empty
        
        yield mock_consumer


@pytest.fixture
def mock_s3_client():
    """
    Fixture to create a mock S3 client
    """
    with patch('content_analytics.dags.storage.boto3.client') as mock_client:
        mock_s3 = MagicMock()
        mock_client.return_value = mock_s3
        yield mock_s3


@pytest.fixture
def mock_uuid():
    """
    Fixture to mock uuid.uuid4 to return a predictable value
    """
    with patch('content_analytics.dags.storage.uuid.uuid4') as mock_uuid4:
        mock_uuid4.return_value = uuid.UUID('12345678-1234-5678-1234-567812345678')
        yield mock_uuid4


@pytest.fixture
def mock_datetime():
    """
    Fixture to mock datetime.now to return a predictable value
    """
    with patch('content_analytics.dags.storage.datetime') as mock_dt:
        mock_dt.now.return_value = datetime(2023, 1, 1, 12, 0, 0)
        # Make sure timedelta still works
        mock_dt.timedelta = timedelta
        # Make sure the class can still be used for type hints
        mock_dt.datetime = datetime
        yield mock_dt


def test_store_events(mock_kafka_consumer, mock_s3_client, mock_uuid, mock_datetime):
    """
    Test the store_events function to ensure it:
    1. Consumes messages from Kafka correctly
    2. Processes and validates them using the MediaEvent model
    3. Transforms them into a DataFrame
    4. Partitions by event_type and date
    5. Uploads to S3 with correct paths
    """
    # Call the function with a mock execution date
    execution_date = datetime(2023, 1, 1)
    store_events(execution_date)
    
    # Verify Kafka consumer was called with correct parameters
    from content_analytics.utils.config import settings
    mock_kafka_consumer.assert_called_once_with(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        auto_offset_reset=settings.kafka_auto_offset_reset,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        max_poll_records=settings.batch_size,
        max_poll_interval_ms=settings.batch_max_poll_interval_ms,
        group_id=settings.kakfa_topic,
    )
    
    # Verify poll was called
    mock_kafka_consumer.poll.assert_called()
    
    # Verify consumer was closed
    mock_kafka_consumer.close.assert_called_once()
    
    # Check that S3 upload_fileobj was called twice (once for each event_type)
    assert mock_s3_client.upload_fileobj.call_count == 2
    
    # Extract the calls to upload_fileobj
    calls = mock_s3_client.upload_fileobj.call_args_list
    
    # Verify the S3 keys used contain the expected partitioning structure
    expected_paths = [
        f"{settings.storage_base_path}/event_type=play/date=",  # Will contain today's date
        f"{settings.storage_base_path}/event_type=pause/date="  # Will contain today's date
    ]
    
    # Check that each call's key argument contains the expected partitioning
    for i, expected_path in enumerate(expected_paths):
        # Get the Key argument from the call
        _, kwargs = calls[i]
        s3_key = kwargs['Key']
        assert expected_path in s3_key
        assert f"{settings.storage_file_prefix}_20230101120000_12345678123456781234567812345678.parquet" in s3_key


def test_store_events_no_messages(mock_kafka_consumer, mock_s3_client):
    """
    Test that the function handles the case of no messages gracefully
    """
    # Configure mock_kafka_consumer to return empty results
    mock_kafka_consumer.poll.return_value = {}
    
    # Call the function
    execution_date = datetime(2023, 1, 1)
    store_events(execution_date)
    
    # Verify Kafka consumer was called
    mock_kafka_consumer.poll.assert_called()
    
    # Verify consumer was closed
    mock_kafka_consumer.close.assert_called_once()
    
    # Verify S3 upload was not called since there were no messages
    mock_s3_client.upload_fileobj.assert_not_called()


def test_store_events_invalid_message(mock_kafka_consumer, mock_s3_client, mock_uuid, mock_datetime):
    """
    Test that the function properly handles invalid messages
    """
    # Create a topic partition and invalid message
    topic_partition = MagicMock()
    
    # Create a valid message
    valid_message = MagicMock()
    valid_message.value = json.dumps(SAMPLE_EVENT)
    
    # Create an invalid message (missing required fields)
    invalid_message = MagicMock()
    invalid_data = {"event_id": "bad_event", "timestamp": "not-a-datetime"}
    invalid_message.value = json.dumps(invalid_data)
    
    # Configure mock to return one valid and one invalid message
    mock_kafka_consumer.poll.side_effect = [
        {topic_partition: [valid_message, invalid_message]},
        {}
    ]
    
    # Call the function
    execution_date = datetime(2023, 1, 1)
    store_events(execution_date)
    
    # Verify Kafka consumer was called
    mock_kafka_consumer.poll.assert_called()
    
    # Verify consumer was closed
    mock_kafka_consumer.close.assert_called_once()
    
    # Verify S3 upload was called only once (for the valid message)
    assert mock_s3_client.upload_fileobj.call_count == 1 