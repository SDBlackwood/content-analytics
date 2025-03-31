import pandas as pd
import pytest
from unittest import mock


@pytest.fixture
def s3_client():
    return mock.Mock()


@pytest.fixture
def input():
    input = pd.DataFrame(
        {
            "event_id": ["test1", "test2", "test3"],
            "event_type": ["play", "pause", "stop"],
            "timestamp": [
                "2024-03-31T10:00:00Z",
                "2024-03-31T10:01:00Z",
                "2024-03-31T10:02:00Z",
            ],
            "user_id": ["user1", "user2", "user3"],
            "device_id": ["dev1", "dev2", "dev3"],
            "ip_address": ["1.1.1.1", "2.2.2.2", "3.3.3.3"],
            "content_id": ["content1", "content2", "content3"],
            "content_type": ["movie", "episode", "ad"],
            "title": ["Movie", "TV", "Ad"],
            "genre": [["action", "drama"], ["comedy"], ["ad"]],
            "duration_seconds": [7200, 1800, 30],
            "language": ["en", "es", "en"],
            "current_timestamp": [120.5, 45.0, 15.5],
            "schema_version": ["1.0", "1.0", "1.0"],
            "event_source": ["web_app", "mobile_app", "web_app"],
        }
    )
    return input


from content_analytics.storage.store_events_simple import __store_as_parquet


def test_store_as_parquet(s3_client, input):
    __store_as_parquet(input, s3_client)

    assert s3_client.upload_fileobj.call_count == len(input)

    for i, call in enumerate(s3_client.upload_fileobj.call_args_list):
        _, kwargs = call
        assert kwargs["Bucket"] == "content-analytics"
        assert kwargs["Fileobj"] is not None
        assert "event_type=" in kwargs["Key"]
        assert "date=2024-03-31" in kwargs["Key"]

        # Get the rows
        expected_rows = input.iloc[[i]]

        # Read the parquet data from the buffer
        buffer = kwargs["Fileobj"]
        actual_df = pd.read_parquet(buffer)

        # Compare the dataframes
        pd.testing.assert_frame_equal(actual_df, expected_rows)
