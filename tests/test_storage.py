from unittest import mock

import pandas as pd
import pytest

from content_analytics.storage.store_events_simple import __store_as_parquet


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


def test_store_as_parquet(s3_client, input):
    with mock.patch.object(pd.DataFrame, 'to_parquet') as mock_to_parquet:
        __store_as_parquet(input, s3_client)

        # Check it was called for each event
        assert mock_to_parquet.call_count == 3

        # We can't check the actual data, but we can check the path is correct
        for i, call in enumerate(mock_to_parquet.call_args_list):
            _, kwargs = call
            
            s3_path = kwargs['path']
            event_type = input["event_type"][i]
            
            assert f"s3://content-analytics/" in s3_path
            assert f"event_type={event_type}" in s3_path
            assert "date=2024-03-31" in s3_path
