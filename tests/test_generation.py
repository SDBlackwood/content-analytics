import pytest
from content_analytics.bin.generate_events import generate_media_event
from content_analytics.utils.data_model import MediaEvent


def test_generation():
    # Generate 1 sample
    event = generate_media_event()
    # Validate the model
    assert event is not None
    # Expect no exeption raised
    MediaEvent.model_validate(event)


def test_specific_example():
    example = {
        "event_id": "event_59d0ccff4d60470284e0ed8ebce12f17",
        "event_type": "stop",
        "timestamp": "2025-03-26T20:24:42.373389",
        "user_id": "user_f8b001e6",
        "device_id": "web_app_aa6577",
        "ip_address": "150.56.163.9",
        "content_id": "content_9ec4257d",
        "content_type": "ad",
        "title": "Compatible 4thgeneration matrix",
        "genre": ["sci-fi", "fantasy"],
        "duration_seconds": 38,
        "language": "en",
        "current_timestamp": 32.82510079721527,
        "schema_version": "1.0",
        "event_source": "streaming_device",
    }
    event = MediaEvent.model_validate(example)
    assert event is not None
    assert event.event_id == example["event_id"]
    assert event.event_type == example["event_type"]
    assert event.timestamp == example["timestamp"]
    assert event.user_id == example["user_id"]
    assert event.device_id == example["device_id"]
