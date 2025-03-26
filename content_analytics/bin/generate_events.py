# Mock data script to generate 1M records into kafka (10 x 100k) for testing purposes
import uuid
import random
import os
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker
import sys

from content_analytics.utils.data_model import MediaEvent

# Lib for creating fake data
fake = Faker()

# Mock Genres
GENRES = [
    ["action", "adventure"],
    ["comedy"],
    ["horror", "thriller"],
    ["sci-fi", "fantasy"],
]

# Mock Content types
CONTENT_TYPES = ["movie", "episode", "ad"]
EVENT_TYPES = ["play", "pause", "stop"]
EVENT_SOURCES = ["web_app", "mobile_app", "smart_tv", "streaming_device"]
LANGUAGES = ["en", "es", "fr"]

# Generate 100 random titles
TITLES = [fake.catch_phrase() for _ in range(100)]


def generate_random_user():
    return {
        "user_id": f"user_{uuid.uuid4()}",
        "device_id": f"{random.choice(EVENT_SOURCES)}_{uuid.uuid4()}",
        "ip_address": fake.ipv4(),
    }


def generate_random_content():
    content_type = random.choice(CONTENT_TYPES)
    # Make ads shorter
    duration = (
        random.randint(30, 7200) if content_type != "ad" else random.randint(15, 120)
    )

    return {
        "content_id": f"content_{uuid.uuid4()}",
        "content_type": content_type,
        "title": random.choice(TITLES),
        "genre": random.choice(GENRES),
        "duration_seconds": duration,
        "language": random.choice(LANGUAGES),
    }


def generate_media_event(event_type=None, user=None, content=None):
    if not event_type:
        event_type = random.choice(EVENT_TYPES)
    if not user:
        user = generate_random_user()
    if not content:
        content = generate_random_content()

    # Set play to be at the start but stop/pause to be somewhere random
    current_timestamp = (
        0.0 if event_type == "play" else random.uniform(0, content["duration_seconds"])
    )

    return MediaEvent(
        event_id=f"event_{uuid.uuid4().hex}",
        event_type=event_type,
        timestamp=datetime.now().isoformat(),
        user_id=user["user_id"],
        device_id=user["device_id"],
        ip_address=user["ip_address"],
        content_id=content["content_id"],
        content_type=content["content_type"],
        title=content["title"],
        genre=content["genre"],
        duration_seconds=content["duration_seconds"],
        language=content["language"],
        current_timestamp=current_timestamp,
        schema_version="1.0",
        event_source=random.choice(EVENT_SOURCES),
    )


def send_events_to_kafka(batch_size, topic, bootstrap_servers):
    producer = KafkaProducer(
        bootstrap_servers="localhost:29092",
    )

    # Pre-generate some users and content for reuse
    users = [generate_random_user() for _ in range(min(batch_size // 10, 100))]
    contents = [generate_random_content() for _ in range(min(batch_size // 5, 200))]

    print(f"Generating {batch_size} events to topic '{topic}'...")

    for i in range(batch_size):
        # Create and send event
        event = generate_media_event(
            user=random.choice(users), content=random.choice(contents)
        )
        producer.send(topic, event.model_dump_json().encode("utf-8"))

        # report/flush every 100 message
        if (i + 1) % 100 == 0:
            print(f"Progress: {i + 1}/{batch_size}")
            producer.flush()

    producer.flush()
    print(f"Completed sending {batch_size} events to '{topic}'")


if __name__ == "__main__":
    # Get configuration from environment variables
    batch_size = int(os.environ.get("BATCH_SIZE", "10000000"))
    topic = os.environ.get("TOPIC", "media-events")
    servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

    send_events_to_kafka(batch_size, topic, servers)
