# Mock data script to generate 1M records into kafka (10 x 100k) for testing purposes
import uuid
import random
import asyncio
from datetime import datetime
from aiokafka import AIOKafkaProducer
from faker import Faker


from content_analytics.utils.data_model import MediaEvent
from content_analytics.utils.config import settings

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


async def send_events_to_kafka(batch_size, topic, bootstrap_servers):
    # Create AIOKafkaProducer
    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
    )
    await producer.start()

    try:
        # Create random users and content of either 100 users/10% of batch size or 200 contents/5% of batch size
        users = [generate_random_user() for _ in range(min(batch_size // 10, 100))]
        contents = [generate_random_content() for _ in range(min(batch_size // 5, 200))]

        print(f"Generating {batch_size} events to topic '{topic}'...")

        tasks = []
        for i in range(batch_size):
            # Create event
            event = generate_media_event(
                user=random.choice(users), content=random.choice(contents)
            )
            # Send event asynchronously
            task = producer.send(topic, event.model_dump_json().encode("utf-8"))
            tasks.append(task)

            # report/flush every 100 messages
            if (i + 1) % 100 == 0:
                print(f"Progress: {i + 1}/{batch_size}")

                # Wait for all pending messages to be sent
                if tasks:
                    await asyncio.gather(*tasks)
                    await producer.flush()
                    # Clear the tasks as they have been flushed
                    tasks = []

        # Final flush and wait for remaining tasks
        if tasks:
            await asyncio.gather(*tasks)
        await producer.flush()
        print(f"Completed sending {batch_size} events to '{topic}'")
    finally:
        await producer.stop()


if __name__ == "__main__":
    # Get configuration from environment variables
    batch_size = settings.batch_size
    topic = settings.kafka_topic
    servers = settings.kafka_bootstrap_servers

    # Run the async function
    asyncio.run(send_events_to_kafka(batch_size, topic, servers))
