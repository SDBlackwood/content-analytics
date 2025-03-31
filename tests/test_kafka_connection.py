import time
from kafka import KafkaConsumer, KafkaProducer
import json
from kafka.errors import KafkaError
import uuid


def test_kafka_connection():
    """Test basic Kafka connectivity without consumer groups"""
    print("\nTesting Kafka connection...")

    # Create a unique topic for this test
    test_topic = f"test-topic-{uuid.uuid4().hex[:8]}"
    test_value = f"test-value-{uuid.uuid4().hex[:8]}"

    # Create a producer
    try:
        print(f"Creating producer for topic {test_topic}...")
        producer = KafkaProducer(
            bootstrap_servers="localhost:29092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        # Send a test message
        print(f"Sending test message with value: {test_value}")
        future = producer.send(test_topic, {"test": test_value})
        result = future.get(timeout=10)  # Wait for the message to be sent
        print(f"Message sent to partition {result.partition} offset {result.offset}")

        # Flush any pending messages
        producer.flush()

        # Close the producer
        producer.close()

        # Create a consumer with manual partition assignment
        print(f"Creating consumer for topic {test_topic}...")
        consumer = KafkaConsumer(
            bootstrap_servers="localhost:29092",
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            enable_auto_commit=False,
            max_poll_records=10,
        )

        # Subscribe to the topic
        consumer.subscribe([test_topic])

        # Poll for messages
        print("Polling for messages...")
        timeout = time.time() + 30  # 30 second timeout
        message_received = False

        while time.time() < timeout and not message_received:
            records = consumer.poll(timeout_ms=1000)
            for tp, messages in records.items():
                for message in messages:
                    print(f"Received message: {message.value}")
                    if message.value.get("test") == test_value:
                        print("Test value matches!")
                        message_received = True
                        break
            if not records:
                print("No messages received, polling again...")

        assert message_received, "Did not receive the test message"

        # Close the consumer
        consumer.close()
        print("Kafka connection test passed!")

    except KafkaError as e:
        print(f"Kafka error: {e}")
        raise

    except Exception as e:
        print(f"Unexpected error: {e}")
        raise


if __name__ == "__main__":
    test_kafka_connection()
