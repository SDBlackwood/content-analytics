#!/bin/bash
set -e

# Creates the kafka topic for which can be triggered via docker composer

# Wait for Kafka to be available
echo "Waiting for Kafka to be available..."
until nc -z kafka 9092
do
  echo "Kafka is unavailable - sleeping"
  sleep 2
done
echo "Kafka is up - creating topics"

echo "Creating media-events topic..."
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic media-events --partitions 3 --replication-factor 1

echo "Creating consumer group..."
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group storage --topic media-events --describe || true

echo "Listing all topics:"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

echo "topics created" 