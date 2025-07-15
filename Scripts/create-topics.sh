#!/bin/sh

echo "Waiting for Kafka to be ready..."
sleep 10

create_topic() {
  local topic=$1
  echo "Creating topic $topic..."
  kafka-topics --bootstrap-server kafka:9092 \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions 1 --replication-factor 1
  
  if [ $? -ne 0 ]; then
    echo "Failed to create topic $topic" >&2
    exit 1
  fi
  echo "Topic $topic created successfully"
}

create_topic "matchmaking.request"
create_topic "matchmaking.complete"

echo "All topics created successfully"
exit 0