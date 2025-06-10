#!/bin/bash

# Check Dataverse Connector Topics
# This script lists the topics created by the Dataverse connector and their messages

# Default prefix
PREFIX="dataverse"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    -p|--prefix)
      PREFIX="$2"
      shift 2
      ;;
    -t|--topic)
      SPECIFIC_TOPIC="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# List all topics
echo "Listing all Kafka topics:"
TOPICS=$(docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list)
echo "$TOPICS"
echo

# Filter topics created by the connector
echo "Filtering for topics with prefix: $PREFIX"
DATAVERSE_TOPICS=$(echo "$TOPICS" | grep "^$PREFIX")

if [ -z "$DATAVERSE_TOPICS" ]; then
  echo "No topics found with prefix $PREFIX"
  echo "The connector may not have created any topics yet."
  exit 0
fi

echo "$DATAVERSE_TOPICS"
echo

# If a specific topic was specified, show its messages
if [ ! -z "$SPECIFIC_TOPIC" ]; then
  echo "Showing messages for topic: $SPECIFIC_TOPIC"
  
  # Check if the topic exists
  if [[ "$TOPICS" != *"$SPECIFIC_TOPIC"* ]]; then
    echo "Error: Topic $SPECIFIC_TOPIC does not exist."
    exit 1
  fi
  
  echo "Press Ctrl+C to stop viewing messages."
  echo "Starting consumer..."
  echo
  
  docker compose exec schema-registry kafka-avro-console-consumer --bootstrap-server kafka:9092 \
    --topic "$SPECIFIC_TOPIC" --from-beginning
else
  echo "To view messages for a specific topic, run:"
  echo "  ./check-topics.sh -t TOPIC_NAME"
  echo
  echo "Available topics:"
  for topic in $DATAVERSE_TOPICS; do
    echo "  $topic"
  done
fi 