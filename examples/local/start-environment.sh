#!/bin/bash

# Start Dataverse Connector Demo Environment
# This script starts the Docker environment and checks for the connector JAR

JAR_PATTERN="../../target/kafka-connect-dataverse-*with-dependencies.jar"

# Check if the connector JAR exists
if ! ls $JAR_PATTERN 1> /dev/null 2>&1; then
    echo "Error: Connector JAR not found in ../../target/"
    echo "Please build the project first with:"
    echo "  cd ../../"
    echo "  mvn clean package"
    exit 1
fi

# Find the exact path to the JAR file
export FILE_PATH=$(find ../../target -name "kafka-connect-dataverse-*with-dependencies.jar" -type f | head -1)
echo "Found connector JAR: $FILE_PATH"

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
    echo "Error: Docker is not running."
    echo "Please start Docker and try again."
    exit 1
fi

# Start the Docker environment
echo "Starting Docker environment..."
docker compose up -d

# Wait for services to be ready
echo "Waiting for services to start up..."
echo "This may take a minute or two..."

# Wait for Kafka Connect to be ready
MAX_ATTEMPTS=30
ATTEMPT=0
while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    ATTEMPT=$((ATTEMPT+1))
    echo -n "."
    
    STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8083/connectors)
    if [ $STATUS -eq 200 ]; then
        echo
        echo "Kafka Connect is ready!"
        break
    fi
    
    sleep 3
    
    if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
        echo
        echo "Error: Timed out waiting for Kafka Connect to start."
        echo "Check the logs with: docker compose logs connect"
        exit 1
    fi
done

# Check for the connector configuration
CONFIG_FILE="dataverse-source-connector.json"
TEMPLATE_FILE="dataverse-source-connector-template.json"

if [ ! -f "$CONFIG_FILE" ]; then
    echo
    echo "Notice: $CONFIG_FILE does not exist."
    echo "Would you like to create it from the template? (y/n)"
    read -r RESPONSE
    if [[ "$RESPONSE" =~ ^([yY][eE][sS]|[yY])$ ]]; then
        cp $TEMPLATE_FILE $CONFIG_FILE
        echo "Created $CONFIG_FILE from template."
        echo "Please edit this file with your Dataverse credentials."
    else
        echo "Skipping configuration creation."
    fi
fi

echo
echo "Environment started successfully!"
echo
echo "Kafka broker is running in KRaft mode at: localhost:29092"
echo "Schema Registry: http://localhost:8081"
echo "Kafka Connect API: http://localhost:8083"
echo
echo "To deploy the connector:"
echo "  ./deploy-connector.sh"
echo
echo "To check for created topics:"
echo "  ./check-topics.sh"
echo
echo "To stop the environment:"
echo "  ./stop-environment.sh" 