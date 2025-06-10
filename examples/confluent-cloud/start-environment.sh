#!/bin/bash

# Start Dataverse Connector with Confluent Cloud
# This script starts the Docker environment and checks for required files

JAR_PATTERN="../../target/kafka-connect-dataverse-*with-dependencies.jar"
PROPERTIES_FILE="confluent-cloud.properties"
PROPERTIES_TEMPLATE="confluent-cloud-template.properties"
CONNECTOR_FILE="dataverse-source-connector.json"
CONNECTOR_TEMPLATE="dataverse-source-connector-template.json"

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

# Check for the Confluent Cloud properties file
if [ ! -f "$PROPERTIES_FILE" ]; then
    echo "Notice: $PROPERTIES_FILE does not exist."
    echo "Would you like to create it from the template? (y/n)"
    read -r RESPONSE
    if [[ "$RESPONSE" =~ ^([yY][eE][sS]|[yY])$ ]]; then
        cp $PROPERTIES_TEMPLATE $PROPERTIES_FILE
        echo "Created $PROPERTIES_FILE from template."
        echo "Please edit this file with your Confluent Cloud credentials."
        exit 1
    else
        echo "Confluent Cloud properties file is required. Exiting."
        exit 1
    fi
fi

# Source the Confluent Cloud properties
source $PROPERTIES_FILE

# Construct the SASL JAAS config properly with double quotes to handle spaces
SASL_JAAS_CONFIG="org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${KAFKA_API_KEY}\" password=\"${KAFKA_API_SECRET}\";"

# Export variables for Docker Compose
export BOOTSTRAP_SERVERS
export SCHEMA_REGISTRY_URL
export SCHEMA_REGISTRY_API_KEY
export SCHEMA_REGISTRY_API_SECRET
export SASL_JAAS_CONFIG

# Debug - print the variables (remove sensitive data in production)
echo "Using Bootstrap Servers: $BOOTSTRAP_SERVERS"
echo "Using Schema Registry URL: $SCHEMA_REGISTRY_URL"
echo "SASL JAAS Config set properly"

# Check for the connector configuration
if [ ! -f "$CONNECTOR_FILE" ]; then
    echo "Notice: $CONNECTOR_FILE does not exist."
    echo "Would you like to create it from the template? (y/n)"
    read -r RESPONSE
    if [[ "$RESPONSE" =~ ^([yY][eE][sS]|[yY])$ ]]; then
        # Replace variables in the template
        sed "s|\${SCHEMA_REGISTRY_URL}|$SCHEMA_REGISTRY_URL|g; s|\${SCHEMA_REGISTRY_API_KEY}|$SCHEMA_REGISTRY_API_KEY|g; s|\${SCHEMA_REGISTRY_API_SECRET}|$SCHEMA_REGISTRY_API_SECRET|g" $CONNECTOR_TEMPLATE > $CONNECTOR_FILE
        echo "Created $CONNECTOR_FILE from template."
        echo "Please edit this file with your Dataverse credentials."
    else
        echo "Skipping configuration creation."
    fi
fi

# Start the Docker environment
echo "Starting Docker environment with Confluent Cloud configuration..."
docker compose up -d

# Wait for Kafka Connect to be ready
MAX_ATTEMPTS=30
ATTEMPT=0
echo "Waiting for Kafka Connect to start..."
echo "This may take a minute or two..."
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

echo
echo "Environment started successfully!"
echo
echo "Kafka Connect API: http://localhost:8083"
echo
echo "To deploy the connector:"
echo "  ./deploy-connector.sh"
echo
echo "To stop the environment:"
echo "  ./stop-environment.sh" 