#!/bin/bash

# Deploy Dataverse Connector to Confluent Cloud
# This script checks if the connector config exists and deploys it

CONFIG_FILE="dataverse-source-connector.json"
TEMPLATE_FILE="dataverse-source-connector-template.json"
CONNECT_URL="http://localhost:8083/connectors"
PROPERTIES_FILE="confluent-cloud.properties"

# Check if the config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: $CONFIG_FILE does not exist."
    echo "Please create it first by copying the template:"
    echo "  cp $TEMPLATE_FILE $CONFIG_FILE"
    echo "Then edit it with your Dataverse credentials."
    exit 1
fi

# Check if the properties file exists
if [ ! -f "$PROPERTIES_FILE" ]; then
    echo "Error: $PROPERTIES_FILE does not exist."
    echo "Please create it first from the template and add your Confluent Cloud credentials."
    exit 1
fi

# Source the Confluent Cloud properties
source $PROPERTIES_FILE

# Construct the SASL JAAS config properly with double quotes to handle spaces
SASL_JAAS_CONFIG="org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${KAFKA_API_KEY}\" password=\"${KAFKA_API_SECRET}\";"

# Replace variables in the connector config
TMP_CONFIG=$(mktemp)
sed "s|\${SCHEMA_REGISTRY_URL}|$SCHEMA_REGISTRY_URL|g; s|\${SCHEMA_REGISTRY_API_KEY}|$SCHEMA_REGISTRY_API_KEY|g; s|\${SCHEMA_REGISTRY_API_SECRET}|$SCHEMA_REGISTRY_API_SECRET|g" $CONFIG_FILE > $TMP_CONFIG

# Check if the connector is already deployed
CONNECTOR_STATUS=$(curl -s $CONNECT_URL/dataverse-source-connector 2>/dev/null)
if [[ $CONNECTOR_STATUS == *"\"name\":\"dataverse-source-connector\""* ]]; then
    echo "Connector is already deployed. Deleting it first..."
    curl -s -X DELETE $CONNECT_URL/dataverse-source-connector
    echo
    sleep 2
fi

# Deploy the connector
echo "Deploying Dataverse connector to Confluent Cloud..."
RESPONSE=$(curl -s -X POST -H "Content-Type: application/json" --data @$TMP_CONFIG $CONNECT_URL)
echo $RESPONSE | jq . 2>/dev/null || echo $RESPONSE
echo

# Remove the temporary file
rm $TMP_CONFIG

# Check connector status
echo "Checking connector status..."
sleep 3
STATUS=$(curl -s $CONNECT_URL/dataverse-source-connector/status)
echo $STATUS | jq . 2>/dev/null || echo $STATUS

echo
echo "Connector deployment completed. Use the following command to check status:"
echo "  curl $CONNECT_URL/dataverse-source-connector/status"
echo
echo "To view logs:"
echo "  docker compose logs -f connect"
echo
echo "To view topics in Confluent Cloud:"
echo "  Log in to the Confluent Cloud web interface and navigate to your cluster's Topics section"
echo "  You should see topics with the prefix 'dataverse-'" 