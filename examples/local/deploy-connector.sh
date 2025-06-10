#!/bin/bash

# Deploy Dataverse Connector
# This script checks if the connector config exists and deploys it

CONFIG_FILE="dataverse-source-connector.json"
TEMPLATE_FILE="dataverse-source-connector-template.json"
CONNECT_URL="http://localhost:8083/connectors"

# Check if the config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: $CONFIG_FILE does not exist."
    echo "Please create it first by copying the template:"
    echo "  cp $TEMPLATE_FILE $CONFIG_FILE"
    echo "Then edit it with your Dataverse credentials."
    exit 1
fi

# Check if the connector is already deployed
CONNECTOR_STATUS=$(curl -s $CONNECT_URL/dataverse-source-connector 2>/dev/null)
if [[ $CONNECTOR_STATUS == *"\"name\":\"dataverse-source-connector\""* ]]; then
    echo "Connector is already deployed. Deleting it first..."
    curl -s -X DELETE $CONNECT_URL/dataverse-source-connector
    echo
    sleep 2
fi

# Deploy the connector
echo "Deploying Dataverse connector..."
RESPONSE=$(curl -s -X POST -H "Content-Type: application/json" --data @$CONFIG_FILE $CONNECT_URL)
echo $RESPONSE | jq . 2>/dev/null || echo $RESPONSE
echo

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