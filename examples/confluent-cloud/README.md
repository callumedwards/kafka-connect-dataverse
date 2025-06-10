# Dataverse Kafka Connect with Confluent Cloud

This directory contains resources to run the Kafka Connect Dataverse connector with Confluent Cloud. This setup allows you to stream data from Microsoft Dataverse to Confluent Cloud Kafka topics using a local Kafka Connect worker.

## Prerequisites

- Docker and Docker Compose
- Java 11 or higher
- Maven
- Confluent Cloud account with:
  - Kafka cluster
  - Schema Registry
  - API keys for both services
- Microsoft Dataverse instance with proper permissions
- Azure AD application with appropriate permissions to access Dataverse

## Building the Connector

Before running this example, you need to build the connector from the project root:

```bash
cd ../../
mvn clean package
```

This will create the connector JAR file in the `target` directory, which will be mounted into the Kafka Connect container.

## Setting Up the Environment

### 1. Configure Confluent Cloud Credentials

Copy the template properties file to create your actual configuration:

```bash
cp confluent-cloud-template.properties confluent-cloud.properties
```

Edit `confluent-cloud.properties` with your Confluent Cloud credentials:
- `BOOTSTRAP_SERVERS`: Your Confluent Cloud Kafka bootstrap servers
- `KAFKA_API_KEY`: Your Confluent Cloud Kafka API key
- `KAFKA_API_SECRET`: Your Confluent Cloud Kafka API secret
- `SCHEMA_REGISTRY_URL`: Your Confluent Cloud Schema Registry URL
- `SCHEMA_REGISTRY_API_KEY`: Your Schema Registry API key
- `SCHEMA_REGISTRY_API_SECRET`: Your Schema Registry API secret

Note: The SASL JAAS configuration is automatically constructed in the scripts using the provided credentials, so you don't need to manually configure it.

### 2. Configure the Dataverse Connector

Copy the template connector configuration file:

```bash
cp dataverse-source-connector-template.json dataverse-source-connector.json
```

Edit `dataverse-source-connector.json` with your Dataverse and Azure AD credentials:
- `dataverse.url`: Your Dataverse instance URL (e.g., https://yourorg.crm.dynamics.com)
- `dataverse.login.accessTokenURL`: Your Azure AD OAuth token URL
- `dataverse.login.clientId`: Your Azure AD application (client) ID
- `dataverse.login.secret`: Your Azure AD client secret
- `dataverse.tables`: Comma-separated list of Dataverse tables to sync

The Schema Registry URL and credentials will be automatically filled in from your Confluent Cloud properties when you deploy the connector.

### 3. Create Topics in Confluent Cloud

Unlike local Kafka installations, Confluent Cloud does not allow automatic topic creation by default. You need to pre-create the topics that will store data from Dataverse.

For each table specified in your connector configuration, create a topic with the following naming pattern:

```
dataverse-<table-name>
```

For example, if your connector is configured to sync the `accounts` and `contacts` tables, create these topics:
- `dataverse-accounts`  
- `dataverse-contacts`

You can create topics using the Confluent Cloud web interface or using the Confluent CLI:

```bash
# Install the Confluent CLI if you haven't already
# Follow instructions at: https://docs.confluent.io/confluent-cli/current/install.html

# Login to Confluent Cloud
confluent login

# Create topics (adjust partitions as needed)
confluent kafka topic create dataverse-accounts --partitions 6
confluent kafka topic create dataverse-contacts --partitions 6
```

## Starting the Environment

Start the Docker environment:

```bash
./start-environment.sh
```

This script will:
1. Check for the connector JAR file
2. Verify that Docker is running
3. Check for and create the Confluent Cloud properties file if needed
4. Properly format the SASL JAAS configuration for Confluent Cloud authentication
5. Start the Kafka Connect worker container
6. Wait for the Kafka Connect REST API to be available
7. Provide instructions for creating topics if they don't already exist

## Deploying the Connector

Once the environment is up and running and you've created the necessary topics, deploy the Dataverse connector:

```bash
./deploy-connector.sh
```

This script will:
1. Check that all required configuration files exist
2. Replace variables in the connector configuration with values from your Confluent Cloud properties
3. Deploy the connector to the local Kafka Connect worker
4. Check the connector's status

## Monitoring the Connector

You can monitor the connector in several ways:

1. **Kafka Connect REST API**:
   - Check connector status: `curl http://localhost:8083/connectors/dataverse-source-connector/status`
   - List deployed connectors: `curl http://localhost:8083/connectors`

2. **Container Logs**:
   - View the Kafka Connect logs: `docker compose logs -f connect`

3. **Confluent Cloud UI**:
   - Log in to the Confluent Cloud web interface
   - Navigate to your cluster's Topics section
   - You should see data flowing into the topics you created

## Stopping the Environment

To stop the environment:

```bash
./stop-environment.sh
```

You'll be asked if you want to remove all containers and volumes or just stop the containers.

## Troubleshooting

If you encounter issues with the connector:

1. Check the Kafka Connect logs:
```bash
docker compose logs -f connect
```

2. Verify your Confluent Cloud credentials in `confluent-cloud.properties`
3. Verify your Dataverse and Azure AD credentials in `dataverse-source-connector.json`
4. Ensure your Azure AD application has appropriate permissions to access Dataverse
5. Check if change tracking is enabled for the tables you're trying to sync
6. Verify that your Confluent Cloud Kafka cluster and Schema Registry are accessible 
7. Confirm that all required topics have been created in Confluent Cloud
8. If you see `UNKNOWN_TOPIC_OR_PARTITION` errors, it means the topic doesn't exist or the connector doesn't have permission to access it

## Additional Resources

- [Project Documentation](../../README.md)
- [Confluent Cloud Documentation](https://docs.confluent.io/cloud/current/overview.html)
- [Kafka Connect Documentation](https://docs.confluent.io/platform/current/connect/index.html)
- [Dataverse Web API Documentation](https://docs.microsoft.com/en-us/dynamics365/customer-engagement/web-api/about) 