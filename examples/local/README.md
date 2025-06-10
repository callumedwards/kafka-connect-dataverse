# Dataverse Kafka Connect - Local Demo

This directory contains resources to run a local demo of the Kafka Connect Dataverse connector. This demo sets up a Kafka ecosystem including Kafka broker (running in KRaft mode), Schema Registry, and Kafka Connect using Docker Compose.

## Prerequisites

- Docker and Docker Compose
- Java 11 or higher
- Maven
- Microsoft Dataverse instance with proper permissions
- Azure AD application with appropriate permissions to access Dataverse

## Building the Connector

Before running the local demo, you need to build the connector from the project root:

```bash
cd ../../
mvn clean package
```

This will create the connector JAR file in the `target` directory, which will be mounted into the Kafka Connect container.

## Setting Up the Environment

1. Copy the template config file to create your actual configuration:

```bash
cp dataverse-source-connector-template.json dataverse-source-connector.json
```

2. Edit `dataverse-source-connector.json` with your Dataverse and Azure AD credentials:
   - `dataverse.url`: Your Dataverse instance URL (e.g., https://yourorg.crm.dynamics.com)
   - `dataverse.login.accessTokenURL`: Your Azure AD OAuth token URL
   - `dataverse.login.clientId`: Your Azure AD application (client) ID
   - `dataverse.login.secret`: Your Azure AD client secret
   - `dataverse.tables`: Comma-separated list of Dataverse tables to sync

## Starting the Demo Environment

Start the Docker environment:

```bash
docker compose up -d
```

Alternatively, use the provided script:

```bash
./start-environment.sh
```

This will start the following services:
- Kafka Broker (running in KRaft mode without Zookeeper)
- Schema Registry
- Kafka Connect

## Deploying the Connector

Once the environment is up and running, you can deploy the Dataverse source connector using the REST API:

```bash
curl -X POST -H "Content-Type: application/json" --data @dataverse-source-connector.json http://localhost:8083/connectors
```

Or use the provided script:

```bash
./deploy-connector.sh
```

## Monitoring the Connector

You can monitor the connector in several ways:

1. **Kafka Connect REST API**:
   - Check connector status: `curl http://localhost:8083/connectors/dataverse-source-connector/status`
   - List topics: `curl http://localhost:8083/connectors/dataverse-source-connector/topics`

2. **Using the check-topics script**:
   - List all topics with the Dataverse prefix: `./check-topics.sh`
   - View messages for a specific topic: `./check-topics.sh -t dataverse-accounts`

## Consuming Data

To consume data from the topics created by the connector:

```bash
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic dataverse-accounts --from-beginning
```

Or use the provided script:

```bash
./check-topics.sh -t dataverse-accounts
```

## Stopping the Demo Environment

To stop the environment:

```bash
docker compose down
```

Or use the provided script:

```bash
./stop-environment.sh
```

## Troubleshooting

If you encounter issues with the connector:

1. Check the Kafka Connect logs:
```bash
docker compose logs -f connect
```

2. Check the Kafka broker logs:
```bash
docker compose logs -f kafka
```

3. Verify your Dataverse and Azure AD credentials in the connector configuration
4. Ensure your Azure AD application has appropriate permissions to access Dataverse
5. Check if change tracking is enabled for the tables you're trying to sync

## Additional Resources

- [Project Documentation](../../README.md)
- [Kafka Connect Documentation](https://docs.confluent.io/platform/current/connect/index.html)
- [Dataverse Web API Documentation](https://docs.microsoft.com/en-us/dynamics365/customer-engagement/web-api/about) 