# Kafka Connect Dataverse Connector

This connector allows you to stream data from Microsoft Dataverse (and Dynamics 365) into Kafka topics. Since Dynamics 365 is built on the Dataverse platform, this connector works seamlessly with both services.

## Features

- Stream data from Microsoft Dataverse/Dynamics 365 tables into Kafka topics
- Support for initial full data load and continuous change tracking
- Delta load capability for efficient change capture
- Support for optimized pagination for large datasets
- Configurable batch sizes for high-throughput environments
- OAuth authentication with Microsoft Entra ID (formerly Azure AD) for secure connectivity
- Automatic schema conversion from Dataverse to Kafka Connect compatible formats

## Getting Started

### Prerequisites

- Apache Kafka 2.6.0 or later
- Kafka Connect runtime
- Java 17 or later
- Microsoft Dataverse or Dynamics 365 instance with **Change Tracking enabled** for the tables you want to sync
- Microsoft Entra ID (Azure AD) connected app with appropriate permissions to access Dataverse/Dynamics 365 Web API

### Building the Connector

To build the connector from source:

```bash
mvn clean package
```

This will create a JAR file in the `target` directory that can be used with Kafka Connect.

### Configuration

The connector supports the following configuration options:

| Name | Description | Default | Importance |
|------|-------------|---------|------------|
| `dataverse.url` | URL of your Dataverse/Dynamics 365 instance | | High |
| `dataverse.login.accessTokenURL` | Microsoft Entra ID OAuth token URL | | High |
| `dataverse.login.clientId` | Microsoft Entra ID application (client) ID | | High |
| `dataverse.login.secret` | Microsoft Entra ID client secret | | High |
| `dataverse.tables` | Comma-separated list of Dataverse/Dynamics 365 tables to sync | | High |
| `dataverse.skip.initial.load` | Whether to skip the initial load and only track changes | false | Medium |
| `dataverse.max.page.size` | Maximum page size for pagination (max allowed by Dataverse is 5000) | 5000 | Medium |
| `dataverse.batch.size` | Maximum batch size for processing entities before flushing to Kafka | 5000 | Medium |
| `poll.interval.ms` | Interval between polls in milliseconds | 30000 | Medium |
| `topic.prefix` | Prefix for the Kafka topic names | dataverse | Medium |

### Example Environments

This project includes complete example environments to help you get started:

1. **Local Environment** (`examples/local`): A complete Kafka ecosystem with Kafka, Schema Registry, and Kafka Connect using Docker Compose with Confluent Platform.

2. **Confluent Cloud Environment** (`examples/confluent-cloud`): Instructions and configuration for deploying the connector to Confluent Cloud.

Please refer to the README files in each example directory for specific setup instructions and available commands.

## How It Works

1. The connector interacts with Dataverse/Dynamics 365 using the Dataverse Web API (OData v4 protocol)
2. For each table, it performs an initial load to retrieve all data (unless skipped)
3. It then uses Dataverse's change tracking capabilities to capture only changes
4. Data is converted to Kafka Connect records and published to Kafka topics
5. Each table's data is published to a separate topic with the configured prefix

### Performance Considerations

- **Max Page Size**: This setting overrides the default Dataverse pagination size. The maximum value supported by Dataverse is 5000. Higher values can improve data retrieval performance but may increase memory usage.

- **Batch Size**: This setting determines how many records are processed before flushing events to Kafka and committing offsets. It allows you to balance between:
  - Processing efficiency (larger batches mean fewer commits)
  - Memory usage (smaller batches use less memory)
  - Resilience (smaller batches mean less data to re-process if a failure occurs)
  - Throttling compliance (smaller batches help avoid hitting Dataverse API rate limits)

### Important Notes

- **Change Tracking**: Ensure that change tracking is enabled for any tables you want to sync in your Dataverse/Dynamics 365 instance.

- **Skip Token Usage**: The connector parses Dataverse skip tokens for batching and skipping the initial load. Microsoft advises that these tokens should not be parsed directly as their format may change in future updates.

- **API Throttling**: Dataverse/Dynamics 365 implements API throttling limits. Consider this when configuring batch sizes and poll intervals to avoid exceeding these limits.

## Development

### Integration Testing

To run the integration tests against a real Dataverse instance:

1. Create a file at `src/integration-test/resources/integration-test.properties` with your test credentials
2. Run the integration tests with: `mvn integration-test`

### Contributing

Contributions are welcome! Feel free to submit issues and pull requests.

## License

This project is licensed under the Apache License, Version 2.0 - see the [LICENSE](LICENSE) file for details.

```
Copyright 2024 Callum Edwards

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. 