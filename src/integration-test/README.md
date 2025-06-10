# Integration Tests for Kafka Connect Dataverse Connector

This directory contains integration tests for the Kafka Connect Dataverse Connector. These tests connect to a real Dataverse instance to verify the connector's functionality.

## Configuration

Before running the integration tests, you need to set up a configuration file with connection details for your Dataverse instance:

1. Copy the template configuration file:
   ```
   cp src/integration-test/resources/integration-test.properties.template src/integration-test/resources/integration-test.properties
   ```

2. Edit the configuration file and fill in your Dataverse instance details:
   ```
   vim src/integration-test/resources/integration-test.properties
   ```

The configuration file should include:
```properties
# Dataverse instance details
dataverse.url=https://yourinstance.crm.dynamics.com
dataverse.login.accessTokenURL=https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token
dataverse.login.clientId=your-client-id
dataverse.login.secret=your-client-secret

# Tables to test with
dataverse.tables=accounts,contacts

# Sample record for delta testing (JSON format)
dataverse.delta.record={'table': 'accounts', 'name': 'Test Account', 'emailaddress1': 'test@example.com', 'telephone1': '123-456-7890'}

# Test timeout in seconds
integration.test.timeout.seconds=30
```

> **Note:** The `integration-test.properties` file is excluded from version control in `.gitignore` to prevent accidentally committing sensitive credentials.

## Running Integration Tests

To run the integration tests:

```bash
mvn integration-test
```

This will:
1. Compile the project
2. Run unit tests
3. Run integration tests

To run a specific test class:

```bash
mvn integration-test -Dtest=OlingoDataverseConnectionIT
```

To run a specific test method:

```bash
mvn integration-test -Dtest=OlingoDataverseConnectionIT#testSkipInitialLoad
```

## Viewing Logs and Test Results

The integration tests use SLF4J with Logback for logging. The configuration is set up to:

1. Write logs to the console during test execution
2. Create a detailed log file at `target/integration-tests.log`

You can view the log file using any text editor or with the command:
```
less target/integration-tests.log
```

Integration test results are stored in:
- `target/failsafe-reports`: Contains test result XML and text files
- `target/integration-tests.log`: Contains detailed logs from the test execution

### Log Levels

The default log configuration sets:
- `DEBUG` level for project classes
- `WARN` level for third-party libraries

To change log levels, edit the file at `src/integration-test/resources/logback-test.xml`.

## Skip Integration Tests

If you want to skip the integration tests but still run unit tests:

```bash
mvn clean verify -DskipITs
```

## Test Classes

The integration tests are organized into the following classes:

- **OlingoDataverseConnectionIT**: Tests the connectivity to Dataverse, metadata retrieval, initial data loading, delta tracking, and pagination.
  - `testConnectAndRetrieveMetadata`: Verifies connection and metadata retrieval
  - `testInitialLoad`: Tests initial data loading
  - `testDeltaLoad`: Tests delta change tracking
  - `testPagination`: Tests pagination functionality
  - `testCrudOperationsAndChanges`: Tests CRUD operations and change tracking
  - `testChangeTrackingWithPagination`: Tests change tracking with pagination
  - `testPaginationWithSmallBatchSize`: Tests pagination with small batch sizes
  - `testChangeTrackingWithPaginationAndSmallBatchSize`: Tests change tracking with pagination and small batch sizes
  - `testSkipInitialLoad`: Tests skipping initial load and only tracking changes

- **DataverseSourceTaskIT**: Tests the Kafka Connect source task functionality.
  - `testTaskInitialLoad`: Tests initial load within the task
  - `testTaskDeltaLoad`: Tests delta load within the task
  - `testTaskPollInterval`: Tests the poll interval behavior
  - `testTaskRestartWithOffset`: Tests resuming from saved offsets

- **DataverseSourceTaskDeltaIT**: Tests specific delta change detection scenarios.
  - `testDetectHelperChanges`: Tests detection of changes made through the test helper
  - `testDetectNewEntityHelperChanges`: Tests detection of newly created entities

## Environment Resilience

The integration tests are designed to work with standard Dataverse environments that support the OData protocol with change tracking. If you're running these tests against environments with limitations, you might need to:

- Ensure change tracking is enabled for the tables you're testing
- Verify that your Dataverse API version supports delta links and change tracking
- Configure appropriate timeouts for your environment's response times


## Troubleshooting

If integration tests fail:

1. Verify your configuration settings in `integration-test.properties`
2. Check that your Dataverse instance is accessible
3. Verify that you have the necessary permissions to access the tables. "Service Reader", "Service Deleter" and  "Service Writer" permissions are required to run these integration tests.
4. Check that the Azure AD client ID and secret are valid
5. Look for detailed error messages in the test output and log files

## Adding New Integration Tests

When adding new integration tests:

1. Extend the `AbstractDataverseIntegrationTest` class to inherit common setup and configuration
2. Use the naming convention `*IT.java` so the Failsafe plugin can find the tests
3. Add appropriate timeout settings to avoid hanging during CI/CD pipeline runs
4. Add proper cleanup logic to restore the system to its initial state after testing
5. Consider adding resilience mechanisms if your test interacts with features that might not be available in all Dataverse environments 