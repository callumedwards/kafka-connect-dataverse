package callum.edwards.kafka.connect.dataverse;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.Assumptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Arrays;

/**
 * Abstract base class for Dataverse integration tests.
 * 
 * This class handles common setup for integration tests, including:
 * - Loading and validating configuration
 * - Creating and configuring the Dataverse connection
 * - Cleaning up resources after tests
 * 
 * Tests will be skipped if the required configuration is not available.
 */
@TestInstance(Lifecycle.PER_CLASS)
public abstract class AbstractDataverseIntegrationTest {
    protected static final Logger log = LoggerFactory.getLogger(AbstractDataverseIntegrationTest.class);
    
    // Required configuration properties
    private static final String[] REQUIRED_PROPERTIES = {
        "dataverse.url",
        "dataverse.tables",
        "dataverse.login.accessTokenURL",
        "dataverse.login.clientId",
        "dataverse.login.secret"
    };
    
    // Default timeout for tests
    protected static final int DEFAULT_TIMEOUT_SECONDS = 30;
    
    // Default small page size for pagination testing
    protected static final int SMALL_PAGE_SIZE = 2;
    
    // Shared connection and configuration
    protected OlingoDataverseConnection connection;
    protected String dataverseTables;
    protected int timeoutSeconds;
    
    /**
     * Initializes the test environment.
     * This method is called once before any test methods in the class.
     */
    @BeforeAll
    public void initAll() {
        // Skip tests if required configuration is missing
        boolean hasRequiredProperties = IntegrationTestConfig.hasRequiredProperties(REQUIRED_PROPERTIES);
        
        // Print debug information
        log.info("Integration test configuration check: " + (hasRequiredProperties ? "PASSED" : "FAILED"));
        for (String prop : REQUIRED_PROPERTIES) {
            String value = IntegrationTestConfig.getProperty(prop);
            log.info("Property " + prop + ": " + (value != null && !value.trim().isEmpty() ? "FOUND" : "MISSING"));
        }
        
        Assumptions.assumeTrue(hasRequiredProperties, 
            "Integration tests skipped due to missing configuration. " +
            "Create integration-test.properties from the template and provide required values.");
        
        // Load configuration values
        this.dataverseTables = IntegrationTestConfig.getProperty("dataverse.tables");
        this.timeoutSeconds = IntegrationTestConfig.getIntProperty("integration.test.timeout.seconds", DEFAULT_TIMEOUT_SECONDS);
        
        log.info("Integration test configuration loaded successfully");
        log.info("Using Dataverse tables: {}", dataverseTables);
        log.info("Test timeout: {} seconds", timeoutSeconds);
    }
    
    /**
     * Creates a fresh connection to Dataverse before each test.
     * This ensures each test runs with a clean connection state.
     */
    @BeforeEach
    public void setupConnection() {
        // Close any existing connection
        if (connection != null) {
            try {
                connection.close();
                log.debug("Closed previous Dataverse connection");
            } catch (Exception e) {
                log.warn("Error closing previous connection", e);
            }
        }
        
        // Create a new connection
        log.info("Creating new Dataverse connection to URL: {}", IntegrationTestConfig.getProperty("dataverse.url"));
        connection = new OlingoDataverseConnection(
            IntegrationTestConfig.getProperty("dataverse.login.accessTokenURL"),
            IntegrationTestConfig.getProperty("dataverse.login.clientId"),
            IntegrationTestConfig.getProperty("dataverse.login.secret"),
            IntegrationTestConfig.getProperty("dataverse.url"),
            IntegrationTestConfig.getProperty("dataverse.url") + "/api/data/v9.2/"
        );
        
        log.info("Created fresh Dataverse connection for test");
    }
    
    /**
     * Gets the array of table names from the configuration.
     * 
     * @return Array of table names
     */
    protected String[] getTableNames() {
        String[] tables = dataverseTables.split(",");
        log.debug("Using tables for test: {}", Arrays.toString(tables));
        return tables;
    }
    
    /**
     * Creates a custom connection with a small page size to force pagination
     * even with small datasets.
     * 
     * @return A connection with page size overridden to SMALL_PAGE_SIZE (default 2)
     * @throws RuntimeException if connection creation fails
     */
    protected OlingoDataverseConnection createConnectionWithSmallPageSize() {
        try {
            // Create a new connection with the same parameters as the default connection
            OlingoDataverseConnection customConnection = new OlingoDataverseConnection(
                IntegrationTestConfig.getProperty("dataverse.login.accessTokenURL"),
                IntegrationTestConfig.getProperty("dataverse.login.clientId"),
                IntegrationTestConfig.getProperty("dataverse.login.secret"),
                IntegrationTestConfig.getProperty("dataverse.url"),
                IntegrationTestConfig.getProperty("dataverse.url") + "/api/data/v9.2/",
                true  // initialize client
            );
            
            customConnection.setMaxPageSize(SMALL_PAGE_SIZE);
            log.info("Created custom connection with maxPageSize = {}", SMALL_PAGE_SIZE);
            return customConnection;
        } catch (Exception e) {
            log.error("Failed to create custom connection with small page size", e);
            throw new RuntimeException("Failed to set up pagination test", e);
        }
    }
} 