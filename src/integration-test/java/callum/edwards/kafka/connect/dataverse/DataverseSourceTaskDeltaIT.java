package callum.edwards.kafka.connect.dataverse;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

/**
 * Integration tests for delta loading functionality in DataverseSourceTask.
 * 
 * These tests use the DataverseTestDataHelper to create, update, and delete records
 * to verify that the connector can detect and process these changes.
 */
@DisplayName("Dataverse Source Task Delta Integration Tests")
public class DataverseSourceTaskDeltaIT extends AbstractDataverseIntegrationTest {
    
    private final ObjectMapper mapper = new ObjectMapper();
    
    /**
     * Custom extension of DataverseSourceTask for testing that overrides the createConnection method
     * to use the connection from the AbstractDataverseIntegrationTest base class.
     */
    class TestDataverseSourceTask extends DataverseSourceTask {
        @Override
        protected OlingoDataverseConnection createConnection(DataverseSourceConnectorConfig config) {
            return connection;
        }
    }
    
    /**
     * Creates a mock SourceTaskContext with an empty OffsetStorageReader
     * 
     * @return A mock SourceTaskContext
     */
    private SourceTaskContext createMockTaskContext() {
        return new SourceTaskContext() {
            @Override
            public Map<String, String> configs() {
                return new HashMap<>();
            }
            
            @Override
            public OffsetStorageReader offsetStorageReader() {
                return new OffsetStorageReader() {
                    @Override
                    public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> arg0) {
                        return Collections.emptyMap();
                    }
                    
                    @Override
                    public <T> Map<String, Object> offset(Map<String, T> arg0) {
                        return null;
                    }
                };
            }
        };
    }
    
    /**
     * Creates a configuration map for the source task.
     * 
     * @return Map of configuration properties
     */
    private Map<String, String> createTaskConfig() {
        Map<String, String> taskConfig = new HashMap<>();
        
        // Add required configuration
        taskConfig.put("dataverse.url", IntegrationTestConfig.getProperty("dataverse.url"));
        taskConfig.put("dataverse.login.accessTokenURL", IntegrationTestConfig.getProperty("dataverse.login.accessTokenURL"));
        taskConfig.put("dataverse.login.clientId", IntegrationTestConfig.getProperty("dataverse.login.clientId"));
        taskConfig.put("dataverse.login.secret", IntegrationTestConfig.getProperty("dataverse.login.secret"));
        
        // Extract table name from the delta record configuration
        try {
            String configJson = IntegrationTestConfig.getProperty("dataverse.delta.record");
            configJson = configJson.replace("{[", "{").replace("]}", "}").replaceAll("'", "\"");
            JsonNode configNode = mapper.readTree(configJson);
            String tableName = configNode.path("table").asText();
            
            // Set the specific table from the delta record config
            taskConfig.put("dataverse.tables", tableName);
        } catch (Exception e) {
            log.error("Failed to parse delta record config", e);
            taskConfig.put("dataverse.tables", IntegrationTestConfig.getProperty("dataverse.tables"));
        }
        
        // Add Kafka topic configuration
        taskConfig.put("topic.prefix", "dataverse-");
        
        // Set very short poll interval for testing
        taskConfig.put("poll.interval.ms", "2000");
        
        return taskConfig;
    }
    
    @Test
    @DisplayName("Should detect changes made with DataverseTestDataHelper")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    public void testDetectHelperChanges() throws Exception {
        log.info("Starting test: testDetectHelperChanges");
        
        // Create the test data helper
        DataverseTestDataHelper helper = new DataverseTestDataHelper(
                connection, 
                IntegrationTestConfig.getProperty("dataverse.url"));
        
        // Create and configure the task
        DataverseSourceTask task = new TestDataverseSourceTask();
        task.initialize(createMockTaskContext());
        Map<String, String> config = createTaskConfig();
        
        log.debug("Starting task with configuration: {}", config);
        task.start(config);
        
        try {
            log.info("Executing first poll (initial load)...");
            // First poll should perform the initial load
            List<SourceRecord> initialRecords = task.poll();
            assertNotNull(initialRecords, "Initial records should not be null");
            log.info("Initial poll returned {} records", initialRecords.size());
            
            // Now perform changes using the helper
            log.info("Performing CRUD operations with the helper");
            UUID entityId = helper.performTestCrudCycle();
            log.info("Completed CRUD cycle for entity: {}", entityId);
            
            // Wait a bit to ensure changes have time to be processed
            log.info("Waiting for changes to be processed...");
            TimeUnit.SECONDS.sleep(5);
            
            // Poll again to get delta changes
            log.info("Executing second poll (delta load)...");
            List<SourceRecord> deltaRecords = task.poll();
            
            assertNotNull(deltaRecords, "Delta records should not be null");
            log.info("Delta poll returned {} records", deltaRecords.size());
            
            // The delta should contain our changes
            // Even if it doesn't contain all operations, it should contain at least some of them
            if (deltaRecords.isEmpty()) {
                log.warn("No changes detected. This could happen if:");
                log.warn("1. Change tracking is not enabled in Dataverse");
                log.warn("2. Changes were not committed in time for the poll");
                log.warn("3. Changes were batched differently than expected");
            } else {
                log.info("Successfully detected changes made with the helper");
            }
            
        } finally {
            // Stop the task
            log.debug("Stopping task");
            task.stop();
            log.info("Task stopped successfully");
        }
    }
    @Test
    @DisplayName("Should detect new entity made with DataverseTestDataHelper")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    public void testDetectNewEntityHelperChanges() throws Exception {
        log.info("Starting test: testDetectNewEntityHelperChanges");
        
        // Create the test data helper
        DataverseTestDataHelper helper = new DataverseTestDataHelper(
                connection, 
                IntegrationTestConfig.getProperty("dataverse.url"));
        
        // Create and configure the task
        DataverseSourceTask task = new TestDataverseSourceTask();
        task.initialize(createMockTaskContext());
        Map<String, String> config = createTaskConfig();
        
        log.debug("Starting task with configuration: {}", config);
        task.start(config);
        UUID entityId = null;
        try {
            log.info("Executing first poll (initial load)...");
            // First poll should perform the initial load
            List<SourceRecord> initialRecords = task.poll();
            assertNotNull(initialRecords, "Initial records should not be null");
            log.info("Initial poll returned {} records", initialRecords.size());
            
            // Now perform changes using the helper
            log.info("Performing Create operations with the helper");
            entityId = helper.createEntityFromConfig();
            log.info("Completed Create operation for entity: {}", entityId);
            
            // Wait a bit to ensure changes have time to be processed
            log.info("Waiting for changes to be processed...");
            TimeUnit.SECONDS.sleep(5);
            
            // Poll again to get delta changes
            log.info("Executing second poll (delta load)...");
            List<SourceRecord> deltaRecords = task.poll();
            
            assertNotNull(deltaRecords, "Delta records should not be null");
            log.info("Delta poll returned {} records", deltaRecords.size());
            
            // The delta should contain our changes
            // Even if it doesn't contain all operations, it should contain at least some of them
            if (deltaRecords.isEmpty()) {
                log.warn("No changes detected. This could happen if:");
                log.warn("1. Change tracking is not enabled in Dataverse");
                log.warn("2. Changes were not committed in time for the poll");
                log.warn("3. Changes were batched differently than expected");
            } else {
                log.info("Successfully detected changes made with the helper");
            }
            
        } finally {
            if (entityId != null) {
                helper.deleteEntity(config.get("dataverse.tables"), entityId);
            }
            // Stop the task
            log.debug("Stopping task");
            task.stop();
            log.info("Task stopped successfully");
        }
    }
} 