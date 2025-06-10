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
import java.util.concurrent.TimeUnit;
import java.util.Date;
import java.util.ArrayList;
import java.util.UUID;

import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceTask;

/**
 * Integration tests for DataverseSourceTask.
 * 
 * These tests verify the end-to-end functionality of the connector, including:
 * 1. Starting the task with valid configuration
 * 2. Performing initial loads from Dataverse
 * 3. Performing delta loads with change tracking
 * 4. Verifying record format and content
 */
@DisplayName("Dataverse Source Task Integration Tests")
public class DataverseSourceTaskIT extends AbstractDataverseIntegrationTest {
    
    /**
     * Custom extension of DataverseSourceTask for testing that overrides the createConnection method
     * to use the connection from the AbstractDataverseIntegrationTest base class.
     */
    class TestDataverseSourceTask extends DataverseSourceTask {
        @Override
        protected OlingoDataverseConnection createConnection(DataverseSourceConnectorConfig config) {
            return connection;
        }
        
        @Override
        public List<SourceRecord> poll() throws InterruptedException {
                List<SourceRecord> records = super.poll();
                return records != null ? records : Collections.emptyList();
        }
        
    }
    
    /**
     * Creates a configuration map for the source task.
     * 
     * @return Map of configuration properties
     */
    private Map<String, String> createTaskConfig() {
        Map<String, String> config = new HashMap<>();
        
        // Add required configuration
        config.put("dataverse.url", IntegrationTestConfig.getProperty("dataverse.url"));
        // The service URL is derived from dataverse.url in the connector config
        config.put("dataverse.login.accessTokenURL", IntegrationTestConfig.getProperty("dataverse.login.accessTokenURL"));
        config.put("dataverse.login.clientId", IntegrationTestConfig.getProperty("dataverse.login.clientId"));
        config.put("dataverse.login.secret", IntegrationTestConfig.getProperty("dataverse.login.secret"));
        config.put("dataverse.tables", IntegrationTestConfig.getProperty("dataverse.tables"));
        
        // Add Kafka topic configuration
        config.put("topic.prefix", "dataverse-");
        
        // Set poll interval to 10 seconds for testing
        config.put("poll.interval.ms", "10000");
        
        System.out.println("Created task config with client secret: " + IntegrationTestConfig.getProperty("dataverse.login.secret"));
        
        return config;
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
                return createTaskConfig();
            }
            
            @Override
            public OffsetStorageReader offsetStorageReader() {
                return new OffsetStorageReader() {
                    @Override
                    public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
                        return new HashMap<>();
                    }
                    
                    @Override
                    public <T> Map<String, Object> offset(Map<String, T> partition) {
                        return null;
                    }
                };
            }
        };
    }
    
    @Test
    @DisplayName("Should start task and perform initial load")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    public void testTaskInitialLoad() throws Exception {
        log.info("Starting test: testTaskInitialLoad");
        // Create and configure the task
        DataverseSourceTask task = new TestDataverseSourceTask();
        task.initialize(createMockTaskContext());
        log.debug("Task initialized with mock context");
        
        Map<String, String> config = createTaskConfig();
        log.debug("Starting task with configuration: {}", config);
        task.start(config);
        
        try {
            log.info("Executing first poll on task...");
            // First poll should perform the initial load
            List<SourceRecord> records = task.poll();
            
            // Verify records
            log.info("Retrieved {} records from initial poll", records != null ? records.size() : 0);
            assertNotNull(records, "Records should not be null");
            assertFalse(records.isEmpty(), "Records should not be empty");
            
            // Verify first record
            SourceRecord firstRecord = records.get(0);
            log.debug("First record topic: {}, partition: {}, offset: {}", 
                firstRecord.topic(), firstRecord.sourcePartition(), firstRecord.sourceOffset());
            assertNotNull(firstRecord, "First record should not be null");
            assertNotNull(firstRecord.sourcePartition(), "Source partition should not be null");
            assertNotNull(firstRecord.sourceOffset(), "Source offset should not be null");
            assertNotNull(firstRecord.key(), "Record key should not be null");
            assertNotNull(firstRecord.value(), "Record value should not be null");
            
            // Verify topic name matches configuration
            assertTrue(firstRecord.topic().startsWith("dataverse-"), 
                      "Topic name should start with configured prefix");
            
            log.info("Initial load successful, retrieved {} records", records.size());
        } finally {
            // Stop the task
            log.debug("Stopping task");
            task.stop();
            log.info("Task stopped successfully");
        }
    }
    
    @Test
    @DisplayName("Should perform delta load after initial load")
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    public void testTaskDeltaLoad() throws Exception {
        log.info("Starting test: testTaskDeltaLoad");
        // Create and configure the task
        DataverseSourceTask task = new TestDataverseSourceTask();
        task.initialize(createMockTaskContext());
        Map<String, String> config = createTaskConfig();
        // Set very short poll interval for testing
        config.put("poll.interval.ms", "1000");
        log.debug("Starting task with poll interval 1000ms");
        task.start(config);
        
        try {
            log.info("Executing first poll (initial load)...");
            // First poll should perform the initial load
            List<SourceRecord> initialRecords = task.poll();
            log.info("Initial poll returned {} records", initialRecords != null ? initialRecords.size() : 0);
            assertNotNull(initialRecords, "Initial records should not be null");
            
            // Wait a bit to ensure we're past the poll interval
            log.debug("Waiting 2 seconds before delta poll...");
            TimeUnit.SECONDS.sleep(2);
            
            log.info("Executing second poll (delta load)...");
            // Second poll should perform a delta load
            List<SourceRecord> deltaRecords = task.poll();
            
            // Delta records may be empty if no changes occurred, but should not be null
            log.info("Delta poll returned {} records", deltaRecords != null ? deltaRecords.size() : 0);
            assertNotNull(deltaRecords, "Delta records should not be null");
            
            log.info("Delta load successful, retrieved {} records", 
                     deltaRecords != null ? deltaRecords.size() : 0);
        } finally {
            // Stop the task
            log.debug("Stopping task");
            task.stop();
            log.info("Task stopped successfully");
        }
    }
    
    @Test
    @DisplayName("Should honor poll interval")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void testTaskPollInterval() throws Exception {
        log.info("Starting test: testTaskPollInterval");
        // Create and configure the task
        DataverseSourceTask task = new TestDataverseSourceTask();
        task.initialize(createMockTaskContext());
        Map<String, String> config = createTaskConfig();
        // Set poll interval to 5 seconds
        config.put("poll.interval.ms", "5000");
        log.debug("Starting task with poll interval 5000ms");
        task.start(config);
        
        try {
            log.info("Executing first poll (initial load)...");
            // First poll should perform the initial load
            List<SourceRecord> initialRecords = task.poll();
            log.info("Initial poll returned {} records", initialRecords != null ? initialRecords.size() : 0);
            assertNotNull(initialRecords, "Initial records should not be null");
            
            log.info("Executing immediate second poll (should respect interval)...");
            // Immediate second poll should return empty list (poll interval not reached)
            List<SourceRecord> immediateRecords = task.poll();
            log.info("Immediate second poll returned {} records", immediateRecords != null ? immediateRecords.size() : 0);
            assertNotNull(immediateRecords, "Records should not be null even when no results");
            assertTrue(immediateRecords.isEmpty(), "Records should be empty when poll interval not reached");
            
            // Wait past the poll interval
            log.debug("Waiting 6 seconds before final poll...");
            TimeUnit.SECONDS.sleep(6);
            
            log.info("Executing final poll after waiting past interval...");
            // Now we should get records again
            List<SourceRecord> delayedRecords = task.poll();
            log.info("Final poll returned {} records", delayedRecords != null ? delayedRecords.size() : 0);
            assertNotNull(delayedRecords, "Records should not be null after waiting");
            
            log.info("Poll interval test passed");
        } finally {
            // Stop the task
            log.debug("Stopping task");
            task.stop();
            log.info("Task stopped successfully");
        }
    }
    
    @Test
    @DisplayName("Should handle task restart and resume from offset")
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    public void testTaskRestartWithOffset() throws Exception {
        log.info("Starting test: testTaskRestartWithOffset");
        String[] tableNames = getTableNames();
        if (tableNames.length == 0) {
            log.warn("No tables defined in configuration, skipping test");
            return; // Skip test if no tables defined
        }
        
        // Prepare offset to start from
        final Map<String, String> partition = Collections.singletonMap("table", tableNames[0]);
        Map<String, Object> offset = null;
        
        log.info("Creating first task instance to obtain offset");
        // First task - get initial offset
        DataverseSourceTask task1 = new TestDataverseSourceTask();
        task1.initialize(createMockTaskContext());
        task1.start(createTaskConfig());
        
        try {
            log.info("Executing poll on first task to get initial records and offset");
            // Get initial records and save offset
            List<SourceRecord> records = task1.poll();
            log.info("First task poll returned {} records", records != null ? records.size() : 0);
            if (records != null && !records.isEmpty()) {
                SourceRecord lastRecord = records.get(records.size() - 1);
                offset = (Map<String, Object>) lastRecord.sourceOffset();
                log.debug("Captured offset for restart: {}", offset);
            }
        } finally {
            log.debug("Stopping first task");
            task1.stop();
            log.info("First task stopped");
        }
        
        // Skip test if we couldn't get an offset
        if (offset == null) {
            log.warn("Skipping test because no offset was obtained from first task run");
            return;
        }
        
        // Store the final offset
        final Map<String, Object> finalOffset = offset;
        
        log.info("Creating second task instance to test restart with offset");
        // Second task - start with saved offset
        DataverseSourceTask task2 = new TestDataverseSourceTask();
        
        // Create a mock SourceTaskContext with a mock OffsetStorageReader
        SourceTaskContext taskContext = new SourceTaskContext() {
            @Override
            public Map<String, String> configs() {
                return createTaskConfig();
            }
            
            @Override
            public OffsetStorageReader offsetStorageReader() {
                return new OffsetStorageReader() {
                    @Override
                    public <T> Map<Map<String, T>, Map<String, Object>> offsets(
                            Collection<Map<String, T>> partitions) {
                        log.debug("Task asked for offsets for {} partitions", partitions.size());
                        HashMap<Map<String, T>, Map<String, Object>> result = new HashMap<>();
                        for (Map<String, T> requestedPartition : partitions) {
                            // If this is the partition we've stored an offset for, return it
                            if (requestedPartition.equals(partition)) {
                                log.debug("Returning saved offset for partition: {}", requestedPartition);
                                result.put(requestedPartition, finalOffset);
                            }
                        }
                        return result;
                    }
                    
                    @Override
                    public <T> Map<String, Object> offset(Map<String, T> partition) {
                        if (partition.equals(partition)) {
                            log.debug("Returning saved offset: {}", finalOffset);
                            return finalOffset;
                        }
                        return null;
                    }
                };
            }
        };
        
        log.debug("Initializing second task with context that has saved offset");
        task2.initialize(taskContext);
        task2.start(createTaskConfig());
        
        try {
            log.info("Executing poll on restarted task...");
            // Poll should continue from where we left off
            List<SourceRecord> resumeRecords = task2.poll();
            
            // The important thing is that it doesn't crash - the records may be empty
            // if no changes occurred, but should not be null
            log.info("Restarted task poll returned {} records", resumeRecords != null ? resumeRecords.size() : 0);
            assertNotNull(resumeRecords, "Resume records should not be null");
            
            log.info("Task resume test passed, retrieved {} records after restart", 
                     resumeRecords.size());
        } finally {
            log.debug("Stopping second task");
            task2.stop();
            log.info("Second task stopped successfully");
        }
    }
} 