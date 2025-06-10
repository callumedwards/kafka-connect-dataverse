/*
 * Copyright 2024 Callum Edwards
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package callum.edwards.kafka.connect.dataverse;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.olingo.client.api.domain.ClientEntitySet;
import org.apache.olingo.client.api.domain.ClientDelta;
import java.net.URI;
import java.net.URISyntaxException;

import java.util.*;

/**
 * DataverseSourceTask is a Kafka Connect source task that retrieves data from Microsoft Dataverse
 * and publishes it to Kafka topics. This implementation uses the OData protocol via Apache Olingo
 * to communicate with Dataverse.
 * 
 * The task supports both initial data loads and delta (incremental) loads using Dataverse's
 * change tracking functionality. Each table specified in the connector configuration will be
 * tracked separately with its own delta link.
 * 
 * The task uses {@link OlingoDataverseConnection} to communicate with Dataverse and 
 * {@link OlingoToConnectConverter} to convert OData entities to Kafka Connect records.
 */
public class DataverseSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(DataverseSourceTask.class);
    
    private DataverseSourceConnectorConfig config;
    private List<String> tables;
    private int pollIntervalMs;
    private long lastPollTime = 0;
    
    // Stores the offset (delta link) for each table
    private Map<String, Map<String, Object>> tableOffsets;
    
    // Key used in offset maps to store the delta link
    private static final String DELTA_LINK_KEY = "deltaLink";
    
    private OlingoDataverseConnection connection;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    /**
     * Initializes the task with configuration properties, establishes the Dataverse connection,
     * and sets up offset tracking for the tables that this task is responsible for.
     * 
     * @param props Configuration properties for the task
     */
    @Override
    public void start(Map<String, String> props) {
        config = new DataverseSourceConnectorConfig(props);
        pollIntervalMs = config.getPollIntervalMs();
        
        // Determine which tables this task should handle
        if (props.containsKey("table")) {
            // Single table mode
            tables = Collections.singletonList(props.get("table"));
        } else if (props.containsKey("task.tables")) {
            // Multiple tables mode - connector has distributed tables among tasks
            tables = Arrays.asList(props.get("task.tables").split(","));
        } else {
            // Default to all configured tables
            tables = config.getTables();
        }
        
        log.info("Starting DataverseSourceTask with tables: {}", tables);
        
        // Initialize the Olingo Dataverse connection
        connection = createConnection(config);
        
        // Initialize offsets for each table from the offset storage
        tableOffsets = new HashMap<>();
        for (String table : tables) {
            Map<String, String> partition = Collections.singletonMap("table", table);
            Map<String, Object> offset = context.offsetStorageReader().offset(partition);
            
            if (offset == null) {
                // No stored offset means this is the first time we're processing this table
                offset = new HashMap<>();
                offset.put(DELTA_LINK_KEY, null); // No delta link initially
            }
            
            tableOffsets.put(table, offset);
        }
    }

    /**
     * Polls Dataverse for new or changed data from each table and converts it to Kafka Connect records.
     * This method respects the configured poll interval and handles both initial loads and delta loads
     * based on whether a delta link exists for each table.
     * 
     * @return A list of SourceRecords to be published to Kafka, or null if no new data is available
     * @throws InterruptedException If the task is interrupted while waiting between polls
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        long now = System.currentTimeMillis();
        long timeSinceLastPoll = now - lastPollTime;
        
        // Respect the poll interval to avoid excessive requests to Dataverse
        if (timeSinceLastPoll < pollIntervalMs) {
            sleep(pollIntervalMs - timeSinceLastPoll);
            return null;
        }
        
        lastPollTime = now;
        List<SourceRecord> allRecords = new ArrayList<>();
        
        // Poll each table
        for (String table : tables) {
            try {
                String topic = config.getTopicForTable(table);
                Map<String, Object> offset = tableOffsets.get(table);
                String deltaLinkStr = (String) offset.get(DELTA_LINK_KEY);
                
                List<SourceRecord> tableRecords;
                
                if (deltaLinkStr == null) {
                    // Initial load - no delta link yet
                    if (config.skipInitialLoad()) {
                        log.info("Skipping initial load of table {} and getting delta link only", table);
                        
                        // Get only the delta link without retrieving all entities
                        URI deltaLink = connection.getDeltaLinkOnly(table);
                        
                        if (deltaLink == null) {
                            log.warn("Failed to get delta link for table {} when skipping initial load. " +
                                     "Change tracking may not be enabled for this table.", table);
                            // Fall back to initial load
                            log.info("Falling back to initial load for table: {}", table);
                            tableRecords = getInitialLoadAsSourceRecords(table, topic);
                        } else {
                            log.info("Successfully obtained delta link for table {} without initial load", table);
                            
                            // Update the offset with the new delta link
                            offset.put(DELTA_LINK_KEY, deltaLink.toString());
                            tableOffsets.put(table, offset);
                            
                            // No records to process yet since we're only tracking changes going forward
                            tableRecords = Collections.emptyList();
                        }
                    } else {
                        log.info("Performing initial load for table: {}", table);
                        tableRecords = getInitialLoadAsSourceRecords(table, topic);
                    }
                } else {
                    // Delta load - use existing delta link
                    log.info("Performing delta load for table: {}", table);
                    tableRecords = getDeltaAsSourceRecords(table, topic, deltaLinkStr);
                }
                
                if (tableRecords != null && !tableRecords.isEmpty()) {
                    allRecords.addAll(tableRecords);
                    
                    // Update the offset with the latest delta link for this table
                    // The delta link is extracted from the last record's source offset
                    Map<String, ?> sourceOffset = tableRecords.get(tableRecords.size()-1).sourceOffset();
                    if (sourceOffset != null && sourceOffset.containsKey(DELTA_LINK_KEY)) {
                        String newDeltaLink = (String) sourceOffset.get(DELTA_LINK_KEY);
                        offset.put(DELTA_LINK_KEY, newDeltaLink);
                        tableOffsets.put(table, offset);
                    }
                }
            } catch (Exception e) {
                log.error("Error polling table {}", table, e);
                // Continue with other tables even if one fails
            }
        }
        
        return allRecords.isEmpty() ? null : allRecords;
    }

    /**
     * Stops the task by closing the Dataverse connection.
     */
    @Override
    public void stop() {
        // Close the connection
        if (connection != null) {
            try {
                connection.close();
                log.info("DataverseSourceTask stopped and connection closed");
            } catch (Exception e) {
                log.error("Error closing Olingo Dataverse connection", e);
            }
        }
    }

    /**
     * Performs an initial load of data from a Dataverse table and converts it to Kafka Connect records.
     * This method is called when no delta link is available for a table and skipInitialLoad is false.
     * 
     * @param table The Dataverse table name
     * @param topic The Kafka topic to publish records to
     * @return A list of SourceRecords containing the table data
     */
    protected List<SourceRecord> getInitialLoadAsSourceRecords(String table, String topic) {
        log.info("Performing initial load of table: {}", table);
        
        // Retrieve the entity set from Dataverse using OlingoDataverseConnection
        ClientEntitySet entitySet = connection.initialLoad(table);
        
        // Convert the entity set to Kafka Connect records
        // The converter handles creating source partitions and offsets with the delta link
        return OlingoToConnectConverter.getClientEntitiesAsSourceRecords(
            table, 
            topic, 
            entitySet, 
            OlingoToConnectConverter.getSchema(connection.getEdm(), table)
        );
    }
    
    /**
     * Performs a delta (incremental) load of data from a Dataverse table using the provided delta link.
     * This method is called when a delta link is available from a previous load.
     * The delta load includes both changed entities and deleted entities.
     * 
     * @param table The Dataverse table name
     * @param topic The Kafka topic to publish records to
     * @param deltaLinkStr The delta link URI as a string
     * @return A list of SourceRecords containing the changed and deleted entities
     * @throws URISyntaxException If the delta link string is not a valid URI
     */
    protected List<SourceRecord> getDeltaAsSourceRecords(String table, String topic, String deltaLinkStr) {
        log.info("Delta load for table: {} with link: {}", table, deltaLinkStr);
        try {
            // Convert the delta link string to a URI
            URI deltaLink = new URI(deltaLinkStr);
            
            // Retrieve changed and deleted entities using the delta link
            ClientDelta entitySet = connection.deltaLoad(deltaLink);
            
            // Get the schema for this table
            Schema schema = OlingoToConnectConverter.getSchema(connection.getEdm(), table);
            
            // Convert changed entities to Kafka Connect records
            List<SourceRecord> records = OlingoToConnectConverter.getClientEntitiesAsSourceRecords(
                table, 
                topic, 
                entitySet, 
                schema
            );
            
            // Add deleted entity records (tombstone records)
            records.addAll(OlingoToConnectConverter.getDeletedEntitiesAsSourceRecords(
                table, 
                topic, 
                entitySet, 
                schema
            ));
            
            return records;
        } catch (URISyntaxException e) {
            log.error("Invalid delta link URI: {}", deltaLinkStr, e);
            throw new RuntimeException("Invalid delta link URI", e);
        }
    }
    
    /**
     * Creates and initializes the Dataverse connection with the provided configuration.
     * 
     * @param config The connector configuration
     * @return An initialized OlingoDataverseConnection
     */
    protected OlingoDataverseConnection createConnection(DataverseSourceConnectorConfig config) {
        log.info("Creating Olingo Dataverse connection to {}", config.getDataverseUrl());
        OlingoDataverseConnection connection = new OlingoDataverseConnection(
            config.getAccessTokenUrl(),
            config.getClientId(),
            config.getClientSecret(),
            config.getDataverseUrl(),
            config.getDataverseServiceURL()
        );
        
        // Configure maxPageSize if needed
        int configuredMaxPageSize = config.getMaxPageSize();
        int defaultMaxPageSize = connection.getMaxPageSize();
        if (configuredMaxPageSize != defaultMaxPageSize) {
            log.info("Setting custom maxPageSize from {} to {}", defaultMaxPageSize, configuredMaxPageSize);
            connection.setMaxPageSize(configuredMaxPageSize);
        }
        
        // Configure batchSize if needed
        int configuredBatchSize = config.getBatchSize();
        int defaultBatchSize = connection.getBatchSize();
        if (configuredBatchSize != defaultBatchSize) {
            log.info("Setting custom batchSize from {} to {}", defaultBatchSize, configuredBatchSize);
            connection.setBatchSize(configuredBatchSize);
        }
        
        return connection;
    }
    
    /**
     * Returns the list of tables that this task is handling.
     * This method is protected to allow access in tests.
     * 
     * @return The list of tables
     */
    protected List<String> getTables() {
        return tables;
    }
    
    /**
     * Gets the offset for a specific table.
     * This method is protected to allow access in tests.
     * 
     * @param table The table to get the offset for
     * @return The offset map for the table
     */
    protected Map<String, Object> getOffsetForTable(String table) {
        return tableOffsets.get(table);
    }
    
    /**
     * Sets the last poll time.
     * This method is protected to allow manipulation in tests.
     * 
     * @param lastPollTime The last poll time in milliseconds
     */
    protected void setLastPollTime(long lastPollTime) {
        this.lastPollTime = lastPollTime;
    }
    
    /**
     * Sleep for the specified number of milliseconds.
     * This method is protected to allow overriding in tests.
     * 
     * @param ms The number of milliseconds to sleep
     * @throws InterruptedException If the thread is interrupted
     */
    protected void sleep(long ms) throws InterruptedException {
        Thread.sleep(ms);
    }
}
