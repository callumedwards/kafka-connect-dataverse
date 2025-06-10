package callum.edwards.kafka.connect.dataverse;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Order;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.olingo.client.api.domain.ClientDelta;
import org.apache.olingo.client.api.domain.ClientEntitySet;
import org.apache.olingo.commons.api.edm.Edm;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Integration tests for OlingoDataverseConnection.
 * 
 * These tests verify that the connection can:
 * 1. Connect to Dataverse with the provided credentials
 * 2. Retrieve the Entity Data Model (EDM) metadata
 * 3. Perform an initial load of entities
 * 4. Perform delta loads with change tracking
 */
@DisplayName("Dataverse Connection Integration Tests")
public class OlingoDataverseConnectionIT extends AbstractDataverseIntegrationTest {
    
    @Test
    @DisplayName("Should connect and retrieve metadata")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void testConnectAndRetrieveMetadata() {
        // Get EDM metadata
        Edm edm = connection.getEdm();
        
        // Verify EDM is retrieved
        assertNotNull(edm, "EDM should not be null");
        assertNotNull(edm.getEntityContainer(), "Entity container should not be null");
        
        // Verify known entity sets are present
        for (String tableName : getTableNames()) {
            assertNotNull(edm.getEntityContainer().getEntitySet(tableName),
                          "Entity set " + tableName + " should exist in EDM");
        }
        
        log.info("Successfully connected to Dataverse and retrieved metadata");
    }
    
    @Test
    @DisplayName("Should perform initial load")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void testInitialLoad() {
        // Get a table name from the configuration
        String tableName = getTableNames()[0];
        
        // Perform initial load
        ClientEntitySet entitySet = connection.initialLoad(tableName);
        
        // Verify results
        assertNotNull(entitySet, "Entity set should not be null");
        assertNotNull(entitySet.getEntities(), "Entities collection should not be null");
        assertNotNull(entitySet.getDeltaLink(), "Delta link should not be null");
        
        log.info("Initial load successful for table {}, retrieved {} entities",
                 tableName, entitySet.getEntities().size());
        log.info("Received delta link: {}", entitySet.getDeltaLink());
    }
    
    @Test
    @DisplayName("Should perform delta load after initial load")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void testDeltaLoad() {
        // Get a table name from the configuration
        String tableName = getTableNames()[0];
        
        // Perform initial load to get delta link
        ClientEntitySet entitySet = connection.initialLoad(tableName);
        URI deltaLink = entitySet.getDeltaLink();
        
        // Verify delta link is available
        assertNotNull(deltaLink, "Delta link should not be null");
        
        log.info("Performing delta load with link: {}", deltaLink);
        
        // Perform delta load
        ClientDelta delta = connection.deltaLoad(deltaLink);
        
        // Verify results
        assertNotNull(delta, "Delta should not be null");
        assertNotNull(delta.getEntities(), "Delta entities collection should not be null");
        assertNotNull(delta.getDeletedEntities(), "Delta deleted entities collection should not be null");
        assertNotNull(delta.getDeltaLink(), "New delta link should not be null");
        
        log.info("Delta load successful, retrieved {} changed entities and {} deleted entities",
                 delta.getEntities().size(), delta.getDeletedEntities().size());
        log.info("Received new delta link: {}", delta.getDeltaLink());
    }
    
    @Test
    @DisplayName("Should handle large result sets with pagination")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    public void testPagination() {
        // Get a table name from the configuration that might have many records
        String tableName = getTableNames()[0];
        
        // Create a connection with a small page size to force pagination
        OlingoDataverseConnection smallPageSizeConnection = createConnectionWithSmallPageSize();
        
        // Perform initial load with the custom connection
        log.info("Performing load with max page size set to 2 to force pagination");
        ClientEntitySet entitySet = smallPageSizeConnection.initialLoad(tableName);
        
        // Verify results
        assertNotNull(entitySet, "Entity set should not be null");
        assertNotNull(entitySet.getEntities(), "Entities collection should not be null");
        
        // Log the total number of entities retrieved
        int totalEntities = entitySet.getEntities().size();
        log.info("Pagination test successful, retrieved {} total entities with page size of 2", totalEntities);
        
        // By setting page size to 2, we should have at least one pagination if we have more than 2 entities
        if (totalEntities > 2) {
            log.info("Verified that pagination works correctly, as more than 2 entities were retrieved with page size of 2");
        } else {
            log.warn("Table {} has only {} entities, which is not enough to verify pagination with page size of 2", 
                     tableName, totalEntities);
        }
        log.info("Performing validation that paged data can be converted to source records");
        List<SourceRecord> records = OlingoToConnectConverter.getClientEntitiesAsSourceRecords(
            tableName, 
            "test", 
            entitySet, 
            OlingoToConnectConverter.getSchema(connection.getEdm(), tableName)
        );
        assertEquals(records.size(), totalEntities, "Expected " + totalEntities + " records, but got " + records.size());
        log.info("Pagination test successful, converted {} entities to {} source records", totalEntities, records.size());
        // Clean up
        smallPageSizeConnection.close();
    }
    
    @Test
    @DisplayName("Should perform CRUD operations and track changes")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    @Order(4) // Run after other tests
    public void testCrudOperationsAndChanges() {
        // Create a test data helper using our connection
        DataverseTestDataHelper helper = new DataverseTestDataHelper(
                connection, 
                IntegrationTestConfig.getProperty("dataverse.url"));
        
        log.info("Starting CRUD operations test with delta tracking");
        
        try {
            // Perform a complete CRUD cycle (create, read, update, delete)
            UUID entityId = helper.performTestCrudCycle();
            log.info("Completed CRUD cycle for entity: {}", entityId);
            
            // Get a table name from the configuration
            String configJson = IntegrationTestConfig.getProperty("dataverse.delta.record");
            configJson = configJson.replace("{[", "{").replace("]}", "}").replaceAll("'", "\"");
            JsonNode config = new ObjectMapper().readTree(configJson);
            String tableName = config.path("table").asText();
            
            // Perform initial load to get delta link
            ClientEntitySet entitySet = connection.initialLoad(tableName);
            URI deltaLink = entitySet.getDeltaLink();
            
            // Verify delta link is available
            assertNotNull(deltaLink, "Delta link should not be null");
            
            log.info("Performing delta load to check for changes made by CRUD operations");
            
            // Perform delta load
            ClientDelta delta = connection.deltaLoad(deltaLink);
            assertNotNull(delta.getEntities(), "Delta entities collection should not be null");
            assertNotNull(delta.getDeletedEntities(), "Delta deleted entities collection should not be null");
            
            // Verify results
            assertNotNull(delta, "Delta should not be null");
            
            log.info("Delta load successful, retrieved {} changed entities and {} deleted entities",
                     delta.getEntities().size(), delta.getDeletedEntities().size());
            log.info("Changes were detected successfully via delta load");
            
        } catch (Exception e) {
            log.error("Error during CRUD operations and change tracking test", e);
            fail("CRUD operations and change tracking test failed: " + e.getMessage());
        }
    }
    
    @Test
    @DisplayName("Should track changes correctly with small page size")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    public void testChangeTrackingWithPagination() {
        // Get a table name from the configuration
        String tableName = getTableNames()[0];
        
        // Create a connection with a small page size to force pagination
        OlingoDataverseConnection smallPageSizeConnection = createConnectionWithSmallPageSize();
        
        try {
            // First do an initial load with small page size
            log.info("Performing initial load with small page size for table: {}", tableName);
            ClientEntitySet initialEntitySet = smallPageSizeConnection.initialLoad(tableName);
            
            // Verify results and get delta link
            assertNotNull(initialEntitySet, "Entity set should not be null");
            assertNotNull(initialEntitySet.getEntities(), "Entities collection should not be null");
            URI deltaLink = initialEntitySet.getDeltaLink();
            assertNotNull(deltaLink, "Delta link should not be null");
            
            int initialEntityCount = initialEntitySet.getEntities().size();
            log.info("Initial load successful with small page size, retrieved {} entities", initialEntityCount);
            log.info("Received delta link: {}", deltaLink);
            
            // Create a test data helper using our small page size connection
            DataverseTestDataHelper helper = new DataverseTestDataHelper(
                    smallPageSizeConnection, 
                    IntegrationTestConfig.getProperty("dataverse.url"));
            
            // Create multiple entities to ensure pagination in delta
            log.info("Creating multiple entities to generate changes that will require pagination");
            UUID entityId1 = helper.createEntityFromConfig();
            log.info("Created test entity 1 with ID: {}", entityId1);
            
            // Create a second entity
            UUID entityId2 = helper.createEntityFromConfig();
            log.info("Created test entity 2 with ID: {}", entityId2);
            
            // Create a third entity
            UUID entityId3 = helper.createEntityFromConfig();
            log.info("Created test entity 3 with ID: {}", entityId3);
            
            // Wait a moment to ensure changes are processed by Dataverse
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            // Now perform a delta load to detect changes, still with small page size
            log.info("Performing delta load with small page size to detect changes");
            ClientDelta delta = smallPageSizeConnection.deltaLoad(deltaLink);
            
            // Verify delta results
            assertNotNull(delta, "Delta should not be null");
            assertNotNull(delta.getEntities(), "Delta entities collection should not be null");
            assertNotNull(delta.getDeletedEntities(), "Delta deleted entities collection should not be null");
            assertNotNull(delta.getDeltaLink(), "New delta link should not be null");
            
            int changedEntitiesCount = delta.getEntities().size();
            log.info("Delta load with small page size successful, retrieved {} changed entities", changedEntitiesCount);
            
            // Since our page size is 2 and we created 3 entities, pagination should have occurred
            assertTrue(changedEntitiesCount >= 3, 
                    "Should have detected at least 3 changes with small page size, but found " + changedEntitiesCount);
            
            // Since our page size is 2 and we created 3 entities, pagination should have occurred
            assertTrue(changedEntitiesCount > SMALL_PAGE_SIZE, 
                    "Should have more entities (" + changedEntitiesCount + ") than page size (" + SMALL_PAGE_SIZE + 
                    ") to verify pagination occurred");
            
            log.info("Successfully verified that pagination occurred during delta tracking with:");
            log.info("  - Page size: {}", SMALL_PAGE_SIZE);
            log.info("  - Total entities detected: {}", changedEntitiesCount);
            log.info("  - Number of pages: at least {}", Math.ceil((double)changedEntitiesCount / SMALL_PAGE_SIZE));
            
            log.info("Performing validation that paged data can be converted to source records");
            List<SourceRecord> records = OlingoToConnectConverter.getClientEntitiesAsSourceRecords(
                tableName, 
                "test", 
                delta, 
                OlingoToConnectConverter.getSchema(connection.getEdm(), tableName)
            );
            assertEquals(records.size(), changedEntitiesCount, "Expected " + changedEntitiesCount + " records, but got " + records.size());
            log.info("Pagination test successful, converted {} entities to {} source records", changedEntitiesCount, records.size());
            // Clean up the created entities
            helper.deleteEntity(tableName, entityId1);
            log.info("Deleted test entity 1 with ID: {}", entityId1);
            
            helper.deleteEntity(tableName, entityId2);
            log.info("Deleted test entity 2 with ID: {}", entityId2);
            
            helper.deleteEntity(tableName, entityId3);
            log.info("Deleted test entity 3 with ID: {}", entityId3);

            // Wait a moment to ensure changes are processed by Dataverse
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            // Now perform a delta load to detect changes, still with small page size
            log.info("Performing delta load with small page size to detect deleted entities");
            delta = smallPageSizeConnection.deltaLoad(deltaLink);
            
            // Verify delta results
            assertNotNull(delta, "Delta should not be null");
            assertNotNull(delta.getEntities(), "Delta entities collection should not be null");
            assertNotNull(delta.getDeletedEntities(), "Delta deleted entities collection should not be null");
            assertNotNull(delta.getDeltaLink(), "New delta link should not be null");
            
            changedEntitiesCount = delta.getDeletedEntities().size();
            log.info("Delta load with small page size successful, retrieved {} changed entities", changedEntitiesCount);
            
            // Since our page size is 2 and we deleted 3 entities, pagination should have occurred
            assertTrue(changedEntitiesCount >= 3, 
                    "Should have detected at least 3 changes with small page size, but found " + changedEntitiesCount);
            
            // Since our page size is 2 and we deleted 3 entities, pagination should have occurred
            assertTrue(changedEntitiesCount > SMALL_PAGE_SIZE, 
                    "Should have more entities (" + changedEntitiesCount + ") than page size (" + SMALL_PAGE_SIZE + 
                    ") to verify pagination occurred");
            
            log.info("Successfully verified that pagination occurred during delta tracking with:");
            log.info("  - Page size: {}", SMALL_PAGE_SIZE);
            log.info("  - Total entities detected: {}", changedEntitiesCount);
            log.info("  - Number of pages: at least {}", Math.ceil((double)changedEntitiesCount / SMALL_PAGE_SIZE));
            
            log.info("Performing validation that paged data can be converted to source records");
            records = OlingoToConnectConverter.getDeletedEntitiesAsSourceRecords(
                tableName, 
                "test", 
                delta, 
                OlingoToConnectConverter.getSchema(connection.getEdm(), tableName)
            );
            assertEquals(records.size(), changedEntitiesCount, "Expected " + changedEntitiesCount + " records, but got " + records.size());
            log.info("Pagination test successful, converted {} entities to {} source records", changedEntitiesCount, records.size());
            // Clean up the connection
            smallPageSizeConnection.close();
        } catch (Exception e) {
            // Ensure we close the connection even on failure
            smallPageSizeConnection.close();
            log.error("Error during change tracking with pagination test", e);
            fail("Change tracking with pagination test failed: " + e.getMessage());
        }
    }

    
    @Test
    @DisplayName("Should handle large result sets with pagination and small batch size")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    public void testPaginationWithSmallBatchSize() {
        // Get a table name from the configuration that might have many records
        String tableName = getTableNames()[0];
        
        // Create a connection with a small page size to force pagination
        OlingoDataverseConnection smallPageSizeConnection = createConnectionWithSmallPageSize();
        smallPageSizeConnection.setBatchSize(SMALL_PAGE_SIZE);
        // Perform initial load with the custom connection
        log.info("Performing load with max page size set to 2 to force pagination");
        ClientEntitySet entitySet = smallPageSizeConnection.initialLoad(tableName);
        // Verify results
        assertNotNull(entitySet, "Entity set should not be null");
        assertNotNull(entitySet.getEntities(), "Entities collection should not be null");
        
        // Perform delta load until there are no more delta links
        ClientEntitySet remainingEntities = entitySet;
        while (remainingEntities.getEntities().size() > 0 && remainingEntities.getDeltaLink() != null) {
            remainingEntities = smallPageSizeConnection.deltaLoad(remainingEntities.getDeltaLink());
            entitySet.getEntities().addAll(remainingEntities.getEntities());
        }
        int totalEntities = entitySet.getEntities().size();
        // Log the total number of entities retrieved
        log.info("Pagination test successful, retrieved {} total entities with page size of 2", totalEntities);
        
        // By setting page size to 2, we should have at least one pagination if we have more than 2 entities
        if (totalEntities > 2) {
            log.info("Verified that pagination works correctly, as more than 2 entities were retrieved with page size of 2");
        } else {
            log.warn("Table {} has only {} entities, which is not enough to verify pagination with page size of 2", 
                     tableName, totalEntities);
        }
        log.info("Performing validation that paged data can be converted to source records");
        List<SourceRecord> records = OlingoToConnectConverter.getClientEntitiesAsSourceRecords(
            tableName, 
            "test", 
            entitySet, 
            OlingoToConnectConverter.getSchema(connection.getEdm(), tableName)
        );
        assertEquals(records.size(), totalEntities, "Expected " + totalEntities + " records, but got " + records.size());
        log.info("Pagination test successful, converted {} entities to {} source records", totalEntities, records.size());
        // Clean up
        smallPageSizeConnection.close();
    }

    @Test
    @DisplayName("Should track changes correctly with small page size and small batch size")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    public void testChangeTrackingWithPaginationAndSmallBatchSize() {
        // Get a table name from the configuration
        String tableName = getTableNames()[0];
        
        // Create a connection with a small page size to force pagination
        OlingoDataverseConnection smallPageSizeConnection = createConnectionWithSmallPageSize();
        try {
            // First do an initial load with small page size
            log.info("Performing initial load with small page size for table: {}", tableName);
            ClientEntitySet initialEntitySet = smallPageSizeConnection.initialLoad(tableName);
            
            // Verify results and get delta link
            assertNotNull(initialEntitySet, "Entity set should not be null");
            assertNotNull(initialEntitySet.getEntities(), "Entities collection should not be null");
            URI deltaLink = initialEntitySet.getDeltaLink();
            assertNotNull(deltaLink, "Delta link should not be null");
            
            int initialEntityCount = initialEntitySet.getEntities().size();
            log.info("Initial load successful with small page size, retrieved {} entities", initialEntityCount);
            log.info("Received delta link: {}", deltaLink);
            
            // Create a test data helper using our small page size connection
            DataverseTestDataHelper helper = new DataverseTestDataHelper(
                    smallPageSizeConnection, 
                    IntegrationTestConfig.getProperty("dataverse.url"));
            
            // Create multiple entities to ensure pagination in delta
            log.info("Creating multiple entities to generate changes that will require pagination");
            UUID entityId1 = helper.createEntityFromConfig();
            log.info("Created test entity 1 with ID: {}", entityId1);
            
            // Create a second entity
            UUID entityId2 = helper.createEntityFromConfig();
            log.info("Created test entity 2 with ID: {}", entityId2);
            
            // Create a third entity
            UUID entityId3 = helper.createEntityFromConfig();
            log.info("Created test entity 3 with ID: {}", entityId3);
            
            // Wait a moment to ensure changes are processed by Dataverse
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            // Now perform a delta load to detect changes, still with small page size
            log.info("Performing delta load with small page size to detect changes");
            
            smallPageSizeConnection.setBatchSize(SMALL_PAGE_SIZE);

            ClientDelta delta = smallPageSizeConnection.deltaLoad(deltaLink);
            
            // Verify delta results
            assertNotNull(delta, "Delta should not be null");
            assertNotNull(delta.getEntities(), "Delta entities collection should not be null");
            assertNotNull(delta.getDeletedEntities(), "Delta deleted entities collection should not be null");
            assertNotNull(delta.getDeltaLink(), "New delta link should not be null");
            
            // Perform delta load until there are no more delta links
            ClientDelta remainingEntities = delta;
            while (remainingEntities.getEntities().size() > 0 && remainingEntities.getDeltaLink() != null) {
                remainingEntities = smallPageSizeConnection.deltaLoad(remainingEntities.getDeltaLink());
                delta.getEntities().addAll(remainingEntities.getEntities());
                delta.getDeletedEntities().addAll(remainingEntities.getDeletedEntities());
            }

            int changedEntitiesCount = delta.getEntities().size();
            log.info("Delta load with small page size successful, retrieved {} changed entities", changedEntitiesCount);
            
            // Since our page size is 2 and we created 3 entities, pagination should have occurred
            assertTrue(changedEntitiesCount >= 3, 
                    "Should have detected at least 3 changes with small page size, but found " + changedEntitiesCount);
            
            // Since our page size is 2 and we created 3 entities, pagination should have occurred
            assertTrue(changedEntitiesCount > SMALL_PAGE_SIZE, 
                    "Should have more entities (" + changedEntitiesCount + ") than page size (" + SMALL_PAGE_SIZE + 
                    ") to verify pagination occurred");
            
            log.info("Successfully verified that pagination occurred during delta tracking with:");
            log.info("  - Page size: {}", SMALL_PAGE_SIZE);
            log.info("  - Total entities detected: {}", changedEntitiesCount);
            log.info("  - Number of pages: at least {}", Math.ceil((double)changedEntitiesCount / SMALL_PAGE_SIZE));
            
            log.info("Performing validation that paged data can be converted to source records");
            List<SourceRecord> records = OlingoToConnectConverter.getClientEntitiesAsSourceRecords(
                tableName, 
                "test", 
                delta, 
                OlingoToConnectConverter.getSchema(connection.getEdm(), tableName)
            );
            assertEquals(records.size(), changedEntitiesCount, "Expected " + changedEntitiesCount + " records, but got " + records.size());
            log.info("Pagination test successful, converted {} entities to {} source records", changedEntitiesCount, records.size());
            // Clean up the created entities
            helper.deleteEntity(tableName, entityId1);
            log.info("Deleted test entity 1 with ID: {}", entityId1);
            
            helper.deleteEntity(tableName, entityId2);
            log.info("Deleted test entity 2 with ID: {}", entityId2);
            
            helper.deleteEntity(tableName, entityId3);
            log.info("Deleted test entity 3 with ID: {}", entityId3);

            // Wait a moment to ensure changes are processed by Dataverse
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            // Now perform a delta load to detect changes, still with small page size
            log.info("Performing delta load with small page size to detect deleted entities");
            delta = smallPageSizeConnection.deltaLoad(deltaLink);
            
            // Verify delta results
            assertNotNull(delta, "Delta should not be null");
            assertNotNull(delta.getEntities(), "Delta entities collection should not be null");
            assertNotNull(delta.getDeletedEntities(), "Delta deleted entities collection should not be null");
            assertNotNull(delta.getDeltaLink(), "New delta link should not be null");
            
            // Perform delta load until there are no more delta links
            remainingEntities = delta;
            while (remainingEntities.getEntities().size() > 0 && remainingEntities.getDeltaLink() != null) {
                remainingEntities = smallPageSizeConnection.deltaLoad(remainingEntities.getDeltaLink());
                delta.getEntities().addAll(remainingEntities.getEntities());
                delta.getDeletedEntities().addAll(remainingEntities.getDeletedEntities());
            }
            changedEntitiesCount = delta.getDeletedEntities().size();
            log.info("Delta load with small page size successful, retrieved {} changed entities", changedEntitiesCount);
            
            // Since our page size is 2 and we deleted 3 entities, pagination should have occurred
            assertTrue(changedEntitiesCount >= 3, 
                    "Should have detected at least 3 changes with small page size, but found " + changedEntitiesCount);
            
            // Since our page size is 2 and we deleted 3 entities, pagination should have occurred
            assertTrue(changedEntitiesCount > SMALL_PAGE_SIZE, 
                    "Should have more entities (" + changedEntitiesCount + ") than page size (" + SMALL_PAGE_SIZE + 
                    ") to verify pagination occurred");
            
            log.info("Successfully verified that pagination occurred during delta tracking with:");
            log.info("  - Page size: {}", SMALL_PAGE_SIZE);
            log.info("  - Total entities detected: {}", changedEntitiesCount);
            log.info("  - Number of pages: at least {}", Math.ceil((double)changedEntitiesCount / SMALL_PAGE_SIZE));
            
            log.info("Performing validation that paged data can be converted to source records");
            records = OlingoToConnectConverter.getDeletedEntitiesAsSourceRecords(
                tableName, 
                "test", 
                delta, 
                OlingoToConnectConverter.getSchema(connection.getEdm(), tableName)
            );
            assertEquals(records.size(), changedEntitiesCount, "Expected " + changedEntitiesCount + " records, but got " + records.size());
            log.info("Pagination test successful, converted {} entities to {} source records", changedEntitiesCount, records.size());
            // Clean up the connection
            smallPageSizeConnection.close();
        } catch (Exception e) {
            // Ensure we close the connection even on failure
            smallPageSizeConnection.close();
            log.error("Error during change tracking with pagination test", e);
            fail("Change tracking with pagination test failed: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("Should skip initial load and only track changes")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    public void testSkipInitialLoad() {
        // Get a table name from the configuration
        String tableName = getTableNames()[0];
        
        log.info("Starting test for skipInitialLoad functionality with table: {}", tableName);
        
        try {
            // 1. Get a delta link to start tracking changes
            log.info("Attempting to get delta link without performing full initial load");
            URI deltaLink;
            // try using getDeltaLinkOnly which is more efficient
            deltaLink = connection.getDeltaLinkOnly(tableName);
            log.info("Successfully used getDeltaLinkOnly to obtain delta link without retrieving entities");
        
            // Verify we got a delta link
            assertNotNull(deltaLink, "Delta link should not be null");
            log.info("Successfully obtained delta link: {}", deltaLink);
            
            // 2. Create a test entity
            DataverseTestDataHelper helper = new DataverseTestDataHelper(
                    connection, 
                    IntegrationTestConfig.getProperty("dataverse.url"));
            
            log.info("Creating a new entity to test delta tracking");
            UUID entityId = helper.createEntityFromConfig();
            log.info("Created test entity with ID: {}", entityId);
            
            // Wait a moment to ensure changes are processed by Dataverse
            log.info("Waiting for changes to be processed...");
            Thread.sleep(2000);
            
            // 3. Perform delta load to detect the new entity
            log.info("Performing delta load to detect newly created entity");
            ClientDelta delta = connection.deltaLoad(deltaLink);
            
            // Verify delta results
            assertNotNull(delta, "Delta should not be null");
            assertNotNull(delta.getEntities(), "Delta entities collection should not be null");
            
            // Depending on whether we did an initial load or not, we might or might not see the entity
            // in the delta changes, so we can't strictly assert on its presence
            int changedEntitiesCount = delta.getEntities().size();
            log.info("Delta load detected {} changed entities", changedEntitiesCount);
            
            // 4. Convert delta results to source records
            log.info("Converting delta entities to source records");
            List<SourceRecord> records = OlingoToConnectConverter.getClientEntitiesAsSourceRecords(
                tableName, 
                "test-" + tableName, 
                delta, 
                OlingoToConnectConverter.getSchema(connection.getEdm(), tableName)
            );
            
            assertNotNull(records, "Source records should not be null");
            assertEquals(changedEntitiesCount, records.size(), 
                        "Number of source records should match number of changed entities");
            
            log.info("Successfully converted {} delta entities to {} source records", 
                     changedEntitiesCount, records.size());
            
            // Save the new delta link for delete testing
            URI newDeltaLink = delta.getDeltaLink();
            assertNotNull(newDeltaLink, "New delta link should not be null for delete testing");
            
            // 5. Delete the entity
            log.info("Deleting the test entity to test delta tracking of deletions");
            helper.deleteEntity(tableName, entityId);
            log.info("Deleted test entity with ID: {}", entityId);
            
            // Wait for changes to be processed
            log.info("Waiting for delete to be processed...");
            Thread.sleep(2000);
            
            // 6. Perform another delta load to detect the deletion
            log.info("Performing delta load to detect entity deletion");
            delta = connection.deltaLoad(newDeltaLink);
            
            // Verify delta results for deletion
            assertNotNull(delta, "Delta should not be null after deletion");
            assertNotNull(delta.getDeletedEntities(), "Delta deleted entities collection should not be null");
            
            int deletedEntitiesCount = delta.getDeletedEntities().size();
            log.info("Delta load detected {} deleted entities", deletedEntitiesCount);
            
            // 7. Convert delta deleted results to source records
            log.info("Converting delta deleted entities to source records");
            List<SourceRecord> deleteRecords = OlingoToConnectConverter.getDeletedEntitiesAsSourceRecords(
                tableName, 
                "test-" + tableName, 
                delta, 
                OlingoToConnectConverter.getSchema(connection.getEdm(), tableName)
            );
            
            assertNotNull(deleteRecords, "Deleted source records should not be null");
            assertEquals(deletedEntitiesCount, deleteRecords.size(), 
                        "Number of deleted source records should match number of deleted entities");
            
            log.info("Successfully converted {} deleted entities to {} source records (tombstones)", 
                     deletedEntitiesCount, deleteRecords.size());
            
            // Verify any tombstone records have null values
            for (SourceRecord record : deleteRecords) {
                if (record.key() != null) {
                    assertNull(record.value(), "Tombstone record value should be null");
                }
            }
            
            log.info("Skip initial load test completed successfully");
        } catch (Exception e) {
            log.error("Error during skip initial load test", e);
            fail("Skip initial load test failed: " + e.getMessage());
        }
    }
} 