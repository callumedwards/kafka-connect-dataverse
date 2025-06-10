package callum.edwards.kafka.connect.dataverse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.Uuid;
import org.apache.olingo.client.api.ODataClient;
import org.apache.olingo.client.api.communication.request.cud.ODataDeleteRequest;
import org.apache.olingo.client.api.communication.request.cud.ODataEntityCreateRequest;
import org.apache.olingo.client.api.communication.request.cud.ODataEntityUpdateRequest;
import org.apache.olingo.client.api.communication.request.cud.UpdateType;
import org.apache.olingo.client.api.communication.request.retrieve.ODataEntityRequest;
import org.apache.olingo.client.api.communication.request.retrieve.ODataEntitySetRequest;
import org.apache.olingo.client.api.communication.response.ODataDeleteResponse;
import org.apache.olingo.client.api.communication.response.ODataEntityCreateResponse;
import org.apache.olingo.client.api.communication.response.ODataEntityUpdateResponse;
import org.apache.olingo.client.api.communication.response.ODataRetrieveResponse;
import org.apache.olingo.client.api.domain.ClientEntity;
import org.apache.olingo.client.api.domain.ClientEntitySet;
import org.apache.olingo.client.api.domain.ClientObjectFactory;
import org.apache.olingo.client.api.domain.ClientProperty;
import org.apache.olingo.client.api.uri.URIBuilder;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

/**
 * Helper class for integration tests to create, update, and delete test records in Dataverse.
 * This class uses the Olingo OData client directly to perform CRUD operations, without
 * modifying the actual implementation code.
 */
public class DataverseTestDataHelper {
    private static final Logger log = LoggerFactory.getLogger(DataverseTestDataHelper.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    
    private final OlingoDataverseConnection connection;
    private final String dataverseServiceUrl;
    private JsonNode configJson; // Cached config JSON
    private String tableName; // Cached table name
    private String keyPropertyName; // Cached key property name

    /**
     * Creates a new helper instance with the specified connection.
     *
     * @param connection The Dataverse connection to use
     * @param dataverseUrl The Dataverse URL
     */
    public DataverseTestDataHelper(OlingoDataverseConnection connection, String dataverseUrl) {
        this.connection = connection;
        this.dataverseServiceUrl = dataverseUrl + "/api/data/v9.2/";
        log.info("Created DataverseTestDataHelper with service URL: {}", dataverseServiceUrl);
        
        // Parse the configuration once during initialization
        parseConfigJson();
    }
    
    /**
     * Parses the dataverse.delta.record property once and caches the results.
     * This avoids repeated parsing of the same JSON in multiple methods.
     */
    private void parseConfigJson() {
        try {
            String configStr = IntegrationTestConfig.getProperty("dataverse.delta.record");
            if (configStr == null || configStr.isEmpty()) {
                log.warn("dataverse.delta.record property is missing or empty");
                return;
            }
            
            // Parse the configuration JSON, handling potential format issues
            configStr = configStr.replace("{[", "{").replace("]}", "}").replaceAll("'", "\"");
            
            this.configJson = mapper.readTree(configStr);
            
            // Extract and cache the table name
            this.tableName = configJson.path("table").asText();
            if (tableName == null || tableName.isEmpty()) {
                log.warn("Table name is missing in dataverse.delta.record");
            } else {
                log.info("Using table name from config: {}", tableName);
            }
            
            // Extract and cache the key property name
            JsonNode keyNode = configJson.path("key");
            if (!keyNode.isMissingNode()) {
                this.keyPropertyName = keyNode.asText();
                log.debug("Found key property '{}' in dataverse.delta.record", keyPropertyName);
            } else {
                // Default key property name based on table name
                this.keyPropertyName = tableName != null ? tableName + "id" : null;
                log.debug("Using default key property name: {}", keyPropertyName);
            }
            
        } catch (Exception e) {
            log.error("Failed to parse dataverse.delta.record configuration", e);
        }
    }
    
    /**
     * Gets the table name from the cached configuration.
     * 
     * @return The table name
     * @throws IllegalArgumentException if the table name is not available
     */
    public String getTableName() {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("Table name is not available in configuration");
        }
        return tableName;
    }
    
    /**
     * Gets the key property name from the cached configuration.
     * 
     * @return The key property name
     * @throws IllegalArgumentException if the key property name is not available
     */
    public String getKeyPropertyName() {
        if (keyPropertyName == null || keyPropertyName.isEmpty()) {
            throw new IllegalArgumentException("Key property name is not available in configuration");
        }
        return keyPropertyName;
    }
    
    /**
     * Gets the record JSON from the cached configuration.
     * 
     * @return The record JSON node
     * @throws IllegalArgumentException if the record JSON is not available
     */
    public JsonNode getRecordJson() {
        if (configJson == null) {
            throw new IllegalArgumentException("Configuration JSON is not available");
        }
        
        JsonNode recordNode = configJson.path("record");
        if (!recordNode.isObject()) {
            throw new IllegalArgumentException("Record definition is missing or invalid in dataverse.delta.record");
        }
        
        return recordNode;
    }

    /**
     * Creates a new entity in Dataverse using the configuration defined in dataverse.delta.record.
     *
     * @return The ID of the created entity
     * @throws RuntimeException if the creation fails
     */
    public UUID createEntityFromConfig() {
        try {
            // Get the record definition from the cached configuration
            JsonNode recordNode = getRecordJson();
            String tableName = getTableName();
            
            // Create an entity with the specified properties
            ClientEntity entity = createEntityFromJson(recordNode, tableName);
            
            // Insert the entity
            return createEntity(tableName, entity);
        } catch (Exception e) {
            log.error("Failed to create entity from configuration", e);
            throw new RuntimeException("Failed to create entity from configuration", e);
        }
    }
    
    /**
     * Creates a ClientEntity object from a JSON node.
     *
     * @param jsonNode The JSON node representing the entity properties
     * @param tableName The name of the table
     * @return A ClientEntity object
     */
    private ClientEntity createEntityFromJson(JsonNode jsonNode, String tableName) {
        ODataClient client = connection.getClient();
        ClientObjectFactory factory = client.getObjectFactory();
        
        log.info("Creating entity for table: {}", tableName);
        
        // Create a new entity without explicitly setting the type name
        // This avoids the error: "A type named 'Microsoft.Dynamics.CRM.accounts' could not be resolved by the model"
        ClientEntity entity = factory.newEntity(null);
        
        // Add all properties from the JSON
        Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String propertyName = field.getKey();
            JsonNode value = field.getValue();
            
            log.debug("Adding property: {} with value: {}", propertyName, value);
            
            addPropertyToEntity(entity, propertyName, value, factory);
        }
        
        return entity;
    }
    
    /**
     * Adds a property to an entity based on the JSON value type.
     *
     * @param entity The entity to add the property to
     * @param propertyName The name of the property
     * @param value The value of the property as a JsonNode
     * @param factory The ClientObjectFactory to create properties with
     */
    private void addPropertyToEntity(ClientEntity entity, String propertyName, JsonNode value, ClientObjectFactory factory) {
        if (value.isTextual()) {
            entity.getProperties().add(factory.newPrimitiveProperty(propertyName, 
                factory.newPrimitiveValueBuilder().buildString(value.asText())));
        } else if (value.isNumber()) {
            if (value.isInt()) {
                entity.getProperties().add(factory.newPrimitiveProperty(propertyName, 
                    factory.newPrimitiveValueBuilder().buildInt32(value.asInt())));
            } else if (value.isDouble()) {
                entity.getProperties().add(factory.newPrimitiveProperty(propertyName, 
                    factory.newPrimitiveValueBuilder().buildDouble(value.asDouble())));
            }
        } else if (value.isBoolean()) {
            entity.getProperties().add(factory.newPrimitiveProperty(propertyName, 
                factory.newPrimitiveValueBuilder().buildBoolean(value.asBoolean())));
        }
    }

    /**
     * Creates a new entity in Dataverse.
     *
     * @param tableName The name of the table to create the entity in
     * @param entity The entity to create
     * @return The ID of the created entity
     * @throws RuntimeException if the creation fails
     */
    public UUID createEntity(String tableName, ClientEntity entity) {
        try {
            ODataClient client = connection.getClient();
            
            // Build the URI for the entity collection
            URI uri = client.newURIBuilder(dataverseServiceUrl)
                     .appendEntitySetSegment(tableName)
                     .build();
            
            log.info("Creating entity in table: {} at URI: {}", tableName, uri);
            
            // Create the request
            ODataEntityCreateRequest<ClientEntity> request = client.getCUDRequestFactory().getEntityCreateRequest(uri, entity);
            
            // Add Prefer header to request representation of the created entity using Olingo preferences
            request.setPrefer(client.newPreferences().returnRepresentation());
            log.debug("Added Prefer: return=representation header to create request");
            
            // Execute the request
            ODataEntityCreateResponse<ClientEntity> response = request.execute();
            
            if (response.getStatusCode() >= 200 && response.getStatusCode() < 300) {
                try {
                    // Try to get the created entity from the response
                    ClientEntity createdEntity = response.getBody();
                    
                    // Get the key property name from the cached configuration
                    String keyPropertyName = this.keyPropertyName;
                    if (keyPropertyName == null || keyPropertyName.isEmpty()) {
                        // Fallback to the default naming pattern if not cached
                        keyPropertyName = tableName + "id";
                        log.debug("Using fallback key property name: {}", keyPropertyName);
                    }
                    
                    // Extract the ID from the created entity
                    String entityIdStr = null;
                    for (ClientProperty property : createdEntity.getProperties()) {
                        if (property.getName().equalsIgnoreCase(keyPropertyName)) {
                            entityIdStr = property.getValue().toString().replaceAll("'", "");
                            break;
                        }
                    }
                    
                    if (entityIdStr != null) {
                        try {
                            UUID entityId = UUID.fromString(entityIdStr);
                            log.info("Successfully created entity with ID: {}", entityId);
                            return entityId;
                        } catch (IllegalArgumentException e) {
                            log.warn("Entity ID '{}' is not a valid UUID, using generated UUID", entityIdStr);
                        }
                    } else {
                        log.warn("Could not find ID property '{}' in response entity, using generated UUID", keyPropertyName);
                    }
                } catch (org.apache.olingo.client.api.http.NoContentException e) {
                    // This is normal for HTTP 204 responses, which don't include a response body
                    log.warn("No content in response (HTTP 204), but entity was created successfully");
                }
                
                log.error("No entity ID found in response");
                throw new RuntimeException("No entity ID found in response");
            } else {
                log.error("Failed to create entity, status code: {}", response.getStatusCode());
                throw new RuntimeException("Failed to create entity, status code: " + response.getStatusCode());
            }
        } catch (org.apache.olingo.client.api.http.NoContentException e) {
            // This is normal for HTTP 204 responses, which don't include a response body
            log.error("No content in response (HTTP 204), but entity was created successfully");
            throw new RuntimeException("No content in response (HTTP 204), but entity was created successfully");
        } catch (Exception e) {
            log.error("Error creating entity in table: {}", tableName, e);
            throw new RuntimeException("Failed to create entity", e);
        }
    }

    /**
     * Updates an existing entity in Dataverse.
     *
     * @param tableName The name of the table containing the entity
     * @param entityId The ID of the entity to update
     * @param entity The updated entity data
     * @throws RuntimeException if the update fails
     */
    public void updateEntity(String tableName, UUID entityId, ClientEntity entity) {
        try {
            ODataClient client = connection.getClient();
            
            // Build the URI for the specific entity
            URI uri = client.newURIBuilder(dataverseServiceUrl)
                     .appendEntitySetSegment(tableName)
                     .appendKeySegment(entityId)
                     .build();
            
            log.info("Updating entity in table: {} with ID: {}", tableName, entityId);
            
            // Create the request - using MERGE (PATCH) for the update
            ODataEntityUpdateRequest<ClientEntity> request = client.getCUDRequestFactory().getEntityUpdateRequest(uri, UpdateType.PATCH, entity);
            
            // Execute the request
            ODataEntityUpdateResponse<ClientEntity> response = request.execute();
            
            if (response.getStatusCode() >= 200 && response.getStatusCode() < 300) {
                log.info("Successfully updated entity with ID: {}", entityId);
            } else {
                log.error("Failed to update entity, status code: {}", response.getStatusCode());
                throw new RuntimeException("Failed to update entity, status code: " + response.getStatusCode());
            }
        } catch (Exception e) {
            log.error("Error updating entity in table: {} with ID: {}", tableName, entityId, e);
            throw new RuntimeException("Failed to update entity", e);
        }
    }

    /**
     * Deletes an entity from Dataverse.
     *
     * @param tableName The name of the table containing the entity
     * @param entityId The ID of the entity to delete
     * @throws RuntimeException if the deletion fails
     */
    public void deleteEntity(String tableName, UUID entityId) {
        try {
            ODataClient client = connection.getClient();
            
            // Build the URI for the specific entity
            URI uri = client.newURIBuilder(dataverseServiceUrl)
                     .appendEntitySetSegment(tableName)
                     .appendKeySegment(entityId)
                     .build();
            
            log.info("Deleting entity from table: {} with ID: {}", tableName, entityId);
            
            // Create the request
            ODataDeleteRequest request = client.getCUDRequestFactory().getDeleteRequest(uri);
            
            // Execute the request
            ODataDeleteResponse response = request.execute();
            
            if (response.getStatusCode() >= 200 && response.getStatusCode() < 300) {
                log.info("Successfully deleted entity with ID: {}", entityId);
            } else {
                log.error("Failed to delete entity, status code: {}", response.getStatusCode());
                throw new RuntimeException("Failed to delete entity, status code: " + response.getStatusCode());
            }
        } catch (Exception e) {
            log.error("Error deleting entity from table: {} with ID: {}", tableName, entityId, e);
            throw new RuntimeException("Failed to delete entity", e);
        }
    }

    /**
     * Retrieves an entity from Dataverse.
     *
     * @param tableName The name of the table containing the entity
     * @param entityId The ID of the entity to retrieve
     * @return The retrieved entity
     * @throws RuntimeException if the retrieval fails
     */
    public ClientEntity getEntity(String tableName, UUID entityId) {
        try {
            ODataClient client = connection.getClient();
            
            // Build the URI for the specific entity
            URI uri = client.newURIBuilder(dataverseServiceUrl)
                     .appendEntitySetSegment(tableName)
                     .appendKeySegment(entityId)
                     .build();
            
            log.info("Retrieving entity from table: {} with ID: {}", tableName, entityId);
            
            // Create the request
            ODataEntityRequest<ClientEntity> request = client.getRetrieveRequestFactory().getEntityRequest(uri);
            
            // Execute the request
            ODataRetrieveResponse<ClientEntity> response = request.execute();
            
            if (response.getStatusCode() >= 200 && response.getStatusCode() < 300) {
                ClientEntity entity = response.getBody();
                log.info("Successfully retrieved entity with ID: {}", entityId);
                return entity;
            } else {
                log.error("Failed to retrieve entity, status code: {}", response.getStatusCode());
                throw new RuntimeException("Failed to retrieve entity, status code: " + response.getStatusCode());
            }
        } catch (Exception e) {
            log.error("Error retrieving entity from table: {} with ID: {}", tableName, entityId, e);
            throw new RuntimeException("Failed to retrieve entity", e);
        }
    }

    /**
     * Updates a specific field of an entity.
     *
     * @param tableName The name of the table
     * @param entityId The ID of the entity to update
     * @param fieldName The name of the field to update
     * @param fieldValue The new value for the field
     * @throws RuntimeException if the update fails
     */
    public void updateEntityField(String tableName, UUID entityId, String fieldName, String fieldValue) {
        ODataClient client = connection.getClient();
        ClientObjectFactory factory = client.getObjectFactory();
        
        // Create a minimal entity with just the field to update
        // Using null for entity type to avoid type resolution errors
        ClientEntity entity = factory.newEntity(null);
        entity.getProperties().add(factory.newPrimitiveProperty(fieldName, 
            factory.newPrimitiveValueBuilder().buildString(fieldValue)));
        
        log.info("Updating field {} to {} for entity {} in table {}", fieldName, fieldValue, entityId, tableName);
        
        // Update the entity
        updateEntity(tableName, entityId, entity);
    }

    /**
     * Performs a complete CRUD cycle for testing purposes:
     * 1. Creates an entity
     * 2. Retrieves the entity
     * 3. Updates the entity
     * 4. Deletes the entity
     *
     * This is useful for integration tests to verify that the connector can capture all these operations.
     *
     * @return The ID of the created (and later deleted) entity
     * @throws RuntimeException if any operation fails
     */
    public UUID performTestCrudCycle() {
        try {
            // Get the table name from the cached configuration
            String tableName = getTableName();
            
            // 1. Create an entity using the configuration
            UUID entityId = createEntityFromConfig();
            log.info("Created test entity with ID: {}", entityId);
            
            // 2. Retrieve the entity to verify it was created
            ClientEntity entity = getEntity(tableName, entityId);
            log.info("Retrieved entity has {} properties", entity.getProperties().size());
            
            // Get the first field from the record configuration for the update
            JsonNode recordJson = getRecordJson();
            String updateField = null;
            String updatedValue = null;
            
            // Find the first string field in the record to update
            Iterator<Map.Entry<String, JsonNode>> fields = recordJson.fields();
            while (fields.hasNext() && updateField == null) {
                Map.Entry<String, JsonNode> field = fields.next();
                // Skip the id field and non-string fields for the update
                if (!field.getKey().toLowerCase().endsWith("id") && field.getValue().isTextual()) {
                    updateField = field.getKey();
                    // Create a new value by appending timestamp to make it unique
                    updatedValue = field.getValue().asText() + " Updated " + System.currentTimeMillis();
                    break;
                }
            }
            
            // Fallback to "name" if no suitable field was found
            if (updateField == null) {
                updateField = "name";
                updatedValue = "Updated Test Entity " + System.currentTimeMillis();
                log.warn("No suitable field found in record config, falling back to default field: {}", updateField);
            }
            
            // 3. Update the entity with the new field value
            log.info("Updating entity using field: {}", updateField);
            updateEntityField(tableName, entityId, updateField, updatedValue);
            log.info("Updated entity field {} to {}", updateField, updatedValue);
            
            // Verify the update
            entity = getEntity(tableName, entityId);
            boolean updateVerified = false;
            for (ClientProperty property : entity.getProperties()) {
                if (property.getName().equalsIgnoreCase(updateField)) {
                    log.info("Verified updated property: {} = {}", 
                            property.getName(), property.getValue().toString());
                    updateVerified = true;
                    break;
                }
            }
            
            if (!updateVerified) {
                log.warn("Could not verify update of field {} in the response entity", updateField);
            }
            
            // Wait a moment to ensure changes are processed by Dataverse
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            // 4. Delete the entity
            deleteEntity(tableName, entityId);
            log.info("Deleted entity with ID: {}", entityId);
            
            return entityId;
        } catch (Exception e) {
            log.error("Error during CRUD test cycle", e);
            throw new RuntimeException("CRUD test cycle failed", e);
        }
    }

    /**
     * Queries entities from a Dataverse table.
     *
     * @param tableName The name of the table to query
     * @param filter Optional OData filter expression (can be null for no filter)
     * @param maxResults Maximum number of results to return (0 for unlimited)
     * @return The list of entities matching the query
     * @throws RuntimeException if the query fails
     */
    public ClientEntitySet queryEntities(String tableName, String filter, int maxResults) {
        try {
            ODataClient client = connection.getClient();
            
            // Build the URI for the entity collection
            URIBuilder uriBuilder = client.newURIBuilder(dataverseServiceUrl)
                    .appendEntitySetSegment(tableName);
            
            // Add filter if provided
            if (filter != null && !filter.isEmpty()) {
                uriBuilder.filter(filter);
            }
            
            // Add top parameter if maxResults is specified
            if (maxResults > 0) {
                uriBuilder.top(maxResults);
            }
            
            URI uri = uriBuilder.build();
            
            log.info("Querying entities from table: {} with URI: {}", tableName, uri);
            
            // Create the request
            ODataEntitySetRequest<ClientEntitySet> request = client.getRetrieveRequestFactory().getEntitySetRequest(uri);
            
            // Execute the request
            ODataRetrieveResponse<ClientEntitySet> response = request.execute();
            
            if (response.getStatusCode() >= 200 && response.getStatusCode() < 300) {
                ClientEntitySet entitySet = response.getBody();
                log.info("Successfully retrieved {} entities from table: {}", 
                         entitySet.getEntities().size(), tableName);
                return entitySet;
            } else {
                log.error("Failed to query entities, status code: {}", response.getStatusCode());
                throw new RuntimeException("Failed to query entities, status code: " + response.getStatusCode());
            }
        } catch (Exception e) {
            log.error("Error querying entities from table: {}", tableName, e);
            throw new RuntimeException("Failed to query entities", e);
        }
    }
} 