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

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.math.BigDecimal;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.olingo.client.api.domain.ClientDeletedEntity;
import org.apache.olingo.client.api.domain.ClientDelta;
import org.apache.olingo.client.api.domain.ClientEntity;
import org.apache.olingo.client.api.domain.ClientEntitySet;
import org.apache.olingo.client.api.domain.ClientProperty;
import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.commons.api.edm.EdmElement;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmKeyPropertyRef;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.constants.EdmTypeKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts OData/Olingo entities from Dataverse to Kafka Connect records.
 * This class handles schema mapping and data conversion between the two systems.
 */
public class OlingoToConnectConverter {

    static final Logger log = LoggerFactory.getLogger(OlingoToConnectConverter.class);

    private static final Map<String, Schema> schemas = Collections.synchronizedMap(new HashMap<>());

    /**
     * Gets the Kafka Connect schema for the given entity set.
     * 
     * @param edm The Edm metadata object
     * @param entitySetName The name of the entity set
     * @return The Schema for the entity set
     * @throws IllegalArgumentException if edm or entitySetName is null or if the entity set cannot be found
     */
    public static Schema getSchema(Edm edm, String entitySetName) {
        if (edm == null) {
            throw new IllegalArgumentException("EDM cannot be null");
        }
        if (entitySetName == null) {
            throw new IllegalArgumentException("Entity set name cannot be null");
        }
        
        // Check if schema is already cached
        if (schemas.get(entitySetName) == null) {
            schemas.put(entitySetName, SchemaBuilder.map(
                buildKeySchema(edm, entitySetName), 
                buildSchema(edm, entitySetName)
            ).name(entitySetName).build());
        }
        return schemas.get(entitySetName);
    }
    
    /**
     * Builds a key schema for the given entity set.
     * 
     * @param edm The Edm metadata object
     * @param entitySetName The name of the entity set
     * @return The key Schema for the entity set
     * @throws IllegalArgumentException if edm or entitySetName is null, or if the entity set cannot be found
     */
    private static Schema buildKeySchema(Edm edm, String entitySetName) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        if (edm == null || entitySetName == null) {
            log.error("Null EDM or entity set name provided");
            throw new IllegalArgumentException("EDM and entity set name cannot be null");
        }
        
        if (edm.getEntityContainer() == null) {
            log.error("Entity container is null in EDM");
            throw new IllegalArgumentException("Entity container is null in EDM");
        }
        
        if (edm.getEntityContainer().getEntitySet(entitySetName) == null) {
            log.error("Entity set {} not found in EDM", entitySetName);
            throw new IllegalArgumentException("Entity set not found: " + entitySetName);
        }
        
        try {
            for (EdmKeyPropertyRef keyRef : edm.getEntityContainer().getEntitySet(entitySetName).getEntityType().getKeyPropertyRefs()) {
                if (keyRef == null || keyRef.getProperty() == null) {
                    log.warn("Null key property reference found in entity set {}", entitySetName);
                    continue;
                }
                schemaBuilder = schemaBuilder.field(keyRef.getName(), getSchema(keyRef.getProperty()));
            }
        } catch (Exception e) {
            log.error("Error building key schema for entity set {}", entitySetName, e);
            throw new IllegalStateException("Failed to build key schema for entity set: " + entitySetName, e);
        }
        
        return schemaBuilder.build();
    }
  
    /**
     * Builds a value schema for the given entity set.
     * 
     * @param edm The Edm metadata object
     * @param entitySetName The name of the entity set
     * @return The value Schema for the entity set
     * @throws IllegalArgumentException if the entity set cannot be found or other validation errors occur
     */
    private static Schema buildSchema(Edm edm, String entitySetName) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        
        if (edm == null || entitySetName == null) {
            log.error("Null EDM or entity set name provided");
            throw new IllegalArgumentException("EDM and entity set name cannot be null");
        }
        
        if (edm.getEntityContainer() == null) {
            log.error("Entity container is null in EDM");
            throw new IllegalArgumentException("Entity container is null in EDM");
        }
        
        if (edm.getEntityContainer().getEntitySet(entitySetName) == null) {
            log.error("Entity set {} not found in EDM", entitySetName);
            throw new IllegalArgumentException("Entity set not found: " + entitySetName);
        }
        
        try {
            EdmEntityType entityType = edm.getEntityContainer().getEntitySet(entitySetName).getEntityType();
            if (entityType == null) {
                log.error("Entity type is null for entity set {}", entitySetName);
                throw new IllegalStateException("Entity type is null for entity set: " + entitySetName);
            }
            
            for (String propertyName : entityType.getPropertyNames()) {
                if (propertyName == null) {
                    log.warn("Null property name found in entity set {}", entitySetName);
                    continue;
                }
                
                EdmElement property = entityType.getProperty(propertyName);
                if (property == null) {
                    log.warn("Property {} in entity set {} is null", propertyName, entitySetName);
                    continue;
                }
                
                schemaBuilder = schemaBuilder.field(propertyName, getSchema(property));
            }
        } catch (Exception e) {
            log.error("Error building schema for entity set {}", entitySetName, e);
            throw new IllegalStateException("Failed to build schema for entity set: " + entitySetName, e);
        }
        
        return schemaBuilder.build();
    }
    
    /**
     * Gets the Kafka Connect schema for the given EDM element.
     * 
     * @param property The EdmElement
     * @return The Schema for the element
     * @throws IllegalArgumentException if property is null
     * @throws UnsupportedOperationException if the property type is not supported
     */
    static Schema getSchema(EdmElement property) {
        if (property == null) {
            log.error("Property is null");
            throw new IllegalArgumentException("Property cannot be null");
        }
        
        if (property.getType() == null) {
            log.error("Property type is null for property {}", property.getName());
            throw new IllegalArgumentException("Property type cannot be null for property: " + property.getName());
        }
        
        if (property.getType().getKind() == null) {
            log.error("Property type kind is null for property {}", property.getName());
            throw new IllegalArgumentException("Property type kind cannot be null for property: " + property.getName());
        }
        
        if (property.getType().getKind().name().equals(EdmTypeKind.PRIMITIVE.name())) {
            try {
                String typeName = property.getType().getName();
                if (typeName == null) {
                    log.error("Property type name is null for property {}", property.getName());
                    return Schema.OPTIONAL_STRING_SCHEMA; // Default to string schema
                }
                
                EdmPrimitiveTypeKind type = EdmPrimitiveTypeKind.getByName(typeName);
                if (type == null) {
                    log.error("Unknown primitive type {} for property {}", typeName, property.getName());
                    return Schema.OPTIONAL_STRING_SCHEMA; // Default to string schema
                }
                
                switch (type) {
                    case Boolean:
                        return Schema.OPTIONAL_BOOLEAN_SCHEMA;
                    case SByte: // signed byte. precision of 8 bit
                        return Schema.OPTIONAL_INT8_SCHEMA;
                    case Single:
                        return Schema.OPTIONAL_FLOAT32_SCHEMA;
                    case Double:
                        return Schema.OPTIONAL_FLOAT64_SCHEMA;
                    case Byte: // unsigned byte. Therefore an integer greater than 8 bit required
                    case Int16:
                        return Schema.OPTIONAL_INT16_SCHEMA;
                    case Int32:
                        return Schema.OPTIONAL_INT32_SCHEMA;
                    case Int64:
                        return Schema.OPTIONAL_INT64_SCHEMA;
                    case Decimal:
                        // Don't actually know the precision of this, so defaulting to float64
                        return Schema.OPTIONAL_FLOAT64_SCHEMA;
                    case Duration:
                        // Could convert this to some sort of date time or millisecond offset. However, defaulting to string
                        return Schema.OPTIONAL_STRING_SCHEMA;
                    case Guid:
                    case String:
                    case Stream:
                        return Schema.OPTIONAL_STRING_SCHEMA;
                    case Date:
                        return Date.builder().optional();
                    case TimeOfDay:
                        return Time.builder().optional();
                    case Binary:
                        return Schema.OPTIONAL_BYTES_SCHEMA;
                    case DateTimeOffset:
                        return Timestamp.builder().optional();
                    case Geography:
                    case GeographyCollection:
                    case GeographyLineString:
                    case GeographyMultiLineString:
                    case GeographyMultiPoint:
                    case GeographyMultiPolygon:
                    case GeographyPoint:
                    case GeographyPolygon:
                    case Geometry:
                    case GeometryCollection:
                    case GeometryLineString:
                    case GeometryMultiLineString:
                    case GeometryMultiPoint:
                    case GeometryMultiPolygon:
                    case GeometryPoint:
                    case GeometryPolygon:
                        // Geographic types default to String
                        return Schema.OPTIONAL_STRING_SCHEMA;
                    default:
                        log.error("Could not identify schema for property {}, defaulting to string", property.getName());
                        return Schema.OPTIONAL_STRING_SCHEMA;
                }
            } catch (Exception e) {
                log.error("Error determining schema for property {}", property.getName(), e);
                return Schema.OPTIONAL_STRING_SCHEMA; // Default to string schema in case of error
            }
        } else {
            log.error("Non-primitive type detected for property {}: {}", 
                property.getName(), property.getType().getKind().name());
            throw new UnsupportedOperationException(
                "Non-primitive type not implemented for property: " + property.getName() + 
                " (Type: " + property.getType().getKind().name() + ")");
        }
    }

    /**
     * Test-friendly accessor for the schema generation based on EdmElement type.
     * This method is provided primarily for testing purposes.
     * 
     * @param property The EdmElement to generate a schema for
     * @return The appropriate Kafka Connect Schema for the element
     */
    public static Schema getSchemaForTesting(EdmElement property) {
        return getSchema(property);
    }
     
    /**
     * Converts a ClientEntitySet to a list of SourceRecords.
     * 
     * @param table The table name
     * @param topic The Kafka topic name
     * @param entitySet The ClientEntitySet to convert
     * @param schema The Schema to use for conversion
     * @return A list of SourceRecords
     * @throws IllegalArgumentException if any required parameter is null
     */
    public static List<SourceRecord> getClientEntitiesAsSourceRecords(String table, String topic, ClientEntitySet entitySet, Schema schema) {
        if (table == null || topic == null || entitySet == null || schema == null) {
            log.error("Null parameter provided to getClientEntitiesAsSourceRecords");
            throw new IllegalArgumentException("Table, topic, entitySet, and schema cannot be null");
        }
        URI partition = entitySet.getDeltaLink() != null ? entitySet.getDeltaLink() : entitySet.getNext();
        if (partition == null) {
            log.error("Delta link is null in entity set");
            throw new IllegalArgumentException("Delta link cannot be null in entity set");
        }
        
        ArrayList<SourceRecord> records = new ArrayList<>();
        
        if (entitySet.getEntities() == null) {
            log.warn("Entity set has null entities collection");
            return records;
        }
        
        try {
            log.debug("Converting {} entities to source records. table: {}, topic: {}, partition: {}", entitySet.getEntities().size(), table, topic, partition);
            for (ClientEntity clientEntity : entitySet.getEntities()) {
                if (clientEntity == null) {
                    log.warn("Null client entity found in entity set");
                    continue;
                }
                
                records.add(createSourceRecord(clientEntity, table, topic, partition, schema));
            }
        } catch (Exception e) {
            log.error("Error creating source records from entity set", e);
            throw new IllegalStateException("Failed to create source records from entity set", e);
        }
        
        return records;
    }

    /**
     * Converts deleted entities from a ClientDelta to a list of SourceRecords.
     * 
     * @param table The table name
     * @param topic The Kafka topic name
     * @param entitySet The ClientDelta containing deleted entities
     * @param schema The Schema to use for conversion
     * @return A list of SourceRecords for deleted entities
     * @throws IllegalArgumentException if any required parameter is null
     */
    public static List<SourceRecord> getDeletedEntitiesAsSourceRecords(String table, String topic, ClientDelta entitySet, Schema schema) {
        if (table == null || topic == null || entitySet == null || schema == null) {
            log.error("Null parameter provided to getDeletedEntitiesAsSourceRecords");
            throw new IllegalArgumentException("Table, topic, entitySet, and schema cannot be null");
        }
        URI partition = entitySet.getDeltaLink() != null ? entitySet.getDeltaLink() : entitySet.getNext();
        if (partition == null) {
            log.error("Delta link is null in entity set");
            throw new IllegalArgumentException("Delta link cannot be null in entity set");
        }
        
        if (schema.keySchema() == null || schema.keySchema().fields() == null || schema.keySchema().fields().isEmpty()) {
            log.error("Schema has no key fields");
            throw new IllegalArgumentException("Schema must have at least one key field");
        }
        
        Map<String, ?> sourcePartition = Collections.singletonMap("table", table);
        Map<String, ?> sourceOffset = Collections.singletonMap("deltaLink", partition.toString());
        ArrayList<SourceRecord> records = new ArrayList<>();
        
        if (entitySet.getDeletedEntities() == null) {
            log.warn("Entity set has null deleted entities collection");
            return records;
        }
        
        try {
            for (ClientDeletedEntity clientDeletedEntity : entitySet.getDeletedEntities()) {
                if (clientDeletedEntity == null) {
                    log.warn("Null deleted entity found in entity set");
                    continue;
                }
                
                if (clientDeletedEntity.getId() == null) {
                    log.warn("Deleted entity has null ID");
                    continue;
                }
                
                String path = clientDeletedEntity.getId().getPath();
                if (path == null) {
                    log.warn("Deleted entity has null path");
                    continue;
                }
                
                Struct key = new Struct(schema.keySchema());
                // Extract entity ID from the path
                try {
                    // Dataverse appears to only ever have a single key
                    key.put(schema.keySchema().fields().get(0), path.substring(path.lastIndexOf('/') + 1));
                } catch (IndexOutOfBoundsException e) {
                    log.error("Failed to extract entity ID from path: {}", path, e);
                    continue;
                }
                
                SourceRecord sourceRecord = new SourceRecord(
                    sourcePartition,
                    sourceOffset,
                    topic,
                    schema.keySchema(),
                    key,
                    null,
                    null);
                
                records.add(sourceRecord);
            }
        } catch (Exception e) {
            log.error("Error creating source records for deleted entities", e);
            throw new IllegalStateException("Failed to create source records for deleted entities", e);
        }
        
        return records;
    }

    /**
     * Creates a SourceRecord from a ClientEntity.
     * 
     * @param record The ClientEntity to convert
     * @param table The table name
     * @param topic The Kafka topic name
     * @param deltaLink The delta link URI
     * @param schema The Schema to use for conversion
     * @return A SourceRecord
     * @throws IllegalArgumentException if any required parameter is null
     */
    private static SourceRecord createSourceRecord(ClientEntity record, String table, String topic, URI deltaLink, Schema schema) {
        if (record == null || table == null || topic == null || deltaLink == null || schema == null) {
            log.error("Null parameter provided to createSourceRecord");
            throw new IllegalArgumentException("Record, table, topic, deltaLink, and schema cannot be null");
        }
        
        Map<String, ?> sourcePartition = Collections.singletonMap("table", table);
        Map<String, ?> sourceOffset = Collections.singletonMap("deltaLink", deltaLink.toString());
        
        try {
            // Build the key and value structs
            Struct keyStruct = buildStruct(record, table, schema.keySchema());
            Struct valueStruct = buildStruct(record, table, schema.valueSchema());
            
            // Create and return the source record
            return new SourceRecord(
                sourcePartition,
                sourceOffset,
                topic,
                schema.keySchema(),
                keyStruct,
                schema.valueSchema(),
                valueStruct);
        } catch (Exception e) {
            log.error("Error creating source record for entity in table {}", table, e);
            throw new IllegalStateException("Failed to create source record for entity in table: " + table, e);
        }
    }
  
    /**
     * Builds a Struct from a ClientEntity.
     * 
     * @param record The ClientEntity to convert
     * @param entitySetName The entity set name
     * @param schema The Schema to use for conversion
     * @return A Struct containing the entity data
     * @throws IllegalArgumentException if any required parameter is null
     */
    private static Struct buildStruct(ClientEntity record, String entitySetName, Schema schema) {
        if (record == null || entitySetName == null || schema == null) {
            log.error("Null parameter provided to buildStruct");
            throw new IllegalArgumentException("Record, entitySetName, and schema cannot be null");
        }
        
        Struct value = new Struct(schema);
        
        if (schema.fields() == null) {
            log.warn("Schema has null fields collection");
            return value;
        }
        
        try {
            for (Field field : schema.fields()) {
                if (field == null) {
                    log.warn("Null field found in schema for entity set {}", entitySetName);
                    continue;
                }
                
                ClientProperty property = record.getProperty(field.name());
                if (property != null) {
                    Object propertyValue = getValue(property, field.schema());
                    if (propertyValue != null) {
                        value.put(field, propertyValue);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error building struct for entity in entity set {}", entitySetName, e);
            throw new IllegalStateException("Failed to build struct for entity in entity set: " + entitySetName, e);
        }
        
        return value;
    }

    /**
     * Extracts a value from a ClientProperty.
     * 
     * @param property The ClientProperty to extract a value from
     * @param schema The Schema to use for conversion
     * @return The extracted value
     */
    static Object getValue(ClientProperty property, Schema schema) {
        if (property == null) {
            log.warn("Property is null");
            return null;
        }
        
        if (schema == null) {
            log.warn("Schema is null for property {}", property.getName());
            return null;
        }
        
        try {
            if (property.getValue() == null) {
                log.debug("Property {} has null value", property.getName());
                return null;
            }
            
            if (!property.getValue().isPrimitive()) {
                log.debug("Property {} is not a primitive value", property.getName());
                return null;
            }
            
            if (property.getPrimitiveValue() == null) {
                log.trace("Property {} has null primitive value", property.getName());
                return null;
            }
            
            if (property.getPrimitiveValue().toValue() == null) {
                log.trace("Property {} has null primitive value content", property.getName());
                return null;
            }
            
            if (property.getPrimitiveValue().getType() == null) {
                log.warn("Property {} has null primitive value type", property.getName());
                return null;
            }
            
            String typeName = property.getPrimitiveValue().getType().getName();
            if (typeName == null) {
                log.warn("Property {} has null type name", property.getName());
                return null;
            }
            
            EdmPrimitiveTypeKind type = EdmPrimitiveTypeKind.getByName(typeName);
            if (type == null) {
                log.warn("Unknown primitive type {} for property {}", typeName, property.getName());
                return null;
            }
            
            switch (type) {
                case Boolean:
                    return property.getPrimitiveValue().toCastValue(Boolean.class);
                case SByte: // signed byte. precision of 8 bit
                    return property.getPrimitiveValue().toCastValue(Byte.class);
                case Single:
                    return property.getPrimitiveValue().toCastValue(Float.class);
                case Double:
                    return property.getPrimitiveValue().toCastValue(Double.class);
                case Byte: // unsigned byte. Therefore an integer greater than 8 bit required
                case Int16:
                    return property.getPrimitiveValue().toCastValue(Short.class);
                case Int32:
                    return property.getPrimitiveValue().toCastValue(Integer.class);
                case Int64:
                    return property.getPrimitiveValue().toCastValue(Long.class);
                case Decimal:
                    // Don't actually know the precision of this, so defaulting to float64
                    BigDecimal decimal = property.getPrimitiveValue().toCastValue(BigDecimal.class);
                    if (decimal != null) {
                        return Double.valueOf(decimal.doubleValue());
                    }
                    return null;
                case Duration:
                    // Could convert this to some sort of date time or millisecond offset. However, defaulting to string
                    Object durationValue = property.getPrimitiveValue().toValue();
                    return durationValue != null ? durationValue.toString() : null;
                case Guid:
                case String:
                case Stream:
                    Object stringValue = property.getPrimitiveValue().toValue();
                    return stringValue != null ? stringValue.toString() : null;
                case Date:
                    return property.getPrimitiveValue().toCastValue(java.util.Date.class);
                case TimeOfDay:
                    return property.getPrimitiveValue().toCastValue(java.util.Date.class);
                case Binary:
                    return property.getPrimitiveValue().toCastValue(byte[].class);
                case DateTimeOffset:
                    return property.getPrimitiveValue().toCastValue(java.util.Date.class);
                case Geography:
                case GeographyCollection:
                case GeographyLineString:
                case GeographyMultiLineString:
                case GeographyMultiPoint:
                case GeographyMultiPolygon:
                case GeographyPoint:
                case GeographyPolygon:
                case Geometry:
                case GeometryCollection:
                case GeometryLineString:
                case GeometryMultiLineString:
                case GeometryMultiPoint:
                case GeometryMultiPolygon:
                case GeometryPoint:
                case GeometryPolygon:
                    // Geographic types default to String
                    Object geoValue = property.getPrimitiveValue().toValue();
                    return geoValue != null ? geoValue.toString() : null;
                default:
                    log.error("Could not identify schema for property {}", property.getName());
                    return null;
            }
        } catch (EdmPrimitiveTypeException e) {
            log.error("Error parsing value for property {}", property.getName(), e);
        } catch (Exception e) {
            log.error("Unexpected error processing property {}", property.getName(), e);
        }
        
        log.debug("Unable to obtain value for property {}", property.getName());
        return null;
    }

    /**
     * Test-friendly accessor for property value conversion.
     * This method is provided primarily for testing purposes.
     * 
     * @param property The ClientProperty to extract a value from
     * @param schema The target schema for the property
     * @return The converted Java object
     */
    public static Object getValueForTesting(ClientProperty property, Schema schema) {
        return getValue(property, schema);
    }
}
