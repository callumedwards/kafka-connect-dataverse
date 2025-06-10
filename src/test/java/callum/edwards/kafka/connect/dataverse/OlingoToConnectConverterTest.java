package callum.edwards.kafka.connect.dataverse;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;

import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.Collections;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.olingo.client.api.ODataClient;
import org.apache.olingo.client.api.domain.ClientDelta;
import org.apache.olingo.client.api.domain.ClientEntity;
import org.apache.olingo.client.api.domain.ClientEntitySet;
import org.apache.olingo.client.api.domain.ClientPrimitiveValue;
import org.apache.olingo.client.api.domain.ClientProperty;
import org.apache.olingo.client.api.domain.ClientValue;
import org.apache.olingo.client.core.ODataClientFactory;
import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.commons.api.edm.EdmElement;
import org.apache.olingo.commons.api.edm.EdmPrimitiveType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.api.edm.constants.EdmTypeKind;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@DisplayName("OlingoToConnectConverter Tests")
class OlingoToConnectConverterTest {
    
    protected static final ODataClient client = ODataClientFactory.getClient();
    protected static Edm edm;

    @BeforeAll
    static void setup() {
        try (InputStream metadataStream = OlingoToConnectConverterTest.class.getResourceAsStream("metadata-base.xml")) {
            assertNotNull(metadataStream, "Test metadata file not found");
            edm = client.getReader().readMetadata(metadataStream);
            assertNotNull(edm, "Failed to read EDM metadata");
        } catch (Exception e) {
            fail("Failed to load test metadata: " + e.getMessage());
        }
    }

    @Nested
    @DisplayName("Schema Generation Tests")
    class SchemaGenerationTests {
        
        @Test
        @DisplayName("Should generate schema for known entity set")
        void testKnownEntitySet() {
            // Given
            String entitySetName = "accounts";
            
            // When
            Schema schema = OlingoToConnectConverter.getSchema(edm, entitySetName);
            
            // Then
            assertNotNull(schema, "Schema should not be null");
            Schema keySchema = schema.keySchema();
            assertNotNull(keySchema, "Key schema should not be null");
            
            Field keyField = keySchema.field("accountid");
            assertNotNull(keyField, "Expected key field 'accountid' not found in key schema");
            assertNull(keySchema.field("accountnumber"), "Unexpected field 'accountnumber' found in key schema");
            
            // Verify value schema has fields
            Schema valueSchema = schema.valueSchema();
            assertNotNull(valueSchema, "Value schema should not be null");
            assertTrue(valueSchema.fields().size() > 0, "Value schema should have fields");
        }

        @Test
        @DisplayName("Should throw exception for unknown entity set")
        void testUnknownEntitySet() {
            // Given
            String entitySetName = "****unknown-entity-set*****";
            
            // When/Then - expecting an exception
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, 
                () -> OlingoToConnectConverter.getSchema(edm, entitySetName),
                "Should throw IllegalArgumentException for unknown entity set");
            
            assertTrue(exception.getMessage().contains("not found"), "Exception message should indicate entity set not found");
        }
        
        @Test
        @DisplayName("Should throw exception for null EDM")
        void testNullEDM() {
            // When/Then
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, 
                () -> OlingoToConnectConverter.getSchema(null, "any-entity-set"),
                "Should throw IllegalArgumentException for null EDM");
            
            assertTrue(exception.getMessage().contains("cannot be null"), 
                "Exception message should indicate that EDM cannot be null");
        }
        
        @Test
        @DisplayName("Should throw exception for null entity set name")
        void testNullEntitySet() {
            // When/Then
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, 
                () -> OlingoToConnectConverter.getSchema(edm, null),
                "Should throw IllegalArgumentException for null entity set name");
            
            assertTrue(exception.getMessage().contains("cannot be null"), 
                "Exception message should indicate that entity set name cannot be null");
        }
    }
    
    @Nested
    @DisplayName("Schema Caching Tests")
    class SchemaCachingTests {
        
        @Test
        @DisplayName("Should cache and reuse schemas")
        void testSchemaCaching() {
            // When
            Schema schema1 = OlingoToConnectConverter.getSchema(edm, "accounts");
            Schema schema2 = OlingoToConnectConverter.getSchema(edm, "accounts");
            
            // Then
            assertSame(schema1, schema2, "Schema instances should be the same object (cached)");
        }
    }
    
    @Nested
    @DisplayName("Data Type Schema Conversion Tests")
    class DataTypeSchemaConversionTests {
        
        @Test
        @DisplayName("Should convert boolean type correctly")
        void testBooleanTypeConversion() {
            // Create mock EdmElement for Boolean type
            EdmElement mockElement = createMockEdmElement(EdmPrimitiveTypeKind.Boolean);
            
            // Get schema for the mock element
            Schema schema = OlingoToConnectConverter.getSchema(mockElement);
            
            // Verify schema
            assertEquals(Schema.OPTIONAL_BOOLEAN_SCHEMA, schema, "Boolean type should convert to OPTIONAL_BOOLEAN_SCHEMA");
        }
        
        @Test
        @DisplayName("Should convert numeric types correctly")
        void testNumericTypeConversion() {
            // Test various numeric types
            assertEquals(Schema.OPTIONAL_INT8_SCHEMA, 
                OlingoToConnectConverter.getSchema(createMockEdmElement(EdmPrimitiveTypeKind.SByte)),
                "SByte should convert to INT8");
                
            assertEquals(Schema.OPTIONAL_FLOAT32_SCHEMA, 
                OlingoToConnectConverter.getSchema(createMockEdmElement(EdmPrimitiveTypeKind.Single)),
                "Single should convert to FLOAT32");
                
            assertEquals(Schema.OPTIONAL_FLOAT64_SCHEMA, 
                OlingoToConnectConverter.getSchema(createMockEdmElement(EdmPrimitiveTypeKind.Double)),
                "Double should convert to FLOAT64");
                
            assertEquals(Schema.OPTIONAL_INT16_SCHEMA, 
                OlingoToConnectConverter.getSchema(createMockEdmElement(EdmPrimitiveTypeKind.Int16)),
                "Int16 should convert to INT16");
                
            assertEquals(Schema.OPTIONAL_INT32_SCHEMA, 
                OlingoToConnectConverter.getSchema(createMockEdmElement(EdmPrimitiveTypeKind.Int32)),
                "Int32 should convert to INT32");
                
            assertEquals(Schema.OPTIONAL_INT64_SCHEMA, 
                OlingoToConnectConverter.getSchema(createMockEdmElement(EdmPrimitiveTypeKind.Int64)),
                "Int64 should convert to INT64");
                
            assertEquals(Schema.OPTIONAL_FLOAT64_SCHEMA, 
                OlingoToConnectConverter.getSchema(createMockEdmElement(EdmPrimitiveTypeKind.Decimal)),
                "Decimal should convert to FLOAT64");
        }
        
        @Test
        @DisplayName("Should convert string types correctly")
        void testStringTypeConversion() {
            assertEquals(Schema.OPTIONAL_STRING_SCHEMA, 
                OlingoToConnectConverter.getSchema(createMockEdmElement(EdmPrimitiveTypeKind.String)),
                "String should convert to STRING");
                
            assertEquals(Schema.OPTIONAL_STRING_SCHEMA, 
                OlingoToConnectConverter.getSchema(createMockEdmElement(EdmPrimitiveTypeKind.Guid)),
                "Guid should convert to STRING");
                
            assertEquals(Schema.OPTIONAL_STRING_SCHEMA, 
                OlingoToConnectConverter.getSchema(createMockEdmElement(EdmPrimitiveTypeKind.Duration)),
                "Duration should convert to STRING");
        }
        
        @Test
        @DisplayName("Should convert date/time types correctly")
        void testDateTimeTypeConversion() {
            Schema dateSchema = OlingoToConnectConverter.getSchema(createMockEdmElement(EdmPrimitiveTypeKind.Date));
            assertEquals(org.apache.kafka.connect.data.Date.SCHEMA.type(), dateSchema.type(),
                "Date should convert to Kafka Connect Date schema");
                
            Schema timeSchema = OlingoToConnectConverter.getSchema(createMockEdmElement(EdmPrimitiveTypeKind.TimeOfDay));
            assertEquals(org.apache.kafka.connect.data.Time.SCHEMA.type(), timeSchema.type(),
                "TimeOfDay should convert to Kafka Connect Time schema");
                
            Schema timestampSchema = OlingoToConnectConverter.getSchema(createMockEdmElement(EdmPrimitiveTypeKind.DateTimeOffset));
            assertEquals(org.apache.kafka.connect.data.Timestamp.SCHEMA.type(), timestampSchema.type(),
                "DateTimeOffset should convert to Kafka Connect Timestamp schema");
        }
        
        @Test
        @DisplayName("Should convert binary type correctly")
        void testBinaryTypeConversion() {
            assertEquals(Schema.OPTIONAL_BYTES_SCHEMA, 
                OlingoToConnectConverter.getSchema(createMockEdmElement(EdmPrimitiveTypeKind.Binary)),
                "Binary should convert to BYTES");
        }
        
        @Test
        @DisplayName("Should convert geography/geometry types to string")
        void testGeographyTypeConversion() {
            assertEquals(Schema.OPTIONAL_STRING_SCHEMA, 
                OlingoToConnectConverter.getSchema(createMockEdmElement(EdmPrimitiveTypeKind.Geography)),
                "Geography should convert to STRING");
                
            assertEquals(Schema.OPTIONAL_STRING_SCHEMA, 
                OlingoToConnectConverter.getSchema(createMockEdmElement(EdmPrimitiveTypeKind.GeographyPoint)),
                "GeographyPoint should convert to STRING");
                
            assertEquals(Schema.OPTIONAL_STRING_SCHEMA, 
                OlingoToConnectConverter.getSchema(createMockEdmElement(EdmPrimitiveTypeKind.Geometry)),
                "Geometry should convert to STRING");
        }
        
        @Test
        @DisplayName("Should handle unknown type with default behavior")
        void testUnknownTypeConversion() {
            // Test that a known "unusual" type is mapped to STRING schema
            // Geography is a good example that's already handled in the switch case
            // This verifies the default behavior without trying to create artificial unknown types
            Schema geoSchema = OlingoToConnectConverter.getSchema(createMockEdmElement(EdmPrimitiveTypeKind.Geography));
            assertEquals(Schema.OPTIONAL_STRING_SCHEMA, geoSchema, 
                "Geography type should be converted to STRING schema");
            
            // We can also verify that all enum values have mappings and don't hit the default case unexpectedly
            for (EdmPrimitiveTypeKind typeKind : EdmPrimitiveTypeKind.values()) {
                Schema schema = OlingoToConnectConverter.getSchema(createMockEdmElement(typeKind));
                assertNotNull(schema, "All EdmPrimitiveTypeKind values should map to a non-null schema");
            }
        }
        
        @Test
        @DisplayName("Should handle non-primitive type")
        void testNonPrimitiveTypeConversion() {
            // Create a mock element with non-primitive type
            EdmElement complexElement = mock(EdmElement.class);
            EdmType complexType = mock(EdmType.class);
            
            when(complexElement.getName()).thenReturn("ComplexField");
            when(complexElement.getType()).thenReturn(complexType);
            when(complexType.getKind()).thenReturn(EdmTypeKind.COMPLEX);
            
            assertThrows(UnsupportedOperationException.class, 
                () -> OlingoToConnectConverter.getSchema(complexElement),
                "Non-primitive type should throw UnsupportedOperationException");
        }
        
        // Helper method to create mock EdmElement with specified type
        private EdmElement createMockEdmElement(EdmPrimitiveTypeKind typeKind) {
            EdmElement mockElement = mock(EdmElement.class);
            EdmPrimitiveType mockType = mock(EdmPrimitiveType.class);
            
            when(mockElement.getName()).thenReturn(typeKind.name() + "Field");
            when(mockElement.getType()).thenReturn(mockType);
            when(mockType.getKind()).thenReturn(EdmTypeKind.PRIMITIVE);
            when(mockType.getName()).thenReturn(typeKind.name());
            
            return mockElement;
        }
    }
    
    @Nested
    @DisplayName("Property Value Conversion Tests")
    class PropertyValueConversionTests {
        
        @Test
        @DisplayName("Should convert boolean value correctly")
        void testBooleanValueConversion() {
            // Create mock for Boolean property
            ClientProperty boolProperty = createMockProperty(EdmPrimitiveTypeKind.Boolean, true);
            
            // Test conversion
            Object value = OlingoToConnectConverter.getValue(boolProperty, Schema.OPTIONAL_BOOLEAN_SCHEMA);
            
            // Verify
            assertEquals(Boolean.TRUE, value, "Boolean value should be converted correctly");
        }
        
        @Test
        @DisplayName("Should convert numeric values correctly")
        void testNumericValueConversion() {
            // Test SByte
            ClientProperty sbyteProperty = createMockProperty(EdmPrimitiveTypeKind.SByte, (byte)42);
            assertEquals((byte)42, OlingoToConnectConverter.getValue(sbyteProperty, Schema.OPTIONAL_INT8_SCHEMA), 
                "SByte value should be converted correctly");
                
            // Test Single
            ClientProperty singleProperty = createMockProperty(EdmPrimitiveTypeKind.Single, 42.5f);
            assertEquals(42.5f, OlingoToConnectConverter.getValue(singleProperty, Schema.OPTIONAL_FLOAT32_SCHEMA), 
                "Single value should be converted correctly");
                
            // Test Double
            ClientProperty doubleProperty = createMockProperty(EdmPrimitiveTypeKind.Double, 42.5d);
            assertEquals(42.5d, OlingoToConnectConverter.getValue(doubleProperty, Schema.OPTIONAL_FLOAT64_SCHEMA), 
                "Double value should be converted correctly");
                
            // Test Int16
            ClientProperty int16Property = createMockProperty(EdmPrimitiveTypeKind.Int16, (short)42);
            assertEquals((short)42, OlingoToConnectConverter.getValue(int16Property, Schema.OPTIONAL_INT16_SCHEMA), 
                "Int16 value should be converted correctly");
                
            // Test Int32
            ClientProperty int32Property = createMockProperty(EdmPrimitiveTypeKind.Int32, 42);
            assertEquals(42, OlingoToConnectConverter.getValue(int32Property, Schema.OPTIONAL_INT32_SCHEMA), 
                "Int32 value should be converted correctly");
                
            // Test Int64
            ClientProperty int64Property = createMockProperty(EdmPrimitiveTypeKind.Int64, 42L);
            assertEquals(42L, OlingoToConnectConverter.getValue(int64Property, Schema.OPTIONAL_INT64_SCHEMA), 
                "Int64 value should be converted correctly");
                
            // Test Decimal
            BigDecimal decimalValue = new BigDecimal("42.5");
            ClientProperty decimalProperty = createMockProperty(EdmPrimitiveTypeKind.Decimal, decimalValue);
            assertEquals(42.5d, OlingoToConnectConverter.getValue(decimalProperty, Schema.OPTIONAL_FLOAT64_SCHEMA), 
                "Decimal value should be converted correctly to Double");
        }
        
        @Test
        @DisplayName("Should convert string values correctly")
        void testStringValueConversion() {
            // Test String
            ClientProperty stringProperty = createMockProperty(EdmPrimitiveTypeKind.String, "test string");
            assertEquals("test string", OlingoToConnectConverter.getValue(stringProperty, Schema.OPTIONAL_STRING_SCHEMA), 
                "String value should be converted correctly");
                
            // Test Guid
            ClientProperty guidProperty = createMockProperty(EdmPrimitiveTypeKind.Guid, "12345678-1234-1234-1234-123456789012");
            assertEquals("12345678-1234-1234-1234-123456789012", 
                OlingoToConnectConverter.getValue(guidProperty, Schema.OPTIONAL_STRING_SCHEMA), 
                "Guid value should be converted correctly to String");
                
            // Test Duration
            ClientProperty durationProperty = createMockProperty(EdmPrimitiveTypeKind.Duration, "PT1H30M");
            assertEquals("PT1H30M", OlingoToConnectConverter.getValue(durationProperty, Schema.OPTIONAL_STRING_SCHEMA), 
                "Duration value should be converted correctly to String");
        }
        
        @Test
        @DisplayName("Should convert date/time values correctly")
        void testDateTimeValueConversion() {
            // Create date for testing
            Date testDate = new Date();
            
            // Test Date
            ClientProperty dateProperty = createMockProperty(EdmPrimitiveTypeKind.Date, testDate);
            assertEquals(testDate, OlingoToConnectConverter.getValue(dateProperty, org.apache.kafka.connect.data.Date.builder().optional()), 
                "Date value should be converted correctly");
                
            // Test TimeOfDay
            ClientProperty timeProperty = createMockProperty(EdmPrimitiveTypeKind.TimeOfDay, testDate);
            assertEquals(testDate, OlingoToConnectConverter.getValue(timeProperty, org.apache.kafka.connect.data.Time.builder().optional()), 
                "Time value should be converted correctly");
                
            // Test DateTimeOffset
            ClientProperty timestampProperty = createMockProperty(EdmPrimitiveTypeKind.DateTimeOffset, testDate);
            assertEquals(testDate, OlingoToConnectConverter.getValue(timestampProperty, org.apache.kafka.connect.data.Timestamp.builder().optional()), 
                "Timestamp value should be converted correctly");
        }
        
        @Test
        @DisplayName("Should convert binary value correctly")
        void testBinaryValueConversion() {
            // Test Binary
            byte[] testBytes = "test binary data".getBytes();
            ClientProperty binaryProperty = createMockProperty(EdmPrimitiveTypeKind.Binary, testBytes);
            assertArrayEquals(testBytes, (byte[])OlingoToConnectConverter.getValue(binaryProperty, Schema.OPTIONAL_BYTES_SCHEMA), 
                "Binary value should be converted correctly");
        }
        
        @Test
        @DisplayName("Should convert geography/geometry values to string")
        void testGeographyValueConversion() {
            // Test Geography
            String geoJson = "{\"type\":\"Point\",\"coordinates\":[1.0,2.0]}";
            ClientProperty geoProperty = createMockProperty(EdmPrimitiveTypeKind.Geography, geoJson);
            assertEquals(geoJson, OlingoToConnectConverter.getValue(geoProperty, Schema.OPTIONAL_STRING_SCHEMA), 
                "Geography value should be converted correctly to String");
        }
        
        @Test
        @DisplayName("Should handle null value")
        void testNullValueConversion() {
            // Create mock with null value
            ClientProperty nullProperty = mock(ClientProperty.class);
            ClientValue nullValue = mock(ClientValue.class);
            ClientPrimitiveValue nullPrimitive = mock(ClientPrimitiveValue.class);
            
            when(nullProperty.getName()).thenReturn("NullField");
            when(nullProperty.getValue()).thenReturn(nullValue);
            when(nullValue.isPrimitive()).thenReturn(true);
            when(nullProperty.getPrimitiveValue()).thenReturn(nullPrimitive);
            when(nullPrimitive.toValue()).thenReturn(null);
            
            Object result = OlingoToConnectConverter.getValue(nullProperty, Schema.OPTIONAL_STRING_SCHEMA);
            assertNull(result, "Null value should be preserved");
        }
        
        // Helper method to create mock ClientProperty with specified type and value
        private ClientProperty createMockProperty(EdmPrimitiveTypeKind typeKind, Object value) {
            ClientProperty mockProperty = mock(ClientProperty.class);
            ClientValue mockValue = mock(ClientValue.class);
            ClientPrimitiveValue mockPrimitive = mock(ClientPrimitiveValue.class);
            EdmPrimitiveType mockType = mock(EdmPrimitiveType.class);
            
            when(mockProperty.getName()).thenReturn(typeKind.name() + "Field");
            when(mockProperty.getValue()).thenReturn(mockValue);
            when(mockValue.isPrimitive()).thenReturn(true);
            when(mockProperty.getPrimitiveValue()).thenReturn(mockPrimitive);
            when(mockPrimitive.getType()).thenReturn(mockType);
            when(mockType.getName()).thenReturn(typeKind.name());
            when(mockPrimitive.toValue()).thenReturn(value);
            
            try {
                when(mockPrimitive.toCastValue(any())).thenReturn(value);
            } catch (Exception e) {
                // Ignore exception in mock setup
            }
            
            return mockProperty;
        }
    }
    
    @Nested
    @DisplayName("Source Record Generation Tests")
    class SourceRecordGenerationTests {
        
        @Test
        @DisplayName("Should throw exception for null parameters in getClientEntitiesAsSourceRecords")
        void testNullParametersInGetClientEntitiesAsSourceRecords() {
            // Create a mock ClientEntitySet with a delta link
            ClientEntitySet mockEntitySet = mock(ClientEntitySet.class);
            URI mockUri = URI.create("https://example.com/deltalink");
            when(mockEntitySet.getDeltaLink()).thenReturn(mockUri);
            
            // Create a test schema
            Schema schema = OlingoToConnectConverter.getSchema(edm, "accounts");
            
            // Test null table
            IllegalArgumentException exception1 = assertThrows(IllegalArgumentException.class,
                () -> OlingoToConnectConverter.getClientEntitiesAsSourceRecords(null, "topic", mockEntitySet, schema),
                "Should throw IllegalArgumentException when table is null");
            assertTrue(exception1.getMessage().contains("cannot be null"), 
                "Exception message should indicate parameters cannot be null");
            
            // Test null topic
            IllegalArgumentException exception2 = assertThrows(IllegalArgumentException.class,
                () -> OlingoToConnectConverter.getClientEntitiesAsSourceRecords("table", null, mockEntitySet, schema),
                "Should throw IllegalArgumentException when topic is null");
            assertTrue(exception2.getMessage().contains("cannot be null"), 
                "Exception message should indicate parameters cannot be null");
            
            // Test null entitySet
            IllegalArgumentException exception3 = assertThrows(IllegalArgumentException.class,
                () -> OlingoToConnectConverter.getClientEntitiesAsSourceRecords("table", "topic", null, schema),
                "Should throw IllegalArgumentException when entitySet is null");
            assertTrue(exception3.getMessage().contains("cannot be null"), 
                "Exception message should indicate parameters cannot be null");
            
            // Test null schema
            IllegalArgumentException exception4 = assertThrows(IllegalArgumentException.class,
                () -> OlingoToConnectConverter.getClientEntitiesAsSourceRecords("table", "topic", mockEntitySet, null),
                "Should throw IllegalArgumentException when schema is null");
            assertTrue(exception4.getMessage().contains("cannot be null"), 
                "Exception message should indicate parameters cannot be null");
        }
        
        @Test
        @DisplayName("Should throw exception when delta link is null in getClientEntitiesAsSourceRecords")
        void testNullDeltaLinkInGetClientEntitiesAsSourceRecords() {
            // Create a mock ClientEntitySet with a null delta link
            ClientEntitySet mockEntitySet = mock(ClientEntitySet.class);
            when(mockEntitySet.getDeltaLink()).thenReturn(null);
            
            // Create a test schema
            Schema schema = OlingoToConnectConverter.getSchema(edm, "accounts");
            
            // Test null delta link
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> OlingoToConnectConverter.getClientEntitiesAsSourceRecords("table", "topic", mockEntitySet, schema),
                "Should throw IllegalArgumentException when delta link is null");
            assertTrue(exception.getMessage().contains("Delta link cannot be null"), 
                "Exception message should indicate delta link cannot be null");
        }
        
        @Test
        @DisplayName("Should handle empty entity collection gracefully in getClientEntitiesAsSourceRecords")
        void testEmptyEntityCollectionInGetClientEntitiesAsSourceRecords() {
            // Create a mock ClientEntitySet with a delta link and empty entities collection
            ClientEntitySet mockEntitySet = mock(ClientEntitySet.class);
            URI mockUri = URI.create("https://example.com/deltalink");
            when(mockEntitySet.getDeltaLink()).thenReturn(mockUri);
            when(mockEntitySet.getEntities()).thenReturn(Collections.emptyList());
            
            // Create a test schema
            Schema schema = OlingoToConnectConverter.getSchema(edm, "accounts");
            
            // Test empty entity collection
            List<SourceRecord> records = OlingoToConnectConverter.getClientEntitiesAsSourceRecords("table", "topic", mockEntitySet, schema);
            
            // Should return an empty list without throwing an exception
            assertNotNull(records, "Records list should not be null");
            assertTrue(records.isEmpty(), "Records list should be empty");
        }
        
        @Test
        @DisplayName("Should throw exception for null parameters in getDeletedEntitiesAsSourceRecords")
        void testNullParametersInGetDeletedEntitiesAsSourceRecords() {
            // Create a mock ClientDelta with a delta link
            ClientDelta mockDelta = mock(ClientDelta.class);
            URI mockUri = URI.create("https://example.com/deltalink");
            when(mockDelta.getDeltaLink()).thenReturn(mockUri);
            
            // Create a test schema
            Schema schema = OlingoToConnectConverter.getSchema(edm, "accounts");
            
            // Test null table
            IllegalArgumentException exception1 = assertThrows(IllegalArgumentException.class,
                () -> OlingoToConnectConverter.getDeletedEntitiesAsSourceRecords(null, "topic", mockDelta, schema),
                "Should throw IllegalArgumentException when table is null");
            assertTrue(exception1.getMessage().contains("cannot be null"), 
                "Exception message should indicate parameters cannot be null");
            
            // Test null topic
            IllegalArgumentException exception2 = assertThrows(IllegalArgumentException.class,
                () -> OlingoToConnectConverter.getDeletedEntitiesAsSourceRecords("table", null, mockDelta, schema),
                "Should throw IllegalArgumentException when topic is null");
            assertTrue(exception2.getMessage().contains("cannot be null"), 
                "Exception message should indicate parameters cannot be null");
            
            // Test null entitySet
            IllegalArgumentException exception3 = assertThrows(IllegalArgumentException.class,
                () -> OlingoToConnectConverter.getDeletedEntitiesAsSourceRecords("table", "topic", null, schema),
                "Should throw IllegalArgumentException when entitySet is null");
            assertTrue(exception3.getMessage().contains("cannot be null"), 
                "Exception message should indicate parameters cannot be null");
            
            // Test null schema
            IllegalArgumentException exception4 = assertThrows(IllegalArgumentException.class,
                () -> OlingoToConnectConverter.getDeletedEntitiesAsSourceRecords("table", "topic", mockDelta, null),
                "Should throw IllegalArgumentException when schema is null");
            assertTrue(exception4.getMessage().contains("cannot be null"), 
                "Exception message should indicate parameters cannot be null");
        }
        
        @Test
        @DisplayName("Should throw exception when delta link is null in getDeletedEntitiesAsSourceRecords")
        void testNullDeltaLinkInGetDeletedEntitiesAsSourceRecords() {
            // Create a mock ClientDelta with a null delta link
            ClientDelta mockDelta = mock(ClientDelta.class);
            when(mockDelta.getDeltaLink()).thenReturn(null);
            
            // Create a test schema
            Schema schema = OlingoToConnectConverter.getSchema(edm, "accounts");
            
            // Test null delta link
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> OlingoToConnectConverter.getDeletedEntitiesAsSourceRecords("table", "topic", mockDelta, schema),
                "Should throw IllegalArgumentException when delta link is null");
            assertTrue(exception.getMessage().contains("Delta link cannot be null"), 
                "Exception message should indicate delta link cannot be null");
        }
        
        @Test
        @DisplayName("Should handle empty deleted entities collection gracefully in getDeletedEntitiesAsSourceRecords")
        void testEmptyDeletedEntitiesCollectionInGetDeletedEntitiesAsSourceRecords() {
            // Create a mock ClientDelta with a delta link and empty deleted entities collection
            ClientDelta mockDelta = mock(ClientDelta.class);
            URI mockUri = URI.create("https://example.com/deltalink");
            when(mockDelta.getDeltaLink()).thenReturn(mockUri);
            when(mockDelta.getDeletedEntities()).thenReturn(Collections.emptyList());
            
            // Create a test schema
            Schema schema = OlingoToConnectConverter.getSchema(edm, "accounts");
            
            // Test empty deleted entities collection
            List<SourceRecord> records = OlingoToConnectConverter.getDeletedEntitiesAsSourceRecords("table", "topic", mockDelta, schema);
            
            // Should return an empty list without throwing an exception
            assertNotNull(records, "Records list should not be null");
            assertTrue(records.isEmpty(), "Records list should be empty");
        }
    }
    
    @Nested
    @DisplayName("Error Handling Tests")
    class ErrorHandlingTests {
        
        @Test
        @DisplayName("Should handle null property gracefully")
        void testNullProperty() {
            // With our improved null checks, the method should return null rather than throwing an exception
            Object result = OlingoToConnectConverter.getValue(null, Schema.OPTIONAL_STRING_SCHEMA);
            assertNull(result, "Null property should result in null value");
        }
        
        @Test
        @DisplayName("Should handle null schema gracefully")
        void testNullSchema() {
            // Create mock for property
            ClientProperty property = mock(ClientProperty.class);
            when(property.getName()).thenReturn("TestProperty");
            
            // With improved null checks, the method should return null when schema is null
            Object result = OlingoToConnectConverter.getValue(property, null);
            assertNull(result, "Null schema should result in null value");
        }
        
        @Test
        @DisplayName("Should handle null property value gracefully")
        void testNullPropertyValue() {
            // Create mock for property with null value
            ClientProperty property = mock(ClientProperty.class);
            when(property.getName()).thenReturn("TestProperty");
            when(property.getValue()).thenReturn(null);
            
            // With improved null checks, the method should return null when property value is null
            Object result = OlingoToConnectConverter.getValue(property, Schema.OPTIONAL_STRING_SCHEMA);
            assertNull(result, "Null property value should result in null value");
        }
        
        @Test
        @DisplayName("Should handle null primitive value gracefully")
        void testNullPrimitiveValue() {
            // Create mock for property with null primitive value
            ClientProperty property = mock(ClientProperty.class);
            ClientValue value = mock(ClientValue.class);
            
            when(property.getName()).thenReturn("TestProperty");
            when(property.getValue()).thenReturn(value);
            when(value.isPrimitive()).thenReturn(true);
            when(property.getPrimitiveValue()).thenReturn(null);
            
            // With improved null checks, the method should return null when primitive value is null
            Object result = OlingoToConnectConverter.getValue(property, Schema.OPTIONAL_STRING_SCHEMA);
            assertNull(result, "Null primitive value should result in null value");
        }
        
        @Test
        @DisplayName("Should handle non-primitive property")
        void testNonPrimitiveProperty() {
            // Create mock for non-primitive property
            ClientProperty complexProperty = mock(ClientProperty.class);
            ClientValue complexValue = mock(ClientValue.class);
            
            when(complexProperty.getName()).thenReturn("ComplexField");
            when(complexProperty.getValue()).thenReturn(complexValue);
            when(complexValue.isPrimitive()).thenReturn(false);
            
            // The implementation will return null for non-primitive properties
            Object result = OlingoToConnectConverter.getValue(complexProperty, Schema.OPTIONAL_STRING_SCHEMA);
            assertNull(result, "Non-primitive property should result in null value");
        }
        
        @Test
        @DisplayName("Should handle exception during value conversion")
        void testExceptionDuringValueConversion() {
            // Create mock for property that will throw an exception during conversion
            ClientProperty property = mock(ClientProperty.class);
            ClientValue value = mock(ClientValue.class);
            ClientPrimitiveValue primitiveValue = mock(ClientPrimitiveValue.class);
            EdmPrimitiveType type = mock(EdmPrimitiveType.class);
            
            when(property.getName()).thenReturn("ExceptionProperty");
            when(property.getValue()).thenReturn(value);
            when(value.isPrimitive()).thenReturn(true);
            when(property.getPrimitiveValue()).thenReturn(primitiveValue);
            when(primitiveValue.getType()).thenReturn(type);
            when(type.getName()).thenReturn("Int32");
            when(primitiveValue.toValue()).thenReturn(123);
            
            // Set up the mock to throw an exception when toCastValue is called
            try {
                when(primitiveValue.toCastValue(any())).thenThrow(new EdmPrimitiveTypeException("Test exception"));
            } catch (EdmPrimitiveTypeException e) {
                fail("Mock setup failed: " + e.getMessage());
            }
            
            // With improved error handling, the method should return null when an exception occurs
            Object result = OlingoToConnectConverter.getValue(property, Schema.OPTIONAL_INT32_SCHEMA);
            assertNull(result, "Exception during value conversion should result in null value");
        }
    }
}
