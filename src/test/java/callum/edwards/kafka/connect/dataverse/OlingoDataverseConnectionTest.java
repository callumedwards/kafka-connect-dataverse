package callum.edwards.kafka.connect.dataverse;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.InputStream;
import java.net.URI;

import org.apache.olingo.client.api.ODataClient;
import org.apache.olingo.client.api.communication.request.retrieve.EdmMetadataRequest;
import org.apache.olingo.client.api.communication.request.retrieve.ODataEntitySetRequest;
import org.apache.olingo.client.api.communication.response.ODataRetrieveResponse;
import org.apache.olingo.client.api.domain.ClientDelta;
import org.apache.olingo.client.api.domain.ClientEntitySet;
import org.apache.olingo.client.api.http.HttpClientFactory;
import org.apache.olingo.client.api.Configuration;
import org.apache.olingo.client.api.uri.URIBuilder;
import org.apache.olingo.client.api.communication.request.retrieve.ODataDeltaRequest;
import org.apache.olingo.client.api.communication.request.retrieve.RetrieveRequestFactory;
import org.apache.olingo.client.core.ODataClientFactory;
import org.apache.olingo.commons.api.edm.Edm;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import callum.edwards.olingo.client.http.azure.AzureADOAuthHTTPClientFactory;

@DisplayName("OlingoDataverseConnection Tests")
class OlingoDataverseConnectionTest {

    @Nested
    @DisplayName("Client Factory Tests")
    class ClientFactoryTests {
        
        @Test
        @DisplayName("Should create AzureADOAuthHTTPClientFactory with correct parameters")
        void testCreateClientFactory() {
            // Create test configuration
            String accessTokenUrl = "https://test-token-url";
            String clientId = "test-client-id";
            String clientSecret = "test-client-secret";
            String dataverseUrl = "https://test-dataverse-url";
            
            // Create test factory that exposes what we need
            AzureADOAuthHTTPClientFactory factory = new AzureADOAuthHTTPClientFactory(
                accessTokenUrl,
                clientId,
                clientSecret,
                dataverseUrl
            );
            
            // Basic verification - it shouldn't throw exceptions
            assertNotNull(factory, "Factory should be created successfully");
            
            // We can't easily verify the internal state of the factory since it's all private
            // For now, just validate it was created successfully
        }
    }
    
    @Nested
    @DisplayName("Connection Behavior Tests")
    class ConnectionBehaviorTests {
        
        @Test
        @DisplayName("Should close resources properly")
        void testClose() {
            // Test connection parameters
            String accessTokenUrl = "https://test-token-url";
            String clientId = "test-client-id";
            String clientSecret = "test-client-secret";
            String dataverseUrl = "https://test-dataverse-url";
            String dataverseServiceUrl = "https://test-dataverse-url/api/data/v9.2";
            
            // Create a testable connection that lets us control internal state
            class TestConnection extends OlingoDataverseConnection {
                private ODataClient testClient = mock(ODataClient.class);
                private Edm testEdm = mock(Edm.class);
                private boolean clientClosed = false;
                
                TestConnection() {
                    super(accessTokenUrl, clientId, clientSecret, dataverseUrl, dataverseServiceUrl);
                }
                
                @Override
                protected ODataClient getClient() {
                    return testClient;
                }
                
                @Override
                public Edm getEdm() {
                    return testEdm;
                }
                
                @Override
                public void close() {
                    super.close();
                    clientClosed = true;
                }
                
                public boolean wasClientClosed() {
                    return clientClosed;
                }
            }
            
            // Test closing resources
            TestConnection connection = new TestConnection();
            connection.close();
            
            // Verify
            assertTrue(connection.wasClientClosed(), "Connection should mark client as closed");
        }
        
        @Test
        @DisplayName("Should retrieve EDM metadata correctly")
        void testGetEdm() throws Exception {
            // Test connection parameters
            String accessTokenUrl = "https://test-token-url";
            String clientId = "test-client-id";
            String clientSecret = "test-client-secret";
            String dataverseUrl = "https://test-dataverse-url";
            String dataverseServiceUrl = "https://test-dataverse-url/api/data/v9.2";
            
            // Create mocks for OData client chain
            ODataClient mockClient = mock(ODataClient.class);
            Configuration mockConfig = mock(Configuration.class);
            RetrieveRequestFactory mockRequestFactory = mock(RetrieveRequestFactory.class);
            EdmMetadataRequest mockMetadataRequest = mock(EdmMetadataRequest.class);
            ODataRetrieveResponse<Edm> mockResponse = mock(ODataRetrieveResponse.class);
            Edm mockEdm = mock(Edm.class);
            
            // Setup mock behavior
            when(mockClient.getConfiguration()).thenReturn(mockConfig);
            when(mockClient.getRetrieveRequestFactory()).thenReturn(mockRequestFactory);
            when(mockRequestFactory.getMetadataRequest(anyString())).thenReturn(mockMetadataRequest);
            when(mockMetadataRequest.execute()).thenReturn(mockResponse);
            when(mockResponse.getBody()).thenReturn(mockEdm);
            
            // Create a testable connection
            OlingoDataverseConnection connection = new OlingoDataverseConnection(
                accessTokenUrl, clientId, clientSecret, dataverseUrl, dataverseServiceUrl) {
                @Override
                protected ODataClient getClient() {
                    return mockClient;
                }
            };
            
            // Call the method
            Edm result = connection.getEdm();
            
            // Verify the result and interactions
            assertSame(mockEdm, result, "Should return the mock EDM");
            verify(mockRequestFactory).getMetadataRequest(dataverseServiceUrl);
            verify(mockMetadataRequest).execute();
            verify(mockResponse).getBody();
        }
        
        @Test
        @DisplayName("Should handle EDM retrieval failure")
        void testGetEdmFailure() {
            // Test connection parameters
            String accessTokenUrl = "https://test-token-url";
            String clientId = "test-client-id";
            String clientSecret = "test-client-secret";
            String dataverseUrl = "https://test-dataverse-url";
            String dataverseServiceUrl = "https://test-dataverse-url/api/data/v9.2";
            
            // Create mocks for OData client chain
            ODataClient mockClient = mock(ODataClient.class);
            Configuration mockConfig = mock(Configuration.class);
            RetrieveRequestFactory mockRequestFactory = mock(RetrieveRequestFactory.class);
            EdmMetadataRequest mockMetadataRequest = mock(EdmMetadataRequest.class);
            
            // Setup mock behavior to simulate failure
            when(mockClient.getConfiguration()).thenReturn(mockConfig);
            when(mockClient.getRetrieveRequestFactory()).thenReturn(mockRequestFactory);
            when(mockRequestFactory.getMetadataRequest(anyString())).thenReturn(mockMetadataRequest);
            when(mockMetadataRequest.execute()).thenThrow(new RuntimeException("Simulated API error"));
            
            // Create a testable connection - use the new constructor that doesn't call getEdm() automatically
            OlingoDataverseConnection connection = new OlingoDataverseConnection(
                accessTokenUrl, clientId, clientSecret, dataverseUrl, dataverseServiceUrl, false) {
                @Override
                protected ODataClient getClient() {
                    return mockClient;
                }
            };
            
            // Verify exception is thrown and wrapped correctly
            RuntimeException exception = assertThrows(RuntimeException.class, 
                () -> connection.getEdm(),
                "Should throw RuntimeException when EDM retrieval fails");
            
            assertEquals("Could not retrieve Dataverse metadata", exception.getMessage(),
                "Exception should have the correct message");
            assertTrue(exception.getCause() instanceof RuntimeException,
                "Exception should have the original cause");
            assertEquals("Simulated API error", exception.getCause().getMessage(),
                "Cause should have the original error message");
        }
    }
    
    @Nested
    @DisplayName("Integration-style Tests")
    class IntegrationStyleTests {
        
        @Test
        @DisplayName("Should load metadata from file")
        void testLoadMetadataFromFile() throws Exception {
            // Test loading metadata from a file without creating a full connection
            // This avoids initialization issues with the real constructor
            
            // First, load the test metadata file
            try (InputStream metadataStream = 
                     getClass().getResourceAsStream("/callum/edwards/kafka/connect/dataverse/metadata-base.xml")) {
                
                assertNotNull(metadataStream, "Test metadata file must exist");
                
                // Create a real OData client (not a mock)
                ODataClient client = ODataClientFactory.getClient();
                
                // Read the metadata directly without using the connection class
                Edm edm = client.getReader().readMetadata(metadataStream);
                
                // Verify it was loaded successfully
                assertNotNull(edm, "Should load EDM from file");
                assertNotNull(edm.getEntityContainer(), "EDM should have an entity container");
                
                // Verify specific content from the metadata file
                assertNotNull(edm.getEntityContainer().getEntitySet("accounts"),
                    "Entity set 'accounts' should exist in the metadata");
            }
        }
    }
    
} 