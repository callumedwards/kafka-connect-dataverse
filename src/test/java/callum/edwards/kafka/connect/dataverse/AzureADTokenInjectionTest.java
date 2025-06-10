package callum.edwards.kafka.connect.dataverse;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.net.URI;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HttpContext;
import org.apache.olingo.client.core.http.OAuth2Exception;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.jupiter.MockitoExtension;

import callum.edwards.olingo.client.http.azure.AzureADOAuthHTTPClientFactory;

/**
 * Tests for token injection in the AzureADOAuthHTTPClientFactory.
 * This test uses Mockito to verify the factory's behavior without requiring
 * an actual HTTP server.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("Azure AD OAuth Token Injection Tests")
public class AzureADTokenInjectionTest {
    
    @Captor
    private ArgumentCaptor<HttpRequestInterceptor> interceptorCaptor;
    
    /**
     * Test subclass of AzureADOAuthHTTPClientFactory that exposes protected methods
     * for testing purposes.
     */
    private static class TestableAzureADOAuthHTTPClientFactory extends AzureADOAuthHTTPClientFactory {
        
        public TestableAzureADOAuthHTTPClientFactory(String accessTokenURL, String clientId, 
                String secret, String resourceURI) {
            super(accessTokenURL, clientId, secret, resourceURI);
        }
        
        @Override
        public void accessToken(DefaultHttpClient client) throws OAuth2Exception {
            super.accessToken(client);
        }
        
        @Override
        public boolean isInited() throws OAuth2Exception {
            return super.isInited();
        }
    }
    
    /**
     * Tests that the factory correctly adds an authorization interceptor
     * to the HTTP client.
     * 
     * Note: This is a partial test that focuses only on verifying that
     * an interceptor is registered. Due to the way AzureADOAuthHTTPClientFactory 
     * is designed, we can't easily mock the token acquisition process
     * without extensive refactoring.
     */
    @Test
    @DisplayName("Factory should add authorization interceptor")
    void testAddAuthorizationInterceptor() throws OAuth2Exception {
        // Create test configuration
        String tokenUrl = "https://login.microsoft.com/tenant/oauth2/token";
        String clientId = "test-client-id";
        String secret = "test-client-secret";
        String resourceUrl = "https://dataverse.instance.com";
        String serviceUrl = "https://dataverse.instance.com/api/data/v9.2";
        
        // Create a mock HTTP client
        DefaultHttpClient mockClient = mock(DefaultHttpClient.class);
        
        // Create the factory with our testable subclass
        TestableAzureADOAuthHTTPClientFactory factory = new TestableAzureADOAuthHTTPClientFactory(
            tokenUrl, clientId, secret, resourceUrl
        );
        
        // Call the method that adds the interceptor
        factory.accessToken(mockClient);
        
        // Verify the client had an interceptor added
        verify(mockClient).addRequestInterceptor(interceptorCaptor.capture());
        
        // Verify the interceptor modifies the Authorization header
        HttpRequestInterceptor interceptor = interceptorCaptor.getValue();
        assertNotNull(interceptor, "An interceptor should be added");
        
        // Create a mock request and context to test the interceptor
        HttpRequest mockRequest = mock(HttpRequest.class);
        HttpContext mockContext = mock(HttpContext.class);
        
        // Execute the interceptor
        try {
            interceptor.process(mockRequest, mockContext);
            
            // Verify it removed any existing Authorization header
            verify(mockRequest).removeHeaders("Authorization");
            
            // Verify it added a new Authorization header
            verify(mockRequest).addHeader(eq("Authorization"), anyString());
        } catch (IOException | HttpException e) {
            fail("Interceptor threw an exception: " + e.getMessage());
        }
    }
} 