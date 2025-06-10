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
package callum.edwards.olingo.client.http.azure;

import org.apache.http.HttpException;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.time.Instant;
import java.util.Date;

import org.apache.olingo.client.core.http.AbstractOAuth2HttpClientFactory;
import org.apache.olingo.client.core.http.OAuth2Exception;

import com.microsoft.aad.msal4j.ClientCredentialFactory;
import com.microsoft.aad.msal4j.ClientCredentialParameters;
import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IAuthenticationResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureADOAuthHTTPClientFactory extends AbstractOAuth2HttpClientFactory {

    private final String clientId;
    private final String secret;
    private final String resourceURI;
    private volatile String token;
    private volatile Date tokenExpiresOn;

    private static final Logger log = LoggerFactory.getLogger(AzureADOAuthHTTPClientFactory.class);

    public AzureADOAuthHTTPClientFactory(final String accessTokenURL, final String clientId,
          final String secret, final String resourceURI) {
        super(null, URI.create(accessTokenURL));
        this.clientId = clientId;
        this.secret = secret;
        this.resourceURI = resourceURI;
    }
  
    @Override
    protected boolean isInited() throws OAuth2Exception {
        return token != null && (tokenExpiresOn == null || new Date().before(tokenExpiresOn));
    }

    @Override
    protected void init() throws OAuth2Exception {
        log.info("Initializing Azure AD OAuth authentication");
        getToken();        
    }

    @Override
    protected void accessToken(DefaultHttpClient client) throws OAuth2Exception {
        log.debug("Adding authorization header with bearer token");
        client.addRequestInterceptor(new HttpRequestInterceptor() {
            @Override
            public void process(final HttpRequest request, final HttpContext context) throws HttpException, IOException {
                request.removeHeaders(HttpHeaders.AUTHORIZATION);
                request.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token);
                log.trace("Added authorization header to request");
            }
        });
    }

    @Override
    protected void refreshToken(DefaultHttpClient client) throws OAuth2Exception {
        log.info("Refreshing Azure AD OAuth token");
        getToken();        
    }
    
    private synchronized void getToken() throws OAuth2Exception {
        log.info("Acquiring token from Azure AD");
        ConfidentialClientApplication app = null;
        try {
            log.debug("Building ConfidentialClientApplication with clientId: {}, authority: {}", 
                clientId, oauth2TokenServiceURI.toString());
            app = ConfidentialClientApplication.builder(
                clientId,
                ClientCredentialFactory.createFromSecret(secret))
                .authority(oauth2TokenServiceURI.toString())
                .build();
        } catch (MalformedURLException e) {
            log.error("Invalid URL for Azure AD authority: {}", oauth2TokenServiceURI, e);
            throw new OAuth2Exception(e);
        }
  
        // Adding .default is the proper way to request the default scope for a resource in Azure AD
        String scope = resourceURI + "/.default";
        log.debug("Requesting token with scope: {}", scope);
  
        ClientCredentialParameters clientCredentialParam = ClientCredentialParameters.builder(
            Collections.singleton(scope))
            .build();
  
        log.debug("Sending token request to Azure AD");
        CompletableFuture<IAuthenticationResult> future = app.acquireToken(clientCredentialParam);
      
        IAuthenticationResult result = null;
        try {
            result = future.get();
        } catch (InterruptedException e) {
            log.error("Token acquisition was interrupted", e);
            Thread.currentThread().interrupt(); // Reset interrupted status
            throw new OAuth2Exception(e);
        } catch (ExecutionException e) {
            log.error("Failed to acquire token: {}", e.getMessage(), e);
            throw new OAuth2Exception(e);
        }
        
        if (result == null) {
            log.error("Received null authentication result");
            throw new OAuth2Exception("Received null authentication result");
        }
        
        token = result.accessToken();
        tokenExpiresOn = result.expiresOnDate(); 
        
        // Calculate token lifetime in minutes for logging
        long expiresInMinutes = 0;
        if (tokenExpiresOn != null) {
            long expiresInMillis = tokenExpiresOn.getTime() - System.currentTimeMillis();
            expiresInMinutes = expiresInMillis / (60 * 1000);
        }
        
        log.info("Token acquired successfully, expires in {} minutes", expiresInMinutes);
        log.debug("Token expiration: {}", tokenExpiresOn);
    }
}
