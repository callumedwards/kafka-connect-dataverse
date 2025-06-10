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
import java.util.List;

import org.apache.olingo.client.api.ODataClient;
import org.apache.olingo.client.api.communication.ODataClientErrorException;
import org.apache.olingo.client.api.communication.request.retrieve.EdmMetadataRequest;
import org.apache.olingo.client.api.communication.request.retrieve.ODataDeltaRequest;
import org.apache.olingo.client.api.communication.request.retrieve.ODataEntitySetRequest;
import org.apache.olingo.client.api.communication.response.ODataRetrieveResponse;
import org.apache.olingo.client.api.domain.ClientDelta;
import org.apache.olingo.client.api.domain.ClientDeletedEntity;
import org.apache.olingo.client.api.domain.ClientEntity;
import org.apache.olingo.client.api.domain.ClientEntitySet;
import org.apache.olingo.client.core.ODataClientFactory;
import org.apache.olingo.commons.api.edm.Edm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import callum.edwards.olingo.client.http.azure.AzureADOAuthHTTPClientFactory;

/**
 * OlingoDataverseConnection provides a connectivity layer between Kafka Connect and Microsoft Dataverse.
 * 
 * This class uses Apache Olingo's OData client to communicate with Dataverse, handling authentication
 * via Azure AD OAuth, retrieving metadata, and performing data operations like initial loads and 
 * delta (change tracking) loads.
 * 
 * The connection maintains the OData client instance and Entity Data Model (EDM) metadata to
 * optimize performance by avoiding unnecessary initialization costs on repeated operations.
 */
public class OlingoDataverseConnection {
    static final Logger log = LoggerFactory.getLogger(OlingoDataverseConnection.class);

    private ODataClient client;
    private Edm edm;
    
    private final String accessTokenUrl;
    private final String clientId;
    private final String clientSecret;
    private final String dataverseUrl;
    private final String dataverseServiceUrl;
    private int maxPageSize = 5000;
    private int batchSize = 5000;
    
    /**
     * Creates a new connection and automatically initializes the client and EDM.
     * This is the standard constructor for normal operation.
     * 
     * @param accessTokenUrl The Azure AD OAuth token URL
     * @param clientId The Azure AD client ID
     * @param clientSecret The Azure AD client secret
     * @param dataverseUrl The base Dataverse URL
     * @param dataverseServiceUrl The Dataverse OData service endpoint URL
     */
    public OlingoDataverseConnection(
            String accessTokenUrl,
            String clientId,
            String clientSecret,
            String dataverseUrl,
            String dataverseServiceUrl) {      
        this.accessTokenUrl = accessTokenUrl;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.dataverseUrl = dataverseUrl;
        this.dataverseServiceUrl = dataverseServiceUrl;
        
        // Initialize client and read EDM
        getClient();
        getEdm();
    }
    
    /**
     * Gets the maximum page size used for pagination in OData requests.
     * This value determines how many entities are requested in a single page.
     * Default value is 5000.
     * 
     * @return The current maximum page size
     */
    public int getMaxPageSize() {
        return maxPageSize;
    }
    
    /**
     * Sets the maximum page size used for pagination in OData requests.
     * This value determines how many entities are requested in a single page.
     * 
     * @param maxPageSize The maximum page size to set (must be greater than 0)
     * @throws IllegalArgumentException if maxPageSize is less than or equal to 0
     */
    public void setMaxPageSize(int maxPageSize) {
        if (maxPageSize <= 0) {
            throw new IllegalArgumentException("maxPageSize must be greater than 0");
        }
        log.info("Setting maxPageSize to: {}", maxPageSize);
        this.maxPageSize = maxPageSize;
    }
    
    /**
     * Gets the batch size used for processing entities in chunks.
     * This value determines the maximum number of entities to process before
     * returning a partial result set with the next link as delta link.
     * Default value is 5000.
     * 
     * @return The current batch size
     */
    public int getBatchSize() {
        return batchSize;
    }
    
    /**
     * Sets the batch size used for processing entities in chunks.
     * This value determines the maximum number of entities to process before
     * returning a partial result set with the next link as delta link.
     * 
     * @param batchSize The batch size to set (must be greater than 0)
     * @throws IllegalArgumentException if batchSize is less than or equal to 0
     */
    public void setBatchSize(int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than 0");
        }
        log.info("Setting batchSize to: {}", batchSize);
        this.batchSize = batchSize;
    }
    
    /**
     * Creates a new connection but does not automatically initialize the EDM.
     * This constructor is primarily intended for testing purposes when you want more
     * control over when the EDM is loaded, or to test failure scenarios.
     * 
     * @param accessTokenUrl The Azure AD OAuth token URL
     * @param clientId The Azure AD client ID
     * @param clientSecret The Azure AD client secret
     * @param dataverseUrl The base Dataverse URL
     * @param dataverseServiceUrl The Dataverse OData service endpoint URL
     * @param initializeClient Whether to initialize the client (but not EDM)
     */
    OlingoDataverseConnection(
            String accessTokenUrl,
            String clientId,
            String clientSecret,
            String dataverseUrl,
            String dataverseServiceUrl,
            boolean initializeClient) {
        this.accessTokenUrl = accessTokenUrl;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.dataverseUrl = dataverseUrl;
        this.dataverseServiceUrl = dataverseServiceUrl;
        
        if (initializeClient) {
            getClient();
        }
    }
    
    /**
     * Gets or initializes the OData client for communicating with Dataverse.
     * If the client is already initialized, the existing instance is returned.
     * Otherwise, a new client is created and configured with Azure AD authentication.
     * 
     * @return The configured ODataClient instance
     */
    protected ODataClient getClient() {
        if (client == null) {
          log.info("Initializing new OData client");
          // Create a new OData client
          client = ODataClientFactory.getClient();
          
          // Configure the client to use Azure AD OAuth authentication
          log.debug("Configuring OAuth authentication with accessTokenUrl: {}, clientId: {}, resourceUri: {}", 
                accessTokenUrl, clientId, dataverseUrl);
          client.getConfiguration().setHttpClientFactory(new 
              AzureADOAuthHTTPClientFactory(accessTokenUrl,
              clientId,
              clientSecret,
              dataverseUrl));
          
          log.debug("OData client initialized with Azure AD authentication");
        }
        return client;
    }

    /**
     * Retrieves the Entity Data Model (EDM) metadata from Dataverse.
     * If the EDM is already loaded, the existing instance is returned.
     * Otherwise, a metadata request is made to the Dataverse service.
     * 
     * The EDM contains information about entities, their properties, relationships,
     * and other metadata that is essential for working with Dataverse data.
     * 
     * @return The Edm metadata object representing the Dataverse data model
     * @throws RuntimeException If the metadata cannot be retrieved from Dataverse
     */
    public Edm getEdm() {
      if (edm == null) {
          try {
              // Get the OData client
              ODataClient client = getClient();
              
              log.info("Retrieving EDM metadata from {}", dataverseServiceUrl);
              // Create and execute a metadata request
              EdmMetadataRequest request = client.getRetrieveRequestFactory().getMetadataRequest(dataverseServiceUrl);
              ODataRetrieveResponse<Edm> response = request.execute();
              
              // Store the EDM for future use
              edm = response.getBody();
              log.info("Successfully retrieved EDM metadata from {}", dataverseServiceUrl);
          } catch (Exception e) {
              log.error("Failed to retrieve EDM metadata from {}: {}", dataverseServiceUrl, e.getMessage(), e);
              throw new RuntimeException("Could not retrieve Dataverse metadata", e);
          }
      }
      return edm;
    }

    /**
     * Performs an initial load of data from a Dataverse table.
     * 
     * This method retrieves entities from the specified table and sets up change tracking
     * by requesting a delta link from Dataverse. The delta link can be used in subsequent
     * requests to retrieve only changes that have occurred since this initial load.
     * 
     * This implementation handles pagination automatically by following the nextLink when
     * the number of results exceeds Dataverse's page size limit (typically 5,000 records).
     * 
     * @param table The name of the Dataverse table (entity set) to load
     * @return A ClientEntitySet containing the loaded entities and a delta link for future change tracking
     */
    public ClientEntitySet initialLoad(String table) {
        log.info("Performing initial load for table: {}", table);
        
        // Build the URI for the entity set (table)
        URI absoluteUri = client.newURIBuilder(dataverseServiceUrl)
            .appendEntitySetSegment(table)
            .build();
        log.debug("Initial load URI: {}", absoluteUri);
        
        // Create a request for the entity set with change tracking enabled
        ODataEntitySetRequest<ClientEntitySet> request = client.getRetrieveRequestFactory().getEntitySetRequest(absoluteUri);    
        
        // Get the preferences as strings and combine them
        String trackChangesPreference = client.newPreferences().trackChanges();
        String maxPageSizePreference = client.newPreferences().maxPageSize(maxPageSize);
        String combinedPreferences = trackChangesPreference + "," + maxPageSizePreference;
        
        // Set the combined preferences
        request.setPrefer(combinedPreferences);
        
        // Execute the request
        log.debug("Executing initial load request with combined preferences: {}", combinedPreferences);
        ODataRetrieveResponse<ClientEntitySet> response = request.execute();
        ClientEntitySet entitySet = response.getBody();
        log.info("Initial load for table {} - received {} entities", table, entitySet.getEntities().size());
        
        // Check for delta link
        if (entitySet.getDeltaLink() != null) {
            log.debug("Received delta link: {}", entitySet.getDeltaLink());
        } else {
            log.warn("No delta link received for table {}", table);
        }
        // Check if we have more pages to fetch (pagination handling)
        URI nextLink = entitySet.getNext();
        if (nextLink != null) {            
            log.info("Found nextLink in response, handling pagination for table {}", table);
            if (entitySet.getEntities().size() > batchSize) {
                log.info("Batch size limit reached, returning current entity set and setting delta link to next link");
                entitySet.setDeltaLink(entitySet.getNext());
                return entitySet;
            }
            ClientEntitySet remainingEntities = fetchRemainingPages(nextLink, entitySet.getEntities().size());
            entitySet.getEntities().addAll(remainingEntities.getEntities());
            entitySet.setDeltaLink(remainingEntities.getDeltaLink());
        }
        
        return entitySet;
    }

    /**
     * Fetches remaining pages of data when a response is paginated.
     * 
     * This method follows the nextLink provided by Dataverse to retrieve additional
     * pages of data until all records have been collected.
     * 
     * @param entitySet The initial entity set that will be populated with additional entities
     * @param nextLink The URI to the next page of results
     * @param currentBatchSize The current size of the batch
     */
    private ClientEntitySet fetchRemainingPages(URI pageLink, int currentBatchSize) {
        
        // Create a request for the next page
        ODataEntitySetRequest<ClientEntitySet> request = client.getRetrieveRequestFactory()
            .getEntitySetRequest(pageLink);
            
        // Apply max page size preference
        String maxPageSizePreference = client.newPreferences().maxPageSize(maxPageSize);
        request.setPrefer(maxPageSizePreference);
        
        // Execute the request
        ClientEntitySet entitySet = null;
        try {
            ODataRetrieveResponse<ClientEntitySet> response = request.execute();
            entitySet = response.getBody();
        }
        catch (ODataClientErrorException e)  {
            if (e.getStatusLine().getStatusCode() == 429) {
                log.warn("Received throttling response (HTTP 429) from Dataverse API during pagination");
                //create a new empty entity set with the same next link. This will be returned to the caller
                //and the next link will propogate back to the caller and stored for the next poll attempt.
                entitySet = client.getObjectFactory().newEntitySet();
                entitySet.setDeltaLink(pageLink);
                return entitySet;
            }
            throw e;
        }
        
        if (entitySet.getDeltaLink() != null) {
            log.debug("Next delta link: {}", entitySet.getDeltaLink());
            return entitySet;
        }
        
        // Check if we have more pages to fetch (pagination handling)
        URI nextLink = entitySet.getNext();
        if (nextLink != null) {            
            log.info("Found nextLink in response, handling pagination: {}", nextLink);
            int newBatchSize = currentBatchSize + entitySet.getEntities().size();
            if (newBatchSize > batchSize) {
                log.info("Batch size limit reached, returning current entity set and setting delta link to next link");
                entitySet.setDeltaLink(entitySet.getNext());
                return entitySet;
            }
            ClientEntitySet remainingEntities = fetchRemainingPages(nextLink, newBatchSize);
            entitySet.getEntities().addAll(remainingEntities.getEntities());
            entitySet.setDeltaLink(remainingEntities.getDeltaLink());
        }

        return entitySet;
    }

    /**
     * Performs a delta (incremental) load using a previously obtained delta link.
     * 
     * This method retrieves only the changes (new, modified, or deleted entities) that
     * have occurred since the delta link was generated. The resulting ClientDelta
     * contains both the changed entities and a new delta link for future requests.
     * 
     * This implementation handles pagination automatically by following the nextLink when
     * the number of results exceeds Dataverse's page size limit (typically 5,000 records).
     * 
     * @param deltaUri The delta link URI from a previous initial or delta load
     * @return A ClientDelta containing changed entities, deleted entities, and a new delta link
     */
    public ClientDelta deltaLoad(URI deltaUri) {
        return deltaLoad(deltaUri, 0);
    }

    private ClientDelta deltaLoad(URI deltaUri, int currentBatchSize) {
        log.info("Performing delta load with URI: {}", deltaUri);
        
        // Execute the delta request using the provided URI
        log.debug("Executing delta request");
        ODataDeltaRequest request = client.getRetrieveRequestFactory().getDeltaRequest(deltaUri);
        
        // Apply max page size preference
        String maxPageSizePreference = client.newPreferences().maxPageSize(maxPageSize);
        request.setPrefer(maxPageSizePreference);
        
        log.debug("Executing delta request with preference: {}", maxPageSizePreference);
        ClientDelta delta = null;
        try {
            delta = request.execute().getBody();
        }
        catch (ODataClientErrorException e) {
            if (e.getStatusLine().getStatusCode() == 429) {
                log.warn("Received throttling response (HTTP 429) from Dataverse API during delta load");
                // Create an empty delta to return with the current delta link preserved for the next poll attempt    
                ClientDelta emptyDelta = client.getObjectFactory().newDelta();
                emptyDelta.setDeltaLink(deltaUri);
                return emptyDelta;
            }
            throw e;
        }
        log.info("Delta load result: {} entities and {} deleted entities", 
        delta.getEntities().size(), delta.getDeletedEntities().size());
        
        // Check for new delta link
        if (delta.getDeltaLink() != null) {
            log.debug("Received new delta link: {}", delta.getDeltaLink());
        } else {
            log.warn("No delta link received in delta response");
        }
        
        // Check if we have more pages to fetch (pagination handling)
        URI nextLink = delta.getNext();
        if (nextLink != null) {
            log.info("Found nextLink in delta response, handling pagination");
            int newBatchSize = currentBatchSize + delta.getEntities().size() + delta.getDeletedEntities().size();
            if (newBatchSize > batchSize) {
                log.info("Batch size limit reached, returning current delta");
                // set the delta link to the next link so we can continue from where we left off
                delta.setDeltaLink(nextLink);
            } else {
                log.debug("Batch size limit not reached, fetching next delta page");
                ClientDelta nextDelta = deltaLoad(nextLink, newBatchSize);
                delta.getEntities().addAll(nextDelta.getEntities());
                delta.getDeletedEntities().addAll(nextDelta.getDeletedEntities());
                delta.setDeltaLink(nextDelta.getDeltaLink());
            }
        }        
        return delta;
    }

    /**
     * Gets only the delta link for a table without retrieving all entities.
     * This is useful when you only want to track changes going forward and don't need initial data.
     * 
     * The method tries two approaches to get a delta link:
     * 1. Request a delta link directly from the Dataverse API
     * 2. If no delta link is provided but a next link is available, it will parse the next link to
     *    extract the necessary information to generate a delta link
     *
     * @param table The name of the Dataverse table to get a delta link for
     * @return A URI representing the delta link, or null if no delta link could be obtained
     * @throws RuntimeException if the delta link cannot be retrieved or generated
     */
    public URI getDeltaLinkOnly(String table) {
        log.info("Getting delta link only for table: {}", table);
        
        // Build the URI for the entity set (table)
        URI absoluteUri = client.newURIBuilder(dataverseServiceUrl)
            .appendEntitySetSegment(table)
            .build();
        log.debug("Delta link request URI: {}", absoluteUri);
        
        // Create a request for the entity set with change tracking enabled
        ODataEntitySetRequest<ClientEntitySet> request = client.getRetrieveRequestFactory().getEntitySetRequest(absoluteUri);    
        
        // Get the preferences as strings and combine them
        String trackChangesPreference = client.newPreferences().trackChanges();
        String maxPageSizePreference = client.newPreferences().maxPageSize(1);
        String combinedPreferences = trackChangesPreference + "," + maxPageSizePreference;
        
        // Set the combined preferences
        request.setPrefer(combinedPreferences);
        
        // Execute the request
        log.debug("Executing delta link request with tracking preference: {}", trackChangesPreference);
        ODataRetrieveResponse<ClientEntitySet> response = request.execute();
        ClientEntitySet entitySet = response.getBody();
        
        // Check if we received a delta link directly
        if (entitySet.getDeltaLink() != null) {
            log.debug("Received delta link directly: {}", entitySet.getDeltaLink());
            return entitySet.getDeltaLink();
        }
        
        // If no direct delta link but there's a next link, try to generate a delta link from it
        URI nextLink = entitySet.getNext();
        if (nextLink != null) {            
            log.info("No direct delta link, attempting to generate one from next link: {}", nextLink);
            
            try {
                // Generate a delta link from the next link
                URI deltaLink = generateDeltaLinkFromNextLink(nextLink);
                log.info("Generated delta link: {}", deltaLink);
                return deltaLink;
            } catch (Exception e) {
                log.error("Failed to generate delta link from next link: {}", nextLink, e);
                throw new RuntimeException("Could not generate delta link from next link", e);
            }
        }
        
        // If we got here, we couldn't get or generate a delta link
        log.warn("No delta link or next link received for table {}", table);
        return null;
    }
    
    /**
     * Generates a delta link from a next link by parsing the skiptoken parameter.
     * 
     * The next link contains a skiptoken parameter which includes a pagingcookie.
     * This pagingcookie contains an XML-formatted cookie with a maxTimestamp value.
     * This method extracts the maxTimestamp and uses it to generate a delta link.
     * 
     * @param nextLink The next link URI from which to extract information
     * @return A URI representing the delta link
     * @throws Exception if parsing fails or required information cannot be extracted
     */
    private URI generateDeltaLinkFromNextLink(URI nextLink) throws Exception {
        String nextLinkStr = nextLink.toString();
        log.debug("Parsing next link: {}", nextLinkStr);
        
        // Extract the skiptoken parameter
        int skipTokenStart = nextLinkStr.indexOf("$skiptoken=");
        if (skipTokenStart == -1) {
            throw new Exception("No skiptoken parameter found in next link");
        }
        
        String skipToken = nextLinkStr.substring(skipTokenStart + 11); // +11 to skip "$skiptoken="
        // If there are other parameters after skiptoken, remove them
        int andIndex = skipToken.indexOf("&");
        if (andIndex != -1) {
            skipToken = skipToken.substring(0, andIndex);
        }
        
        // URL decode the skiptoken
        skipToken = java.net.URLDecoder.decode(skipToken, "UTF-8");
        log.debug("Extracted skiptoken: {}", skipToken);
        
        // Extract the pagingcookie
        int pagingCookieStart = skipToken.indexOf("pagingcookie=");
        if (pagingCookieStart == -1) {
            throw new Exception("No pagingcookie found in skiptoken");
        }
        
        int pagingCookieValueStart = skipToken.indexOf('"', pagingCookieStart) + 1;
        int pagingCookieValueEnd = skipToken.indexOf('"', pagingCookieValueStart);
        String pagingCookie = skipToken.substring(pagingCookieValueStart, pagingCookieValueEnd);
        
        // URL decode the pagingcookie
        pagingCookie = java.net.URLDecoder.decode(pagingCookie, "UTF-8");
        log.debug("Extracted pagingcookie: {}", pagingCookie);
        
        // Extract the maxTimestamp from the cookie XML
        int maxTimestampStart = pagingCookie.indexOf("maxTimestamp=");
        if (maxTimestampStart == -1) {
            throw new Exception("No maxTimestamp found in pagingcookie");
        }
        
        int maxTimestampValueStart = pagingCookie.indexOf('"', maxTimestampStart) + 1;
        int maxTimestampValueEnd = pagingCookie.indexOf('"', maxTimestampValueStart);
        String maxTimestamp = pagingCookie.substring(maxTimestampValueStart, maxTimestampValueEnd);
        log.debug("Extracted maxTimestamp: {}", maxTimestamp);
        
        // Generate the current date in the required format (MM/dd/yyyy HH:mm:ss)
        java.text.SimpleDateFormat dateFormat = new java.text.SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String currentDate = dateFormat.format(new java.util.Date());
        
        // Construct the delta token
        String deltaToken = maxTimestamp + "!" + currentDate;
        log.debug("Generated delta token: {}", deltaToken);
        
        // Extract the base URL from the next link
        String baseUrl = nextLinkStr.substring(0, skipTokenStart);
        
        // Replace skiptoken with deltatoken
        String deltaLinkStr = baseUrl + "$deltatoken=" + java.net.URLEncoder.encode(deltaToken, "UTF-8");
        log.debug("Generated delta link string: {}", deltaLinkStr);
        
        return new URI(deltaLinkStr);
    }

    /**
     * Closes the connection and releases associated resources.
     * 
     * This method should be called when the connection is no longer needed
     * to ensure proper cleanup of resources.
     */
    public void close() {
       if (client != null) {
           try {
               // Release references to client and EDM to allow garbage collection
               client = null;
               edm = null;
               log.info("Closed Dataverse connection");
           } catch (Exception e) {
               log.error("Error closing Dataverse connection", e);
           }
       }
   }
}
