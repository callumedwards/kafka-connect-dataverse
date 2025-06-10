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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.List;
import java.util.Map;

public class DataverseSourceConnectorConfig extends AbstractConfig {

    public static final String DATAVERSE_URL_CONFIG = "dataverse.url";
    private static final String DATAVERSE_URL_DOC = "Dataverse API URL";

    public static final String DATAVERSE_LOGIN_ACCESS_TOKEN_URL_CONFIG = "dataverse.login.accessTokenURL";
    private static final String DATAVERSE_LOGIN_ACCESS_TOKEN_URL_DOC = "The URL for the access token, e.g. https://login.microsoftonline.com/{tenant}/";

    public static final String DATAVERSE_LOGIN_CLIENT_ID_CONFIG = "dataverse.login.clientId";
    private static final String DATAVERSE_LOGIN_CLIENT_ID_DOC = "The client ID for the access token";

    public static final String DATAVERSE_LOGIN_SECRET_CONFIG = "dataverse.login.secret";
    private static final String DATAVERSE_LOGIN_SECRET_DOC = "The client secret for the access token";

    public static final String TOPIC_PREFIX_CONFIG = "topic.prefix";
    private static final String TOPIC_PREFIX_DOC = "The prefix for topics where data will be published. Table names will be appended to this prefix.";

    public static final String TABLES_CONFIG = "dataverse.tables";
    private static final String TABLES_DOC = "Comma-separated list of Dataverse tables to synchronize";

    public static final String POLL_INTERVAL_MS_CONFIG = "poll.interval.ms";
    private static final String POLL_INTERVAL_MS_DOC = "How often to poll the Dataverse API in milliseconds";
    private static final int POLL_INTERVAL_MS_DEFAULT = 60000; // 1 minute default
    
    public static final String MAX_PAGE_SIZE_CONFIG = "dataverse.max.page.size";
    private static final String MAX_PAGE_SIZE_DOC = "Maximum number of records to retrieve per page in OData requests";
    private static final int MAX_PAGE_SIZE_DEFAULT = 5000; // Default to 5000 as per OlingoDataverseConnection
    
    public static final String BATCH_SIZE_CONFIG = "dataverse.batch.size";
    private static final String BATCH_SIZE_DOC = "Maximum number of records to process in a single batch before returning results";
    private static final int BATCH_SIZE_DEFAULT = 5000; // Default to 5000 as per OlingoDataverseConnection

    public static final String SKIP_INITIAL_LOAD_CONFIG = "dataverse.skip.initial.load";
    private static final String SKIP_INITIAL_LOAD_DOC = "If true, skip the initial full table load and only track delta changes going forward";
    private static final boolean SKIP_INITIAL_LOAD_DEFAULT = false;

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(DATAVERSE_URL_CONFIG, Type.STRING, Importance.HIGH, DATAVERSE_URL_DOC)
            .define(DATAVERSE_LOGIN_ACCESS_TOKEN_URL_CONFIG, Type.STRING, Importance.HIGH, DATAVERSE_LOGIN_ACCESS_TOKEN_URL_DOC)
            .define(DATAVERSE_LOGIN_CLIENT_ID_CONFIG, Type.STRING, Importance.HIGH, DATAVERSE_LOGIN_CLIENT_ID_DOC)
            .define(DATAVERSE_LOGIN_SECRET_CONFIG, Type.PASSWORD, Importance.HIGH, DATAVERSE_LOGIN_SECRET_DOC)
            .define(TOPIC_PREFIX_CONFIG, Type.STRING, Importance.HIGH, TOPIC_PREFIX_DOC)
            .define(TABLES_CONFIG, Type.LIST, Importance.HIGH, TABLES_DOC)
            .define(POLL_INTERVAL_MS_CONFIG, Type.INT, POLL_INTERVAL_MS_DEFAULT, Importance.MEDIUM, POLL_INTERVAL_MS_DOC)
            .define(MAX_PAGE_SIZE_CONFIG, Type.INT, MAX_PAGE_SIZE_DEFAULT, Importance.MEDIUM, MAX_PAGE_SIZE_DOC)
            .define(BATCH_SIZE_CONFIG, Type.INT, BATCH_SIZE_DEFAULT, Importance.MEDIUM, BATCH_SIZE_DOC)
            .define(SKIP_INITIAL_LOAD_CONFIG, Type.BOOLEAN, SKIP_INITIAL_LOAD_DEFAULT, Importance.MEDIUM, SKIP_INITIAL_LOAD_DOC);

    public DataverseSourceConnectorConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
    }

    public String getDataverseUrl() {
        return getString(DATAVERSE_URL_CONFIG);
    }

    public String getAccessTokenUrl() {
        return getString(DATAVERSE_LOGIN_ACCESS_TOKEN_URL_CONFIG);
    }

    public String getClientId() {
        return getString(DATAVERSE_LOGIN_CLIENT_ID_CONFIG);
    }

    public String getClientSecret() {
        return getPassword(DATAVERSE_LOGIN_SECRET_CONFIG).value();
    }

    public String getTopicPrefix() {
        return getString(TOPIC_PREFIX_CONFIG);
    }

    public List<String> getTables() {
        return getList(TABLES_CONFIG);
    }

    public int getPollIntervalMs() {
        return getInt(POLL_INTERVAL_MS_CONFIG);
    }
    
    public int getMaxPageSize() {
        return getInt(MAX_PAGE_SIZE_CONFIG);
    }
    
    public int getBatchSize() {
        return getInt(BATCH_SIZE_CONFIG);
    }
    
    public boolean skipInitialLoad() {
        return getBoolean(SKIP_INITIAL_LOAD_CONFIG);
    }
    
    /**
     * Gets the full topic name for a specific table
     * 
     * @param tableName The name of the table
     * @return The full topic name (prefix + table name)
     */
    public String getTopicForTable(String tableName) {
        String prefix = getTopicPrefix();
        // Ensure we don't double up on separators
        if (prefix.endsWith(".") || prefix.endsWith("-") || prefix.endsWith("_")) {
            return prefix + tableName;
        } else {
            return prefix + "." + tableName;
        }
    }

    public String getDataverseServiceURL() {
        return getDataverseUrl() + "/api/data/v9.2/";
    }
}
