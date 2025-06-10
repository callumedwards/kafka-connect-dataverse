package callum.edwards.kafka.connect.dataverse;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to load and access configuration for integration tests.
 * 
 * This class loads configuration from a properties file which contains
 * connection details for a real Dataverse instance. The file path can be
 * specified via the system property 'integration.test.config' or will
 * default to looking for 'integration-test.properties' in the classpath.
 */
public class IntegrationTestConfig {
    private static final Logger log = LoggerFactory.getLogger(IntegrationTestConfig.class);
    private static final String DEFAULT_CONFIG_PATH = "integration-test.properties";
    private static final String CONFIG_SYSTEM_PROPERTY = "integration.test.config";
    
    private static Properties properties;
    
    private IntegrationTestConfig() {
        // Private constructor to prevent instantiation
    }
    
    /**
     * Loads the integration test configuration.
     * 
     * @return Properties containing the integration test configuration
     * @throws IOException if loading the configuration fails
     */
    public static synchronized Properties getConfig() throws IOException {
        if (properties == null) {
            properties = new Properties();
            
            // Try to load from system property path first
            String configPath = System.getProperty(CONFIG_SYSTEM_PROPERTY);
            if (configPath != null) {
                Path path = Paths.get(configPath);
                if (Files.exists(path)) {
                    try (InputStream is = new FileInputStream(path.toFile())) {
                        properties.load(is);
                        log.info("Loaded integration test configuration from {}", configPath);
                        return properties;
                    }
                } else {
                    log.warn("Specified configuration file not found: {}", configPath);
                }
            }
            
            // Fall back to classpath
            try (InputStream is = IntegrationTestConfig.class.getClassLoader().getResourceAsStream(DEFAULT_CONFIG_PATH)) {
                if (is != null) {
                    properties.load(is);
                    log.info("Loaded integration test configuration from classpath: {}", DEFAULT_CONFIG_PATH);
                } else {
                    throw new IOException("Integration test configuration file not found in classpath: " + DEFAULT_CONFIG_PATH);
                }
            }
        }
        
        return properties;
    }
    
    /**
     * Gets a property value from the configuration.
     * 
     * @param key The property key
     * @return The property value, or null if not found
     */
    public static String getProperty(String key) {
        try {
            return getConfig().getProperty(key);
        } catch (IOException e) {
            log.error("Failed to load configuration", e);
            return null;
        }
    }
    
    /**
     * Gets a property value from the configuration with a default value.
     * 
     * @param key The property key
     * @param defaultValue The default value to return if the property is not found
     * @return The property value, or the default value if not found
     */
    public static String getProperty(String key, String defaultValue) {
        try {
            return getConfig().getProperty(key, defaultValue);
        } catch (IOException e) {
            log.error("Failed to load configuration", e);
            return defaultValue;
        }
    }
    
    /**
     * Gets an integer property value from the configuration.
     * 
     * @param key The property key
     * @param defaultValue The default value to return if the property is not found or not a valid integer
     * @return The property value as an integer, or the default value if not found or not a valid integer
     */
    public static int getIntProperty(String key, int defaultValue) {
        String value = getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            log.warn("Property {} is not a valid integer: {}", key, value);
            return defaultValue;
        }
    }
    
    /**
     * Checks if all required properties are present in the configuration.
     * 
     * @param requiredProperties Array of required property keys
     * @return true if all required properties are present, false otherwise
     */
    public static boolean hasRequiredProperties(String... requiredProperties) {
        try {
            Properties props = getConfig();
            for (String key : requiredProperties) {
                if (!props.containsKey(key) || props.getProperty(key).trim().isEmpty()) {
                    log.error("Required property missing or empty: {}", key);
                    return false;
                }
            }
            return true;
        } catch (IOException e) {
            log.error("Failed to load configuration", e);
            return false;
        }
    }
} 