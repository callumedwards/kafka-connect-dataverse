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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class VersionUtil {
    private static final Logger log = LoggerFactory.getLogger(VersionUtil.class);
    private static final String VERSION_FILE = "version.properties";
    private static final String VERSION_PROPERTY = "version";
    private static String version = "unknown";
    
    static {
        try (InputStream stream = VersionUtil.class.getClassLoader().getResourceAsStream(VERSION_FILE)) {
            Properties props = new Properties();
            props.load(stream);
            version = props.getProperty(VERSION_PROPERTY, version);
        } catch (Exception e) {
            log.warn("Error while loading version:", e);
        }
    }
    
    public static String getVersion() {
        return version;
    }
}
