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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataverseSourceConnector extends SourceConnector {
    private static final Logger log = LoggerFactory.getLogger(DataverseSourceConnector.class);
    
    private Map<String, String> configProps;
    private DataverseSourceConnectorConfig config;

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting Dataverse Source Connector");
        configProps = props;
        config = new DataverseSourceConnectorConfig(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return DataverseSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<String> tables = config.getTables();
        int numTasks = Math.min(tables.size(), maxTasks);
        
        List<Map<String, String>> taskConfigs = new ArrayList<>(numTasks);
        
        // If we have fewer tables than maxTasks, create one task per table
        if (tables.size() <= maxTasks) {
            for (String table : tables) {
                Map<String, String> taskConfig = new HashMap<>(configProps);
                taskConfig.put("table", table);
                taskConfigs.add(taskConfig);
                log.info("Created task for table: {}", table);
            }
        } else {
            // If we have more tables than maxTasks, distribute tables across tasks
            int tablesPerTask = (int) Math.ceil((double) tables.size() / numTasks);
            for (int i = 0; i < numTasks; i++) {
                int startIndex = i * tablesPerTask;
                int endIndex = Math.min(startIndex + tablesPerTask, tables.size());
                
                if (startIndex < tables.size()) {
                    Map<String, String> taskConfig = new HashMap<>(configProps);
                    
                    // Create a comma-separated list of tables for this task
                    StringBuilder tableList = new StringBuilder();
                    for (int j = startIndex; j < endIndex; j++) {
                        if (tableList.length() > 0) {
                            tableList.append(",");
                        }
                        tableList.append(tables.get(j));
                    }
                    
                    taskConfig.put("task.tables", tableList.toString());
                    taskConfigs.add(taskConfig);
                    log.info("Created task for tables: {}", tableList);
                }
            }
        }
        
        return taskConfigs;
    }

    @Override
    public void stop() {
        // Nothing to do here
    }

    @Override
    public ConfigDef config() {
        return DataverseSourceConnectorConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }
}
