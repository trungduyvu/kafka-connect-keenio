/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.snapkitchen.kafkaconnect.keenio;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Sink connector that send kafka data to keen io
 */
public class KeenIOSinkConnector extends SinkConnector {

    public static final String KEEN_IO_COLLECTION_CONFIG = "keen_io_collection";
    public static final String KEEN_IO_PROJECT_KEY_CONFIG = "keen_io_project_key";
    public static final String KEEN_IO_WRITE_KEY_CONFIG = "keen_io_write_key";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(KEEN_IO_COLLECTION_CONFIG, Type.STRING, Importance.HIGH, "Keen IO collection name")
            .define(KEEN_IO_PROJECT_KEY_CONFIG, Type.STRING, Importance.HIGH, "Keen IO project key")
            .define(KEEN_IO_WRITE_KEY_CONFIG, Type.STRING, Importance.HIGH, "Keen IO write key");

    private String collection;
    private String projectKey;
    private String writeKey;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        collection = props.get(KEEN_IO_COLLECTION_CONFIG);
        projectKey = props.get(KEEN_IO_PROJECT_KEY_CONFIG);
        writeKey = props.get(KEEN_IO_WRITE_KEY_CONFIG);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return KeenIOSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>();
            if (collection != null){
                config.put(KEEN_IO_COLLECTION_CONFIG, collection);
            }
            if (collection != null) {
                config.put(KEEN_IO_WRITE_KEY_CONFIG, writeKey);
            }
            if (collection != null) {
                config.put(KEEN_IO_PROJECT_KEY_CONFIG, projectKey);
            }
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {
        // nothing to do?
    }

    public ConfigDef config() {
        return CONFIG_DEF;
    }
}