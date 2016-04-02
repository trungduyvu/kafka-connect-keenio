package com.snapkitchen.kafkaconnect.keenio;

import io.keen.client.java.JavaKeenClientBuilder;
import io.keen.client.java.KeenClient;
import io.keen.client.java.KeenProject;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
/**
 * Created by tvu on 3/23/16.
 */
public class KeenIOSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(KeenIOSinkTask.class);

    private KeenClient keenClient;
    private String projectKey;
    private String writeKey;
    private String collection;
    private JSONParser jsonParser;

    public String version() {
        return AppInfoParser.getVersion();
    }

    public void start(Map<String, String> props) {
        projectKey = props.get(KeenIOSinkConnector.KEEN_IO_PROJECT_KEY_CONFIG);
        writeKey = props.get(KeenIOSinkConnector.KEEN_IO_WRITE_KEY_CONFIG);
        collection = props.get(KeenIOSinkConnector.KEEN_IO_COLLECTION_CONFIG);

        keenClient = new JavaKeenClientBuilder().build();
        KeenProject project = new KeenProject(projectKey, writeKey, null);
        keenClient.setDefaultProject(project);

        jsonParser = new JSONParser();
    }

    public void put(Collection<SinkRecord> sinkRecords) {
        for (SinkRecord record : sinkRecords) {
            try {
                JSONObject jsonObject = (JSONObject) jsonParser.parse((String) record.value());
                Map<String, Object> event = new HashMap<>();
                Iterator<?> keys = jsonObject.keySet().iterator();
                while(keys.hasNext()) {
                    String key = (String)keys.next();
                    Object value = jsonObject.get(key);
                    event.put(key, value);
                }
                keenClient.client().queueEvent(collection, event);
            } catch (ParseException e) {
                e.printStackTrace();
                continue;
            }
        }
    }

    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        keenClient.client().sendQueuedEvents();
    }

    public void stop() {

    }
}
