package com.snapkitchen.kafkaconnect.keenio;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;

/**
 * Created by tvu on 3/23/16.
 */
public class KeenIOSinkTask extends SinkTask {
    public String version() {
        return null;
    }

    public void start(Map<String, String> map) {

    }

    public void put(Collection<SinkRecord> collection) {

    }

    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {

    }

    public void stop() {

    }
}
