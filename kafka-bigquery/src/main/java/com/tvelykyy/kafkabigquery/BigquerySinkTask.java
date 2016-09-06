package com.tvelykyy.kafkabigquery;

import com.tvelykyy.bigquery.BigqueryClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class BigquerySinkTask extends SinkTask {
    private static final Logger LOG = LoggerFactory.getLogger(BigquerySinkTask.class);
    private static final int BATCH_SIZE = 500;

    private String projectId;
    private String datasetId;
    private String tableId;
    private BigqueryClient client;

    private Converter converter;

    public BigquerySinkTask() {
        converter = new JsonConverter();
        Map<String, Object> configs = new HashMap<>();
        configs.put("schemas.enable", false);
        converter.configure(configs, false);
    }

    @Override
    public String version() {
        return "0.1";
    }

    @Override
    public void start(final Map<String, String> props) {
        LOG.info("Configuring Bigquery Sink task");
        projectId = props.get(BigquerySinkConnector.PROJECT_ID);
        datasetId = props.get(BigquerySinkConnector.DATASET_ID);
        tableId = props.get(BigquerySinkConnector.TABLE_ID);

        client = new BigqueryClient(projectId);
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        LOG.info("Streaming {} records to bigquery by batches of {} elements", records.size(), BATCH_SIZE);

        List<byte[]> jsonRecords = new LinkedList<>();
        for (SinkRecord record : records) {
            byte[] json = converter.fromConnectData(record.topic(), record.valueSchema(), record.value());
            jsonRecords.add(json);

            if (jsonRecords.size() % BATCH_SIZE == 0) {
                stream(jsonRecords);
            }
        }

        if (jsonRecords.size() > 0) {
            stream(jsonRecords);
        }
    }

    private void stream(List<byte[]> records) {
        LOG.info("Streaming batch of {} records", records.size());
        try {
            client.streamJsons(records, datasetId, tableId);
        } catch (IOException e) {
            LOG.error("Failed to stream record to bigquery: {}", e.getMessage());
        }
        records.clear();
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> offsets) {
    }

    @Override
    public void stop() {
    }

}
