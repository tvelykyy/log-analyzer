package com.tvelykyy.bigquery;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.*;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Example of Bigquery Streaming.
 */
public class BigqueryClient {
    private static final Logger LOG = LoggerFactory.getLogger(BigqueryClient.class);

    private final Bigquery bigquery = initBigquery();
    private final Gson gson = new Gson();

    private final String project;

    public BigqueryClient(final String project) {
        this.project = project;
    }

    private Bigquery initBigquery() {
        try {
            return BigqueryServiceFactory.getService();
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public List<TableRow> executeQuery(String sql) {
        try {
            QueryResponse response = bigquery.jobs()
                    .query(project, new QueryRequest().setQuery(sql))
                    .execute();
            return response.getRows();
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public TableDataInsertAllResponse streamJsons(final List<byte[]> jsonRows, String dataset, String table) throws IOException {
        List<TableDataInsertAllRequest.Rows> rows = new LinkedList<>();
        for (byte[] jsonRow : jsonRows) {
            ByteArrayInputStream in = new ByteArrayInputStream(jsonRow);
            JsonReader jsonReader = new JsonReader(new InputStreamReader(in));

            Map<String, Object> rowData =
                    gson.<Map<String, Object>>fromJson(jsonReader, (new HashMap<String, Object>()).getClass());
            rows.add(new TableDataInsertAllRequest.Rows().setJson(rowData));
        }

        TableDataInsertAllResponse response = streamRows(rows, dataset, table);

        response.getInsertErrors().forEach(e -> {
            try {
                LOG.error(e.toPrettyString());
            } catch (IOException exception) {
                LOG.error(exception.getMessage());
            }
        });

        return response;
    }

    public TableDataInsertAllResponse streamJson(final byte[] row, String dataset, String table) throws IOException {
        return streamJsons(Collections.singletonList(row), dataset, table);
    }

    private TableDataInsertAllResponse streamRows(final List<TableDataInsertAllRequest.Rows> rows, String dataset, String table) throws IOException {

        return bigquery
                .tabledata()
                .insertAll(project, dataset, table, new TableDataInsertAllRequest().setRows(rows))
                .execute();
    }
}

