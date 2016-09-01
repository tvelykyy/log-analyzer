package com.tvelykyy.loganalyzer.webui.service;

import com.google.api.services.bigquery.model.TableRow;
import com.tvelykyy.bigquery.BigqueryClient;
import com.tvelykyy.loganalyzer.webui.model.IpTotalForPeriod;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static java.util.stream.Collectors.toList;

@Service
public class BigqueryService implements DdosService {

    private static final String QUERY_SKELETON = "SELECT ip, count(*) AS total FROM [%s:%s.%s] " +
            "WHERE TIMESTAMP_TO_MSEC(CURRENT_TIMESTAMP()) - datetime < %s GROUP BY ip HAVING count(*) > 3";

    @Autowired
    private BigqueryClient client;

    @Value("${bigquery.project}")
    private String project;

    @Value("${bigquery.dataset}")
    private String dataset;

    @Value("${bigquery.table}")
    private String table;

    @Value("${last.minutes}")
    private Integer periodInMinutes;

    private String query;

    @Override
    public IpTotalForPeriod getActivityIpsTotal() {
        List<TableRow> tableRows = client.executeQuery(query);

        ZonedDateTime end = ZonedDateTime.now();
        List<IpTotalForPeriod.IpTotal> ipsTotal = tableRows.stream()
                .map(row -> new IpTotalForPeriod.IpTotal((String) row.getF().get(0).getV(), Long.parseLong((String) row.getF().get(1).getV())))
                .collect(toList());
        ZonedDateTime start = end.minus(periodInMinutes, ChronoUnit.MILLIS);

        return new IpTotalForPeriod(ipsTotal, start, end);

    }

    @PostConstruct
    public void generateQuery() {
        long millis = periodInMinutes * 60 * 1000;
        query = String.format(QUERY_SKELETON, project, dataset, table, millis);
    }

}
