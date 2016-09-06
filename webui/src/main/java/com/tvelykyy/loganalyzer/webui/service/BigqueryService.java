package com.tvelykyy.loganalyzer.webui.service;

import com.google.api.services.bigquery.model.TableRow;
import com.tvelykyy.bigquery.BigqueryClient;
import com.tvelykyy.loganalyzer.webui.model.IpActivityForPeriod;
import com.tvelykyy.loganalyzer.webui.model.IpsSummaryForPeriod;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static java.util.stream.Collectors.toList;

@Service
public class BigqueryService implements DdosService {

    private static final String SUMMARY_QUERY_SKELETON = "SELECT ip, count(*) AS total FROM [%s:%s.%s] " +
            "WHERE TIMESTAMP_TO_MSEC(CURRENT_TIMESTAMP()) - datetime < %s GROUP BY ip HAVING count(*) > 100";

    private static final String ACTIVITY_QUERY_SKELETON = "SELECT datetime, count(*) AS total FROM [%s:%s.%s] " +
            "WHERE TIMESTAMP_TO_MSEC(CURRENT_TIMESTAMP()) - datetime < %s AND ip = '{ip}' GROUP BY ip, datetime";

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

    private String summaryQuery;
    private String activityQuery;

    @Override
    public IpsSummaryForPeriod getIpsSummary() {
        List<TableRow> tableRows = client.executeQuery(summaryQuery);

        ZonedDateTime end = ZonedDateTime.now();
        List<IpsSummaryForPeriod.IpSummary> ipsTotal = tableRows.stream()
                .map(row -> new IpsSummaryForPeriod.IpSummary((String) row.getF().get(0).getV(), Long.parseLong((String) row.getF().get(1).getV())))
                .collect(toList());
        ZonedDateTime start = end.minus(periodInMinutes, ChronoUnit.MILLIS);

        return new IpsSummaryForPeriod(ipsTotal, start, end);
    }

    @Override
    public IpActivityForPeriod getIpActivity(String ip) {
        String query = activityQuery.replace("{ip}", ip);

        List<TableRow> tableRows = client.executeQuery(query);
        ZonedDateTime end = ZonedDateTime.now();
        List<IpActivityForPeriod.IpActivity> ipActivity = tableRows.stream()
                .map(row -> new IpActivityForPeriod.IpActivity(ZonedDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong((String) row.getF().get(0).getV())), ZoneId.systemDefault()),
                        Long.parseLong((String) row.getF().get(1).getV())))
                .collect(toList());

        ZonedDateTime start = end.minus(periodInMinutes, ChronoUnit.MILLIS);
        return new IpActivityForPeriod(ip, ipActivity, start, end);

    }

    @PostConstruct
    public void generateQuery() {
        long millis = periodInMinutes * 60 * 1000;
        summaryQuery = String.format(SUMMARY_QUERY_SKELETON, project, dataset, table, millis);
        activityQuery = String.format(ACTIVITY_QUERY_SKELETON, project, dataset, table, millis);
    }

}
