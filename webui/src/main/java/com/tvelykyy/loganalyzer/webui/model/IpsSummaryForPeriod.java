package com.tvelykyy.loganalyzer.webui.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.ZonedDateTime;
import java.util.List;

@AllArgsConstructor
@Getter
public class IpsSummaryForPeriod {
    private final List<IpSummary> ipsTotal;
    private final ZonedDateTime start;
    private final ZonedDateTime end;

    @AllArgsConstructor
    @Getter
    public static class IpSummary {
        private final String ip;
        private final long total;
    }
}


