package com.tvelykyy.loganalyzer.webui.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.ZonedDateTime;
import java.util.List;

@AllArgsConstructor
@Getter
public class IpTotalForPeriod {
    private final List<IpTotal> ipsTotal;
    private final ZonedDateTime start;
    private final ZonedDateTime end;

    @AllArgsConstructor
    @Getter
    public static class IpTotal {
        private final String ip;
        private final long total;
    }
}


