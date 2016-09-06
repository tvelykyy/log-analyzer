package com.tvelykyy.loganalyzer.webui.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.ZonedDateTime;
import java.util.List;

@AllArgsConstructor
@Getter
public class IpActivityForPeriod {
    private final String ip;
    private final List<IpActivity> ipActivity;
    private final ZonedDateTime start;
    private final ZonedDateTime end;

    @AllArgsConstructor
    @Getter
    public static class IpActivity {
        private final ZonedDateTime datetime;
        private final long total;
    }
}
