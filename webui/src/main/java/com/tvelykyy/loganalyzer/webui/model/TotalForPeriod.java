package com.tvelykyy.loganalyzer.webui.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class TotalForPeriod {
    private final long total;
    private final long start;
    private final long end;
}
