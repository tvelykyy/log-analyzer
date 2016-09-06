package com.tvelykyy.loganalyzer.webui.service;

import com.tvelykyy.loganalyzer.webui.model.IpActivityForPeriod;
import com.tvelykyy.loganalyzer.webui.model.IpsSummaryForPeriod;
import com.tvelykyy.loganalyzer.webui.model.TotalForPeriod;

public interface DdosService {
    TotalForPeriod getTotal(long startIncluded, long endExcluded);

    IpsSummaryForPeriod getIpsSummary();

    IpActivityForPeriod getIpActivity(String ip);
}
