package com.tvelykyy.loganalyzer.webui.service;

import com.tvelykyy.loganalyzer.webui.model.IpActivityForPeriod;
import com.tvelykyy.loganalyzer.webui.model.IpsSummaryForPeriod;

public interface DdosService {
    IpsSummaryForPeriod getIpsSummary();

    IpActivityForPeriod getIpActivity(String ip);
}
