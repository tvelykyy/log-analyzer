package com.tvelykyy.loganalyzer.webui.service;

import com.tvelykyy.loganalyzer.webui.model.IpTotalForPeriod;

public interface DdosService {
    IpTotalForPeriod getActivityIpsTotal();
}
