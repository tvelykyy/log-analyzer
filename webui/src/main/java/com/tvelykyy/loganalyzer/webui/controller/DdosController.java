package com.tvelykyy.loganalyzer.webui.controller;

import com.tvelykyy.loganalyzer.webui.model.IpActivityForPeriod;
import com.tvelykyy.loganalyzer.webui.model.IpsSummaryForPeriod;
import com.tvelykyy.loganalyzer.webui.model.TotalForPeriod;
import com.tvelykyy.loganalyzer.webui.service.DdosService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/ddos")
public class DdosController {

    @Autowired
    private DdosService ddosService;

    @RequestMapping("/total")
    public TotalForPeriod total(@RequestParam long start, @RequestParam long end) {
        return ddosService.getTotal(start, end);
    }

    @RequestMapping("")
    public IpsSummaryForPeriod ddos() {
        return ddosService.getIpsSummary();
    }

    @RequestMapping("/{ip:.+}")
    public IpActivityForPeriod ddosIp(@PathVariable String ip) {
        return ddosService.getIpActivity(ip);
    }

}
