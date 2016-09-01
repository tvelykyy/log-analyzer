package com.tvelykyy.loganalyzer.webui.controller;

import com.tvelykyy.loganalyzer.webui.model.IpTotalForPeriod;
import com.tvelykyy.loganalyzer.webui.service.DdosService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DdosController {

    @Autowired
    private DdosService ddosService;

    @RequestMapping("/ddos")
    IpTotalForPeriod ddos() {
        return ddosService.getActivityIpsTotal();
    }

}
