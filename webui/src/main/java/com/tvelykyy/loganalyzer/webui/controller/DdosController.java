package com.tvelykyy.loganalyzer.webui.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DdosController {

    @RequestMapping("/ddos")
    String ddos() {
        return "Hello Ddos";
    }

}
