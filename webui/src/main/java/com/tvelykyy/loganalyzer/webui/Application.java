package com.tvelykyy.loganalyzer.webui;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.tvelykyy.bigquery.BigqueryClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

import java.io.IOException;

@SpringBootApplication
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean
    public BigqueryClient getBigQueryClient(@Value("${bigquery.project}") String project) throws IOException {
        return new BigqueryClient(project);
    }

    @Bean
    public Jackson2ObjectMapperBuilder objectMapperBuilder() {
        Jackson2ObjectMapperBuilder builder = new Jackson2ObjectMapperBuilder();
        builder.featuresToDisable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS);
        return builder;
    }
}