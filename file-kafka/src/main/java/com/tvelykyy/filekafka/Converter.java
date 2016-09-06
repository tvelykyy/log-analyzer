package com.tvelykyy.filekafka;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Converter {
    private static final Logger LOGGER = LoggerFactory.getLogger(Converter.class);

    private static final String IP = "ip";
    private static final String DATETIME = "datetime";
    private static final String PAGE = "page";
    private static final String STATUS_CODE = "statusCode";
    private static final String SIZE = "size";
    private static final String REFERRER = "referrer";
    private static final String USERAGENT = "useragent";

    private final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("d/MMM/uuuu:HH:mm:ss Z");

    private final Schema schema;

    private final Pattern pattern;
    private final String LOG_ENTRY_PATTERN = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"GET (.+?) HTTP/1.1\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";

    public Converter() {
        SchemaBuilder builder = SchemaBuilder.struct().name("logentry");
        builder.field(IP, Schema.STRING_SCHEMA);
        builder.field(DATETIME, Schema.INT64_SCHEMA);
        builder.field(PAGE, Schema.STRING_SCHEMA);
        builder.field(STATUS_CODE, Schema.INT32_SCHEMA);
        builder.field(SIZE, Schema.INT32_SCHEMA);
        builder.field(REFERRER, Schema.STRING_SCHEMA);
        builder.field(USERAGENT, Schema.STRING_SCHEMA);
        schema = builder.build();

        pattern = Pattern.compile(LOG_ENTRY_PATTERN);
    }

    public Schema getSchema() {
        return schema;
    }

    public Struct convertToStruct(String logMessage) {
        Matcher matcher = pattern.matcher(logMessage);

        Struct struct = new Struct(schema);
        if (matcher.matches()) {
            struct.put(IP, matcher.group(1));
            struct.put(DATETIME, ZonedDateTime.parse(matcher.group(4), DATE_FORMATTER).toInstant().toEpochMilli());
            struct.put(PAGE, matcher.group(5));
            struct.put(STATUS_CODE, Integer.valueOf(matcher.group(6)));
            struct.put(SIZE, Integer.valueOf(matcher.group(7)));
            struct.put(REFERRER, matcher.group(8));
            struct.put(USERAGENT, matcher.group(9));
        }

        return struct;
    }

}
