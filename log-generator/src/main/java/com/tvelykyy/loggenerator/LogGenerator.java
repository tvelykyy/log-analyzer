package com.tvelykyy.loggenerator;

import com.tvelykyy.loggenerator.ip.Ipv4Generator;
import com.tvelykyy.loggenerator.page.Page;
import com.tvelykyy.loggenerator.page.PageGenerator;
import com.tvelykyy.loggenerator.referrer.ReferrerGenerator;
import com.tvelykyy.loggenerator.statuscode.StatusCodeGenerator;
import com.tvelykyy.loggenerator.useragent.UserAgentGenerator;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Class generate log messages using predefined set of generators.
 */
public class LogGenerator {
    // Always GET requests.
    private static final String LOG_FORMAT = "%s - - [%s] \"GET %s HTTP/1.1\" %s %s \"%s\" \"%s\"";
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("d/MMM/uuuu:HH:mm:ss Z");

    private Ipv4Generator ip;
    private PageGenerator page;
    private StatusCodeGenerator statusCode;
    private ReferrerGenerator referrer;
    private UserAgentGenerator userAgent;

    public LogGenerator(final Ipv4Generator ip, final PageGenerator page,
                        final StatusCodeGenerator statusCode, final ReferrerGenerator referrer,
                        final UserAgentGenerator userAgent) {
        this.ip = ip;
        this.page = page;
        this.statusCode = statusCode;
        this.referrer = referrer;
        this.userAgent = userAgent;
    }

    /**
     * Generates log message using provided generators.
     * @return String representing log message.
     */
    public String get() {
        Page p = page.get();
        String log = String.format(
            LOG_FORMAT,
            ip.get(),
            ZonedDateTime.now().format(DATE_FORMATTER),
            p.getPath(),
            statusCode.get(),
            p.getSize(),
            referrer.get(),
            userAgent.get()
        );

        return log;
    }
}
