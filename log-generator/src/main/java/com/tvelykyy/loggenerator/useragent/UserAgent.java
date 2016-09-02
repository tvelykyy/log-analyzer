package com.tvelykyy.loggenerator.useragent;

/**
 * Enum represents set of predefined list of user-agents.
 */
public enum UserAgent {
    CHROME_41("Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"),
    FIREFOX_40("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1"),
    IE_11("Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko"),
    SAFARI_7("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A");

    private String detailed;

    UserAgent(String detailed) {
        this.detailed = detailed;
    }

    public String getDetailed() {
        return detailed;
    }

}
