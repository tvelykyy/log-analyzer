package com.tvelykyy.loggenerator.page;

public class ConsecutivePageGenerator implements PageGenerator {
    private int num;

    @Override
    public Page get() {
        return new Page((++num) + ".html", num);
    }
}
