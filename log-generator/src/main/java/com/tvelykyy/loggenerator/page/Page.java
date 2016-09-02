package com.tvelykyy.loggenerator.page;

/**
 * Class represents combination of pseudo-random site page and it's size.
 */
public class Page {
    private final String path;
    private final int size;

    public Page(String path, int size) {
        this.path = path;
        this.size = size;
    }

    public String getPath() {
        return path;
    }

    public int getSize() {
        return size;
    }
}
