package com.tvelykyy.loggenerator.page;

import java.util.Random;

/**
 * This implementations used for returning pseudo-random pages generate beforehand.
 */
public class RandomPageGenerator implements PageGenerator {
    private static final Random R = new Random();

    /**
     * Collection of pre-generated pseudo-random {@link Page} instances.
     */
    private static final Pages pages = new Pages();

    @Override
    public Page get() {
        return pages.getPage(R.nextInt(pages.getCount()));
    }
}
