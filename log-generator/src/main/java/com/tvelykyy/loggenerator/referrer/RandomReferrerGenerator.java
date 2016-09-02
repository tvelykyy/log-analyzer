package com.tvelykyy.loggenerator.referrer;

import java.util.Random;

/**
 * This implementations used for returning pseudo-random referrers generated beforehand.
 */
public class RandomReferrerGenerator implements ReferrerGenerator {
    private static final Random R = new Random();

    /**
     * Collection of pre-generated referrers.
     */
    private static final Referrers refs = new Referrers();

    @Override
    public String get() {
        return refs.getReferrer(R.nextInt(refs.getCount()));
    }
}
