package com.tvelykyy.loggenerator.ip;

import java.util.Random;

/**
 * This implementations generates pseudo-random ip address.
 */
public class RandomIpv4Generator implements Ipv4Generator {
    private static final Random R = new Random();
    private static final int BOUND = 255;

    @Override
    public String get() {
        return String.format("%s.%s.%s.%s", R.nextInt(BOUND), R.nextInt(BOUND), R.nextInt(BOUND), R.nextInt(BOUND));
    }
}
