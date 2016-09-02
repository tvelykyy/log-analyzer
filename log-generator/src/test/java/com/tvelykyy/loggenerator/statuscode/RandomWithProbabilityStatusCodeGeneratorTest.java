package com.tvelykyy.loggenerator.statuscode;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.Assert.assertTrue;

public class RandomWithProbabilityStatusCodeGeneratorTest {
    private static final double OK_CODE_UPPER = 0.93;
    private static final double OK_CODE_LOWER = 0.87;
    private static final double FAIL_CODE_UPPER = 0.08;
    private static final double FAIL_CODE_LOWER = 0.03;

    private static final Map<Integer, Integer> COUNTS = new HashMap<Integer, Integer>() {{
        put(200, 0);
        put(404, 0);
        put(503, 0);
    }};
    private final int ITERATIONS = 1000;

    @Test
    public void testGetWithProbability() {
        //Given
        StatusCodeGenerator generator = new RandomWithProbabilityStatusCodeGenerator();


        //When
        IntStream.range(0, ITERATIONS).forEach(i -> {
            int statusCode = generator.get();
            COUNTS.put(statusCode, COUNTS.get(statusCode) + 1);
        });

        //Then
        assertTrue(getPercentage(200) < OK_CODE_UPPER);
        assertTrue(getPercentage(200) > OK_CODE_LOWER);
        assertTrue(getPercentage(404) > FAIL_CODE_LOWER);
        assertTrue(getPercentage(404) < FAIL_CODE_UPPER);
        assertTrue(getPercentage(503) > FAIL_CODE_LOWER);
        assertTrue(getPercentage(503) < FAIL_CODE_UPPER);
    }

    private double getPercentage(int statusCode) {
        return COUNTS.get(statusCode) / (double) ITERATIONS;
    }

}
