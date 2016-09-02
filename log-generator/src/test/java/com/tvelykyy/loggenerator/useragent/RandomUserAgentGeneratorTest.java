package com.tvelykyy.loggenerator.useragent;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class RandomUserAgentGeneratorTest {

    @Test
    public void testGet() {
        //Given
        UserAgentGenerator generator = new RandomUserAgentGenerator();

        //When
        String userAgent = generator.get();

        //Then
        assertNotNull(userAgent);
    }
}
