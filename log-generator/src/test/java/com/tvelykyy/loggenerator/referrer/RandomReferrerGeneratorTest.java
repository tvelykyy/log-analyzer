package com.tvelykyy.loggenerator.referrer;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class RandomReferrerGeneratorTest {

    @Test
    public void testGet() {
        //Given
        ReferrerGenerator generator = new RandomReferrerGenerator();

        //When
        String referrer = generator.get();

        //Then
        assertNotNull(referrer);
    }

}