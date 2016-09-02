package com.tvelykyy.loggenerator.ip;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RandomIpv4GeneratorTest {

    @Test
    public void testFormat() {
        //Given
        Ipv4Generator generator = new RandomIpv4Generator();

        //When
        String ip = generator.get();
        String[] parts = ip.split("\\.");

        //Then
        assertEquals(4, parts.length);
        for (String part : parts) {
            int mask = Integer.parseInt(part);
            assertTrue(mask > 0);
            assertTrue(mask < 255);
        }
    }

}
