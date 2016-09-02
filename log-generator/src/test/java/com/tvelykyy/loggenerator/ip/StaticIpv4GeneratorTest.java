package com.tvelykyy.loggenerator.ip;

import org.junit.Test;

import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StaticIpv4GeneratorTest {

    @Test
    public void testFormat() {
        //Given
        Ipv4Generator generator = new StaticIpv4Generator();

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

    @Test
    public void testAlwaysTheSame() {
        //Given
        Ipv4Generator generator = new StaticIpv4Generator();

        //When
        String ip = generator.get();

        //Then
        IntStream.range(0, 5).forEach(i -> {
            assertEquals(ip, generator.get());
        });
    }
}
