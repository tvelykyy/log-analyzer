package com.tvelykyy.loggenerator.page;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RandomPageGeneratorTest {

    @Test
    public void testGet() {
        //Given
        PageGenerator generator = new RandomPageGenerator();

        //When
        Page page = generator.get();

        //Then
        assertNotNull(page);
        assertNotNull(page.getPath());
        assertTrue(page.getSize() > 0);
    }

}
