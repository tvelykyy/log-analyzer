package com.tvelykyy.loggenerator;

import com.tvelykyy.loggenerator.ip.StaticIpv4Generator;
import com.tvelykyy.loggenerator.page.RandomPageGenerator;
import com.tvelykyy.loggenerator.referrer.RandomReferrerGenerator;
import com.tvelykyy.loggenerator.statuscode.RandomWithProbabilityStatusCodeGenerator;
import com.tvelykyy.loggenerator.useragent.RandomUserAgentGenerator;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class LogGeneratorTest {

    //Just make no exception is thrown when working in cooperation.
    @Test
    public void testGet() {
        //Given
        LogGenerator generator = new LogGenerator(new StaticIpv4Generator(),
            new RandomPageGenerator(),
            new RandomWithProbabilityStatusCodeGenerator(),
            new RandomReferrerGenerator(),
            new RandomUserAgentGenerator());

        //When
        String logMessage = generator.get();

        //Then
        assertNotNull(logMessage);
    }

}
