package com.tvelykyy.loggenerator;

import com.tvelykyy.loggenerator.ip.RandomIpv4Generator;
import com.tvelykyy.loggenerator.ip.StaticIpv4Generator;
import com.tvelykyy.loggenerator.page.ConsecutivePageGenerator;
import com.tvelykyy.loggenerator.page.RandomPageGenerator;
import com.tvelykyy.loggenerator.referrer.RandomReferrerGenerator;
import com.tvelykyy.loggenerator.statuscode.RandomWithProbabilityStatusCodeGenerator;
import com.tvelykyy.loggenerator.useragent.RandomUserAgentGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(2);

        executor.submit(() -> {
            LogGenerator gen = new LogGenerator(new RandomIpv4Generator(),
                new ConsecutivePageGenerator(),
                new RandomWithProbabilityStatusCodeGenerator(),
                new RandomReferrerGenerator(),
                new RandomUserAgentGenerator()
            );

            for (int i = 0; i < 100000; i++) {
//          while (true) {
                try {
                    Thread.sleep(3);
                } catch (InterruptedException e) {
                    //ignore
                }
                LOGGER.debug(gen.get());
            }
        });

        executor.submit(() -> {
            LogGenerator gen = new LogGenerator(new StaticIpv4Generator(),
                new RandomPageGenerator(),
                new RandomWithProbabilityStatusCodeGenerator(),
                new RandomReferrerGenerator(),
                new RandomUserAgentGenerator()
            );

            for (int i = 0; i < 10000; i++)
                try {
                    Thread.sleep(3);
                } catch (InterruptedException e) {
                    //ignore
                }
                LOGGER.debug(gen.get());
        });

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.MINUTES);
        System.out.println("Exiting normally...");
    }
}
