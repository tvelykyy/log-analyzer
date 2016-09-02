package com.tvelykyy.loggenerator.statuscode;

import com.tvelykyy.loggenerator.DistributedRandomNumberGenerator;

/**
 * This implementations returns pseudo-random http status codes with predefined probability.
 */
public class RandomWithProbabilityStatusCodeGenerator implements StatusCodeGenerator {
    DistributedRandomNumberGenerator generator;

    /**
     * Defines probabilities for each status code.
     */
    public RandomWithProbabilityStatusCodeGenerator() {
        generator = new DistributedRandomNumberGenerator();
        generator.addNumber(200, 0.9d);
        generator.addNumber(404, 0.05d);
        generator.addNumber(503, 0.05d);
    }

    @Override
    public int get() {
        return generator.getDistributedRandomNumber();
    }

}
