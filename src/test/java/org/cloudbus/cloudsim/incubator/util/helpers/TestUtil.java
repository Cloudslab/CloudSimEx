package org.cloudbus.cloudsim.incubator.util.helpers;

import java.util.Random;

import org.uncommons.maths.number.NumberGenerator;
import org.uncommons.maths.random.GaussianGenerator;
import org.uncommons.maths.random.MersenneTwisterRNG;

/**
 * Defines a set of common test functions and constants.
 * 
 * @author nikolay.grozev
 * 
 */
public class TestUtil {

    /**
     * Seed for testing purposes.
     */
    public static final byte[] SEED = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };

    /**
     * Returns a newly seeded gausian (normal distributions) number generator.
     * 
     * @param mean
     *            - the mean of the generator.
     * @param stDev
     *            - the standard deviation of the generator.
     * @return a newly seeded gausian (normal distribution) number generator.
     */
    public static NumberGenerator<Double> createSeededGaussian(final double mean, final double stDev) {
	Random rng = new MersenneTwisterRNG(TestUtil.SEED);
	return new GaussianGenerator(mean, stDev, rng);
    }

}
