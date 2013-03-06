package org.cloudbus.cloudsim.ex.util.helpers;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import org.uncommons.maths.number.NumberGenerator;
import org.uncommons.maths.random.GaussianGenerator;
import org.uncommons.maths.random.MersenneTwisterRNG;

/**
 * Defines a set of common test functions and constants for test purposes.
 * 
 * @author nikolay.grozev
 * 
 */
public class TestUtil {

    /**
     * Properties for the log configuration.
     */
    public static Properties LOG_PROPS = new Properties();
    static {
	// LOG_PROPS.put(CustomLog.LOG_LEVEL_PROP_KEY, Level.FINEST.getName());
	LOG_PROPS.put("ShutStandardLogger", "true");
    }

    /**
     * Byte array seed for testing purposes.
     */
    public static final byte[] SEED_ARRAY = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };

    /**
     * Numeric seed for testing purposes.
     */
    public static final long SEED = Arrays.hashCode(SEED_ARRAY);

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
	Random rng = new MersenneTwisterRNG(TestUtil.SEED_ARRAY);
	return new GaussianGenerator(mean, stDev, rng);
    }

}
