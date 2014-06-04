package org.cloudbus.cloudsim.ex.web.workload.freq;

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.cloudbus.cloudsim.ex.util.helpers.TestUtil;
import org.cloudbus.cloudsim.ex.web.workload.freq.CompositeValuedSet;
import org.cloudbus.cloudsim.ex.web.workload.freq.PeriodicStochasticFrequencyFunction;
import org.junit.Test;

/**
 * 
 * @author nikolay.grozev
 * 
 */
public class PeriodicStochasticFrequencyFunctionTest {

    // We need big delta, since we are doing some roundings
    private static final double DELTA = 1;
    private static final int TEST_SIZE = 1000;

    private static final double UNIT = 10;
    private static final double PERIOD_LENGTH = 100;
    private static final double NULL_POINT = 5;

    @Test
    public void testMainScenarios() {
        PeriodicStochasticFrequencyFunction fun = new PeriodicStochasticFrequencyFunction(UNIT, PERIOD_LENGTH,
                NULL_POINT, CompositeValuedSet.createCompositeValuedSet(TestUtil.SEED_ARRAY, "[0,20] m=10 std=1",
                        "(20,40]m=15.1 std=1.6", "(40,60] m=16 std=0.1", "(60,80] m=300 std=1", "(80,100] m=10 std=0"));

        Random r = new Random(TestUtil.SEED);
        DescriptiveStatistics stat = new DescriptiveStatistics();

        // Test the first interval
        for (int i = 0; i < TEST_SIZE; i++) {
            double x = NULL_POINT + r.nextDouble() * 20;
            stat.addValue(fun.getFrequency(x));
        }
        assertEquals(10, stat.getMean(), DELTA);
        assertEquals(1, stat.getStandardDeviation(), DELTA);

        // Test the second interval
        stat.clear();
        for (int i = 0; i < TEST_SIZE; i++) {
            double x = NULL_POINT + 20 + r.nextDouble() * 20;
            stat.addValue(fun.getFrequency(x));
        }
        assertEquals(15.1, stat.getMean(), DELTA);
        assertEquals(1.5, stat.getStandardDeviation(), DELTA);

        // Test the third interval
        stat.clear();
        for (int i = 0; i < TEST_SIZE; i++) {
            double x = NULL_POINT + 40 + r.nextDouble() * 20;
            stat.addValue(fun.getFrequency(x));
        }
        assertEquals(16, stat.getMean(), DELTA);
        assertEquals(0.1, stat.getStandardDeviation(), DELTA);

        // Test the 4th interval
        stat.clear();
        for (int i = 0; i < TEST_SIZE; i++) {
            double x = NULL_POINT + 60 + r.nextDouble() * 20;
            stat.addValue(fun.getFrequency(x));
        }
        assertEquals(300, stat.getMean(), DELTA);
        assertEquals(1, stat.getStandardDeviation(), DELTA);

        // Test the 5th interval
        for (int i = 0; i < TEST_SIZE; i++) {
            double x = NULL_POINT + 80 + r.nextDouble() * 20;
            assertEquals(10, fun.getFrequency(x), DELTA);
        }

        // Test outside values - before the null point
        for (int i = 0; i < TEST_SIZE; i++) {
            double x = r.nextDouble() * NULL_POINT;
            assertEquals(10, fun.getFrequency(x), DELTA);
        }

        stat.clear();
        for (int i = 0; i < TEST_SIZE; i++) {
            double x = NULL_POINT + 60 + r.nextDouble() * 20 - PERIOD_LENGTH;
            stat.addValue(fun.getFrequency(x));
        }
        assertEquals(300, stat.getMean(), DELTA);
        assertEquals(1, stat.getStandardDeviation(), DELTA);

        // Test outside values - after the inital interval
        stat.clear();
        for (int i = 0; i < TEST_SIZE; i++) {
            double x = NULL_POINT + 60 + r.nextDouble() * 20 + 2 * PERIOD_LENGTH;
            stat.addValue(fun.getFrequency(x));
        }
        assertEquals(300, stat.getMean(), DELTA);
        assertEquals(1, stat.getStandardDeviation(), DELTA);
    }

}
