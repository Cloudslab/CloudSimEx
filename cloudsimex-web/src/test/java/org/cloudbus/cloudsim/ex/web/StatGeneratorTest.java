package org.cloudbus.cloudsim.ex.web;

import static org.cloudbus.cloudsim.ex.util.helpers.TestUtil.createSeededGaussian;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.cloudbus.cloudsim.ex.disk.DataItem;
import org.cloudbus.cloudsim.ex.web.StatGenerator;
import org.cloudbus.cloudsim.ex.web.WebCloudlet;
import org.junit.Before;
import org.junit.Test;
import org.uncommons.maths.number.NumberGenerator;

/**
 * 
 * @author nikolay.grozev
 * 
 */
public class StatGeneratorTest {

    private static final int GEN_RAM_MEAN = 200;
    private static final int GEN_RAM_STDEV = 10;
    private NumberGenerator<Double> genRAM;

    private static final int GEN_CPU_MEAN = 25;
    private static final int GEN_CPU_STDEV = 2;
    private NumberGenerator<Double> genCPU;

    private Map<String, NumberGenerator<Double>> testGenerators = new HashMap<>();

    private static final DataItem data = new DataItem(65);

    @Before
    public void setUp() {
        genRAM = createSeededGaussian(GEN_RAM_MEAN, GEN_RAM_STDEV);
        genCPU = createSeededGaussian(GEN_CPU_MEAN, GEN_CPU_STDEV);
        testGenerators = new HashMap<>();
        testGenerators.put(StatGenerator.CLOUDLET_LENGTH, genCPU);
        testGenerators.put(StatGenerator.CLOUDLET_RAM, genRAM);
    }

    @Test
    public void testHandlingEmptyAndNonemtpyCases() {
        // Should be empty in the start
        StatGenerator generator = new StatGenerator(testGenerators, data);
        assertTrue(generator.isEmpty());
        assertNull(generator.peek());
        assertNull(generator.poll());

        generator.notifyOfTime(15);

        // Should not be empty now
        assertFalse(generator.isEmpty());
        Object peeked = generator.peek();
        Object peekedAgain = generator.peek();
        Object polled = generator.poll();
        assertNotNull(peeked);
        assertTrue(peeked == peekedAgain);
        assertTrue(peeked == polled);

        // Should be empty again
        assertTrue(generator.isEmpty());
        assertNull(generator.peek());
        assertNull(generator.poll());
    }

    @Test
    public void testHandlingTimeConstraints() {
        // Should be empty in the start
        StatGenerator generator = new StatGenerator(testGenerators, 3, 12, data);

        // Notify for times we are not interested in (they are outside [3;12])
        generator.notifyOfTime(2);
        generator.notifyOfTime(15);
        generator.notifyOfTime(17);

        // Should be empty now...
        assertTrue(generator.isEmpty());
        assertNull(generator.peek());
        assertNull(generator.poll());

        // Notify for times again.
        generator.notifyOfTime(2); // Not Interested - outside [3;12]
        generator.notifyOfTime(5); // Interested
        generator.notifyOfTime(5); // Not Interested - it is repeated
        generator.notifyOfTime(7); // Interested
        generator.notifyOfTime(10); // Interested
        generator.notifyOfTime(10); // Not Interested - it is repeated
        generator.notifyOfTime(18); // Not Interested

        // Should not be empty now
        assertFalse(generator.isEmpty());
        Object peeked = generator.peek();
        Object peekedAgain = generator.peek();
        assertTrue(peeked == peekedAgain);

        // Check if we have 3 things in the generator
        int i = 0;
        while (!generator.isEmpty()) {
            peeked = generator.peek();
            peekedAgain = generator.peek();
            Object polled = generator.poll();
            assertNotNull(peeked);
            assertTrue(peeked == peekedAgain);
            assertTrue(peeked == polled);
            i++;
        }
        assertEquals(3, i);

        // Should be empty again... we polled everything
        assertTrue(generator.isEmpty());
        assertNull(generator.peek());
        assertNull(generator.poll());
    }

    @Test
    public void testStatisticsAreUsedOK() {
        StatGenerator generator = new StatGenerator(testGenerators, data);

        DescriptiveStatistics ramStat = new DescriptiveStatistics();
        DescriptiveStatistics cpuStat = new DescriptiveStatistics();

        // Generate 100 values
        int size = 100;
        for (int i = 0; i < size; i++) {
            generator.notifyOfTime(i + 5);
        }

        // Compute descriptive statistics
        for (int i = 0; i < size; i++) {
            WebCloudlet c = generator.poll();
            ramStat.addValue(c.getRam());
            cpuStat.addValue(c.getCloudletLength());
        }

        // Allow for delta, because of using doubles, and rounding some of the
        // numbers
        double delta = 10;
        assertEquals(GEN_RAM_MEAN, ramStat.getMean(), delta);
        assertEquals(GEN_RAM_STDEV, ramStat.getStandardDeviation(), delta);
        assertEquals(GEN_CPU_MEAN, cpuStat.getMean(), delta);
        assertEquals(GEN_CPU_STDEV, cpuStat.getStandardDeviation(), delta);

        // Assert we have exhausted the generator
        assertTrue(generator.isEmpty());
    }
}
