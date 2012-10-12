package org.cloudbus.cloudsim.incubator.web;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.cloudbus.cloudsim.Vm;
import org.junit.Before;
import org.junit.Test;
import org.uncommons.maths.number.NumberGenerator;
import org.uncommons.maths.random.GaussianGenerator;
import org.uncommons.maths.random.MersenneTwisterRNG;

/**
 * 
 * @author nikolay.grozev
 * 
 */
public class ASStatGeneratorTest {

    // Seed it for testing
    private static final byte[] SEED = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };

    private static final int GEN_RAM_MEAN = 200;
    private static final int GEN_RAM_STDEV = 10;
    private GaussianGenerator genRAM;

    private static final int GEN_CPU_MEAN = 25;
    private static final int GEN_CPU_STDEV = 2;
    private GaussianGenerator genCPU;

    private static IWebBroker DUMMY_BROKER = new IWebBroker() {

	@Override
	public <T extends Vm> List<T> getVmList() {
	    return new ArrayList<T>();
	}

	@Override
	public int getId() {
	    return 1;
	}
    };

    private Map<String, NumberGenerator<Double>> testGenerators = new HashMap<>();

    @Before
    public void setUP() {
	Random RNG = new MersenneTwisterRNG(SEED);
	genRAM = new GaussianGenerator(GEN_RAM_MEAN, GEN_RAM_STDEV, RNG);
	genCPU = new GaussianGenerator(GEN_CPU_MEAN, GEN_CPU_STDEV, RNG);
	testGenerators = new HashMap<>();
	testGenerators.put(ASStatGenerator.CLOUDLET_LENGTH, genCPU);
	testGenerators.put(ASStatGenerator.CLOUDLET_RAM, genRAM);
    }

    @Test
    public void testHandlingEmptyAndNonemtpyCases() {
	// Should be empty in the start
	ASStatGenerator generator = new ASStatGenerator(DUMMY_BROKER, testGenerators);
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
	ASStatGenerator generator = new ASStatGenerator(DUMMY_BROKER, testGenerators, 3, 12);

	// Notify for times we are not interested in...
	generator.notifyOfTime(2);
	generator.notifyOfTime(15);
	generator.notifyOfTime(17);

	// Should be empty now...
	assertTrue(generator.isEmpty());
	assertNull(generator.peek());
	assertNull(generator.poll());

	// Notify for times again.
	generator.notifyOfTime(2); // Not Interested
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
	ASStatGenerator generator = new ASStatGenerator(DUMMY_BROKER, testGenerators);

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

	//Allow for delta, because of using doubles, and rounding some of the numbers
	double delta = 1;
	assertEquals(GEN_RAM_MEAN, ramStat.getMean(), delta);
	assertEquals(GEN_RAM_STDEV, ramStat.getStandardDeviation(), delta);
	assertEquals(GEN_CPU_MEAN, cpuStat.getMean(), delta);
	assertEquals(GEN_CPU_STDEV, cpuStat.getStandardDeviation(), delta);

	// Assert we have exhausted the generator
	assertTrue(generator.isEmpty());
    }
}
