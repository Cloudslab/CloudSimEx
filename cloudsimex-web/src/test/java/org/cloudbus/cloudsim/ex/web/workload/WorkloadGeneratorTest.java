package org.cloudbus.cloudsim.ex.web.workload;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertArrayEquals;

import java.util.List;
import java.util.Map;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.cloudbus.cloudsim.ex.disk.DataItem;
import org.cloudbus.cloudsim.ex.util.helpers.TestUtil;
import org.cloudbus.cloudsim.ex.web.WebSession;
import org.cloudbus.cloudsim.ex.web.workload.freq.ConstFreqFunction;
import org.cloudbus.cloudsim.ex.web.workload.freq.FrequencyFunction;
import org.cloudbus.cloudsim.ex.web.workload.sessions.ConstSessionGenerator;
import org.cloudbus.cloudsim.ex.web.workload.sessions.ISessionGenerator;
import org.junit.Test;

/**
 * 
 * @author nikolay.grozev
 * 
 */
public class WorkloadGeneratorTest {

    private static final double DELTA = 0.1;
    private static final int REPEAT_STAT = 10000;

    private static final int FREQ_VALUE = 10;
    private static final int FREQ_UNIT = 100;

    private static final int AS_CLOUDLET_LENGTH = 10;
    private static final int AS_RAM = 12;
    private static final int DB_CLOUDLET_LENGTH = 15;
    private static final int DB_RAM = 25;
    private static final int DB_CLOUDLET_IO_LENGTH = 27;

    private static final String[] metadata = new String[] { "X", "Y" };

    private StatWorkloadGenerator workloadGenerator;
    private FrequencyFunction freqFunction;
    private ISessionGenerator sessionGenerator;

    private final DataItem data = new DataItem(65);

    @Test
    public void testConstantFreqFunScenario() {
        freqFunction = new ConstFreqFunction(FREQ_UNIT, FREQ_VALUE);
        sessionGenerator = new ConstSessionGenerator(AS_CLOUDLET_LENGTH, AS_RAM, DB_CLOUDLET_LENGTH, DB_RAM,
                DB_CLOUDLET_IO_LENGTH, false, data, metadata);

        workloadGenerator = new StatWorkloadGenerator(TestUtil.SEED_ARRAY, freqFunction, sessionGenerator);

        // Test with different ratios of the lengths
        testWebSessionGeneration(0.11);
        testWebSessionGeneration(1);
        testWebSessionGeneration(3);
        testWebSessionGeneration(5.7);
    }

    /**
     * 
     * @param timesLen
     *            - how big is the length compared to the unit of the frequency
     *            function
     */
    private void testWebSessionGeneration(final double timesLen) {
        double startTime = 15;
        double periodLen = FREQ_UNIT * timesLen;
        double expectedFreq = FREQ_VALUE * timesLen;

        DescriptiveStatistics countStat = new DescriptiveStatistics();
        for (int i = 0; i < REPEAT_STAT; i++) {
            Map<Double, List<WebSession>> result = workloadGenerator.generateSessions(startTime, periodLen);
            countStat.addValue(result.size());

            for (Map.Entry<Double, List<WebSession>> entry : result.entrySet()) {
                assertTrue(entry.getKey() > startTime - DELTA);
                assertTrue(entry.getKey() < startTime + periodLen + DELTA);

                // Assert the metadata has been generated successfully
                for (WebSession sess : entry.getValue()) {
                    assertArrayEquals(metadata, sess.getMetadata());
                }
            }
        }

        // Check that the mean is indeed as expected
        assertEquals(expectedFreq, countStat.getMean(), DELTA);
        // The variance converges less slowly... so we need bigger delta, or
        // more runs of the test. We choose bigger delta, since we want well
        // performing tests.
        double varDelta = 20 * DELTA;
        assertEquals(expectedFreq, countStat.getVariance(), varDelta);
    }

}
