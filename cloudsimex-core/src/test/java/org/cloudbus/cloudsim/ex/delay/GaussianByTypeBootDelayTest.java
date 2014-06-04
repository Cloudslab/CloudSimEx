package org.cloudbus.cloudsim.ex.delay;

import static org.apache.commons.lang3.tuple.ImmutablePair.of;
import static org.cloudbus.cloudsim.Consts.NIX_OS;
import static org.cloudbus.cloudsim.Consts.WINDOWS;
import static org.junit.Assert.*;

import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.cloudbus.cloudsim.ex.util.helpers.TestUtil;
import org.cloudbus.cloudsim.ex.vm.VMex;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class GaussianByTypeBootDelayTest {

    private static final double DEFAULT_DELAY = 5.1;

    private GaussianByTypeBootDelay bootDelay;

    private static final Map<Pair<String, String>, Pair<Double, Double>> DELAY_DEFS = ImmutableMap
            .<Pair<String, String>, Pair<Double, Double>> builder()
            // === Standard On-Demand Instances
            .put(of("m1.small", NIX_OS), of(100.0, 5.0)).put(of("m1.xlarge", NIX_OS), of(100.0, 7.0))

            // === Micro On-Demand Instances
            .put(of("t1.micro", NIX_OS), of(55.0, 3.0))

            // === High-Memory On-Demand Instances
            .put(of("m2.4xlarge", NIX_OS), of(110.0, 5.0))

            // === High-CPU On-Demand Instances
            .put(of("c1.medium", NIX_OS), of(90.0, 2.0)).put(of("c1.xlarge", NIX_OS), of(90.0, 2.0))

            // .put(of("hs1.8xlarge", NIX_OS), of())

            // === Wildcards
            .put(of((String) null, WINDOWS), of(810.2, 10.0)).build();

    @Before
    public void setUp() {
        bootDelay = new GaussianByTypeBootDelay(DELAY_DEFS, TestUtil.SEED_ARRAY, DEFAULT_DELAY);
    }

    @Test
    public void testBootDelay() {
        double delta = 0.1;
        DescriptiveStatistics stat = new DescriptiveStatistics();

        VMex vmex = new VMex("TEST_VM", 2, 3, 4, 5, 6, 7, "vmm", null);

        // Test the return of the default value
        vmex.getMetadata().setType("hs1.8xlarge");
        vmex.getMetadata().setOS(NIX_OS);
        assertEquals(DEFAULT_DELAY, bootDelay.getDelay(vmex), delta);

        // Test c1.medium
        stat.clear();
        vmex.getMetadata().setType("c1.medium");
        for (int i = 0; i < 10_000; i++) {
            stat.addValue(bootDelay.getDelay(vmex));
        }
        assertEquals(90.0, stat.getMean(), delta);
        assertEquals(2.0, stat.getStandardDeviation(), delta);

        // Test t1.micro
        stat.clear();
        vmex.getMetadata().setType("t1.micro");
        for (int i = 0; i < 10_000; i++) {
            stat.addValue(bootDelay.getDelay(vmex));
        }
        assertEquals(55.0, stat.getMean(), delta);
        assertEquals(3.0, stat.getStandardDeviation(), delta);

        // Test wildcard
        stat.clear();
        vmex.getMetadata().setOS(WINDOWS);
        for (int i = 0; i < 10_000; i++) {
            stat.addValue(bootDelay.getDelay(vmex));
        }
        assertEquals(810.2, stat.getMean(), delta);
        assertEquals(10.0, stat.getStandardDeviation(), delta);
    }

}
