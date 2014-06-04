package org.cloudbus.cloudsim.ex.billing;

import static java.math.BigDecimal.valueOf;
import static org.apache.commons.lang3.tuple.ImmutablePair.of;
import static org.cloudbus.cloudsim.Consts.HOUR;
import static org.cloudbus.cloudsim.Consts.MINUTE;
import static org.cloudbus.cloudsim.Consts.NIX_OS;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.commons.lang3.tuple.Pair;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.ex.BaseDatacenterBrokerTest;
import org.cloudbus.cloudsim.ex.delay.ConstantVMBootDelay;
import org.cloudbus.cloudsim.ex.vm.VMStatus;
import org.cloudbus.cloudsim.ex.vm.VMex;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class GoogleOnDemandPolicyTest extends BaseDatacenterBrokerTest {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void testGoogleBillingSimpleScenario() {
        CloudSim.startSimulation();
        // List<Cloudlet> resultList = broker.getCloudletReceivedList();
        CloudSim.stopSimulation();

        vm1.getMetadata().setType("n1-standard-1-d");
        vm1.getMetadata().setOS(NIX_OS);
        vm2.getMetadata().setType("n1-standard-2-d");
        vm2.getMetadata().setOS(NIX_OS);

        double d1PricePerHour = 0.132;
        double d2PricePerHour = 0.265;

        ImmutableMap<Pair<String, String>, BigDecimal> prices = ImmutableMap
                .<Pair<String, String>, BigDecimal> builder()
                .put(of("n1-standard-1-d", NIX_OS), valueOf(d1PricePerHour))
                .put(of("n1-standard-2-d", NIX_OS), valueOf(d2PricePerHour)).build();
        IVmBillingPolicy policy = new GoogleOnDemandPolicy(prices);
        broker.setVMBillingPolicy(policy);
        BigDecimal bill = broker.bill();

        // Compute expected bill
        int times = Math.max(10, SIM_LENGTH / MINUTE + 1);
        double expectedBill = times * (d1PricePerHour / 60.0 + d2PricePerHour / 60.0);

        assertEquals(expectedBill, bill.doubleValue(), 0.01);

        // Now test billing before time...
        assertEquals((d1PricePerHour + d2PricePerHour) * 0.4, policy.bill(Arrays.asList(vm1, vm2), Consts.HOUR * 0.4)
                .doubleValue(), 0.01);
        assertEquals((d1PricePerHour + d2PricePerHour) * (1 / 6d),
                policy.bill(Arrays.asList(vm1, vm2), Consts.HOUR / 10).doubleValue(), 0.01);
    }

    @Test
    public void testGoogleBillingBothVMStoppedScenario() {
        // Run some workload - should not affect the billing
        int firstTwoCloudletDur = 50;
        Cloudlet cloudlet1 = createCloudlet(firstTwoCloudletDur);
        Cloudlet cloudlet2 = createCloudlet(firstTwoCloudletDur);
        cloudlet1.setUserId(broker.getId());
        cloudlet2.setUserId(broker.getId());

        cloudlet1.setVmId(vm1.getId());
        cloudlet2.setVmId(vm2.getId());
        broker.submitCloudletList(Arrays.asList(cloudlet1, cloudlet2));

        int destroyVM1Time = HOUR * 10 / 3;
        int destroyVM2Time = HOUR / 10;

        broker.destroyVMsAfter(Arrays.asList(vm1), destroyVM1Time);
        broker.destroyVMsAfter(Arrays.asList(vm2), destroyVM2Time);

        CloudSim.startSimulation();
        // List<Cloudlet> resultList = broker.getCloudletReceivedList();
        CloudSim.stopSimulation();

        vm1.getMetadata().setType("n1-standard-1-d");
        vm1.getMetadata().setOS(NIX_OS);
        vm2.getMetadata().setType("n1-standard-2-d");
        vm2.getMetadata().setOS(NIX_OS);

        double d1Price = 0.132;
        double d2Price = 0.265;

        ImmutableMap<Pair<String, String>, BigDecimal> prices = ImmutableMap
                .<Pair<String, String>, BigDecimal> builder().put(of("n1-standard-1-d", NIX_OS), valueOf(d1Price))
                .put(of("n1-standard-2-d", NIX_OS), valueOf(d2Price)).build();
        IVmBillingPolicy policy = new GoogleOnDemandPolicy(prices);
        broker.setVMBillingPolicy(policy);
        BigDecimal bill = broker.bill();

        // Compute expected bill
        int times1 = Math.max(10, destroyVM1Time / MINUTE + 1);
        int times2 = Math.max(10, destroyVM2Time / MINUTE + 1);
        double expectedBill = (d1Price / 60) * times1 + (d2Price / 60) * times2;

        assertEquals(expectedBill, bill.doubleValue(), 0.01);
    }

    @Test
    public void testGoogleBillingBothVMStoppedScenarioWithDelay() {
        int delay = 10;
        datacenter.setDelayDistribution(new ConstantVMBootDelay(delay));

        // Run some workload - should not affect the billing
        int firstTwoCloudletDur = 50;
        Cloudlet cloudlet1 = createCloudlet(firstTwoCloudletDur);
        Cloudlet cloudlet2 = createCloudlet(firstTwoCloudletDur);
        cloudlet1.setUserId(broker.getId());
        cloudlet2.setUserId(broker.getId());

        cloudlet1.setVmId(vm1.getId());
        cloudlet2.setVmId(vm2.getId());
        broker.submitCloudletList(Arrays.asList(cloudlet1, cloudlet2));

        int destroyVM1Time = HOUR * 10 / 3;
        int destroyVM2Time = HOUR / 10;

        broker.destroyVMsAfter(Arrays.asList(vm1), destroyVM1Time);
        broker.destroyVMsAfter(Arrays.asList(vm2), destroyVM2Time);

        CloudSim.startSimulation();
        // List<Cloudlet> resultList = broker.getCloudletReceivedList();
        CloudSim.stopSimulation();

        vm1.getMetadata().setType("n1-standard-1-d");
        vm1.getMetadata().setOS(NIX_OS);
        vm2.getMetadata().setType("n1-standard-2-d");
        vm2.getMetadata().setOS(NIX_OS);

        double d1Price = 0.132;
        double d2Price = 0.265;

        ImmutableMap<Pair<String, String>, BigDecimal> prices = ImmutableMap
                .<Pair<String, String>, BigDecimal> builder().put(of("n1-standard-1-d", NIX_OS), valueOf(d1Price))
                .put(of("n1-standard-2-d", NIX_OS), valueOf(d2Price)).build();
        IVmBillingPolicy policy = new GoogleOnDemandPolicy(prices);
        broker.setVMBillingPolicy(policy);
        BigDecimal bill = broker.bill();

        // Compute expected bill
        int times1 = Math.max(10, (destroyVM1Time - delay) / MINUTE + 1);
        int times2 = Math.max(10, (destroyVM2Time - delay) / MINUTE + 1);
        double expectedBill = (d1Price / 60) * times1 + (d2Price / 60) * times2;

        assertEquals(expectedBill, bill.doubleValue(), 0.01);
    }

    @Test
    public void testGoogleBillingMultipleVMs() {

        int destroyVM1Time = HOUR + 5;
        int destroyVM2Time = HOUR / 10;

        broker.destroyVMsAfter(Arrays.asList(vm1), destroyVM1Time);
        broker.destroyVMsAfter(Arrays.asList(vm2), destroyVM2Time);

        vm1.getMetadata().setType("n1-standard-1-d");
        vm1.getMetadata().setOS(NIX_OS);
        vm2.getMetadata().setType("n1-standard-2-d");
        vm2.getMetadata().setOS(NIX_OS);

        double d1Price = 0.132;
        double d2Price = 0.265;

        int times1 = Math.max(10, destroyVM1Time / MINUTE + 1);
        int times2 = Math.max(10, destroyVM2Time / MINUTE + 1);
        double expectedBill = (d1Price / 60) * times1 + (d2Price / 60) * times2;

        double timeDelta = HOUR * 2.3 + 5;
        for (int i = 0; i < 8; i++) {
            VMex vmNew1 = createVM();
            VMex vmNew2 = createVM();

            vmNew1.getMetadata().setType("n1-standard-1-d");
            vmNew1.getMetadata().setOS(NIX_OS);
            vmNew2.getMetadata().setType("n1-standard-2-d");
            vmNew2.getMetadata().setOS(NIX_OS);

            broker.createVmsAfter(Arrays.asList(vmNew1, vmNew2), (i + 1) * timeDelta);
            broker.destroyVMsAfter(Arrays.asList(vmNew1, vmNew2), (i + 2) * timeDelta);
            int times = Math.max(10, (int) timeDelta / MINUTE + 1);
            expectedBill += (d1Price / 60) * times + (d2Price / 60) * times;
        }

        ImmutableMap<Pair<String, String>, BigDecimal> prices = ImmutableMap
                .<Pair<String, String>, BigDecimal> builder().put(of("n1-standard-1-d", NIX_OS), valueOf(d1Price))
                .put(of("n1-standard-2-d", NIX_OS), valueOf(d2Price)).build();
        IVmBillingPolicy policy = new GoogleOnDemandPolicy(prices);

        CloudSim.startSimulation();
        // List<Cloudlet> resultList = broker.getCloudletReceivedList();
        CloudSim.stopSimulation();
        broker.setVMBillingPolicy(policy);
        BigDecimal bill = broker.bill();

        assertEquals(expectedBill, bill.doubleValue(), 0.01);
    }

    @Test
    public void testNormalisedCostPerMinute() {
        vm1.getMetadata().setType("n1-standard-1-d");
        vm1.getMetadata().setOS(NIX_OS);
        vm2.getMetadata().setType("n1-standard-2-d");
        vm2.getMetadata().setOS(NIX_OS);

        double d1PricePerHour = 0.132;
        double d2PricePerHour = 0.265;

        ImmutableMap<Pair<String, String>, BigDecimal> prices = ImmutableMap
                .<Pair<String, String>, BigDecimal> builder()
                .put(of("n1-standard-1-d", NIX_OS), valueOf(d1PricePerHour))
                .put(of("n1-standard-2-d", NIX_OS), valueOf(d2PricePerHour)).build();
        IVmBillingPolicy policy = new GoogleOnDemandPolicy(prices);
        assertEquals(d1PricePerHour / 60, policy.normalisedCostPerMinute(vm1).doubleValue(), 0.01);
        assertEquals(d2PricePerHour / 60, policy.normalisedCostPerMinute(vm2).doubleValue(), 0.01);
    }

    @Test
    public void testNexChargeTime() {
        final Queue<Double> vmTimes = new LinkedList<>(Arrays.asList(0d, 30d * MINUTE, 100d * MINUTE));
        VMex vmMock = new VMex("Test", broker.getId(), VM_MIPS, 1, VM_RAM, VM_BW, VM_SIZE, "Test",
                new CloudletSchedulerTimeShared()) {
            @Override
            protected double getCurrentTime() {
                return vmTimes.poll();
            }
        };

        final Queue<Double> policyTimes = new LinkedList<>(Arrays.asList(35d * MINUTE + 1, 60d * MINUTE + 1,
                91d * MINUTE + 1, 101d * MINUTE + 1));
        IVmBillingPolicy policyMock = new GoogleOnDemandPolicy(ImmutableMap.<Pair<String, String>, BigDecimal> of()) {
            @Override
            protected double getCurrentTime() {
                return policyTimes.poll();
            }
        };

        // Asks when the VM is still in init phase
        assertEquals(-1d, policyMock.nexChargeTime(vmMock), 0.01);

        // Sets the RUNNING status 30min after start
        vmMock.setStatus(VMStatus.RUNNING);

        // Asks 35 mins after start - the VM is now running
        assertEquals(40d * MINUTE, policyMock.nexChargeTime(vmMock), 0.01);

        // Asks 60 mins after start - the VM is now running
        assertEquals(61d * MINUTE, policyMock.nexChargeTime(vmMock), 0.01);

        // Asks 91 mins after start - the VM is now running
        assertEquals(92d * MINUTE, policyMock.nexChargeTime(vmMock), 0.01);

        // Sets the TERMINATED status 100min after start
        vmMock.setStatus(VMStatus.TERMINATED);

        // Asks 101 mins after start - the VM is now stopped
        assertEquals(-1d, policyMock.nexChargeTime(vmMock), 0.01);
    }

}
