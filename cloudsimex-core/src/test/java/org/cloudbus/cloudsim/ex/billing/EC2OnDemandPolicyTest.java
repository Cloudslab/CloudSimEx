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

public class EC2OnDemandPolicyTest extends BaseDatacenterBrokerTest {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void testEC2BillingSimpleScenario() {
        CloudSim.startSimulation();
        // List<Cloudlet> resultList = broker.getCloudletReceivedList();
        CloudSim.stopSimulation();

        vm1.getMetadata().setType("m1.small");
        vm1.getMetadata().setOS(NIX_OS);
        vm2.getMetadata().setType("m1.medium");
        vm2.getMetadata().setOS(NIX_OS);

        double smallPrice = 0.065;
        double medPrice = 0.130;

        ImmutableMap<Pair<String, String>, BigDecimal> prices = ImmutableMap
                .<Pair<String, String>, BigDecimal> builder().put(of("m1.small", NIX_OS), valueOf(smallPrice))
                .put(of("m1.medium", NIX_OS), valueOf(medPrice)).build();
        IVmBillingPolicy policy = new EC2OnDemandPolicy(prices);
        broker.setVMBillingPolicy(policy);
        BigDecimal bill = broker.bill();

        // Compute expected bill
        int times = SIM_LENGTH / HOUR + 1;
        double expectedBill = times * (smallPrice + medPrice);

        assertEquals(expectedBill, bill.doubleValue(), 0.01);

        // Now test billing before time...
        assertEquals((smallPrice + medPrice) * 2,
                policy.bill(Arrays.asList(vm1, vm2), Consts.HOUR * 1.4).doubleValue(), 0.01);
    }

    @Test
    public void testEC2BillingSimpleScenarioWithDelay() {
        int delay = 10;
        datacenter.setDelayDistribution(new ConstantVMBootDelay(delay));

        CloudSim.startSimulation();
        // List<Cloudlet> resultList = broker.getCloudletReceivedList();
        CloudSim.stopSimulation();

        vm1.getMetadata().setType("m1.small");
        vm1.getMetadata().setOS(NIX_OS);
        vm2.getMetadata().setType("m1.medium");
        vm2.getMetadata().setOS(NIX_OS);

        double smallPrice = 0.065;
        double medPrice = 0.130;

        ImmutableMap<Pair<String, String>, BigDecimal> prices = ImmutableMap
                .<Pair<String, String>, BigDecimal> builder().put(of("m1.small", NIX_OS), valueOf(smallPrice))
                .put(of("m1.medium", NIX_OS), valueOf(medPrice)).build();
        IVmBillingPolicy policy = new EC2OnDemandPolicy(prices);
        broker.setVMBillingPolicy(policy);
        BigDecimal bill = broker.bill();

        // Compute expected bill
        int times = (SIM_LENGTH - delay) / HOUR + 1;
        double expectedBill = times * (smallPrice + medPrice);

        assertEquals(expectedBill, bill.doubleValue(), 0.01);
    }

    @Test
    public void testEC2BillingBothVMStoppedScenario() {
        // Run some workload - should not affect the billing
        int firstTwoCloudletDur = 50;
        Cloudlet cloudlet1 = createCloudlet(firstTwoCloudletDur);
        Cloudlet cloudlet2 = createCloudlet(firstTwoCloudletDur);
        cloudlet1.setUserId(broker.getId());
        cloudlet2.setUserId(broker.getId());

        cloudlet1.setVmId(vm1.getId());
        cloudlet2.setVmId(vm2.getId());
        broker.submitCloudletList(Arrays.asList(cloudlet1, cloudlet2));

        int destroyVM1Time = HOUR / 3;
        int destroyVM2Time = (HOUR * 4) / 3;

        broker.destroyVMsAfter(Arrays.asList(vm1), destroyVM1Time);
        broker.destroyVMsAfter(Arrays.asList(vm2), destroyVM2Time);

        CloudSim.startSimulation();
        // List<Cloudlet> resultList = broker.getCloudletReceivedList();
        CloudSim.stopSimulation();

        vm1.getMetadata().setType("m1.small");
        vm1.getMetadata().setOS(NIX_OS);
        vm2.getMetadata().setType("m1.medium");
        vm2.getMetadata().setOS(NIX_OS);

        double smallPrice = 0.065;
        double medPrice = 0.130;

        ImmutableMap<Pair<String, String>, BigDecimal> prices = ImmutableMap
                .<Pair<String, String>, BigDecimal> builder().put(of("m1.small", NIX_OS), valueOf(smallPrice))
                .put(of("m1.medium", NIX_OS), valueOf(medPrice)).build();
        IVmBillingPolicy policy = new EC2OnDemandPolicy(prices);
        broker.setVMBillingPolicy(policy);
        BigDecimal bill = broker.bill();

        // Compute expected bill
        int times1 = destroyVM1Time / HOUR + 1;
        int times2 = destroyVM2Time / HOUR + 1;
        double expectedBill = smallPrice * times1 + medPrice * times2;

        assertEquals(expectedBill, bill.doubleValue(), 0.01);
    }

    @Test
    public void testEC2BillingMultipleVMs() {

        int destroyVM1Time = HOUR + 5;
        int destroyVM2Time = HOUR / 4;

        broker.destroyVMsAfter(Arrays.asList(vm1), destroyVM1Time);
        broker.destroyVMsAfter(Arrays.asList(vm2), destroyVM2Time);

        vm1.getMetadata().setType("m1.small");
        vm1.getMetadata().setOS(NIX_OS);
        vm2.getMetadata().setType("m1.medium");
        vm2.getMetadata().setOS(NIX_OS);

        double smallPrice = 0.065;
        double medPrice = 0.130;

        int times1 = destroyVM1Time / HOUR + 1;
        int times2 = destroyVM2Time / HOUR + 1;
        double expectedBill = smallPrice * times1 + medPrice * times2;

        double timeDelta = HOUR * 2.3;

        for (int i = 0; i < 8; i++) {
            VMex vmNew1 = createVM();
            VMex vmNew2 = createVM();

            vmNew1.getMetadata().setType("m1.small");
            vmNew1.getMetadata().setOS(NIX_OS);
            vmNew2.getMetadata().setType("m1.medium");
            vmNew2.getMetadata().setOS(NIX_OS);

            broker.createVmsAfter(Arrays.asList(vmNew1, vmNew2), (i + 1) * timeDelta);
            broker.destroyVMsAfter(Arrays.asList(vmNew1, vmNew2), (i + 2) * timeDelta);
            int times = (int) timeDelta / HOUR + 1;
            expectedBill += smallPrice * times + medPrice * times;
        }

        ImmutableMap<Pair<String, String>, BigDecimal> prices = ImmutableMap
                .<Pair<String, String>, BigDecimal> builder().put(of("m1.small", NIX_OS), valueOf(smallPrice))
                .put(of("m1.medium", NIX_OS), valueOf(medPrice)).build();
        IVmBillingPolicy policy = new EC2OnDemandPolicy(prices);

        CloudSim.startSimulation();
        // List<Cloudlet> resultList = broker.getCloudletReceivedList();
        CloudSim.stopSimulation();
        broker.setVMBillingPolicy(policy);
        BigDecimal bill = broker.bill();

        assertEquals(expectedBill, bill.doubleValue(), 0.01);
    }

    @Test
    public void testNormalisedCostPerMinute() {
        vm1.getMetadata().setType("m1.small");
        vm1.getMetadata().setOS(NIX_OS);
        vm2.getMetadata().setType("m1.medium");
        vm2.getMetadata().setOS(NIX_OS);

        double smallPrice = 0.065;
        double medPrice = 0.130;

        ImmutableMap<Pair<String, String>, BigDecimal> prices = ImmutableMap
                .<Pair<String, String>, BigDecimal> builder().put(of("m1.small", NIX_OS), valueOf(smallPrice))
                .put(of("m1.medium", NIX_OS), valueOf(medPrice)).build();
        IVmBillingPolicy policy = new EC2OnDemandPolicy(prices);
        assertEquals(smallPrice / 60, policy.normalisedCostPerMinute(vm1).doubleValue(), 0.01);
        assertEquals(medPrice / 60, policy.normalisedCostPerMinute(vm2).doubleValue(), 0.01);
    }

    @Test
    public void testNexChargeTime() {
        final Queue<Double> vmTimes = new LinkedList<>(Arrays.asList(0d, 30d * MINUTE, 100d * MINUTE));
        VMex vmMock = new VMex("TestVM", broker.getId(), VM_MIPS, 1, VM_RAM, VM_BW, VM_SIZE, "Test",
                new CloudletSchedulerTimeShared()) {
            @Override
            protected double getCurrentTime() {
                return vmTimes.poll();
            }
        };

        final Queue<Double> policyTimes = new LinkedList<>(Arrays.asList(35d * MINUTE + 1, 60d * MINUTE + 1,
                91d * MINUTE + 1, 101d * MINUTE + 1));
        IVmBillingPolicy policyMock = new EC2OnDemandPolicy(ImmutableMap.<Pair<String, String>, BigDecimal> of()) {
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
        assertEquals(90d * MINUTE, policyMock.nexChargeTime(vmMock), 0.01);

        // Asks 60 mins after start - the VM is now running
        assertEquals(90d * MINUTE, policyMock.nexChargeTime(vmMock), 0.01);

        // Asks 91 mins after start - the VM is now running
        assertEquals(150d * MINUTE, policyMock.nexChargeTime(vmMock), 0.01);

        // Sets the TERMINATED status 100min after start
        vmMock.setStatus(VMStatus.TERMINATED);

        // Asks 101 mins after start - the VM is now stopped
        assertEquals(-1d, policyMock.nexChargeTime(vmMock), 0.01);
    }

}
