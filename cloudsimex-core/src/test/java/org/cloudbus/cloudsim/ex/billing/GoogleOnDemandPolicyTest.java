package org.cloudbus.cloudsim.ex.billing;

import static java.math.BigDecimal.valueOf;
import static org.apache.commons.lang3.tuple.ImmutablePair.of;
import static org.cloudbus.cloudsim.ex.billing.IVmBillingPolicy.LINUX;
import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.ex.vm.VMex;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class GoogleOnDemandPolicyTest extends BaseBillingPolicyTest {

    @Before
    public void setUp() throws Exception {
	super.setUp();
    }

    @Test
    public void testGoogleBillingSimpleScenario() {
	CloudSim.startSimulation();
	List<Cloudlet> resultList = broker.getCloudletReceivedList();
	CloudSim.stopSimulation();

	((VMex) vm1).getMetadata()[0] = "n1-standard-1-d";
	((VMex) vm1).getMetadata()[1] = LINUX;
	((VMex) vm2).getMetadata()[0] = "n1-standard-2-d";
	((VMex) vm2).getMetadata()[1] = LINUX;

	double d1PricePerHour = 0.132;
	double d2PricePerHour = 0.265;

	ImmutableMap<Pair<String, String>, BigDecimal> prices =
		ImmutableMap.<Pair<String, String>, BigDecimal> builder()
			.put(of("n1-standard-1-d", LINUX), valueOf(d1PricePerHour))
			.put(of("n1-standard-2-d", LINUX), valueOf(d2PricePerHour))
			.build();
	IVmBillingPolicy<VMex> policy = new GoogleOnDemandPolicy(prices);
	BigDecimal bill = broker.bill(policy);

	// Compute expected bill
	int times = Math.min(10, SIM_LENGTH / IVmBillingPolicy.MINUTE + 1);
	double expectedBill = times * (d1PricePerHour / 60.0 + d2PricePerHour / 60.0);

	assertEquals(expectedBill, bill.doubleValue(), 0.01);
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

	int destroyVM1Time = IVmBillingPolicy.HOUR * 10 / 3;
	int destroyVM2Time = IVmBillingPolicy.HOUR / 10;

	broker.destroyVMsAfter(Arrays.asList(vm1), destroyVM1Time);
	broker.destroyVMsAfter(Arrays.asList(vm2), destroyVM2Time);

	CloudSim.startSimulation();
	List<Cloudlet> resultList = broker.getCloudletReceivedList();
	CloudSim.stopSimulation();

	((VMex) vm1).getMetadata()[0] = "n1-standard-1-d";
	((VMex) vm1).getMetadata()[1] = LINUX;
	((VMex) vm2).getMetadata()[0] = "n1-standard-2-d";
	((VMex) vm2).getMetadata()[1] = LINUX;

	double d1Price = 0.132;
	double d2Price = 0.265;

	ImmutableMap<Pair<String, String>, BigDecimal> prices =
		ImmutableMap.<Pair<String, String>, BigDecimal> builder()
			.put(of("n1-standard-1-d", LINUX), valueOf(d1Price))
			.put(of("n1-standard-2-d", LINUX), valueOf(d2Price))
			.build();
	IVmBillingPolicy<VMex> policy = new GoogleOnDemandPolicy(prices);
	BigDecimal bill = broker.bill(policy);

	// Compute expected bill
	int times1 = Math.min(10, destroyVM1Time / IVmBillingPolicy.MINUTE + 1);
	int times2 = Math.min(10, destroyVM2Time / IVmBillingPolicy.MINUTE + 1);
	double expectedBill = (d1Price / 60) * times1 + (d2Price / 60) * times2;

	assertEquals(expectedBill, bill.doubleValue(), 0.01);
    }

}
