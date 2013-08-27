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
import org.jfree.data.time.Hour;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class EC2OnDemandPolicyTest extends BaseBillingPolicyTest {

    @Before
    public void setUp() throws Exception {
	super.setUp();
    }

    @Test
    public void testEC2BillingSimpleScenario() {
	CloudSim.startSimulation();
	List<Cloudlet> resultList = broker.getCloudletReceivedList();
	CloudSim.stopSimulation();
	
	((VMex) vm1).getMetadata()[0] = "m1.small";
	((VMex) vm1).getMetadata()[1] = LINUX;
	((VMex) vm2).getMetadata()[0] = "m1.medium";
	((VMex) vm2).getMetadata()[1] = LINUX;
	
	double smallPrice = 0.065;
	double medPrice = 0.130;
	
	ImmutableMap<Pair<String, String>, BigDecimal> prices =
		ImmutableMap.<Pair<String, String>, BigDecimal> builder()
		.put(of("m1.small", LINUX), valueOf(smallPrice))
		.put(of("m1.medium", LINUX), valueOf(medPrice))
		.build();
	IVmBillingPolicy<VMex> policy = new EC2OnDemandPolicy(prices);
	BigDecimal bill = broker.bill(policy);
	
	// Compute expected bill
	int times = SIM_LENGTH / IVmBillingPolicy.HOUR + 1;
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
	
	int destroyVM1Time = IVmBillingPolicy.HOUR / 3;
	int destroyVM2Time = (IVmBillingPolicy.HOUR * 4) / 3;
	
	broker.destroyVMsAfter(Arrays.asList(vm1), destroyVM1Time);
	broker.destroyVMsAfter(Arrays.asList(vm2), destroyVM2Time);
	
	CloudSim.startSimulation();
	List<Cloudlet> resultList = broker.getCloudletReceivedList();
	CloudSim.stopSimulation();
	
	((VMex) vm1).getMetadata()[0] = "m1.small";
	((VMex) vm1).getMetadata()[1] = LINUX;
	((VMex) vm2).getMetadata()[0] = "m1.medium";
	((VMex) vm2).getMetadata()[1] = LINUX;
	
	double smallPrice = 0.065;
	double medPrice = 0.130;
	
	ImmutableMap<Pair<String, String>, BigDecimal> prices =
		ImmutableMap.<Pair<String, String>, BigDecimal> builder()
		.put(of("m1.small", LINUX), valueOf(smallPrice))
		.put(of("m1.medium", LINUX), valueOf(medPrice))
		.build();
	IVmBillingPolicy<VMex> policy = new EC2OnDemandPolicy(prices);
	BigDecimal bill = broker.bill(policy);
	
	// Compute expected bill
	int times1 = destroyVM1Time / IVmBillingPolicy.HOUR + 1;
	int times2 = destroyVM2Time / IVmBillingPolicy.HOUR + 1;
	double expectedBill = smallPrice * times1 + medPrice * times2;
	
	assertEquals(expectedBill, bill.doubleValue(), 0.01);
    }
}
