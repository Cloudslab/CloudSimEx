package org.cloudbus.cloudsim.ex;

import static java.math.BigDecimal.valueOf;
import static org.apache.commons.lang3.tuple.ImmutablePair.of;
import static org.cloudbus.cloudsim.ex.billing.IVmBillingPolicy.LINUX;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.ex.billing.EC2OnDemandPolicy;
import org.cloudbus.cloudsim.ex.billing.IVmBillingPolicy;
import org.cloudbus.cloudsim.ex.vm.VMex;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class DatacenterBrokerEXTest extends BaseDatacenterBrokerTest {

    @Before
    public void setUp() throws Exception {
	super.setUp();
    }
    
    @Test
    public void testTwoVmOneFail() {
	int cloudletDuration = 100;
	int killTime = 30;

	Cloudlet cloudlet1 = createCloudlet(cloudletDuration);
	Cloudlet cloudlet2 = createCloudlet(cloudletDuration);
	cloudlet1.setUserId(broker.getId());
	cloudlet2.setUserId(broker.getId());

	cloudlet1.setVmId(vm1.getId());
	cloudlet2.setVmId(vm2.getId());
	broker.submitCloudletList(Arrays.asList(cloudlet1, cloudlet2));
	broker.destroyVMsAfter(Arrays.asList(vm1), killTime);

	CloudSim.startSimulation();
	List<Cloudlet> resultList = broker.getCloudletReceivedList();
	CloudSim.stopSimulation();

	assertEquals(2, resultList.size());

	// The cloudlet must have failed... as we killed the VM before it is
	// done
	assertEquals(Cloudlet.FAILED_RESOURCE_UNAVAILABLE, cloudlet1.getCloudletStatus());

	// The cloudlet must have succeeded... as we did not killed the VM
	assertEquals(Cloudlet.SUCCESS, cloudlet2.getCloudletStatus());
	assertEquals(cloudletDuration, cloudlet2.getFinishTime(), 1);
    }

    @Test
    public void testTwoVmBothFail() {
	int cloudletDuration = 50;
	int killTime1 = 30;
	int killTime2 = 55;

	Cloudlet cloudlet1 = createCloudlet(cloudletDuration);
	Cloudlet cloudlet2 = createCloudlet(cloudletDuration);
	cloudlet1.setUserId(broker.getId());
	cloudlet2.setUserId(broker.getId());

	cloudlet1.setVmId(vm1.getId());
	cloudlet2.setVmId(vm2.getId());
	broker.submitCloudletList(Arrays.asList(cloudlet1, cloudlet2));
	broker.destroyVMsAfter(Arrays.asList(vm1), killTime1);
	broker.destroyVMsAfter(Arrays.asList(vm2), killTime2);

	CloudSim.startSimulation();
	List<Cloudlet> resultList = broker.getCloudletReceivedList();
	CloudSim.stopSimulation();

	assertEquals(2, resultList.size());

	// The cloudlet must have failed... as we killed the VM before it is
	// done
	assertEquals(Cloudlet.FAILED_RESOURCE_UNAVAILABLE, cloudlet1.getCloudletStatus());

	// The cloudlet must have succeeded... as we killed the VM after its end
	assertEquals(Cloudlet.SUCCESS, cloudlet2.getCloudletStatus());
	assertEquals(cloudletDuration, cloudlet2.getFinishTime(), 1);
    }

    @Test
    public void testVmsAreShutProperly() {
	int firstTwoCloudletDur = 50;
	Cloudlet cloudlet1 = createCloudlet(firstTwoCloudletDur);
	Cloudlet cloudlet2 = createCloudlet(firstTwoCloudletDur);
	cloudlet1.setUserId(broker.getId());
	cloudlet2.setUserId(broker.getId());

	cloudlet1.setVmId(vm1.getId());
	cloudlet2.setVmId(vm2.getId());
	broker.submitCloudletList(Arrays.asList(cloudlet1, cloudlet2));

	// Have everything clean after 10 secs
	broker.destroyVMsAfter(Arrays.asList(vm1, vm2), 10);

	int vmNum = HOST_MIPS / VM_MIPS;
	List<Vm> vms = createVms(vmNum);
	broker.createVmsAfter(new ArrayList<>(vms), 11);

	List<Cloudlet> cloudlets = new ArrayList<>();

	// This loop should not result in an error for nor resources, since we
	// drop and create a VM on each iteration.
	double period = 12;
	double cloudletDur = 2 * period;
	for (int i = 0; i < 10; i++) {
	    double destroyTime = (i + 2) * period;
	    Vm first = vms.remove(0);
	    broker.destroyVMsAfter(Arrays.asList(first), destroyTime);

	    double startTime = destroyTime + 1;
	    Vm newVM = createVM();
	    vms.add(newVM);
	    broker.createVmsAfter(Arrays.asList(newVM), startTime);

	    Cloudlet cloudlet = createCloudlet(cloudletDur);
	    cloudlet.setVmId(newVM.getId());
	    cloudlet.setUserId(broker.getId());
	    cloudlets.add(cloudlet);
	    broker.submitCloudletList(Arrays.asList(cloudlet), startTime + 0.1);
	}

	CloudSim.startSimulation();
	List<Cloudlet> resultList = broker.getCloudletReceivedList();
	CloudSim.stopSimulation();

	assertEquals(2 + cloudlets.size(), resultList.size());

	// The cloudlets must have failed... as we killed the VMs before they
	// are done
	assertEquals(Cloudlet.FAILED_RESOURCE_UNAVAILABLE, cloudlet1.getCloudletStatus());
	assertEquals(Cloudlet.FAILED_RESOURCE_UNAVAILABLE, cloudlet2.getCloudletStatus());

	for (Cloudlet cloudlet : cloudlets) {
	    assertEquals(Cloudlet.SUCCESS, cloudlet.getCloudletStatus());
	    assertEquals(cloudletDur, cloudlet.getFinishTime() - cloudlet.getSubmissionTime(), 1);
	}
    }


}
