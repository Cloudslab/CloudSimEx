package org.cloudbus.cloudsim.ex;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.ex.delay.ConstantVMBootDelay;
import org.cloudbus.cloudsim.ex.delay.IVMBootDelayDistribution;
import org.cloudbus.cloudsim.ex.vm.VMStatus;
import org.junit.Before;
import org.junit.Test;

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

    @Test
    public void testBootTime() {
        double delay = 10;
        IVMBootDelayDistribution delayDistribution = new ConstantVMBootDelay(delay);
        datacenter.setDelayDistribution(delayDistribution);

        int killTime1 = 30;
        int killTime2 = 55;

        int cloudlet1and2_Duration = 50;
        int cloudlet3_Duration = (int) (killTime1 - delay) / 2;

        Cloudlet cloudlet1 = createCloudlet(cloudlet1and2_Duration);
        Cloudlet cloudlet2 = createCloudlet(cloudlet1and2_Duration);
        Cloudlet cloudlet3 = createCloudlet(cloudlet3_Duration);
        cloudlet1.setUserId(broker.getId());
        cloudlet2.setUserId(broker.getId());
        cloudlet3.setUserId(broker.getId());

        cloudlet1.setVmId(vm1.getId());
        cloudlet2.setVmId(vm2.getId());
        cloudlet3.setVmId(vm1.getId());
        broker.submitCloudletList(Arrays.asList(cloudlet1, cloudlet2, cloudlet3));
        broker.destroyVMsAfter(Arrays.asList(vm1), killTime1);
        broker.destroyVMsAfter(Arrays.asList(vm2), killTime2);

        CloudSim.startSimulation();
        // List<Cloudlet> resultList = broker.getCloudletReceivedList();
        CloudSim.stopSimulation();

        assertEquals(VMStatus.TERMINATED, vm1.getStatus());
        assertEquals(0, vm1.getSubmissionTime(), 0.01);
        assertEquals(delay, vm1.getStartTime(), 0.01);
        assertEquals(killTime1, vm1.getEndTime(), 0.01);

        assertEquals(VMStatus.TERMINATED, vm2.getStatus());
        assertEquals(0, vm2.getSubmissionTime(), 0.01);
        assertEquals(delay, vm2.getStartTime(), 0.01);
        assertEquals(killTime2, vm2.getEndTime(), 0.01);

        // The cloudlet must have failed... as we killed the VM before it is
        // done
        assertEquals(Cloudlet.FAILED_RESOURCE_UNAVAILABLE, cloudlet1.getCloudletStatus());

        // The cloudlet must have succeeded... as the VM boot time took time ...
        assertEquals(Cloudlet.FAILED_RESOURCE_UNAVAILABLE, cloudlet2.getCloudletStatus());

        // The cloudlet must have succeeded... as we killed the VM after its end
        assertEquals(Cloudlet.SUCCESS, cloudlet3.getCloudletStatus());
        // We execute 2 cloudlets together on the same VM...
        double expectedEnd = vm1.getStartTime() + 2 * cloudlet3_Duration;
        assertEquals(expectedEnd, cloudlet3.getFinishTime(), 0.01);
    }

}
