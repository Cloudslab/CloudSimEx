package org.cloudbus.cloudsim.ex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.ex.vm.MonitoredVMex;
import org.junit.Before;
import org.junit.Test;

public class MonitoringBorkerEXTest extends BaseDatacenterBrokerTest {

    private static final double MONITORING_PERIOD = 0.1;
    private static final double SUMMARY_PERIOD_LEN = 5;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void testWithLongRunningJobs() {
        int cloudletDuration = 100;
        double delta = 0.05;
        boolean printUtilisation = false;

        Cloudlet cloudlet1 = createCloudlet(cloudletDuration);
        Cloudlet cloudlet2 = createCloudlet(cloudletDuration);
        cloudlet1.setUserId(broker.getId());
        cloudlet2.setUserId(broker.getId());

        cloudlet1.setVmId(vm1.getId());
        cloudlet2.setVmId(vm1.getId());
        broker.submitCloudletList(Arrays.asList(cloudlet1, cloudlet2));

        ((MonitoringBorkerEX) broker).recordUtilisationPeriodically(1);

        CloudSim.startSimulation();
        List<Cloudlet> resultList = broker.getCloudletReceivedList();
        CloudSim.stopSimulation();

        assertEquals(2, resultList.size());
        assertEquals(Cloudlet.SUCCESS, cloudlet1.getCloudletStatus());
        assertEquals(Cloudlet.SUCCESS, cloudlet2.getCloudletStatus());

        for (Map.Entry<Double, Map<Integer, double[]>> e : ((MonitoringBorkerEX) broker).getRecordedUtilisations()
                .entrySet()) {
            // In the beginning it will be inaccurate ...
            double time = e.getKey();
            if (time < SUMMARY_PERIOD_LEN) {
                continue;
            }

            double[] vm1Observations = e.getValue().get(vm1.getId());
            double[] vm2Observations = e.getValue().get(vm2.getId());

            if (printUtilisation) {
                if (time <= 2 * cloudletDuration + SUMMARY_PERIOD_LEN) {
                    System.err.printf("Time=%.3f\tVM=%d;\tCPU=%.4f;\t", time, vm1.getId(), vm1Observations[0]);
                    System.err.printf("VM=%d;\tCPU=%.4f;\t\n", vm2.getId(), vm2Observations[0]);
                }
            }

            // Validate VM 1 ...
            if (time <= 2 * cloudletDuration) {
                assertEquals(1, vm1Observations[0], delta);
                assertEquals(0, vm1Observations[1], delta);
                assertEquals(0, vm1Observations[2], delta);
            } else if (time >= 2 * cloudletDuration + SUMMARY_PERIOD_LEN) {
                assertEquals(0, vm1Observations[0], delta);
                assertEquals(0, vm1Observations[1], delta);
                assertEquals(0, vm1Observations[2], delta);
            } else {
                assertTrue(vm1Observations[0] < 1);
                assertEquals(0, vm1Observations[1], delta);
                assertEquals(0, vm1Observations[2], delta);
            }

            // Validate VM 2 ...
            assertEquals(0, vm2Observations[0], delta);
            assertEquals(0, vm2Observations[1], delta);
            assertEquals(0, vm2Observations[2], delta);
        }

        // Test the internal representation of the monitored data.
        assertEquals(SUMMARY_PERIOD_LEN / MONITORING_PERIOD, (double) ((MonitoredVMex) vm1).getMonitoredData().size(),
                1);
        assertEquals(SUMMARY_PERIOD_LEN / MONITORING_PERIOD, (double) ((MonitoredVMex) vm1).getMonitoredData()
                .dataSize(), 5);
        assertEquals(SUMMARY_PERIOD_LEN / MONITORING_PERIOD, (double) ((MonitoredVMex) vm2).getMonitoredData().size(),
                1);
        assertEquals(SUMMARY_PERIOD_LEN / MONITORING_PERIOD, (double) ((MonitoredVMex) vm2).getMonitoredData()
                .dataSize(), 5);
    }

    @Test
    public void testWithConstantUtilisation() throws Exception {
        double expecteCPUUtilVM1 = 0.5;
        double expecteCPUUtilVM2 = 0.6;
        double delta = 0.1;
        boolean printUtilisation = false;

        super.setUp();
        testConstantUtilisation(expecteCPUUtilVM1, expecteCPUUtilVM2, delta, printUtilisation);

        expecteCPUUtilVM1 = 0.2;
        expecteCPUUtilVM2 = 0.1;
        super.setUp();
        testConstantUtilisation(expecteCPUUtilVM1, expecteCPUUtilVM2, delta, printUtilisation);

        expecteCPUUtilVM1 = 0.3;
        expecteCPUUtilVM2 = 0.45;
        super.setUp();
        testConstantUtilisation(expecteCPUUtilVM1, expecteCPUUtilVM2, delta, printUtilisation);

        expecteCPUUtilVM1 = 0.95;
        expecteCPUUtilVM2 = 0.85;
        super.setUp();
        testConstantUtilisation(expecteCPUUtilVM1, expecteCPUUtilVM2, delta, printUtilisation);
    }

    public void testConstantUtilisation(double expecteCPUUtilVM1, double expecteCPUUtilVM2, double delta,
            boolean printUtilisation) {
        // Each cloudlet will take only that much seconds
        double cloudletDuration = 0.6;

        double offset1 = 0.01;
        double offset2 = 0.02;

        double periodBetweenJobsVM1 = cloudletDuration / expecteCPUUtilVM1;
        double periodBetweenJobsVM2 = cloudletDuration / expecteCPUUtilVM2;
        int numCloudletsPerVM = 200;
        for (int i = 0; i < numCloudletsPerVM; i++) {
            Cloudlet cloudlet1 = createCloudlet(cloudletDuration);
            Cloudlet cloudlet2 = createCloudlet(cloudletDuration);
            cloudlet1.setUserId(broker.getId());
            cloudlet2.setUserId(broker.getId());

            cloudlet1.setVmId(vm1.getId());
            cloudlet2.setVmId(vm2.getId());

            double timeCloudLetVM1 = offset1 + i * periodBetweenJobsVM1;
            broker.submitCloudletList(Arrays.asList(cloudlet1), timeCloudLetVM1);

            double timeCloudLetVM2 = offset2 + i * periodBetweenJobsVM2;
            broker.submitCloudletList(Arrays.asList(cloudlet2), timeCloudLetVM2);
        }

        ((MonitoringBorkerEX) broker).recordUtilisationPeriodically(1);

        CloudSim.startSimulation();
        List<Cloudlet> resultList = broker.getCloudletReceivedList();
        CloudSim.stopSimulation();

        assertEquals(2 * numCloudletsPerVM, resultList.size());
        for (Cloudlet c : resultList) {
            assertEquals(Cloudlet.SUCCESS, c.getCloudletStatus());
        }

        for (Map.Entry<Double, Map<Integer, double[]>> e : ((MonitoringBorkerEX) broker).getRecordedUtilisations()
                .entrySet()) {
            // In the beginning it will be inaccurate ...
            double time = e.getKey();
            if (time <= SUMMARY_PERIOD_LEN) {
                continue;
            }

            double[] vm1Observations = e.getValue().get(vm1.getId());
            double[] vm2Observations = e.getValue().get(vm2.getId());

            if (printUtilisation) {
                if (time <= 2 * numCloudletsPerVM + offset2) {
                    System.err.printf("Time=%.3f\tVM=%d;\tCPU=%.4f;\t", time, vm1.getId(), vm1Observations[0]);
                    System.err.printf("VM=%d;\tCPU=%.4f;\t\n", vm2.getId(), vm2Observations[0]);
                }
            }

            // Validate VM 1 ...
            if (time < numCloudletsPerVM * periodBetweenJobsVM1 + offset1) {
                assertEquals(expecteCPUUtilVM1, vm1Observations[0], delta);
                assertEquals(0, vm1Observations[1], delta);
                assertEquals(0, vm1Observations[2], delta);
            } else if (time > numCloudletsPerVM * periodBetweenJobsVM1 + SUMMARY_PERIOD_LEN + offset1) {
                assertEquals(0, vm1Observations[0], delta);
                assertEquals(0, vm1Observations[1], delta);
                assertEquals(0, vm1Observations[2], delta);
            } else {
                assertTrue(vm1Observations[0] < expecteCPUUtilVM1);
                assertEquals(0, vm1Observations[1], delta);
                assertEquals(0, vm1Observations[2], delta);
            }

            // Validate VM 2 ...
            if (time < numCloudletsPerVM * periodBetweenJobsVM2 + offset2) {
                assertEquals(expecteCPUUtilVM2, vm2Observations[0], delta);
                assertEquals(0, vm2Observations[1], delta);
                assertEquals(0, vm2Observations[2], delta);
            } else if (time > numCloudletsPerVM * periodBetweenJobsVM2 + SUMMARY_PERIOD_LEN + offset2) {
                assertEquals(0, vm2Observations[0], delta);
                assertEquals(0, vm2Observations[1], delta);
                assertEquals(0, vm2Observations[2], delta);
            } else {
                assertTrue(vm2Observations[0] < expecteCPUUtilVM2);
                assertEquals(0, vm2Observations[1], delta);
                assertEquals(0, vm2Observations[2], delta);
            }
        }

        // Test the internal representation of the monitored data.
        assertEquals(SUMMARY_PERIOD_LEN / MONITORING_PERIOD, (double) ((MonitoredVMex) vm1).getMonitoredData().size(),
                1);
        assertEquals(SUMMARY_PERIOD_LEN / MONITORING_PERIOD, (double) ((MonitoredVMex) vm1).getMonitoredData()
                .dataSize(), 5);
        assertEquals(SUMMARY_PERIOD_LEN / MONITORING_PERIOD, (double) ((MonitoredVMex) vm2).getMonitoredData().size(),
                1);
        assertEquals(SUMMARY_PERIOD_LEN / MONITORING_PERIOD, (double) ((MonitoredVMex) vm2).getMonitoredData()
                .dataSize(), 5);
    }

    @Override
    protected MonitoredVMex createVM() {
        int pesNumber = 1; // number of cpus
        String vmm = "Xen"; // VMM name

        return new MonitoredVMex("Test", broker.getId(), VM_MIPS, pesNumber, VM_RAM, VM_BW, VM_SIZE, vmm,
                new CloudletSchedulerTimeShared(), SUMMARY_PERIOD_LEN);
    }

    @Override
    protected DatacenterBrokerEX createBroker() throws Exception {
        // return new DatacenterBrokerEX("Broker", SIM_LENGTH);
        return new MonitoringBorkerEX("Broker", SIM_LENGTH, MONITORING_PERIOD, MONITORING_PERIOD);
    }

}
