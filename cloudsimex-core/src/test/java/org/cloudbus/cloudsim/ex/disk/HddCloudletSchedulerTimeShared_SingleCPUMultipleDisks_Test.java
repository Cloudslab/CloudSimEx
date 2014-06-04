package org.cloudbus.cloudsim.ex.disk;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.ex.DatacenterEX;
import org.cloudbus.cloudsim.ex.util.CustomLog;
import org.cloudbus.cloudsim.ex.util.Id;
import org.cloudbus.cloudsim.ex.util.helpers.TestUtil;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

/**
 * 
 * @author nikolay.grozev
 * 
 */
public class HddCloudletSchedulerTimeShared_SingleCPUMultipleDisks_Test {

    private static final double DELTA = 0.01;

    protected DatacenterEX datacenter;
    protected DatacenterBroker broker;
    protected HddVm vm1;
    protected HddVm vm2;

    // These two items reside on the first disk
    private static final int ITEM_SIZE = 5;
    protected static DataItem dataItem_1_1 = new DataItem(ITEM_SIZE);
    protected static DataItem dataItem_1_2 = new DataItem(ITEM_SIZE);

    // These two items reside on the second disk
    protected static DataItem dataItem_2_1 = new DataItem(ITEM_SIZE);
    protected static DataItem dataItem_2_2 = new DataItem(ITEM_SIZE);

    private static final int HOST_MIPS = 1000;
    private static final int HOST_MIOPS = 100;
    private static final int HOST_RAM = 2048; // host memory (MB)
    private static final long HOST_STORAGE = 1000000; // host storage
    private static final int HOST_BW = 10000;

    private static final int VM_MIPS = 250;
    private static final long VM_SIZE = 10000;
    private static final int VM_RAM = 512;
    private static final long VM_BW = 1000;

    // The two disks attached to the host
    private static HddPe disk1 = new HddPe(new PeProvisionerSimple(HOST_MIOPS), dataItem_1_1, dataItem_1_2);
    private static HddPe disk2 = new HddPe(new PeProvisionerSimple(HOST_MIOPS), dataItem_2_1, dataItem_2_2);

    private HddCloudlet cloudlet1;
    private HddCloudlet cloudlet2;
    private HddCloudlet cloudlet3;
    private HddCloudlet cloudlet4;

    @Before
    public void setUp() throws Exception {
        CustomLog.configLogger(TestUtil.LOG_PROPS);

        int numBrokers = 1;
        boolean trace_flag = false;

        CloudSim.init(numBrokers, Calendar.getInstance(), trace_flag);

        datacenter = createDatacenterWithSingleHostAndTwoDisks("TestDatacenter");

        // Create Broker
        broker = new DatacenterBroker("Broker");
    }

    @Test
    public void testTwoCloudletsPerVmsOnDifferentDisks() {
        // create two VMs - one on each disk
        ImmutableMap<String, Integer[]> vmsToDisks = ImmutableMap.<String, Integer[]> builder()
                .put("vm1", new Integer[] { disk1.getId() }).put("vm2", new Integer[] { disk2.getId() }).build();

        // the cloudlets refer to the data on the disks of their VM
        ImmutableMap<String, DataItem> cloudletsToDataItems = ImmutableMap.<String, DataItem> builder()
                .put("cloudlet1", dataItem_1_1).put("cloudlet2", dataItem_1_2).put("cloudlet3", dataItem_2_1)
                .put("cloudlet4", dataItem_2_2).build();

        // the lengths of the cloiudlets in terms of CPU and IO
        ImmutableMap<String, Integer[]> cloudletsLength = ImmutableMap.<String, Integer[]> builder()
                .put("cloudlet1", new Integer[] { 1 * VM_MIPS, 2 * HOST_MIOPS })
                .put("cloudlet2", new Integer[] { 2 * VM_MIPS, 1 * HOST_MIOPS })
                .put("cloudlet3", new Integer[] { 1 * VM_MIPS, 2 * HOST_MIOPS })
                .put("cloudlet4", new Integer[] { 2 * VM_MIPS, 1 * HOST_MIOPS }).build();

        provisionVMsAndScheduleCloudlets(vmsToDisks, cloudletsToDataItems, cloudletsLength);

        // the expected results from the test
        ImmutableMap<String, Integer> cloudletExpectedStatus = ImmutableMap.<String, Integer> builder()
                .put("cloudlet1", Cloudlet.SUCCESS).put("cloudlet2", Cloudlet.SUCCESS)
                .put("cloudlet3", Cloudlet.SUCCESS).put("cloudlet4", Cloudlet.SUCCESS).build();
        ImmutableMap<String, Double> cloudletExpectedExecTimes = ImmutableMap.<String, Double> builder()
                .put("cloudlet1", 3.0).put("cloudlet2", 3.0).put("cloudlet3", 3.0).put("cloudlet4", 3.0).build();

        runtTestAndValidateResults(cloudletExpectedStatus, cloudletExpectedExecTimes);
    }

    @Test
    public void testTwoCloudletsPerVmsOnOneDisk() {
        // create two VMs on a single disk - the other disk is not used
        ImmutableMap<String, Integer[]> vmsToDisks = ImmutableMap.<String, Integer[]> builder()
                .put("vm1", new Integer[] { disk1.getId() }).put("vm2", new Integer[] { disk1.getId() }).build();

        // the cloudlets refer to data on the disk, which hosts the VMs
        ImmutableMap<String, DataItem> cloudletsToDataItems = ImmutableMap.<String, DataItem> builder()
                .put("cloudlet1", dataItem_1_1).put("cloudlet2", dataItem_1_2).put("cloudlet3", dataItem_1_1)
                .put("cloudlet4", dataItem_1_2).build();

        // the lengths of the cloiudlets in terms of CPU and IO
        ImmutableMap<String, Integer[]> cloudletsLength = ImmutableMap.<String, Integer[]> builder()
                .put("cloudlet1", new Integer[] { 1 * VM_MIPS, 2 * HOST_MIOPS })
                .put("cloudlet2", new Integer[] { 2 * VM_MIPS, 1 * HOST_MIOPS })
                .put("cloudlet3", new Integer[] { 1 * VM_MIPS, 2 * HOST_MIOPS })
                .put("cloudlet4", new Integer[] { 2 * VM_MIPS, 1 * HOST_MIOPS }).build();

        provisionVMsAndScheduleCloudlets(vmsToDisks, cloudletsToDataItems, cloudletsLength);

        // the expected results from the test
        ImmutableMap<String, Integer> cloudletExpectedStatus = ImmutableMap.<String, Integer> builder()
                .put("cloudlet1", Cloudlet.SUCCESS).put("cloudlet2", Cloudlet.SUCCESS)
                .put("cloudlet3", Cloudlet.SUCCESS).put("cloudlet4", Cloudlet.SUCCESS).build();
        ImmutableMap<String, Double> cloudletExpectedExecTimes = ImmutableMap.<String, Double> builder()
                .put("cloudlet1", 6.0).put("cloudlet2", 4.0).put("cloudlet3", 6.0).put("cloudlet4", 4.0).build();

        runtTestAndValidateResults(cloudletExpectedStatus, cloudletExpectedExecTimes);
    }

    @Test
    public void testTwoVmsSharingBothDisks() {
        // create two VMs both using both disks
        ImmutableMap<String, Integer[]> vmsToDisks = ImmutableMap.<String, Integer[]> builder()
                .put("vm1", new Integer[] { disk1.getId(), disk2.getId() })
                .put("vm2", new Integer[] { disk1.getId(), disk2.getId() }).build();

        // the cloudlets refer to data on the disk, which hosts the VMs
        ImmutableMap<String, DataItem> cloudletsToDataItems = ImmutableMap.<String, DataItem> builder()
                .put("cloudlet1", dataItem_1_1).put("cloudlet2", dataItem_2_1).put("cloudlet3", dataItem_1_2)
                .put("cloudlet4", dataItem_2_2).build();

        // the lengths of the cloiudlets in terms of CPU and IO
        ImmutableMap<String, Integer[]> cloudletsLength = ImmutableMap.<String, Integer[]> builder()
                .put("cloudlet1", new Integer[] { 1 * VM_MIPS, 2 * HOST_MIOPS })
                .put("cloudlet2", new Integer[] { 2 * VM_MIPS, 1 * HOST_MIOPS })
                .put("cloudlet3", new Integer[] { 2 * VM_MIPS, 4 * HOST_MIOPS })
                .put("cloudlet4", new Integer[] { 4 * VM_MIPS, 2 * HOST_MIOPS }).build();

        provisionVMsAndScheduleCloudlets(vmsToDisks, cloudletsToDataItems, cloudletsLength);

        // the expected results from the test
        ImmutableMap<String, Integer> cloudletExpectedStatus = ImmutableMap.<String, Integer> builder()
                .put("cloudlet1", Cloudlet.SUCCESS).put("cloudlet2", Cloudlet.SUCCESS)
                .put("cloudlet3", Cloudlet.SUCCESS).put("cloudlet4", Cloudlet.SUCCESS).build();
        ImmutableMap<String, Double> cloudletExpectedExecTimes = ImmutableMap.<String, Double> builder()
                .put("cloudlet1", 4.0).put("cloudlet2", 3.0).put("cloudlet3", 8.0).put("cloudlet4", 6.0).build();

        runtTestAndValidateResults(cloudletExpectedStatus, cloudletExpectedExecTimes);
    }

    @Test
    public void testCloudletsWithoutAccessToData() {
        // create two VMs both using both disks
        ImmutableMap<String, Integer[]> vmsToDisks = ImmutableMap.<String, Integer[]> builder()
                .put("vm1", new Integer[] { disk1.getId() }).put("vm2", new Integer[] { disk2.getId() }).build();

        // the cloudlets refer to data on the disk, which hosts the VMs
        // cloudlets 2 and 4 will fail, since theit data is not accessible by
        // the respective VMs
        ImmutableMap<String, DataItem> cloudletsToDataItems = ImmutableMap.<String, DataItem> builder()
                .put("cloudlet1", dataItem_1_1).put("cloudlet2", dataItem_2_1).put("cloudlet3", dataItem_2_2)
                .put("cloudlet4", dataItem_1_2).build();

        // the lengths of the cloiudlets in terms of CPU and IO
        ImmutableMap<String, Integer[]> cloudletsLength = ImmutableMap.<String, Integer[]> builder()
                .put("cloudlet1", new Integer[] { 1 * VM_MIPS, 1 * HOST_MIOPS })
                .put("cloudlet2", new Integer[] { 20 * VM_MIPS, 100 * HOST_MIOPS })
                .put("cloudlet3", new Integer[] { 1 * VM_MIPS, 1 * HOST_MIOPS })
                .put("cloudlet4", new Integer[] { 40 * VM_MIPS, 200 * HOST_MIOPS }).build();

        provisionVMsAndScheduleCloudlets(vmsToDisks, cloudletsToDataItems, cloudletsLength);

        // the expected results from the test
        ImmutableMap<String, Integer> cloudletExpectedStatus = ImmutableMap.<String, Integer> builder()
                .put("cloudlet1", Cloudlet.SUCCESS).put("cloudlet2", Cloudlet.FAILED)
                .put("cloudlet3", Cloudlet.SUCCESS).put("cloudlet4", Cloudlet.FAILED).build();
        ImmutableMap<String, Double> cloudletExpectedExecTimes = ImmutableMap.<String, Double> builder()
                .put("cloudlet1", 1.0).put("cloudlet2", -1.0).put("cloudlet3", 1.0).put("cloudlet4", -1.0).build();

        runtTestAndValidateResults(cloudletExpectedStatus, cloudletExpectedExecTimes);
    }

    private void runtTestAndValidateResults(final ImmutableMap<String, Integer> cloudletExpectedStatus,
            final ImmutableMap<String, Double> cloudletExpectedExecTimes) {
        CloudSim.startSimulation();
        List<Cloudlet> resultList = broker.getCloudletReceivedList();
        CloudSim.stopSimulation();

        assertEquals(4, resultList.size());
        assertEquals(cloudlet1.getCloudletStatus(), cloudletExpectedStatus.get("cloudlet1").intValue());
        assertEquals(cloudlet2.getCloudletStatus(), cloudletExpectedStatus.get("cloudlet2").intValue());
        assertEquals(cloudlet3.getCloudletStatus(), cloudletExpectedStatus.get("cloudlet3").intValue());
        assertEquals(cloudlet4.getCloudletStatus(), cloudletExpectedStatus.get("cloudlet4").intValue());

        double cloudletExecTime1 = cloudlet1.getCloudletStatus() == Cloudlet.SUCCESS ? cloudlet1.getFinishTime()
                - cloudlet1.getExecStartTime() : -1;
        double cloudletExecTime2 = cloudlet2.getCloudletStatus() == Cloudlet.SUCCESS ? cloudlet2.getFinishTime()
                - cloudlet2.getExecStartTime() : -1;
        double cloudletExecTime3 = cloudlet3.getCloudletStatus() == Cloudlet.SUCCESS ? cloudlet3.getFinishTime()
                - cloudlet3.getExecStartTime() : -1;
        double cloudletExecTime4 = cloudlet4.getCloudletStatus() == Cloudlet.SUCCESS ? cloudlet4.getFinishTime()
                - cloudlet4.getExecStartTime() : -1;
        assertEquals(cloudletExpectedExecTimes.get("cloudlet1"), cloudletExecTime1, DELTA);
        assertEquals(cloudletExpectedExecTimes.get("cloudlet2"), cloudletExecTime2, DELTA);
        assertEquals(cloudletExpectedExecTimes.get("cloudlet3"), cloudletExecTime3, DELTA);
        assertEquals(cloudletExpectedExecTimes.get("cloudlet4"), cloudletExecTime4, DELTA);
    }

    private void provisionVMsAndScheduleCloudlets(final ImmutableMap<String, Integer[]> vmIdsToDiskIds,
            final ImmutableMap<String, DataItem> cloudLetIdsToDiskIds,
            final ImmutableMap<String, Integer[]> cloudletsLength) {

        // Create virtual machines
        List<Vm> vmlist = new ArrayList<Vm>();

        int pesNumber = 1; // number of cpus
        String vmm = "Xen"; // VMM name

        vm1 = new HddVm("Test", broker.getId(), VM_MIPS, HOST_MIOPS, pesNumber, VM_RAM, VM_BW, VM_SIZE, vmm,
                new HddCloudletSchedulerTimeShared(), vmIdsToDiskIds.get("vm1"));
        vm2 = new HddVm("Test", broker.getId(), VM_MIPS, HOST_MIOPS, pesNumber, VM_RAM, VM_BW, VM_SIZE, vmm,
                new HddCloudletSchedulerTimeShared(), vmIdsToDiskIds.get("vm2"));

        // add the VMs to the vmList
        vmlist.add(vm1);
        vmlist.add(vm2);

        // submit vm list to the broker
        broker.submitVmList(vmlist);

        Integer[] lengths = cloudletsLength.get("cloudlet1");
        cloudlet1 = new HddCloudlet(lengths[0], lengths[1], 5, broker.getId(), false,
                cloudLetIdsToDiskIds.get("cloudlet1"));
        cloudlet1.setVmId(vm1.getId());

        lengths = cloudletsLength.get("cloudlet2");
        cloudlet2 = new HddCloudlet(lengths[0], lengths[1], 5, broker.getId(), false,
                cloudLetIdsToDiskIds.get("cloudlet2"));
        cloudlet2.setVmId(vm1.getId());

        lengths = cloudletsLength.get("cloudlet3");
        cloudlet3 = new HddCloudlet(lengths[0], lengths[1], 5, broker.getId(), false,
                cloudLetIdsToDiskIds.get("cloudlet3"));
        cloudlet3.setVmId(vm2.getId());

        lengths = cloudletsLength.get("cloudlet4");
        cloudlet4 = new HddCloudlet(lengths[0], lengths[1], 5, broker.getId(), false,
                cloudLetIdsToDiskIds.get("cloudlet4"));
        cloudlet4.setVmId(vm2.getId());

        broker.submitCloudletList(Arrays.asList(cloudlet1, cloudlet2, cloudlet3, cloudlet4));
    }

    private static DatacenterEX createDatacenterWithSingleHostAndTwoDisks(final String name) {
        List<Host> hostList = new ArrayList<Host>();

        List<Pe> peList = new ArrayList<>();
        List<HddPe> hddList = new ArrayList<>();

        peList.add(new Pe(Id.pollId(Pe.class), new PeProvisionerSimple(HOST_MIPS)));
        hddList.add(disk1);
        hddList.add(disk2);

        hostList.add(new HddHost(new RamProvisionerSimple(HOST_RAM), new BwProvisionerSimple(HOST_BW), HOST_STORAGE,
                peList, hddList, new VmSchedulerTimeSharedOverSubscription(peList), new VmDiskScheduler(hddList)));

        String arch = "x86";
        String os = "Linux";
        String vmm = "Xen";
        double time_zone = 10.0;
        double cost = 3.0;
        double costPerMem = 0.05;
        double costPerStorage = 0.001;
        double costPerBw = 0.0;
        LinkedList<Storage> storageList = new LinkedList<Storage>();

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(arch, os, vmm, hostList, time_zone,
                cost, costPerMem, costPerStorage, costPerBw);

        DatacenterEX datacenter = null;
        try {
            datacenter = new HddDataCenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList,
                    0);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return datacenter;
    }

}
