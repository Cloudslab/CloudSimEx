package org.cloudbus.cloudsim.incubator.disk;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.incubator.util.CustomLog;
import org.cloudbus.cloudsim.incubator.util.Id;
import org.cloudbus.cloudsim.incubator.web.WebBroker;
import org.cloudbus.cloudsim.incubator.web.WebCloudlet;
import org.cloudbus.cloudsim.incubator.web.WebDataCenter;
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
public class HddCloudletSchedulerTimeShared_MultipleDisks_Test {

    private static final double DELTA = 0.01;

    protected WebDataCenter datacenter;
    protected WebBroker broker;
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

    private WebCloudlet cloudlet1;
    private WebCloudlet cloudlet2;
    private WebCloudlet cloudlet3;
    private WebCloudlet cloudlet4;

    @Before
    public void setUp() throws Exception {

	Properties props = new Properties();
	// props.put(CustomLog.LOG_LEVEL_PROP_KEY, Level.FINEST.getName());
	// props.put("ShutStandardLogger", "true");
	CustomLog.configLogger(props);

	int numBrokers = 1;
	boolean trace_flag = false;

	CloudSim.init(numBrokers, Calendar.getInstance(), trace_flag);

	datacenter = createDatacenterWithSingleHostAndMultipleDisks("TestDatacenter");

	// Create Broker
	broker = new WebBroker("Broker", 5, 10000);

    }

    private void provisionVMsAndScheduleCloudlets(final ImmutableMap<String, Integer[]> vmIdsToDiskIds,
	    final ImmutableMap<String, DataItem> cloudLetIdsToDiskIds,
	    final double job1TimesMips,
	    final double job1TimesIOMIPS,
	    final double job2TimesMips,
	    final double job2TimesIOMIPS,
	    final double job3TimesMips,
	    final double job3TimesIOMIPS,
	    final double job4TimesMips,
	    final double job4TimesIOMIPS) {

	// Create virtual machines
	List<Vm> vmlist = new ArrayList<Vm>();

	int pesNumber = 1; // number of cpus
	String vmm = "Xen"; // VMM name

	vm1 = new HddVm(broker.getId(), VM_MIPS, HOST_MIOPS, pesNumber,
		VM_RAM, VM_BW, VM_SIZE, vmm, new HddCloudletSchedulerTimeShared(), vmIdsToDiskIds.get("vm1"));
	vm2 = new HddVm(broker.getId(), VM_MIPS, HOST_MIOPS, pesNumber,
		VM_RAM, VM_BW, VM_SIZE, vmm, new HddCloudletSchedulerTimeShared(), vmIdsToDiskIds.get("vm2"));

	// add the VMs to the vmList
	vmlist.add(vm1);
	vmlist.add(vm2);

	// submit vm list to the broker
	broker.submitVmList(vmlist);

	cloudlet1 = new WebCloudlet(0, (int) (VM_MIPS * job1TimesMips),
		(int) (HOST_MIOPS * job1TimesIOMIPS), 5, broker.getId(), cloudLetIdsToDiskIds.get("cloudlet1"));
	cloudlet1.setVmId(vm1.getId());

	cloudlet2 = new WebCloudlet(0, (int) (VM_MIPS * job2TimesMips),
		(int) (HOST_MIOPS * job2TimesIOMIPS), 5, broker.getId(), cloudLetIdsToDiskIds.get("cloudlet2"));
	cloudlet2.setVmId(vm1.getId());

	cloudlet3 = new WebCloudlet(0, (int) (VM_MIPS * job3TimesMips),
		(int) (HOST_MIOPS * job3TimesIOMIPS), 5, broker.getId(), cloudLetIdsToDiskIds.get("cloudlet3"));
	cloudlet3.setVmId(vm2.getId());

	cloudlet4 = new WebCloudlet(0, (int) (VM_MIPS * job4TimesMips),
		(int) (HOST_MIOPS * job4TimesIOMIPS), 5, broker.getId(), cloudLetIdsToDiskIds.get("cloudlet4"));
	cloudlet4.setVmId(vm2.getId());

	broker.submitCloudletList(Arrays.asList(cloudlet1, cloudlet2, cloudlet3, cloudlet4));
    }

    @Test
    public void testTwoCloudletsPerVmsOnDifferentDisks() {
	// create two VMs - one on each disk
	ImmutableMap<String, Integer[]> vmsToDisks = ImmutableMap.<String, Integer[]> builder()
		.put("vm1", new Integer[] { disk1.getId() })
		.put("vm2", new Integer[] { disk2.getId() }).build();

	// the cloudlets refer to the data on the disks of their VM
	ImmutableMap<String, DataItem> cloudletsToDataItems = ImmutableMap.<String, DataItem> builder()
		.put("cloudlet1", dataItem_1_1)
		.put("cloudlet2", dataItem_1_2)
		.put("cloudlet3", dataItem_2_1)
		.put("cloudlet4", dataItem_2_2).build();

	provisionVMsAndScheduleCloudlets(vmsToDisks, cloudletsToDataItems, 1, 2, 2, 1, 1, 2, 2, 1);

	CloudSim.startSimulation();
	List<Cloudlet> resultList = broker.getCloudletReceivedList();
	CloudSim.stopSimulation();

	assertEquals(4, resultList.size());
	assertEquals(cloudlet1.getCloudletStatus(), Cloudlet.SUCCESS);
	assertEquals(cloudlet2.getCloudletStatus(), Cloudlet.SUCCESS);
	assertEquals(cloudlet3.getCloudletStatus(), Cloudlet.SUCCESS);
	assertEquals(cloudlet4.getCloudletStatus(), Cloudlet.SUCCESS);

	double cloudletExecTime1 = cloudlet1.getFinishTime() - cloudlet1.getExecStartTime();
	double cloudletExecTime2 = cloudlet2.getFinishTime() - cloudlet2.getExecStartTime();
	double cloudletExecTime3 = cloudlet3.getFinishTime() - cloudlet3.getExecStartTime();
	double cloudletExecTime4 = cloudlet4.getFinishTime() - cloudlet4.getExecStartTime();
	assertEquals(3, cloudletExecTime1, DELTA);
	assertEquals(3, cloudletExecTime2, DELTA);
	assertEquals(3, cloudletExecTime3, DELTA);
	assertEquals(3, cloudletExecTime4, DELTA);
    }

    @Test
    public void testTwoCloudletsPerVmsOnOneDisk() {
	// create two VMs on a single disk - the other disk is not used
	ImmutableMap<String, Integer[]> vmsToDisks = ImmutableMap.<String, Integer[]> builder()
		.put("vm1", new Integer[] { disk1.getId() })
		.put("vm2", new Integer[] { disk1.getId() }).build();

	// the cloudlets refer to data on the disk, which hosts the VMs
	ImmutableMap<String, DataItem> cloudletsToDataItems = ImmutableMap.<String, DataItem> builder()
		.put("cloudlet1", dataItem_1_1)
		.put("cloudlet2", dataItem_1_2)
		.put("cloudlet3", dataItem_1_1)
		.put("cloudlet4", dataItem_1_2).build();

	provisionVMsAndScheduleCloudlets(vmsToDisks, cloudletsToDataItems, 1, 2, 2, 1, 1, 2, 2, 1);

	CloudSim.startSimulation();
	List<Cloudlet> resultList = broker.getCloudletReceivedList();
	CloudSim.stopSimulation();

	assertEquals(4, resultList.size());
	assertEquals(cloudlet1.getCloudletStatus(), Cloudlet.SUCCESS);
	assertEquals(cloudlet2.getCloudletStatus(), Cloudlet.SUCCESS);
	assertEquals(cloudlet3.getCloudletStatus(), Cloudlet.SUCCESS);
	assertEquals(cloudlet4.getCloudletStatus(), Cloudlet.SUCCESS);

	double cloudletExecTime1 = cloudlet1.getFinishTime() - cloudlet1.getExecStartTime();
	double cloudletExecTime2 = cloudlet2.getFinishTime() - cloudlet2.getExecStartTime();
	double cloudletExecTime3 = cloudlet3.getFinishTime() - cloudlet3.getExecStartTime();
	double cloudletExecTime4 = cloudlet4.getFinishTime() - cloudlet4.getExecStartTime();
	assertEquals(6, cloudletExecTime1, DELTA);
	assertEquals(4, cloudletExecTime2, DELTA);
	assertEquals(6, cloudletExecTime3, DELTA);
	assertEquals(4, cloudletExecTime4, DELTA);
    }

    private static WebDataCenter createDatacenterWithSingleHostAndMultipleDisks(final String name) {
	List<Host> hostList = new ArrayList<Host>();

	List<Pe> peList = new ArrayList<>();
	List<HddPe> hddList = new ArrayList<>();

	peList.add(new Pe(Id.pollId(Pe.class), new PeProvisionerSimple(HOST_MIPS)));
	hddList.add(disk1);
	hddList.add(disk2);

	hostList.add(new HddHost(new RamProvisionerSimple(HOST_RAM),
		new BwProvisionerSimple(HOST_BW), HOST_STORAGE, peList, hddList,
		new VmSchedulerTimeShared(peList), new VmDiskScheduler(hddList)));

	String arch = "x86";
	String os = "Linux";
	String vmm = "Xen";
	double time_zone = 10.0;
	double cost = 3.0;
	double costPerMem = 0.05;
	double costPerStorage = 0.001;
	double costPerBw = 0.0;
	LinkedList<Storage> storageList = new LinkedList<Storage>();

	DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
		arch, os, vmm, hostList, time_zone, cost, costPerMem,
		costPerStorage, costPerBw);

	WebDataCenter datacenter = null;
	try {
	    datacenter = new WebDataCenter(name, characteristics,
		    new VmAllocationPolicySimple(hostList), storageList, 0);
	} catch (Exception e) {
	    e.printStackTrace();
	}

	return datacenter;
    }

}
