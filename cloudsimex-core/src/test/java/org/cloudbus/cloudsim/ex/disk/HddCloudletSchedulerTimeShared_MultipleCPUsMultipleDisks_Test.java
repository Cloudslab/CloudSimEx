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
import org.cloudbus.cloudsim.ex.util.CustomLog;
import org.cloudbus.cloudsim.ex.util.Id;
import org.cloudbus.cloudsim.ex.util.helpers.TestUtil;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author nikolay.grozev
 * 
 */
public class HddCloudletSchedulerTimeShared_MultipleCPUsMultipleDisks_Test {

    private static final double DELTA = 0.01;

    protected HddDataCenter datacenter;
    protected DatacenterBroker broker;
    protected HddVm vm1;
    protected HddVm vm2;
    protected HddVm vm3;

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

    @Before
    public void setUp() throws Exception {
	CustomLog.configLogger(TestUtil.LOG_PROPS);

	int numBrokers = 1;
	boolean trace_flag = false;

	CloudSim.init(numBrokers, Calendar.getInstance(), trace_flag);

	datacenter = createDatacenterWithSingleHostAndTwoDisks("TestDatacenter");

	// Create Broker
	broker = new DatacenterBroker("Broker");

	// Create virtual machines
	List<Vm> vmlist = new ArrayList<Vm>();

	int pesNumber = 1; // number of cpus
	String vmm = "Xen"; // VMM name

	// create 3 VMs. Vm1 has access to both hdds. Vm2 and Vm3 access only
	// disk 1 and 2 respectively
	vm1 = new HddVm(broker.getId(), VM_MIPS, HOST_MIOPS, pesNumber,
		VM_RAM, VM_BW, VM_SIZE, vmm, new HddCloudletSchedulerTimeShared());
	vm2 = new HddVm(broker.getId(), VM_MIPS, HOST_MIOPS, pesNumber,
		VM_RAM, VM_BW, VM_SIZE, vmm, new HddCloudletSchedulerTimeShared(), disk1.getId());
	vm3 = new HddVm(broker.getId(), VM_MIPS, HOST_MIOPS, pesNumber,
		VM_RAM, VM_BW, VM_SIZE, vmm, new HddCloudletSchedulerTimeShared(), disk2.getId());

	// add the VMs to the vmList
	vmlist.add(vm1);
	vmlist.add(vm2);
	vmlist.add(vm3);

	// submit vm list to the broker
	broker.submitVmList(vmlist);
    }

    @Test
    public void testFourJobsOnThreeVms() {
	double job1TimesMips = 2;
	double job1TimesIOMIPS = 1;

	double job2TimesMips = 1;
	double job2TimesIOMIPS = 2;

	double job3TimesMips = 2;
	double job3TimesIOMIPS = 1;

	double job4TimesMips = 1;
	double job4TimesIOMIPS = 2;

	HddCloudlet cloudlet1 = new HddCloudlet((int) (VM_MIPS * job1TimesMips),
		(int) (HOST_MIOPS * job1TimesIOMIPS), 5, broker.getId(), false, dataItem_1_1);
	HddCloudlet cloudlet2 = new HddCloudlet((int) (VM_MIPS * job2TimesMips),
		(int) (HOST_MIOPS * job2TimesIOMIPS), 5, broker.getId(), false, dataItem_1_2);
	HddCloudlet cloudlet3 = new HddCloudlet((int) (VM_MIPS * job3TimesMips),
		(int) (HOST_MIOPS * job3TimesIOMIPS), 5, broker.getId(), false, dataItem_2_2);
	HddCloudlet cloudlet4 = new HddCloudlet((int) (VM_MIPS * job4TimesMips),
		(int) (HOST_MIOPS * job4TimesIOMIPS), 5, broker.getId(), false, dataItem_2_2);

	cloudlet1.setVmId(vm1.getId());
	cloudlet2.setVmId(vm2.getId());
	cloudlet3.setVmId(vm2.getId()); // Should fail - data is not there
	cloudlet4.setVmId(vm3.getId());

	broker.submitCloudletList(Arrays.asList(cloudlet1, cloudlet2, cloudlet3, cloudlet4));

	CloudSim.startSimulation();
	List<Cloudlet> resultList = broker.getCloudletReceivedList();
	CloudSim.stopSimulation();

	assertEquals(4, resultList.size());

	assertEquals(cloudlet1.getCloudletStatus(), Cloudlet.SUCCESS);
	assertEquals(cloudlet2.getCloudletStatus(), Cloudlet.SUCCESS);
	assertEquals(cloudlet3.getCloudletStatus(), Cloudlet.FAILED);
	assertEquals(cloudlet4.getCloudletStatus(), Cloudlet.SUCCESS);

	double cloudletExecTime1 = cloudlet1.getFinishTime() - cloudlet1.getExecStartTime();
	double cloudletExecTime2 = cloudlet2.getFinishTime() - cloudlet2.getExecStartTime();
	double cloudletExecTime4 = cloudlet4.getFinishTime() - cloudlet4.getExecStartTime();
	assertEquals(2, cloudletExecTime1, DELTA);
	assertEquals(4, cloudletExecTime2, DELTA);
	assertEquals(4, cloudletExecTime4, DELTA);
    }

    private static HddDataCenter createDatacenterWithSingleHostAndTwoDisks(final String name) {
	List<Host> hostList = new ArrayList<Host>();

	List<Pe> peList = new ArrayList<>();
	List<HddPe> hddList = new ArrayList<>();

	peList.add(new Pe(Id.pollId(Pe.class), new PeProvisionerSimple(HOST_MIPS)));
	peList.add(new Pe(Id.pollId(Pe.class), new PeProvisionerSimple(HOST_MIPS)));
	hddList.add(disk1);
	hddList.add(disk2);

	hostList.add(new HddHost(new RamProvisionerSimple(HOST_RAM),
		new BwProvisionerSimple(HOST_BW), HOST_STORAGE, peList, hddList,
		new VmSchedulerTimeSharedOverSubscription(peList), new VmDiskScheduler(hddList)));

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

	HddDataCenter datacenter = null;
	try {
	    datacenter = new HddDataCenter(name, characteristics,
		    new VmAllocationPolicySimple(hostList), storageList, 0);
	} catch (Exception e) {
	    e.printStackTrace();
	}

	return datacenter;
    }

}
