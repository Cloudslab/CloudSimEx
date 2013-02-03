package org.cloudbus.cloudsim.incubator.web;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.VmSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.incubator.util.CustomLog;
import org.cloudbus.cloudsim.incubator.util.Id;
import org.cloudbus.cloudsim.incubator.web.extensions.HDPe;
import org.cloudbus.cloudsim.incubator.web.extensions.HddCloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.incubator.web.extensions.HddHost;
import org.cloudbus.cloudsim.incubator.web.extensions.HddVm;
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
public class SimpleWebLoadBalancerTest {

    protected WebDataCenter datacenter;
    protected WebBroker broker;
    protected ILoadBalancer balancer;

    private static final int HOST_MIPS = 1000;
    private static final int HOST_MIOPS = 100;
    private static final int HOST_RAM = 2048; // host memory (MB)
    private static final long HOST_STORAGE = 1000000; // host storage
    private static final int HOST_BW = 10000;

    private static final int VM_MIPS = 250;
    private static final long VM_SIZE = 10000;
    private static final int VM_RAM = 512;
    private static final long VM_BW = 1000;

    @Before
    public void setUp() throws Exception {
	CustomLog.configLogger(new Properties());

	int numBrokers = 1;
	boolean trace_flag = false;

	CloudSim.init(numBrokers, Calendar.getInstance(), trace_flag);

	datacenter = createDatacenter();

	// Create Broker
	broker = new WebBroker("Broker", 5, 10000);
    }

    @Test
    public void testLoadBalancingAmongEqualServers() {
	// Create virtual machines
	List<Vm> vmlist = new ArrayList<Vm>();

	int pesNumber = 1; // number of cpus
	String vmm = "Xen"; // VMM name

	// create two VMs
	HddVm appVm1 = new HddVm(broker.getId(), VM_MIPS, HOST_MIOPS, pesNumber,
		VM_RAM, VM_BW, VM_SIZE, vmm, new HddCloudletSchedulerTimeShared());
	HddVm appVm2 = new HddVm(broker.getId(), VM_MIPS, HOST_MIOPS, pesNumber,
		VM_RAM, VM_BW, VM_SIZE, vmm, new HddCloudletSchedulerTimeShared());
	HddVm dbVm = new HddVm(broker.getId(), VM_MIPS, HOST_MIOPS, pesNumber,
		VM_RAM, VM_BW, VM_SIZE, vmm, new HddCloudletSchedulerTimeShared());

	// add the VMs to the vmList
	vmlist.add(appVm1);
	vmlist.add(appVm2);
	vmlist.add(dbVm);

	// submit vm list to the broker
	broker.submitVmList(vmlist);
	balancer = new SimpleWebLoadBalancer(Arrays.asList(appVm1, appVm2), dbVm);
	broker.addLoadBalancer(balancer);

	//Should take > 10s
	WebCloudlet session1AppCloudlet = new WebCloudlet(0, VM_SIZE * 10, 10, 10, broker.getId(), null);
	//Should take > 10s
	WebCloudlet session1DbCloudlet = new WebCloudlet(0, VM_SIZE * 10, 10, 10, broker.getId(), null);
	WebSession session1 = new WebSession(new IterableGenerator<WebCloudlet>(session1AppCloudlet),
		new IterableGenerator<WebCloudlet>(session1DbCloudlet), broker.getId(), -1, 100);

	//Should take > 10s
	WebCloudlet session2AppCloudlet = new WebCloudlet(0, VM_SIZE * 10, 10, 10, broker.getId(), null);
	//Should take > 10s
	WebCloudlet session2DbCloudlet = new WebCloudlet(0, VM_SIZE * 10, 10, 10, broker.getId(), null);
	WebSession session2 = new WebSession(new IterableGenerator<WebCloudlet>(session2AppCloudlet),
		new IterableGenerator<WebCloudlet>(session2DbCloudlet), broker.getId(), -1, 100);

	//Fire it on the 5th sec
	broker.submitSessionsAtTime(Arrays.asList(session1), balancer.getId(), 5);
	//Fire it on the 6th sec
	broker.submitSessionsAtTime(Arrays.asList(session2), balancer.getId(), 6);

	CloudSim.startSimulation();
//	List<Cloudlet> resultList = broker.getCloudletReceivedList();
	CloudSim.stopSimulation();

	//The load balancer should have redistributed them on different app servers
	assertTrue(session1.getAppVmId() != session2.getAppVmId());
	assertEquals(session1.getDbVmId(), session2.getDbVmId());
    }
    
    @Test
    public void testLoadBalancingAmongDiffServers() {
	// Create virtual machines
	List<Vm> vmlist = new ArrayList<Vm>();

	int pesNumber = 1; // number of cpus
	String vmm = "Xen"; // VMM name

	// create two VMs
	HddVm appVm1 = new HddVm(broker.getId(), VM_MIPS, HOST_MIOPS, pesNumber,
		VM_RAM, VM_BW, VM_SIZE, vmm, new HddCloudletSchedulerTimeShared());
	HddVm appVm2 = new HddVm(broker.getId(), VM_MIPS * 2, HOST_MIOPS, pesNumber,
		VM_RAM, VM_BW, VM_SIZE, vmm, new HddCloudletSchedulerTimeShared());
	HddVm dbVm = new HddVm(broker.getId(), VM_MIPS, HOST_MIOPS, pesNumber,
		VM_RAM, VM_BW, VM_SIZE, vmm, new HddCloudletSchedulerTimeShared());

	// add the VMs to the vmList
	vmlist.add(appVm1);
	vmlist.add(appVm2);
	vmlist.add(dbVm);

	// submit vm list to the broker
	broker.submitVmList(vmlist);
	balancer = new SimpleWebLoadBalancer(Arrays.asList(appVm1, appVm2), dbVm);
	broker.addLoadBalancer(balancer);

	//Should take > 10s
	WebCloudlet session1AppCloudlet = new WebCloudlet(0, VM_SIZE * 10, 10, 10, broker.getId(), null);
	//Should take > 10s
	WebCloudlet session1DbCloudlet = new WebCloudlet(0, VM_SIZE * 10, 10, 10, broker.getId(), null);
	WebSession session1 = new WebSession(new IterableGenerator<WebCloudlet>(session1AppCloudlet),
		new IterableGenerator<WebCloudlet>(session1DbCloudlet), broker.getId(), -1, 100);

	//Should take > 10s
	WebCloudlet session2AppCloudlet = new WebCloudlet(0, VM_SIZE * 10, 10, 10, broker.getId(), null);
	//Should take > 10s
	WebCloudlet session2DbCloudlet = new WebCloudlet(0, VM_SIZE * 10, 10, 10, broker.getId(), null);
	WebSession session2 = new WebSession(new IterableGenerator<WebCloudlet>(session2AppCloudlet),
		new IterableGenerator<WebCloudlet>(session2DbCloudlet), broker.getId(), -1, 100);
	
	//Should take > 10s
	WebCloudlet session3AppCloudlet = new WebCloudlet(0, VM_SIZE * 10, 10, 10, broker.getId(), null);
	//Should take > 10s
	WebCloudlet session3DbCloudlet = new WebCloudlet(0, VM_SIZE * 10, 10, 10, broker.getId(), null);
	WebSession session3 = new WebSession(new IterableGenerator<WebCloudlet>(session3AppCloudlet),
		new IterableGenerator<WebCloudlet>(session3DbCloudlet), broker.getId(), -1, 100);
	
	//Fire it on the 5th sec
	broker.submitSessionsAtTime(Arrays.asList(session1), balancer.getId(), 5);
	//Fire it on the 6th sec
	broker.submitSessionsAtTime(Arrays.asList(session2), balancer.getId(), 6);
	//Fire it on the 7th sec
	broker.submitSessionsAtTime(Arrays.asList(session3), balancer.getId(), 7);

	CloudSim.startSimulation();
//	List<Cloudlet> resultList = broker.getCloudletReceivedList();
	CloudSim.stopSimulation();

	//The First 2 sessions should be redistributed to different app servers
	assertTrue(session1.getAppVmId() != session2.getAppVmId());
	//The 3rd session should go to the "stronger" server
	assertEquals(session3.getAppVmId().intValue(), appVm2.getId());
	
	assertEquals(session1.getDbVmId(), session2.getDbVmId());
	assertEquals(session1.getDbVmId(), session3.getDbVmId());
    }
    
    
    @Test
    public void testLoadBalancingFailedServers() {
	// Create virtual machines
	List<Vm> vmlist = new ArrayList<Vm>();

	int pesNumber = 1; // number of cpus
	String vmm = "Xen"; // VMM name

	// create two VMs
	HddVm appVm1 = new HddVm(broker.getId(), VM_MIPS * 2, HOST_MIOPS, pesNumber,
		VM_RAM, VM_BW, VM_SIZE, vmm, new HddCloudletSchedulerTimeShared());
	HddVm appVm2 = new HddVm(broker.getId(), VM_MIPS, HOST_MIOPS, pesNumber,
		VM_RAM, VM_BW, VM_SIZE, vmm, new HddCloudletSchedulerTimeShared());
	HddVm dbVm = new HddVm(broker.getId(), VM_MIPS, HOST_MIOPS, pesNumber,
		VM_RAM, VM_BW, VM_SIZE, vmm, new HddCloudletSchedulerTimeShared());

	// add the VMs to the vmList
	vmlist.add(appVm1);
	vmlist.add(appVm2);
	vmlist.add(dbVm);

	// submit vm list to the broker
	broker.submitVmList(vmlist);
	balancer = new SimpleWebLoadBalancer(Arrays.asList(appVm1, appVm2), dbVm);
	broker.addLoadBalancer(balancer);

	//Should take > 10s
	WebCloudlet session1AppCloudlet = new WebCloudlet(0, VM_SIZE * 10, 10, VM_RAM * 2, broker.getId(), null);
	//Should take > 10s
	WebCloudlet session1DbCloudlet = new WebCloudlet(0, VM_SIZE * 10, 10, 10, broker.getId(), null);
	WebSession session1 = new WebSession(new IterableGenerator<WebCloudlet>(session1AppCloudlet),
		new IterableGenerator<WebCloudlet>(session1DbCloudlet), broker.getId(), -1, 100);

	//Should take > 10s
	WebCloudlet session2AppCloudlet = new WebCloudlet(0, VM_SIZE * 10, 10, 10, broker.getId(), null);
	//Should take > 10s
	WebCloudlet session2DbCloudlet = new WebCloudlet(0, VM_SIZE * 10, 10, 10, broker.getId(), null);
	WebSession session2 = new WebSession(new IterableGenerator<WebCloudlet>(session2AppCloudlet),
		new IterableGenerator<WebCloudlet>(session2DbCloudlet), broker.getId(), -1, 100);
	
	//Should take > 10s
	WebCloudlet session3AppCloudlet = new WebCloudlet(0, VM_SIZE * 10, 10, 10, broker.getId(), null);
	//Should take > 10s
	WebCloudlet session3DbCloudlet = new WebCloudlet(0, VM_SIZE * 10, 10, 10, broker.getId(), null);
	WebSession session3 = new WebSession(new IterableGenerator<WebCloudlet>(session3AppCloudlet),
		new IterableGenerator<WebCloudlet>(session3DbCloudlet), broker.getId(), -1, 100);
	
	//Fire it on the 5th sec
	broker.submitSessionsAtTime(Arrays.asList(session1), balancer.getId(), 5);
	//Fire it on the 6th sec
	broker.submitSessionsAtTime(Arrays.asList(session2), balancer.getId(), 6);
	//Fire it on the 7th sec
	broker.submitSessionsAtTime(Arrays.asList(session3), balancer.getId(), 7);

	CloudSim.startSimulation();
//	List<Cloudlet> resultList = broker.getCloudletReceivedList();
	CloudSim.stopSimulation();

	//The First server will go down.. so the other sessions need to go to other servers..
	assertTrue(session1.getAppVmId() != session2.getAppVmId());
	assertEquals(session2.getAppVmId().intValue(), session3.getAppVmId().intValue());
	
	assertEquals(session1.getDbVmId(), session2.getDbVmId());
	assertEquals(session1.getDbVmId(), session3.getDbVmId());
    }
    
    
    @Test
    public void testLoadBalancingAmongEqualServersConcurrentSubmission() {
	// Create virtual machines
	List<Vm> vmlist = new ArrayList<Vm>();
	
	int pesNumber = 1; // number of cpus
	String vmm = "Xen"; // VMM name
	
	// create two VMs
	HddVm appVm1 = new HddVm(broker.getId(), VM_MIPS, HOST_MIOPS, pesNumber,
		VM_RAM, VM_BW, VM_SIZE, vmm, new HddCloudletSchedulerTimeShared());
	HddVm appVm2 = new HddVm(broker.getId(), VM_MIPS, HOST_MIOPS, pesNumber,
		VM_RAM, VM_BW, VM_SIZE, vmm, new HddCloudletSchedulerTimeShared());
	HddVm dbVm = new HddVm(broker.getId(), VM_MIPS, HOST_MIOPS, pesNumber,
		VM_RAM, VM_BW, VM_SIZE, vmm, new HddCloudletSchedulerTimeShared());
	
	// add the VMs to the vmList
	vmlist.add(appVm1);
	vmlist.add(appVm2);
	vmlist.add(dbVm);
	
	// submit vm list to the broker
	broker.submitVmList(vmlist);
	balancer = new SimpleWebLoadBalancer(Arrays.asList(appVm1, appVm2), dbVm);
	broker.addLoadBalancer(balancer);
	
	//Should take > 10s
	WebCloudlet session1AppCloudlet = new WebCloudlet(0, VM_SIZE * 10, 10, 10, broker.getId(), null);
	//Should take > 10s
	WebCloudlet session1DbCloudlet = new WebCloudlet(0, VM_SIZE * 10, 10, 10, broker.getId(), null);
	WebSession session1 = new WebSession(new IterableGenerator<WebCloudlet>(session1AppCloudlet),
		new IterableGenerator<WebCloudlet>(session1DbCloudlet), broker.getId(), -1, 100);
	
	//Should take > 10s
	WebCloudlet session2AppCloudlet = new WebCloudlet(0, VM_SIZE * 10, 10, 10, broker.getId(), null);
	//Should take > 10s
	WebCloudlet session2DbCloudlet = new WebCloudlet(0, VM_SIZE * 10, 10, 10, broker.getId(), null);
	WebSession session2 = new WebSession(new IterableGenerator<WebCloudlet>(session2AppCloudlet),
		new IterableGenerator<WebCloudlet>(session2DbCloudlet), broker.getId(), -1, 100);
	
	//Fire it on the 5th sec
	broker.submitSessions(Arrays.asList(session1, session2), balancer.getId());
	
	CloudSim.startSimulation();
//	List<Cloudlet> resultList = broker.getCloudletReceivedList();
	CloudSim.stopSimulation();
	
	//The load balancer should have redistributed them on different app servers
	assertTrue(session1.getAppVmId() != session2.getAppVmId());
	assertEquals(session1.getDbVmId(), session2.getDbVmId());
    }

    private static WebDataCenter createDatacenter() {
	List<Host> hostList = new ArrayList<Host>();

	List<Pe> peList = new ArrayList<>();
	List<HDPe> hddList = new ArrayList<>();

	peList.add(new Pe(Id.pollId(Pe.class), new PeProvisionerSimple(HOST_MIPS)));
	hddList.add(new HDPe(new PeProvisionerSimple(HOST_MIOPS)));

	hostList.add(new HddHost(new RamProvisionerSimple(HOST_RAM),
		new BwProvisionerSimple(HOST_BW), HOST_STORAGE, peList, hddList,
		new VmSchedulerTimeShared(peList), new VmSchedulerTimeSharedOverSubscription(hddList)));

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
	    datacenter = new WebDataCenter("TestDatacenter", characteristics,
		    new VmAllocationPolicySimple(hostList), storageList, 0);
	} catch (Exception e) {
	    e.printStackTrace();
	}

	return datacenter;
    }
}
