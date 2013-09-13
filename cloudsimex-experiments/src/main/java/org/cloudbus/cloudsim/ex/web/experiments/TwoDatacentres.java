package org.cloudbus.cloudsim.ex.web.experiments;

import static org.cloudbus.cloudsim.Consts.DAY;
import static org.cloudbus.cloudsim.Consts.HOUR;
import static org.cloudbus.cloudsim.ex.web.experiments.ExperimentsUtil.HOURS;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmScheduler;
import org.cloudbus.cloudsim.VmSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.ex.VmSchedulerMapVmsToPes;
import org.cloudbus.cloudsim.ex.disk.DataItem;
import org.cloudbus.cloudsim.ex.disk.HddCloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.ex.disk.HddDataCenter;
import org.cloudbus.cloudsim.ex.disk.HddHost;
import org.cloudbus.cloudsim.ex.disk.HddPe;
import org.cloudbus.cloudsim.ex.disk.HddVm;
import org.cloudbus.cloudsim.ex.disk.VmDiskScheduler;
import org.cloudbus.cloudsim.ex.util.CustomLog;
import org.cloudbus.cloudsim.ex.util.Id;
import org.cloudbus.cloudsim.ex.web.ConstGenerator;
import org.cloudbus.cloudsim.ex.web.ILoadBalancer;
import org.cloudbus.cloudsim.ex.web.SimpleDBBalancer;
import org.cloudbus.cloudsim.ex.web.SimpleWebLoadBalancer;
import org.cloudbus.cloudsim.ex.web.WebSession;
import org.cloudbus.cloudsim.ex.web.workload.IWorkloadGenerator;
import org.cloudbus.cloudsim.ex.web.workload.StatWorkloadGenerator;
import org.cloudbus.cloudsim.ex.web.workload.brokers.PerformanceLoggingWebBroker;
import org.cloudbus.cloudsim.ex.web.workload.brokers.WebBroker;
import org.cloudbus.cloudsim.ex.web.workload.freq.CompositeValuedSet;
import org.cloudbus.cloudsim.ex.web.workload.freq.FrequencyFunction;
import org.cloudbus.cloudsim.ex.web.workload.freq.PeriodicStochasticFrequencyFunction;
import org.cloudbus.cloudsim.ex.web.workload.sessions.GeneratorsUtil;
import org.cloudbus.cloudsim.ex.web.workload.sessions.StatSessionGenerator;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

/**
 * 
 * @author nikolay.grozev
 * 
 */
public class TwoDatacentres {

    public static String RESULT_DIR = "results/stat/";
    protected int simulationLength = DAY + HOUR / 2;
    protected int step = 60;
    protected String experimentName;

    protected VmSchedulerMapVmsToPes<Pe> vmScheduler1;
    protected Pe pe1;
    protected Pe pe2;

    protected VmSchedulerMapVmsToPes<Pe> vmScheduler2;
    protected Pe pe3;
    protected Pe pe4;

    private static final DataItem DATA1 = new DataItem(5);
    private static final DataItem DATA2 = new DataItem(5);

    /**
     * @param args
     * @throws IOException
     */
    public static void main(final String[] args) throws IOException {
	ExperimentsUtil.parseExperimentParameters(args);
	new TwoDatacentres().runExperimemt();
    }

    /**
     * Sets up the experiment and runs it.
     * 
     * @throws SecurityException
     * @throws IOException
     */
    public final void runExperimemt() throws SecurityException, IOException {
	long simulationStart = System.currentTimeMillis();

	CustomLog.redirectToFile(RESULT_DIR + "/performance_sessions_DC1_2.csv");

	try {
	    // Step1: Initialize the CloudSim package. It should be called
	    // before creating any entities.
	    int numBrokers = 2; // number of brokers we'll be using
	    boolean trace_flag = false; // mean trace events
	    CloudSim.init(numBrokers, Calendar.getInstance(), trace_flag, 0.001);

	    // Step 2: Create Datacenters
	    Datacenter dc1 = createDatacenter1("WebDataCenter1");
	    Datacenter dc2 = createDatacenter2("WebDataCenter2");

	    // Step 3: Create Brokers
	    WebBroker brokerDC1 = new PerformanceLoggingWebBroker("BrokerDC1", step, simulationLength, 1,
		    0.01,
		    5 * step,
		    dc1.getId());

	    WebBroker brokerDC2 = new PerformanceLoggingWebBroker("BrokerDC2", step, simulationLength, 1,
		    0.01,
		    5 * step,
		    dc2.getId());

	    // Step 4: Create virtual machines
	    HddVm dbServerVMDC1 = createVM("Db-Srv", brokerDC1.getId(), 10000, 10000, 512);
	    List<HddVm> appServersVMDC1 = Arrays.asList(createVM("App-Srv", brokerDC1.getId(), 10000, 10000, 512));

	    vmScheduler1.map(dbServerVMDC1.getId(), pe1.getId());
	    vmScheduler1.map(appServersVMDC1.get(0).getId(), pe2.getId());

	    HddVm dbServerVMDC2 = createVM("Db-Srv", brokerDC2.getId(),
		    (int) (10000 * ((2666 * (100 - 3.028893) / 100) / 3400.0)), 7500, 1666);
	    List<HddVm> appServersVMDC2 =
		    Arrays.asList(createVM("App-Srv", brokerDC2.getId(),
			    (int) (10000 * ((2266 * (100 - 0.241971) / 100) / 3400.0)), 7500, 1656));

	    vmScheduler2.map(dbServerVMDC2.getId(), pe3.getId());
	    vmScheduler2.map(appServersVMDC2.get(0).getId(), pe4.getId());

	    // Step 5: Create load balancers for the virtual machines in the 2
	    // datacenters
	    ILoadBalancer balancerDC1 = new SimpleWebLoadBalancer(
		    1, "127.0.0.1", appServersVMDC1, new SimpleDBBalancer(dbServerVMDC1));
	    brokerDC1.addLoadBalancer(balancerDC1);

	    ILoadBalancer balancerDC2 = new SimpleWebLoadBalancer(
		    1, "127.0.0.1", appServersVMDC2, new SimpleDBBalancer(dbServerVMDC2));
	    brokerDC2.addLoadBalancer(balancerDC2);

	    // Step 6: Add the virtual machines for the data centers
	    List<Vm> vmlistDC1 = new ArrayList<Vm>();
	    vmlistDC1.addAll(balancerDC1.getAppServers());
	    vmlistDC1.addAll(balancerDC1.getDbBalancer().getVMs());
	    brokerDC1.submitVmList(vmlistDC1);

	    List<Vm> vmlistDC2 = new ArrayList<Vm>();
	    vmlistDC2.addAll(balancerDC2.getAppServers());
	    vmlistDC2.addAll(balancerDC2.getDbBalancer().getVMs());
	    brokerDC2.submitVmList(vmlistDC2);

	    // Step 7: Define the workload and associate it with load balancers
	    List<? extends IWorkloadGenerator> workloadDC1 = generateWorkloadsDC(brokerDC1.getId(), 0 * HOUR, DATA1);
	    brokerDC1.addWorkloadGenerators(workloadDC1, balancerDC1.getAppId());

	    List<? extends IWorkloadGenerator> workloadDC2 = generateWorkloadsDC(brokerDC2.getId(), 12 * HOUR, DATA2);
	    brokerDC2.addWorkloadGenerators(workloadDC2, balancerDC2.getAppId());

	    // Step 8: Starts the simulation
	    CloudSim.startSimulation();

	    // Step 9: get the results
	    List<WebSession> resultDC1Sessions = brokerDC1.getServedSessions();
	    List<Cloudlet> cloudletsDC1 = brokerDC1.getCloudletReceivedList();

	    List<WebSession> resultDC2Sessions = brokerDC2.getServedSessions();
	    List<Cloudlet> cloudletsDC2 = brokerDC2.getCloudletReceivedList();

	    // Step 10 : stop the simulation and print the results
	    CloudSim.stopSimulation();
	    CustomLog.redirectToFile(RESULT_DIR + "simulation_sessions_DC1.csv");
	    CustomLog.printResults(WebSession.class, resultDC1Sessions);

	    // CustomLog.redirectToFile(dir + "simulation_cloudlets_" +
	    // numOfSessions + ".csv");
	    // CustomLog.printResults(WebCloudlet.class, cloudlets);

	    CustomLog.redirectToFile(RESULT_DIR + "simulation_sessions_DC2.csv");
	    CustomLog.printResults(WebSession.class, resultDC2Sessions);

	    // CustomLog.redirectToFile(dir + "simulation_cloudlets_" +
	    // numOfSessions + ".csv");
	    // CustomLog.printResults(WebCloudlet.class, cloudlets);

	    System.err.println();
	    System.err.println(experimentName + ": Simulation is finished!");
	} catch (Exception e) {
	    System.err.println(experimentName + ": The simulation has been terminated due to an unexpected error");
	    e.printStackTrace();
	}
	System.err.println(experimentName + ": Finished in " + (System.currentTimeMillis() - simulationStart) / 1000
		+ " seconds");
    }

    protected List<? extends IWorkloadGenerator> generateWorkloadsDC(final int userId, final double nullPoint,
	    final DataItem dataItem) {
	String[] periods = new String[] {
		String.format("[%d,%d] m=10  std=1", HOURS[0], HOURS[6]),
		String.format("(%d,%d] m=30  std=2", HOURS[6], HOURS[7]),
		String.format("(%d,%d] m=50  std=3", HOURS[7], HOURS[10]),
		String.format("(%d,%d] m=100 std=4", HOURS[10], HOURS[14]),
		String.format("(%d,%d] m=50  std=3", HOURS[14], HOURS[17]),
		String.format("(%d,%d] m=30  std=2", HOURS[17], HOURS[18]),
		String.format("(%d,%d] m=10  std=1", HOURS[18], HOURS[24]) };
	return generateWorkload(userId, nullPoint, dataItem, periods);
    }

    protected List<? extends IWorkloadGenerator> generateWorkload(final int userId, final double nullPoint,
	    final DataItem dataItem,
	    final String[] periods) {
	try (InputStream asIO = new FileInputStream(RESULT_DIR + "web_cloudlets.txt");
		InputStream dbIO = new FileInputStream(RESULT_DIR + "db_cloudlets.txt")) {
	    StatSessionGenerator sessGen = new StatSessionGenerator(GeneratorsUtil.parseStream(asIO),
		    GeneratorsUtil.parseStream(dbIO), userId, step, new ConstGenerator<String[]>(new String[] {}),
		    dataItem);

	    double unit = HOUR;
	    double periodLength = DAY;

	    FrequencyFunction freqFun = new PeriodicStochasticFrequencyFunction(unit, periodLength, nullPoint,
		    CompositeValuedSet.createCompositeValuedSet(periods));
	    return Arrays.asList(new StatWorkloadGenerator(freqFun, sessGen));
	} catch (IOException e) {
	    e.printStackTrace();
	}
	return null;
    }

    private HddVm createVM(String name, final int brokerId, final int mips, final int ioMips, final int ram) {
	// VM description
	// int mips = 10000;
	// int ioMips = 10000;
	long size = 10000; // image size (MB)
	// int ram = 512; // vm memory (MB)
	long bw = 1000;
	int pesNumber = 1; // number of cpus
	String vmm = "Xen"; // VMM name

	// create two VMs
	HddVm appServerVM = new HddVm(name, brokerId, mips, ioMips, pesNumber,
		ram, bw, size, vmm, new HddCloudletSchedulerTimeShared(), new Integer[0]);
	return appServerVM;
    }

    private Datacenter createDatacenter1(final String name) {
	List<Host> hostList = new ArrayList<Host>();

	List<Pe> peList = new ArrayList<>();
	List<HddPe> hddList = new ArrayList<>();

	int mips = 10000;
	int iops = 10000;

	for (int i = 0; i < 8; i++) {
	    peList.add(new Pe(Id.pollId(Pe.class), new PeProvisionerSimple(mips)));
	}
	pe1 = peList.get(0);
	pe2 = peList.get(1);
	hddList.add(new HddPe(new PeProvisionerSimple(iops), DATA1));

	int ram = 2048 * 4; // host memory (MB)
	long storage = 1000000; // host storage
	int bw = 10000;

	vmScheduler1 = new VmSchedulerMapVmsToPes<Pe>(peList) {

	    @Override
	    protected VmScheduler createSchedulerFroPe(final Pe pe) {
		return new VmSchedulerTimeSharedOverSubscription(Arrays.asList(pe));
	    }
	};

	hostList.add(new HddHost(new RamProvisionerSimple(ram),
		new BwProvisionerSimple(bw), storage, peList, hddList,
		vmScheduler1,
		new VmDiskScheduler(hddList)));

	// 5. Create a DatacenterCharacteristics object that stores the
	// properties of a data center: architecture, OS, list of
	// Machines, allocation policy: time- or space-shared, time zone
	// and its price (G$/Pe time unit).
	String arch = "x86"; // system architecture
	String os = "Linux"; // operating system
	String vmm = "Xen";
	double time_zone = 10.0; // time zone this resource located
	double cost = 3.0; // the cost of using processing in this resource
	double costPerMem = 0.05; // the cost of using memory in this resource
	double costPerStorage = 0.001; // the cost of using storage in this
	// resource
	double costPerBw = 0.0; // the cost of using bw in this resource
	LinkedList<Storage> storageList = new LinkedList<Storage>();

	DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
		arch, os, vmm, hostList, time_zone, cost, costPerMem,
		costPerStorage, costPerBw);

	// 6. Finally, we need to create a HddDatacenter object.
	Datacenter datacenter = null;
	try {
	    datacenter = new HddDataCenter(name, characteristics,
		    new VmAllocationPolicySimple(hostList), storageList, 0);
	} catch (Exception e) {
	    e.printStackTrace();
	}

	return datacenter;
    }

    private Datacenter createDatacenter2(final String name) {
	List<Host> hostList = new ArrayList<Host>();

	List<Pe> peList = new ArrayList<>();
	List<HddPe> hddList = new ArrayList<>();

	int mips = (int) (10000 * (2.5 / 3.4));
	int iops = 7500;

	for (int i = 0; i < 8; i++) {
	    peList.add(new Pe(Id.pollId(Pe.class), new PeProvisionerSimple(mips)));
	}
	pe3 = peList.get(0);
	pe4 = peList.get(1);
	hddList.add(new HddPe(new PeProvisionerSimple(iops), DATA2));

	int ram = 2048 * 4; // host memory (MB)
	long storage = 1000000; // host storage
	int bw = 10000;

	vmScheduler2 = new VmSchedulerMapVmsToPes<Pe>(peList) {

	    @Override
	    protected VmScheduler createSchedulerFroPe(final Pe pe) {
		return new VmSchedulerTimeSharedOverSubscription(Arrays.asList(pe));
	    }
	};

	hostList.add(new HddHost(new RamProvisionerSimple(ram),
		new BwProvisionerSimple(bw), storage, peList, hddList,
		vmScheduler2,
		new VmDiskScheduler(hddList)));

	// 5. Create a DatacenterCharacteristics object that stores the
	// properties of a data center: architecture, OS, list of
	// Machines, allocation policy: time- or space-shared, time zone
	// and its price (G$/Pe time unit).
	String arch = "x86"; // system architecture
	String os = "Linux"; // operating system
	String vmm = "Xen";
	double time_zone = 10.0; // time zone this resource located
	double cost = 3.0; // the cost of using processing in this resource
	double costPerMem = 0.05; // the cost of using memory in this resource
	double costPerStorage = 0.001; // the cost of using storage in this
	// resource
	double costPerBw = 0.0; // the cost of using bw in this resource
	LinkedList<Storage> storageList = new LinkedList<Storage>();

	DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
		arch, os, vmm, hostList, time_zone, cost, costPerMem,
		costPerStorage, costPerBw);

	// 6. Finally, we need to create a HddDatacenter object.
	Datacenter datacenter = null;
	try {
	    datacenter = new HddDataCenter(name, characteristics,
		    new VmAllocationPolicySimple(hostList), storageList, 0);
	} catch (Exception e) {
	    e.printStackTrace();
	}

	return datacenter;
    }
}
