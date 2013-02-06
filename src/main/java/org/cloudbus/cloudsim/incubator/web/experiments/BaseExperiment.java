package org.cloudbus.cloudsim.incubator.web.experiments;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.incubator.disk.HddCloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.incubator.disk.HddDataCenter;
import org.cloudbus.cloudsim.incubator.disk.HddHost;
import org.cloudbus.cloudsim.incubator.disk.HddPe;
import org.cloudbus.cloudsim.incubator.disk.HddVm;
import org.cloudbus.cloudsim.incubator.disk.VmDiskScheduler;
import org.cloudbus.cloudsim.incubator.util.CustomLog;
import org.cloudbus.cloudsim.incubator.util.TextUtil;
import org.cloudbus.cloudsim.incubator.web.ILoadBalancer;
import org.cloudbus.cloudsim.incubator.web.SimpleWebLoadBalancer;
import org.cloudbus.cloudsim.incubator.web.WebBroker;
import org.cloudbus.cloudsim.incubator.web.WebSession;
import org.cloudbus.cloudsim.incubator.web.workload.WorkloadGenerator;
import org.cloudbus.cloudsim.incubator.web.workload.freq.CompositeValuedSet;
import org.cloudbus.cloudsim.incubator.web.workload.freq.FrequencyFunction;
import org.cloudbus.cloudsim.incubator.web.workload.freq.PeriodicStochasticFrequencyFunction;
import org.cloudbus.cloudsim.incubator.web.workload.sessions.ConstSessionGenerator;
import org.cloudbus.cloudsim.incubator.web.workload.sessions.ISessionGenerator;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

/**
 * Base experiment. It's protected methods and properties should be overridden
 * to customize different lifecycle events - creation of data centers, workload
 * etc.
 * 
 * @author nikolay.grozev
 * 
 */
public class BaseExperiment {

    protected static final int MINUTE = 60;
    protected static final int HOUR = 60 * MINUTE;
    protected static final int DAY = 24 * HOUR;
    protected static final Integer[] HOURS = new Integer[25];

    protected int simulationLength;
    protected int refreshTime;
    protected String experimentName;

    static {
	for (int i = 0; i <= 24; i++) {
	    HOURS[i] = i * HOUR;
	}
    }

    public static void main(final String[] args) throws IOException {
	// Step 0: Set up the logger
	parseExperimentParameters(args);

	new BaseExperiment(2 * DAY, 4, "[Base Experimnet]").runExperimemt();
    }

    public BaseExperiment(final int simulationLength, final int refreshTime, final String experimentName) {
	super();
	this.simulationLength = simulationLength;
	this.refreshTime = refreshTime;
	this.experimentName = experimentName;
    }

    /**
     * Sets up the experiment and runs it.
     * 
     * @throws SecurityException
     * @throws IOException
     */
    public final void runExperimemt() throws SecurityException, IOException {
	long simulationStart = System.currentTimeMillis();

	try {
	    // Step1: Initialize the CloudSim package. It should be called
	    // before creating any entities.
	    int numBrokers = 2; // number of brokers we'll be using
	    boolean trace_flag = false; // mean trace events
	    CloudSim.init(numBrokers, Calendar.getInstance(), trace_flag);

	    // Step 2: Create Datacenters
	    Datacenter dc1 = createDatacenter("WebDataCenter1");
	    Datacenter dc2 = createDatacenter("WebDataCenter2");

	    // Step 3: Create Brokers
	    WebBroker brokerDC1 = new WebBroker("BrokerDC1", refreshTime, simulationLength,
		    Arrays.asList(dc1.getId()));
	    WebBroker brokerDC2 = new WebBroker("BrokerDC2", refreshTime, simulationLength,
		    Arrays.asList(dc2.getId()));

	    // Step 4: Create virtual machines
	    HddVm dbServerVMDC1 = createVM(brokerDC1.getId());
	    HddVm dbServerVMDC2 = createVM(brokerDC2.getId());

	    List<HddVm> appServersVMDC1 = createApplicationServerVMS(brokerDC1);
	    List<HddVm> appServersVMDC2 = createApplicationServerVMS(brokerDC2);

	    // Step 5: Create load balancers for the virtual machines in the 2
	    // datacenters
	    ILoadBalancer balancerDC1 = new SimpleWebLoadBalancer(
		    appServersVMDC1, dbServerVMDC1);
	    brokerDC1.addLoadBalancer(balancerDC1);

	    ILoadBalancer balancerDC2 = new SimpleWebLoadBalancer(
		    appServersVMDC2, dbServerVMDC2);
	    brokerDC2.addLoadBalancer(balancerDC2);

	    // Step 6: Add the virtual machines fo the data centers
	    List<Vm> vmlistDC1 = new ArrayList<Vm>();
	    vmlistDC1.addAll(balancerDC1.getAppServers());
	    vmlistDC1.add(balancerDC1.getDbServer());
	    brokerDC1.submitVmList(vmlistDC1);

	    List<Vm> vmlistDC2 = new ArrayList<Vm>();
	    vmlistDC2.addAll(balancerDC2.getAppServers());
	    vmlistDC2.add(balancerDC2.getDbServer());
	    brokerDC2.submitVmList(vmlistDC2);

	    // Step 7: Define the workload and associate it with load balancers
	    List<WorkloadGenerator> workloadDC1 = generateWorkloadsDC1();
	    brokerDC1.addWorkloadGenerators(workloadDC1, balancerDC1.getId());

	    List<WorkloadGenerator> workloadDC2 = generateWorkloadsDC2();
	    brokerDC2.addWorkloadGenerators(workloadDC2, balancerDC2.getId());

	    // Step 8: Starts the simulation
	    CloudSim.startSimulation();

	    // Step 9: get the results
	    // List<WebCloudlet> resultDC1 =
	    // brokerDC1.getCloudletReceivedList();
	    // List<WebCloudlet> resultDC2 =
	    // brokerDC2.getCloudletReceivedList();
	    List<WebSession> resultDC1Sessions = brokerDC1.getServedSessions();
	    List<WebSession> resultDC2Sessions = brokerDC2.getServedSessions();

	    // Step 10 : stop the simulation and print the results
	    CloudSim.stopSimulation();
	    printResults(WebSession.class, resultDC1Sessions, resultDC2Sessions);

	    // Comparator<WebCloudlet> comparator = new
	    // Comparator<WebCloudlet>() {
	    // @Override
	    // public int compare(WebCloudlet o1, WebCloudlet o2) {
	    // return
	    // Double.valueOf(o1.getIdealStartTime()).compareTo(o2.getIdealStartTime());
	    // }
	    // };
	    // Collections.sort(resultDC1, comparator);
	    // Collections.sort(resultDC2, comparator);
	    // printDetails(WebCloudlet.class, resultDC1, resultDC2);

	    System.err.println();
	    System.err.println(experimentName + ": Simulation is finished!");
	} catch (Exception e) {
	    e.printStackTrace();
	    System.err.println(experimentName + ": The simulation has been terminated due to an unexpected error");
	}
	System.err.println(experimentName + ": Finished in " + (System.currentTimeMillis() - simulationStart) / 1000
		+ " seconds");
    }

    protected List<HddVm> createApplicationServerVMS(final WebBroker brokerDC1) {
	return Arrays.asList(createVM(brokerDC1.getId()), createVM(brokerDC1.getId()));
    }

    protected HddVm createVM(final int brokerId) {
	// VM description
	int mips = 250;
	int ioMips = 200;
	long size = 10000; // image size (MB)
	int ram = 1024; // vm memory (MB)
	long bw = 1000;
	int pesNumber = 1; // number of cpus
	String vmm = "Xen"; // VMM name

	// create two VMs
	HddVm appServerVM = new HddVm(brokerId, mips, ioMips, pesNumber,
		ram, bw, size, vmm, new HddCloudletSchedulerTimeShared());
	return appServerVM;
    }

    protected List<WorkloadGenerator> generateWorkloadsDC1() {
	double nullPoint = 0;
	String[] periods = new String[] {
		String.format("[%d,%d] m=6 std=1", HOURS[0], HOURS[5]),
		String.format("(%d,%d] m=20 std=2", HOURS[5], HOURS[6]),
		String.format("(%d,%d] m=40 std=2", HOURS[6], HOURS[7]),
		String.format("(%d,%d] m=50 std=4", HOURS[7], HOURS[8]),
		String.format("(%d,%d] m=80 std=4", HOURS[8], HOURS[9]),
		String.format("(%d,%d] m=100 std=5", HOURS[9], HOURS[12]),
		String.format("(%d,%d] m=50 std=2", HOURS[12], HOURS[13]),
		String.format("(%d,%d] m=90 std=5", HOURS[13], HOURS[14]),
		String.format("(%d,%d] m=100 std=5", HOURS[14], HOURS[17]),
		String.format("(%d,%d] m=80 std=2", HOURS[17], HOURS[18]),
		String.format("(%d,%d] m=50 std=2", HOURS[18], HOURS[19]),
		String.format("(%d,%d] m=40 std=2", HOURS[19], HOURS[20]),
		String.format("(%d,%d] m=20 std=2", HOURS[20], HOURS[21]),
		String.format("(%d,%d] m=6 std=1", HOURS[21], HOURS[24]) };
	return generateWorkload(nullPoint, periods);
    }

    protected List<WorkloadGenerator> generateWorkloadsDC2() {
	double nullPoint = 12 * HOUR;
	String[] periods = new String[] {
		String.format("[%d,%d] m=9 std=1", HOURS[0], HOURS[5]),
		String.format("(%d,%d] m=30 std=2", HOURS[5], HOURS[6]),
		String.format("(%d,%d] m=60 std=2", HOURS[6], HOURS[7]),
		String.format("(%d,%d] m=75 std=4", HOURS[7], HOURS[8]),
		String.format("(%d,%d] m=120 std=4", HOURS[8], HOURS[9]),
		String.format("(%d,%d] m=150 std=5", HOURS[9], HOURS[12]),
		String.format("(%d,%d] m=75 std=2", HOURS[12], HOURS[13]),
		String.format("(%d,%d] m=135 std=5", HOURS[13], HOURS[14]),
		String.format("(%d,%d] m=150 std=5", HOURS[14], HOURS[17]),
		String.format("(%d,%d] m=120 std=2", HOURS[17], HOURS[18]),
		String.format("(%d,%d] m=75 std=2", HOURS[18], HOURS[19]),
		String.format("(%d,%d] m=60 std=2", HOURS[19], HOURS[20]),
		String.format("(%d,%d] m=30 std=2", HOURS[20], HOURS[21]),
		String.format("(%d,%d] m=9 std=1", HOURS[21], HOURS[24]) };
	return generateWorkload(nullPoint, periods);
    }

    protected List<WorkloadGenerator> generateWorkload(final double nullPoint, final String[] periods) {
	int asCloudletLength = 200;
	int asRam = 1;
	int dbCloudletLength = 50;
	int dbRam = 1;
	int dbCloudletIOLength = 50;
	int duration = 200;

	return generateWorkload(nullPoint, periods, asCloudletLength, asRam, dbCloudletLength, dbRam,
		dbCloudletIOLength, duration);
    }

    protected List<WorkloadGenerator> generateWorkload(final double nullPoint, final String[] periods,
	    final int asCloudletLength,
	    final int asRam, final int dbCloudletLength, final int dbRam, final int dbCloudletIOLength,
	    final int duration) {
	int numberOfCloudlets = duration / refreshTime;
	numberOfCloudlets = numberOfCloudlets == 0 ? 1 : numberOfCloudlets;

	ISessionGenerator sessGen = new ConstSessionGenerator(asCloudletLength, asRam, dbCloudletLength,
		dbRam, dbCloudletIOLength, duration, numberOfCloudlets);

	double unit = HOUR;
	double periodLength = DAY;

	FrequencyFunction freqFun = new PeriodicStochasticFrequencyFunction(unit, periodLength, nullPoint,
		CompositeValuedSet.createCompositeValuedSet(periods));
	return Arrays.asList(new WorkloadGenerator(freqFun, sessGen));
    }

    protected Datacenter createDatacenter(final String name) {
	List<Host> hostList = new ArrayList<Host>();

	List<Pe> peList = new ArrayList<>();
	List<HddPe> hddList = new ArrayList<>();

	int mips = 500;
	int iops = 1000;

	peList.add(new Pe(0, new PeProvisionerSimple(mips)));
	peList.add(new Pe(1, new PeProvisionerSimple(mips)));
	hddList.add(new HddPe(new PeProvisionerSimple(iops)));

	int ram = 2048 * 4; // host memory (MB)
	long storage = 1000000; // host storage
	int bw = 10000;

	hostList.add(new HddHost(new RamProvisionerSimple(ram),
		new BwProvisionerSimple(bw), storage, peList, hddList,
		new VmSchedulerTimeSharedOverSubscription(peList),
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

	// 6. Finally, we need to create a PowerDatacenter object.
	Datacenter datacenter = null;
	try {
	    datacenter = new HddDataCenter(name, characteristics,
		    new VmAllocationPolicySimple(hostList), storageList, 0);
	} catch (Exception e) {
	    e.printStackTrace();
	}

	return datacenter;
    }

    /**
     * Prints the objects' details with a header.
     * 
     * @param list
     *            list of Cloudlets
     */
    protected static void printResults(final Class<?> caption, final List<?>... lines) {
	// Print header line
	CustomLog.printLine(TextUtil.getCaptionLine(caption));

	// Print details for each cloudlet
	for (List<?> list : lines) {
	    for (Object o : list) {
		CustomLog.print(TextUtil.getTxtLine(o));
	    }
	}
    }

    /**
     * Parses the experiments parameters and sets up the logger.
     * 
     * @param args
     * @throws IOException
     */
    protected static void parseExperimentParameters(final String[] args) throws IOException {
	Properties props = new Properties();
	try (InputStream is = Files.newInputStream(Paths.get(args[1]))) {
	    props.load(is);
	}
	props.put(CustomLog.FILE_PATH_PROP_KEY, args[0]);
	CustomLog.configLogger(props);
    }
}
