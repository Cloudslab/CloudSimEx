package org.cloudbus.cloudsim.ex.web.experiments;

import static org.cloudbus.cloudsim.ex.web.experiments.ExperimentsUtil.DAY;
import static org.cloudbus.cloudsim.ex.web.experiments.ExperimentsUtil.HOUR;
import static org.cloudbus.cloudsim.ex.web.experiments.ExperimentsUtil.HOURS;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.cloudbus.cloudsim.ex.DatacenterBrokerEX;
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
import org.cloudbus.cloudsim.ex.web.ILoadBalancer;
import org.cloudbus.cloudsim.ex.web.RandomListGenerator;
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

import com.google.common.collect.ImmutableMap;

/**
 * 
 * @author nikolay.grozev
 * 
 */
public class TwoDatacentres_MultiCloudFramework {

    private static final Set<String> EURO_CODES = Collections.unmodifiableSet(new LinkedHashSet<>(
	    Arrays.asList("BE", "FR", "AT", "BG", "IT", "PL", "CZ", "CY", "PT", "DK", "LV", "RO", "DE",
		    "LT", "SI", "EE", "LU", "SK", "IE", "HU", "FI", "EL", "MT", "SE", "ES", "NL", "UK")));

    private static final Set<String> US_CODES = Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList("US")));

    private static final String DUBLIN_IP = "5.149.168.0";
    private static final String NEW_YORK_IP = "74.221.217.130";

    private static final String HAMINA_FINLAND_IP = "80.222.136.0";
    private static final String DALAS_IP = "74.125.126.0";

    private static final int MIPS_VM_GOOGLE = (int) (10000 * (2600.0 / 3400.0));
    private static final int MIPS_DB_VM_EC2 = (int) (10000 * ((2266 * (100 - 0.241971) / 100) / 3400.0));
    private static final int MIPS_AS_VM_EC2 = (int) (10000 * ((2666 * (100 - 3.028893) / 100) / 3400.0));

    public static String DEF_DIR = "multi-cloud/";
    public static String RESULT_DIR = "multi-cloud/stat/";

    protected int simulationLength = DAY + HOUR / 2;
    protected int step = 60;
    protected String experimentName;

    private static final DataItem DATA_EURO1 = new DataItem(5);
    private static final DataItem DATA_EURO2 = new DataItem(5);
    private static final DataItem DATA_EURO3 = new DataItem(5);
    private static final DataItem DATA_EURO4 = new DataItem(5);
    private static final DataItem[] EURO_DATA_ITEMS = new DataItem[] { DATA_EURO1, DATA_EURO2, DATA_EURO3, DATA_EURO4 };

    private static final DataItem DATA_US1 = new DataItem(5);
    private static final DataItem DATA_US2 = new DataItem(5);
    private static final DataItem DATA_US3 = new DataItem(5);
    private static final DataItem DATA_US4 = new DataItem(5);
    private static final DataItem[] US_DATA_ITEMS = new DataItem[] { DATA_US1, DATA_US2, DATA_US3, DATA_US4 };

    /**
     * @param args
     * @throws IOException
     */
    public static void main(final String[] args) throws IOException {
	ExperimentsUtil.parseExperimentParameters(args);
	new TwoDatacentres_MultiCloudFramework().runExperimemt();
    }

    /**
     * Sets up the experiment and runs it.
     * 
     * @throws SecurityException
     * @throws IOException
     */
    public final void runExperimemt() throws SecurityException, IOException {
	long simulationStart = System.currentTimeMillis();

	CustomLog.redirectToFile(RESULT_DIR + "/log.txt");

	try {
	    // == == == == == == == == == == == == == == == == == == == == == ==
	    // Step1: Initialize the CloudSim package.
	    int numBrokers = 4; // number of brokers to use
	    boolean trace_flag = false;
	    CloudSim.init(numBrokers, Calendar.getInstance(), trace_flag, 0.001);

	    // == == == == == == == == == == == == == == == == == == == == == ==
	    // Step 2: Create Datacenters
	    int hardwareMips = (int) (10000 * 3.4);
	    int hardwareMiops = 7500;
	    int hardwareRam = 2048 * 4;
	    Datacenter dcEuroGoogle = createDC("EuroDataCenter1", EURO_DATA_ITEMS, hardwareMips, hardwareMiops,
		    hardwareRam);
	    Datacenter dcEuroEC2 = createDC("EuroDataCenter2", EURO_DATA_ITEMS, hardwareMips, hardwareMiops,
		    hardwareRam);

	    Datacenter dcUSGoogle = createDC("USDataCenter1", US_DATA_ITEMS, hardwareMips, hardwareMiops, hardwareRam);
	    Datacenter dcUSEC2 = createDC("USDataCenter2", US_DATA_ITEMS, hardwareMips, hardwareMiops, hardwareRam);

	    // == == == == == == == == == == == == == == == == == == == == == ==
	    // Step 3: Create Brokers
	    double offset = 0.01;
	    double logPeriod = 1;
	    double idlePeriod = 5 * step;
	    WebBroker brokerEuroGoogle = new PerformanceLoggingWebBroker("BrokerEuroDC1", step, simulationLength,
		    logPeriod, offset, idlePeriod,
		    dcEuroGoogle.getId());

	    WebBroker brokerEuroEC2 = new PerformanceLoggingWebBroker("BrokerEuroDC2", step, simulationLength,
		    logPeriod, offset, idlePeriod,
		    dcEuroEC2.getId());

	    WebBroker brokerUSGoogle = new PerformanceLoggingWebBroker("BrokerUSDC1", step, simulationLength,
		    logPeriod, offset, idlePeriod,
		    dcUSGoogle.getId());

	    WebBroker brokerUSEC2 = new PerformanceLoggingWebBroker("BrokerUSDC2", step, simulationLength,
		    logPeriod, offset, idlePeriod,
		    dcUSEC2.getId());

	    // == == == == == == == == == == == == == == == == == == == == == ==
	    // Step 4: Create virtual machines
	    int numDBs = 3;
	    int numApp = 3;
	    List<HddVm> dbServersEuroGoogle = createVMs(brokerEuroGoogle.getId(), MIPS_VM_GOOGLE, 7500, 3840, numDBs);
	    List<HddVm> appServersEuroGoogle = createVMs(brokerEuroGoogle.getId(), MIPS_VM_GOOGLE, 7500, 3840, numApp);

	    List<HddVm> dbServersEuroEC2 = createVMs(brokerEuroGoogle.getId(), MIPS_AS_VM_EC2, 7500, 1666, numDBs);
	    List<HddVm> appServersEuroEC2 = createVMs(brokerEuroGoogle.getId(), MIPS_DB_VM_EC2, 7500, 1666, numApp);

	    List<HddVm> dbServersUSGoogle = createVMs(brokerEuroEC2.getId(), MIPS_VM_GOOGLE, 7500, 3840, numDBs);
	    List<HddVm> appServersUSGoogle = createVMs(brokerEuroEC2.getId(), MIPS_VM_GOOGLE, 7500, 3840, numApp);

	    List<HddVm> dbServersUSEC2 = createVMs(brokerEuroEC2.getId(), MIPS_AS_VM_EC2, 7500, 1666, numDBs);
	    List<HddVm> appServersUSEC2 = createVMs(brokerEuroEC2.getId(), MIPS_DB_VM_EC2, 7500, 1656, numApp);

	    // == == == == == == == == == == == == == == == == == == == == == ==
	    // Step 5: Create load balancers
	    ILoadBalancer balancerEuroGoogle = new SimpleWebLoadBalancer(
		    1, HAMINA_FINLAND_IP, appServersEuroGoogle, new SimpleDBBalancer(dbServersEuroGoogle));
	    brokerEuroGoogle.addLoadBalancer(balancerEuroGoogle);

	    ILoadBalancer balancerEuroEC2 = new SimpleWebLoadBalancer(
		    1, DUBLIN_IP, appServersEuroEC2, new SimpleDBBalancer(dbServersEuroEC2));
	    brokerEuroEC2.addLoadBalancer(balancerEuroEC2);

	    ILoadBalancer balancerUSGoogle = new SimpleWebLoadBalancer(
		    1, DALAS_IP, dbServersUSGoogle, new SimpleDBBalancer(appServersUSGoogle));
	    brokerUSGoogle.addLoadBalancer(balancerUSGoogle);

	    ILoadBalancer balancerUSEC2 = new SimpleWebLoadBalancer(
		    1, NEW_YORK_IP, dbServersUSEC2, new SimpleDBBalancer(appServersUSEC2));
	    brokerUSEC2.addLoadBalancer(balancerUSEC2);

	    // == == == == == == == == == == == == == == == == == == == == == ==
	    // Step 6: Add the virtual machines for the data centers
	    ImmutableMap<ILoadBalancer, WebBroker> balancersToBrokers = ImmutableMap
		    .<ILoadBalancer, WebBroker> builder()
		    .put(balancerEuroGoogle, brokerEuroGoogle)
		    .put(balancerEuroEC2, brokerEuroEC2)
		    .put(balancerUSGoogle, brokerUSGoogle)
		    .put(balancerUSEC2, brokerUSEC2).build();

	    for (Map.Entry<ILoadBalancer, WebBroker> el : balancersToBrokers.entrySet()) {
		List<Vm> vmlistDC1 = new ArrayList<Vm>();
		vmlistDC1.addAll(el.getKey().getAppServers());
		vmlistDC1.addAll(el.getKey().getDbBalancer().getVMs());

		el.getValue().submitVmList(vmlistDC1);
	    }

	    // == == == == == == == == == == == == == == == == == == == == == ==
	    // Step 7: Define the workload and associate it with load balancers
	    List<? extends IWorkloadGenerator> workloadEuro =
		    generateWorkloadsDC(
			    brokerEuroGoogle.getId(),
			    0 * HOUR,
			    ImmutableMap.<String[], Double> of(new String[] { "US" }, 1.0, new String[] { "EU" }, 10.0),
			    EURO_DATA_ITEMS);
	    brokerEuroGoogle.addWorkloadGenerators(workloadEuro, balancerEuroGoogle.getAppId());

	    List<? extends IWorkloadGenerator> workloadUS =
		    generateWorkloadsDC(
			    brokerUSGoogle.getId(),
			    12 * HOUR,
			    ImmutableMap.<String[], Double> of(new String[] { "US" }, 10.0, new String[] { "EU" }, 1.0),
			    US_DATA_ITEMS);
	    brokerUSGoogle.addWorkloadGenerators(workloadUS, balancerEuroEC2.getAppId());

	    // == == == == == == == == == == == == == == == == == == == == == ==
	    // Step 8: Starts the simulation
	    CloudSim.startSimulation();

	    // == == == == == == == == == == == == == == == == == == == == == ==
	    // Step 9: get the results
	    List<WebSession> resultDC1Sessions = brokerEuroGoogle.getServedSessions();
	    List<Cloudlet> cloudletsDC1 = brokerEuroGoogle.getCloudletReceivedList();

	    List<WebSession> resultDC2Sessions = brokerEuroEC2.getServedSessions();
	    List<Cloudlet> cloudletsDC2 = brokerEuroEC2.getCloudletReceivedList();

	    // == == == == == == == == == == == == == == == == == == == == == ==
	    // Step 10 : stop the simulation and print the results
	    CloudSim.stopSimulation();
	    CustomLog.redirectToFile(RESULT_DIR + "simulation_sessions_DC1.csv");
	    CustomLog.printResults(WebSession.class, resultDC1Sessions);

	    CustomLog.redirectToFile(RESULT_DIR + "simulation_sessions_DC2.csv");
	    CustomLog.printResults(WebSession.class, resultDC2Sessions);

	    System.err.println();
	    System.err.println(experimentName + ": Simulation is finished!");
	} catch (Exception e) {
	    System.err.println(experimentName + ": The simulation has been terminated due to an unexpected error");
	    e.printStackTrace();
	}
	System.err.println(experimentName + ": Finished in " +
		(System.currentTimeMillis() - simulationStart) / 1000 + " seconds");
    }

    protected List<? extends IWorkloadGenerator> generateWorkloadsDC(final int userId, final double nullPoint,
	    final Map<String[], Double> valuesAndFreqs, final DataItem... data) {
	String[] periods = new String[] {
		String.format("[%d,%d] m=10  std=1", HOURS[0], HOURS[6]),
		String.format("(%d,%d] m=30  std=2", HOURS[6], HOURS[7]),
		String.format("(%d,%d] m=50  std=3", HOURS[7], HOURS[10]),
		String.format("(%d,%d] m=100 std=4", HOURS[10], HOURS[14]),
		String.format("(%d,%d] m=50  std=3", HOURS[14], HOURS[17]),
		String.format("(%d,%d] m=30  std=2", HOURS[17], HOURS[18]),
		String.format("(%d,%d] m=10  std=1", HOURS[18], HOURS[24]) };
	return generateWorkload(userId, nullPoint, periods, valuesAndFreqs, data);
    }

    protected List<? extends IWorkloadGenerator> generateWorkload(final int userId, final double nullPoint,
	    final String[] periods, final Map<String[], Double> valuesAndFreqs, final DataItem[] data) {
	try (InputStream asIO = new FileInputStream(DEF_DIR + "web_cloudlets.txt");
		InputStream dbIO = new FileInputStream(DEF_DIR + "db_cloudlets.txt")) {
	    StatSessionGenerator sessGen = new StatSessionGenerator(GeneratorsUtil.parseStream(asIO),
		    GeneratorsUtil.parseStream(dbIO), userId, step, new RandomListGenerator<>(valuesAndFreqs), data);

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

    private List<HddVm> createVMs(final int brokerId, final int mips, final int ioMips, final int ram, int numberOfVms) {
	List<HddVm> hddVMs = new ArrayList<>();
	for (int i = 0; i < numberOfVms; i++) {
	    // VM description
	    // int mips = 10000;
	    // int ioMips = 10000;
	    long size = 10000; // image size (MB)
	    // int ram = 512; // vm memory (MB)
	    long bw = 1000;
	    int pesNumber = 1; // number of cpus
	    String vmm = "Xen"; // VMM name

	    HddVm hdedVm = new HddVm(brokerId, mips, ioMips, pesNumber,
		    ram, bw, size, vmm, new HddCloudletSchedulerTimeShared());
	    hddVMs.add(hdedVm);
	}
	return hddVMs;
    }

    /*
     * https://developers.google.com/compute/docs/instances
     */
    // mips = (int) (10000 * 3.4), miops = 7500, ram = 2048*4
    private Datacenter createDC(final String name, final DataItem[] dataItems, final int mips, final int miops,
	    final int ramInMb) {
	List<Host> hostList = new ArrayList<Host>();

	for (int i = 0; i < 100; i++) {
	    HddHost host = createHost(dataItems, mips, miops, ramInMb);
	    hostList.add(host);
	}

	Datacenter datacenter = createDCWithHosts(name, hostList);

	return datacenter;
    }

    private static Datacenter createDCWithHosts(final String name, final List<Host> hostList) {
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

    private static HddHost createHost(final DataItem[] dataItems, final int mips, final int miops, final int ramInMb) {
	List<HddPe> hddList = new ArrayList<>();

	List<Pe> peList = new ArrayList<>();

	for (int i = 0; i < 8; i++) {
	    peList.add(new Pe(Id.pollId(Pe.class), new PeProvisionerSimple(mips)));
	}
	hddList.add(new HddPe(new PeProvisionerSimple(miops), dataItems));

	long storage = 1000000; // host storage
	int bw = 10000;

	VmSchedulerMapVmsToPes<Pe> vmScheduler = new VmSchedulerMapVmsToPes<Pe>(peList) {

	    @Override
	    protected VmScheduler createSchedulerFroPe(final Pe pe) {
		return new VmSchedulerTimeSharedOverSubscription(Arrays.asList(pe));
	    }
	};

	HddHost host = new HddHost(new RamProvisionerSimple(ramInMb),
		new BwProvisionerSimple(bw), storage, peList, hddList,
		vmScheduler,
		new VmDiskScheduler(hddList));
	return host;
    }
}
