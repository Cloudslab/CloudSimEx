package org.cloudbus.cloudsim.ex.web.experiments;

import static org.cloudbus.cloudsim.Consts.*;

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
import org.cloudbus.cloudsim.ex.web.ConstGenerator;
import org.cloudbus.cloudsim.ex.web.ILoadBalancer;
import org.cloudbus.cloudsim.ex.web.SimpleDBBalancer;
import org.cloudbus.cloudsim.ex.web.SimpleWebLoadBalancer;
import org.cloudbus.cloudsim.ex.web.WebSession;
import org.cloudbus.cloudsim.ex.web.workload.IWorkloadGenerator;
import org.cloudbus.cloudsim.ex.web.workload.SimpleWorkloadGenerator;
import org.cloudbus.cloudsim.ex.web.workload.brokers.PerformanceLoggingWebBroker;
import org.cloudbus.cloudsim.ex.web.workload.brokers.WebBroker;
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
public class SingleDatacentre {

    public static String RESULT_DIR = "results/stat/";
    protected int numOfSessions = 500;
    protected int simulationLength = DAY;
    protected int step = 60;
    protected String experimentName;

    protected VmSchedulerMapVmsToPes<Pe> vmScheduler;
    protected Pe pe1;
    protected Pe pe2;

    private static final DataItem DATA = new DataItem(5);

    /**
     * @param args
     * @throws IOException
     */
    public static void main(final String[] args) throws IOException {
	ExperimentsUtil.parseExperimentParameters(args);
	new SingleDatacentre().runExperimemt();
    }

    /**
     * Sets up the experiment and runs it.
     * 
     * @throws SecurityException
     * @throws IOException
     */
    public final void runExperimemt() throws SecurityException, IOException {
	long simulationStart = System.currentTimeMillis();

	CustomLog.redirectToFile(RESULT_DIR + "/performance_sessions_" + numOfSessions + ".csv");

	try {
	    // Step1: Initialize the CloudSim package. It should be called
	    // before creating any entities.
	    int numBrokers = 1; // number of brokers we'll be using
	    boolean trace_flag = false; // mean trace events
	    CloudSim.init(numBrokers, Calendar.getInstance(), trace_flag, 0.001);

	    // Step 2: Create Datacenters
	    Datacenter dc1 = createDatacenter("WebDataCenter1");

	    // Step 3: Create Brokers
	    WebBroker brokerDC1 = new PerformanceLoggingWebBroker("BrokerDC1", step, simulationLength, 0.1,
		    0.01,
		    5 * step,
		    dc1.getId());

	    // Step 4: Create virtual machines
	    HddVm dbServerVMDC1 = createVM(brokerDC1.getId());
	    List<HddVm> appServersVMDC1 = createApplicationServerVMS(brokerDC1);

	    vmScheduler.map(dbServerVMDC1.getId(), pe1.getId());
	    vmScheduler.map(appServersVMDC1.get(0).getId(), pe2.getId());

	    // Step 5: Create load balancers for the virtual machines in the 2
	    // datacenters
	    ILoadBalancer balancerDC1 = new SimpleWebLoadBalancer(
		    1, "127.0.0.1", appServersVMDC1, new SimpleDBBalancer(dbServerVMDC1));
	    brokerDC1.addLoadBalancer(balancerDC1);

	    // Step 6: Add the virtual machines for the data centers
	    List<Vm> vmlistDC1 = new ArrayList<Vm>();
	    vmlistDC1.addAll(balancerDC1.getAppServers());
	    vmlistDC1.addAll(balancerDC1.getDbBalancer().getVMs());
	    brokerDC1.submitVmList(vmlistDC1);

	    // Step 7: Define the workloadseed and associate it with load balancers
	    List<? extends IWorkloadGenerator> workloadDC1 = generateWorkloadsDC1(brokerDC1.getId());
	    brokerDC1.addWorkloadGenerators(workloadDC1, balancerDC1.getAppId());

	    // Step 8: Starts the simulation
	    CloudSim.startSimulation();

	    // Step 9: get the results
	    List<WebSession> resultDC1Sessions = brokerDC1.getServedSessions();
	    List<Cloudlet> cloudlets = brokerDC1.getCloudletReceivedList();

	    // Step 10 : stop the simulation and print the results
	    CloudSim.stopSimulation();
	    CustomLog.redirectToFile(RESULT_DIR + "simulation_sessions_" + numOfSessions + ".csv");
	    CustomLog.printResults(WebSession.class, resultDC1Sessions);

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

    private List<? extends IWorkloadGenerator> generateWorkloadsDC1(final int userId) {
	try (InputStream asIO = new FileInputStream(RESULT_DIR + "web_cloudlets.txt");
		InputStream dbIO = new FileInputStream(RESULT_DIR + "db_cloudlets.txt")) {
	    StatSessionGenerator sessionGenerator = new StatSessionGenerator(GeneratorsUtil.parseStream(asIO),
		    GeneratorsUtil.parseStream(dbIO), userId, step, new ConstGenerator<String[]>(new String[]{}), DATA);

	    // return Arrays.asList(new
	    // PeriodWorkloadGenerator(sessionGenerator, 0.1, numOfSessions));

	    return Arrays.asList(new SimpleWorkloadGenerator(numOfSessions,
		    sessionGenerator, null, null, 1));

	    // return Arrays.asList(new SimpleWorkloadGenerator(numOfSessions /
	    // 2, sessionGenerator, null, null, 2));

	} catch (IOException e) {
	    e.printStackTrace();
	}
	return null;
    }

    private List<HddVm> createApplicationServerVMS(final DatacenterBrokerEX brokerDC1) {
	return Arrays.asList(createVM(brokerDC1.getId()));
    }

    private HddVm createVM(final int brokerId) {
	// VM description
	int mips = 10000;
	int ioMips = 10000;
	long size = 10000; // image size (MB)
	int ram = 512; // vm memory (MB)
	long bw = 1000;
	int pesNumber = 1; // number of cpus
	String vmm = "Xen"; // VMM name

	// create two VMs
	HddVm appServerVM = new HddVm("App-Srv", brokerId, mips, ioMips, pesNumber,
		ram, bw, size, vmm, new HddCloudletSchedulerTimeShared(), new Integer[0]);
	return appServerVM;
    }

    private Datacenter createDatacenter(final String name) {
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
	hddList.add(new HddPe(new PeProvisionerSimple(iops), DATA));

	int ram = 2048 * 4; // host memory (MB)
	long storage = 1000000; // host storage
	int bw = 10000;

	vmScheduler = new VmSchedulerMapVmsToPes<Pe>(peList) {

	    @Override
	    protected VmScheduler createSchedulerFroPe(final Pe pe) {
		return new VmSchedulerTimeSharedOverSubscription(Arrays.asList(pe));
	    }
	};

	hostList.add(new HddHost(new RamProvisionerSimple(ram),
		new BwProvisionerSimple(bw), storage, peList, hddList,
		vmScheduler,
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
