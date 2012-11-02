/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation
 *               of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009, The University of Melbourne, Australia
 */

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

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Datacenter;
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
import org.cloudbus.cloudsim.incubator.util.TextUtil;
import org.cloudbus.cloudsim.incubator.web.ILoadBalancer;
import org.cloudbus.cloudsim.incubator.web.SimpleWebLoadBalancer;
import org.cloudbus.cloudsim.incubator.web.WebBroker;
import org.cloudbus.cloudsim.incubator.web.WebCloudlet;
import org.cloudbus.cloudsim.incubator.web.WebDataCenter;
import org.cloudbus.cloudsim.incubator.web.WebSession;
import org.cloudbus.cloudsim.incubator.web.extensions.HDPe;
import org.cloudbus.cloudsim.incubator.web.extensions.HddCloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.incubator.web.extensions.HddHost;
import org.cloudbus.cloudsim.incubator.web.extensions.HddVm;
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
 * 
 * @author nikolay.grozev
 * 
 */
public class Experiment {

    private static final int MINUTE = 60;
    private static final int HOUR = 60 * MINUTE;
    private static final int DAY = 24 * HOUR;
    private static final int SIMULATION_LENGTH = 2 * DAY;
    private static final Integer[] HOURS = new Integer[25];

    private static int REFRESH_TIME = 5;

    static {
	for (int i = 0; i <= 24; i++) {
	    HOURS[i] = i * HOUR;
	}
    }

    /**
     * Creates main() to run this example
     * 
     * @throws IOException
     * @throws SecurityException
     */
    public static void main(String[] args) throws SecurityException, IOException {
	long simulationStart = System.currentTimeMillis();

	// Step 0: Set up the logger
	Properties props = new Properties();
	try (InputStream is = Files.newInputStream(Paths.get("custom_log.properties"))) {
	    props.load(is);
	}
	CustomLog.configLogger(props);

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
	    WebBroker brokerDC1 = new WebBroker("BrokerDC1", REFRESH_TIME, SIMULATION_LENGTH,
		    Arrays.asList(dc1.getId()));
	    WebBroker brokerDC2 = new WebBroker("BrokerDC2", REFRESH_TIME, SIMULATION_LENGTH,
		    Arrays.asList(dc2.getId()));

	    // Step 4: Create virtual machines
	    HddVm appServerVM1DC1 = createVM(brokerDC1.getId());
	    HddVm appServerVM2DC1 = createVM(brokerDC1.getId());
	    HddVm dbServerVMDC1 = createVM(brokerDC1.getId());

	    HddVm appServerVM1DC2 = createVM(brokerDC2.getId());
	    HddVm appServerVM2DC2 = createVM(brokerDC2.getId());
	    HddVm dbServerVMDC2 = createVM(brokerDC2.getId());

	    // Step 5: Create load balancers for the virtual machines in the 2
	    // datacenters
	    ILoadBalancer balancerDC1 = new SimpleWebLoadBalancer(
		    Arrays.asList(appServerVM1DC1, appServerVM2DC1), dbServerVMDC1);
	    brokerDC1.addLoadBalancer(balancerDC1);

	    ILoadBalancer balancerDC2 = new SimpleWebLoadBalancer(
		    Arrays.asList(appServerVM1DC2, appServerVM2DC2), dbServerVMDC2);
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
	    List<Cloudlet> resultDC1 = brokerDC1.getCloudletReceivedList();
	    List<Cloudlet> resultDC2 = brokerDC2.getCloudletReceivedList();
	    List<WebSession> resultDC1Sessions = brokerDC1.getServedSessions();
	    List<WebSession> resultDC2Sessions = brokerDC2.getServedSessions();

	    // Step 10 : stop the simulation and print the results
	    CloudSim.stopSimulation();
	    printDetails(resultDC1Sessions, resultDC2Sessions);

	    System.err.println();
	    System.err.println("Simulation is finished!");
	} catch (Exception e) {
	    e.printStackTrace();
	    System.err.println("The simulation has been terminated due to an unexpected error");
	}
	System.err.println("Finished in " + (System.currentTimeMillis() - simulationStart) / 1000 + " seconds");
    }

    private static HddVm createVM(int brokerId) {
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

    private static List<WorkloadGenerator> generateWorkloadsDC1() {
	double nullPoint = 0;

	String[] periods = new String[] {
		String.format("[%d,%d] m=5 std=1", HOURS[0], HOURS[5]),
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
		String.format("(%d,%d] m=5 std=1", HOURS[21], HOURS[24]) };
	return generateWorkload(nullPoint, periods);
    }

    private static List<WorkloadGenerator> generateWorkloadsDC2() {
	double nullPoint = 12 * HOUR;

	String[] periods = new String[] {
		String.format("[%d,%d] m=10 std=1", HOURS[0], HOURS[5]),
		String.format("(%d,%d] m=40 std=2", HOURS[5], HOURS[6]),
		String.format("(%d,%d] m=80 std=2", HOURS[6], HOURS[7]),
		String.format("(%d,%d] m=100 std=4", HOURS[7], HOURS[8]),
		String.format("(%d,%d] m=160 std=4", HOURS[8], HOURS[9]),
		String.format("(%d,%d] m=200 std=5", HOURS[9], HOURS[12]),
		String.format("(%d,%d] m=100 std=2", HOURS[12], HOURS[13]),
		String.format("(%d,%d] m=180 std=5", HOURS[13], HOURS[14]),
		String.format("(%d,%d] m=200 std=5", HOURS[14], HOURS[17]),
		String.format("(%d,%d] m=160 std=2", HOURS[17], HOURS[18]),
		String.format("(%d,%d] m=100 std=2", HOURS[18], HOURS[19]),
		String.format("(%d,%d] m=80 std=2", HOURS[19], HOURS[20]),
		String.format("(%d,%d] m=40 std=2", HOURS[20], HOURS[21]),
		String.format("(%d,%d] m=10 std=1", HOURS[21], HOURS[24]) };
	return generateWorkload(nullPoint, periods);
    }

    private static List<WorkloadGenerator> generateWorkload(double nullPoint, String[] periods) {
	int asCloudletLength = 100;
	int asRam = 1;
	int dbCloudletLength = 10;
	int dbRam = 1;
	int dbCloudletIOLength = 5;
	int duration = 200;

	int numberOfCloudlets = duration / REFRESH_TIME;
	numberOfCloudlets = numberOfCloudlets == 0 ? 1 : numberOfCloudlets;

	ISessionGenerator sessGen = new ConstSessionGenerator(asCloudletLength, asRam, dbCloudletLength,
		dbRam, dbCloudletIOLength, duration, numberOfCloudlets);

	double unit = HOUR;
	double periodLength = DAY;

	FrequencyFunction freqFun = new PeriodicStochasticFrequencyFunction(unit, periodLength, nullPoint,
		CompositeValuedSet.createCompositeValuedSet(periods));
	return Arrays.asList(new WorkloadGenerator(freqFun, sessGen));
    }

    private static Datacenter createDatacenter(String name) {
	List<Host> hostList = new ArrayList<Host>();

	List<Pe> peList = new ArrayList<>();
	List<HDPe> hddList = new ArrayList<>();

	int mips = 1000;
	int iops = 1000;

	peList.add(new Pe(0, new PeProvisionerSimple(mips)));
	hddList.add(new HDPe(new PeProvisionerSimple(iops)));

	int ram = 2048 * 2; // host memory (MB)
	long storage = 1000000; // host storage
	int bw = 10000;

	hostList.add(new HddHost(new RamProvisionerSimple(ram),
		new BwProvisionerSimple(bw), storage, peList, hddList,
		new VmSchedulerTimeShared(peList), new VmSchedulerTimeSharedOverSubscription(hddList)));

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
	    datacenter = new WebDataCenter(name, characteristics,
		    new VmAllocationPolicySimple(hostList), storageList, 0);
	} catch (Exception e) {
	    e.printStackTrace();
	}

	return datacenter;
    }

    /**
     * Prints the results with a header.
     * 
     * @param list
     *            list of Cloudlets
     */
    private static void printDetails(List<?>... lists) {
	// Print header line
	CustomLog.printLine(TextUtil.getCaptionLine(lists[0].get(0).getClass()));

	// Print details for each cloudlet
	for (List<?> list : lists) {
	    for (Object o : list) {
		CustomLog.print(TextUtil.getTxtLine(o));
	    }
	}
    }
}
