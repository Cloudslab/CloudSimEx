/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation
 *               of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009, The University of Melbourne, Australia
 */

package org.cloudbus.cloudsim.incubator.web.examples;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

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
import org.cloudbus.cloudsim.incubator.web.ASStatGenerator;
import org.cloudbus.cloudsim.incubator.web.IGenerator;
import org.cloudbus.cloudsim.incubator.web.WebBroker;
import org.cloudbus.cloudsim.incubator.web.WebCloudlet;
import org.cloudbus.cloudsim.incubator.web.WebSession;
import org.cloudbus.cloudsim.incubator.web.extensions.HDPe;
import org.cloudbus.cloudsim.incubator.web.extensions.HddVm;
import org.cloudbus.cloudsim.incubator.web.extensions.HddCloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.incubator.web.extensions.HddHost;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.uncommons.maths.number.NumberGenerator;
import org.uncommons.maths.random.GaussianGenerator;
import org.uncommons.maths.random.MersenneTwisterRNG;

public class CloudSimWebExampleNew {

    /**
     * Creates main() to run this example
     * 
     * @throws IOException
     * @throws SecurityException
     */
    public static void main(String[] args) throws SecurityException,
	    IOException {

	// Step 0: Set up the logger
	Properties props = new Properties();
	try (InputStream is = Files.newInputStream(Paths.get("custom_log.properties"))) {
	    props.load(is);
	}
	CustomLog.configLogger(props);

	CustomLog.printLine("Example of Web session modelling.");

	try {
	    // Step1: Initialize the CloudSim package. It should be called
	    // before creating any entities.
	    int numBrokers = 1; // number of brokers we'll be using
	    boolean trace_flag = false; // mean trace events

	    // Initialize CloudSim
	    CloudSim.init(numBrokers, Calendar.getInstance(), trace_flag);

	    // Step 2: Create Datacenters - the resource providers in CloudSim
	    // Datacenter datacenter0 =
	    createDatacenter("WebDataCenter");

	    // Step 3: Create Broker
	    WebBroker broker = new WebBroker("Broker", 5, 10000);

	    // Step 4: Create virtual machines
	    List<Vm> vmlist = new ArrayList<Vm>();

	    // VM description
	    int mips = 250;
	    int ioMips = 200;
	    long size = 10000; // image size (MB)
	    int ram = 512; // vm memory (MB)
	    long bw = 1000;
	    int pesNumber = 1; // number of cpus
	    String vmm = "Xen"; // VMM name

	    // create two VMs
	    HddVm appServerVM = new HddVm(broker.getId(), mips, ioMips, pesNumber,
		    ram, bw, size, vmm, new HddCloudletSchedulerTimeShared());

	    HddVm dbServerVM = new HddVm(broker.getId(), mips, ioMips, pesNumber,
		    ram, bw, size, vmm, new HddCloudletSchedulerTimeShared());

	    // add the VMs to the vmList
	    vmlist.add(appServerVM);
	    vmlist.add(dbServerVM);

	    // submit vm list to the broker
	    broker.submitVmList(vmlist);

	    List<WebSession> sessions = generateRandomSessions(broker, appServerVM, dbServerVM, 10);
	    broker.submitSessions(sessions);

	    // Sixth step: Starts the simulation
	    CloudSim.startSimulation();

	    // Final step: Print results when simulation is over
	    List<Cloudlet> newList = broker.getCloudletReceivedList();

	    CloudSim.stopSimulation();

	    printCloudletList(newList);

	    // Print the debt of each user to each datacenter
	    // datacenter0.printDebts();

	    System.err.println("\nSimulation is finished!");
	} catch (Exception e) {
	    e.printStackTrace();
	    System.err.println("The simulation has been terminated due to an unexpected error");
	}
    }

    private static List<WebSession> generateRandomSessions(WebBroker broker, Vm appServerVM, Vm dbServerVM,
	    int sessionNum) {
	// Create Random Generators
	Random rng = new MersenneTwisterRNG();
	GaussianGenerator gen = new GaussianGenerator(1000, 20, rng);
	Map<String, NumberGenerator<Double>> generators = new HashMap<>();
	generators.put(ASStatGenerator.CLOUDLET_LENGTH, gen);
	generators.put(ASStatGenerator.CLOUDLET_RAM, gen);
	generators.put(ASStatGenerator.CLOUDLET_IO, gen);
	IGenerator<WebCloudlet> asGenerator = new ASStatGenerator(broker, generators);
	IGenerator<WebCloudlet> dbGenerator = new ASStatGenerator(broker, generators);

	List<WebSession> sessions = new ArrayList<>();
	for (int i = 0; i < sessionNum; i++) {
	    WebSession session = new WebSession(asGenerator, dbGenerator);
	    session.setAppVmId(appServerVM.getId());
	    session.setDbVmId(dbServerVM.getId());
	    sessions.add(session);
	}
	return sessions;
    }

    private static Datacenter createDatacenter(String name) {

	// Here are the steps needed to create a PowerDatacenter:
	// 1. We need to create a list to store
	// our machine
	List<Host> hostList = new ArrayList<Host>();

	// 2. A Machine contains one or more PEs or CPUs/Cores.
	// In this example, it will have only one core.
	List<Pe> peList = new ArrayList<>();
	List<HDPe> hddList = new ArrayList<>();

	int mips = 1000;
	int iops = 1000;

	// 3. Create PEs and add these into a list.
	peList.add(new Pe(0, new PeProvisionerSimple(mips))); // need to store
							      // Pe id and
							      // MIPS Rating

	hddList.add(new HDPe(new PeProvisionerSimple(iops)));
	
	// 4. Create Host with its id and list of PEs and add them to the list
	// of machines
	int ram = 2048; // host memory (MB)
	long storage = 1000000; // host storage
	int bw = 10000;

	hostList.add(new HddHost(new RamProvisionerSimple(ram),
		new BwProvisionerSimple(bw), storage, peList, hddList,
		new VmSchedulerTimeShared(peList), new VmSchedulerTimeSharedOverSubscription(hddList))); // This is our machine

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
	LinkedList<Storage> storageList = new LinkedList<Storage>(); // we are
								     // not
								     // adding
								     // SAN
								     // devices
								     // by
								     // now

	DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
		arch, os, vmm, hostList, time_zone, cost, costPerMem,
		costPerStorage, costPerBw);

	// 6. Finally, we need to create a PowerDatacenter object.
	Datacenter datacenter = null;
	try {
	    datacenter = new Datacenter(name, characteristics,
		    new VmAllocationPolicySimple(hostList), storageList, 0);
	} catch (Exception e) {
	    e.printStackTrace();
	}

	return datacenter;
    }

    /**
     * Prints the Cloudlet objects
     * 
     * @param list
     *            list of Cloudlets
     */
    private static void printCloudletList(List<Cloudlet> list) {
	int size = list.size();
	Cloudlet cloudlet;

	CustomLog.printLine("\n\n========== OUTPUT ==========");
	// Printe header line
	CustomLog.printLine(TextUtil.getCaptionLine(WebCloudlet.class));

	// Print details for each cloudlet
	for (int i = 0; i < size; i++) {
	    cloudlet = list.get(i);
	    CustomLog.print(TextUtil.getTxtLine(cloudlet));
	}
    }
}
