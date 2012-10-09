/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation
 *               of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009, The University of Melbourne, Australia
 */

package org.cloudbus.cloudsim.web.examples;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.cloudbus.cloudsim.web.ASStatGenerator;
import org.cloudbus.cloudsim.web.IGenerator;
import org.cloudbus.cloudsim.web.WebBroker;
import org.cloudbus.cloudsim.web.WebCloudlet;
import org.cloudbus.cloudsim.web.WebSession;
import org.cloudbus.cloudsimgoodies.util.CustomLog;
import org.cloudbus.cloudsimgoodies.util.Id;
import org.uncommons.maths.number.NumberGenerator;
import org.uncommons.maths.random.GaussianGenerator;
import org.uncommons.maths.random.MersenneTwisterRNG;

/**
 * A simple example showing how to create a datacenter with one host and run two
 * cloudlets on it. The cloudlets run in VMs with the same MIPS requirements.
 * The cloudlets will take the same time to complete the execution.
 */
public class CloudSimWebExample {


	/** The vmlist. */
	private static List<Vm> vmlist;

	/**
	 * Creates main() to run this example
	 * 
	 * @throws IOException
	 * @throws SecurityException
	 */
	public static void main(String[] args) throws SecurityException,
			IOException {

		Properties props = new Properties();
		props.load(CloudSimWebExample.class
				.getResourceAsStream("custom_log.properties"));

		CustomLog.configLogger(props);

		CustomLog.printLine("Starting CloudSimExample2...", null);

		try {
			// First step: Initialize the CloudSim package. It should be called
			// before creating any entities.
			int num_user = 1; // number of cloud users
			Calendar calendar = Calendar.getInstance();
			boolean trace_flag = false; // mean trace events

			// Initialize the CloudSim library
			CloudSim.init(num_user, calendar, trace_flag);

			// Second step: Create Datacenters
			// Datacenters are the resource providers in CloudSim. We need at
			// list one of them to run a CloudSim simulation
			Datacenter datacenter0 = createDatacenter("Datacenter_0");

			// Third step: Create Broker
			WebBroker broker = createBroker();
			int brokerId = broker.getId();

			// Fourth step: Create one virtual machine
			vmlist = new ArrayList<Vm>();

			// VM description
			int mips = 250;
			long size = 10000; // image size (MB)
			int ram = 512; // vm memory (MB)
			long bw = 1000;
			int pesNumber = 1; // number of cpus
			String vmm = "Xen"; // VMM name

			// create two VMs
			Vm vm1 = new Vm(Id.pollId(Vm.class), brokerId, mips, pesNumber, ram, bw, size,
					vmm, new CloudletSchedulerTimeShared());

			Vm vm2 = new Vm(Id.pollId(Vm.class), brokerId, mips, pesNumber, ram, bw, size,
					vmm, new CloudletSchedulerTimeShared());

			// add the VMs to the vmList
			vmlist.add(vm1);
			vmlist.add(vm2);

			// submit vm list to the broker
			broker.submitVmList(vmlist);

			Random rng = new MersenneTwisterRNG();
			GaussianGenerator gen = new GaussianGenerator(100, 15, rng);
			Map<String, NumberGenerator<Double>> generators = new HashMap<>();
			generators.put(ASStatGenerator.CLOUDLET_LENGTH, gen);
			generators.put(ASStatGenerator.CLOUDLET_RAM, gen);
			IGenerator<WebCloudlet> testGenerator = new ASStatGenerator(broker, generators);
			WebSession session = new WebSession(testGenerator, testGenerator);
			
			broker.submitSessions(Arrays.asList(session));

			// Sixth step: Starts the simulation
			CloudSim.startSimulation();

			// Final step: Print results when simulation is over
			List<Cloudlet> newList = broker.getCloudletReceivedList();

			CloudSim.stopSimulation();

			printCloudletList(newList);

			// Print the debt of each user to each datacenter
			datacenter0.printDebts();

			CustomLog.printLine("CloudSimExample2 finished!", null);
		} catch (Exception e) {
			e.printStackTrace();
			CustomLog
					.printLine(
							"The simulation has been terminated due to an unexpected error",
							null);
		}
	}

	private static Datacenter createDatacenter(String name) {

		// Here are the steps needed to create a PowerDatacenter:
		// 1. We need to create a list to store
		// our machine
		List<Host> hostList = new ArrayList<Host>();

		// 2. A Machine contains one or more PEs or CPUs/Cores.
		// In this example, it will have only one core.
		List<Pe> peList = new ArrayList<Pe>();

		int mips = 1000;

		// 3. Create PEs and add these into a list.
		peList.add(new Pe(0, new PeProvisionerSimple(mips))); // need to store
																// Pe id and
																// MIPS Rating

		// 4. Create Host with its id and list of PEs and add them to the list
		// of machines
		int hostId = 0;
		int ram = 2048; // host memory (MB)
		long storage = 1000000; // host storage
		int bw = 10000;

		hostList.add(new Host(hostId, new RamProvisionerSimple(ram),
				new BwProvisionerSimple(bw), storage, peList,
				new VmSchedulerTimeShared(peList))); // This is our machine

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

	// We strongly encourage users to develop their own broker policies, to
	// submit vms and cloudlets according
	// to the specific rules of the simulated scenario
	private static WebBroker createBroker() {

		WebBroker broker = null;
		try {
			broker = new WebBroker("Broker", 5, 10000);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return broker;
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

		String indent = "    ";
		CustomLog.printLine(CustomLog.NEW_LINE, null);
		CustomLog.printLine("========== OUTPUT ==========", null);
		CustomLog.printLine("Cloudlet ID" + indent + "STATUS" + indent
				+ "Data center ID" + indent + "VM ID" + indent + "Time"
				+ indent + "Start Time" + indent + "Finish Time", null);

		DecimalFormat dft = new DecimalFormat("###.##");
		for (int i = 0; i < size; i++) {
			cloudlet = list.get(i);
			CustomLog.print(
					indent + cloudlet.getCloudletId() + indent + indent, null);

			if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS) {
				CustomLog.print("SUCCESS", null);

				CustomLog.printLine(
						indent + indent + cloudlet.getResourceId() + indent
								+ indent + indent + cloudlet.getVmId() + indent
								+ indent
								+ dft.format(cloudlet.getActualCPUTime())
								+ indent + indent
								+ dft.format(cloudlet.getExecStartTime())
								+ indent + indent
								+ dft.format(cloudlet.getFinishTime()), null);
			}
		}

	}
}
