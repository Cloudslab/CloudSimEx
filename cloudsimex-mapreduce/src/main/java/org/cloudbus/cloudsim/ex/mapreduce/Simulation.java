package org.cloudbus.cloudsim.ex.mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.NetworkTopology;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.ex.mapreduce.models.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.Requests;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.MapReduceDatacenter;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VMType;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.MapTask;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.ReduceTask;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.yaml.snakeyaml.Yaml;

/**
 * This class contains the main method for execution of the simulation.
 * Here, simulation parameters are defined. Parameters that are dynamic
 * are read from the properties file, whereas other parameters are hardcoded.
 * 
 * Decision on what should be configurable and what is hardcoded was somehow
 * arbitrary. Therefore, if you think that some of the hardcoded values should
 * be customizable, it can be added as a Property. In the Property code there
 * is comments on how to add new properties to the experiment.
 *
 */
public class Simulation {
	
	private static Cloud cloud;
	private static Requests requests;

	/** The cloudlet list. */
	private static List<MapTask> cloudletList_MapTasks;
	private static List<ReduceTask> cloudletList_ReduceTasks;

	/** The vmlist. */
	private static List<VMType> vmlist;
	
	public Simulation(){
		
	}

	/**
	 * Prints input parameters and execute the simulation a number of times,
	 * as defined in the configuration.
	 * 
	 */
	public static void main(String[] args) {
		new Simulation();
		Log.printLine("========== Simulation configuration ==========");
		for (Properties property: Properties.values()){
			Log.printLine("= "+property+": "+property.getProperty());
		}
		Log.printLine("==============================================");
		Log.printLine("");
				
		int rounds = Integer.parseInt(Properties.EXPERIMENT_ROUNDS.getProperty());
		for (int round=1; round<=rounds; round++) {
			runSimulationRound(round);
		}
	}
				
	/**
	 * One round of the simulation is executed by this method. Output
	 * is printed to the log.
	 * 
	 */
	private static void runSimulationRound(int round) {
		Log.printLine("Starting simulation round "+round+".");

		try {
			// Initialize the CloudSim library
			CloudSim.init(1,Calendar.getInstance(),false);
						
						
			// Create Broker
			MapReduceEngine engine = createMapReduceEngine();
			Cloud.brokerID = engine.getId();
			
			// Create datacentres and cloudlets
			loadYAMLFiles();

			CloudSim.startSimulation();
			engine.printExecutionSummary();


			// Print the debt of each user to each datacenter
			for (MapReduceDatacenter mapReduceDatacenter : cloud.mapReduceDatacenters)
				mapReduceDatacenter.printSummary();

			Log.printLine("");
			Log.printLine("");
		} catch (Exception e) {
			Log.printLine("Unwanted errors happen.");
			e.printStackTrace();
		} finally {
			CloudSim.stopSimulation();
		}
	}


	private static void loadYAMLFiles()
	{
		Yaml yaml = new Yaml();
		
		InputStream document = null;
		try {
			if(Cloud.brokerID == -1)
				throw new Exception("brokerID is not set");
			
			document = new FileInputStream(new File("Cloud.yaml"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		cloud = (Cloud) yaml.load(document);
		
		try {
			document = new FileInputStream(new File("Requests.yaml"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		requests = (Requests) yaml.load(document);
	}

	
	private static MapReduceEngine createMapReduceEngine(){
		MapReduceEngine engine = null;
		try {
			engine = new MapReduceEngine("MapReduce_Broker");
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return engine;
	}
}
