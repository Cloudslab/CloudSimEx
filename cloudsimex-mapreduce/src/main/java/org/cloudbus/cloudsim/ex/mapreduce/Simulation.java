package org.cloudbus.cloudsim.ex.mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Calendar;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.PrivateCloudDatacenter;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.PublicCloudDatacenter;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Requests;
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
	
	//From Cloud.yaml
	private static Cloud cloud;
	
	//From Requests.yaml
	private static Requests requests;
	
	
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
			//WARNING: the engine name must be "MapReduceEngine"
			MapReduceEngine engine = new MapReduceEngine();
			Cloud.brokerID = engine.getId();
			
			// Create datacentres and cloudlets
			cloud = YamlFile.getCloudFromYaml(Properties.CLOUD.getProperty());
			requests = YamlFile.getRequestsFromYaml(Properties.REQUESTS.getProperty());
			engine.setCloud(cloud);
			engine.setRequests(requests);
			
			//START
			CloudSim.startSimulation();
			engine.printExecutionSummary();
			

			Log.printLine("");
			Log.printLine("");
		} catch (Exception e) {
			Log.printLine("Unwanted errors happen.");
			e.printStackTrace();
		} finally {
			CloudSim.stopSimulation();
		}
	}
}
