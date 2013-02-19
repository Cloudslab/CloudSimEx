package org.cloudbus.cloudsim.mapreduce;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.NetworkTopology;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

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
public class GenerateDeadline {
	
	public GenerateDeadline(){
		
	}

	/**
	 * Prints input parameters and execute the simulation a number of times,
	 * as defined in the configuration.
	 * 
	 */
	public static void main(String[] args) {
		
		if (args.length!=1){
			System.out.println("Must specify DAG file.");
			System.exit(1);
		}
		
		new GenerateDeadline();
		Log.printLine("========== Simulation configuration ==========");
		for (Properties property: Properties.values()){
			Log.printLine("= "+property+": "+property.getProperty());
		}
		Log.printLine("==============================================");
		Log.printLine("");
				
		runSimulation(args[0]);
	}
				
	/**
	 * One round of the simulation is executed by this method. Output
	 * is printed to the log.
	 * 
	 */
	private static void runSimulation(String dagFile) {
		Log.printLine("Starting simulation.");
		
		try {
			CloudSim.init(1,Calendar.getInstance(),false);

			OptimalMapReduceDatacenter datacenter = createDatacenter("Datacenter");
			MapReduceEngine engine = createWorkflowEngine(dagFile);
			
			double latency = 0.5;
			NetworkTopology.addLink(datacenter.getId(),engine.getId(),100000,latency);

			CloudSim.startSimulation();
			engine.printExecutionSummary();
			datacenter.printSummary();

			Log.printLine("");
			Log.printLine("");
		} catch (Exception e) {
			Log.printLine("Unwanted errors happen.");
			e.printStackTrace();
		} finally {
			CloudSim.stopSimulation();
		}
	}

	private static OptimalMapReduceDatacenter createDatacenter(String name) throws Exception{
		int hosts = Integer.parseInt(Properties.HOSTS_PERDATACENTER.getProperty());
		int ram = 8*Integer.parseInt(Properties.MEMORY_PERHOST.getProperty());
		int cores = 8*Integer.parseInt(Properties.CORES_PERHOST.getProperty());
		int mips = 8*Integer.parseInt(Properties.MIPS_PERCORE.getProperty());
		long storage = 8*Long.parseLong(Properties.STORAGE_PERHOST.getProperty());
		double bw = Double.parseDouble(Properties.INTERNAL_BANDWIDTH.getProperty());
		double latency = Double.parseDouble(Properties.INTERNAL_LATENCY.getProperty());
		long delay = Long.parseLong(Properties.VM_DELAY.getProperty());
		String offerName = Properties.VM_OFFERS.getProperty();
		
		VMOffers offers = null;
		try{				
			Class<?> offerClass = Class.forName(offerName,true,VMOffers.class.getClassLoader());
			offers = (VMOffers) offerClass.newInstance();
		} catch (Exception e){
			e.printStackTrace();
			return null;
		}
							
		List<Host> hostList = new ArrayList<Host>();
		for(int i=0;i<hosts;i++){
			List<Pe> peList = new ArrayList<Pe>();
			for(int j=0;j<cores;j++) peList.add(new Pe(j, new PeProvisionerSimple(mips)));
			
			hostList.add(new Host(i,new RamProvisionerSimple(ram),new BwProvisionerSimple(1000000),
								  storage,peList,new VmSchedulerTimeShared(peList)));
		}

		DatacenterCharacteristics characteristics = new DatacenterCharacteristics("Xeon","Linux","Xen",hostList,10.0,0.0,0.00,0.00,0.00);
				
		return new OptimalMapReduceDatacenter(name,characteristics,new VmAllocationPolicySimple(hostList),bw,latency,mips,delay,offers);
	}

	private static MapReduceEngine createWorkflowEngine(String dagFile){
		String offerName = Properties.VM_OFFERS.getProperty();
		int baseMIPS = Integer.parseInt(Properties.MIPS_PERCORE.getProperty());
		VMOffers offers = null;
		
		try{		
			Class<?> offerClass = Class.forName(offerName,true,VMOffers.class.getClassLoader());
			offers = (VMOffers) offerClass.newInstance();
		
			return new MapReduceEngine(dagFile,1000000000,baseMIPS,new OptimalPolicy(),offers,0);
		} catch (Exception e){
			e.printStackTrace();
			return null;
		}
	}
}
