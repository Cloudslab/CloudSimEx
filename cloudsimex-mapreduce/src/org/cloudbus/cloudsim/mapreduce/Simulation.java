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
public class Simulation {
	
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
		
		long seed1 = SeedGenerator.getSeed1(round-1);
		long seed2 = SeedGenerator.getSeed2(round-1);
		long seed3 = SeedGenerator.getSeed2(round-1);

		try {
			CloudSim.init(1,Calendar.getInstance(),false);
			
			String datacentre1_name = Properties.DATACENTER1_NAME.getProperty();
			int datacentre1_hosts = Integer.parseInt(Properties.DATACENTER1_HOSTS.getProperty());
			
			WorkflowDatacenter datacenter = createDatacenter(datacentre1_name, datacentre1_hosts, seed1, seed2);
			//STOPPED HERE
			WorkflowEngine engine = createWorkflowEngine(seed3);
			
			double latency = Double.parseDouble(Properties.NETWORK_LATENCY.getProperty());
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

	private static WorkflowDatacenter createDatacenter(String name, int hosts, long seed1, long seed2) throws Exception{
		int ram = 8*Integer.parseInt(Properties.MEMORY_PERHOST.getProperty());
		int cores = 8*Integer.parseInt(Properties.CORES_PERHOST.getProperty());
		int mips = 8*Integer.parseInt(Properties.MIPS_PERCORE.getProperty());
		long storage = 8*Long.parseLong(Properties.STORAGE_PERHOST.getProperty());
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
								  storage,peList,new VmSchedulerIaaS(peList,seed1)));
		}

		DatacenterCharacteristics characteristics = new DatacenterCharacteristics("Xeon","Linux","Xen",hostList,10.0,0.0,0.00,0.00,0.00);
		
		//bandwidth and latency are 0.0, is that OK?
		return new WorkflowDatacenter(name,characteristics,new VmAllocationPolicySimple(hostList),0.0,0.0,mips,delay,offers,seed2);
	}

	private static WorkflowEngine createWorkflowEngine(long seed){
		String dagFile = Properties.DAG_FILE.getProperty();
		String className = Properties.SCHEDULING_POLICY.getProperty();
		String offerName = Properties.VM_OFFERS.getProperty();
		double dbDeadline = Long.parseLong(Properties.DAG_DEADLINE.getProperty())*0.75; //makes the deadline 25% smaller
		long deadline = (long) Math.ceil(dbDeadline);
		
		int baseMIPS = Integer.parseInt(Properties.MIPS_PERCORE.getProperty());
		Policy policy = null;
		VMOffers offers = null;
		
		try{		
			Class<?> policyClass = Class.forName(className,true,Policy.class.getClassLoader());
			policy = (Policy) policyClass.newInstance();
			
			Class<?> offerClass = Class.forName(offerName,true,VMOffers.class.getClassLoader());
			offers = (VMOffers) offerClass.newInstance();
		
			return new WorkflowEngine(dagFile,deadline,baseMIPS,policy,offers,seed);
		} catch (Exception e){
			e.printStackTrace();
			return null;
		}
	}
}
