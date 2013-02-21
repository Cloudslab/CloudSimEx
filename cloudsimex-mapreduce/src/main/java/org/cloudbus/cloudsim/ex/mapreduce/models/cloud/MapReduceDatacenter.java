package org.cloudbus.cloudsim.ex.mapreduce.models.cloud;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerSpaceShared;
import org.cloudbus.cloudsim.ex.mapreduce.SeedGenerator;
import org.cloudbus.cloudsim.ex.mapreduce.VmSchedulerIaaS;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

public class MapReduceDatacenter extends Datacenter {
	
	public List<VMType> vmTypes;

	
	public MapReduceDatacenter (String name, int hosts, int memory_perhost, int cores_perhost, int mips_precore_perhost, List<VMType> vmtypes) throws Exception {
		super(name,
				createChars(hosts, cores_perhost, mips_precore_perhost, memory_perhost, 1000000),
				new VmAllocationPolicySimple( getHostList(hosts, cores_perhost, mips_precore_perhost, memory_perhost, 1000000)),
				new LinkedList<Storage>(),
				0);
		
		this.vmTypes = vmtypes;
	}
	
	
	public MapReduceDatacenter (String name, int hosts, int memory_perhost, int cores_perhost, int mips_precore_perhost) throws Exception {
		super(name,
				createChars(hosts, cores_perhost, mips_precore_perhost, memory_perhost, 1000000),
				new VmAllocationPolicySimple( getHostList(hosts, cores_perhost, mips_precore_perhost, memory_perhost, 1000000)),
				new LinkedList<Storage>(),
				0);
	}

	private static DatacenterCharacteristics createChars(int hosts, int cores_perhost, int mips_precore_perhost, int memory_perhost, int storage) {
		List<Host> hostList = getHostList(hosts, cores_perhost, mips_precore_perhost, memory_perhost, storage);
		
		DatacenterCharacteristics characteristics = new DatacenterCharacteristics("Xeon","Linux","Xen",hostList,10.0,0.0,0.00,0.00,0.00);
		
		return characteristics;
	}
	
	private static List<Host> getHostList(int hosts, int cores_perhost, int mips_precore_perhost, int memory_perhost, int storage)
	{
		List<Host> hostList = new ArrayList<Host>();
		for(int i=0;i<hosts;i++){
			List<Pe> peList = new ArrayList<Pe>();
			for(int j=0;j<cores_perhost;j++) peList.add(new Pe(j, new PeProvisionerSimple(mips_precore_perhost)));
			
			hostList.add(new Host(i,new RamProvisionerSimple(memory_perhost),new BwProvisionerSimple(1000000),
					  storage,peList,new VmSchedulerSpaceShared(peList)));
			//hostList.add(new Host(i,new RamProvisionerSimple(memory_perhost),new BwProvisionerSimple(1000000),
			//		  storage,peList,new VmSchedulerIaaS(peList,SeedGenerator.getSeed1(1))));
		}
		
		return hostList;
	}
	
	//Simulation output
	public void printSummary(){
		
		DecimalFormat df = new DecimalFormat("#.##");
		double cost = 0.0;
			
		Log.printLine();
		Log.printLine("======== DATACENTER SUMMARY ========");
		Log.printLine("= Cost: "+df.format(cost));
		
		Log.printLine("User id\t\tDebt");

		Set<Integer> keys = getDebts().keySet();
		Iterator<Integer> iter = keys.iterator();
		while (iter.hasNext()) {
			int key = iter.next();
			double value = getDebts().get(key);
			Log.printLine(key + "\t\t" + df.format(value));
		}
		
		Log.printLine("========== END OF SUMMARY =========");
	}
}
