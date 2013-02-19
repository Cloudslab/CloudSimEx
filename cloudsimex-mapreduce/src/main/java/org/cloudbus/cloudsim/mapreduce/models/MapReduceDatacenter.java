package org.cloudbus.cloudsim.mapreduce.models;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.mapreduce.SeedGenerator;
import org.cloudbus.cloudsim.mapreduce.VmSchedulerIaaS;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

public class MapReduceDatacenter extends Datacenter {
	
	public List<VMType> vmtypes;

	public MapReduceDatacenter (String name, int hosts, int memory, int cores, int mips_precore, List<VMType> vmtypes) throws Exception {
		super(name,
				createChars(hosts, cores, mips_precore, memory, 1000000),
				new VmAllocationPolicySimple( getHostList(hosts, cores, mips_precore, memory, 1000000)),
				new LinkedList<Storage>(),
				0);
		
		this.vmtypes = vmtypes;
	}

	private static DatacenterCharacteristics createChars(int hosts, int cores, int mips_precore, int memory, int storage) {
		List<Host> hostList = getHostList(hosts, cores, mips_precore, memory, storage);
		
		DatacenterCharacteristics characteristics = new DatacenterCharacteristics("Xeon","Linux","Xen",hostList,10.0,0.0,0.00,0.00,0.00);
		
		return characteristics;
	}
	
	private static List<Host> getHostList(int hosts, int cores, int mips_precore, int memory, int storage)
	{
		List<Host> hostList = new ArrayList<Host>();
		for(int i=0;i<hosts;i++){
			List<Pe> peList = new ArrayList<Pe>();
			for(int j=0;j<cores;j++) peList.add(new Pe(j, new PeProvisionerSimple(mips_precore)));
			
			hostList.add(new Host(i,new RamProvisionerSimple(memory),new BwProvisionerSimple(1000000),
								  storage,peList,new VmSchedulerIaaS(peList,SeedGenerator.getSeed1(1))));
		}
		
		return hostList;
	}
}
