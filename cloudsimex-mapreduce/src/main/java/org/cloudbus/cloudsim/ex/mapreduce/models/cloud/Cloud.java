package org.cloudbus.cloudsim.ex.mapreduce.models.cloud;

import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.*;
import org.cloudbus.cloudsim.ex.util.Id;

public class Cloud {
	
	public List<MapReduceDatacenter> mapReduceDatacenters;
	public List<DataSource> dataSources;
	public List<List> throughputs_vm_vm;
	public List<List> throughputs_vm_ds;
	
	public static int brokerID = -1;

	public List<VMType> getAllVMTypes()
	{
		
		List<VMType> vmlist = new ArrayList<VMType>();
		for (MapReduceDatacenter mapReduceDatacenter : mapReduceDatacenters) {
			vmlist.addAll(mapReduceDatacenter.vmTypes);
		}
		
		return vmlist;
	}
	
	public VMType findVMType(int VMTypeId)
	{
		for (MapReduceDatacenter mapReduceDatacenter : mapReduceDatacenters) {
			for (VMType vmType : mapReduceDatacenter.vmTypes) {
				if(vmType.getId() == VMTypeId)
					return vmType;
			}
		}
		
		return null;
	}
}
