package org.cloudbus.cloudsim.ex.mapreduce.models;

import java.util.List;

import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.*;
import org.cloudbus.cloudsim.ex.util.Id;

public class Cloud {
	
	public List<MapReduceDatacenter> mapReduceDatacenters;
	public List<DataSource> dataSources;
	public List<List> throughputs_vm_vm;
	public List<List> throughputs_vm_ds;
	
	public static int brokerID = -1;
	
	public Cloud() {
		
		
		
		/*
		List<DataSource> dataSources = new ArrayList<DataSource>();
		List<MapReduceDatacenter> mapReduceDatacenters = new ArrayList<MapReduceDatacenter>();
		Double[][] throughputs_vm_vm;
		Double[][] throughputs_vm_ds;
		
		this.dataSources = dataSources;
		this.mapReduceDatacenters = mapReduceDatacenters;
		this.throughputs_vm_vm = throughputs_vm_vm;
		this.throughputs_vm_ds = throughputs_vm_ds;
		*/
	}
}
