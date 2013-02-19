package org.cloudbus.cloudsim.mapreduce.models;

import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.mapreduce.models.cloud.*;
import org.yaml.snakeyaml.Yaml;

public class Cloud {
	
	List<MapReduceDatacenter> mapReduceDatacenters = new ArrayList<MapReduceDatacenter>();
	List<DataSource> dataSources = new ArrayList<DataSource>();
	Double[][] throughputs_vm_vm;
	Double[][] throughputs_vm_ds;
	
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
