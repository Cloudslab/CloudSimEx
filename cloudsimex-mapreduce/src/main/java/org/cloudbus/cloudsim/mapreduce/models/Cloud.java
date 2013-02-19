package org.cloudbus.cloudsim.mapreduce.models;

import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.mapreduce.models.cloud.*;

public class Cloud {
	
	List<DataSource> dataSources = new ArrayList<DataSource>();
	List<MapReduceDatacenter> mapReduceDatacenters = new ArrayList<MapReduceDatacenter>();
	Double[][] throughputs_vm_vm;
	Double[][] throughputs_vm_ds;
}
