package org.cloudbus.cloudsim.ex.mapreduce.models.request;

import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.ex.util.Id;

public class MapTask extends Task {
	
	public List<String> dataSources;
	
	public MapTask(String description, int dSize, int idSize, int mipb, List<String> dataSources) {
		super(description, dSize, idSize, mipb);
		
		this.dataSources = dataSources;
	}

}
