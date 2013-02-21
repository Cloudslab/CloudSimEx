package org.cloudbus.cloudsim.ex.mapreduce.models.request;

import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.ex.mapreduce.models.Cloud;
import org.cloudbus.cloudsim.ex.util.Id;

public class MapTask extends Cloudlet {
	
	public String description;
	public List<String> dataSources;
	
	public MapTask(String description, int dSize, int idSize, int mipb, List<String> dataSources) {
		//WARNING: pesNumber is 1, is that OK?
		super(Id.pollId(MapTask.class), mipb*dSize, 1, dSize, idSize, new UtilizationModelFull(), new UtilizationModelFull(), new UtilizationModelFull());
		
		this.description = description;
		this.dataSources = dataSources;
		
		this.setUserId(Cloud.brokerID);
	}

}
