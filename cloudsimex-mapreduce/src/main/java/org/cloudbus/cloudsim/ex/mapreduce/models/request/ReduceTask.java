package org.cloudbus.cloudsim.ex.mapreduce.models.request;

import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.ex.mapreduce.models.Cloud;
import org.cloudbus.cloudsim.ex.util.Id;

public class ReduceTask extends Cloudlet {

	public String description;
	
	public ReduceTask(String description, int idSize, int mipb) {
		//WARNING: pesNumber is 1, is that OK?
		super(Id.pollId(MapTask.class), mipb*idSize, 1, idSize, 0, new UtilizationModelFull(), new UtilizationModelFull(), new UtilizationModelFull());
		
		this.description = description;
		
		this.setUserId(Cloud.brokerID);
	}

}
