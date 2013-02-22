package org.cloudbus.cloudsim.ex.mapreduce.models.request;

import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.ex.util.Id;

public class ReduceTask extends Task {
	
	public ReduceTask(String description, int idSize, int mipb) {
		super(description, idSize, 0, mipb);
	}

}
