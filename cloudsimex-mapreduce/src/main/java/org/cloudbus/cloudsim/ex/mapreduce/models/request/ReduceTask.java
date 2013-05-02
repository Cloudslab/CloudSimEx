package org.cloudbus.cloudsim.ex.mapreduce.models.request;

import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.ex.util.Id;

public class ReduceTask extends Task {
	
	public ReduceTask(String name, String description, int mipb) {
		//reduce task dSize is 0 for now, and it will be updated in Request constractor, after creating the job
		super(name, description, 0, mipb);
	}

	public void updateDSize(Request request)
	{
		for (MapTask mapTask : request.job.mapTasks)
			if (mapTask.intermediateData.containsKey(name))
				dSize += mapTask.intermediateData.get(name);
	}

}
