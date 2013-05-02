package org.cloudbus.cloudsim.ex.mapreduce.models.request;

import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.ex.util.Id;

public class ReduceTask extends Task {
	
	public String description;
	
	public ReduceTask(String name, int mi, String description) {
		//reduce task dSize is 0 for now, and it will be updated in Request constractor, after creating the job
		super(name, 0, mi);
		
		this.description = description;
	}

	public void updateDSize(Request request)
	{
		for (MapTask mapTask : request.job.mapTasks)
			if (mapTask.intermediateData.containsKey(name))
				dSize += mapTask.intermediateData.get(name);
	}

}
