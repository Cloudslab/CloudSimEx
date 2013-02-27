package org.cloudbus.cloudsim.ex.mapreduce.models.request;

import java.util.List;

import org.cloudbus.cloudsim.ex.mapreduce.models.request.*;

public class Requests {
	
	public List<Request> requests;
	
	public Task getTaskFromId(int taskId)
	{
		Task task = null;
		for (Request request : requests) {
			task = request.getTaskFromId(taskId);
			if(task != null)
				return task;
		}
		
		return task;
	}
	
	public double getSubmissionTime(int taskId)
	{
		for (Request request : requests) {
			for (MapTask mapTask : request.job.mapTasks) {
				if(mapTask.getCloudletId() == taskId)
					return request.submissionTime;
			}
			
			for (ReduceTask reduceTask : request.job.reduceTasks) {
				if(reduceTask.getCloudletId() == taskId)
					return request.submissionTime;
			}
		}
		
		return 0.0;
	}

	public Request getRequestFromId(int requestId) {
		for (Request request : requests) {
			if(request.id == requestId)
				return request;
		}
		return null;
		
	}

}
