package org.cloudbus.cloudsim.ex.mapreduce.models.request;

import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmInstance;

public class Requests {

    public List<Request> requests;

    public Requests(List<Request> requests)
    {
	this.requests = requests;
    }

    public Request getRequestFromTaskId(int taskId)
    {
	for (Request request : requests) {
	    for (MapTask mapTask : request.job.mapTasks) {
		if (mapTask.getCloudletId() == taskId)
		    return request;
	    }

	    for (ReduceTask reduceTask : request.job.reduceTasks) {
		if (reduceTask.getCloudletId() == taskId)
		    return request;
	    }
	}

	return null;
    }

    public Task getTaskFromId(int taskId)
    {
	Task task = null;
	for (Request request : requests) {
	    task = Request.getTaskFromId(taskId, request.job);
	    if (task != null)
		return task;
	}

	return task;
    }

    public double getSubmissionTime(int taskId)
    {
	for (Request request : requests) {
	    for (MapTask mapTask : request.job.mapTasks) {
		if (mapTask.getCloudletId() == taskId)
		    return request.submissionTime;
	    }

	    for (ReduceTask reduceTask : request.job.reduceTasks) {
		if (reduceTask.getCloudletId() == taskId)
		    return request.submissionTime;
	    }
	}

	return 0.0;
    }

    public Request getRequestFromId(int requestId) {
	for (Request request : requests) {
	    if (request.id == requestId)
		return request;
	}
	return null;

    }

    public VmInstance getVmInstance(int vmInstanceId) {
	for (Request request : requests) {
	    VmInstance vmInstance = request.getProvisionedVm(vmInstanceId);
	    if (vmInstance != null)
		return vmInstance;
	}
	return null;
    }

    public List<Task> getAllTasks()
    {
	List<Task> allTasks = new ArrayList<Task>();
	for (Request request : requests) {
	    allTasks.addAll(request.getAllTasks());
	}
	return allTasks;
    }

}
