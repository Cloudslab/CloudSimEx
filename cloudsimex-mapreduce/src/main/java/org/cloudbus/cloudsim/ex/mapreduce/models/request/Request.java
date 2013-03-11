package org.cloudbus.cloudsim.ex.mapreduce.models.request;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmInstance;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmType;
import org.cloudbus.cloudsim.ex.util.Id;
import org.yaml.snakeyaml.Yaml;

public class Request extends SimEvent {
	public int id;
	public double submissionTime;
	public double budget;
	public int deadline;
	public Job job;
	public UserClass userClass;
	public double firstSubmissionTime;
	public double lastFinishTime;
	
	public List<VmInstance> mapAndReduceVmProvisionList;
	public List<VmInstance> reduceOnlyVmProvisionList;
	
	public Map<Integer, Integer> schedulingPlanForMap; //<MapTask ID, VM ID>
	public Map<Integer, Integer> schedulingPlanForReduce; //<ReduceTask ID, VM ID>

	public Request(double submissionTime, double budget, int deadline, String jobFile, UserClass userClass) {
		id = Id.pollId(Request.class);
		this.submissionTime = submissionTime;
		this.budget = budget;
		this.deadline = deadline;
		this.userClass = userClass;
		job = readJobYAML(jobFile);
		firstSubmissionTime = -1;
		lastFinishTime = -1;
		
		mapAndReduceVmProvisionList = new ArrayList<VmInstance>();
		reduceOnlyVmProvisionList = new ArrayList<VmInstance>();
		
		schedulingPlanForMap = new HashMap<Integer, Integer>();
		schedulingPlanForReduce = new HashMap<Integer, Integer>();

		for (MapTask mapTask : job.mapTasks) {
			mapTask.requestId = id;

			mapTask.setExecStartTime(submissionTime);

			// Set map task execution time
			mapTask.setCloudletLength(getMapMillionInstructions(mapTask));
		}

		for (ReduceTask reduceTask : job.reduceTasks) {
			reduceTask.requestId = id;

			reduceTask.setExecStartTime(submissionTime);

			// Set the reduce task execution time
			reduceTask.setCloudletLength(getReduceMillionInstructions(reduceTask));
		}
	}

	public Task getTaskFromId(int taskId) {
		for (MapTask mapTask : job.mapTasks) {
			if (mapTask.getCloudletId() == taskId)
				return mapTask;
		}

		for (ReduceTask reduceTask : job.reduceTasks) {
			if (reduceTask.getCloudletId() == taskId)
				return reduceTask;
		}

		return null;
	}

	private long getReduceMillionInstructions(ReduceTask reduceTask) {
		// Set the reduce task execution time
		long totalIntermediateDataSizeInMegaByte = 0;

		for (MapTask mapTask : job.mapTasks)
			if (mapTask.intermediateData.containsValue(reduceTask.name))
				totalIntermediateDataSizeInMegaByte += mapTask.intermediateData.get(reduceTask.name);

		long reduceTaskLengthInMillionInstructionsPerByte = totalIntermediateDataSizeInMegaByte * 1000000 * reduceTask.getCloudletLength();

		return reduceTaskLengthInMillionInstructionsPerByte;
	}

	private long getMapMillionInstructions(MapTask mapTask) {
		long dataSizeInMegaByte = mapTask.getCloudletFileSize();
		long mapTaskLengthInMillionInstructionsPerByte = dataSizeInMegaByte * 1000000 * mapTask.getCloudletLength();
		return mapTaskLengthInMillionInstructionsPerByte;
	}

	private Job readJobYAML(String jobFile) {
		Job job = new Job();

		Yaml yaml = new Yaml();
		InputStream document = null;

		try {
			document = new FileInputStream(new File(jobFile));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		job = (Job) yaml.load(document);

		return job;
	}

	public boolean isTaskInThisRequest(int cloudletId) {
		Task task = getTaskFromId(cloudletId);
		if(task == null)
			return false;
		else
			return true;
	}

}
