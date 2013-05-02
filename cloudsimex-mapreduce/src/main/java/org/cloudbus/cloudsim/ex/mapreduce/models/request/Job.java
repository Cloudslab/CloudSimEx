package org.cloudbus.cloudsim.ex.mapreduce.models.request;

import java.util.List;

public class Job {
	public String dataSourceName;
	public List<MapTask> mapTasks;
	public List<ReduceTask> reduceTasks;
}
