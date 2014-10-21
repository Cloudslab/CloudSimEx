package org.cloudbus.cloudsim.ex.mapreduce.models.request;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmInstance;
import org.cloudbus.cloudsim.ex.mapreduce.policy.Policy.CloudDeploymentModel;
import org.cloudbus.cloudsim.ex.util.Id;
import org.yaml.snakeyaml.Yaml;

public class Request extends SimEvent {
    public int id;
    public double submissionTime;
    public double budget;
    public double deadline;
    public Job job;
    public UserClass userClass;

    public List<VmInstance> mapAndReduceVmProvisionList;
    public List<VmInstance> reduceOnlyVmProvisionList;

    public Map<Integer, Integer> schedulingPlan; // <Task ID, VM ID>

    public String policy;
    public String jobFile;

    private int experimentNumber;
    private int workloadNumber;
    
    private Long algoStartTime = null;
    private Long algoFirstSoulationFoundedTime = null;
    private Long algorithRunningTime = null;

    private CloudDeploymentModel cloudDeploymentModel = CloudDeploymentModel.Hybrid;

    private String logMessage = "";
    
    

    public Request(double submissionTime, double deadline, double budget, String jobFile, UserClass userClass) {
	id = Id.pollId(Request.class);
	this.submissionTime = submissionTime;
	DecimalFormat df = new DecimalFormat(".00");
	this.budget = Double.valueOf(df.format(budget));
	this.deadline = Double.valueOf(df.format(deadline));
	this.jobFile = "inputs/profiles/" + jobFile;
	this.userClass = userClass;
	mapAndReduceVmProvisionList = new ArrayList<VmInstance>();
	reduceOnlyVmProvisionList = new ArrayList<VmInstance>();
	schedulingPlan = new HashMap<Integer, Integer>();

	job = readJobYAML(this.jobFile);
	// Add Extra Map Tasks
	List<MapTask> copyOfMapTasks = new ArrayList<MapTask>(job.mapTasks);
	for (MapTask mapTask : copyOfMapTasks) {
	    for (int i = mapTask.extraTasks; i > 0; i--)
		job.mapTasks.add(new MapTask(1, mapTask.dSize, mapTask.mi, mapTask.intermediateData));
	}
	// Set Request Id and data source name in all Map Tasks
	for (MapTask mapTask : job.mapTasks) {
	    mapTask.requestId = id;
	    mapTask.dataSourceName = job.dataSourceName;
	}
	// Set Request Id and data size in all Reduce Tasks
	for (ReduceTask reduceTask : job.reduceTasks) {
	    reduceTask.requestId = id;
	    reduceTask.updateDSize(this);
	}
    }

    public Request(double submissionTime, double deadline, double budget, Job job, String jobFile, UserClass userClass,
	    List<VmInstance> mapAndReduceVmProvisionList, List<VmInstance> reduceOnlyVmProvisionList,
	    Map<Integer, Integer> schedulingPlan, int experimentNumber, int workloadNumber,
	    CloudDeploymentModel cloudDeploymentModel, String logMessage)
    {
	id = Id.pollId(Request.class);
	this.submissionTime = submissionTime;
	this.budget = budget;
	this.deadline = deadline;
	this.job = job;
	this.jobFile = jobFile;
	this.userClass = userClass;
	this.mapAndReduceVmProvisionList = mapAndReduceVmProvisionList;
	this.reduceOnlyVmProvisionList = reduceOnlyVmProvisionList;
	this.schedulingPlan = schedulingPlan;
	this.experimentNumber = experimentNumber;
	this.workloadNumber = workloadNumber;
	this.cloudDeploymentModel = cloudDeploymentModel;
	this.logMessage = logMessage;
    }

    public int getId() {
	return id;
    }

    public void setId(int id) {
	this.id = id;
    }

    public double getSubmissionTime() {
	return submissionTime;
    }

    public void setSubmissionTime(double submissionTime) {
	this.submissionTime = submissionTime;
    }

    public double getBudget() {
	return budget;
    }

    public void setBudget(double budget) {
	this.budget = budget;
    }

    public double getDeadline() {
	return deadline;
    }

    public void setDeadline(double deadline) {
	this.deadline = deadline;
    }

    public Job getJob() {
	return job;
    }

    public void setJob(Job job) {
	this.job = job;
    }

    public UserClass getUserClass() {
	return userClass;
    }

    public void setUserClass(UserClass userClass) {
	this.userClass = userClass;
    }

    public List<VmInstance> getMapAndReduceVmProvisionList() {
	return mapAndReduceVmProvisionList;
    }

    public void setMapAndReduceVmProvisionList(
	    List<VmInstance> mapAndReduceVmProvisionList) {
	this.mapAndReduceVmProvisionList = mapAndReduceVmProvisionList;
    }

    public List<VmInstance> getReduceOnlyVmProvisionList() {
	return reduceOnlyVmProvisionList;
    }

    public void setReduceOnlyVmProvisionList(
	    List<VmInstance> reduceOnlyVmProvisionList) {
	this.reduceOnlyVmProvisionList = reduceOnlyVmProvisionList;
    }

    public Map<Integer, Integer> getSchedulingPlan() {
	return schedulingPlan;
    }

    public void setSchedulingPlan(Map<Integer, Integer> schedulingPlan) {
	this.schedulingPlan = schedulingPlan;
    }

    public String getPolicy() {
	return policy;
    }

    public void setPolicy(String policy) {
	this.policy = policy;
    }

    public String getJobFile() {
	return jobFile;
    }

    public void setJobFile(String jobFile) {
	this.jobFile = jobFile;
    }

    public synchronized String getLogMessage() {
	return logMessage;
    }

    public synchronized void setLogMessage(String logMessage) {
	if (this.logMessage.equals(""))
	    this.logMessage = logMessage;
	else
	    this.logMessage += " | " + logMessage;
    }

    public CloudDeploymentModel getCloudDeploymentModel() {
	return cloudDeploymentModel;
    }

    public void setCloudDeploymentModel(CloudDeploymentModel cloudDeploymentModel) {
	this.cloudDeploymentModel = cloudDeploymentModel;
    }

    public double getExecutionTime()
    {
	double firstSubmissionTime = -1;
	double lastFinishTime = -1;

	for (MapTask mapTask : job.mapTasks)
	{
	    if (firstSubmissionTime == -1 || firstSubmissionTime > mapTask.getSubmissionTime())
		firstSubmissionTime = mapTask.getSubmissionTime();
	    if (lastFinishTime == -1 || lastFinishTime < mapTask.getFinishTime())
		lastFinishTime = mapTask.getFinishTime();
	}

	for (ReduceTask reduceTask : job.reduceTasks)
	{
	    if (firstSubmissionTime == -1 || firstSubmissionTime > reduceTask.getSubmissionTime())
		firstSubmissionTime = reduceTask.getSubmissionTime();
	    if (lastFinishTime == -1 || lastFinishTime < reduceTask.getFinishTime())
		lastFinishTime = reduceTask.getFinishTime();
	}

	return lastFinishTime - firstSubmissionTime;
    }

    public double getCost()
    {
	if (mapAndReduceVmProvisionList.size() == 0 && reduceOnlyVmProvisionList.size() == 0)
	    return -1;
	double totalCost = 0.0;
	for (VmInstance vm : mapAndReduceVmProvisionList)
	    totalCost += vm.getExecutionCost();
	for (VmInstance vm : reduceOnlyVmProvisionList)
	    totalCost += vm.getExecutionCost();

	return totalCost;
    }

    public String getIsDeadlineViolated()
    {
	if (getExecutionTime() == -1)
	    return "YES! FAILED TO RUN";

	if (getExecutionTime() > deadline)
	    return "YES! (" + (getExecutionTime() - deadline) + " seconds) over deadline";
	return "No (" + (deadline - getExecutionTime()) + " seconds) earlier";
    }

    public String getIsBudgetViolated()
    {
	if (getCost() == -1)
	    return "YES! FAILED TO RUN";

	if (getCost() > budget)
	    return "YES! ($" + (getCost() - budget) + ") over budget";
	return "No ($" + (budget - getCost()) + ") savings";

    }

    public boolean getIsDeadlineViolatedBoolean()
    {
	if (getExecutionTime() == -1)
	    return true;

	if (getExecutionTime() > deadline)
	    return true;
	return false;
    }

    public boolean getIsBudgetViolatedBoolean()
    {
	if (getCost() == -1)
	    return true;

	if (getCost() > budget)
	    return true;
	return false;

    }

    public String getJ()
    {
	return "J={" + job.mapTasks.size() + "-" + job.reduceTasks.size() + "}";
    }

    private Job readJobYAML(String jobFile) {
	new Job();

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
	Task task = getTaskFromId(cloudletId, job);
	if (task == null)
	    return false;
	else
	    return true;
    }

    public VmInstance getProvisionedVmFromTaskId(int TaskId)
    {
	int vmInstanceId = -1;
	if (schedulingPlan.containsKey(TaskId))
	    vmInstanceId = schedulingPlan.get(TaskId);
	else
	    return null;

	return getProvisionedVm(vmInstanceId);
    }

    public VmInstance getProvisionedVm(int vmInstanceId)
    {
	for (VmInstance vmInstance : mapAndReduceVmProvisionList) {
	    if (vmInstance.getId() == vmInstanceId)
		return vmInstance;
	}

	for (VmInstance vmInstance : reduceOnlyVmProvisionList) {
	    if (vmInstance.getId() == vmInstanceId)
		return vmInstance;
	}

	return null;
    }

    public List<Task> getAllTasks()
    {
	List<Task> allTasks = new ArrayList<Task>();
	for (Task task : job.mapTasks)
	    allTasks.add(task);
	for (Task task : job.reduceTasks)
	    allTasks.add(task);
	return allTasks;
    }

    public int getNumberOfVMs()
    {
	return mapAndReduceVmProvisionList.size() + reduceOnlyVmProvisionList.size();
    }

    // /// STATIC METHODS ////

    public static Task getTaskFromId(int taskId, Job job) {
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

    public int getExperimentNumber()
    {
	return experimentNumber;
    }

    public void setExperimentNumber(int experimentNumber)
    {
	this.experimentNumber = experimentNumber;
    }

    public int getWorkloadNumber()
    {
	return workloadNumber;
    }

    public void setWorkloadNumber(int workloadNumber)
    {
	this.workloadNumber = workloadNumber;
    }

    public int getNumberOfTasks() {
	return job.mapTasks.size() + job.reduceTasks.size();
    }
    
    

    public Long getAlgorithRunningTime() {
        return algorithRunningTime;
    }

    public void setAlgorithRunningTime(Long algorithRunningTime) {
        this.algorithRunningTime = algorithRunningTime;
    }
    
    

    public synchronized Long getAlgoStartTime() {
        return algoStartTime;
    }

    public synchronized void setAlgoStartTime(Long algoStartTime) {
        this.algoStartTime = algoStartTime;
    }
    

    public synchronized Long getAlgoFirstSoulationFoundedTime() {
        return algoFirstSoulationFoundedTime;
    }

    public synchronized void setAlgoFirstSoulationFoundedTime(Long algoFirstSoulationFoundedTime) {
        this.algoFirstSoulationFoundedTime = algoFirstSoulationFoundedTime;
    }

    @Override
    public Request clone() {
	return new Request(submissionTime, deadline, budget, job, jobFile, userClass,
		mapAndReduceVmProvisionList, reduceOnlyVmProvisionList,
		schedulingPlan, experimentNumber, workloadNumber, cloudDeploymentModel, logMessage);
    }

}
