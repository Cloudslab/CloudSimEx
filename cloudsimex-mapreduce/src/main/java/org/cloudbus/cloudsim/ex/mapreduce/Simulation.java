package org.cloudbus.cloudsim.ex.mapreduce;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Calendar;
import java.util.Map;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmInstance;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Requests;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Task;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.UserClass;
import org.cloudbus.cloudsim.ex.util.CustomLog;

/**
 * This class contains the main method for execution of the simulation. Here,
 * simulation parameters are defined. Parameters that are dynamic are read from
 * the properties file, whereas other parameters are hardcoded.
 * 
 * Decision on what should be configurable and what is hardcoded was somehow
 * arbitrary. Therefore, if you think that some of the hardcoded values should
 * be customizable, it can be added as a Property. In the Property code there is
 * comments on how to add new properties to the experiment.
 * 
 */
public class Simulation {

    // From Cloud.yaml
    private static Cloud cloud;

    // From Requests.yaml
    private static Requests requests;

    private static MapReduceEngine engine;

    private static java.util.Properties props = new java.util.Properties();

    /**
     * Prints input parameters and execute the simulation a number of times, as
     * defined in the configuration.
     * 
     * @throws Exception
     * 
     */
    public static void main(String[] args) throws Exception {

	Log.printLine("========== Simulation configuration ==========");
	for (Properties property : Properties.values()) {
	    Log.printLine("= " + property + ": " + property.getProperty());
	}
	Log.printLine("==============================================");
	Log.printLine("");

	String[] experimentFilesName = Properties.EXPERIMENT.getProperty().split(",");
	if (experimentFilesName.length == 0)
	    experimentFilesName = new String[] { Properties.EXPERIMENT.getProperty() };

	// default.log logging
	try (InputStream is = Files.newInputStream(Paths.get("custom_log.properties"))) {
	    props.load(is);
	}
	CustomLog.configLogger(props);

	// vms.csv logging
	CustomLog.redirectToFile("output/logs/vms.csv");
	CustomLog.printHeader(VmInstance.class, ",",
		new String[] { "ExperimentNumber", "WorkloadNumber", "RequestId", "J", "UserClass",
			"Policy", "CloudDeploymentModel", "Id", "Name", "ExecutionTime", "ExecutionCost",
			"TasksIdAsString" });
	// tasks.csv logging
	CustomLog.redirectToFile("output/logs/tasks.csv");
	CustomLog.printHeader(Task.class, ",", new String[] { "ExperimentNumber", "WorkloadNumber", "RequestId", "J",
		"UserClass",
		"Policy", "CloudDeploymentModel", "CloudletId", "TaskType", "CloudletLength",
		"CloudletStatusString", "SubmissionTime", "ExecStartTime", "FinalExecTime", "FinishTime",
		"InstanceVmId", "VmType" });
	// requests.csv logging
	CustomLog.redirectToFile("output/logs/requests.csv");
	CustomLog.printHeader(Request.class, ",", new String[] { "ExperimentNumber", "WorkloadNumber", "Id", "J",
		"UserClass", "Policy",
		"CloudDeploymentModel", "Deadline", "Budget", "ExecutionTime", "Cost", "IsDeadlineViolated",
		"IsBudgetViolated", "NumberOfVMs", "LogMessage" });

	for (int experimentNumber = 0; experimentNumber < experimentFilesName.length; experimentNumber++)
	{
	    String experimentFileName = "experiments/" + experimentFilesName[experimentNumber];
	    Experiment experiment = YamlFile.getRequestsFromYaml(experimentFileName);
	    Experiment.currentExperimentName = experimentFilesName[experimentNumber].split(".yaml")[0];

	    // Experiments Plotting
	    Experiment.logExperimentsHeader(experiment.workloads.get(0).requests);

	    for (int workloadNumber = 0; workloadNumber < experiment.workloads.size(); workloadNumber++) {
		// BACK TO DEFAULT LOG FILE
		CustomLog.redirectToFile(props.getProperty("FilePath"), true);
		CustomLog.printLine("[[[[[[[ Experiment Number: " + (experimentNumber + 1) + " | Workload Number: "
			+ (workloadNumber + 1) + " ]]]]]]]");
		runSimulationRound(experimentNumber, experimentFileName, workloadNumber,
			experiment.workloads.get(workloadNumber).userClassAllowedPercentage);
		CustomLog.closeAndRemoveHandlers();
	    }
	}
    }

    /**
     * One round of the simulation is executed by this method. Output is printed
     * to the log.
     * 
     */
    private static void runSimulationRound(int experimentNumber, String experimentFileName, int workloadNumber,
	    Map<UserClass, Double> userClassAllowedPercentage) {
	Log.printLine("Starting simulation for experiment number: " + (experimentNumber + 1) + " and workload number: "
		+ (workloadNumber + 1));

	try {

	    // Initialize the CloudSim library
	    CloudSim.init(1, Calendar.getInstance(), false);

	    // Create Broker
	    engine = new MapReduceEngine();
	    engine.currentWorkloadNumber = workloadNumber + 1;
	    engine.currentExperimentNumber = experimentNumber + 1;
	    Cloud.brokerID = engine.getId();

	    // Create datacentres and cloudlets
	    cloud = YamlFile.getCloudFromYaml("inputs/" + Properties.CLOUD.getProperty());
	    cloud.setUserClassAllowedPercentage(userClassAllowedPercentage);
	    engine.setCloud(cloud);
	    Experiment Experiments = YamlFile.getRequestsFromYaml(experimentFileName);

	    requests = Experiments.workloads.get(workloadNumber).requests;

	    int preExperimentIndex = workloadNumber - 1;
	    while (requests.requests.size() == 0 && preExperimentIndex >= 0)
	    {
		requests = Experiments.workloads.get(preExperimentIndex).requests;
		for (Request request : requests.requests)
		{
		    request.policy = Experiments.workloads.get(workloadNumber).policy;
		    request.setCloudDeploymentModel(Experiments.workloads.get(workloadNumber).cloudDeploymentModel);
		}
		preExperimentIndex--;
	    }
	    engine.setRequests(requests);

	    // START
	    CloudSim.startSimulation();
	    engine.logExecutionSummary();

	    Log.printLine("");
	    Log.printLine("");
	} catch (Exception e) {
	    Log.printLine("Unwanted errors happen.");
	    e.printStackTrace();
	} finally {
	    CloudSim.stopSimulation();
	}
    }
}
