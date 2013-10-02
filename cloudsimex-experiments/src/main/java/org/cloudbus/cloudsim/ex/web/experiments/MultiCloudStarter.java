package org.cloudbus.cloudsim.ex.web.experiments;

import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.ex.util.ExperimentDefinition;
import org.cloudbus.cloudsim.ex.util.ExperimentsRunner;

/**
 * 
 * @author nikolay.grozev
 * 
 */
public class MultiCloudStarter {

    /**
     * Run all experiments we need asynchronously.
     * 
     * @param args
     * @throws Execution
     */
    public static void main(final String[] args) throws Exception {
	Class<?> experimentClass = MultiCloudFramework.class;

	String logProp = "../custom_log.properties";
	String urandProp = "-Djava.security.egd=file:/dev/./urandom";

	// Map the main experiment classes to the output files
	List<ExperimentDefinition> experiments = new ArrayList<>();

	int n = 1;
	int latencySLA = 30;
	double monitoringPeriod = 0.01;
	double autoscalingPeriod = 10;

	// AutoScaling
	double autoscaleTriggerCPU = 0.70;
	double autoscaleTriggerRAM = 0.70;

	// Load balancing
	double loadbalancingThresholdCPU = 0.80;
	double loadbalancingThresholdRAM = 0.80;

	experiments.add(new ExperimentDefinition(
		experimentClass,
		(int)(2.5 * ExperimentDefinition.GIGABYTE_IN_MEGA),
		(int)(1 * ExperimentDefinition.GIGABYTE_IN_MEGA),
		urandProp,
		logProp,
		String.valueOf(n),
		String.valueOf(latencySLA),
		String.valueOf(200),
		String.valueOf(monitoringPeriod),
		String.valueOf(autoscalingPeriod),
		String.valueOf(autoscaleTriggerCPU),
		String.valueOf(autoscaleTriggerRAM),
		String.valueOf(loadbalancingThresholdCPU),
		String.valueOf(loadbalancingThresholdRAM),
		String.valueOf(15),
		String.valueOf(Boolean.TRUE)
		));

	experiments.add(new ExperimentDefinition(
		experimentClass,
		(int)(2.5 * ExperimentDefinition.GIGABYTE_IN_MEGA),
		(int)(1 * ExperimentDefinition.GIGABYTE_IN_MEGA),
		urandProp,
		logProp,
		String.valueOf(n),
		String.valueOf(latencySLA),
		String.valueOf(200),
		String.valueOf(monitoringPeriod),
		String.valueOf(autoscalingPeriod),
		String.valueOf(autoscaleTriggerCPU),
		String.valueOf(autoscaleTriggerRAM),
		String.valueOf(loadbalancingThresholdCPU),
		String.valueOf(loadbalancingThresholdRAM),
		String.valueOf(15),
		String.valueOf(Boolean.FALSE)
		));

	// Run the experiments with custom_log.properties config of the loggers
	// and leave 1 CPU free at all times.
	ExperimentsRunner.runExperiments(experiments, -1);
    }
}
