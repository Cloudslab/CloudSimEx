package org.cloudbus.cloudsim.ex.web.experiments;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;
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
	Class<?> experiment = MultiCloudFramework.class;

	String logProp = "../custom_log.properties";

	// Map the main experiment classes to the output files
	List<Map.Entry<? extends Class<?>, String[]>> experiments = new ArrayList<>();

	int n = 0;
	int latencySLA = 40;
	double monitoringPeriod = 0.01;
	double autoscalingPeriod = 10;

	// AutoScaling
	double autoscaleTriggerCPU = 0.70;
	double autoscaleTriggerRAM = 0.70;

	// Load balancing
	double loadbalancingThresholdCPU = 0.80;
	double loadbalancingThresholdRAM = 0.80;

	for (double wld : new Double[] { 150d, 200d, 250d}) {
	    String minRam = "-Xms" + 512 + "m";
	    String maxRam = "-Xmx" + (wld < 100 ? 1024 : 2048) + "m";

	    experiments.add(ImmutablePair.of(experiment, new String[] {
		    "-Djava.security.egd=file:/dev/./urandom",
		    minRam,
		    maxRam,
		    logProp,
		    String.valueOf(n),
		    String.valueOf(latencySLA),
		    String.valueOf(wld),
		    String.valueOf(monitoringPeriod),
		    String.valueOf(autoscalingPeriod),
		    String.valueOf(autoscaleTriggerCPU),
		    String.valueOf(autoscaleTriggerRAM),
		    String.valueOf(loadbalancingThresholdCPU),
		    String.valueOf(loadbalancingThresholdRAM),
	    }));
	}

	// Run the experiments with custom_log.properties config of the loggers
	// and leave 1 CPU free at all times.
	ExperimentsRunner.runExperiments(experiments, 1);
    }

}
