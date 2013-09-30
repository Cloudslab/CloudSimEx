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

	int n = 1;
	int latencySLA = 40;
	double monitoringPeriod = 0.01;
	double autoscalingPeriod = 10;

	// AutoScaling
	double autoscaleTriggerCPU = 0.70;
	double autoscaleTriggerRAM = 0.70;

	// Load balancing
	double loadbalancingThresholdCPU = 0.80;
	double loadbalancingThresholdRAM = 0.80;

	for (double[] wld : new double[][] { { 1d, 1024d }, { 200d, 2048d } }) {
	    String minRam = "-Xms" + 1024 + "m";
	    String maxRam = "-Xmx" + (int) wld[1] + "m";

	    experiments.add(ImmutablePair.of(experiment, new String[] {
		    "-Djava.security.egd=file:/dev/./urandom",
		    minRam,
		    maxRam,
		    logProp,
		    String.valueOf(n),
		    String.valueOf(latencySLA),
		    String.valueOf(wld[0]),
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
	ExperimentsRunner.runExperiments(experiments, -1);
    }
}
