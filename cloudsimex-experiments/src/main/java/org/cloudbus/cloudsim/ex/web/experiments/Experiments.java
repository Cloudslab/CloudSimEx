package org.cloudbus.cloudsim.ex.web.experiments;

import java.util.LinkedHashMap;
import java.util.Map;

import org.cloudbus.cloudsim.ex.util.ExperimentsRunner;

/**
 * Runs the experiments for the
 * "Modelling and Simulation of Three-Tier Applications in a Multi-Cloud Environment"
 * paper.
 * 
 * @author nikolay.grozev
 * 
 */
public class Experiments {

    /**
     * Run all experiments we need asynchronously.
     * 
     * @param args
     * @throws Execution
     */
    public static void main(final String[] args) throws Exception {
	// The main classes of the experiments
	Class<?>[] experimens = new Class<?>[] {
		SingleDataCentre100.class,
		// SingleDataCentre250.class,
		// SingleDataCentre500.class,
		// SingleDataCentre1000.class
	};

	// Map the main experiment classes to the output files
	Map<Class<?>, String> experiments = new LinkedHashMap<>();
	for (Class<?> clazz : experimens) {
	    experiments.put(clazz, String.format("%s.log", clazz.getSimpleName()));
	}

	// Run the experiments with custom_log.properties config of the loggers
	// and leave 1 CPU free at all times.
	ExperimentsRunner.runExperiments(experiments, "../custom_log.properties", 1);
    }

}
