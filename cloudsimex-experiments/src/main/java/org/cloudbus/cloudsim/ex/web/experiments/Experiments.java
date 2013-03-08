package org.cloudbus.cloudsim.ex.web.experiments;

import java.util.LinkedHashMap;
import java.util.Map;

import org.cloudbus.cloudsim.ex.util.ExperimentsRunner;
import org.cloudbus.cloudsim.ex.web.experiments.cases.SingleDataCentre100;
import org.cloudbus.cloudsim.ex.web.experiments.cases.SingleDataCentre200;
import org.cloudbus.cloudsim.ex.web.experiments.cases.SingleDataCentre250;
import org.cloudbus.cloudsim.ex.web.experiments.cases.SingleDataCentre300;
import org.cloudbus.cloudsim.ex.web.experiments.cases.SingleDataCentre400;
import org.cloudbus.cloudsim.ex.web.experiments.cases.SingleDataCentre500;
import org.cloudbus.cloudsim.ex.web.experiments.cases.SingleDataCentre600;
import org.cloudbus.cloudsim.ex.web.experiments.cases.SingleDataCentre700;

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
		SingleDataCentre200.class,
		SingleDataCentre250.class,
		SingleDataCentre300.class,
		SingleDataCentre400.class,
		SingleDataCentre500.class,
		SingleDataCentre600.class,
		SingleDataCentre700.class,
		// SingleDataCentre1000.class
	};

	// Map the main experiment classes to the output files
	Map<Class<?>, String> experiments = new LinkedHashMap<>();
	for (Class<?> clazz : experimens) {
	    experiments.put(clazz,
		    String.format("%s.log", clazz.getSimpleName()));
	}

	// Run the experiments with custom_log.properties config of the loggers
	// and leave 1 CPU free at all times.
	ExperimentsRunner.runExperiments(experiments,
		"../custom_log.properties", 1);
    }

}
