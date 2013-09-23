package org.cloudbus.cloudsim.ex.web.experiments;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;
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
	// Class<?>[] experimens = new Class<?>[] {
	// // SingleDataCentre10.class,
	// // SingleDataCentre25.class,
	// SingleDataCentre50.class,
	// // SingleDataCentre75.class,
	// SingleDataCentre100.class,
	// // SingleDataCentre125.class,
	// // SingleDataCentre150.class,
	// // SingleDataCentre175.class,
	// SingleDataCentre200.class,
	// SingleDataCentre250.class,
	// SingleDataCentre300.class,
	// SingleDataCentre400.class,
	// SingleDataCentre500.class,
	// SingleDataCentre600.class,
	// SingleDataCentre700.class,
	// // SingleDataCentre1000.class
	// };

	Class<?>[] experiments = new Class<?>[] { TwoDatacentres.class };

	// Map the main experiment classes to the output files
	List<Map.Entry<? extends Class<?>, String[]>> experimentsDefs = new ArrayList<>();

	for (Class<?> clazz : experiments) {
	    experimentsDefs.add(ImmutablePair.of(clazz,
		    new String[] { SingleDatacentre.RESULT_DIR + String.format("%s.log", clazz.getSimpleName(), "../custom_log.properties") }
	    ));
	}

	// Run the experiments with custom_log.properties config of the loggers
	// and leave 1 CPU free at all times.
	ExperimentsRunner.runExperiments(experimentsDefs, 1);
    }

}
