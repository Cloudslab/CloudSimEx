package org.cloudbus.cloudsim.ex.web.experiments;

import java.util.LinkedHashMap;
import java.util.Map;

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
	Class<?>[] experimens = new Class<?>[] { TwoDatacentres_MultiCloudFramework.class };

	// Map the main experiment classes to the output files
	Map<Class<?>, String> experiments = new LinkedHashMap<>();
	for (Class<?> clazz : experimens) {
	    experiments.put(clazz,
		    TwoDatacentres_MultiCloudFramework.RESULT_DIR + String.format("%s.log", clazz.getSimpleName()));
	    
	}

	// Run the experiments with custom_log.properties config of the loggers
	// and leave 1 CPU free at all times.
	ExperimentsRunner.runExperiments(experiments, "../custom_log.properties", 1);
    }

}
