package org.cloudbus.cloudsim.incubator.web.experiments;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.cloudbus.cloudsim.incubator.util.ExperimentsRunner;

/**
 * Runs a bunch of experiments.
 * 
 * @author nikolay.grozev
 * 
 */
public class Experiments {

    /**
     * @param args
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws IOException
     * @throws InvocationTargetException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws SecurityException
     * @throws NoSuchMethodException
     */
    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException,
	    NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException,
	    InvocationTargetException {
	Map<Class<?>, String> experiments = new LinkedHashMap<>();

	Class<?>[] experimens = new Class<?>[] {
		BaselineExperiment.class,
		ExperimentWithHeavierAS.class,
		ExperimentWithHeavierDB.class,
		ExperimentWithHeavierASAndMoreAS.class,
		ExperimentWithHeavierDBAndMoreDB.class,
	};

	for (Class<?> klass : experimens) {
	    experiments.put(klass, String.format("results/%s.log", klass.getSimpleName()));
	}

	//cp -f * "/home/nikolay/Important Data/papers/Formal_Model/RStat/"
	ExperimentsRunner.runExperiments(experiments, "custom_log.properties");
    }

}
