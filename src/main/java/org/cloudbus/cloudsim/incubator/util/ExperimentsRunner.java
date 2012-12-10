package org.cloudbus.cloudsim.incubator.util;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * 
 * A utility that runs a set of experiments in different JVM processes. Since
 * CloudSim makes heavy use of static data, experiments can not be run in
 * multiple threads within the same JVM. With this utility class one can spawn
 * multiple independent JVM process, redirect their standard outputs to a single
 * place and synchronize with their ends.
 * 
 * <br>
 * <br>
 * Each experiment is specified by a class with a main method and an output
 * file, where the output from the CustomLog is stored. Each class's main method
 * should take two parameters - the output file and the config file for the
 * logger. It is responsibility of the implementers of these classes to parse
 * and use these parameters.
 * 
 * @author nikolay.grozev
 * 
 */
public class ExperimentsRunner {

    private static final List<Process> PROCESSES = new ArrayList<>();
    private static Thread shutdownHook = null;

    /**
     * Runs a set of experiments in separated processes. If only one experiment
     * is provided - it is run in the current process. Allows users to specify
     * how many processors to remain idle, even if they are needed for the
     * faster execution of the experiment.
     * 
     * @param experimentsToOutputs
     *            - the experiments's main classes mapped to the output files
     *            they'll be using.
     * @param logPropertiesFile
     *            - the properties for the loggers of the experiments.
     * @param numFreeCPUs
     *            - number of processors to leave unused. Must be non-negative
     *            and less than the number processors - 1. For example if 0 -
     *            all processors/cores can be used if required. If 1 - then 1
     *            CPU will be free at all times.
     * 
     * @throws Exception
     */
    public static synchronized void runExperiments(final Map<Class<?>, String> experimentsToOutputs,
	    final String logPropertiesFile, final int numFreeCPUs) throws Exception {

	// If only one experiment - run it in this process
	// If more than on experiment - spawn a new process for each
	if (experimentsToOutputs.size() == 1) {
	    Class<?> experiment = (Class<?>) experimentsToOutputs.keySet().toArray()[0];
	    String file = experimentsToOutputs.get(experiment);

	    // Find the main method and run it here
	    Method main = experiment.getMethod("main", String[].class);
	    String[] params = new String[] { file, logPropertiesFile };
	    main.invoke(null, (Object) params);
	} else if (!experimentsToOutputs.isEmpty()) {
	    // Prints the pid of the current process... so we know who to kill
	    printPIDInformation();

	    // If this process dies - kill the spawn subprocesses.
	    addHookToKillProcesses();

	    int cores = Runtime.getRuntime().availableProcessors();
	    // If possible leave one core free
	    int coresToUse = cores == 1 ? 1 : cores - numFreeCPUs;

	    ExecutorService pool = Executors.newFixedThreadPool(coresToUse);
	    Collection<Future<?>> futures = new ArrayList<Future<?>>();

	    for (Map.Entry<Class<?>, String> entry : experimentsToOutputs.entrySet()) {
		final Class<?> experiment = entry.getKey();
		final String file = entry.getValue();
		Runnable runnable = new Runnable() {
		    @Override
		    public void run() {
			int resultStatus;
			try {
			    resultStatus = exec(experiment, file, logPropertiesFile);
			} catch (IOException | InterruptedException e) {
			    resultStatus = 1;
			}
			if (resultStatus != 0) {
			    System.err.println("!!! Experiment " + experiment.getCanonicalName() + " has failed!!!");
			}
		    };
		};
		futures.add(pool.submit(runnable));
	    }

	    // Wait until all are finished
	    for (Future<?> future : futures) {
		future.get();
	    }

	    pool.shutdown();
	}
	System.err.println();
	System.err.println("All experiments are finished");
    }

    private synchronized static void addHookToKillProcesses() {
	if (shutdownHook == null) {
	    shutdownHook = new Thread() {
		@Override
		public void run() {
		    System.err.println("Killing subprocesses...");
		    for (Process p : PROCESSES) {
			p.destroy();
		    }
		    System.err.println("All subprocesses are killed. Shutting down.");
		}
	    };
	    Runtime.getRuntime().addShutdownHook(shutdownHook);
	}
    }

    /**
     * Inspired by
     * http://stackoverflow.com/questions/636367/executing-a-java-application
     * -in-a-separate-process
     * 
     * @param klass
     * @param logPropertiesFile
     * @param logFile
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    private static int exec(Class<?> klass, String logFile, String logPropertiesFile) throws IOException,
	    InterruptedException {
	String javaHome = System.getProperty("java.home");
	String javaBin = javaHome +
		File.separator + "bin" +
		File.separator + "java";
	String classpath = System.getProperty("java.class.path");
	String className = klass.getCanonicalName();

	ProcessBuilder builder = new ProcessBuilder(
		javaBin, "-cp", classpath, className, logFile, logPropertiesFile);

	// Redirect the standard I/O to here (this process)
	builder.inheritIO();

	// Start the process
	Process process = builder.start();

	// Keep a reference to the process, so that it can be killed
	PROCESSES.add(process);

	// Wait until the process is done.
	process.waitFor();

	// Return the status of the process
	return process.exitValue();
    }

    private static void printPIDInformation() throws IOException {
	byte[] bo = new byte[100];
	String[] cmd = { "bash", "-c", "echo $PPID" };
	Process p = Runtime.getRuntime().exec(cmd);
	p.getInputStream().read(bo);

	String pid = new String(bo).trim();
	System.err.println("Main process Id (PID) is: " + pid + ". Use: ");
	System.err.println("\tkill -SIGINT " + pid);
	System.err.println("to kill all experiments");
	System.err.println();
    }
}
