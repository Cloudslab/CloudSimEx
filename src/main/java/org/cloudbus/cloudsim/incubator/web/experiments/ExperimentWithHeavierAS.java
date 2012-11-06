package org.cloudbus.cloudsim.incubator.web.experiments;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.cloudbus.cloudsim.incubator.web.workload.WorkloadGenerator;
import org.cloudbus.cloudsim.incubator.web.workload.freq.CompositeValuedSet;
import org.cloudbus.cloudsim.incubator.web.workload.freq.FrequencyFunction;
import org.cloudbus.cloudsim.incubator.web.workload.freq.PeriodicStochasticFrequencyFunction;
import org.cloudbus.cloudsim.incubator.web.workload.sessions.ConstSessionGenerator;
import org.cloudbus.cloudsim.incubator.web.workload.sessions.ISessionGenerator;

/**
 * 
 * @author nikolay.grozev
 * 
 */
public class ExperimentWithHeavierAS extends BaselineExperiment {

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
	// Step 0: Set up the logger
	parseExperimentParameters(args);

	new ExperimentWithHeavierAS(2 * DAY, 4, "[Heavy AS Experiment]").runExperimemt();
    }

    public ExperimentWithHeavierAS(int simulationLength, int refreshTime, String experimentName) {
	super(simulationLength, refreshTime, experimentName);
    }

    protected List<WorkloadGenerator> generateWorkload(double nullPoint, String[] periods) {
	int asCloudletLength = 300;
	int asRam = 1;
	int dbCloudletLength = 50;
	int dbRam = 1;
	int dbCloudletIOLength = 50;
	int duration = 200;

	int numberOfCloudlets = duration / refreshTime;
	numberOfCloudlets = numberOfCloudlets == 0 ? 1 : numberOfCloudlets;

	ISessionGenerator sessGen = new ConstSessionGenerator(asCloudletLength, asRam, dbCloudletLength,
		dbRam, dbCloudletIOLength, duration, numberOfCloudlets);

	double unit = HOUR;
	double periodLength = DAY;

	FrequencyFunction freqFun = new PeriodicStochasticFrequencyFunction(unit, periodLength, nullPoint,
		CompositeValuedSet.createCompositeValuedSet(periods));
	return Arrays.asList(new WorkloadGenerator(freqFun, sessGen));
    }

}
