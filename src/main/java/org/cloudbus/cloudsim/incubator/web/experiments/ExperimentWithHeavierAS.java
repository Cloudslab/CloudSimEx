package org.cloudbus.cloudsim.incubator.web.experiments;

import java.io.IOException;
import java.util.List;

import org.cloudbus.cloudsim.incubator.web.workload.WorkloadGenerator;

/**
 * 
 * @author nikolay.grozev
 * 
 */
public class ExperimentWithHeavierAS extends BaseExperiment {

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

    @Override
    protected List<WorkloadGenerator> generateWorkload(double nullPoint, String[] periods) {
	int asCloudletLength = 150;
	int asRam = 1;
	int dbCloudletLength = 50;
	int dbRam = 1;
	int dbCloudletIOLength = 50;
	int duration = 200;

	return generateWorkload(nullPoint, periods, asCloudletLength, asRam, dbCloudletLength, dbRam,
		dbCloudletIOLength, duration);
    }

}
