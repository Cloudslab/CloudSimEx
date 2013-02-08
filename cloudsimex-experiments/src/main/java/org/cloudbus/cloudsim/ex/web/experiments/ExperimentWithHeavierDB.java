package org.cloudbus.cloudsim.ex.web.experiments;

import java.io.IOException;
import java.util.List;

import org.cloudbus.cloudsim.ex.web.workload.WorkloadGenerator;

/**
 * 
 * @author nikolay.grozev
 * 
 */
public class ExperimentWithHeavierDB extends BaseExperiment {

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
	// Step 0: Set up the logger
	parseExperimentParameters(args);

	new ExperimentWithHeavierDB(2 * DAY, 4, "[Heavy DB Experiment]").runExperimemt();
    }

    public ExperimentWithHeavierDB(int simulationLength, int refreshTime, String experimentName) {
	super(simulationLength, refreshTime, experimentName);
    }

    @Override
    protected List<WorkloadGenerator> generateWorkload(double nullPoint, String[] periods) {
	int asCloudletLength = 30;
	int asRam = 1;
	int dbCloudletLength = 30;
	int dbRam = 1;
	int dbCloudletIOLength = 70;
	int duration = 200;

	return generateWorkload(nullPoint, periods, asCloudletLength, asRam, dbCloudletLength, dbRam,
		dbCloudletIOLength, duration);
    }

}
