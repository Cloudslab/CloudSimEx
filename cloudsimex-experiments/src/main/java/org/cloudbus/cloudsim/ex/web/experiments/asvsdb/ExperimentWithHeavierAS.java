package org.cloudbus.cloudsim.ex.web.experiments.asvsdb;

import static org.cloudbus.cloudsim.Consts.*;
import static org.cloudbus.cloudsim.ex.web.experiments.ExperimentsUtil.parseExperimentParameters;

import java.io.IOException;
import java.util.List;

import org.cloudbus.cloudsim.ex.web.workload.StatWorkloadGenerator;

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
    public static void main(final String[] args) throws IOException {
	// Step 0: Set up the logger
	parseExperimentParameters(args);

	new ExperimentWithHeavierAS(2 * DAY, 4, "[Heavy AS Experiment]").runExperimemt();
    }

    public ExperimentWithHeavierAS(final int simulationLength, final int refreshTime, final String experimentName) {
	super(simulationLength, refreshTime, experimentName);
    }

    @Override
    protected List<StatWorkloadGenerator> generateWorkload(final double nullPoint, final String[] periods) {
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
