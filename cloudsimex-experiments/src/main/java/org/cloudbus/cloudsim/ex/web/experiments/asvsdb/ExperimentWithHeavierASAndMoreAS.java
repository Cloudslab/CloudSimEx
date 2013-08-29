package org.cloudbus.cloudsim.ex.web.experiments.asvsdb;

import static org.cloudbus.cloudsim.Consts.*;
import static org.cloudbus.cloudsim.ex.web.experiments.ExperimentsUtil.parseExperimentParameters;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.cloudbus.cloudsim.ex.DatacenterBrokerEX;
import org.cloudbus.cloudsim.ex.disk.HddVm;

/**
 * 
 * @author nikolay.grozev
 * 
 */
public class ExperimentWithHeavierASAndMoreAS extends ExperimentWithHeavierAS {

    /**
     * @param args
     * @throws IOException
     */
    public static void main(final String[] args) throws IOException {
	// Step 0: Set up the logger
	parseExperimentParameters(args);

	new ExperimentWithHeavierASAndMoreAS(2 * DAY, 4, "[Heavy AS Experiment]").runExperimemt();
    }

    public ExperimentWithHeavierASAndMoreAS(final int simulationLength, final int refreshTime,
	    final String experimentName) {
	super(simulationLength, refreshTime, experimentName);
    }

    @Override
    protected List<HddVm> createApplicationServerVMS(final DatacenterBrokerEX brokerDC1) {
	return Arrays.asList(createVM(brokerDC1.getId()), createVM(brokerDC1.getId()), createVM(brokerDC1.getId()));
    }

}
