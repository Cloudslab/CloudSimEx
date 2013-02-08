package org.cloudbus.cloudsim.ex.web.experiments;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.cloudbus.cloudsim.ex.disk.HddVm;
import org.cloudbus.cloudsim.ex.web.WebBroker;

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
    public static void main(String[] args) throws IOException {
	// Step 0: Set up the logger
	parseExperimentParameters(args);

	new ExperimentWithHeavierASAndMoreAS(2 * DAY, 4, "[Heavy AS Experiment]").runExperimemt();
    }

    public ExperimentWithHeavierASAndMoreAS(int simulationLength, int refreshTime, String experimentName) {
	super(simulationLength, refreshTime, experimentName);
    }

    @Override
    protected List<HddVm> createApplicationServerVMS(WebBroker brokerDC1) {
        return Arrays.asList(createVM(brokerDC1.getId()), createVM(brokerDC1.getId()), createVM(brokerDC1.getId()));
    }

}
