package org.cloudbus.cloudsim.ex.web.experiments.cases;

import java.io.IOException;

import org.cloudbus.cloudsim.ex.web.experiments.ExperimentsUtil;
import org.cloudbus.cloudsim.ex.web.experiments.SingleDatacentre;

public class SingleDataCentre500 extends SingleDatacentre {

    public SingleDataCentre500() {
	numOfSessions = 500;
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(final String[] args) throws IOException {
	ExperimentsUtil.parseExperimentParameters(args);
	new SingleDataCentre500().runExperimemt();
    }

}
