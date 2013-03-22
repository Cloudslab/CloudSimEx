package org.cloudbus.cloudsim.ex.web.experiments.cases;

import java.io.IOException;

import org.cloudbus.cloudsim.ex.web.experiments.ExperimentsUtil;
import org.cloudbus.cloudsim.ex.web.experiments.SingleDatacentre;

public class SingleDataCentre150 extends SingleDatacentre {

    public SingleDataCentre150() {
	numOfSessions = 150;
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(final String[] args) throws IOException {
	ExperimentsUtil.parseExperimentParameters(args);
	new SingleDataCentre150().runExperimemt();
    }

}
