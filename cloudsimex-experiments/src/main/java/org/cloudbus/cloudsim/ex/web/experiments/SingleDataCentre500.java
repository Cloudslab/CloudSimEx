package org.cloudbus.cloudsim.ex.web.experiments;

import java.io.IOException;

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
