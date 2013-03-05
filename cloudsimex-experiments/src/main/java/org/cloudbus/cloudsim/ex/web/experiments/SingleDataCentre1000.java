package org.cloudbus.cloudsim.ex.web.experiments;

import java.io.IOException;

public class SingleDataCentre1000 extends SingleDatacentre {

    public SingleDataCentre1000() {
	numOfSessions = 1000;
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(final String[] args) throws IOException {
	ExperimentsUtil.parseExperimentParameters(args);
	new SingleDataCentre1000().runExperimemt();
    }

}
