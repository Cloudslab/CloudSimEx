package org.cloudbus.cloudsim.ex.web.experiments;

import java.io.IOException;

public class SingleDataCentre100 extends SingleDatacentre {

    public SingleDataCentre100() {
	numOfSessions = 100;
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(final String[] args) throws IOException {
	ExperimentsUtil.parseExperimentParameters(args);
	new SingleDataCentre100().runExperimemt();
    }

}
