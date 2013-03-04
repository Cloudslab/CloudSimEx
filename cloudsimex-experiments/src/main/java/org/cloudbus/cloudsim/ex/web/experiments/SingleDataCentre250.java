package org.cloudbus.cloudsim.ex.web.experiments;

import java.io.IOException;

public class SingleDataCentre250 extends SingleDatacentre {

    public SingleDataCentre250() {
	numOfSessions = 250;
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(final String[] args) throws IOException {
	ExperimentsUtil.parseExperimentParameters(args);
	new SingleDataCentre250().runExperimemt();
    }

}
