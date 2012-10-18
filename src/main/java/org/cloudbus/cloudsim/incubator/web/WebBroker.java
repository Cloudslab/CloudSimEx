package org.cloudbus.cloudsim.incubator.web;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;

/**
 * A broker that takes care of the submission of web sessions to the data
 * centers it handles. The broker submits the cloudlets of the provided web
 * sessions continously over a specified period. Consequently clients must
 * specify the endpoint (in terms of time) of the simulation.
 * 
 * @author nikolay.grozev
 * 
 */
public class WebBroker extends DatacenterBroker implements IWebBroker {

    private static final int TIMER_TAG = 123456;

    private boolean isTimerRunning = false;
    private final double refreshPeriod;
    private final double lifeLength;

    private List<WebSession> sessions = new ArrayList<>();

    /**
     * Creates a new web broker.
     * 
     * @param name
     *            - the name of the broker.
     * @param refreshPeriod
     *            - the period of polling web sessions for new cloudlets.
     * @param lifeLength
     *            - the length of the simulation.
     * @throws Exception
     *             - if something goes wrong. See the documentation of the super
     *             class.
     */
    public WebBroker(final String name, final double refreshPeriod,
	    final double lifeLength) throws Exception {
	super(name);
	this.refreshPeriod = refreshPeriod;
	this.lifeLength = lifeLength;
    }

    /*
     * (non-Javadoc)
     * @see org.cloudbus.cloudsim.DatacenterBroker#processEvent(org.cloudbus.cloudsim.core.SimEvent)
     */
    @Override
    public void processEvent(SimEvent ev) {
	if (!isTimerRunning) {
	    isTimerRunning = true;
	    sendNow(getId(), TIMER_TAG);
	}

	super.processEvent(ev);
    }

    /**
     * Submits new web sessions to this broker.
     * @param webSessions - the new web sessions.
     */
    public void submitSessions(final List<WebSession> webSessions) {
	sessions.addAll(webSessions);
    }

    /**
     * (non-Javadoc)
     * @see org.cloudbus.cloudsim.DatacenterBroker#processOtherEvent(org.cloudbus.cloudsim.core.SimEvent)
     */
    @Override
    protected void processOtherEvent(SimEvent ev) {
	switch (ev.getTag()) {
	    case TIMER_TAG:
		if (CloudSim.clock() < lifeLength) {
		    send(getId(), refreshPeriod, TIMER_TAG);
		    updateSessions();
		}
		break;

	    default:
		super.processOtherEvent(ev);
	}
    }

    private void updateSessions() {
	// CustomLog.print("Updating sessions", null);
	for (WebSession sess : sessions) {
	    double currTime = CloudSim.clock();

	    sess.notifyOfTime(currTime);
	    WebCloudlet[] webCloudlets = sess.pollCloudlets(currTime);

	    if (webCloudlets != null) {
		getCloudletList().addAll(Arrays.asList(webCloudlets));
		submitCloudlets();
	    }
	}
    }

    /*
     * (non-Javadoc)
     * @see org.cloudbus.cloudsim.DatacenterBroker#processCloudletReturn(org.cloudbus.cloudsim.core.SimEvent)
     */
    @Override
    protected void processCloudletReturn(SimEvent ev) {
	Cloudlet cloudlet = (Cloudlet) ev.getData();
	if (CloudSim.clock() >= lifeLength) {
	    // kill the broker only if it's life is over
	    super.processCloudletReturn(ev);
	} else {
	    getCloudletReceivedList().add(cloudlet);
	    cloudletsSubmitted--;
	}

	//CustomLog.printLine(TextUtil.getTxtLine(cloudlet));
    }

}
