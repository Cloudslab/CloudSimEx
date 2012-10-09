package org.cloudbus.cloudsim.web;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsimgoodies.util.CustomLog;

public class WebBroker extends DatacenterBroker {

	private static final int TIMER_TAG = 123456;

	private boolean isTimerRunning = false;
	private final double refreshPeriod;
	private final double lifeLength;

	private List<WebSession> sessions = new ArrayList<>();

	public WebBroker(final String name, final double refreshPeriod,
			final double lifeLength) throws Exception {
		super(name);
		this.refreshPeriod = refreshPeriod;
		this.lifeLength = lifeLength;
	}

	@Override
	public void processEvent(SimEvent ev) {
		if (!isTimerRunning) {
			isTimerRunning = true;
			sendNow(getId(), TIMER_TAG);
		}

		super.processEvent(ev);
	}

	public void submitSessions(final List<WebSession> webSessions) {
		sessions.addAll(webSessions);
	}
	
	@Override
	protected void processOtherEvent(SimEvent ev) {
		switch (ev.getTag()) {
		case TIMER_TAG:
			CustomLog.printLine(
					"Event: " + getName() + "Time: " + CloudSim.clock(), null);
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
		for (WebSession sess : sessions) {
			double currTime = CloudSim.clock();

			sess.prefetch(currTime);
			WebCloudlet[] webCloudlets = sess.pollCloudlets(currTime);
			if (webCloudlets != null) {
//				WebCloudlet asCloudLet = webCloudlets[0];
//				WebCloudlet dbCloudLet = webCloudlets[1];
				submitCloudletList(Arrays.asList(webCloudlets));
			}
		}

	}

}
