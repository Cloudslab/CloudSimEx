package org.cloudbus.cloudsim.web;

import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsimgoodies.util.CustomLog;

public class WebBroker extends DatacenterBroker {

	private static final int TIMER_TAG = 123456;

	private boolean isTimerRunning = false;
	private final double refreshPeriod;
	private final double lifeLength;

	public WebBroker(final String name, final double refreshPeriod, final double lifeLength) throws Exception {
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

	@Override
	protected void processOtherEvent(SimEvent ev) {
		switch (ev.getTag()) {
		case TIMER_TAG:
			CustomLog.printLine("Event: " + getName() + "Time: " + CloudSim.clock(), null);
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
		
	}

}
