package org.cloudbus.cloudsim.web;

import java.util.LinkedList;
import java.util.Queue;

public class WebSession {

	private Queue<WebCloudLet> appServerCloudLets = new LinkedList<WebCloudLet>();
	private Queue<WebCloudLet> dbServerCloudLets = new LinkedList<WebCloudLet>();

	private WebCloudLet currentAppServerCloudLet = null;
	private WebCloudLet currentDBServerCloudLet = null;

	public WebCloudLet[] pollCloudlets(final double currTime) {

		WebCloudLet[] result = null;
		boolean appCloudletFinished = currentAppServerCloudLet == null
				|| currentAppServerCloudLet.isFinished();
		boolean dbCloudletFinished = currentDBServerCloudLet == null
				|| currentDBServerCloudLet.isFinished();
		boolean appServerNextReady = !appServerCloudLets.isEmpty()
				&& appServerCloudLets.peek().getIdealStartTime() >= currTime;
		boolean dbServerNextReady = !dbServerCloudLets.isEmpty()
				&& dbServerCloudLets.peek().getIdealStartTime() >= currTime;

		if (appCloudletFinished && dbCloudletFinished && appServerNextReady
				&& dbServerNextReady) {
			result = new WebCloudLet[] { appServerCloudLets.poll(),
					dbServerCloudLets.poll() };
			currentAppServerCloudLet = result[0];
			currentDBServerCloudLet = result[0];
		}
		return result;
	}

}
