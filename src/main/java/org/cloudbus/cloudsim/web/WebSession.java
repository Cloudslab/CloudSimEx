package org.cloudbus.cloudsim.web;

public class WebSession {

	private IGenerator<WebCloudlet> appServerCloudLets;
	private IGenerator<WebCloudlet> dbServerCloudLets;

	private WebCloudlet currentAppServerCloudLet = null;
	private WebCloudlet currentDBServerCloudLet = null;

	private int appVmId = -1;
	private int dbVmId = -1;

	public WebSession(IGenerator<WebCloudlet> appServerCloudLets,
			IGenerator<WebCloudlet> dbServerCloudLets) {
		super();
		this.appServerCloudLets = appServerCloudLets;
		this.dbServerCloudLets = dbServerCloudLets;
	}

	public WebCloudlet[] pollCloudlets(final double currTime) {

		WebCloudlet[] result = null;
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
			result = new WebCloudlet[] { appServerCloudLets.poll(),
					dbServerCloudLets.poll() };
			currentAppServerCloudLet = result[0];
			currentDBServerCloudLet = result[1];

			currentAppServerCloudLet.setVmId(appVmId);
			currentDBServerCloudLet.setVmId(dbVmId);
		}
		return result;
	}

	public void prefetch(final Double time) {
		appServerCloudLets.prefetch(time);
		dbServerCloudLets.prefetch(time);
	}

	public int getAppVmId() {
		return appVmId;
	}

	public void setAppVmId(int appVmId) {
		this.appVmId = appVmId;
	}

	public int getDbVmId() {
		return dbVmId;
	}

	public void setDbVmId(int dbVmId) {
		this.dbVmId = dbVmId;
	}

}
