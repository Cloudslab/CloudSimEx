package org.cloudbus.cloudsim.incubator.web;

import org.cloudbus.cloudsim.incubator.util.Id;
import org.cloudbus.cloudsim.incubator.util.Textualize;

/**
 * A web session is a session established between a user an application server
 * (deployed in a VM) and a database server (deployed in another VM). Throughout
 * it's lifetime a session continuously generates workload on the servers
 * deployed in these virtual machines. The workload is represented as cloudlets,
 * which are sent to the assigned servers.
 * 
 * <br/>
 * <br/>
 * 
 * To achieve this a web session generates two consequent "streams" of cloudlets
 * and directs them to the servers. These streams are based on two instances of
 * {@link IGenerator} passed when constructing the session. The session takes
 * care to synchronize the two generators, so that consequent cloudlets from the
 * two generators are executed at the same pace. Thus if one of the servers is
 * performing better than the other, this will not be reflected in a quicker
 * exhaustion of its respective generator.
 * 
 * <br/>
 * <br/>
 * 
 * Since the session utilizes {@link IGenerator} instances, which are unaware of
 * the simulation time, it needs to notify them of how time changes. By design
 * (to improve testability) web sessions are also unaware of simulation time.
 * Thus they need to be notified by an external "clock entity" of the simulation
 * time at a predefined period of time. This is done by the
 * {@link WebSession.notifyOfTime} method.
 * 
 * 
 * @author nikolay.grozev
 * 
 */
@Textualize(properties = { "ReadableStartTime", "StartTime", "SessionId", "AppVmId", "DbVmId", "Delay", "Complete" })
public class WebSession {

    private final IGenerator<? extends WebCloudlet> appServerCloudLets;
    private final IGenerator<? extends WebCloudlet> dbServerCloudLets;

    private WebCloudlet currentAppServerCloudLet = null;
    private WebCloudlet currentDBServerCloudLet = null;

    private Integer appVmId = null;
    private Integer dbVmId = null;

    private int userId;
    private int cloudletsLeft;

    private final double idealEnd;
    private Double startTime;

    private final int sessionId;

    /**
     * Creates a new instance with the specified cloudlet generators.
     * 
     * @param appServerCloudLets
     *            - a generator for cloudlets for the application server. Must
     *            not be null.
     * @param dbServerCloudLets
     *            - a generator for cloudlets for the db server. Must not be
     *            null.
     * @param userId
     *            - the use id. Not a valid user id must be sed either through a
     *            constructor or the set method, before this instance is used.
     */
    public WebSession(final IGenerator<? extends WebCloudlet> appServerCloudLets,
	    final IGenerator<? extends WebCloudlet> dbServerCloudLets,
	    final int userId,
	    final int numberOfCloudlets,
	    final double idealEnd) {
	super();
	sessionId = Id.pollId(getClass());

	this.appServerCloudLets = appServerCloudLets;
	this.dbServerCloudLets = dbServerCloudLets;
	this.userId = userId;

	this.cloudletsLeft = numberOfCloudlets;
	this.idealEnd = idealEnd;
    }

    public int getUserId() {
	return userId;
    }

    public void setUserId(final int userId) {
	this.userId = userId;
    }

    /**
     * Creates two cloudlets to submit to the virtual machines. The first
     * cloudlet is for the application server, the second - for the database. If
     * at this time no cloudlets should be sent to the servers null is returned.
     * 
     * @param currTime
     *            - the current time of the simulation.
     * @return the result as described above.
     */
    public WebCloudlet[] pollCloudlets(final double currTime) {

	WebCloudlet[] result = null;
	boolean appCloudletFinished = currentAppServerCloudLet == null
		|| currentAppServerCloudLet.isFinished();
	boolean dbCloudletFinished = currentDBServerCloudLet == null
		|| currentDBServerCloudLet.isFinished();
	boolean appServerNextReady = !appServerCloudLets.isEmpty()
		&& appServerCloudLets.peek().getIdealStartTime() <= currTime;
	boolean dbServerNextReady = !dbServerCloudLets.isEmpty()
		&& dbServerCloudLets.peek().getIdealStartTime() <= currTime;

	if (cloudletsLeft != 0 && appCloudletFinished && dbCloudletFinished && appServerNextReady && dbServerNextReady) {
	    result = new WebCloudlet[] {
		    appServerCloudLets.poll(),
		    dbServerCloudLets.poll() };
	    currentAppServerCloudLet = result[0];
	    currentDBServerCloudLet = result[1];

	    currentAppServerCloudLet.setVmId(appVmId);
	    currentDBServerCloudLet.setVmId(dbVmId);

	    currentAppServerCloudLet.setSessionId(getSessionId());
	    currentDBServerCloudLet.setSessionId(getSessionId());

	    currentAppServerCloudLet.setUserId(userId);
	    currentDBServerCloudLet.setUserId(userId);
	    cloudletsLeft--;

	    if (startTime == null) {
		startTime = Math.min(currentAppServerCloudLet.getIdealStartTime(),
			currentDBServerCloudLet.getIdealStartTime());
	    }
	}
	return result;
    }

    /**
     * NOTE!!! - used only for test purposes.
     * 
     * @return - the current app server cloudlet
     */
    /* package access */WebCloudlet getCurrentAppServerCloudLet() {
	return currentAppServerCloudLet;
    }

    /**
     * NOTE!!! - used only for test purposes.
     * 
     * @return - the current db server cloudlet.
     */
    /* package access */WebCloudlet getCurrentDBServerCloudLet() {
	return currentDBServerCloudLet;
    }

    /**
     * Gets notified of the current time.
     * 
     * @param time
     *            - the current CloudSim time.
     */
    public void notifyOfTime(final double time) {
	appServerCloudLets.notifyOfTime(time);
	dbServerCloudLets.notifyOfTime(time);
    }

    /**
     * Returns the id of the VM hosting the application server.
     * 
     * @return the id of the VM hosting the application server.
     */
    public Integer getAppVmId() {
	return appVmId;
    }

    /**
     * Sets the id of the VM hosting the app server. NOTE! you must set this id
     * before polling cloudlets.
     * 
     * @param appVmId
     *            - the id of the VM hosting the app server.
     */
    public void setAppVmId(final int appVmId) {
	this.appVmId = appVmId;
    }

    /**
     * Returns the id of the VM hosting the db server.
     * 
     * @return the id of the VM hosting the db server.
     */
    public Integer getDbVmId() {
	return dbVmId;
    }

    /**
     * Sets the id of the VM hosting the db server. NOTE! you must set this id
     * before polling cloudlets.
     * 
     * @param dbVmId
     *            - the id of the VM hosting the db server.
     */
    public void setDbVmId(final int dbVmId) {
	this.dbVmId = dbVmId;
    }

    /**
     * Returns the id of the session.
     * 
     * @return the id of the session.
     */
    public int getSessionId() {
	return sessionId;
    }

    /**
     * Returns the accumulated delay of this session. If the session has a
     * cloudlet running -1 is returned.
     * 
     * @return the accumulated delay of this session. If the session has a
     *         cloudlet running -1 is returned.
     */
    public double getDelay() {
	double delayAS = currentAppServerCloudLet == null || !currentAppServerCloudLet.isFinished() ? -1
		: currentAppServerCloudLet.getFinishTime() - idealEnd;
	double delayDB = currentDBServerCloudLet == null || !currentDBServerCloudLet.isFinished() ? -1
		: currentDBServerCloudLet.getFinishTime() - idealEnd;
	return Math.max(0, Math.max(delayAS, delayDB));
    }

    /**
     * Returns the starting time of the session.
     * 
     * @return the starting time of the session.
     */
    public double getStartTime() {
	return startTime;
    }

    /**
     * Returns a readable representation of the start time.
     * 
     * @return a readable representation of the start time.
     */
    public String getReadableStartTime() {
	int days = (startTime.intValue() / (24 * 3600));
	int hours = (startTime.intValue() / 3600);
	int minutes = (startTime.intValue()) / 60;
	int rest = (startTime.intValue()) % 60;

	// Now normalize the values
	hours = hours % 24;
	minutes = minutes % 60;
	return String.format("%2d: %2d: %2d: %2d", days, hours, minutes, rest);
    }

    /**
     * Returns if the session has completed.
     * 
     * @return if the session has completed.
     */
    public boolean isComplete() {
	return cloudletsLeft == 0 &&
		currentAppServerCloudLet != null &&
		currentAppServerCloudLet.isFinished() &&
		currentDBServerCloudLet != null &&
		currentDBServerCloudLet.isFinished();
    }
}
