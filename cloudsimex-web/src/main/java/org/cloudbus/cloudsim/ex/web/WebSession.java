package org.cloudbus.cloudsim.ex.web;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.cloudbus.cloudsim.ex.disk.HddVm;
import org.cloudbus.cloudsim.ex.util.Id;
import org.cloudbus.cloudsim.ex.util.Textualize;

/**
 * A web session is a session established between a user an application server
 * (deployed in a VM) and can use several database servers (deployed in separate
 * VMs). Throughout it's lifetime a session continuously generates workload on
 * the servers deployed in these virtual machines. The workload is represented
 * as cloudlets, which are sent to the assigned servers.
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
@Textualize(properties = { "SessionId", "AppVmId", "DbVmId", "ReadableStartTime", "StartTime", "FinishTime",
	"IdealEnd", "Delay", "Complete" })
public class WebSession {

    private final IGenerator<? extends WebCloudlet> appServerCloudLets;
    private final IGenerator<? extends Collection<? extends WebCloudlet>> dbServerCloudLets;

    private WebCloudlet currentAppServerCloudLet = null;
    private List<? extends WebCloudlet> currentDBServerCloudLets = null;

    private Integer appVmId = null;
    private IDBBalancer dbBalancer;

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
     *            - the use id. A valid user id must be set either through a
     *            constructor or the set method, before this instance is used.
     */
    public WebSession(final IGenerator<? extends WebCloudlet> appServerCloudLets,
	    final IGenerator<? extends Collection<? extends WebCloudlet>> dbServerCloudLets,
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
     * Creates cloudlets to submit to the virtual machines. If at this time no
     * cloudlets should be sent to the servers null is returned.
     * 
     * @param currTime
     *            - the current time of the simulation.
     * @return the result as described above.
     */
    public StepCloudlets pollCloudlets(final double currTime) {

	StepCloudlets result = null;
	boolean appCloudletFinished = currentAppServerCloudLet == null
		|| currentAppServerCloudLet.isFinished();
	boolean dbCloudletFinished = currentDBServerCloudLets == null
		|| areAllCloudletsFinished(currentDBServerCloudLets);
	boolean appServerNextReady = !appServerCloudLets.isEmpty()
		&& appServerCloudLets.peek().getIdealStartTime() <= currTime;
	boolean dbServerNextReady = !dbServerCloudLets.isEmpty()
		&& getEarliestIdealStartTime(dbServerCloudLets.peek()) <= currTime;

	if (cloudletsLeft != 0 && appCloudletFinished && dbCloudletFinished && appServerNextReady && dbServerNextReady) {
	    result = new StepCloudlets(
		    appServerCloudLets.poll(),
		    new ArrayList<>(dbServerCloudLets.poll()));
	    currentAppServerCloudLet = result.asCloudlet;
	    currentDBServerCloudLets = result.dbCloudlets;

	    currentAppServerCloudLet.setVmId(appVmId);
	    assignToServer(dbBalancer, currentDBServerCloudLets);

	    currentAppServerCloudLet.setSessionId(getSessionId());
	    currentAppServerCloudLet.setUserId(userId);
	    setSessionAndUserIds(getSessionId(), userId, currentDBServerCloudLets);

	    cloudletsLeft--;

	    if (startTime == null) {
		startTime = Math.min(currentAppServerCloudLet.getIdealStartTime(),
			getEarliestIdealStartTime(currentDBServerCloudLets));
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
     * @return - the current db server cloudlets.
     */
    /* package access */List<? extends WebCloudlet> getCurrentDBServerCloudLets() {
	return currentDBServerCloudLets;
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
     * Returns the balancer of DB cloudlets to the VMs hosting the db server.
     * 
     * @return the balancer of DB cloudlets to the VMs hosting the db server.
     */
    public IDBBalancer getDbBalancer() {
	return dbBalancer;
    }

    /**
     * Sets the balancer of DB cloudlets to the VMs hosting the db server. NOTE!
     * you must set this before polling cloudlets.
     * 
     * @param dbBalancer
     *            - the DB VM balancer.
     */
    public void setDbBalancer(final IDBBalancer dbBalancer) {
	this.dbBalancer = dbBalancer;
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
	return Math.max(0, getFinishTime() - idealEnd);
    }

    /**
     * Returns the finish of this session. If the session has a cloudlet running
     * -1 is returned.
     * 
     * @return the finish time of this session. If the session has a cloudlet
     *         running -1 is returned.
     */
    public double getFinishTime() {
	double finishAS = currentAppServerCloudLet == null || !currentAppServerCloudLet.isFinished() ? -1
		: currentAppServerCloudLet.getFinishTime();
	double finishDB = currentDBServerCloudLets == null || !areAllCloudletsFinished(currentDBServerCloudLets) ? -1
		: getLatestFinishTime(currentDBServerCloudLets);
	return Math.max(0, Math.max(finishAS, finishDB));
    }

    /**
     * Returns the ideal end time of this session.
     * 
     * @return the ideal end time of this session.
     */
    public double getIdealEnd() {
	return idealEnd;
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
		currentDBServerCloudLets != null &&
		areAllCloudletsFinished(currentDBServerCloudLets);
    }

    /**
     * Returns if the virtual machines serving this session have been set up, so
     * that it can execute.
     * 
     * @return if the virtual machines serving this session have been set up, so
     *         that it can execute.
     */
    public boolean areVirtualMachinesReady() {
	for (HddVm vm : getDbBalancer().getVMs()) {
	    if (vm.getHost() == null) {
		return false;
	    }
	}
	return appVmId != null;
    }

    private static boolean areAllCloudletsFinished(final List<? extends WebCloudlet> cloudlets) {
	boolean result = true;
	for (WebCloudlet cl : cloudlets) {
	    if (!cl.isFinished()) {
		result = false;
		break;
	    }
	}
	return result;
    }

    private static double getLatestFinishTime(final Collection<? extends WebCloudlet> cloudlets) {
	double result = -1;
	for (WebCloudlet cl : cloudlets) {
	    if (cl.getFinishTime() > result) {
		result = cl.getFinishTime();
	    }
	}
	return result;
    }

    private static double getEarliestIdealStartTime(final Collection<? extends WebCloudlet> cloudlets) {
	double result = -1;
	for (WebCloudlet cl : cloudlets) {
	    if (result == -1 || cl.getIdealStartTime() < result) {
		result = cl.getIdealStartTime();
	    }
	}
	return result;
    }

    private static void assignToServer(final IDBBalancer dbBalancer, final Collection<? extends WebCloudlet> cloudlets) {
	for (WebCloudlet cl : cloudlets) {
	    dbBalancer.allocateToServer(cl);
	}
    }

    private static void setSessionAndUserIds(final int sessId, final int userId,
	    final Collection<? extends WebCloudlet> cloudlets) {
	for (WebCloudlet cl : cloudlets) {
	    cl.setSessionId(sessId);
	    cl.setUserId(userId);
	}
    }

    /**
     * 
     * A structure, containing the cloudlets that need to be executed for a step
     * in the simulation.
     * 
     * @author nikolay.grozev
     * 
     */
    public static class StepCloudlets {
	/**
	 * The app server clouldet for the step.
	 */
	public WebCloudlet asCloudlet;
	/**
	 * The db cloudlets for the step.
	 */
	public List<? extends WebCloudlet> dbCloudlets;

	public StepCloudlets(final WebCloudlet asCloudlet, final List<? extends WebCloudlet> dbCloudlets) {
	    super();
	    this.asCloudlet = asCloudlet;
	    this.dbCloudlets = dbCloudlets;
	}
    }

}
