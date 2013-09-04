package org.cloudbus.cloudsim.ex.web.workload.brokers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.ex.MonitoringBorkerEX;
import org.cloudbus.cloudsim.ex.util.CustomLog;
import org.cloudbus.cloudsim.ex.web.ILoadBalancer;
import org.cloudbus.cloudsim.ex.web.WebCloudlet;
import org.cloudbus.cloudsim.ex.web.WebSession;
import org.cloudbus.cloudsim.ex.web.workload.IWorkloadGenerator;

/**
 * A broker that takes care of the submission of web sessions to the data center
 * it handles. The broker submits the cloudlets of the provided web sessions
 * continuously over a specified period. Consequently clients must specify the
 * endpoint (in terms of time) of the simulation.
 * 
 * @author nikolay.grozev
 * 
 */
public class WebBroker extends MonitoringBorkerEX {

    // FIXME find a better way to get an unused tag instead of hardcoding
    // 1234567
    protected static final int TIMER_TAG = 1234567;
    protected static final int SUBMIT_SESSION_TAG = TIMER_TAG + 1;
    protected static final int UPDATE_SESSION_TAG = SUBMIT_SESSION_TAG + 1;

    private boolean isTimerRunning = false;
    private final double stepPeriod;
    private final Map<Long, ILoadBalancer> appsToLoadBalancers = new HashMap<>();
    private final Map<Long, List<IWorkloadGenerator>> appsToGenerators = new HashMap<>();

    private final LinkedHashMap<Integer, WebSession> servedSessions = new LinkedHashMap<>();
    private final List<WebSession> canceledSessions = new ArrayList<>();

    /** Mapping of application Ids to entry points. */
    private final Map<Long, EntryPoint> entryPoins = new HashMap<>();

    private String[] metadata;

    /**
     * By default CloudSim's brokers use all available datacenters. So we need
     * to enforce only the data center we want.
     */
    private final int dataCenterId;

    /**
     * Creates a new web broker.
     * 
     * @param name
     *            - the name of the broker.
     * @param refreshPeriod
     *            - the period of polling web sessions for new cloudlets.
     * @param lifeLength
     *            - the length of the simulation.
     * @param dataCenterIds
     *            - the ids of the datacenters this broker operates with. If
     *            null all present data centers are used.
     * 
     * @throws Exception
     *             - if something goes wrong. See the documentation of the super
     *             class.
     */
    public WebBroker(final String name, final double refreshPeriod, final double lifeLength,
	    final double monitoringPeriod, final int dataCenterId, final String... metadata)
	    throws Exception {
	super(name, lifeLength, monitoringPeriod);
	this.stepPeriod = refreshPeriod;
	this.dataCenterId = dataCenterId;
	this.metadata = metadata;
    }

    /**
     * Creates a new web broker.
     * 
     * @param name
     *            - the name of the broker.
     * @param refreshPeriod
     *            - the period of polling web sessions for new cloudlets.
     * @param lifeLength
     *            - the length of the simulation.
     * @param dataCenterIds
     *            - the ids of the datacenters this broker operates with. If
     *            null all present data centers are used.
     * 
     * @throws Exception
     *             - if something goes wrong. See the documentation of the super
     *             class.
     */
    public WebBroker(final String name, final double refreshPeriod, final double lifeLength, final int dataCenterId)
	    throws Exception {
	this(name, refreshPeriod, lifeLength, -1, dataCenterId);
    }

    /**
     * Returns the id of the datacentre that this web broker handles.
     * 
     * @return the id of the datacentre that this web broker handles.
     */
    public int getDataCenterId() {
	return dataCenterId;
    }

    /**
     * Returns the sessions canceled due to lack of resources to serve them.
     * 
     * @return the session that were canceled due to lack of resources to serve
     *         them.
     */
    public List<WebSession> getCanceledSessions() {
	return canceledSessions;
    }

    public String[] getMetadata() {
        return metadata;
    }

    /**
     * Returns the sessions that were successfully served.
     * 
     * @return the sessions that were successfully served.
     */
    public List<WebSession> getServedSessions() {
	return new ArrayList<>(servedSessions.values());
    }

    /**
     * Returns the load balancers of this broker.
     * 
     * @return the load balancers of this broker.
     */
    public Map<Long, ILoadBalancer> getLoadBalancers() {
	return appsToLoadBalancers;
    }

    public double getStepPeriod() {
	return stepPeriod;
    }

    @Override
    public void processEvent(final SimEvent ev) {
	if (!isTimerRunning) {
	    isTimerRunning = true;
	    sendNow(getId(), TIMER_TAG);
	}

	super.processEvent(ev);
    }

    public void submitSessions(final List<WebSession> webSessions, final long appId) {
	if (entryPoins.containsKey(appId)) {
	    EntryPoint entryPoint = entryPoins.get(appId);
	    entryPoint.dispatchSessions(webSessions);
	} else {
	    submitSessionsDirectly(webSessions, appId);
	}
    }

    /**
     * Submits new web sessions to this broker.
     * 
     * @param webSessions
     *            - the new web sessions.
     */
    /* pack access */void submitSessionsDirectly(final List<WebSession> webSessions, final long appId) {
	if (!CloudSim.running()) {
	    submitSessionsAtTime(webSessions, appId, 0);
	} else {
	    for (WebSession session : webSessions) {
		appsToLoadBalancers.get(appId).assignToServers(session);

		// If the load balancer could not assign it...
		if (session.getAppVmId() == null || session.getDbBalancer() == null) {
		    canceledSessions.add(session);
		    CustomLog.printf("Session could not be served and is canceled. Session id:%d",
			    session.getSessionId());
		} else {
		    // Let the session prepare the first cloudlets
		    if (session.areVirtualMachinesReady()) {
			session.notifyOfTime(CloudSim.clock());
		    } else {
			// If the VMs are not yet ready - start the session
			// later and extend its ideal end
			session.setIdealEnd(session.getIdealEnd() + stepPeriod);
			session.notifyOfTime(CloudSim.clock() + stepPeriod);
		    }

		    servedSessions.put(session.getSessionId(), session);

		    // Start the session or schedule it if its VMs are not
		    // initiated.
		    if (session.areVirtualMachinesReady()) {
			updateSessions(session.getSessionId());
		    } else {
			send(getId(), stepPeriod, UPDATE_SESSION_TAG, session.getSessionId());
		    }
		}
		session.setUserId(getId());
	    }
	}
    }

    /**
     * Submits new sessions after the specified delay.
     * 
     * @param webSessions
     *            - the list of sessions to submit.
     * @param loadBalancerId
     *            - the id of the load balancer to submit to.
     * @param delay
     *            - the delay to submit after.
     */
    public void submitSessionsAtTime(final List<WebSession> webSessions, final long loadBalancerId, final double delay) {
	Object data = new Object[] { webSessions, loadBalancerId };
	if (isTimerRunning) {
	    send(getId(), delay, SUBMIT_SESSION_TAG, data);
	} else {
	    presetEvent(getId(), SUBMIT_SESSION_TAG, data, delay);
	}
    }

    /**
     * Adds a new load balancer for handling incoming sessions.
     * 
     * @param balancer
     *            - the balancer to add. Must not be null.
     */
    public void addLoadBalancer(final ILoadBalancer balancer) {
	appsToLoadBalancers.put(balancer.getAppId(), balancer);
	appsToGenerators.put(balancer.getAppId(), new ArrayList<IWorkloadGenerator>());
    }

    /**
     * Associates this broker with an entry point, which can distribute the
     * incoming workload to multiple borkers/clouds.
     * 
     * @param entryPoint
     *            - the entry point. Must not be null.
     */
    public void addEntryPoint(final EntryPoint entryPoint) {
	EntryPoint currEP = entryPoins.get(entryPoint.getAppId());
	if (entryPoint != entryPoins.get(entryPoint.getAppId())) {
	    entryPoins.put(entryPoint.getAppId(), entryPoint);
	    entryPoint.registerBroker(this);
	    if (currEP != null) {
		currEP.deregisterBroker(this);
	    }
	}
    }

    /**
     * Dis-associates the entry point and this broker/data centre.
     * 
     * @param entryPoint
     *            - the entry point. Must not be null.
     */
    public void removeEntryPoint(final EntryPoint entryPoint) {
	if (entryPoint == entryPoins.get(entryPoint.getAppId())) {
	    entryPoins.remove(entryPoint.getAppId());
	    entryPoint.deregisterBroker(this);
	}
    }

    /**
     * Adds a new workload generator for the specified load balancer.
     * 
     * @param workloads
     *            - the workload generators.
     * @param loadBalancerId
     *            - the id of the load balancer. There must such a load balancer
     *            registered before this method is called.
     */
    public void addWorkloadGenerators(final List<? extends IWorkloadGenerator> workloads, final long loadBalancerId) {
	appsToGenerators.get(loadBalancerId).addAll(workloads);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.cloudbus.cloudsim.DatacenterBroker#processOtherEvent(org.cloudbus
     * .cloudsim.core.SimEvent)
     */
    @SuppressWarnings("unchecked")
    @Override
    protected void processOtherEvent(final SimEvent ev) {
	switch (ev.getTag()) {
	    case TIMER_TAG:
		if (CloudSim.clock() < getLifeLength()) {
		    send(getId(), stepPeriod, TIMER_TAG);
		    generateWorkload();
		}
		break;
	    case SUBMIT_SESSION_TAG:
		Object[] data = (Object[]) ev.getData();
		submitSessions((List<WebSession>) data[0], (Long) data[1]);
		break;
	    case UPDATE_SESSION_TAG:
		Integer sessId = (Integer) ev.getData();
		updateSessions(sessId);
	    default:
		super.processOtherEvent(ev);
	}
    }

    private void generateWorkload() {
	double currTime = CloudSim.clock();
	for (Map.Entry<Long, List<IWorkloadGenerator>> balancersToWorkloadGens : appsToGenerators.entrySet()) {
	    long balancerId = balancersToWorkloadGens.getKey();
	    for (IWorkloadGenerator gen : balancersToWorkloadGens.getValue()) {
		Map<Double, List<WebSession>> timeToSessions = gen.generateSessions(currTime, stepPeriod);
		for (Map.Entry<Double, List<WebSession>> sessEntry : timeToSessions.entrySet()) {
		    if (currTime == sessEntry.getKey()) {
			submitSessions(sessEntry.getValue(), balancerId);
		    } else {
			submitSessionsAtTime(sessEntry.getValue(), balancerId, sessEntry.getKey() - currTime);
		    }
		}
	    }
	}
    }

    private void updateSessions(final Integer... sessionIds) {
	for (Integer id : sessionIds.length == 0 ? servedSessions.keySet() : Arrays.asList(sessionIds)) {
	    WebSession sess = servedSessions.get(id);

	    // Check if all VMs for the sessions are set. In the simulation
	    // start, this may not be so, as the refreshing action of the broker
	    // may happen before the mapping of VMs to hosts.
	    if (sess.areVirtualMachinesReady()) {
		double currTime = CloudSim.clock();

		// sess.notifyOfTime(currTime);
		WebSession.StepCloudlets webCloudlets = sess.pollCloudlets(currTime);

		if (webCloudlets != null) {
		    getCloudletList().add(webCloudlets.asCloudlet);
		    getCloudletList().addAll(webCloudlets.dbCloudlets);
		    submitCloudlets();

		    double nextIdealTime = currTime + stepPeriod;
		    sess.notifyOfTime(nextIdealTime);

		    send(getId(), stepPeriod, UPDATE_SESSION_TAG, sess.getSessionId());
		}
	    }
	}
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.cloudbus.cloudsim.DatacenterBroker#processCloudletReturn(org.cloudbus
     * .cloudsim.core.SimEvent)
     */
    @Override
    protected void processCloudletReturn(final SimEvent ev) {
	super.processCloudletReturn(ev);
	Cloudlet cloudlet = (Cloudlet) ev.getData();
	if (CloudSim.clock() < getLifeLength()) {
	    // kill the broker only if its life length is over/expired
	    if (cloudlet instanceof WebCloudlet) {
		updateSessions(((WebCloudlet) cloudlet).getSessionId());
	    }
	}
    }

    /*
     * (non-Javadoc)
     * 
     * @see cloudsim.core.SimEntity#startEntity()
     */
    @Override
    public void startEntity() {
	Log.printLine(getName() + " is starting...");
	schedule(getId(), 0, CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST, Arrays.asList(dataCenterId));
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void processResourceCharacteristicsRequest(final SimEvent ev) {
	setDatacenterIdsList(ev.getData() == null ? CloudSim.getCloudResourceList() : (List<Integer>) ev.getData());
	setDatacenterCharacteristicsList(new HashMap<Integer, DatacenterCharacteristics>());

	for (Integer datacenterId : getDatacenterIdsList()) {
	    sendNow(datacenterId, CloudSimTags.RESOURCE_CHARACTERISTICS, getId());
	}
    }

}
